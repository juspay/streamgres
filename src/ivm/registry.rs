//! Subscription lifecycle: registering, replacing, and removing
//! subscriptions on the [`SingleTableIVM`], including twin sharing (an
//! identical query served from the shared frame) and the in-place
//! condition edit the join layer uses for its `IN` lists.

use super::index::TableIndex;
use super::{window, QueryId, SingleTableIVM};
use crate::model::{Condition, DataFrameKey, DataFrameOperation, DataFrameRow, SingleTableReadQuery};

impl SingleTableIVM {
    /// Register a client subscription under `query_uuid`, returning its
    /// initial result set as `Add` operations.
    ///
    /// The query's `Where` is normalized to DNF and indexed in the table's
    /// routing index (a `WHERE FALSE` query has no disjuncts and touches no
    /// index at all). The initial rows come from `initial` when the caller
    /// already fetched them (the join layer narrows sub queries itself) —
    /// `initial` must be the query's **complete** current result set, since
    /// twin sharing may serve later identical registrations from it;
    /// otherwise, a query **structurally identical** to one already
    /// registered under another uuid is served from the shared frame — the
    /// twin's current rows, no storage query, counted in
    /// `snapshots_shared` — and only a query with no twin runs against
    /// [`super::Storage`]. Rows already shared with other subscriptions
    /// are tagged rather than duplicated.
    ///
    /// Re-registering a uuid with the identical query is a no-op returning
    /// no operations (the client already holds its state). Re-registering
    /// with a changed query replaces the subscription: old index entries
    /// and row tags removed first — counting fires on exact counts, so a
    /// leftover link would corrupt the replacement's counters.
    pub fn register_query(
        &mut self,
        query_uuid: impl Into<QueryId>,
        select_query: SingleTableReadQuery,
        initial: Option<Vec<(DataFrameKey, DataFrameRow)>>,
    ) -> Vec<DataFrameOperation> {
        let query_uuid = query_uuid.into();
        self.stats.queries_registered += 1;

        if self.select_queries.get(&query_uuid) == Some(&select_query) {
            return Vec::new();
        }
        if self.select_queries.contains_key(&query_uuid) {
            self.unregister_query(query_uuid.as_str());
        }

        let dnf = select_query.filter.to_dnf();
        if !dnf.is_empty() {
            self.tables
                .entry(select_query.table.clone())
                .or_default()
                .register(&query_uuid, dnf, &mut self.stats);
        }
        let records = match initial {
            Some(records) => records,
            None => match self.identical_subscription(&select_query, &query_uuid) {
                Some(twin) => {
                    self.stats.snapshots_shared += 1;
                    self.rows_of(&twin)
                }
                None => self.storage.select(&Self::storage_query(&select_query)),
            },
        };
        self.select_queries
            .insert(query_uuid.clone(), select_query);

        let mut ops = Vec::new();
        for (key, row) in records {
            if let Some(op) = self.upsert_row(query_uuid.as_str(), &key, &row) {
                ops.push(op);
            }
        }
        self.rebuild_window(query_uuid.as_str());
        ops.extend(self.evict_overflow(query_uuid.as_str()));
        self.sync_boundary(query_uuid.as_str());
        ops
    }

    /// Edit one condition of a subscription's filter **in place** — the
    /// value-list change of a join `IN` gaining or losing a value. The
    /// stored filter's matching leaves are rewritten and the routing
    /// index swaps the condition inside this subscription's disjuncts
    /// only ([`index`-module `update_condition`], splitting shared
    /// counters correctly) — no DNF re-normalization, no
    /// unregister/re-register churn.
    ///
    /// As with [`SingleTableIVM::replace_query`], the caller owns
    /// reconciling held rows with the new condition (fetch what widened
    /// in, prune what narrowed out), and the subscription is skipped as a
    /// twin donor until [`SingleTableIVM::mark_reconciled`]. Unknown
    /// uuids and identical conditions are no-ops.
    pub fn replace_condition(&mut self, query_uuid: &str, old: &Condition, new: Condition) {
        let Some(query) = self.select_queries.get_mut(query_uuid) else {
            return;
        };
        if *old == new {
            return;
        }
        query.filter.replace_condition(old, &new);
        let table = query.table.clone();
        if let Some(table_index) = self.tables.get_mut(&table) {
            table_index.update_condition(&QueryId::from(query_uuid), old, &new, &mut self.stats);
        }
        self.stale_views.insert(QueryId::from(query_uuid));
    }

    /// Declare a `replace_query` / `replace_condition` reconciliation
    /// complete: the caller has finished every fetch and prune the change
    /// required, so the subscription's held rows again match its filter
    /// and it may donate twin snapshots. Only the caller can know when
    /// that point is reached — a widened filter needs a fetch, a narrowed
    /// one a prune, a swapped one both — so nothing clears the flag
    /// implicitly. A no-op for unknown or already-reconciled uuids.
    pub fn mark_reconciled(&mut self, query_uuid: &str) {
        self.stale_views.remove(query_uuid);
    }

    /// The storage-facing form of a subscription's query: identical except
    /// that a windowed (finite-limit) query is issued with its limit
    /// doubled, to fill the window's buffer.
    pub(super) fn storage_query(query: &SingleTableReadQuery) -> SingleTableReadQuery {
        SingleTableReadQuery {
            limit: window::storage_limit(query),
            ..query.clone()
        }
    }

    /// Remove a subscription: its routing-index entries and its tag on
    /// every shared row it holds — walked off its held-key index, not by
    /// scanning the table — dropping rows nobody holds anymore. A table
    /// index that routes nothing afterwards is dropped too. Unknown uuids
    /// are a no-op.
    pub fn unregister_query(&mut self, query_uuid: &str) {
        let Some(query) = self.select_queries.remove(query_uuid) else {
            return;
        };
        if let Some(table_index) = self.tables.get_mut(&query.table) {
            table_index.unregister(query_uuid);
        }
        if self
            .tables
            .get(&query.table)
            .is_some_and(TableIndex::is_empty)
        {
            self.tables.remove(&query.table);
        }
        self.stale_views.remove(query_uuid);
        self.windows.remove(query_uuid);
        let keys = self.held.remove(query_uuid).unwrap_or_default();
        if let Some(frame) = self.frames.get_mut(&query.table) {
            for key in keys {
                if let Some(row) = frame.rows.get_mut(&key) {
                    row.subscribers.remove(query_uuid);
                    if row.subscribers.is_empty() {
                        frame.rows.remove(&key);
                    }
                }
            }
        }
        if self
            .frames
            .get(&query.table)
            .is_some_and(|frame| frame.rows.is_empty())
        {
            self.frames.remove(&query.table);
        }
    }

    /// Swap the registered query of `query_uuid` for `select_query`,
    /// reindexing its routing while leaving its frame tags untouched.
    ///
    /// The caller owns keeping held rows consistent with the new filter —
    /// this is the general reseat seam (for a single `IN` value change,
    /// [`SingleTableIVM::replace_condition`] is the cheap edit). Until the
    /// caller declares that reconciliation complete
    /// ([`SingleTableIVM::mark_reconciled`], after however many fetches
    /// and prunes the change needs — a swap needs both), the subscription
    /// is skipped as a twin donor: its filter is ahead of its held rows,
    /// and a registration served from it would inherit the gap
    /// permanently. The table must stay the same (a cross-table swap is
    /// refused); unknown uuids and identical queries are no-ops.
    pub fn replace_query(&mut self, query_uuid: &str, select_query: SingleTableReadQuery) {
        let Some(existing) = self.select_queries.get_mut(query_uuid) else {
            return;
        };
        if *existing == select_query || existing.table != select_query.table {
            return;
        }
        let table = select_query.table.clone();
        *existing = select_query.clone();
        self.stale_views.insert(QueryId::from(query_uuid));
        if let Some(table_index) = self.tables.get_mut(&table) {
            table_index.unregister(query_uuid);
        }
        let dnf = select_query.filter.to_dnf();
        if !dnf.is_empty() {
            self.tables
                .entry(table.clone())
                .or_default()
                .register(&QueryId::from(query_uuid), dnf, &mut self.stats);
        }
        if self
            .tables
            .get(&table)
            .is_some_and(TableIndex::is_empty)
        {
            self.tables.remove(&table);
        }
        self.rebuild_window(query_uuid);
        self.sync_boundary(query_uuid);
    }

    /// Another registered subscription with a structurally identical
    /// query, if any — the sharing seam of registration. Skips
    /// subscriptions in a `replace_query` / `replace_condition`
    /// maintenance window, whose rows lag their filter.
    ///
    /// A linear scan: registration is rare. A future query-keyed index
    /// must also be maintained inside `replace_query` and
    /// `replace_condition`, which rewrite stored queries in place.
    fn identical_subscription(
        &self,
        select_query: &SingleTableReadQuery,
        exclude: &QueryId,
    ) -> Option<QueryId> {
        self.select_queries
            .iter()
            .find(|(uuid, query)| {
                *query == select_query
                    && *uuid != exclude
                    && !self.stale_views.contains(uuid.as_str())
            })
            .map(|(uuid, _)| uuid.clone())
    }

    /// The (key, image) pairs a subscription currently holds, materialized
    /// from its held-key index — what a twin registration is served
    /// instead of a storage result.
    fn rows_of(&self, query_uuid: &QueryId) -> Vec<(DataFrameKey, DataFrameRow)> {
        let Some(query) = self.select_queries.get(query_uuid) else {
            return Vec::new();
        };
        let Some(frame) = self.frames.get(&query.table) else {
            return Vec::new();
        };
        let Some(keys) = self.held.get(query_uuid) else {
            return Vec::new();
        };
        keys.iter()
            .filter_map(|key| {
                frame.rows.get(key).map(|row| {
                    (
                        key.clone(),
                        DataFrameRow {
                            data: row.data.clone(),
                        },
                    )
                })
            })
            .collect()
    }
}
