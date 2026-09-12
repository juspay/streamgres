//! Subscription lifecycle: registering, replacing, and removing
//! subscriptions on the [`SingleTableIVM`], including twin sharing (an
//! identical query served from the shared frame, found through a
//! query-keyed index) and the in-place condition edits the join layer
//! uses for its set-valued `IN` leaves.

use super::index::TableIndex;
use super::window::Window;
use super::{window, FetchKind, SingleTableIVM};
use crate::model::{
    Condition, DataFrameKey, DataFrameOperation, SingleTableReadQuery, SubId, Value,
};

impl SingleTableIVM {
    /// Register a subscription, returning its engine id and whatever of
    /// its initial result set is available at once, as `Add` operations.
    /// Ids are handed out by the engine and never reused; the layer above
    /// maps a client's own ids to them.
    ///
    /// The query's `Where` is normalized to DNF and indexed in the table's
    /// routing index (a `WHERE FALSE` query has no disjuncts and touches no
    /// index at all), so the subscription routes from this moment on. A
    /// query **structurally identical** to one already registered is
    /// served from the shared frame — the twin's current rows, no storage
    /// read, counted in `snapshots_shared` — found through the query-keyed
    /// index in one lookup. A query with no twin records one storage read
    /// ([`super::FetchKind::Snapshot`]) that the runtime runs and lands
    /// later through [`SingleTableIVM::land_fetch`]; until then the
    /// returned operations are empty. Rows already shared with other
    /// subscriptions are tagged rather than duplicated.
    pub fn register_query(&mut self, select_query: SingleTableReadQuery) -> (SubId, Vec<DataFrameOperation>) {
        let sub = SubId(self.next_sub);
        self.next_sub += 1;
        self.stats.queries_registered += 1;

        let dnf = select_query.filter.to_dnf();
        if !dnf.is_empty() {
            self.tables
                .entry(select_query.table.clone())
                .or_default()
                .register(sub, dnf, &mut self.stats);
        }
        let twin = self.identical_subscription(&select_query, sub);
        let storage_query = Self::storage_query(&select_query);
        self.by_query
            .entry(select_query.clone())
            .or_default()
            .insert(sub);
        self.select_queries.insert(sub, select_query);

        let mut ops = Vec::new();
        match twin {
            Some(twin) => {
                self.stats.snapshots_shared += 1;
                let table = self.select_queries[&sub].table.clone();
                for key in self.keys_of(twin) {
                    if let Some(update) = self.share_view(sub, twin, &table, &key) {
                        ops.push(update.op);
                    }
                }
                self.rebuild_window(sub);
                let inherited = self.windows.get(&twin).and_then(Window::frontier);
                if let Some(window) = self.windows.get_mut(&sub) {
                    window.set_frontier(inherited);
                }
            }
            None => {
                self.rebuild_window(sub);
                self.issue(sub, storage_query, FetchKind::Snapshot);
            }
        }
        ops.extend(self.evict_overflow(sub));
        self.sync_boundary(sub);
        (sub, ops)
    }

    /// Edit one condition of a subscription's filter **in place**: the
    /// stored filter's matching leaves are rewritten and the routing index
    /// swaps the condition inside this subscription's disjuncts only
    /// (splitting shared counters correctly) — no DNF re-normalization, no
    /// unregister/re-register churn.
    ///
    /// As with [`SingleTableIVM::replace_query`], the caller owns
    /// reconciling held rows with the new condition (fetch what widened
    /// in, prune what narrowed out), and the subscription is skipped as a
    /// twin donor until [`SingleTableIVM::mark_reconciled`]. Unknown
    /// subscriptions and identical conditions are no-ops.
    pub fn replace_condition(&mut self, sub: SubId, old: &Condition, new: Condition) {
        let Some(query) = self.select_queries.get_mut(&sub) else {
            return;
        };
        if *old == new {
            return;
        }
        let before = query.clone();
        query.filter.replace_condition(old, &new);
        let after = query.clone();
        self.move_query_key(sub, &before, after);
        if let Some(table_index) = self.tables.get_mut(&before.table) {
            table_index.update_condition(sub, old, &new, &mut self.stats);
        }
        self.stale_views.insert(sub);
    }

    /// Add `value` to the set behind `condition`, a set-valued `IN` leaf of
    /// `sub`'s filter, and file the leaf under it in the index: the O(1)
    /// form of a join edge gaining a value. The stored filter needs no
    /// rewrite, since it holds the same set, and the query's identity does
    /// not change. As with [`SingleTableIVM::replace_condition`], the
    /// caller fetches the value's rows and then calls
    /// [`SingleTableIVM::mark_reconciled`]. Reports whether the set
    /// changed; a member already present, an unknown subscription, or a
    /// condition that is not set-valued changes nothing.
    pub fn set_insert(&mut self, sub: SubId, condition: &Condition, value: &Value) -> bool {
        let Value::Set(set) = &condition.value else {
            return false;
        };
        let Some(query) = self.select_queries.get(&sub) else {
            return false;
        };
        if !set.insert(value) {
            return false;
        }
        let table = query.table.clone();
        if let Some(table_index) = self.tables.get_mut(&table) {
            table_index.set_insert(condition, value);
        }
        self.stats.conditions_replaced += 1;
        self.stale_views.insert(sub);
        true
    }

    /// Remove `value` from the set behind `condition` and unfile the leaf
    /// from it: the O(1) form of a join edge losing a value. The caller
    /// prunes the value's held rows and then calls
    /// [`SingleTableIVM::mark_reconciled`]. Reports whether the set changed.
    pub fn set_remove(&mut self, sub: SubId, condition: &Condition, value: &Value) -> bool {
        let Value::Set(set) = &condition.value else {
            return false;
        };
        let Some(query) = self.select_queries.get(&sub) else {
            return false;
        };
        if !set.remove(value) {
            return false;
        }
        let table = query.table.clone();
        if let Some(table_index) = self.tables.get_mut(&table) {
            table_index.set_remove(condition, value);
        }
        self.stats.conditions_replaced += 1;
        self.stale_views.insert(sub);
        true
    }

    /// Count `count` snapshots served by an outer layer from rows this
    /// engine already holds (the join layer serving an identical tree from
    /// its shared parts), so `snapshots_shared` stays the one number for
    /// "registrations that touched no storage".
    pub(super) fn note_shared_snapshots(&mut self, count: u64) {
        self.stats.snapshots_shared += count;
    }

    /// Declare a `replace_query` / `replace_condition` / set edit
    /// reconciliation complete: the caller has finished every fetch and
    /// prune the change required, so the subscription's held rows again
    /// match its filter and it may donate twin snapshots. Only the caller
    /// can know when that point is reached — a widened filter needs a
    /// fetch, a narrowed one a prune, a swapped one both — so nothing
    /// clears the flag implicitly. A no-op for unknown or already
    /// reconciled subscriptions.
    pub fn mark_reconciled(&mut self, sub: SubId) {
        self.stale_views.remove(&sub);
    }

    /// The storage-facing form of a subscription's query: identical except
    /// that a windowed (finite-limit) query is issued with its limit
    /// doubled, to fill the window's buffer.
    fn storage_query(query: &SingleTableReadQuery) -> SingleTableReadQuery {
        SingleTableReadQuery {
            limit: window::storage_limit(query),
            ..query.clone()
        }
    }

    /// Remove a subscription: its routing-index entries and its tag on
    /// every shared row it holds — walked off its held index, not by
    /// scanning the table — dropping rows nobody holds anymore. A table
    /// index that routes nothing afterwards is dropped too; reads not yet
    /// taken by the runtime are withdrawn and reads already out land as
    /// no-ops. Unknown subscriptions are a no-op.
    pub fn unregister_query(&mut self, sub: SubId) {
        let Some(query) = self.select_queries.remove(&sub) else {
            return;
        };
        self.pending.remove(&sub);
        self.requests.retain(|fetch| fetch.sub != sub);
        if let Some(twins) = self.by_query.get_mut(&query) {
            twins.remove(&sub);
            if twins.is_empty() {
                self.by_query.remove(&query);
            }
        }
        if let Some(table_index) = self.tables.get_mut(&query.table) {
            table_index.unregister(sub);
        }
        if self
            .tables
            .get(&query.table)
            .is_some_and(TableIndex::is_empty)
        {
            self.tables.remove(&query.table);
        }
        self.stale_views.remove(&sub);
        self.windows.remove(&sub);
        let ids = self.held.remove(&sub).unwrap_or_default();
        if let Some(frame) = self.frames.get_mut(&query.table) {
            for id in ids {
                if let Some(row) = frame.row_mut(id) {
                    row.subscribers.remove(&sub);
                }
                frame.drop_if_unheld(id);
            }
        }
        if self
            .frames
            .get(&query.table)
            .is_some_and(|frame| frame.is_empty())
        {
            self.frames.remove(&query.table);
        }
    }

    /// Swap the registered query of `sub` for `select_query`, reindexing
    /// its routing while leaving its frame tags untouched.
    ///
    /// The caller owns keeping held rows consistent with the new filter —
    /// this is the general reseat seam (for a single value change of a
    /// set-valued leaf, [`SingleTableIVM::set_insert`] is the cheap edit).
    /// Until the caller declares that reconciliation complete
    /// ([`SingleTableIVM::mark_reconciled`], after however many fetches
    /// and prunes the change needs — a swap needs both), the subscription
    /// is skipped as a twin donor: its filter is ahead of its held rows,
    /// and a registration served from it would inherit the gap
    /// permanently. The table must stay the same (a cross-table swap is
    /// refused); unknown subscriptions and identical queries are no-ops.
    pub fn replace_query(&mut self, sub: SubId, select_query: SingleTableReadQuery) {
        let Some(existing) = self.select_queries.get_mut(&sub) else {
            return;
        };
        if *existing == select_query || existing.table != select_query.table {
            return;
        }
        let table = select_query.table.clone();
        let before = existing.clone();
        *existing = select_query.clone();
        self.move_query_key(sub, &before, select_query.clone());
        self.stale_views.insert(sub);
        if let Some(table_index) = self.tables.get_mut(&table) {
            table_index.unregister(sub);
        }
        let dnf = select_query.filter.to_dnf();
        if !dnf.is_empty() {
            self.tables
                .entry(table.clone())
                .or_default()
                .register(sub, dnf, &mut self.stats);
        }
        if self
            .tables
            .get(&table)
            .is_some_and(TableIndex::is_empty)
        {
            self.tables.remove(&table);
        }
        self.rebuild_window(sub);
        self.sync_boundary(sub);
    }

    /// Re-key `sub` in the query-keyed index after its stored query
    /// changed shape (a swap or a literal in-place edit).
    fn move_query_key(&mut self, sub: SubId, before: &SingleTableReadQuery, after: SingleTableReadQuery) {
        if let Some(twins) = self.by_query.get_mut(before) {
            twins.remove(&sub);
            if twins.is_empty() {
                self.by_query.remove(before);
            }
        }
        self.by_query.entry(after).or_default().insert(sub);
    }

    /// Another registered subscription with a structurally identical
    /// query, if any — the sharing seam of registration, one lookup in the
    /// query-keyed index. Skips subscriptions in a maintenance window or
    /// with a storage read still out, whose rows lag their filter.
    fn identical_subscription(&self, select_query: &SingleTableReadQuery, exclude: SubId) -> Option<SubId> {
        self.by_query
            .get(select_query)?
            .iter()
            .find(|twin| {
                **twin != exclude && !self.stale_views.contains(twin) && !self.is_pending(**twin)
            })
            .copied()
    }

    /// The keys a subscription currently holds, from its held index — what
    /// a twin registration is served, view by view, instead of a storage
    /// result.
    fn keys_of(&self, sub: SubId) -> Vec<DataFrameKey> {
        let Some(query) = self.select_queries.get(&sub) else {
            return Vec::new();
        };
        let Some(frame) = self.frames.get(&query.table) else {
            return Vec::new();
        };
        let Some(ids) = self.held.get(&sub) else {
            return Vec::new();
        };
        ids.iter()
            .filter_map(|id| frame.row(*id))
            .map(|row| row.key.clone())
            .collect()
    }
}
