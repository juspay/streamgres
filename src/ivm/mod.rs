//! Incremental View Maintenance (IVM) — the core of the engine.
//!
//! Clients subscribe with [`ReadQuery`]s; every incoming [`WriteQuery`] is
//! routed to the subscriptions it affects, and each affected subscription
//! receives the minimal [`DataFrameOperation`]s that bring its result set up
//! to date. The engine applies the same operations to its own materialized
//! [`DataFrame`]s, so engine and client views stay in lockstep.
//!
//! # How a write is routed
//!
//! A write can affect a subscription in exactly two ways, and the engine
//! checks for both:
//!
//! 1. **The new row matches the query** (insert, or update moving a row in /
//!    changing it in place) — decided by *DNF counting*. At registration a
//!    query's `Where` is normalized to disjunctive normal form
//!    ([`crate::model::Where::to_dnf`]): an OR of **disjuncts**, each an AND
//!    of leaf conditions. Each distinct condition on the write's table is
//!    evaluated against the row image exactly once; every condition that
//!    matches bumps the shared counter of each disjunct containing it, and
//!    a counter reaching its size **fires** every subscription whose filter
//!    contains that disjunct — identical disjunct shapes share one counter
//!    across subscriptions. Firing is exact (there is no verification pass)
//!    and the bookkeeping after evaluation is proportional to the
//!    *matching* links only, not to the number of registered subscriptions.
//!    A disjunct with no conditions (no `WHERE` at all, or shapes like
//!    `x OR TRUE`) is invisible to the condition index and fires on every
//!    same-table row write. The machinery lives in one `TableIndex` per
//!    table (see the `index` module).
//! 2. **The query currently holds the row** (delete, or update moving a row
//!    out) — found by checking each same-table query's materialized frame
//!    for the row's key. A delete carries no column values, so predicate
//!    matching cannot find these.
//!
//! From `matches_after` (1) and `present_before` (2) the operation follows:
//!
//! | `matches_after` | `present_before` | emitted operation      |
//! |-----------------|------------------|------------------------|
//! | yes             | no               | `Add` (row enters)     |
//! | yes             | yes              | `Add` (row refreshed)  |
//! | no              | yes              | `Delete` (row leaves)  |
//! | no              | no               | not impacted           |
//!
//! # Scope notes
//!
//! - `order_by` and `limit` are **not yet enforced** — every matching row is
//!   kept. Maintaining a `LIMIT` window incrementally needs an ordered
//!   per-subscription structure plus storage access for refills; this lands
//!   with the Diesel/Postgres connector on the roadmap.
//! - Frames fill only from writes seen after registration; `register_query`
//!   returns the subscription's starting frame (empty until the storage
//!   connector can serve real initial result sets).
//! - DNF can blow up exponentially for adversarial filters; a size cap with
//!   a tree-evaluation fallback is deliberately deferred (see
//!   [`crate::model::Where::to_dnf`]).
//! - The engine is **single-threaded by design** for now, enforced at
//!   compile time (the index's shared counter handles are not `Send`).
//!   Multithreading is a later, deliberate step — see the `index` module
//!   header.

mod index;
mod predicate;
mod stats;

pub use predicate::{eval_condition, evaluate};
pub use stats::IvmStats;

use std::collections::{BTreeSet, HashMap};

use crate::model::{
    DataFrame, DataFrameKey, DataFrameOperation, DataFrameRow, ReadQuery, TableName, WriteQuery,
};
use index::TableIndex;

/// The client-facing handle of one registered subscription.
///
/// A dedicated type rather than a bare `String`, so a subscription id can
/// never be confused with the other strings routing code passes around
/// (table names, column names). Constructed from any string-ish value;
/// compares, orders, and hashes exactly like the underlying id, and maps
/// keyed by `QueryId` accept a plain `&str` for lookups.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryId(String);

impl QueryId {
    /// The id as a borrowed string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for QueryId {
    /// Wraps a borrowed id.
    fn from(id: &str) -> Self {
        QueryId(id.to_owned())
    }
}

impl From<String> for QueryId {
    /// Wraps an owned id.
    fn from(id: String) -> Self {
        QueryId(id)
    }
}

impl std::borrow::Borrow<str> for QueryId {
    /// Lets maps keyed by [`QueryId`] be queried with a plain `&str`.
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for QueryId {
    /// Renders as the bare id, honoring width/alignment format flags.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(&self.0)
    }
}

/// The engine. One instance maintains all subscriptions over one logical
/// database (single-table queries only for now; joins are future work).
///
/// - `select_queries`: subscription handle → its query; the [`QueryId`] is
///   what the WebSocket layer will use to address a client's subscription.
/// - `forward_index`: materialized result set per subscription.
/// - `tables`: one routing index per table — the condition →
///   shared-disjunct-counter machinery (see the `index` module).
/// - `write_epoch`: monotonic write number; disjunct counters are lazily
///   invalidated by comparing against it, so no per-write reset sweep is
///   needed.
/// - `stats`: operation counters; not part of the sync state.
pub struct IVM {
    select_queries: HashMap<QueryId, ReadQuery>,
    forward_index: HashMap<QueryId, DataFrame>,
    tables: HashMap<TableName, TableIndex>,
    write_epoch: u64,
    stats: IvmStats,
}

/// One confirmed impact of a write on a subscription, with the two facts
/// that determine which operation to emit.
///
/// - `matches_after`: does the post-write row image satisfy the query's
///   `Where`? Always `false` for deletes.
/// - `present_before`: did the query's materialized frame hold the row
///   before this write?
struct Impact {
    uuid: QueryId,
    matches_after: bool,
    present_before: bool,
}

impl IVM {
    /// Create an empty engine with no subscriptions and zeroed stats.
    pub fn new() -> Self {
        IVM {
            select_queries: HashMap::new(),
            forward_index: HashMap::new(),
            tables: HashMap::new(),
            write_epoch: 0,
            stats: IvmStats::default(),
        }
    }

    /// Register a client subscription under `query_uuid`, returning the
    /// subscription's starting result set.
    ///
    /// Normalizes the query's `Where` to DNF and indexes it in the table's
    /// routing index (a `WHERE FALSE` query has no disjuncts and touches no
    /// index at all). The returned frame is the snapshot to hand the client
    /// before streaming operations — empty until initial result sets arrive
    /// with the storage connector.
    ///
    /// Re-registering a uuid with the identical query is a no-op — the index
    /// already routes to it, and re-adding it would double-count its
    /// conditions. Re-registering with a changed query replaces the
    /// subscription (old index entries removed, frame reset). Stale entries
    /// are never left behind: counting fires on exact counts, so a leftover
    /// link would corrupt the replacement's counters.
    pub fn register_query(
        &mut self,
        query_uuid: impl Into<QueryId>,
        select_query: ReadQuery,
    ) -> &DataFrame {
        let query_uuid = query_uuid.into();
        self.stats.queries_registered += 1;

        if self.select_queries.get(&query_uuid) == Some(&select_query) {
            return self.forward_index.entry(query_uuid).or_default();
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
        self.select_queries
            .insert(query_uuid.clone(), select_query);
        self.forward_index.entry(query_uuid).or_default()
    }

    /// Remove a subscription: its routing-index entries and its materialized
    /// frame. A table index that routes nothing afterwards is dropped.
    /// Unknown uuids are a no-op.
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
        self.forward_index.remove(query_uuid);
    }

    /// Which registered subscriptions does this write affect?
    ///
    /// Returns subscription ids in deterministic (sorted) order. Records
    /// routing counters in [`IVM::stats`].
    pub fn search_impacted_queries(&mut self, write_query: &WriteQuery) -> Vec<QueryId> {
        self.analyze(write_query)
            .into_iter()
            .map(|impact| impact.uuid)
            .collect()
    }

    /// Route a write: find the impacted subscriptions, emit one
    /// [`DataFrameOperation`] per impacted subscription, and apply those
    /// operations to the engine's own materialized frames.
    ///
    /// The returned `(subscription id, operation)` pairs are exactly what
    /// the WebSocket layer will push to clients. `analyze` only reports
    /// impacts where `matches_after` or `present_before` holds, so every
    /// impact maps to exactly one operation.
    pub fn incremental_update(
        &mut self,
        write_query: &WriteQuery,
    ) -> Vec<(QueryId, DataFrameOperation)> {
        self.stats.writes_processed += 1;

        let impacts = self.analyze(write_query);
        let key = DataFrameKey::new(write_query.pkey_value().clone());
        let row_image = write_query.new_row_image();

        let mut ops: Vec<(QueryId, DataFrameOperation)> = Vec::new();
        for impact in impacts {
            let op = if impact.matches_after {
                let data = row_image
                    .clone()
                    .expect("matches_after is only true when the write has a row image");
                self.stats.ops_add += 1;
                DataFrameOperation::Add(key.clone(), DataFrameRow { data })
            } else if impact.present_before {
                self.stats.ops_delete += 1;
                DataFrameOperation::Delete(key.clone())
            } else {
                continue;
            };
            ops.push((impact.uuid, op));
        }

        for (uuid, op) in &ops {
            if let Some(frame) = self.forward_index.get_mut(uuid) {
                frame.apply(op);
            }
        }

        ops
    }

    /// The materialized result set currently held for a subscription.
    pub fn dataframe_for(&self, query_uuid: &str) -> Option<&DataFrame> {
        self.forward_index.get(query_uuid)
    }

    /// Number of registered subscriptions.
    pub fn query_count(&self) -> usize {
        self.select_queries.len()
    }

    /// The engine's operation counters, accumulated since creation or the
    /// last [`IVM::reset_stats`].
    pub fn stats(&self) -> &IvmStats {
        &self.stats
    }

    /// Reset all operation counters to zero; sync state is untouched.
    pub fn reset_stats(&mut self) {
        self.stats = IvmStats::default();
    }

    /// The single source of truth for "is this subscription impacted, and
    /// how" — both public routing entry points build on it.
    ///
    /// Way 1 (row matches after the write) delegates to the table's routing
    /// index under a freshly bumped write epoch; deletes carry no row image
    /// and skip it entirely. Way 2 (row held before the write) scans each
    /// same-table subscription's frame for the row's key — catches updates
    /// moving a row out, and deletes. Every returned [`Impact`] has at
    /// least one of the two facts set.
    fn analyze(&mut self, write_query: &WriteQuery) -> Vec<Impact> {
        let table_name = write_query.table().clone();
        let key = DataFrameKey::new(write_query.pkey_value().clone());
        let row_image = write_query.new_row_image();

        let mut matched: BTreeSet<QueryId> = BTreeSet::new();
        if let Some(row) = &row_image {
            self.write_epoch += 1;
            if let Some(table_index) = self.tables.get(&table_name) {
                matched = table_index.matched(row, self.write_epoch, &mut self.stats);
            }
        }

        let mut holding: BTreeSet<QueryId> = BTreeSet::new();
        for (uuid, query) in &self.select_queries {
            if query.table != table_name {
                continue;
            }
            self.stats.membership_probes += 1;
            if self
                .forward_index
                .get(uuid)
                .is_some_and(|frame| frame.contains(&key))
            {
                self.stats.membership_hits += 1;
                holding.insert(uuid.clone());
            }
        }

        let mut impacts = Vec::new();
        for uuid in matched.union(&holding) {
            self.stats.queries_impacted += 1;
            impacts.push(Impact {
                uuid: uuid.clone(),
                matches_after: matched.contains(uuid),
                present_before: holding.contains(uuid),
            });
        }
        impacts
    }
}

impl Default for IVM {
    /// Same as [`IVM::new`].
    fn default() -> Self {
        Self::new()
    }
}
