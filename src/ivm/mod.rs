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
//!    changing it in place). Found via the *reverse index*: every registered
//!    leaf [`Condition`] is probed against the new row image; queries whose
//!    condition matched become candidates, and each candidate's full `Where`
//!    tree is then verified. Queries whose filter needs no matching leaf to
//!    be true (full-table subscriptions, `x OR TRUE`) are invisible to the
//!    index and are added as candidates directly.
//! 2. **The query currently holds the row** (delete, or update moving a row
//!    out). Found by checking each same-table query's materialized frame for
//!    the row's key — a delete carries no column values, so predicate
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
//! # v1 scope
//!
//! - `order_by` and `limit` are **not yet enforced** — every matching row is
//!   kept. Maintaining a `LIMIT` window incrementally needs storage access
//!   (when a row leaves the window, the next row must be fetched); this
//!   lands with the Diesel/Postgres connector on the roadmap.
//! - Frames fill only from writes seen after registration; `register_query`
//!   returns the subscription's starting frame (empty until the storage
//!   connector can serve real initial result sets).

mod predicate;
mod stats;

pub use predicate::{eval_condition, evaluate};
pub use stats::IvmStats;

use std::collections::{BTreeSet, HashMap};

use crate::model::{
    Condition, DataFrame, DataFrameKey, DataFrameOperation, DataFrameRow, ReadQuery, WriteQuery,
};

/// The engine. One instance maintains all subscriptions over one logical
/// database (single-table queries only in v1; joins are future work).
pub struct IVM {
    /// Subscription handle → its query. The uuid is what the WebSocket layer
    /// will use to address a client's subscription.
    select_queries: HashMap<String, ReadQuery>,
    /// Materialized result set per subscription, keyed by its uuid.
    forward_index: HashMap<String, DataFrame>,
    /// Leaf condition → every subscription whose filter contains it. Probing
    /// these against a write's row image narrows the write down to candidate
    /// queries without touching every subscription's full predicate.
    reverse_index: HashMap<Condition, Vec<String>>,
    /// Operation counters; not part of the sync state.
    stats: IvmStats,
}

/// One confirmed impact of a write on a subscription, with the two facts
/// that determine which operation to emit.
struct Impact {
    uuid: String,
    /// Does the post-write row image satisfy the query's `Where`?
    /// Always `false` for deletes.
    matches_after: bool,
    /// Did the query's materialized frame hold the row before this write?
    present_before: bool,
}

impl IVM {
    pub fn new() -> Self {
        IVM {
            select_queries: HashMap::new(),
            forward_index: HashMap::new(),
            reverse_index: HashMap::new(),
            stats: IvmStats::default(),
        }
    }

    /// Register a client subscription under `query_uuid`, returning the
    /// subscription's starting result set.
    ///
    /// Indexes every leaf condition of the query's `Where` tree and creates
    /// the subscription's own materialized frame. The returned frame is the
    /// snapshot to hand the client before streaming operations — empty until
    /// initial result sets arrive with the storage connector.
    ///
    /// Re-registering a uuid replaces its subscription; the frame is reset
    /// unless the query is unchanged. Conditions of a replaced query stay
    /// indexed until an unregister API exists — harmless, since candidates
    /// are always verified against the current query.
    pub fn register_query(&mut self, query_uuid: String, select_query: ReadQuery) -> &DataFrame {
        self.stats.queries_registered += 1;

        for condition in select_query.filter.leaf_conditions() {
            let subscribers = self.reverse_index.entry(condition.clone()).or_default();
            if !subscribers.contains(&query_uuid) {
                subscribers.push(query_uuid.clone());
                self.stats.conditions_indexed += 1;
            }
        }

        let replaced = self.select_queries.insert(query_uuid.clone(), select_query);
        let query_changed =
            replaced.is_some_and(|previous| previous != self.select_queries[&query_uuid]);
        let frame = self.forward_index.entry(query_uuid).or_default();
        if query_changed {
            frame.records.clear();
        }
        frame
    }

    /// Which registered subscriptions does this write affect?
    ///
    /// Returns subscription uuids in deterministic (sorted) order. Records
    /// routing counters in [`IVM::stats`].
    pub fn search_impacted_queries(&mut self, write_query: &WriteQuery) -> Vec<String> {
        self.analyze(write_query)
            .into_iter()
            .map(|impact| impact.uuid)
            .collect()
    }

    /// Route a write: find the impacted subscriptions, emit one
    /// [`DataFrameOperation`] per impacted subscription, and apply those
    /// operations to the engine's own materialized frames.
    ///
    /// The returned `(subscription uuid, operation)` pairs are exactly what
    /// the WebSocket layer will push to clients.
    pub fn incremental_update(
        &mut self,
        write_query: &WriteQuery,
    ) -> Vec<(String, DataFrameOperation)> {
        self.stats.writes_processed += 1;

        let impacts = self.analyze(write_query);
        let key = DataFrameKey::new(write_query.pkey_value().clone());
        let row_image = write_query.new_row_image();

        let mut ops: Vec<(String, DataFrameOperation)> = Vec::new();
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
                // analyze() only reports impacts where one of the two holds.
                continue;
            };
            ops.push((impact.uuid, op));
        }

        // Maintain our own view of each impacted subscription.
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

    pub fn stats(&self) -> &IvmStats {
        &self.stats
    }

    pub fn reset_stats(&mut self) {
        self.stats = IvmStats::default();
    }

    /// Candidate generation + verification. The single source of truth for
    /// "is this subscription impacted, and how" — both public routing entry
    /// points build on it.
    fn analyze(&mut self, write_query: &WriteQuery) -> Vec<Impact> {
        let table_name = write_query.table().to_owned();
        let key = DataFrameKey::new(write_query.pkey_value().clone());
        let row_image = write_query.new_row_image();

        // BTreeSets keep uuid handling deterministic across runs.
        let mut candidates: BTreeSet<String> = BTreeSet::new();
        let mut holding_row: BTreeSet<String> = BTreeSet::new();

        // Way 1: the new row image may match a query's predicate.
        if let Some(row) = &row_image {
            for (condition, subscribers) in &self.reverse_index {
                self.stats.index_probes += 1;
                if eval_condition(condition, row, &mut self.stats.conditions_evaluated) {
                    self.stats.index_hits += 1;
                    candidates.extend(subscribers.iter().cloned());
                }
            }
            // Queries whose filter can be true with zero matching leaf
            // conditions (no filter at all, or shapes like `x OR TRUE`) are
            // invisible to the reverse index; they are candidates for every
            // same-table write that carries a row. Together with the index
            // this makes candidate generation complete — see
            // `Where::vacuously_satisfiable`.
            for (uuid, query) in &self.select_queries {
                if query.table == table_name && query.filter.vacuously_satisfiable() {
                    candidates.insert(uuid.clone());
                }
            }
        }

        // Way 2: a query may currently hold the row (update-out / delete).
        for (uuid, query) in &self.select_queries {
            if query.table != table_name {
                continue;
            }
            self.stats.membership_probes += 1;
            let holds = self
                .forward_index
                .get(uuid)
                .is_some_and(|frame| frame.contains(&key));
            if holds {
                self.stats.membership_hits += 1;
                holding_row.insert(uuid.clone());
                candidates.insert(uuid.clone());
            }
        }

        // Verification: index candidates are only *hints* (one matching leaf
        // condition, possibly even from a same-named column on another
        // table), so confirm the table and the full predicate tree.
        let mut impacts = Vec::new();
        for uuid in candidates {
            let query = &self.select_queries[&uuid];
            if query.table != table_name {
                continue;
            }
            let matches_after = match &row_image {
                Some(row) => {
                    self.stats.full_evaluations += 1;
                    evaluate(&query.filter, row, &mut self.stats.conditions_evaluated)
                }
                None => false,
            };
            let present_before = holding_row.contains(&uuid);
            if matches_after || present_before {
                self.stats.queries_impacted += 1;
                impacts.push(Impact {
                    uuid,
                    matches_after,
                    present_before,
                });
            }
        }
        impacts
    }
}

impl Default for IVM {
    fn default() -> Self {
        Self::new()
    }
}
