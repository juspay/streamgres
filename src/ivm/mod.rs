//! Incremental View Maintenance (IVM) — the core of the engine.
//!
//! Clients subscribe with [`SingleTableReadQuery`]s and get back an engine
//! id ([`SubId`]); every incoming [`WriteQuery`] is routed to the
//! subscriptions it affects, and each affected subscription receives the
//! minimal [`DataFrameOperation`]s that bring its result set up to date,
//! grouped per client ([`ClientUpdate`]).
//!
//! # Shared frames
//!
//! The engine keeps **one materialized frame per table**, not per
//! subscription: each row carries the set of subscriptions currently
//! holding it. A row wanted by many queries is stored once; a new
//! subscriber to data already present just tags itself onto the rows. A
//! subscription's own view (what its client holds) is exactly the rows
//! tagged with its id — mirrored in a per-subscription **held-key index**
//! (subscription → row keys), so a view is enumerated in time proportional
//! to its own size, never by scanning the table. Every tag change reaches
//! that client as an `Add` or `Delete` — engine tags and client frames
//! stay in lockstep.
//!
//! Registration exploits the sharing: a query **structurally identical**
//! to one already registered is served straight from the shared frame —
//! the twin's current rows are tagged for the new subscription and
//! returned as its snapshot, with no storage query. A subscription midway
//! through a [`SingleTableIVM::replace_query`] maintenance window (filter
//! swapped, rows not yet reconciled) is skipped as a donor.
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
//!    table (see the `index` module). A single condition whose value list
//!    changes — the join layer's `IN` gaining or losing a value — is
//!    edited **in place**, in the stored filter and inside this
//!    subscription's indexed disjuncts alone
//!    ([`SingleTableIVM::replace_condition`]; shared counters split
//!    correctly), with no DNF re-normalization. Windowed candidates not
//!    already holding the row are additionally checked against the
//!    window's admission boundary (see the `window` module) — admission
//!    only, never residence: a held row worsening in place keeps its slot
//!    until a better arrival evicts it.
//! 2. **The query currently holds the row** (delete, or update moving a row
//!    out) — found by checking whether the shared row is tagged with the
//!    subscription. A delete carries no column values, so predicate
//!    matching cannot find these.
//!
//! From `matches_after` (1) and `present_before` (2) the operation follows:
//!
//! | `matches_after` | `present_before` | emitted operation(s)                           |
//! |-----------------|------------------|------------------------------------------------|
//! | yes             | no               | `Add` (row enters)                             |
//! | yes             | yes              | `Delete(old)` + `Add(new)` (replaced in place) |
//! | no              | yes              | `Delete(old)` (row leaves)                     |
//! | no              | no               | not impacted                                   |
//!
//! Every emitted operation is self-contained: a `Delete` carries the row
//! image it removed, and an in-place replacement is the adjacent
//! `Delete(old)` + `Add(new)` pair inside the engine (the join layer diffs
//! it); per client the pair folds into the one `Add` (see the `update`
//! module).
//!
//! # Scope notes
//!
//! - A finite `limit` is enforced through a per-subscription **window**
//!   (see the `window` module): the engine buffers twice the requested
//!   limit (storage reads are issued with the doubled limit), admits new
//!   rows past a full buffer only when they beat the worst held value,
//!   evicts past capacity, and asks for a refill from storage when a
//!   removal drains the buffer to the requested limit. `order_by` decides
//!   *which* rows the window keeps — it does not order the operation
//!   stream (clients sort their own frames). A query without a finite
//!   limit keeps every matching row; a `LIMIT 0` subscription is
//!   permanently empty (its registration reads nothing and later writes
//!   never admit).
//! - The engine **never reads storage itself**. Frames fill three ways:
//!   the initial result set of a registration, writes seen afterwards,
//!   and the narrowed reads the join layer asks for when a join value
//!   becomes referenced ([`SingleTableIVM::fetch`]). Each read is
//!   recorded as a [`Fetch`] request (see the `engine` module) and landed
//!   later by the runtime through [`Engine::land`]; between the two the
//!   subscription's routing is live, its window publishes no admission
//!   boundary (so nothing the read will miss is turned away), and it is
//!   skipped as a twin donor. **Positions never enter the engine**: the
//!   runtime brings every read up to the point the engine has reached
//!   before landing it (a delivered write past the read's snapshot is
//!   applied to the result first), so a landed row is current, and a row
//!   the frame already holds is simply tagged. The whole engine sits at
//!   one position at every moment.
//! - DNF can blow up exponentially for adversarial filters; a size cap with
//!   a tree-evaluation fallback is deliberately deferred (see
//!   [`crate::model::Where::to_dnf`]).
//! - The engine is **single-threaded by design** for now, enforced at
//!   compile time (the index's shared counter handles are not `Send`).
//!   Multithreading is a later, deliberate step — see the `index` module
//!   header.

mod columns;
mod engine;
mod frames;
mod index;
mod multi;
mod predicate;
mod registry;
mod stats;
mod update;
mod window;

pub use crate::model::{ClientId, SubId};
pub use engine::{Engine, Fetch, FetchId, FetchKind};
pub use multi::{MultiTableIVM, MultiTableUpdate};
pub use predicate::{eval_condition, evaluate};
pub use stats::IvmStats;
pub use update::{ClientUpdate, QueryPart, Target};
pub use window::{order_cmp, order_rows};

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::model::frame::{RowId, TableFrame};
use crate::model::{
    DataFrameKey, DataFrameOperation, DataFrameRow, SingleTableReadQuery, TableName, WriteQuery,
};
use index::TableIndex;
use update::{Raw, group};
use window::Window;

/// One operation for one single-table subscription — what
/// [`SingleTableIVM::incremental_update`] emits; the join layer consumes
/// these for its parts, and the [`Engine`] impl groups them per client.
///
/// - `query`: which subscription (the engine id `register_query` returned).
/// - `table`: the table the operation lands on — the written table, which
///   is also the subscription's table.
/// - `op`: the delta itself.
#[derive(Debug, Clone, PartialEq)]
pub struct SingleTableUpdate {
    pub query: SubId,
    pub table: TableName,
    pub op: DataFrameOperation,
}

/// The engine. One instance maintains all subscriptions over one logical
/// database, one subscription per single-table query; multi-table joins
/// are layered on top by [`MultiTableIVM`].
///
/// - `select_queries`: subscription id → its query. Ids are `u64`s the
///   engine hands out at registration; the transport maps client ids to
///   them.
/// - `by_query`: query → the subscriptions registered with exactly that
///   query, the one-lookup twin index; re-keyed whenever a stored query
///   changes shape.
/// - `frames`: ONE shared frame per table, rows tagged with their
///   subscribers' ids (see the module header).
/// - `held`: subscription → the row ids of the shared rows it currently
///   holds — the forward mirror of the rows' subscriber tags, kept in
///   lockstep with them, so a subscription's view is enumerable without
///   scanning its table's frame. A (subscription, row) pair is two small
///   integers, one in each direction.
/// - `windows`: subscription → its ORDER BY / LIMIT window (finite-limit
///   queries only): the order-value → row-key map whose worst entry is
///   the admission boundary, published into the table's routing index
///   whenever it moves (see the `window` module).
/// - `stale_views`: subscriptions midway through a `replace_query` /
///   `replace_condition` maintenance window — filter already swapped,
///   held rows not yet reconciled — excluded as twin donors until the
///   caller declares the reconciliation complete
///   ([`SingleTableIVM::mark_reconciled`]).
/// - `tables`: one routing index per table — the condition →
///   shared-disjunct-counter machinery (see the `index` module).
/// - `pending`: subscription → how many of its storage reads are still
///   out; while nonzero the subscription publishes no admission boundary,
///   asks for no refill, and donates no twin snapshot.
/// - `requests`: the reads asked for since the runtime last took them.
/// - `clients`: subscription id → its client, and `by_client` the
///   reverse, kept only for subscriptions registered through the
///   [`Engine`] seam (the join layer's inner parts have none).
/// - `write_epoch`: monotonic write number; disjunct counters are lazily
///   invalidated by comparing against it, so no per-write reset sweep is
///   needed.
/// - `next_sub`: the next subscription id to hand out; never reused.
/// - `next_fetch`: the next read id to hand out; never reused.
/// - `stats`: operation counters; not part of the sync state.
pub struct SingleTableIVM {
    select_queries: HashMap<SubId, SingleTableReadQuery>,
    by_query: HashMap<SingleTableReadQuery, BTreeSet<SubId>>,
    frames: HashMap<TableName, TableFrame>,
    held: HashMap<SubId, HashSet<RowId>>,
    windows: HashMap<SubId, Window>,
    stale_views: HashSet<SubId>,
    tables: HashMap<TableName, TableIndex>,
    pending: HashMap<SubId, u32>,
    requests: Vec<Fetch>,
    clients: HashMap<SubId, ClientId>,
    by_client: HashMap<ClientId, BTreeSet<SubId>>,
    write_epoch: u64,
    next_sub: u64,
    next_fetch: u64,
    stats: IvmStats,
}

/// One confirmed impact of a write on a subscription, with the two facts
/// that determine which operation to emit.
///
/// - `matches_after`: does the post-write row image satisfy the query's
///   `Where`? Always `false` for deletes.
/// - `present_before`: was the shared row tagged with this subscription
///   before the write?
struct Impact {
    sub: SubId,
    matches_after: bool,
    present_before: bool,
}

impl Default for SingleTableIVM {
    /// [`SingleTableIVM::new`].
    fn default() -> Self {
        Self::new()
    }
}

impl SingleTableIVM {
    /// An empty engine.
    pub fn new() -> Self {
        SingleTableIVM {
            select_queries: HashMap::new(),
            by_query: HashMap::new(),
            frames: HashMap::new(),
            held: HashMap::new(),
            windows: HashMap::new(),
            stale_views: HashSet::new(),
            tables: HashMap::new(),
            pending: HashMap::new(),
            requests: Vec::new(),
            clients: HashMap::new(),
            by_client: HashMap::new(),
            write_epoch: 0,
            next_sub: 0,
            next_fetch: 0,
            stats: IvmStats::default(),
        }
    }

    /// Take the storage reads recorded since the last call, oldest first.
    pub fn take_requests(&mut self) -> Vec<Fetch> {
        std::mem::take(&mut self.requests)
    }

    /// Whether a storage read for `sub` is still out.
    pub fn is_pending(&self, sub: SubId) -> bool {
        self.pending.get(&sub).is_some_and(|count| *count > 0)
    }

    /// Which registered subscriptions does this write affect?
    ///
    /// Returns subscription ids in deterministic (sorted) order. Records
    /// routing counters in [`SingleTableIVM::stats`].
    pub fn search_impacted_queries(&mut self, write_query: &WriteQuery) -> Vec<SubId> {
        self.analyze(
            write_query.table(),
            write_query.pkey_value(),
            write_query.new_row_image(),
        )
        .into_iter()
        .map(|impact| impact.sub)
        .collect()
    }

    /// Route a write: find the impacted subscriptions, emit one
    /// [`SingleTableUpdate`] per self-contained operation, and bring the
    /// shared frame's data and tags in line.
    ///
    /// Per impacted subscription: a holder of the row's pre-image gets
    /// `Delete(key, old)`, a subscription the new image fires gets
    /// `Add(key, new)`, and one in both camps gets the adjacent pair — the
    /// in-place replacement row of the module header's table. The shared
    /// row's data is written once; each firing subscription tags itself,
    /// each no-longer-matching holder untags itself, and the row is
    /// dropped when its last tag goes.
    pub fn incremental_update(&mut self, write_query: &WriteQuery) -> Vec<SingleTableUpdate> {
        self.stats.writes_processed += 1;
        let table = write_query.table().clone();
        let key = write_query.pkey_value().clone();
        let row_image = write_query.new_row_image();
        let impacts = self.analyze(&table, &key, row_image);
        let old_data = self
            .frames
            .get(&table)
            .and_then(|frame| frame.get(&key))
            .map(|row| row.data.clone());

        let mut ops: Vec<SingleTableUpdate> = Vec::new();
        for impact in &impacts {
            if impact.present_before {
                let data = old_data
                    .clone()
                    .expect("present_before is only true when the shared row is materialized");
                self.stats.ops_delete += 1;
                ops.push(SingleTableUpdate {
                    query: impact.sub,
                    table: table.clone(),
                    op: DataFrameOperation::Delete(key.clone(), data),
                });
            }
            if impact.matches_after {
                let row = row_image
                    .expect("matches_after is only true when the write has a row image")
                    .clone();
                self.stats.ops_add += 1;
                ops.push(SingleTableUpdate {
                    query: impact.sub,
                    table: table.clone(),
                    op: DataFrameOperation::Add(key.clone(), row),
                });
            }
        }

        if !impacts.is_empty() {
            let frame = self.frames.entry(table).or_default();
            for impact in &impacts {
                if impact.matches_after {
                    let data = row_image.expect("checked above").clone();
                    let (id, row) = frame.entry(&key, || data.clone());
                    row.data = data;
                    row.subscribers.insert(impact.sub);
                    self.held.entry(impact.sub).or_default().insert(id);
                } else if let Some(id) = frame.id_of(&key) {
                    if let Some(row) = frame.row_mut(id) {
                        row.subscribers.remove(&impact.sub);
                    }
                    if let Some(ids) = self.held.get_mut(&impact.sub) {
                        ids.remove(&id);
                    }
                }
            }
            if let Some(id) = frame.id_of(&key) {
                frame.drop_if_unheld(id);
            }
        }

        let mut window_ops = Vec::new();
        for impact in &impacts {
            window_ops.extend(self.maintain_window(
                impact.sub,
                &key,
                row_image,
                impact.matches_after,
                impact.present_before,
            ));
        }
        ops.append(&mut window_ops);

        let impacted: Vec<SubId> = impacts.iter().map(|impact| impact.sub).collect();
        self.gate_updates(&impacted, ops)
    }

    /// Tag `sub` onto the frame row `key` of `table` (which must be
    /// materialized), emitting its `Add` with `image` (the frame's image);
    /// nothing when it already holds the row. The caller maintains the
    /// window.
    pub(super) fn tag_row(
        &mut self,
        sub: SubId,
        table: &TableName,
        key: &DataFrameKey,
        image: &DataFrameRow,
    ) -> Option<SingleTableUpdate> {
        let frame = self.frames.get_mut(table)?;
        let id = frame.id_of(key)?;
        let row = frame.row_mut(id)?;
        if !row.subscribers.insert(sub) {
            return None;
        }
        self.held.entry(sub).or_default().insert(id);
        self.stats.ops_add += 1;
        Some(SingleTableUpdate {
            query: sub,
            table: table.clone(),
            op: DataFrameOperation::Add(key.clone(), image.clone()),
        })
    }

    /// Wrap bare operations of `sub` into updates, for the runtime seam.
    fn tagged(&self, sub: SubId, ops: Vec<DataFrameOperation>) -> Vec<SingleTableUpdate> {
        let Some(query) = self.select_queries.get(&sub) else {
            return Vec::new();
        };
        let table = query.table.clone();
        ops.into_iter()
            .map(|op| SingleTableUpdate {
                query: sub,
                table: table.clone(),
                op,
            })
            .collect()
    }

    /// Address per-subscription updates to their clients (subscriptions
    /// without one, the join layer's inner parts, produce nothing here)
    /// and group them per client and row.
    fn addressed(&self, updates: Vec<SingleTableUpdate>) -> Vec<ClientUpdate> {
        group(
            updates
                .into_iter()
                .filter_map(|update| {
                    let client = *self.clients.get(&update.query)?;
                    Some(Raw {
                        client,
                        table: update.table,
                        target: Target {
                            sub: update.query,
                            part: QueryPart::main(),
                        },
                        op: update.op,
                    })
                })
                .collect(),
        )
    }

    /// Number of registered subscriptions.
    pub fn query_count(&self) -> usize {
        self.select_queries.len()
    }

    /// The engine's operation counters, accumulated since creation or the
    /// last [`SingleTableIVM::reset_stats`].
    pub fn stats(&self) -> &IvmStats {
        &self.stats
    }

    /// Reset all operation counters to zero; sync state is untouched.
    pub fn reset_stats(&mut self) {
        self.stats = IvmStats::default();
    }

    /// The single source of truth for "is this subscription impacted, and
    /// how" by a row of `table_name` identified by `key` taking the image
    /// `row_image` (`None` for a delete) — every routing entry point
    /// builds on it.
    ///
    /// Way 1 (row matches after the write) delegates to the table's routing
    /// index under a freshly bumped write epoch — the index itself also
    /// applies each candidate's admission boundary, exempting holders;
    /// deletes carry no row image and skip it entirely. Way 2 (row held
    /// before the write) checks the shared row's subscriber tags per
    /// same-table subscription — catches updates moving a row out, and
    /// deletes. Every returned [`Impact`] has at least one of the two
    /// facts set.
    fn analyze(
        &mut self,
        table_name: &TableName,
        key: &DataFrameKey,
        row_image: Option<&DataFrameRow>,
    ) -> Vec<Impact> {
        let table_name = table_name.clone();
        let key = key.clone();

        let mut holding: BTreeSet<SubId> = BTreeSet::new();
        let holders: Vec<SubId> = self
            .frames
            .get(&table_name)
            .and_then(|frame| frame.get(&key))
            .map(|row| row.subscribers.iter().copied().collect())
            .unwrap_or_default();
        for sub in holders {
            if self.select_queries.contains_key(&sub) {
                self.stats.membership_probes += 1;
                self.stats.membership_hits += 1;
                holding.insert(sub);
            }
        }

        let mut matched: BTreeSet<SubId> = BTreeSet::new();
        if let Some(row) = row_image {
            debug_assert!(
                key.pkey_value
                    .iter()
                    .all(|(column, value)| row.data.get(column) == Some(value)),
                "a write's record must carry its own primary-key values"
            );
            self.write_epoch += 1;
            if let Some(table_index) = self.tables.get(&table_name) {
                matched =
                    table_index.matched(&row.data, self.write_epoch, &holding, &mut self.stats);
            }
        }

        let mut impacts = Vec::new();
        for sub in matched.union(&holding) {
            self.stats.queries_impacted += 1;
            impacts.push(Impact {
                sub: *sub,
                matches_after: matched.contains(sub),
                present_before: holding.contains(sub),
            });
        }
        impacts
    }
}

impl Engine for SingleTableIVM {
    type Query = SingleTableReadQuery;

    /// [`SingleTableIVM::register_query`], its snapshot addressed to
    /// `client`.
    fn subscribe(
        &mut self,
        client: ClientId,
        query: SingleTableReadQuery,
    ) -> (SubId, Vec<ClientUpdate>) {
        let (sub, ops) = self.register_query(query);
        self.clients.insert(sub, client);
        self.by_client.entry(client).or_default().insert(sub);
        let updates = self.tagged(sub, ops);
        (sub, self.addressed(updates))
    }

    /// [`SingleTableIVM::unregister_query`].
    fn unsubscribe(&mut self, sub: SubId) {
        if let Some(client) = self.clients.remove(&sub)
            && let Some(subs) = self.by_client.get_mut(&client)
        {
            subs.remove(&sub);
            if subs.is_empty() {
                self.by_client.remove(&client);
            }
        }
        self.unregister_query(sub);
    }

    /// Every subscription of `client`, unregistered.
    fn unsubscribe_client(&mut self, client: ClientId) {
        for sub in self.by_client.remove(&client).unwrap_or_default() {
            self.clients.remove(&sub);
            self.unregister_query(sub);
        }
    }

    /// [`SingleTableIVM::incremental_update`], grouped per client.
    fn route(&mut self, write: &WriteQuery) -> Vec<ClientUpdate> {
        let updates = self.incremental_update(write);
        self.addressed(updates)
    }

    /// [`SingleTableIVM::land_fetch`], grouped per client.
    fn land(&mut self, fetch: &Fetch, rows: &[(DataFrameKey, DataFrameRow)]) -> Vec<ClientUpdate> {
        let updates = self.land_fetch(fetch, rows);
        self.addressed(updates)
    }

    /// [`SingleTableIVM::take_requests`].
    fn requests(&mut self) -> Vec<Fetch> {
        self.take_requests()
    }
}
