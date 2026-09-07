//! Multi-table subscriptions: LEFT JOINs maintained over the single-table
//! engine.
//!
//! A [`MultiTableReadQuery`] registers one inner single-table subscription
//! per part — `{uuid}_0` for the main table, `{uuid}_{k}` for left join `k`
//! (1-based); stripping the suffix recovers the external id. Main rows are
//! visible purely by the main `WHERE` (left join: an empty sub side renders
//! as null, it never hides the main row), so there is no admission,
//! exclusion, or reverse-admission — main-part operations forward directly.
//!
//! # The registered `IN` condition
//!
//! Each sub part is registered as `sub WHERE AND sub_col IN (referenced
//! values)` — the join condition lives *inside* the inner subscription's
//! filter, so sub-table writes route natively: a row matching the filter
//! (join condition included) fires an `Add`, a held row moving out fires
//! a `Delete` via membership, and unreferenced rows never fire at all.
//! A reference change edits that one `IN` condition **in place** — in the
//! stored filter and inside the sub part's indexed disjuncts
//! ([`SingleTableIVM::replace_condition`]) — with no unregister /
//! re-register churn. The join layer's only sub-side work is forwarding
//! operations and keeping its `right` counts current.
//!
//! # Counts and zero crossings
//!
//! Per query and per join, `left` counts each join value over the main
//! rows and `right` over the held sub rows. Only **left** crossings act:
//!
//! - `left` 0 → >0: the value joins the `IN` list
//!   ([`SingleTableIVM::replace_condition`]) and its sub rows are fetched.
//! - `left` >0 → 0: the value leaves the `IN` list and its held sub rows
//!   are pruned from the current data — no storage round-trip.
//!
//! `right` crossings change nothing (left join): `right == 0` with
//! `left > 0` is simply a null sub side. The invariant to hold is the
//! other direction — `left == 0` implies the value has no rows on the
//! right. Registration runs one storage query for the main part and then
//! **one** query per sub part with the full `IN` list (no per-value
//! loops), returning the whole snapshot as operations.
//!
//! Clients keep one frame per part plus the join spec and compose the
//! joined view themselves; updates arrive as
//! `(query, table, part, operation)` — the table names where the delta
//! lands, the part disambiguates self-joins.

use std::collections::HashMap;
use std::rc::Rc;

use super::storage::Storage;
use super::{IvmStats, QueryId, SingleTableIVM};
use crate::model::{
    ColumnName, ComparisonOperator, Condition, DataFrameKey, DataFrameOperation, DataFrameRow,
    LeftJoin, MultiTableReadQuery, SingleTableReadQuery, TableName, Value, Where, WriteQuery,
};

/// Which part of a multi-table subscription an operation belongs to — the
/// client patches the corresponding frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryPart {
    Main,
    Join(usize),
}

/// One operation for one part of one multi-table subscription — the unit
/// the transport pushes to the subscribed client.
///
/// - `query`: which subscription (the external id the client registered).
/// - `table`: the table the operation lands on — what a client keying its
///   local frames by table applies it to.
/// - `part`: which part of the query produced it — kept beside the table
///   because a self-join makes the table alone ambiguous.
/// - `op`: the delta itself.
#[derive(Debug, Clone, PartialEq)]
pub struct MultiTableUpdate {
    pub query: QueryId,
    pub table: TableName,
    pub part: QueryPart,
    pub op: DataFrameOperation,
}

impl MultiTableUpdate {
    /// Assemble one update, deriving the destination table from the
    /// part's position in the spec.
    fn new(
        spec: &MultiTableReadQuery,
        query: QueryId,
        part: QueryPart,
        op: DataFrameOperation,
    ) -> Self {
        let table = match part {
            QueryPart::Main => spec.main_table.table.clone(),
            QueryPart::Join(index) => spec.left_joins[index].sub_table.table.clone(),
        };
        MultiTableUpdate {
            query,
            table,
            part,
            op,
        }
    }
}

/// Per-join reference state of one subscription.
///
/// - `left`: how many main rows carry each join value.
/// - `right`: how many sub rows the join's part holds per value; purely
///   observational under left-join semantics (zero is a valid steady
///   state), kept for the `left == 0 ⇒ right == 0` invariant and for
///   future join flavors.
/// - `referenced`: the values with `left > 0`, in first-referenced order —
///   the deterministic `IN` list registered into the sub part's filter.
///
/// Entries leave `left`/`right` when they reach zero, so the maps only
/// hold live values.
#[derive(Default)]
struct JoinKeyCounts {
    left: HashMap<Value, u64>,
    right: HashMap<Value, u64>,
    referenced: Vec<Value>,
}

/// The join layer. Owns the inner [`SingleTableIVM`] exclusively, so the
/// deterministic `{uuid}_{part}` id scheme cannot collide with anything
/// registered from outside; storage lives inside the inner engine.
///
/// - `single`: the inner engine holding every part's routing and the
///   shared per-table frames.
/// - `select_queries`: external id → the multi-table spec (shared, cheap
///   to clone per update).
/// - `join_state`: external id → one `JoinKeyCounts` per join, parallel to
///   the spec's `left_joins`.
pub struct MultiTableIVM {
    single: SingleTableIVM,
    select_queries: HashMap<QueryId, Rc<MultiTableReadQuery>>,
    join_state: HashMap<QueryId, Vec<JoinKeyCounts>>,
}

/// The internal id of part `part` of subscription `uuid`.
fn part_id(uuid: &QueryId, part: usize) -> QueryId {
    QueryId::from(format!("{uuid}_{part}"))
}

/// Recover `(external id, part index)` from an internal part id.
///
/// Unambiguous even for external ids that themselves contain underscores:
/// `part_id` appends exactly one `_{part}` suffix and this strips exactly
/// one (`rsplit_once`), and the join layer owns its inner engine
/// exclusively, so every inner id was built by `part_id`.
fn split_part_id(internal: &QueryId) -> Option<(QueryId, usize)> {
    let (base, suffix) = internal.as_str().rsplit_once('_')?;
    Some((QueryId::from(base), suffix.parse().ok()?))
}

/// The row's value in `column`; a missing column joins like `NULL` (never).
fn join_value(row: &DataFrameRow, column: &ColumnName) -> Value {
    row.data.get(column.as_str()).cloned().unwrap_or(Value::Null)
}

/// The sub part's join condition: rows attach only while their join value
/// is among the currently referenced ones. An empty list matches nothing.
fn in_condition(join: &LeftJoin, referenced: &[Value]) -> Condition {
    Condition::new(
        join.sub_table_column.clone(),
        ComparisonOperator::IN,
        Value::List(referenced.to_vec()),
    )
}

/// The sub part's registered query: the join's own `WHERE` narrowed to
/// the currently referenced values — the join condition made part of the
/// filter, so sub-table writes route natively — with the limit normalized
/// away (a `LIMIT` on a join's sub side has no SQL meaning: it would cap
/// the whole side across every referenced value).
fn sub_query_for(join: &LeftJoin, referenced: &[Value]) -> SingleTableReadQuery {
    SingleTableReadQuery {
        filter: Where::AND(vec![
            join.sub_table.filter.clone(),
            Where::Condition(in_condition(join, referenced)),
        ]),
        limit: u32::MAX,
        ..join.sub_table.clone()
    }
}

impl MultiTableIVM {
    /// An empty join layer; the inner engine reads initial data and
    /// fetches from `storage`.
    pub fn new(storage: Rc<dyn Storage>) -> Self {
        MultiTableIVM {
            single: SingleTableIVM::new(storage),
            select_queries: HashMap::new(),
            join_state: HashMap::new(),
        }
    }

    /// Register a multi-table subscription under `query_uuid`, returning
    /// its initial snapshot as operations.
    ///
    /// The main part registers first and loads its result set from
    /// storage; its rows seed the `left` counts and the per-join `IN`
    /// lists; each sub part then registers with its narrowed filter and
    /// loads its rows in a single storage query. Re-registering the
    /// identical spec is a no-op returning no operations; a changed spec
    /// replaces the subscription.
    pub fn register_query(
        &mut self,
        query_uuid: impl Into<QueryId>,
        query: MultiTableReadQuery,
    ) -> Vec<MultiTableUpdate> {
        let query_uuid = query_uuid.into();
        if self
            .select_queries
            .get(&query_uuid)
            .is_some_and(|existing| **existing == query)
        {
            return Vec::new();
        }
        if self.select_queries.contains_key(&query_uuid) {
            self.unregister_query(query_uuid.as_str());
        }

        let main_ops =
            self.single
                .register_query(part_id(&query_uuid, 0), query.main_table.clone(), None);
        self.join_state.insert(
            query_uuid.clone(),
            query
                .left_joins
                .iter()
                .map(|_| JoinKeyCounts::default())
                .collect(),
        );

        let mut out = Vec::new();
        for op in main_ops {
            if let DataFrameOperation::Add(_, row) = &op {
                for (index, join) in query.left_joins.iter().enumerate() {
                    let value = join_value(row, &join.main_table_column);
                    if self.left_bump(&query_uuid, index, value.clone()) {
                        self.push_reference(&query_uuid, index, value);
                    }
                }
            }
            out.push(MultiTableUpdate::new(
                &query,
                query_uuid.clone(),
                QueryPart::Main,
                op,
            ));
        }
        for (index, join) in query.left_joins.iter().enumerate() {
            let referenced = self.referenced(&query_uuid, index);
            let sub_ops = self.single.register_query(
                part_id(&query_uuid, index + 1),
                sub_query_for(join, &referenced),
                None,
            );
            for op in sub_ops {
                if let DataFrameOperation::Add(_, row) = &op {
                    self.right_bump(
                        &query_uuid,
                        index,
                        join_value(row, &join.sub_table_column),
                    );
                }
                out.push(MultiTableUpdate::new(
                    &query,
                    query_uuid.clone(),
                    QueryPart::Join(index),
                    op,
                ));
            }
        }

        self.select_queries.insert(query_uuid, Rc::new(query));
        out
    }

    /// Remove a multi-table subscription: every inner part and its join
    /// state. Unknown ids are a no-op.
    pub fn unregister_query(&mut self, query_uuid: &str) {
        let Some(spec) = self.select_queries.remove(query_uuid) else {
            return;
        };
        let uuid = QueryId::from(query_uuid);
        for part in 0..=spec.left_joins.len() {
            self.single.unregister_query(part_id(&uuid, part).as_str());
        }
        self.join_state.remove(query_uuid);
    }

    /// Route one write through the inner engine and forward the resulting
    /// per-part operations, maintaining the join state on the way: main
    /// operations adjust `left` counts (zero crossings widen or narrow the
    /// sub parts' `IN` lists, fetching or pruning their rows); sub
    /// operations only adjust `right` counts, because the registered `IN`
    /// condition already routed them correctly.
    ///
    /// An in-place main-row replacement arrives from the inner engine as
    /// an adjacent `Delete(old)` + `Add(new)` pair for the same key; the
    /// pair is recognized and its join values *diffed*, so a rewrite that
    /// keeps a join value does not swing that value's `left` count through
    /// zero (which would prune and refetch its sub rows for nothing).
    ///
    /// Sub-part operations are forwarded **before** main-part ones: they
    /// were captured against the pre-write `IN` lists, while handling a
    /// main operation can mutate the very same sub frames (fetch on a new
    /// reference, prune on a released one). In a self-join — the same
    /// table as main and sub — one write produces both kinds, and
    /// forwarding a stale sub `Add` after the prune's `Delete` would
    /// resurrect the row on the client; sub-first keeps every emission
    /// consistent with the frame at its moment.
    pub fn incremental_update(&mut self, write: &WriteQuery) -> Vec<MultiTableUpdate> {
        let applied = self.single.incremental_update(write);
        let (sub_ops, main_ops): (Vec<_>, Vec<_>) = applied
            .into_iter()
            .partition(|update| {
                split_part_id(&update.query).is_some_and(|(_, part)| part != 0)
            });

        let mut out = Vec::new();
        for update in sub_ops {
            let Some((uuid, part)) = split_part_id(&update.query) else {
                continue;
            };
            let Some(spec) = self.select_queries.get(&uuid).cloned() else {
                continue;
            };
            if part - 1 < spec.left_joins.len() {
                self.handle_sub_op(&uuid, &spec, part - 1, update.op, &mut out);
            }
        }
        let mut main_ops = main_ops.into_iter().peekable();
        while let Some(update) = main_ops.next() {
            let Some((uuid, _)) = split_part_id(&update.query) else {
                continue;
            };
            let Some(spec) = self.select_queries.get(&uuid).cloned() else {
                continue;
            };
            let paired_add = match (&update.op, main_ops.peek()) {
                (DataFrameOperation::Delete(key, _), Some(next))
                    if next.query == update.query
                        && matches!(&next.op, DataFrameOperation::Add(next_key, _) if next_key == key) =>
                {
                    Some(main_ops.next().expect("peeked just above").op)
                }
                _ => None,
            };
            match paired_add {
                Some(add) => self.handle_main_replace(&uuid, &spec, update.op, add, &mut out),
                None => self.handle_main_op(&uuid, &spec, update.op, &mut out),
            }
        }
        out
    }

    /// The rows currently held for one part of a subscription — key →
    /// image, an inspection view for tests and debugging. `None` for
    /// unknown ids.
    pub fn rows_for(
        &self,
        query_uuid: &str,
        part: QueryPart,
    ) -> Option<HashMap<DataFrameKey, DataFrameRow>> {
        let index = match part {
            QueryPart::Main => 0,
            QueryPart::Join(join_index) => join_index + 1,
        };
        self.single
            .rows_for(part_id(&QueryId::from(query_uuid), index).as_str())
    }

    /// The inner engine's routing counters.
    pub fn stats(&self) -> &IvmStats {
        self.single.stats()
    }

    /// A standalone main-part change: forward it as-is (left join —
    /// visibility is the main `WHERE` alone), then reference the new
    /// image's join values on an `Add` or release the removed image's on a
    /// `Delete`.
    fn handle_main_op(
        &mut self,
        uuid: &QueryId,
        spec: &Rc<MultiTableReadQuery>,
        op: DataFrameOperation,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        match op {
            DataFrameOperation::Add(key, row) => {
                out.push(MultiTableUpdate::new(
                    spec,
                    uuid.clone(),
                    QueryPart::Main,
                    DataFrameOperation::Add(key, row.clone()),
                ));
                for (index, join) in spec.left_joins.iter().enumerate() {
                    let value = join_value(&row, &join.main_table_column);
                    self.reference_value(uuid, spec, index, value, out);
                }
            }
            DataFrameOperation::Delete(key, row) => {
                out.push(MultiTableUpdate::new(
                    spec,
                    uuid.clone(),
                    QueryPart::Main,
                    DataFrameOperation::Delete(key, row.clone()),
                ));
                for (index, join) in spec.left_joins.iter().enumerate() {
                    let old = join_value(&row, &join.main_table_column);
                    self.release_value(uuid, spec, index, &old, out);
                }
            }
        }
    }

    /// An in-place main-row replacement (`Delete(old)` + `Add(new)`, same
    /// key): forward both operations, then move `left` references only for
    /// joins whose value actually changed — the new value referenced
    /// first, the old released after.
    fn handle_main_replace(
        &mut self,
        uuid: &QueryId,
        spec: &Rc<MultiTableReadQuery>,
        delete: DataFrameOperation,
        add: DataFrameOperation,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let old_row = delete.row().clone();
        let new_row = add.row().clone();
        out.push(MultiTableUpdate::new(
            spec,
            uuid.clone(),
            QueryPart::Main,
            delete,
        ));
        out.push(MultiTableUpdate::new(
            spec,
            uuid.clone(),
            QueryPart::Main,
            add,
        ));
        for (index, join) in spec.left_joins.iter().enumerate() {
            let new_value = join_value(&new_row, &join.main_table_column);
            let old_value = join_value(&old_row, &join.main_table_column);
            if old_value == new_value {
                continue;
            }
            self.reference_value(uuid, spec, index, new_value, out);
            self.release_value(uuid, spec, index, &old_value, out);
        }
    }

    /// A sub-part change: forward it as-is — the registered `IN` condition
    /// already decided relevance — and keep the `right` counts current: an
    /// `Add` bumps its image's join value, a `Delete` drops its removed
    /// image's (a replacement's adjacent pair nets to a move between
    /// values).
    fn handle_sub_op(
        &mut self,
        uuid: &QueryId,
        spec: &Rc<MultiTableReadQuery>,
        join_index: usize,
        op: DataFrameOperation,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let column = &spec.left_joins[join_index].sub_table_column;
        match &op {
            DataFrameOperation::Add(_, row) => {
                self.right_bump(uuid, join_index, join_value(row, column));
            }
            DataFrameOperation::Delete(_, row) => {
                self.right_drop(uuid, join_index, &join_value(row, column));
            }
        }
        out.push(MultiTableUpdate::new(
            spec,
            uuid.clone(),
            QueryPart::Join(join_index),
            op,
        ));
    }

    /// A main row now carries `value` on join `join_index`: bump `left`,
    /// and on a 0 → 1 crossing widen the sub part's `IN` condition in
    /// place and fetch the value's sub rows (one narrowed storage query),
    /// forwarding their `Add`s.
    fn reference_value(
        &mut self,
        uuid: &QueryId,
        spec: &Rc<MultiTableReadQuery>,
        join_index: usize,
        value: Value,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let before = self.referenced(uuid, join_index);
        if !self.left_bump(uuid, join_index, value.clone()) {
            return;
        }
        let join = &spec.left_joins[join_index];
        self.push_reference(uuid, join_index, value.clone());
        let referenced = self.referenced(uuid, join_index);
        let sub_part = part_id(uuid, join_index + 1);
        self.single.replace_condition(
            sub_part.as_str(),
            &in_condition(join, &before),
            in_condition(join, &referenced),
        );
        let adds = self.single.fetch(
            sub_part.as_str(),
            join.sub_table_column.as_str(),
            std::slice::from_ref(&value),
        );
        self.single.mark_reconciled(sub_part.as_str());
        for op in adds {
            if let DataFrameOperation::Add(_, row) = &op {
                self.right_bump(uuid, join_index, join_value(row, &join.sub_table_column));
            }
            out.push(MultiTableUpdate::new(
                spec,
                uuid.clone(),
                QueryPart::Join(join_index),
                op,
            ));
        }
    }

    /// A main row no longer carries `value` on join `join_index`: drop
    /// `left`, and on a crossing to 0 narrow the sub part's `IN` condition
    /// in place and prune its held rows from the current data — no storage
    /// round-trip — forwarding their `Delete`s.
    fn release_value(
        &mut self,
        uuid: &QueryId,
        spec: &Rc<MultiTableReadQuery>,
        join_index: usize,
        value: &Value,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let before = self.referenced(uuid, join_index);
        if !self.left_drop(uuid, join_index, value) {
            return;
        }
        let join = &spec.left_joins[join_index];
        self.pull_reference(uuid, join_index, value);
        let referenced = self.referenced(uuid, join_index);
        let sub_part = part_id(uuid, join_index + 1);
        self.single.replace_condition(
            sub_part.as_str(),
            &in_condition(join, &before),
            in_condition(join, &referenced),
        );
        let deletes = self.single.delete_rows(
            sub_part.as_str(),
            join.sub_table_column.as_str(),
            std::slice::from_ref(value),
        );
        self.single.mark_reconciled(sub_part.as_str());
        for op in deletes {
            self.right_drop(uuid, join_index, value);
            out.push(MultiTableUpdate::new(
                spec,
                uuid.clone(),
                QueryPart::Join(join_index),
                op,
            ));
        }
    }

    /// The current `IN` list of join `join_index`, in first-referenced
    /// order.
    fn referenced(&self, uuid: &QueryId, join_index: usize) -> Vec<Value> {
        self.join_state
            .get(uuid)
            .and_then(|joins| joins.get(join_index))
            .map(|join| {
                debug_assert!(
                    join.referenced.len() == join.left.len(),
                    "the IN list must mirror exactly the values with left > 0"
                );
                join.referenced.clone()
            })
            .unwrap_or_default()
    }

    /// Append a newly referenced value to join `join_index`'s `IN` list.
    fn push_reference(&mut self, uuid: &QueryId, join_index: usize, value: Value) {
        if let Some(join) = self
            .join_state
            .get_mut(uuid)
            .and_then(|joins| joins.get_mut(join_index))
            && !join.referenced.contains(&value) {
                join.referenced.push(value);
            }
    }

    /// Remove a no-longer-referenced value from join `join_index`'s `IN`
    /// list.
    fn pull_reference(&mut self, uuid: &QueryId, join_index: usize, value: &Value) {
        if let Some(join) = self
            .join_state
            .get_mut(uuid)
            .and_then(|joins| joins.get_mut(join_index))
        {
            join.referenced.retain(|candidate| candidate != value);
        }
    }

    /// Increment `value`'s `left` count on join `join_index`; reports
    /// whether this was the 0 → 1 crossing.
    fn left_bump(&mut self, uuid: &QueryId, join_index: usize, value: Value) -> bool {
        let Some(join) = self
            .join_state
            .get_mut(uuid)
            .and_then(|joins| joins.get_mut(join_index))
        else {
            return false;
        };
        let count = join.left.entry(value).or_insert(0);
        *count += 1;
        *count == 1
    }

    /// Decrement `value`'s `left` count on join `join_index`; reports
    /// whether it reached zero (the entry is removed when it does).
    fn left_drop(&mut self, uuid: &QueryId, join_index: usize, value: &Value) -> bool {
        Self::drop_in(
            self.join_state
                .get_mut(uuid)
                .and_then(|joins| joins.get_mut(join_index))
                .map(|join| &mut join.left),
            value,
        )
    }

    /// Increment `value`'s `right` count on join `join_index`.
    fn right_bump(&mut self, uuid: &QueryId, join_index: usize, value: Value) {
        if let Some(join) = self
            .join_state
            .get_mut(uuid)
            .and_then(|joins| joins.get_mut(join_index))
        {
            *join.right.entry(value).or_insert(0) += 1;
        }
    }

    /// Decrement `value`'s `right` count on join `join_index`; reports
    /// whether it reached zero (the entry is removed when it does).
    fn right_drop(&mut self, uuid: &QueryId, join_index: usize, value: &Value) -> bool {
        Self::drop_in(
            self.join_state
                .get_mut(uuid)
                .and_then(|joins| joins.get_mut(join_index))
                .map(|join| &mut join.right),
            value,
        )
    }

    /// Decrement `value` in one count map, removing the entry at zero;
    /// reports whether it reached zero. Absent values report `false`.
    fn drop_in(counts: Option<&mut HashMap<Value, u64>>, value: &Value) -> bool {
        let Some(counts) = counts else {
            return false;
        };
        let Some(count) = counts.get_mut(value) else {
            return false;
        };
        *count -= 1;
        if *count == 0 {
            counts.remove(value);
            true
        } else {
            false
        }
    }
}
