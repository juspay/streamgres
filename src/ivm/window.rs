//! ORDER BY / LIMIT maintenance: the per-subscription window state and
//! the engine hooks that keep it — and its **boundary condition** in the
//! routing index — in lockstep with the held rows.
//!
//! A query with a finite `LIMIT L` is maintained as a **doubled buffer**:
//! storage queries are issued with `2·L`, and the subscription keeps a
//! [`Window`] — its held rows' order values, best → worst, plus the
//! **frontier**: the worst order value known to be *covered* from
//! storage, meaning every matching row strictly better than it is held.
//! Storage reads set the frontier (the worst value fetched when a read
//! returned as many rows as it asked for; absent when it returned fewer,
//! since storage is then exhausted and every matching row is held), and
//! evictions pull it in (an evicted row is back in storage unheld, so the
//! covered prefix ends at its value). Three behaviors derive from it:
//!
//! - **admission boundary**: the strict condition "better than the
//!   frontier" (`<` for ASC, `>` for DESC), whether or not the buffer is
//!   full; absent while storage is exhausted. It is *pushed into the
//!   table's routing index* ([`SingleTableIVM::sync_boundary`]) whenever
//!   it changes, so `matched()` itself drops candidates the boundary
//!   rejects — a write beyond the frontier never even reaches the engine,
//!   and rows are never admitted unboundedly. The boundary gates
//!   **admission only**: a row the subscription already holds is exempt
//!   (it keeps its slot when its value worsens, until a better arrival
//!   evicts it).
//! - **eviction** ([`SingleTableIVM::evict_overflow`]): rows past
//!   capacity are untagged worst-first, each emitting its `Delete` and
//!   becoming the frontier.
//! - **refill** ([`SingleTableIVM::refill`]): when a removal drains the
//!   buffer to `L`, one storage read is asked for from the frontier
//!   *inclusive* (`>=` for ASC, `<=` for DESC), sized for the missing rows
//!   plus the held rows the threshold returns again (they dedup on
//!   upsert), back up to capacity; when it lands the frontier moves to
//!   the worst value fetched. Anchoring both the boundary and the refill
//!   at the frontier rather than at the worst *held* row is what keeps
//!   the top-`L` exact: a row admitted while the buffer had room can
//!   never push the refill threshold past storage rows that were never
//!   fetched, and rows tying the frontier stay reachable.
//!
//! `ORDER BY` may name several columns, compared in turn. The order value
//! of a row is then a tuple, the frontier one too, and the boundary and
//! refill threshold become the lexicographic comparison spelled out as a
//! predicate: for `ORDER BY a ASC, b ASC` and frontier `(fa, fb)` the
//! admission boundary is `a < fa OR (a = fa AND b < fb)`, one branch per
//! column with the earlier columns tied, each branch strict in its own
//! direction; the refill threshold is the same shape with the operators
//! reversed and the last column inclusive. A frontier with a `NULL` or
//! `NaN` in any column is unenforceable, as for one column.
//!
//! **What the client sees.** The buffer is the engine's; the client is sent
//! exactly the best `L` rows. After every step that touched a windowed
//! subscription its raw operations are narrowed
//! ([`SingleTableIVM::gate_window`]) to the difference between the previous
//! and the current best-`L` prefix: a row entering the prefix is an `Add`
//! (whether it arrived from storage or a write, or moved up from the
//! buffer), a row leaving it a `Delete` (whether it left the buffer or was
//! pushed down into it), a shown row rewritten in place its own operations
//! (the `Delete` + `Add` pair the join layer diffs, one `Add` at the
//! client); buffer rows below the prefix produce nothing. A twin registration and
//! [`SingleTableIVM::rows_for`] see the same prefix.
//!
//! A storage read lands some time after it is asked for, and writes route
//! in between. While one is out the window publishes **no boundary** (so
//! no write the read will not return is turned away; arrivals are
//! admitted and, past capacity, evicted, each eviction pulling the
//! frontier in as usual) and asks for no further refill; a read that
//! covers the whole filter (a registration's snapshot, a refill) also
//! clears the frontier when asked for, since what storage holds beyond
//! the held rows is unknown until it lands. Landing then re-derives the
//! frontier: a full result covers up to its worst value, pulled in by any
//! eviction that happened meanwhile; a short result leaves it where the
//! evictions put it (absent if there were none, meaning storage is
//! exhausted).
//!
//! Rows are ordered with [`order_cmp`], a total order extending
//! [`crate::model::Value::compare`]: `NaN` sorts above every other
//! numeric and `Null` above everything (Postgres's convention that both
//! are "largest"), and remaining incomparable pairs fall back to a fixed
//! variant rank. A `Null`/`NaN` **frontier** is unenforceable as a
//! predicate (comparisons touching them are always false), so the window
//! then publishes no boundary rather than reject everything — admission
//! falls back to accept-then-evict, and refills run unthresholded, sized
//! to capacity (re-fetched held rows dedup harmlessly). Ties are broken
//! arbitrarily, like SQL. A `LIMIT 0` subscription publishes an
//! always-false boundary (`IN ()`), so it stays permanently empty.

use std::cmp::Ordering;

use super::{SingleTableIVM, SingleTableUpdate};
use crate::model::{
    ComparisonOperator, Condition, DataFrameKey, DataFrameOperation, DataFrameRow, Order, OrderBy,
    SingleTableReadQuery, SubId, TableName, Value, Where,
};

/// The ORDER BY / LIMIT state of one subscription: its held rows' order
/// values (one per `ORDER BY` column), best → worst, the storage frontier
/// the boundary and the refill threshold are anchored at, and the keys
/// last delivered to the client (the best-`L` prefix at the last step).
pub(super) struct Window {
    order: Vec<OrderBy>,
    user_limit: usize,
    entries: Vec<(Vec<Value>, DataFrameKey)>,
    frontier: Option<Vec<Value>>,
    shown: Vec<DataFrameKey>,
}

impl Window {
    /// An empty window for `query`; `None` when the query has no finite,
    /// positive limit (`LIMIT 0` is handled by an always-false boundary
    /// instead — see the module header).
    pub(super) fn for_query(query: &SingleTableReadQuery) -> Option<Self> {
        if !windowed(query) {
            return None;
        }
        Some(Window {
            order: query.order_by.clone(),
            user_limit: query.limit as usize,
            entries: Vec::new(),
            frontier: None,
            shown: Vec::new(),
        })
    }

    /// The buffer capacity: twice the user's limit.
    fn capacity(&self) -> usize {
        self.user_limit * 2
    }

    /// The order value of a row image: one value per `ORDER BY` column
    /// (`Null` where the column is absent).
    pub(super) fn order_value(&self, row: &DataFrameRow) -> Vec<Value> {
        self.order
            .iter()
            .map(|clause| row.data.get(&clause.column).cloned().unwrap_or(Value::Null))
            .collect()
    }

    /// Insert one row's order value, keeping best → worst order; ties land
    /// after their equals (stable). Idempotent: a key already present is
    /// re-inserted at its new value's position, never duplicated.
    pub(super) fn insert(&mut self, value: Vec<Value>, key: DataFrameKey) {
        self.remove(&key);
        let position = self
            .entries
            .iter()
            .position(|(existing, _)| is_worse(existing, &value, &self.order))
            .unwrap_or(self.entries.len());
        self.entries.insert(position, (value, key));
    }

    /// The keys of the best `L` held rows: what the client is shown.
    pub(super) fn shown_prefix(&self) -> Vec<DataFrameKey> {
        self.entries
            .iter()
            .take(self.user_limit)
            .map(|(_, key)| key.clone())
            .collect()
    }

    /// Record the current prefix as delivered, returning the previous and
    /// the current one for the gate to diff.
    pub(super) fn take_shown(&mut self) -> (Vec<DataFrameKey>, Vec<DataFrameKey>) {
        let current = self.shown_prefix();
        let previous = std::mem::replace(&mut self.shown, current.clone());
        (previous, current)
    }

    /// Drop one row from the window; reports whether it was present. The
    /// frontier is untouched: a removal does not change what storage
    /// covers.
    pub(super) fn remove(&mut self, key: &DataFrameKey) -> bool {
        match self
            .entries
            .iter()
            .position(|(_, existing)| existing == key)
        {
            Some(index) => {
                self.entries.remove(index);
                true
            }
            None => false,
        }
    }

    /// The worst held row past capacity, removed from the window and
    /// recorded as the frontier (it is back in storage, unheld) — the
    /// engine untags it and emits its `Delete`.
    pub(super) fn pop_overflow(&mut self) -> Option<DataFrameKey> {
        if self.entries.len() <= self.capacity() {
            return None;
        }
        let (value, key) = self.entries.pop()?;
        self.cover(value);
        Some(key)
    }

    /// Pull the frontier in to `value` unless the current frontier is
    /// already at or better than it: the covered prefix only ever shrinks
    /// here (a held row that worsened in place past the frontier and is
    /// then evicted must not widen it).
    fn cover(&mut self, value: Vec<Value>) {
        let keep = self
            .frontier
            .as_ref()
            .is_some_and(|frontier| !is_worse(frontier, &value, &self.order));
        if !keep {
            self.frontier = Some(value);
        }
    }

    /// The worst order value among fetched rows.
    fn worst_of(&self, fetched: &[(DataFrameKey, DataFrameRow)]) -> Option<Vec<Value>> {
        fetched
            .iter()
            .map(|(_, row)| self.order_value(row))
            .reduce(|worst, value| {
                if is_worse(&value, &worst, &self.order) {
                    value
                } else {
                    worst
                }
            })
    }

    /// Record a landed storage read that asked for `requested` rows: a
    /// full result proves rows beyond its worst value exist unheld, so the
    /// frontier pulls in to it (from absent, it is set); a short result
    /// says nothing beyond what evictions already recorded. A read of the
    /// whole filter cleared the frontier when it was asked for, so for it
    /// a full result sets the frontier to its worst value (pulled in by
    /// any eviction meanwhile) and a short one leaves storage exhausted.
    pub(super) fn note_fetch(
        &mut self,
        requested: usize,
        fetched: &[(DataFrameKey, DataFrameRow)],
    ) {
        if fetched.len() >= requested
            && let Some(worst) = self.worst_of(fetched)
        {
            self.cover(worst);
        }
    }

    /// The current frontier — what a twin registration inherits.
    pub(super) fn frontier(&self) -> Option<Vec<Value>> {
        self.frontier.clone()
    }

    /// Adopt a frontier wholesale (a twin's, whose held rows this window
    /// was just rebuilt from).
    pub(super) fn set_frontier(&mut self, frontier: Option<Vec<Value>>) {
        self.frontier = frontier;
    }

    /// The admission boundary: strictly better than the frontier, column
    /// by column (`<` for ASC, `>` for DESC, the earlier columns tied).
    /// Absent while storage is exhausted, and absent for an unenforceable
    /// frontier (`Null`/`NaN` in any column).
    pub(super) fn boundary_condition(&self) -> Option<Where> {
        let frontier = self.frontier.as_ref()?;
        if !enforceable(frontier) {
            return None;
        }
        Some(lexicographic(
            &self.order,
            frontier,
            |direction, _| match direction {
                Order::ASC => ComparisonOperator::LT,
                Order::DESC => ComparisonOperator::GT,
            },
        ))
    }

    /// The refill read as (limit, threshold): `None` when storage is
    /// exhausted (nothing left to fetch). Otherwise the threshold is "at or
    /// worse than the frontier" (`>` / `<` by direction with the earlier
    /// columns tied, inclusive on the last column) and the limit is the
    /// missing count plus the held rows at or beyond the frontier, which
    /// the threshold returns again and the upsert dedups; an unenforceable
    /// frontier yields an unthresholded read sized to capacity.
    pub(super) fn refill_plan(&self) -> Option<(u32, Option<Where>)> {
        let frontier = self.frontier.as_ref()?;
        if !enforceable(frontier) {
            return Some((self.capacity() as u32, None));
        }
        let held_beyond = self
            .entries
            .iter()
            .filter(|(value, _)| !is_worse(frontier, value, &self.order))
            .count();
        let limit = (self.missing() as usize + held_beyond) as u32;
        let threshold = lexicographic(&self.order, frontier, |direction, last| {
            match (direction, last) {
                (Order::ASC, false) => ComparisonOperator::GT,
                (Order::ASC, true) => ComparisonOperator::GTE,
                (Order::DESC, false) => ComparisonOperator::LT,
                (Order::DESC, true) => ComparisonOperator::LTE,
            }
        });
        Some((limit, Some(threshold)))
    }

    /// Whether the buffer has drained to the user's limit — the refill
    /// trigger, checked after removals.
    pub(super) fn needs_refill(&self) -> bool {
        self.entries.len() <= self.user_limit
    }

    /// How many rows a refill should fetch to restore full capacity.
    pub(super) fn missing(&self) -> u32 {
        self.capacity().saturating_sub(self.entries.len()) as u32
    }
}

/// Whether a query is windowed: a finite, positive limit.
pub(super) fn windowed(query: &SingleTableReadQuery) -> bool {
    query.limit > 0 && query.limit < u32::MAX
}

/// The limit a storage query should carry: doubled for a windowed query
/// (to fill the buffer), untouched otherwise.
pub(super) fn storage_limit(query: &SingleTableReadQuery) -> u32 {
    if windowed(query) {
        query.limit.saturating_mul(2)
    } else {
        query.limit
    }
}

/// Whether a frontier can be enforced as a predicate — comparisons
/// touching `Null` or `NaN` are always false, so a condition built from
/// one would reject or fetch nothing.
fn enforceable(values: &[Value]) -> bool {
    values
        .iter()
        .all(|value| !value.is_null() && !matches!(value, Value::Float(f) if f.is_nan()))
}

/// Whether `existing` sorts strictly worse than `candidate` under `order`:
/// the first column that differs decides, worse being greater for ASC and
/// less for DESC; a full tie is not worse.
fn is_worse(existing: &[Value], candidate: &[Value], order: &[OrderBy]) -> bool {
    for (index, clause) in order.iter().enumerate() {
        let ordering = order_cmp(
            existing.get(index).unwrap_or(&Value::Null),
            candidate.get(index).unwrap_or(&Value::Null),
        );
        if ordering == Ordering::Equal {
            continue;
        }
        return match clause.direction {
            Order::ASC => ordering == Ordering::Greater,
            Order::DESC => ordering == Ordering::Less,
        };
    }
    false
}

/// The lexicographic comparison against `frontier` as a predicate: one
/// branch per column, the earlier columns tied and the column itself
/// compared with the operator `operator_for` picks from its direction and
/// whether it is the last; a single column is the bare condition.
fn lexicographic(
    order: &[OrderBy],
    frontier: &[Value],
    operator_for: impl Fn(Order, bool) -> ComparisonOperator,
) -> Where {
    let branches: Vec<Where> = order
        .iter()
        .enumerate()
        .map(|(index, clause)| {
            let mut conjuncts: Vec<Where> = order[..index]
                .iter()
                .zip(frontier)
                .map(|(tied, value)| {
                    Where::Condition(Condition::new(
                        tied.column.clone(),
                        ComparisonOperator::EQ,
                        value.clone(),
                    ))
                })
                .collect();
            conjuncts.push(Where::Condition(Condition::new(
                clause.column.clone(),
                operator_for(clause.direction, index + 1 == order.len()),
                frontier.get(index).cloned().unwrap_or(Value::Null),
            )));
            if conjuncts.len() == 1 {
                conjuncts.pop().expect("one conjunct")
            } else {
                Where::AND(conjuncts)
            }
        })
        .collect();
    if branches.len() == 1 {
        branches.into_iter().next().expect("one branch")
    } else {
        Where::OR(branches)
    }
}

/// The order of two rows under `order`: the first differing column
/// decides, reversed for DESC; a full tie is `Equal`. The comparison the
/// storage doubles sort by.
pub fn order_rows(order: &[OrderBy], a: &DataFrameRow, b: &DataFrameRow) -> Ordering {
    for clause in order {
        let ordering = order_cmp(
            a.data.get(&clause.column).unwrap_or(&Value::Null),
            b.data.get(&clause.column).unwrap_or(&Value::Null),
        );
        if ordering == Ordering::Equal {
            continue;
        }
        return match clause.direction {
            Order::ASC => ordering,
            Order::DESC => ordering.reverse(),
        };
    }
    Ordering::Equal
}

/// Total order over [`Value`]s for window and storage sorting: delegates
/// to [`Value::compare`] where defined; `NaN` sorts above every other
/// numeric (all `NaN`s equal) and `Null` above everything — Postgres's
/// convention — and remaining incomparable pairs fall back to a fixed
/// variant rank. Deterministic and transitive, if semantically arbitrary
/// across types.
pub fn order_cmp(a: &Value, b: &Value) -> Ordering {
    if let Some(ordering) = a.compare(b) {
        return ordering;
    }
    let a_nan = matches!(a, Value::Float(f) if f.is_nan());
    let b_nan = matches!(b, Value::Float(f) if f.is_nan());
    let a_numeric = matches!(a, Value::Int(_) | Value::Float(_));
    let b_numeric = matches!(b, Value::Int(_) | Value::Float(_));
    if a_nan && b_numeric && !b_nan {
        return Ordering::Greater;
    }
    if b_nan && a_numeric && !a_nan {
        return Ordering::Less;
    }
    rank(a).cmp(&rank(b))
}

/// The variant rank backing [`order_cmp`]'s cross-type fallback; `Null`
/// last, per the module header.
fn rank(value: &Value) -> u8 {
    match value {
        Value::Bool(_) => 0,
        Value::Int(_) | Value::Float(_) => 1,
        Value::String(_) => 2,
        Value::Date(_) => 3,
        Value::Datetime(_) => 4,
        Value::List(_) => 5,
        Value::Map(_) | Value::Set(_) => 6,
        Value::Null => 7,
    }
}

impl SingleTableIVM {
    /// Publish the subscription's current admission boundary into its
    /// table's routing index — the `boundaries` side table `matched()`
    /// filters candidates through. Called after every change that can
    /// move the boundary; a `LIMIT 0` query publishes the always-false
    /// `OR()`, and a subscription with a storage read out publishes none.
    pub(super) fn sync_boundary(&mut self, sub: SubId) {
        let Some(query) = self.select_queries.get(&sub) else {
            return;
        };
        let table = query.table.clone();
        let boundary = if query.limit == 0 {
            Some(Where::OR(Vec::new()))
        } else if self.is_pending(sub) {
            None
        } else {
            self.windows
                .get(&sub)
                .and_then(|window| window.boundary_condition())
        };
        self.tables
            .entry(table)
            .or_default()
            .set_boundary(sub, boundary);
    }

    /// (Re)derive a subscription's window entries from the rows it
    /// currently holds, with no frontier yet — the caller records the
    /// storage read or twin the rows came from; queries without a finite
    /// positive limit carry no window.
    pub(super) fn rebuild_window(&mut self, sub: SubId) {
        let Some(query) = self.select_queries.get(&sub) else {
            self.windows.remove(&sub);
            return;
        };
        let Some(mut window) = Window::for_query(query) else {
            self.windows.remove(&sub);
            return;
        };
        if let Some(ids) = self.held.get(&sub)
            && let Some(frame) = self.frames.get(&query.table)
        {
            for id in ids {
                if let Some(row) = frame.row(*id) {
                    let value = window.order_value(&row.data);
                    window.insert(value, row.key.clone());
                }
            }
        }
        self.windows.insert(sub, window);
    }

    /// Untag worst-held rows until the subscription is back at buffer
    /// capacity, returning their `Delete`s; each eviction pulls the
    /// frontier in to the evicted value.
    pub(super) fn evict_overflow(&mut self, sub: SubId) -> Vec<DataFrameOperation> {
        let mut ops = Vec::new();
        while let Some(evicted) = self
            .windows
            .get_mut(&sub)
            .and_then(|window| window.pop_overflow())
        {
            self.stats.window_evictions += 1;
            if let Some(op) = self.remove_row(sub, &evicted) {
                ops.push(op);
            }
        }
        ops
    }

    /// Ask for a drained buffer's refill back to capacity: one storage
    /// read from the frontier inclusive (unthresholded for an
    /// unenforceable one; nothing at all when storage is exhausted, or
    /// while another read is out), ordered, sized by the window's refill
    /// plan. The frontier is cleared until it lands (see the module
    /// header); landing tags the rows in and re-derives it.
    pub(super) fn refill(&mut self, sub: SubId) {
        if self.is_pending(sub) {
            return;
        }
        let Some(query) = self.select_queries.get(&sub).cloned() else {
            return;
        };
        let Some((limit, threshold)) = self.windows.get(&sub).and_then(Window::refill_plan) else {
            return;
        };
        if limit == 0 {
            return;
        }
        let mut parts = vec![query.filter.clone()];
        parts.extend(threshold);
        let refill_query = SingleTableReadQuery {
            table: query.table.clone(),
            filter: Where::AND(parts),
            order_by: query.order_by.clone(),
            limit,
        };
        if let Some(window) = self.windows.get_mut(&sub) {
            window.set_frontier(None);
        }
        self.stats.window_refills += 1;
        self.issue(sub, refill_query, super::FetchKind::Refill);
    }

    /// Window bookkeeping for one impacted subscription after a routed
    /// write: track the row's arrival/departure in the window, evict past
    /// capacity, ask for a refill when a removal drained the buffer to
    /// the user's limit, and republish the boundary. A no-op for
    /// subscriptions without a window.
    pub(super) fn maintain_window(
        &mut self,
        sub: SubId,
        key: &DataFrameKey,
        row_image: Option<&DataFrameRow>,
        matches_after: bool,
        present_before: bool,
    ) -> Vec<SingleTableUpdate> {
        let drained = {
            let Some(window) = self.windows.get_mut(&sub) else {
                return Vec::new();
            };
            if present_before {
                window.remove(key);
            }
            if matches_after {
                let row = row_image.expect("matches_after implies a row image");
                let value = window.order_value(row);
                window.insert(value, key.clone());
            }
            present_before && !matches_after && window.needs_refill()
        };
        let table = match self.select_queries.get(&sub) {
            Some(query) => query.table.clone(),
            None => return Vec::new(),
        };
        let ops = self.evict_overflow(sub);
        if drained {
            self.refill(sub);
        }
        self.sync_boundary(sub);
        ops.into_iter()
            .map(|op| SingleTableUpdate {
                query: sub,
                table: table.clone(),
                op,
            })
            .collect()
    }

    /// Narrow one windowed subscription's raw operations to the client's
    /// view: the difference between the prefix delivered last time and the
    /// best-`L` prefix now, plus the raw operations of a row that stays in
    /// the prefix (a rewrite's `Delete` + `Add` pair, which the join layer
    /// diffs; see the module header). A subscription without a window
    /// passes its operations through.
    pub(super) fn gate_window(
        &mut self,
        sub: SubId,
        raw: Vec<DataFrameOperation>,
    ) -> Vec<DataFrameOperation> {
        let (previous, current) = match self.windows.get_mut(&sub) {
            Some(window) => window.take_shown(),
            None => return raw,
        };
        let frame = self
            .select_queries
            .get(&sub)
            .and_then(|query| self.frames.get(&query.table));
        let latest = |key: &DataFrameKey, adds: bool| -> Option<DataFrameRow> {
            raw.iter()
                .rev()
                .find_map(|op| match (op, adds) {
                    (DataFrameOperation::Add(candidate, row), true)
                    | (DataFrameOperation::Delete(candidate, row), false)
                        if candidate == key =>
                    {
                        Some(row.clone())
                    }
                    _ => None,
                })
                .or_else(|| {
                    frame
                        .and_then(|frame| frame.get(key))
                        .map(|row| row.data.clone())
                })
        };
        let mut out = Vec::new();
        for key in previous.iter().filter(|key| !current.contains(key)) {
            if let Some(image) = latest(key, false) {
                out.push(DataFrameOperation::Delete(key.clone(), image));
            }
        }
        for key in &current {
            if !previous.contains(key) {
                if let Some(image) = latest(key, true) {
                    out.push(DataFrameOperation::Add(key.clone(), image));
                }
            } else {
                out.extend(raw.iter().filter(|op| op.key() == key).cloned());
            }
        }
        out
    }

    /// Gate the windowed subscriptions among `subs` in `updates`: their
    /// raw operations are replaced by [`SingleTableIVM::gate_window`]'s
    /// result, every other update passes through in order.
    pub(super) fn gate_updates(
        &mut self,
        subs: &[SubId],
        updates: Vec<SingleTableUpdate>,
    ) -> Vec<SingleTableUpdate> {
        let windowed: Vec<SubId> = subs
            .iter()
            .copied()
            .filter(|sub| self.windows.contains_key(sub))
            .fold(Vec::new(), |mut seen, sub| {
                if !seen.contains(&sub) {
                    seen.push(sub);
                }
                seen
            });
        if windowed.is_empty() {
            return updates;
        }
        let mut out = Vec::new();
        let mut raw: Vec<(SubId, TableName, Vec<DataFrameOperation>)> = windowed
            .iter()
            .filter_map(|sub| {
                self.select_queries
                    .get(sub)
                    .map(|query| (*sub, query.table.clone(), Vec::new()))
            })
            .collect();
        for update in updates {
            match raw.iter_mut().find(|(sub, _, _)| *sub == update.query) {
                Some((_, _, ops)) => ops.push(update.op),
                None => out.push(update),
            }
        }
        for (sub, table, ops) in raw {
            for op in self.gate_window(sub, ops) {
                out.push(SingleTableUpdate {
                    query: sub,
                    table: table.clone(),
                    op,
                });
            }
        }
        out
    }
}
