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
//!   buffer to `L`, one storage query fetches from the frontier
//!   *inclusive* (`>=` for ASC, `<=` for DESC), sized for the missing rows
//!   plus the held rows the threshold returns again (they dedup on
//!   upsert), back up to capacity; the frontier then moves to the worst
//!   value fetched. Anchoring both the boundary and the refill at the
//!   frontier rather than at the worst *held* row is what keeps the
//!   top-`L` exact: a row admitted while the buffer had room can never
//!   push the refill threshold past storage rows that were never fetched,
//!   and rows tying the frontier stay reachable.
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

use super::{QueryId, SingleTableIVM, SingleTableUpdate};
use crate::model::{
    ColumnName, ComparisonOperator, Condition, DataFrameKey, DataFrameOperation, DataFrameRow,
    Order, SingleTableReadQuery, Value, Where,
};

/// The ORDER BY / LIMIT state of one subscription: its held rows' order
/// values, best → worst, and the storage frontier the boundary and the
/// refill threshold are anchored at.
pub(super) struct Window {
    pub(super) column: ColumnName,
    ascending: bool,
    user_limit: usize,
    entries: Vec<(Value, DataFrameKey)>,
    frontier: Option<Value>,
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
            column: query.order_by.column.clone(),
            ascending: matches!(query.order_by.direction, Order::ASC),
            user_limit: query.limit as usize,
            entries: Vec::new(),
            frontier: None,
        })
    }

    /// The buffer capacity: twice the user's limit.
    fn capacity(&self) -> usize {
        self.user_limit * 2
    }

    /// The order value of a row image (`Null` when the column is absent).
    pub(super) fn order_value(&self, row: &DataFrameRow) -> Value {
        row.data
            .get(self.column.as_str())
            .cloned()
            .unwrap_or(Value::Null)
    }

    /// Insert one row's order value, keeping best → worst order; ties land
    /// after their equals (stable). Idempotent: a key already present is
    /// re-inserted at its new value's position, never duplicated.
    pub(super) fn insert(&mut self, value: Value, key: DataFrameKey) {
        self.remove(&key);
        let ascending = self.ascending;
        let position = self
            .entries
            .iter()
            .position(|(existing, _)| is_worse(existing, &value, ascending))
            .unwrap_or(self.entries.len());
        self.entries.insert(position, (value, key));
    }

    /// Drop one row from the window; reports whether it was present. The
    /// frontier is untouched: a removal does not change what storage
    /// covers.
    pub(super) fn remove(&mut self, key: &DataFrameKey) -> bool {
        match self.entries.iter().position(|(_, existing)| existing == key) {
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
    fn cover(&mut self, value: Value) {
        let keep = self
            .frontier
            .as_ref()
            .is_some_and(|frontier| !is_worse(frontier, &value, self.ascending));
        if !keep {
            self.frontier = Some(value);
        }
    }

    /// The worst order value among fetched rows.
    fn worst_of(&self, fetched: &[(DataFrameKey, DataFrameRow)]) -> Option<Value> {
        fetched
            .iter()
            .map(|(_, row)| self.order_value(row))
            .reduce(|worst, value| {
                if is_worse(&value, &worst, self.ascending) {
                    value
                } else {
                    worst
                }
            })
    }

    /// Record a storage read of the subscription's whole filter that asked
    /// for `requested` rows: a full result sets the frontier to the worst
    /// value fetched (storage may hold more beyond it); a short result
    /// clears it (every matching row is now held).
    pub(super) fn note_fetch(&mut self, requested: usize, fetched: &[(DataFrameKey, DataFrameRow)]) {
        self.frontier = if fetched.len() >= requested {
            self.worst_of(fetched)
        } else {
            None
        };
    }

    /// Record a storage read narrowed to a subset of the filter: a full
    /// result proves rows beyond its worst value exist unheld, so the
    /// frontier pulls in to it; a short result says nothing about the
    /// rest of the filter.
    pub(super) fn note_narrowed_fetch(
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
    pub(super) fn frontier(&self) -> Option<Value> {
        self.frontier.clone()
    }

    /// Adopt a frontier wholesale (a twin's, whose held rows this window
    /// was just rebuilt from).
    pub(super) fn set_frontier(&mut self, frontier: Option<Value>) {
        self.frontier = frontier;
    }

    /// The admission boundary: strictly better than the frontier (`<` for
    /// ASC, `>` for DESC). Absent while storage is exhausted, and absent
    /// for an unenforceable (`Null`/`NaN`) frontier.
    pub(super) fn boundary_condition(&self) -> Option<Condition> {
        let frontier = self.frontier.as_ref()?;
        if !enforceable(frontier) {
            return None;
        }
        let operator = if self.ascending {
            ComparisonOperator::LT
        } else {
            ComparisonOperator::GT
        };
        Some(Condition::new(
            self.column.clone(),
            operator,
            frontier.clone(),
        ))
    }

    /// The refill read as (limit, threshold): `None` when storage is
    /// exhausted (nothing left to fetch). Otherwise the threshold is the
    /// frontier inclusive (`>=` for ASC, `<=` for DESC) and the limit is
    /// the missing count plus the held rows at or beyond the frontier,
    /// which the threshold returns again and the upsert dedups; an
    /// unenforceable frontier yields an unthresholded read sized to
    /// capacity.
    pub(super) fn refill_plan(&self) -> Option<(u32, Option<Condition>)> {
        let frontier = self.frontier.as_ref()?;
        if !enforceable(frontier) {
            return Some((self.capacity() as u32, None));
        }
        let operator = if self.ascending {
            ComparisonOperator::GTE
        } else {
            ComparisonOperator::LTE
        };
        let held_beyond = self
            .entries
            .iter()
            .filter(|(value, _)| !is_worse(frontier, value, self.ascending))
            .count();
        let limit = (self.missing() as usize + held_beyond) as u32;
        let threshold = Condition::new(self.column.clone(), operator, frontier.clone());
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

/// Whether a boundary value can be enforced as a predicate condition —
/// comparisons touching `Null` or `NaN` are always false, so conditions
/// built from them would reject or fetch nothing.
fn enforceable(value: &Value) -> bool {
    !value.is_null() && !matches!(value, Value::Float(f) if f.is_nan())
}

/// Whether `existing` sorts strictly worse than `candidate` for the given
/// direction (worse = greater for ASC, less for DESC).
fn is_worse(existing: &Value, candidate: &Value, ascending: bool) -> bool {
    let ordering = order_cmp(existing, candidate);
    if ascending {
        ordering == Ordering::Greater
    } else {
        ordering == Ordering::Less
    }
}

/// Total order over [`Value`]s for window and storage sorting: delegates
/// to [`Value::compare`] where defined; `NaN` sorts above every other
/// numeric (all `NaN`s equal) and `Null` above everything — Postgres's
/// convention — and remaining incomparable pairs fall back to a fixed
/// variant rank. Deterministic and transitive, if semantically arbitrary
/// across types.
pub(super) fn order_cmp(a: &Value, b: &Value) -> Ordering {
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
    /// `IN ()`.
    pub(super) fn sync_boundary(&mut self, query_uuid: &str) {
        let Some(query) = self.select_queries.get(query_uuid) else {
            return;
        };
        let table = query.table.clone();
        let boundary = if query.limit == 0 {
            Some(Condition::new(
                query.order_by.column.clone(),
                ComparisonOperator::IN,
                Value::List(Vec::new()),
            ))
        } else {
            self.windows
                .get(query_uuid)
                .and_then(|window| window.boundary_condition())
        };
        self.tables
            .entry(table)
            .or_default()
            .set_boundary(&QueryId::from(query_uuid), boundary);
    }

    /// (Re)derive a subscription's window entries from the rows it
    /// currently holds, with no frontier yet — the caller records the
    /// storage read or twin the rows came from; queries without a finite
    /// positive limit carry no window.
    pub(super) fn rebuild_window(&mut self, query_uuid: &str) {
        let Some(query) = self.select_queries.get(query_uuid) else {
            self.windows.remove(query_uuid);
            return;
        };
        let Some(mut window) = Window::for_query(query) else {
            self.windows.remove(query_uuid);
            return;
        };
        if let Some(keys) = self.held.get(query_uuid)
            && let Some(frame) = self.frames.get(&query.table)
        {
            for key in keys {
                if let Some(row) = frame.rows.get(key) {
                    let value = window.order_value(&DataFrameRow {
                        data: row.data.clone(),
                    });
                    window.insert(value, key.clone());
                }
            }
        }
        self.windows.insert(QueryId::from(query_uuid), window);
    }

    /// Untag worst-held rows until the subscription is back at buffer
    /// capacity, returning their `Delete`s; each eviction pulls the
    /// frontier in to the evicted value.
    pub(super) fn evict_overflow(&mut self, query_uuid: &str) -> Vec<DataFrameOperation> {
        let mut ops = Vec::new();
        while let Some(evicted) = self
            .windows
            .get_mut(query_uuid)
            .and_then(|window| window.pop_overflow())
        {
            self.stats.window_evictions += 1;
            if let Some(op) = self.remove_row(query_uuid, &evicted) {
                ops.push(op);
            }
        }
        ops
    }

    /// Refill a drained buffer back to capacity: one storage query from
    /// the frontier inclusive (unthresholded for an unenforceable one;
    /// nothing at all when storage is exhausted), ordered, sized by the
    /// window's refill plan, tagged in and returned as `Add`s, with the
    /// frontier moved to the worst value fetched and any overshoot
    /// evicted.
    fn refill(&mut self, query_uuid: &str) -> Vec<DataFrameOperation> {
        let Some(query) = self.select_queries.get(query_uuid).cloned() else {
            return Vec::new();
        };
        let Some((limit, threshold)) = self
            .windows
            .get(query_uuid)
            .and_then(Window::refill_plan)
        else {
            return Vec::new();
        };
        if limit == 0 {
            return Vec::new();
        }
        let mut parts = vec![query.filter.clone()];
        parts.extend(threshold.map(Where::Condition));
        let refill_query = SingleTableReadQuery {
            table: query.table.clone(),
            filter: Where::AND(parts),
            order_by: query.order_by.clone(),
            limit,
        };
        self.stats.window_refills += 1;
        let records = self.storage.select(&refill_query);
        let mut ops = Vec::new();
        for (key, row) in &records {
            if let Some(op) = self.upsert_row(query_uuid, key, row) {
                ops.push(op);
            }
            if let Some(window) = self.windows.get_mut(query_uuid) {
                let value = window.order_value(row);
                window.insert(value, key.clone());
            }
        }
        if let Some(window) = self.windows.get_mut(query_uuid) {
            window.note_fetch(limit as usize, &records);
        }
        ops.extend(self.evict_overflow(query_uuid));
        ops
    }

    /// Window bookkeeping for one impacted subscription after a routed
    /// write: track the row's arrival/departure in the window, evict past
    /// capacity, refill when a removal drained the buffer to the user's
    /// limit, and republish the boundary. A no-op for subscriptions
    /// without a window.
    pub(super) fn maintain_window(
        &mut self,
        uuid: &QueryId,
        key: &DataFrameKey,
        row_image: Option<&DataFrameRow>,
        matches_after: bool,
        present_before: bool,
    ) -> Vec<SingleTableUpdate> {
        let drained = {
            let Some(window) = self.windows.get_mut(uuid.as_str()) else {
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
        let table = match self.select_queries.get(uuid.as_str()) {
            Some(query) => query.table.clone(),
            None => return Vec::new(),
        };
        let mut ops = self.evict_overflow(uuid.as_str());
        if drained {
            ops.extend(self.refill(uuid.as_str()));
        }
        self.sync_boundary(uuid.as_str());
        ops.into_iter()
            .map(|op| SingleTableUpdate {
                query: uuid.clone(),
                table: table.clone(),
                op,
            })
            .collect()
    }
}
