//! ORDER BY / LIMIT maintenance: the per-subscription window state and
//! the engine hooks that keep it — and its **boundary condition** in the
//! routing index — in lockstep with the held rows.
//!
//! A query with a finite `LIMIT L` is maintained as a **doubled buffer**:
//! storage queries are issued with `2·L`, and the subscription keeps a
//! [`Window`] — its held rows' order values, best → worst — from which
//! three behaviors derive:
//!
//! - **admission boundary**: once the buffer is full, the strict
//!   condition "better than the worst held value" (`<` for ASC, `>` for
//!   DESC). It is *pushed into the table's routing index*
//!   ([`SingleTableIVM::sync_boundary`]) whenever it changes, so
//!   `matched()` itself drops candidates the boundary rejects — a write
//!   worse than every held row never even reaches the engine, and rows
//!   are never admitted unboundedly. The boundary gates **admission
//!   only**: a row the subscription already holds is exempt (it keeps its
//!   slot when its value worsens, until a better arrival evicts it).
//! - **eviction** ([`SingleTableIVM::evict_overflow`]): rows past
//!   capacity are untagged worst-first, each emitting its `Delete`.
//! - **refill** ([`SingleTableIVM::refill`]): when a removal drains the
//!   buffer to `L`, one storage query fetches the rows strictly beyond
//!   the worst held value, back up to capacity.
//!
//! Rows are ordered with [`order_cmp`], a total order extending
//! [`crate::model::Value::compare`]: `NaN` sorts above every other
//! numeric and `Null` above everything (Postgres's convention that both
//! are "largest"), and remaining incomparable pairs fall back to a fixed
//! variant rank. A `Null`/`NaN` **boundary** is unenforceable as a
//! predicate (comparisons touching them are always false), so the window
//! then publishes no boundary rather than reject everything — admission
//! falls back to accept-then-evict, and refills run unthresholded
//! (re-fetched held rows dedup harmlessly). Ties for "worst" are broken
//! arbitrarily, like SQL. A `LIMIT 0` subscription publishes an
//! always-false boundary (`IN ()`), so it stays permanently empty.

use std::cmp::Ordering;

use super::{QueryId, SingleTableIVM, SingleTableUpdate};
use crate::model::{
    ColumnName, ComparisonOperator, Condition, DataFrameKey, DataFrameOperation, Order,
    SingleTableReadQuery, Value, Where,
};

/// The ORDER BY / LIMIT state of one subscription: its held rows' order
/// values, best → worst — the map from order-column values to row keys
/// that yields the boundary (worst value) on every change.
pub(super) struct Window {
    pub(super) column: ColumnName,
    ascending: bool,
    user_limit: usize,
    entries: Vec<(Value, DataFrameKey)>,
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
        })
    }

    /// The buffer capacity: twice the user's limit.
    fn capacity(&self) -> usize {
        self.user_limit * 2
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

    /// Drop one row from the window; reports whether it was present.
    pub(super) fn remove(&mut self, key: &DataFrameKey) -> bool {
        match self.entries.iter().position(|(_, existing)| existing == key) {
            Some(index) => {
                self.entries.remove(index);
                true
            }
            None => false,
        }
    }

    /// The worst held row past capacity, removed from the window — the
    /// engine untags it and emits its `Delete`.
    pub(super) fn pop_overflow(&mut self) -> Option<DataFrameKey> {
        if self.entries.len() > self.capacity() {
            self.entries.pop().map(|(_, key)| key)
        } else {
            None
        }
    }

    /// The admission boundary once the buffer is full: strictly better
    /// than the worst held value (`<` for ASC, `>` for DESC). Absent
    /// while the buffer has room, and absent for an unenforceable
    /// (`Null`/`NaN`) worst value.
    pub(super) fn boundary_condition(&self) -> Option<Condition> {
        if self.entries.len() < self.capacity() {
            return None;
        }
        let (boundary, _) = self.entries.last()?;
        if !enforceable(boundary) {
            return None;
        }
        let operator = if self.ascending {
            ComparisonOperator::LT
        } else {
            ComparisonOperator::GT
        };
        Some(Condition::new(self.column.clone(), operator, boundary.clone()))
    }

    /// The refill filter: strictly beyond the worst held value (`>` for
    /// ASC, `<` for DESC); `None` when the window is empty or the worst is
    /// unenforceable (`Null`/`NaN`) — the refill then runs unconstrained
    /// and re-fetched held rows dedup harmlessly.
    pub(super) fn refill_threshold(&self) -> Option<Condition> {
        let (boundary, _) = self.entries.last()?;
        if !enforceable(boundary) {
            return None;
        }
        let operator = if self.ascending {
            ComparisonOperator::GT
        } else {
            ComparisonOperator::LT
        };
        Some(Condition::new(self.column.clone(), operator, boundary.clone()))
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
        Value::Map(_) => 6,
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

    /// (Re)derive a subscription's window from the rows it currently
    /// holds; queries without a finite positive limit carry no window.
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
                    let value = row
                        .data
                        .get(window.column.as_str())
                        .cloned()
                        .unwrap_or(Value::Null);
                    window.insert(value, key.clone());
                }
            }
        }
        self.windows.insert(QueryId::from(query_uuid), window);
    }

    /// Untag worst-held rows until the subscription is back at buffer
    /// capacity, returning their `Delete`s.
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

    /// Refill a drained buffer back to capacity: one storage query for the
    /// rows strictly beyond the worst held value (unthresholded when the
    /// window is empty or the worst is unenforceable), ordered, limited to
    /// what is missing, tagged in and returned as `Add`s.
    fn refill(&mut self, query_uuid: &str) -> Vec<DataFrameOperation> {
        let Some(query) = self.select_queries.get(query_uuid).cloned() else {
            return Vec::new();
        };
        let Some((missing, threshold)) = self
            .windows
            .get(query_uuid)
            .map(|window| (window.missing(), window.refill_threshold()))
        else {
            return Vec::new();
        };
        if missing == 0 {
            return Vec::new();
        }
        let mut parts = vec![query.filter.clone()];
        parts.extend(threshold.map(Where::Condition));
        let refill_query = SingleTableReadQuery {
            table: query.table.clone(),
            filter: Where::AND(parts),
            order_by: query.order_by.clone(),
            limit: missing,
        };
        self.stats.window_refills += 1;
        let records = self.storage.select(&refill_query);
        let mut ops = Vec::new();
        for (key, row) in records {
            if let Some(op) = self.upsert_row(query_uuid, &key, &row) {
                ops.push(op);
            }
            if let Some(window) = self.windows.get_mut(query_uuid) {
                let value = row
                    .data
                    .get(window.column.as_str())
                    .cloned()
                    .unwrap_or(Value::Null);
                window.insert(value, key);
            }
        }
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
        row_image: Option<&crate::model::DataFrameRow>,
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
                let value = row
                    .data
                    .get(window.column.as_str())
                    .cloned()
                    .unwrap_or(Value::Null);
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
