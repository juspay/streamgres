//! Frame surgery and inspection: tagging rows into and out of the shared
//! per-table frames ([`crate::model::frame::TableFrame`]) for one
//! subscription at a time — the row-level seams the join layer maintains
//! its driven parts with — plus the read-only views tests and callers
//! inspect. Tags and the held index store compact ids
//! ([`crate::model::SubId`], [`crate::model::frame::RowId`]) and are
//! always changed together.

use std::collections::HashMap;

use super::{window, SingleTableIVM};
use crate::model::{
    ComparisonOperator, Condition, DataFrameKey, DataFrameOperation, DataFrameRow,
    SingleTableReadQuery, SubId, TableName, Value, Where,
};

impl SingleTableIVM {
    /// Pull rows for subscription `sub` from storage — its own filter
    /// narrowed to `column IN values` — and tag them into the shared frame.
    /// Returns the `Add` operations for rows the subscription gained or
    /// whose data changed (rows it already holds identically produce
    /// none), plus the eviction `Delete`s a windowed subscription emits
    /// when the fetched rows push it past capacity.
    pub fn fetch(&mut self, sub: SubId, column: &str, values: &[Value]) -> Vec<DataFrameOperation> {
        let Some(query) = self.select_queries.get(&sub) else {
            return Vec::new();
        };
        let limit = window::storage_limit(query);
        let narrowed = SingleTableReadQuery {
            filter: Where::AND(vec![
                query.filter.clone(),
                Where::Condition(Condition::new(
                    column,
                    ComparisonOperator::IN,
                    Value::List(values.to_vec()),
                )),
            ]),
            limit,
            ..query.clone()
        };
        let records = self.storage.select(&narrowed);
        let mut ops = Vec::new();
        for (key, row) in &records {
            if let Some(op) = self.upsert_row(sub, key, row) {
                ops.push(op);
            }
            if let Some(window) = self.windows.get_mut(&sub) {
                let value = window.order_value(row);
                window.insert(value, key.clone());
            }
        }
        if let Some(window) = self.windows.get_mut(&sub) {
            window.note_narrowed_fetch(limit as usize, &records);
        }
        ops.extend(self.evict_overflow(sub));
        self.sync_boundary(sub);
        ops
    }

    /// Tag one storage row into the shared frame for `sub`, writing its
    /// data (shared across holders). Returns the `Add` to forward — a bare
    /// `Add` even when it overwrites held data, since receivers apply
    /// `Add` as insert-or-replace — or `None` when the subscription
    /// already holds the row with identical data (or is unknown).
    pub fn upsert_row(
        &mut self,
        sub: SubId,
        key: &DataFrameKey,
        row: &DataFrameRow,
    ) -> Option<DataFrameOperation> {
        let query = self.select_queries.get(&sub)?;
        let table = query.table.clone();
        let frame = self.frames.entry(table).or_default();
        let (id, shared) = frame.entry(key, || row.data.clone());
        let was_held = shared.subscribers.contains(&sub);
        let changed = shared.data != row.data;
        shared.data = row.data.clone();
        shared.subscribers.insert(sub);
        self.held.entry(sub).or_default().insert(id);
        if was_held && !changed {
            return None;
        }
        Some(DataFrameOperation::Add(key.clone(), row.clone()))
    }

    /// Untag one row from `sub` (dropping it when nobody holds it).
    /// Returns the `Delete` — carrying the removed image — to forward, or
    /// `None` if the subscription did not hold it.
    pub fn remove_row(&mut self, sub: SubId, key: &DataFrameKey) -> Option<DataFrameOperation> {
        let table = self.select_queries.get(&sub)?.table.clone();
        let frame = self.frames.get_mut(&table)?;
        let id = frame.id_of(key)?;
        let row = frame.row_mut(id)?;
        if !row.subscribers.remove(&sub) {
            return None;
        }
        let data = row.data.clone();
        if let Some(ids) = self.held.get_mut(&sub) {
            ids.remove(&id);
        }
        frame.drop_if_unheld(id);
        if let Some(window) = self.windows.get_mut(&sub) {
            window.remove(key);
        }
        Some(DataFrameOperation::Delete(key.clone(), DataFrameRow { data }))
    }

    /// Untag every row `sub` holds whose `column` equals one of `values`.
    /// Returns the `Delete` operations.
    pub fn delete_rows(&mut self, sub: SubId, column: &str, values: &[Value]) -> Vec<DataFrameOperation> {
        let doomed: Vec<DataFrameKey> = self
            .rows_matching_any(sub, column, values)
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        let mut ops = Vec::new();
        for key in doomed {
            if let Some(op) = self.remove_row(sub, &key) {
                ops.push(op);
            }
        }
        ops
    }

    /// The rows `sub` holds whose `column` equals `value`.
    pub fn rows_matching(&self, sub: SubId, column: &str, value: &Value) -> Vec<(DataFrameKey, DataFrameRow)> {
        self.rows_matching_any(sub, column, std::slice::from_ref(value))
    }

    /// The rows `sub` holds whose `column` equals one of `values` — walked
    /// off the subscription's held index, so cost scales with its own view,
    /// not the table.
    fn rows_matching_any(&self, sub: SubId, column: &str, values: &[Value]) -> Vec<(DataFrameKey, DataFrameRow)> {
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
            .filter(|row| row.data.get(column).is_some_and(|v| values.contains(v)))
            .map(|row| {
                (
                    row.key.clone(),
                    DataFrameRow {
                        data: row.data.clone(),
                    },
                )
            })
            .collect()
    }

    /// The subscription's current view — every shared row it holds, as
    /// key → image, enumerated from its held index. An inspection seam for
    /// tests and debugging, not a sync mechanism: clients build their
    /// frames from the operation stream. `None` for unknown subscriptions.
    pub fn rows_for(&self, sub: SubId) -> Option<HashMap<DataFrameKey, DataFrameRow>> {
        let query = self.select_queries.get(&sub)?;
        let frame = self.frames.get(&query.table);
        let mut view = HashMap::new();
        if let Some(ids) = self.held.get(&sub) {
            for id in ids {
                if let Some(row) = frame.and_then(|frame| frame.row(*id)) {
                    view.insert(
                        row.key.clone(),
                        DataFrameRow {
                            data: row.data.clone(),
                        },
                    );
                }
            }
        }
        Some(view)
    }

    /// The subscriptions currently holding one shared row — its subscriber
    /// tags, in sorted order; empty when the row is not materialized. The
    /// "which query sets is this row subscribed to" inspection view.
    pub fn holders_of(&self, table: &TableName, key: &DataFrameKey) -> Vec<SubId> {
        self.frames
            .get(table)
            .and_then(|frame| frame.get(key))
            .map(|row| row.subscribers.iter().copied().collect())
            .unwrap_or_default()
    }
}
