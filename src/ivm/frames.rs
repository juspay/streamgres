//! Frame surgery and inspection: tagging rows into and out of the shared
//! per-table frames ([`crate::model::frame::TableFrame`]) for one
//! subscription at a time — the row-level seams the join layer maintains
//! its sub parts with — plus the read-only views tests and callers
//! inspect.

use std::collections::{BTreeSet, HashMap};

use super::{window, QueryId, SingleTableIVM};
use crate::model::frame::SharedRow;
use crate::model::{
    ComparisonOperator, Condition, DataFrameKey, DataFrameOperation, DataFrameRow,
    SingleTableReadQuery, TableName, Value, Where,
};

impl SingleTableIVM {
    /// Pull rows for `query_uuid` from storage — the subscription's own
    /// filter narrowed to `column IN values` — and tag them into the
    /// shared frame. Returns the `Add` operations for rows this
    /// subscription gained or whose data changed (rows it already holds
    /// identically produce none), plus the eviction `Delete`s a windowed
    /// subscription emits when the fetched rows push it past capacity.
    pub fn fetch(
        &mut self,
        query_uuid: &str,
        column: &str,
        values: &[Value],
    ) -> Vec<DataFrameOperation> {
        let Some(query) = self.select_queries.get(query_uuid) else {
            return Vec::new();
        };
        let narrowed = SingleTableReadQuery {
            filter: Where::AND(vec![
                query.filter.clone(),
                Where::Condition(Condition::new(
                    column,
                    ComparisonOperator::IN,
                    Value::List(values.to_vec()),
                )),
            ]),
            limit: window::storage_limit(query),
            ..query.clone()
        };
        let records = self.storage.select(&narrowed);
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
        ops.extend(self.evict_overflow(query_uuid));
        self.sync_boundary(query_uuid);
        ops
    }

    /// Tag one storage row into the shared frame for `query_uuid`,
    /// writing its data (shared across holders). Returns the `Add` to
    /// forward — a bare `Add` even when it overwrites held data, since
    /// receivers apply `Add` as insert-or-replace — or `None` when the
    /// subscription already holds the row with identical data (or is
    /// unknown).
    pub fn upsert_row(
        &mut self,
        query_uuid: &str,
        key: &DataFrameKey,
        row: &DataFrameRow,
    ) -> Option<DataFrameOperation> {
        let query = self.select_queries.get(query_uuid)?;
        let table = query.table.clone();
        let frame = self.frames.entry(table).or_default();
        let shared = frame.rows.entry(key.clone()).or_insert_with(|| SharedRow {
            data: row.data.clone(),
            subscribers: BTreeSet::new(),
        });
        let was_held = shared.subscribers.contains(query_uuid);
        let changed = shared.data != row.data;
        shared.data = row.data.clone();
        shared.subscribers.insert(QueryId::from(query_uuid));
        self.held
            .entry(QueryId::from(query_uuid))
            .or_default()
            .insert(key.clone());
        if was_held && !changed {
            return None;
        }
        Some(DataFrameOperation::Add(key.clone(), row.clone()))
    }

    /// Untag one row from `query_uuid` (dropping it when nobody holds it).
    /// Returns the `Delete` — carrying the removed image — to forward, or
    /// `None` if the subscription did not hold it.
    pub fn remove_row(
        &mut self,
        query_uuid: &str,
        key: &DataFrameKey,
    ) -> Option<DataFrameOperation> {
        let table = self.select_queries.get(query_uuid)?.table.clone();
        let frame = self.frames.get_mut(&table)?;
        let row = frame.rows.get_mut(key)?;
        if !row.subscribers.remove(query_uuid) {
            return None;
        }
        if let Some(keys) = self.held.get_mut(query_uuid) {
            keys.remove(key);
        }
        let data = row.data.clone();
        if row.subscribers.is_empty() {
            frame.rows.remove(key);
        }
        if let Some(window) = self.windows.get_mut(query_uuid) {
            window.remove(key);
        }
        Some(DataFrameOperation::Delete(
            key.clone(),
            DataFrameRow { data },
        ))
    }

    /// Untag every row `query_uuid` holds whose `column` equals one of
    /// `values`. Returns the `Delete` operations.
    pub fn delete_rows(
        &mut self,
        query_uuid: &str,
        column: &str,
        values: &[Value],
    ) -> Vec<DataFrameOperation> {
        let doomed: Vec<DataFrameKey> = self
            .rows_matching_any(query_uuid, column, values)
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        let mut ops = Vec::new();
        for key in doomed {
            if let Some(op) = self.remove_row(query_uuid, &key) {
                ops.push(op);
            }
        }
        ops
    }

    /// The rows `query_uuid` holds whose `column` equals `value`.
    pub fn rows_matching(
        &self,
        query_uuid: &str,
        column: &str,
        value: &Value,
    ) -> Vec<(DataFrameKey, DataFrameRow)> {
        self.rows_matching_any(query_uuid, column, std::slice::from_ref(value))
    }

    /// The rows `query_uuid` holds whose `column` equals one of `values` —
    /// walked off the subscription's held-key index, so cost scales with
    /// its own view, not the table.
    fn rows_matching_any(
        &self,
        query_uuid: &str,
        column: &str,
        values: &[Value],
    ) -> Vec<(DataFrameKey, DataFrameRow)> {
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
            .filter_map(|key| frame.rows.get(key).map(|row| (key, row)))
            .filter(|(_, row)| row.data.get(column).is_some_and(|v| values.contains(v)))
            .map(|(key, row)| {
                (
                    key.clone(),
                    DataFrameRow {
                        data: row.data.clone(),
                    },
                )
            })
            .collect()
    }

    /// The subscription's current view — every shared row it holds, as
    /// key → image, enumerated from its held-key index. An inspection seam
    /// for tests and debugging, not a sync mechanism: clients build their
    /// frames from the operation stream. `None` for unknown uuids.
    pub fn rows_for(&self, query_uuid: &str) -> Option<HashMap<DataFrameKey, DataFrameRow>> {
        let query = self.select_queries.get(query_uuid)?;
        let frame = self.frames.get(&query.table);
        let mut view = HashMap::new();
        if let Some(keys) = self.held.get(query_uuid) {
            for key in keys {
                if let Some(row) = frame.and_then(|frame| frame.rows.get(key)) {
                    view.insert(
                        key.clone(),
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
    pub fn holders_of(&self, table: &TableName, key: &DataFrameKey) -> Vec<QueryId> {
        self.frames
            .get(table)
            .and_then(|frame| frame.rows.get(key))
            .map(|row| row.subscribers.iter().cloned().collect())
            .unwrap_or_default()
    }
}
