//! Frame surgery and inspection: tagging rows into and out of the shared
//! per-table frames ([`crate::model::frame::TableFrame`]) for one
//! subscription at a time — the row-level seams the join layer maintains
//! its driven parts with — plus the storage-read seam (asking for rows,
//! landing them) and the read-only views tests and callers inspect. Tags
//! and the held index store compact ids ([`crate::model::SubId`],
//! [`crate::model::frame::RowId`]) and are always changed together.

use std::collections::HashMap;

use super::predicate::evaluate;
use super::{Fetch, FetchId, FetchKind, SingleTableIVM, SingleTableUpdate, window};
use crate::model::{
    ComparisonOperator, Condition, DataFrameKey, DataFrameOperation, DataFrameRow,
    SingleTableReadQuery, SubId, TableName, Value, Where,
};

impl SingleTableIVM {
    /// Ask for the rows of subscription `sub` matching its own filter
    /// narrowed to `column IN values`: one recorded storage read, landed
    /// later through [`SingleTableIVM::land_fetch`]. Until it lands the
    /// subscription publishes no admission boundary and donates no twin
    /// snapshot. Unknown subscriptions are a no-op.
    pub fn fetch(&mut self, sub: SubId, column: &str, values: &[Value]) {
        let Some(query) = self.select_queries.get(&sub) else {
            return;
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
        self.issue(sub, narrowed, FetchKind::Narrowed);
        self.sync_boundary(sub);
    }

    /// Record one storage read for `sub`, counting it as pending; a read
    /// for no rows at all (`LIMIT 0`) is not worth a round trip and is
    /// dropped.
    pub(super) fn issue(&mut self, sub: SubId, query: SingleTableReadQuery, kind: FetchKind) {
        if query.limit == 0 {
            return;
        }
        let id = FetchId(self.next_fetch);
        self.next_fetch += 1;
        *self.pending.entry(sub).or_default() += 1;
        self.stats.storage_reads += 1;
        self.requests.push(Fetch {
            id,
            sub,
            kind,
            query,
        });
    }

    /// Land the rows a recorded read returned for the read's subscription.
    /// The runtime has brought them up to the engine's position, so each
    /// is current: a row the frame does not hold is adopted with the
    /// read's image, a row it holds keeps the frame's (equal) image, and
    /// the subscription is tagged onto it if it was not already (rows the
    /// stream routed to it while the read was out already are). The
    /// window's frontier is sized from the whole result, overflow is
    /// evicted, a refill is asked for if the window drained while the read
    /// was out, and the boundary is republished. Returns the
    /// subscription's `Add`s and evictions; no other subscription is
    /// touched. A read for a subscription that is gone lands as nothing.
    pub fn land_fetch(
        &mut self,
        fetch: &Fetch,
        rows: &[(DataFrameKey, DataFrameRow)],
    ) -> Vec<SingleTableUpdate> {
        let sub = fetch.sub;
        let Some(query) = self.select_queries.get(&sub).cloned() else {
            return Vec::new();
        };
        let table = query.table.clone();
        let mut updates = Vec::new();
        for (key, row) in rows {
            updates.extend(self.land_row(sub, &table, &query.filter, key, row));
        }
        if let Some(window) = self.windows.get_mut(&sub) {
            window.note_fetch(fetch.query.limit as usize, rows);
        }
        let outstanding = match self.pending.get_mut(&sub) {
            Some(count) => {
                *count = count.saturating_sub(1);
                *count
            }
            None => 0,
        };
        if outstanding == 0 {
            self.pending.remove(&sub);
        }
        let evictions = self.evict_overflow(sub);
        updates.extend(self.tagged(sub, evictions));
        if outstanding == 0
            && self
                .windows
                .get(&sub)
                .is_some_and(super::window::Window::needs_refill)
        {
            self.refill(sub);
        }
        self.sync_boundary(sub);
        updates
    }

    /// Adopt one landed row for `sub`, whose filter is `filter`: the frame
    /// row is materialized with the read's image if absent (the frame's
    /// image is current, and so is the read's, so an existing row keeps
    /// what it has), and `sub` is tagged onto it and sent the `Add` unless
    /// it already held it or the image does not satisfy its filter.
    fn land_row(
        &mut self,
        sub: SubId,
        table: &TableName,
        filter: &Where,
        key: &DataFrameKey,
        row: &DataFrameRow,
    ) -> Vec<SingleTableUpdate> {
        let frame = self.frames.entry(table.clone()).or_default();
        let (id, shared) = frame.entry(key, || row.clone());
        debug_assert!(
            shared.data == *row,
            "a read brought up to the engine's position agrees with the frame"
        );
        if shared.held_by(sub) {
            return Vec::new();
        }
        let image = shared.data.clone();
        if !evaluate(filter, &image.data, &mut 0) {
            frame.drop_if_unheld(id);
            return Vec::new();
        }
        let mut updates = Vec::new();
        if let Some(update) = self.tag_row(sub, table, key, &image) {
            updates.push(update);
            self.track_landed(sub, key, &image);
        }
        updates
    }

    /// Tag `sub` onto the row `key` of `table` that `from` holds (the twin
    /// path) and return the `Add` with the frame's image, or `None` when
    /// `sub` already holds the row or `from` does not.
    pub(super) fn share_row(
        &mut self,
        sub: SubId,
        from: SubId,
        table: &TableName,
        key: &DataFrameKey,
    ) -> Option<SingleTableUpdate> {
        let frame = self.frames.get(table)?;
        let row = frame.get(key)?;
        if !row.held_by(from) {
            return None;
        }
        let image = row.data.clone();
        self.tag_row(sub, table, key, &image)
    }

    /// Record a landed row in `sub`'s window, if it has one.
    fn track_landed(&mut self, sub: SubId, key: &DataFrameKey, image: &DataFrameRow) {
        if let Some(window) = self.windows.get_mut(&sub) {
            let value = window.order_value(image);
            window.insert(value, key.clone());
        }
    }

    /// Untag one row from `sub` (dropping it when nobody holds it).
    /// Returns the `Delete` — carrying the image `sub` held — to forward,
    /// or `None` if the subscription did not hold it.
    pub fn remove_row(&mut self, sub: SubId, key: &DataFrameKey) -> Option<DataFrameOperation> {
        let table = self.select_queries.get(&sub)?.table.clone();
        let frame = self.frames.get_mut(&table)?;
        let id = frame.id_of(key)?;
        let row = frame.row_mut(id)?;
        if !row.subscribers.remove(&sub) {
            return None;
        }
        let removed = row.data.clone();
        if let Some(ids) = self.held.get_mut(&sub) {
            ids.remove(&id);
        }
        frame.drop_if_unheld(id);
        if let Some(window) = self.windows.get_mut(&sub) {
            window.remove(key);
        }
        Some(DataFrameOperation::Delete(key.clone(), removed))
    }

    /// Untag every row `sub` holds whose `column` equals one of `values`.
    /// Returns the `Delete` operations.
    pub fn delete_rows(
        &mut self,
        sub: SubId,
        column: &str,
        values: &[Value],
    ) -> Vec<DataFrameOperation> {
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
    pub fn rows_matching(
        &self,
        sub: SubId,
        column: &str,
        value: &Value,
    ) -> Vec<(DataFrameKey, DataFrameRow)> {
        self.rows_matching_any(sub, column, std::slice::from_ref(value))
    }

    /// The rows `sub` holds whose `column` equals one of `values` — walked
    /// off the subscription's held index, so cost scales with its own view,
    /// not the table.
    fn rows_matching_any(
        &self,
        sub: SubId,
        column: &str,
        values: &[Value],
    ) -> Vec<(DataFrameKey, DataFrameRow)> {
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
            .filter(|row| {
                row.data
                    .data
                    .get(column)
                    .is_some_and(|v| values.contains(v))
            })
            .map(|row| (row.key.clone(), row.data.clone()))
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
                    view.insert(row.key.clone(), row.data.clone());
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
