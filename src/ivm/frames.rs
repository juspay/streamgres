//! Frame surgery and inspection: tagging rows into and out of the shared
//! per-table frames ([`crate::model::frame::TableFrame`]) for one
//! subscription at a time — the row-level seams the join layer maintains
//! its driven parts with — plus the storage-read seam (asking for rows,
//! landing them) and the read-only views tests and callers inspect. Tags
//! and the held index store compact ids ([`crate::model::SubId`],
//! [`crate::model::frame::RowId`]) and are always changed together.

use std::collections::HashMap;

use super::predicate::evaluate;
use super::window::Window;
use super::{Fetch, FetchId, FetchKind, SingleTableIVM, SingleTableUpdate, window};
use crate::model::{
    ColumnName, ComparisonOperator, Condition, DataFrameKey, DataFrameOperation, DataFrameRow,
    SingleTableReadQuery, SubId, TableName, Value, Where,
};

impl SingleTableIVM {
    /// Ask for the rows of subscription `sub` matching its own filter
    /// narrowed to `column IN values`: one recorded storage read, landed
    /// later through [`SingleTableIVM::land_fetch`]. Until it lands the
    /// subscription publishes no admission boundary; the read serves every
    /// subscription of its query, twins registered meanwhile included. A
    /// set-valued `IN` leaf of the filter on the same column is replaced
    /// by the narrowed values in the read's filter: the values are members
    /// of that set (the caller inserted them just before), so the rows are
    /// the same, and the read renders a handful of literals instead of the
    /// whole set. Unknown subscriptions are a no-op.
    pub fn fetch(&mut self, sub: SubId, column: &str, values: &[Value]) {
        let Some(query) = self.select_queries.get(&sub) else {
            return;
        };
        let narrowed = Where::Condition(Condition::new(
            column,
            ComparisonOperator::IN,
            Value::List(values.to_vec()),
        ));
        let narrowed = SingleTableReadQuery {
            filter: Where::AND(vec![
                narrow_set_leaves(&query.filter, column, &narrowed),
                narrowed,
            ]),
            limit: window::storage_limit(query),
            ..query.clone()
        };
        self.issue(sub, narrowed, FetchKind::Narrowed);
        self.sync_boundary(sub);
    }

    /// Record one storage read for `sub`, counted as pending by `sub` and
    /// every subscription of its query, all of which it will land into; a
    /// read for no rows at all (`LIMIT 0`) is not worth a round trip and
    /// is dropped.
    pub(super) fn issue(&mut self, sub: SubId, query: SingleTableReadQuery, kind: FetchKind) {
        if query.limit == 0 {
            return;
        }
        let id = FetchId(self.next_fetch);
        self.next_fetch += 1;
        let readers = self.query_group(sub);
        for reader in &readers {
            *self.pending.entry(*reader).or_default() += 1;
        }
        self.readers.insert(id, readers);
        self.stats.storage_reads += 1;
        self.requests.push(Fetch {
            id,
            sub,
            kind,
            query: std::sync::Arc::new(query),
        });
    }

    /// `sub` and every other subscription with its query, in id order;
    /// `sub` alone if it is not registered.
    fn query_group(&self, sub: SubId) -> Vec<SubId> {
        self.select_queries
            .get(&sub)
            .and_then(|query| self.by_query.get(query))
            .map(|group| group.iter().copied().collect())
            .unwrap_or_else(|| vec![sub])
    }

    /// Land the rows a recorded read returned, for every subscription the
    /// read serves ([`SingleTableIVM::readers_of`]: the one that asked and
    /// its twins). The runtime has brought the rows up to the engine's
    /// position, so each is current: a row the frame does not hold is
    /// adopted with the read's image, a row it holds keeps the frame's
    /// (equal) image, and each subscription is tagged onto it if it was
    /// not already (rows the stream routed to it while the read was out
    /// already are). Per subscription, the window's frontier is sized
    /// from the whole result, overflow is evicted, a refill is asked for
    /// if the window drained while the read was out, and the boundary is
    /// republished. Returns those subscriptions' `Add`s and evictions; no
    /// other is touched. A read whose subscriptions are all gone lands as
    /// nothing.
    pub fn land_fetch(
        &mut self,
        fetch: &Fetch,
        rows: &[(DataFrameKey, DataFrameRow)],
    ) -> Vec<SingleTableUpdate> {
        let readers = self
            .readers
            .remove(&fetch.id)
            .unwrap_or_else(|| vec![fetch.sub]);
        let mut updates = Vec::new();
        for sub in readers {
            updates.extend(self.land_for(sub, fetch, rows));
        }
        updates
    }

    /// [`SingleTableIVM::land_fetch`] for one of the read's subscriptions.
    fn land_for(
        &mut self,
        sub: SubId,
        fetch: &Fetch,
        rows: &[(DataFrameKey, DataFrameRow)],
    ) -> Vec<SingleTableUpdate> {
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
        self.gate_updates(&[sub], updates)
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
        let ops = self.remove_rows_where(sub, column, values, |_| true);
        self.gate_window(sub, ops)
    }

    /// Untag every row `sub` holds whose `column` equals one of `values`
    /// and that its filter no longer matches: the prune after a set-valued
    /// leaf lost those values. A row another branch of the filter still
    /// admits stays. Returns the `Delete` operations.
    pub fn prune_rows(
        &mut self,
        sub: SubId,
        column: &str,
        values: &[Value],
    ) -> Vec<DataFrameOperation> {
        let Some(filter) = self
            .select_queries
            .get(&sub)
            .map(|query| query.filter.clone())
        else {
            return Vec::new();
        };
        let ops = self.remove_rows_where(sub, column, values, |row| {
            !evaluate(&filter, &row.data, &mut 0)
        });
        self.gate_window(sub, ops)
    }

    /// Untag the rows `sub` holds whose `column` equals one of `values`
    /// and that `doomed` selects; the `Delete` operations.
    fn remove_rows_where(
        &mut self,
        sub: SubId,
        column: &str,
        values: &[Value],
        doomed: impl Fn(&DataFrameRow) -> bool,
    ) -> Vec<DataFrameOperation> {
        let doomed: Vec<DataFrameKey> = self
            .rows_matching_any(sub, column, values)
            .into_iter()
            .filter(|(_, row)| doomed(row))
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

    /// Index `column` of `table`'s frame by value from now on, so the rows
    /// of one join value are found without a scan; the join layer asks
    /// this for every column a part joins on.
    pub fn index_column(&mut self, table: &TableName, column: &ColumnName) {
        self.frames
            .entry(table.clone())
            .or_default()
            .index_column(column);
    }

    /// The rows `sub` holds whose `column` equals one of `values`: from
    /// the column's value index when the frame has one (the matches, not
    /// the view), otherwise walked off the subscription's held index, so
    /// the cost scales with its own view, never with the table.
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
        let mut out = Vec::new();
        for value in values {
            let Some(with_value) = frame.rows_with(column, value) else {
                return ids
                    .iter()
                    .filter_map(|id| frame.row(*id))
                    .filter(|row| {
                        row.data
                            .data
                            .get(column)
                            .is_some_and(|v| values.contains(v))
                    })
                    .map(|row| (row.key.clone(), row.data.clone()))
                    .collect();
            };
            for id in with_value {
                if !ids.contains(&id) {
                    continue;
                }
                if let Some(row) = frame.row(id) {
                    out.push((row.key.clone(), row.data.clone()));
                }
            }
        }
        out
    }

    /// The subscription's current view — every shared row it holds, as
    /// key → image, enumerated from its held index. An inspection seam for
    /// tests and debugging, not a sync mechanism: clients build their
    /// frames from the operation stream. `None` for unknown subscriptions.
    pub fn rows_for(&self, sub: SubId) -> Option<HashMap<DataFrameKey, DataFrameRow>> {
        let query = self.select_queries.get(&sub)?;
        let frame = self.frames.get(&query.table);
        let shown = self.windows.get(&sub).map(Window::shown_prefix);
        Some(
            self.held
                .get(&sub)
                .into_iter()
                .flatten()
                .filter_map(|id| frame.and_then(|frame| frame.row(*id)))
                .filter(|row| shown.as_ref().is_none_or(|shown| shown.contains(&row.key)))
                .map(|row| (row.key.clone(), row.data.clone()))
                .collect(),
        )
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

/// `filter` with every set-valued `IN` leaf on `column` replaced by
/// `narrowed` (the `column IN values` a narrowed read restricts itself
/// to); every other leaf stands.
fn narrow_set_leaves(filter: &Where, column: &str, narrowed: &Where) -> Where {
    match filter {
        Where::Condition(condition)
            if condition.column == column
                && condition.comparison_operator == ComparisonOperator::IN
                && matches!(condition.value, Value::Set(_)) =>
        {
            narrowed.clone()
        }
        Where::Condition(condition) => Where::Condition(condition.clone()),
        Where::AND(children) => Where::AND(
            children
                .iter()
                .map(|child| narrow_set_leaves(child, column, narrowed))
                .collect(),
        ),
        Where::OR(children) => Where::OR(
            children
                .iter()
                .map(|child| narrow_set_leaves(child, column, narrowed))
                .collect(),
        ),
    }
}
