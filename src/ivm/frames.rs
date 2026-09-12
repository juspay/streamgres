//! Frame surgery and inspection: tagging rows into and out of the shared
//! per-table frames ([`crate::model::frame::TableFrame`]) for one
//! subscription at a time — the row-level seams the join layer maintains
//! its driven parts with — plus the storage-read seam (asking for rows,
//! landing them) and the read-only views tests and callers inspect. Tags
//! and the held index store compact ids ([`crate::model::SubId`],
//! [`crate::model::frame::RowId`]) and are always changed together.

use std::collections::HashMap;

use super::predicate::evaluate;
use super::{window, Fetch, FetchId, FetchKind, SingleTableIVM, SingleTableUpdate};
use crate::model::frame::Hold;
use crate::model::position::RowAt;
use crate::model::{
    ComparisonOperator, Condition, DataFrameKey, DataFrameOperation, DataFrameRow, Lsn,
    SingleTableReadQuery, SubId, TableName, Value, Where,
};

/// What a landing found for one row already in the frame, read in one
/// borrow so the decision can be applied in another.
struct Found {
    frame_newer: bool,
    same: bool,
    frame_image: DataFrameRow,
    view: Option<DataFrameRow>,
}

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

    /// Land the rows a recorded read returned, positioned at `at`, for
    /// the read's subscription: each row is merged into the shared frame
    /// by currency (`land_row` below), the window's frontier is sized
    /// from the whole result, overflow is evicted, a refill is asked for
    /// if the window drained while the read was out, and the boundary is
    /// republished. Returns the subscription's `Add`s and evictions; no
    /// other subscription is touched. A read for a subscription that is
    /// gone lands as nothing.
    pub fn land_fetch(&mut self, fetch: &Fetch, rows: &[(DataFrameKey, DataFrameRow)], at: Lsn) -> Vec<SingleTableUpdate> {
        let sub = fetch.sub;
        let Some(query) = self.select_queries.get(&sub).cloned() else {
            return Vec::new();
        };
        let table = query.table.clone();
        let mut updates = Vec::new();
        for (key, row) in rows {
            updates.extend(self.land_row(sub, &table, &query.filter, key, row, at));
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

    /// Merge one landed row into the shared frame for `sub`, whose filter
    /// is `filter`. The candidate image is the newer of the frame's and
    /// the read's: the frame's when the frame is newer than the read (a
    /// write the read did not see) or the two are equal, the read's when
    /// the frame is older, in which case `sub` goes **ahead of the frame**
    /// on this row (its [`Hold`] keeps the read's location and image until
    /// the write that produced it arrives, while every other holder keeps
    /// the frame's). `sub` then holds the candidate iff it satisfies the
    /// filter: it is tagged and sent an `Add`, or, if it already held a
    /// different image, the replace pair; a held row whose candidate no
    /// longer matches is deleted for it. A row the frame does not hold is
    /// inserted with the read's image and stamped with the read; a row
    /// equal to the read's and not newer is stamped too.
    fn land_row(
        &mut self,
        sub: SubId,
        table: &TableName,
        filter: &Where,
        key: &DataFrameKey,
        row: &DataFrameRow,
        at: Lsn,
    ) -> Vec<SingleTableUpdate> {
        let mut updates = Vec::new();
        let frame = self.frames.entry(table.clone()).or_default();
        let Some(id) = frame.id_of(key) else {
            frame.entry(key, || (row.clone(), RowAt::Landed(at)));
            updates.extend(self.tag_row(sub, table, key, row));
            self.track_landed(sub, key, row);
            return updates;
        };
        let Some(shared) = frame.row_mut(id) else {
            return updates;
        };
        let found = Found {
            frame_newer: shared.at.newer_than(at),
            same: shared.data == *row,
            frame_image: shared.data.clone(),
            view: shared.held_by(sub).then(|| shared.view(sub).clone()),
        };
        if found.same && !found.frame_newer {
            shared.at = RowAt::Landed(at);
        }
        let ahead = !found.frame_newer && !found.same;
        let candidate = if ahead { row.clone() } else { found.frame_image };
        let hold = Hold {
            at: if ahead { at } else { shared.at.lsn() },
            ahead: ahead.then(|| candidate.clone()),
        };
        let matches = evaluate(filter, &candidate.data, &mut 0);
        match (found.view, matches) {
            (None, false) => {}
            (None, true) => {
                shared.subscribers.insert(sub, hold);
                self.held.entry(sub).or_default().insert(id);
                self.stats.ops_add += 1;
                updates.push(SingleTableUpdate {
                    query: sub,
                    table: table.clone(),
                    op: DataFrameOperation::Add(key.clone(), candidate.clone()),
                });
                self.track_landed(sub, key, &candidate);
            }
            (Some(view), true) => {
                shared.subscribers.insert(sub, hold);
                if view != candidate {
                    self.stats.ops_delete += 1;
                    self.stats.ops_add += 1;
                    updates.push(SingleTableUpdate {
                        query: sub,
                        table: table.clone(),
                        op: DataFrameOperation::Delete(key.clone(), view),
                    });
                    updates.push(SingleTableUpdate {
                        query: sub,
                        table: table.clone(),
                        op: DataFrameOperation::Add(key.clone(), candidate.clone()),
                    });
                    self.track_landed(sub, key, &candidate);
                }
            }
            (Some(view), false) => {
                shared.subscribers.remove(&sub);
                if let Some(ids) = self.held.get_mut(&sub) {
                    ids.remove(&id);
                }
                self.stats.ops_delete += 1;
                updates.push(SingleTableUpdate {
                    query: sub,
                    table: table.clone(),
                    op: DataFrameOperation::Delete(key.clone(), view),
                });
                if let Some(window) = self.windows.get_mut(&sub) {
                    window.remove(key);
                }
            }
        }
        updates
    }

    /// Tag `sub` onto the row `key` of `table` with exactly what `from`
    /// holds for it (the twin path): the frame's image at the frame's
    /// location, or `from`'s own image and location while it is ahead of
    /// the frame, in which case `sub` goes ahead with it. Returns the
    /// `Add`, or `None` when `sub` already holds the row or `from` does
    /// not.
    pub(super) fn share_view(
        &mut self,
        sub: SubId,
        from: SubId,
        table: &TableName,
        key: &DataFrameKey,
    ) -> Option<SingleTableUpdate> {
        let frame = self.frames.get_mut(table)?;
        let id = frame.id_of(key)?;
        let row = frame.row_mut(id)?;
        let hold = row.subscribers.get(&from).cloned()?;
        if row.held_by(sub) {
            return None;
        }
        let image = row.view(from).clone();
        row.subscribers.insert(sub, hold);
        self.held.entry(sub).or_default().insert(id);
        self.stats.ops_add += 1;
        Some(SingleTableUpdate {
            query: sub,
            table: table.clone(),
            op: DataFrameOperation::Add(key.clone(), image),
        })
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
        let hold = row.subscribers.remove(&sub)?;
        let removed = hold.ahead.unwrap_or_else(|| row.data.clone());
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
            .filter(|row| row.view(sub).data.get(column).is_some_and(|v| values.contains(v)))
            .map(|row| (row.key.clone(), row.view(sub).clone()))
            .collect()
    }

    /// The subscription's current view — every shared row it holds, as
    /// key → the image it holds (the frame's, or its own while ahead of
    /// the frame), enumerated from its held index. An inspection seam for
    /// tests and debugging, not a sync mechanism: clients build their
    /// frames from the operation stream. `None` for unknown subscriptions.
    pub fn rows_for(&self, sub: SubId) -> Option<HashMap<DataFrameKey, DataFrameRow>> {
        let query = self.select_queries.get(&sub)?;
        let frame = self.frames.get(&query.table);
        let mut view = HashMap::new();
        if let Some(ids) = self.held.get(&sub) {
            for id in ids {
                if let Some(row) = frame.and_then(|frame| frame.row(*id)) {
                    view.insert(row.key.clone(), row.view(sub).clone());
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
            .map(|row| row.subscribers.keys().copied().collect())
            .unwrap_or_default()
    }
}
