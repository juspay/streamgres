//! Row identities, row images, the delta operations shipped to clients,
//! and the shared frame those deltas are computed against.
//!
//! Two layers live here. The **wire vocabulary** — [`DataFrameKey`] (which
//! row), [`DataFrameRow`] (its image), [`DataFrameOperation`] (what
//! happened to it) — is what crosses the boundary: clients only ever
//! receive operations, never a frame. The **shared frame** —
//! [`TableFrame`] of [`SharedRow`]s — is the engine's materialization: one
//! frame per table, each row stored once and tagged with every
//! subscription holding it; it never crosses the boundary.
//!
//! Inside the engine the operation stream's ordering contract is per key:
//! operations on the same key are applied in stream order — a row changing
//! in place is `Delete(old)` immediately followed by `Add(new)`, and a row
//! admitted and then evicted within one step ships its `Add` before its
//! `Delete`. Operations on different rows commute. What a client receives
//! is the per-client grouping of that stream (see `ivm::ClientUpdate`),
//! where an in-place change has collapsed to the one `Add` a receiver
//! applies as insert-or-replace.

use std::collections::{BTreeSet, HashMap};
use std::hash::{Hash, Hasher};

use super::query::SubId;
use super::schema::ColumnName;
use super::value::{Value, unordered_map_hash};

/// The identity of a row: its primary-key values — deliberately nothing
/// else.
///
/// The table is encoded by location (the engine keys its shared frames by
/// table name), which queries hold the row is fan-out metadata kept beside
/// the row (the engine's per-row subscriber tags), and which subscription
/// an operation addresses rides in the envelope around the operation.
/// Putting any of those into the key would either repeat them per row and
/// per wire operation, or — for a query id — split one shared row into
/// many identities and undo the sharing.
///
/// Used as a `HashMap` key, hence the manual `Hash` (a `HashMap` field has
/// order-independent equality, so the hash must be order-independent too —
/// see `unordered_map_hash` in `model::value`).
#[derive(Debug, Clone, PartialEq)]
pub struct DataFrameKey {
    pub pkey_value: HashMap<ColumnName, Value>,
}

impl Eq for DataFrameKey {}

impl Hash for DataFrameKey {
    /// Hashes the primary-key map order-independently so equal keys hash
    /// identically regardless of `HashMap` iteration order.
    fn hash<H: Hasher>(&self, state: &mut H) {
        unordered_map_hash(&self.pkey_value).hash(state);
    }
}

/// One full row image. v1 has no column projection
/// (`SingleTableReadQuery` selects whole rows), so this is the whole row.
#[derive(Debug, Clone, PartialEq)]
pub struct DataFrameRow {
    pub data: HashMap<ColumnName, Value>,
}

impl DataFrameRow {
    /// Builds a row image from `(column, value)` pairs.
    pub fn new(pairs: impl IntoIterator<Item = (impl Into<ColumnName>, Value)>) -> Self {
        DataFrameRow {
            data: pairs
                .into_iter()
                .map(|(column, value)| (column.into(), value))
                .collect(),
        }
    }
}

/// The unit of an incremental update — self-contained, with no pre-image
/// side channel:
///
/// - `Add(key, row)`: the row enters the result set with this image; a
///   receiver applies it as insert-or-replace.
/// - `Delete(key, row)`: the row leaves, and it carries the removed image —
///   a bare key would say nothing about the values that just vanished,
///   which downstream consumers (join maintenance, clients) need.
#[derive(Debug, Clone, PartialEq)]
pub enum DataFrameOperation {
    Delete(DataFrameKey, DataFrameRow),
    Add(DataFrameKey, DataFrameRow),
}

/// The engine's compact id for one row of one table's frame: handed out
/// once when the row first enters the frame and retired, never reused,
/// when its last holder leaves. Per table, so never globally unique.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RowId(pub u64);

/// One row of a shared frame: its identity, its current image, and the
/// subscriptions holding it — the "which query sets is this row subscribed
/// to" index that membership routing reads. The image is the current one
/// for every holder: the runtime brings every read up to the engine's
/// position before landing it, so no holder ever sees a different image.
pub struct SharedRow {
    pub key: DataFrameKey,
    pub data: DataFrameRow,
    pub subscribers: BTreeSet<SubId>,
}

impl SharedRow {
    /// Whether `sub` holds this row.
    pub fn held_by(&self, sub: SubId) -> bool {
        self.subscribers.contains(&sub)
    }
}

/// The one shared frame of a table: row identity → row id → shared row.
///
/// Engine state, not wire vocabulary — a subscription's view of it is the
/// rows tagged with its id, and reaches the client only as operations.
/// Every (subscription, row) pair is stored as two small integers (a tag
/// on the row, an entry in the subscription's held index), never as a
/// copy of the key or the subscription's name.
#[derive(Default)]
pub struct TableFrame {
    ids: HashMap<DataFrameKey, RowId>,
    rows: HashMap<RowId, SharedRow>,
    next_id: u64,
}

impl TableFrame {
    /// The row id of `key`, if the frame holds the row.
    pub fn id_of(&self, key: &DataFrameKey) -> Option<RowId> {
        self.ids.get(key).copied()
    }

    /// The shared row of `key`.
    pub fn get(&self, key: &DataFrameKey) -> Option<&SharedRow> {
        self.rows.get(self.ids.get(key)?)
    }

    /// The shared row under `id`.
    pub fn row(&self, id: RowId) -> Option<&SharedRow> {
        self.rows.get(&id)
    }

    /// The shared row under `id`, mutably.
    pub fn row_mut(&mut self, id: RowId) -> Option<&mut SharedRow> {
        self.rows.get_mut(&id)
    }

    /// The row for `key`, materialized with `data` and no holders if the
    /// frame did not hold it yet; returns its id and the row.
    pub fn entry(
        &mut self,
        key: &DataFrameKey,
        data: impl FnOnce() -> DataFrameRow,
    ) -> (RowId, &mut SharedRow) {
        let id = match self.ids.get(key) {
            Some(id) => *id,
            None => {
                let id = RowId(self.next_id);
                self.next_id += 1;
                self.ids.insert(key.clone(), id);
                self.rows.insert(
                    id,
                    SharedRow {
                        key: key.clone(),
                        data: data(),
                        subscribers: BTreeSet::new(),
                    },
                );
                id
            }
        };
        (id, self.rows.get_mut(&id).expect("inserted or found above"))
    }

    /// Drop the row under `id` if no subscription holds it; reports
    /// whether it was dropped. The id is retired.
    pub fn drop_if_unheld(&mut self, id: RowId) -> bool {
        let unheld = self
            .rows
            .get(&id)
            .is_some_and(|row| row.subscribers.is_empty());
        if unheld && let Some(row) = self.rows.remove(&id) {
            self.ids.remove(&row.key);
        }
        unheld
    }

    /// Whether the frame holds no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// How many rows the frame holds.
    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

impl DataFrameKey {
    /// Builds a row identity from `(primary-key column, value)` pairs.
    pub fn new(pkey_value: impl IntoIterator<Item = (impl Into<ColumnName>, Value)>) -> Self {
        DataFrameKey {
            pkey_value: pkey_value
                .into_iter()
                .map(|(column, value)| (column.into(), value))
                .collect(),
        }
    }
}

impl DataFrameOperation {
    /// The identity of the row this operation touches.
    pub fn key(&self) -> &DataFrameKey {
        match self {
            DataFrameOperation::Add(key, _) => key,
            DataFrameOperation::Delete(key, _) => key,
        }
    }

    /// The row image this operation carries — the new image for an `Add`,
    /// the removed image for a `Delete`.
    pub fn row(&self) -> &DataFrameRow {
        match self {
            DataFrameOperation::Add(_, row) => row,
            DataFrameOperation::Delete(_, row) => row,
        }
    }
}
