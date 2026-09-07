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
//! The operation stream's ordering contract is per key: operations on the
//! same key are applied in stream order — a row changing in place ships as
//! `Delete(old)` immediately followed by `Add(new)`, and a row admitted
//! and then evicted within one step ships its `Add` before its `Delete`.
//! Operations on different rows commute — a snapshot's `Add`s may arrive
//! in any order and still converge, because a receiver applies `Add` as
//! insert-or-replace and `Delete` as remove.

use std::collections::{BTreeSet, HashMap};
use std::hash::{Hash, Hasher};

use super::query::QueryId;
use super::value::{unordered_map_hash, Value};

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
    pub pkey_value: HashMap<String, Value>,
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
    pub data: HashMap<String, Value>,
}

/// The unit of an incremental update — self-contained, with no pre-image
/// side channel:
///
/// - `Add(key, row)`: the row enters the result set with this image; a
///   receiver applies it as insert-or-replace.
/// - `Delete(key, row)`: the row leaves, and it carries the removed image —
///   a bare key would say nothing about the values that just vanished,
///   which downstream consumers (join maintenance, clients) need.
///
/// A row changing in place is emitted as `Delete(old)` immediately followed
/// by `Add(new)` for the same key.
#[derive(Debug, Clone, PartialEq)]
pub enum DataFrameOperation {
    Delete(DataFrameKey, DataFrameRow),
    Add(DataFrameKey, DataFrameRow),
}

/// One shared, materialized row of a table frame.
///
/// - `data`: the full row image, stored once no matter how many
///   subscriptions hold the row.
/// - `subscribers`: every subscription whose result set currently contains
///   the row; the row is dropped when this empties.
pub struct SharedRow {
    pub data: HashMap<String, Value>,
    pub subscribers: BTreeSet<QueryId>,
}

/// The one shared frame of a table: row identity → shared row.
///
/// Engine state, not wire vocabulary — a subscription's view of it is the
/// rows tagged with its id, and reaches the client only as operations.
#[derive(Default)]
pub struct TableFrame {
    pub rows: HashMap<DataFrameKey, SharedRow>,
}

impl DataFrameKey {
    /// Builds a row identity from primary-key column name → value.
    pub fn new(pkey_value: HashMap<String, Value>) -> Self {
        DataFrameKey { pkey_value }
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
