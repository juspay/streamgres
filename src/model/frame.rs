//! Materialized result sets and the delta operations that maintain them.
//!
//! A `DataFrame` is the engine's materialized copy of one subscription's
//! result set. A `DataFrameOperation` is the unit shipped to a client so it
//! can maintain its own copy — the same operation the engine applies to its
//! `DataFrame` is what goes over the wire (once the WebSocket layer exists).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use super::value::{unordered_map_hash, Value};

/// The identity of a row inside a `DataFrame`: its primary-key values.
///
/// Used as a `HashMap` key, hence the manual `Hash` (a `HashMap` field has
/// order-independent equality, so the hash must be order-independent too —
/// see [`unordered_map_hash`]).
#[derive(Debug, Clone, PartialEq)]
pub struct DataFrameKey {
    pub pkey_value: HashMap<String, Value>,
}

impl Eq for DataFrameKey {}

impl Hash for DataFrameKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unordered_map_hash(&self.pkey_value).hash(state);
    }
}

/// One row of a materialized result set. v1 has no column projection
/// (`ReadQuery` selects whole rows), so this is the full row image.
#[derive(Debug, Clone, PartialEq)]
pub struct DataFrameRow {
    pub data: HashMap<String, Value>,
}

/// A materialized result set: row identity → row.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DataFrame {
    pub records: HashMap<DataFrameKey, DataFrameRow>,
}

/// The unit of an incremental update.
///
/// `Add` is an upsert: it both brings a new row into the result set and
/// replaces an existing row whose values changed. Together with `Delete`
/// this covers every way a write can affect a result set — a row entering,
/// leaving, or changing in place.
#[derive(Debug, Clone, PartialEq)]
pub enum DataFrameOperation {
    Delete(DataFrameKey),
    Add(DataFrameKey, DataFrameRow),
}

impl DataFrameKey {
    pub fn new(pkey_value: HashMap<String, Value>) -> Self {
        DataFrameKey { pkey_value }
    }
}

impl DataFrame {
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn contains(&self, key: &DataFrameKey) -> bool {
        self.records.contains_key(key)
    }

    /// Apply one delta to this materialized view. Idempotent: re-applying
    /// the same operation leaves the frame unchanged.
    pub fn apply(&mut self, op: &DataFrameOperation) {
        match op {
            DataFrameOperation::Add(key, row) => {
                self.records.insert(key.clone(), row.clone());
            }
            DataFrameOperation::Delete(key) => {
                self.records.remove(key);
            }
        }
    }
}

impl DataFrameOperation {
    /// The identity of the row this operation touches.
    pub fn key(&self) -> &DataFrameKey {
        match self {
            DataFrameOperation::Add(key, _) => key,
            DataFrameOperation::Delete(key) => key,
        }
    }
}
