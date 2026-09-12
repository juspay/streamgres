//! The storage boundary: where rows the engine does not yet hold are read
//! from. Every read is asynchronous and reports, with its rows, the WAL
//! location its snapshot reflects every commit up to; how a source arrives
//! at that location is its own business (see `pg`). The runtime never
//! calls it from inside the engine, so a slow read never stalls the write
//! stream. The in-process [`MemoryStorage`] answers immediately (its
//! futures are ready when created), which is what lets the synchronous
//! driver run it inline.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;

use crate::ivm::{evaluate, order_cmp};
use crate::model::Snapshot;
use crate::model::{
    DataFrameKey, DataFrameRow, Lsn, Order, SingleTableReadQuery, TableName, Value, WriteQuery,
};

/// A failed storage read; the runtime re-issues the read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageError(pub String);

impl fmt::Display for StorageError {
    /// The message.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StorageError {}

/// Read-only access to the source of truth for row data.
///
/// The engine and its runtime are single-threaded by design, so the
/// returned futures need not be `Send`; a driver awaits them on the thread
/// that owns the engine.
#[allow(async_fn_in_trait)]
pub trait Storage {
    /// Rows of `query.table` whose full row image satisfies `query.filter`,
    /// read from **one consistent snapshot** of the source, as (identity,
    /// image) pairs, together with the position that snapshot reflects.
    /// The snapshot must reflect every write committed at or below
    /// `at_least`, the stream position the engine has already applied
    /// (a source that takes its snapshot at read time satisfies this by
    /// construction; one that mints snapshots ahead of time must pick or
    /// mint a fresh one). When `limit` is finite the result must honor
    /// `order_by` and `limit` (window maintenance depends on getting the
    /// *best* rows); an unlimited query may return rows in any order.
    async fn select(&self, query: &SingleTableReadQuery, at_least: Option<Lsn>) -> Result<Snapshot, StorageError>;
}

/// In-process storage: plain tables of rows, kept in insertion order so
/// selects (and therefore the operations they lead to) are deterministic.
/// Reads answer immediately and are positioned at location zero: below
/// every write the synchronous driver ever routes, so nothing a read
/// returns is mistaken for reflecting a write routed after it.
///
/// Interior mutability lets tests hold a shared handle and mirror every
/// write into storage *before* routing it, the same order of events a
/// real database produces (commit first, change feed second).
#[derive(Default)]
pub struct MemoryStorage {
    tables: RefCell<HashMap<TableName, Vec<(DataFrameKey, DataFrameRow)>>>,
}

impl MemoryStorage {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mirror one write into the store: insert/update upsert the row by
    /// primary key (full-row-image semantics, like the engine), delete
    /// removes it.
    pub fn apply(&self, write: &WriteQuery) {
        let mut tables = self.tables.borrow_mut();
        let rows = tables.entry(write.table().clone()).or_default();
        let position = rows.iter().position(|(key, _)| key == write.pkey_value());
        match write.new_row_image() {
            Some(image) => {
                let entry = (write.pkey_value().clone(), image.clone());
                match position {
                    Some(index) => rows[index] = entry,
                    None => rows.push(entry),
                }
            }
            None => {
                if let Some(index) = position {
                    rows.remove(index);
                }
            }
        }
    }

    /// The rows `query` selects right now: a linear scan of the table,
    /// filtering by the query's `Where` via the same predicate evaluation
    /// the engine routes with. A finite `limit` sorts by the `order_by`
    /// column (via the window module's total order) and truncates; an
    /// unlimited query keeps insertion order.
    pub fn rows(&self, query: &SingleTableReadQuery) -> Vec<(DataFrameKey, DataFrameRow)> {
        let tables = self.tables.borrow();
        let Some(rows) = tables.get(&query.table) else {
            return Vec::new();
        };
        let mut selected: Vec<(DataFrameKey, DataFrameRow)> = rows
            .iter()
            .filter(|(_, row)| evaluate(&query.filter, &row.data, &mut 0))
            .cloned()
            .collect();
        if query.limit != u32::MAX {
            let column = query.order_by.column.as_str();
            selected.sort_by(|(_, a), (_, b)| {
                let ordering = order_cmp(
                    a.data.get(column).unwrap_or(&Value::Null),
                    b.data.get(column).unwrap_or(&Value::Null),
                );
                match query.order_by.direction {
                    Order::ASC => ordering,
                    Order::DESC => ordering.reverse(),
                }
            });
            selected.truncate(query.limit as usize);
        }
        selected
    }
}

impl Storage for MemoryStorage {
    /// [`MemoryStorage::rows`], positioned at zero; ready immediately (an
    /// in-process read is always current, so `at_least` is moot).
    async fn select(&self, query: &SingleTableReadQuery, _at_least: Option<Lsn>) -> Result<Snapshot, StorageError> {
        Ok(Snapshot {
            rows: self.rows(query),
            at: Lsn(0),
        })
    }
}
