//! The storage boundary: where rows the engine does not yet hold are read
//! from. Every read is asynchronous and reports, with its rows, the WAL
//! location its snapshot reflects every commit up to. A source is told how
//! far the write stream has been delivered ([`Storage::advance`]) and may
//! only answer reads from snapshots at or below that point, so a read is
//! never ahead of what the engine has applied; it also says the lowest
//! location a read may be positioned at from now on ([`Storage::floor`]),
//! which is how far back the runtime keeps delivered writes to bring a
//! read up. The in-process [`MemoryStorage`] is always at the stream
//! position (it holds the current rows, no bring-up needed); a source that
//! mints snapshots ahead of time (see `pg`) flips to a newer snapshot only
//! once the stream has passed it.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::ivm::{evaluate, order_rows};
use crate::model::Snapshot;
use crate::model::{DataFrameKey, DataFrameRow, Lsn, SingleTableReadQuery, TableName, WriteQuery};

/// A failed storage read; the runtime parks the read and hands it out
/// again once the stream moves, unless the read was refused (the message
/// begins with [`REFUSED`]): that read will never succeed, and the
/// subscriptions depending on it are told why instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageError(pub String);

/// The prefix of a refusal's message.
pub const REFUSED: &str = "refused: ";

impl StorageError {
    /// A read that will never succeed, with the reason a client can be told.
    pub fn refused(reason: impl Into<String>) -> Self {
        StorageError(format!("{REFUSED}{}", reason.into()))
    }

    /// The reason, when the read was refused rather than merely failed.
    pub fn refusal(&self) -> Option<&str> {
        self.0.strip_prefix(REFUSED)
    }
}

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
    /// That position must not exceed the last [`Storage::advance`]: the
    /// snapshot may be behind the stream (the runtime brings its rows up),
    /// never ahead of it. When `limit` is finite the result must honor
    /// `order_by` and `limit` (window maintenance depends on getting the
    /// *best* rows); an unlimited query may return rows in any order.
    async fn select(&self, query: &SingleTableReadQuery) -> Result<Snapshot, StorageError>;

    /// [`Storage::select`] for a query already shared: a source that runs
    /// the read as a task of its own takes the handle instead of copying
    /// the query. The default reads through the handle.
    async fn select_shared(
        &self,
        query: Arc<SingleTableReadQuery>,
    ) -> Result<Snapshot, StorageError> {
        self.select(&query).await
    }

    /// How many rows of `query.table` satisfy `query.filter`, counted no
    /// further than `cap`: the answer is exact below `cap`, and `cap`
    /// itself means "at least that many". A planner asks this before it
    /// registers a join, to learn which side is small enough to read
    /// whole; a source that can stop counting early does, and the default
    /// reads the rows and counts them.
    async fn count(&self, query: &SingleTableReadQuery, cap: u64) -> Result<u64, StorageError> {
        let unlimited = SingleTableReadQuery {
            limit: u32::MAX,
            ..query.clone()
        };
        let snapshot = self.select(&unlimited).await?;
        Ok((snapshot.rows.len() as u64).min(cap))
    }

    /// The stream has been delivered (and applied by the engine) up to
    /// `feed`: a snapshot at or below it may now serve reads.
    fn advance(&self, feed: Lsn);

    /// The lowest location a read issued from now on can be positioned
    /// at; the runtime keeps every delivered write above it.
    fn floor(&self) -> Lsn;

    /// A write the stream delivered at `at`, before it is routed: a source
    /// mirroring tables in memory applies it. No-op by default.
    fn absorb(&self, write: &WriteQuery, at: Lsn) {
        let _ = (write, at);
    }
}

/// In-process storage: plain tables of rows, kept in insertion order so
/// selects (and therefore the operations they lead to) are deterministic.
/// Reads answer immediately and are positioned at the stream position the
/// store was last advanced to: the store holds exactly the rows the
/// engine's position implies, so nothing needs bringing up.
///
/// Interior mutability lets tests hold a shared handle and mirror every
/// write into storage *before* routing it, the same order of events a
/// real database produces (commit first, change feed second).
#[derive(Default)]
pub struct MemoryStorage {
    tables: RefCell<HashMap<TableName, Vec<(DataFrameKey, DataFrameRow)>>>,
    position: Cell<Lsn>,
}

impl MemoryStorage {
    /// An empty store at location zero.
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

    /// Put a table's rows in place (a warm-up from another source),
    /// upserting by primary key.
    pub fn load(&self, table: &TableName, rows: Vec<(DataFrameKey, DataFrameRow)>) {
        let mut tables = self.tables.borrow_mut();
        let held = tables.entry(table.clone()).or_default();
        for (key, row) in rows {
            match held.iter().position(|(existing, _)| *existing == key) {
                Some(index) => held[index] = (key, row),
                None => held.push((key, row)),
            }
        }
    }

    /// The location the store was last advanced to.
    pub fn position(&self) -> Lsn {
        self.position.get()
    }

    /// The rows `query` selects right now: a linear scan of the table,
    /// filtering by the query's `Where` via the same predicate evaluation
    /// the engine routes with. A finite `limit` sorts by the `order_by`
    /// columns in turn (via the window module's total order) and
    /// truncates; an unlimited query keeps insertion order.
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
            selected.sort_by(|(_, a), (_, b)| order_rows(&query.order_by, a, b));
            selected.truncate(query.limit as usize);
        }
        selected
    }
}

impl Storage for MemoryStorage {
    /// [`MemoryStorage::rows`], positioned at the store's position; ready
    /// immediately.
    async fn select(&self, query: &SingleTableReadQuery) -> Result<Snapshot, StorageError> {
        Ok(Snapshot {
            rows: self.rows(query),
            at: self.position.get(),
        })
    }

    /// The store is current at `feed`.
    fn advance(&self, feed: Lsn) {
        if feed > self.position.get() {
            self.position.set(feed);
        }
    }

    /// Reads are never behind the position.
    fn floor(&self) -> Lsn {
        self.position.get()
    }

    /// Apply the write and move the position to it.
    fn absorb(&self, write: &WriteQuery, at: Lsn) {
        self.apply(write);
        self.advance(at);
    }
}
