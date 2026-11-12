//! Where in the source's history a write or a storage read sits: one
//! [`Position`] for both. The engine itself never compares positions; the
//! runtime does, to bring a read's result up to the point the engine has
//! reached before the engine sees it (see `sync::Runtime`).

use std::fmt;

use super::frame::{DataFrameKey, DataFrameRow};

/// A point in the source's history, ordered as the source orders its
/// commits. Each backend says what it is: a WAL location for Postgres
/// (`sync::pg::Lsn`), a hybrid-logical timestamp for a database that has
/// those. A write carries the position of its transaction's commit; a read
/// carries the position its snapshot reflects every commit up to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Position(pub u128);

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The result of one storage read: the rows, and the location the read's
/// snapshot reflects every commit up to (and nothing beyond).
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub rows: Vec<(DataFrameKey, DataFrameRow)>,
    pub at: Position,
}
