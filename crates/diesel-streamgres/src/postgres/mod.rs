//! What the wrapper knows of PostgreSQL itself: column types, encoding
//! values in its binary format, rows kept after a read, and the cursor
//! Diesel reads them through.
//!
//! Public for what the rest of the API exposes: the rows a
//! [`Verdict`](crate::subscriptions::Verdict) carries and their column
//! types, the cursor and row types of the connections' Diesel impls, and
//! what answering [`Outcome::NeedsTypes`](crate::subscriptions::Outcome)
//! on a connection of one's own takes.

mod cursor;
pub(crate) mod encode;
mod row;
mod types;

pub use self::cursor::{SyncedCursor, SyncedField, SyncedRow};
pub use self::row::{Columns, OwnedCursor, OwnedField, OwnedRow, OwnedValue, capture_all};
pub use self::types::{
    ALL_TABLES_SQL, ColumnType, TableAttribute, TableTypes, TypeKind, TypeLookup, all_table_types,
};
