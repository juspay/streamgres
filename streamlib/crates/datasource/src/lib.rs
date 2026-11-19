//! The DataSource contract: consistent reads plus an ordered change stream, tied by one position type.

mod catalog;
mod change;
mod checked;
mod error;
mod query;
mod source;
mod value;

#[cfg(feature = "testing")]
pub mod testing;

pub use catalog::{Catalog, Column, Key, Row, Table, complete};
pub use change::{Change, Txn};
pub use checked::Checked;
pub use error::{Result, SourceError};
pub use query::{Dir, Filter, Op, Pred, Query, Truth};
pub use source::{Changes, Feed, Lifecycle, Position, Snapshot, Snapshots, Source};
pub use value::{Value, ValueType};
