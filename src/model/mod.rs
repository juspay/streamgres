//! The data model the whole engine operates on.
//!
//! Split by concern, re-exported flat — `use jus_sync::model::*` gives the
//! full vocabulary:
//!
//! - [`value`] — [`Value`] / [`ValueType`], the dynamic value representation
//! - [`schema`] — [`Catalog`] / [`DbTable`] / [`DbColumn`]: the single
//!   authoritative schema home; queries reference tables by name and
//!   resolve them in the catalog
//! - [`query`] — [`SingleTableReadQuery`] / [`WriteQuery`], the `Where`
//!   predicate tree, and the [`QueryId`] subscription handle
//! - [`frame`] — row identities, images, the [`DataFrameOperation`] delta
//!   unit, and the engine's shared [`frame::TableFrame`] materialization
//!
//! Structure changes here ripple through the whole engine (these types are
//! index keys in `crate::ivm`), so keep field changes deliberate and discuss
//! them in an issue first — see "Design notes" in the README.

pub mod frame;
pub mod query;
pub mod schema;
pub mod value;

pub use frame::{DataFrameKey, DataFrameOperation, DataFrameRow};
pub use query::{
    ComparisonOperator, Condition, DeleteQuery, Disjunct, InsertQuery, Join, MultiTableReadQuery,
    Order, OrderBy, QueryId, SingleTableReadQuery, UpdateQuery, Where, WriteQuery,
};
pub use schema::{Catalog, ColumnName, DbColumn, DbTable, TableName};
pub use value::{Value, ValueType, SharedSet};
