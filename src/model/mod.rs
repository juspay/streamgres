//! The data model the whole engine operates on.
//!
//! Split by concern, re-exported flat — `use jus_sync::model::*` gives the
//! full vocabulary:
//!
//! - [`value`] — [`Value`] / [`ValueType`], the dynamic value representation
//! - [`schema`] — [`Catalog`] / [`DbTable`] / [`DbColumn`] / [`DbRecord`]:
//!   the single authoritative schema home; queries and records reference
//!   tables by name and resolve them in the catalog
//! - [`query`] — [`ReadQuery`] / [`WriteQuery`] and the `Where` predicate tree
//! - [`frame`] — [`DataFrame`] and the [`DataFrameOperation`] delta unit
//!
//! Structure changes here ripple through the whole engine (these types are
//! index keys in `crate::ivm`), so keep field changes deliberate and discuss
//! them in an issue first — see "Design notes" in the README.

pub mod frame;
pub mod query;
pub mod schema;
pub mod value;

pub use frame::{DataFrame, DataFrameKey, DataFrameOperation, DataFrameRow};
pub use query::{
    ComparisonOperator, Condition, DeleteQuery, Disjunct, InsertQuery, Order, OrderBy, ReadQuery,
    UpdateQuery, Where, WriteQuery,
};
pub use schema::{Catalog, DbColumn, DbRecord, DbTable, TableName};
pub use value::{Value, ValueType};
