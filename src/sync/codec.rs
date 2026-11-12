//! How a source's cells become the engine's values. Each database's
//! readers and change feed decode through one [`CellDecoder`], so the
//! engine only ever holds exact [`Value`]s, whatever form the source sends
//! them in (Postgres binary, a changefeed's JSON).

use crate::model::{Value, ValueType};

/// Decodes one non-null cell of a source into the exact [`Value`] for the
/// column's declared type.
pub trait CellDecoder {
    /// One cell as the source delivers it.
    type Cell<'a>;
    /// Why a cell is not a value of its type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// The value of `cell` in a column declared as `declared`.
    fn decode(&self, declared: &ValueType, cell: Self::Cell<'_>) -> Result<Value, Self::Error>;
}
