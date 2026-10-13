//! Rows the cache can hold.
//!
//! A `PgRow` borrows from the libpq result it came from, so it can neither
//! outlive the statement nor cross threads. [`OwnedRow`] keeps what a
//! `PgRow` yields, each value's type OID and raw bytes in the format
//! Postgres sent them in, and implements Diesel's [`Row`] over that, so
//! `#[derive(Queryable)]`, `#[derive(QueryableByName)]` and every
//! `FromSql` decode it exactly as they decode a live row. Nothing is
//! re-encoded: a cached row is byte-identical to the one Postgres served.
//!
//! The column names are the same for every row of a result set, so they
//! are captured once per set and shared ([`Columns`]). The OID travels
//! with each value: Diesel only ever asks for it through a value, and a
//! NULL has none.

use std::num::NonZeroU32;
use std::sync::Arc;

use diesel::QueryResult;
use diesel::pg::{Pg, PgValue};
use diesel::row::{Field, PartialRow, Row, RowIndex, RowSealed};

/// The column names of one result set, shared by all its rows.
pub type Columns = Arc<[String]>;

/// One non-NULL value as Postgres sent it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedValue {
    type_oid: NonZeroU32,
    bytes: Box<[u8]>,
}

/// A row that owns its bytes. Cloning shares them: a cached row is handed
/// to every read that hits it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedRow {
    columns: Columns,
    /// One entry per column; `None` is SQL `NULL`.
    values: Arc<[Option<OwnedValue>]>,
}

impl OwnedRow {
    /// Copies a live row. The column list is taken from `columns` when one
    /// is given, so the rows of one result set share it; otherwise it is
    /// read off the row.
    ///
    /// `None` when a column has no name, which Diesel's row types never
    /// produce for a Postgres result.
    pub fn capture<'a, R>(row: &R, columns: Option<&Columns>) -> Option<OwnedRow>
    where
        R: Row<'a, Pg>,
    {
        let count = row.field_count();
        let mut values = Vec::with_capacity(count);
        let mut fresh = match columns {
            Some(_) => None,
            None => Some(Vec::with_capacity(count)),
        };
        for index in 0..count {
            let field = row.get(index)?;
            values.push(field.value().map(|value| OwnedValue {
                type_oid: value.get_oid(),
                bytes: Box::from(value.as_bytes()),
            }));
            if let Some(fresh) = fresh.as_mut() {
                fresh.push(field.field_name()?.to_string());
            }
        }
        let columns = match (columns, fresh) {
            (Some(shared), _) => Arc::clone(shared),
            (None, Some(fresh)) => Arc::from(fresh),
            (None, None) => unreachable!("one of the two is always set"),
        };
        Some(OwnedRow {
            columns,
            values: Arc::from(values),
        })
    }

    /// The columns this row has, shared with the rest of its result set.
    pub fn columns(&self) -> &Columns {
        &self.columns
    }

    /// One column's value, `None` for NULL or out of range.
    pub fn value(&self, index: usize) -> Option<&OwnedValue> {
        self.values.get(index)?.as_ref()
    }

    /// How many bytes the values hold, for a size-bounded cache.
    pub fn byte_len(&self) -> usize {
        self.values
            .iter()
            .map(|value| value.as_ref().map_or(0, |value| value.bytes.len()))
            .sum()
    }
}

impl RowSealed for OwnedRow {}

impl RowIndex<usize> for OwnedRow {
    fn idx(&self, idx: usize) -> Option<usize> {
        (idx < self.values.len()).then_some(idx)
    }
}

impl<'a> RowIndex<&'a str> for OwnedRow {
    fn idx(&self, name: &'a str) -> Option<usize> {
        self.columns.iter().position(|column| column == name)
    }
}

impl<'a> Row<'a, Pg> for OwnedRow {
    type Field<'f>
        = OwnedField<'f>
    where
        'a: 'f,
        Self: 'f;
    type InnerPartialRow = Self;

    fn field_count(&self) -> usize {
        self.values.len()
    }

    fn get<'b, I>(&'b self, idx: I) -> Option<Self::Field<'b>>
    where
        'a: 'b,
        Self: RowIndex<I>,
    {
        let index = self.idx(idx)?;
        Some(OwnedField { row: self, index })
    }

    fn partial_row(&self, range: std::ops::Range<usize>) -> PartialRow<'_, Self::InnerPartialRow> {
        PartialRow::new(self, range)
    }
}

/// One column of an [`OwnedRow`].
pub struct OwnedField<'a> {
    row: &'a OwnedRow,
    index: usize,
}

impl<'a> Field<'a, Pg> for OwnedField<'a> {
    fn field_name(&self) -> Option<&str> {
        Some(&self.row.columns[self.index])
    }

    fn value(&self) -> Option<PgValue<'_>> {
        let value = self.row.value(self.index)?;
        Some(PgValue::new(&value.bytes, &value.type_oid))
    }
}

/// Copies every row a live cursor yields. Stops at the first error and
/// returns it, so a half-read result is never kept.
pub fn capture_all<'a, I, R>(cursor: I) -> QueryResult<Vec<OwnedRow>>
where
    I: Iterator<Item = QueryResult<R>>,
    R: Row<'a, Pg>,
{
    let mut rows = Vec::new();
    let mut columns: Option<Columns> = None;
    for row in cursor {
        let row = row?;
        let owned = OwnedRow::capture(&row, columns.as_ref())
            .ok_or(diesel::result::Error::DeserializationError(
                "a result row without column names cannot be kept".into(),
            ))?;
        if columns.is_none() {
            columns = Some(Arc::clone(owned.columns()));
        }
        rows.push(owned);
    }
    Ok(rows)
}

/// Yields owned rows the way a live cursor yields live ones, so Diesel's
/// `load` machinery can decode a cached result.
pub struct OwnedCursor {
    rows: Arc<[OwnedRow]>,
    next: usize,
}

impl OwnedCursor {
    pub fn new(rows: Arc<[OwnedRow]>) -> Self {
        OwnedCursor { rows, next: 0 }
    }
}

impl Iterator for OwnedCursor {
    type Item = QueryResult<OwnedRow>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.rows.get(self.next)?.clone();
        self.next += 1;
        Some(Ok(row))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = self.rows.len() - self.next;
        (left, Some(left))
    }
}

impl ExactSizeIterator for OwnedCursor {}
