//! One cursor type for both sources of rows.
//!
//! Diesel's `LoadConnection` names a single cursor and row type per
//! connection, so a read served from the cache and one served live by
//! Postgres come back through the same enum, the way a
//! `#[derive(MultiConnection)]` connection carries its backends' rows.

use diesel::QueryResult;
use diesel::pg::{Pg, PgValue};
use diesel::row::{Field, PartialRow, Row, RowIndex, RowSealed};

use crate::row::{OwnedCursor, OwnedField, OwnedRow};

/// Rows on their way to the application.
pub enum SyncedCursor<I> {
    Live(I),
    Cached(OwnedCursor),
    /// A live fallback holding its concurrency slot until exhaustion or drop.
    Limited(I, Option<crate::cache::FallbackPermit>),
}

impl<I, R> Iterator for SyncedCursor<I>
where
    I: Iterator<Item = QueryResult<R>>,
{
    type Item = QueryResult<SyncedRow<R>>;

    /// The next row, live or cached.
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SyncedCursor::Live(inner) => inner.next().map(|row| row.map(SyncedRow::Live)),
            SyncedCursor::Cached(inner) => inner.next().map(|row| row.map(SyncedRow::Cached)),
            SyncedCursor::Limited(inner, permit) => {
                let row = inner.next();
                if row.is_none() {
                    permit.take();
                }
                row.map(|row| row.map(SyncedRow::Live))
            }
        }
    }

    /// The inner cursor's hint.
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            SyncedCursor::Live(inner) | SyncedCursor::Limited(inner, _) => inner.size_hint(),
            SyncedCursor::Cached(inner) => inner.size_hint(),
        }
    }
}

impl<I, R> ExactSizeIterator for SyncedCursor<I> where I: ExactSizeIterator<Item = QueryResult<R>> {}

/// One row, live or cached.
pub enum SyncedRow<R> {
    Live(R),
    Cached(OwnedRow),
}

impl<R> RowSealed for SyncedRow<R> {}

impl<R> RowIndex<usize> for SyncedRow<R>
where
    R: RowIndex<usize>,
{
    /// The position, as the row behind it says.
    fn idx(&self, idx: usize) -> Option<usize> {
        match self {
            SyncedRow::Live(row) => row.idx(idx),
            SyncedRow::Cached(row) => row.idx(idx),
        }
    }
}

impl<'a, R> RowIndex<&'a str> for SyncedRow<R>
where
    R: RowIndex<&'a str>,
{
    /// The position of a named column, as the row behind it says.
    fn idx(&self, idx: &'a str) -> Option<usize> {
        match self {
            SyncedRow::Live(row) => row.idx(idx),
            SyncedRow::Cached(row) => row.idx(idx),
        }
    }
}

impl<'a, R> Row<'a, Pg> for SyncedRow<R>
where
    R: Row<'a, Pg>,
{
    type Field<'f>
        = SyncedField<'f, R::Field<'f>>
    where
        'a: 'f,
        Self: 'f;
    type InnerPartialRow = Self;

    /// How many columns the row has.
    fn field_count(&self) -> usize {
        match self {
            SyncedRow::Live(row) => row.field_count(),
            SyncedRow::Cached(row) => row.field_count(),
        }
    }

    /// One column, by position or name.
    fn get<'b, I>(&'b self, idx: I) -> Option<Self::Field<'b>>
    where
        'a: 'b,
        Self: RowIndex<I>,
    {
        let index = self.idx(idx)?;
        match self {
            SyncedRow::Live(row) => row.get(index).map(SyncedField::Live),
            SyncedRow::Cached(row) => row.get(index).map(SyncedField::Cached),
        }
    }

    /// The columns in `range`, for a nested `Queryable`.
    fn partial_row(&self, range: std::ops::Range<usize>) -> PartialRow<'_, Self::InnerPartialRow> {
        PartialRow::new(self, range)
    }
}

/// One column of a [`SyncedRow`].
pub enum SyncedField<'a, F> {
    Live(F),
    Cached(OwnedField<'a>),
}

impl<'a, F> Field<'a, Pg> for SyncedField<'a, F>
where
    F: Field<'a, Pg>,
{
    /// The column's name.
    fn field_name(&self) -> Option<&str> {
        match self {
            SyncedField::Live(field) => field.field_name(),
            SyncedField::Cached(field) => field.field_name(),
        }
    }

    /// The column's value; `None` for `NULL`.
    fn value(&self) -> Option<PgValue<'_>> {
        match self {
            SyncedField::Live(field) => field.value(),
            SyncedField::Cached(field) => field.value(),
        }
    }
}
