//! A cached row as Diesel reads one: each field's bytes in Postgres's
//! binary format with its type's OID, so a cached row deserializes
//! through the same `FromSql` as a row from Postgres.

use std::num::NonZeroU32;
use std::ops::Range;

use crate::diesel::pg::{Pg, PgValue};
use crate::diesel::row::{Field, PartialRow, Row, RowIndex, RowSealed};

/// One row: its columns' names and types, and each cell's bytes.
pub(crate) struct CachedRow<'a> {
    names: &'a [&'a str],
    oids: &'a [NonZeroU32],
    cells: &'a [Option<Vec<u8>>],
}

impl<'a> CachedRow<'a> {
    pub(crate) fn new(
        names: &'a [&'a str],
        oids: &'a [NonZeroU32],
        cells: &'a [Option<Vec<u8>>],
    ) -> Self {
        CachedRow { names, oids, cells }
    }
}

/// One field of a [`CachedRow`].
pub(crate) struct CachedField<'f> {
    name: &'f str,
    oid: &'f NonZeroU32,
    bytes: Option<&'f [u8]>,
}

impl RowSealed for CachedRow<'_> {}

impl<'a> Row<'a, Pg> for CachedRow<'a> {
    type Field<'f>
        = CachedField<'f>
    where
        'a: 'f,
        Self: 'f;
    type InnerPartialRow = Self;

    fn field_count(&self) -> usize {
        self.cells.len()
    }

    fn get<'b, I>(&'b self, idx: I) -> Option<Self::Field<'b>>
    where
        'a: 'b,
        Self: RowIndex<I>,
    {
        let index = self.idx(idx)?;
        Some(CachedField {
            name: self.names.get(index)?,
            oid: self.oids.get(index)?,
            bytes: self.cells.get(index)?.as_deref(),
        })
    }

    fn partial_row(&self, range: Range<usize>) -> PartialRow<'_, Self::InnerPartialRow> {
        PartialRow::new(self, range)
    }
}

impl RowIndex<usize> for CachedRow<'_> {
    fn idx(&self, idx: usize) -> Option<usize> {
        (idx < self.cells.len()).then_some(idx)
    }
}

impl<'n> RowIndex<&'n str> for CachedRow<'_> {
    fn idx(&self, field_name: &'n str) -> Option<usize> {
        self.names.iter().position(|name| *name == field_name)
    }
}

impl<'f> Field<'f, Pg> for CachedField<'f> {
    fn field_name(&self) -> Option<&str> {
        Some(self.name)
    }

    fn value(&self) -> Option<PgValue<'_>> {
        self.bytes.map(|bytes| PgValue::new(bytes, self.oid))
    }
}
