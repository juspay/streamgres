//! Cached answer metadata and Postgres binary encoding.
//! Shared row composition lives in [`crate::compose`].

use std::ops::Range;
use std::sync::Arc;

use super::Refusal;
use crate::compose::{Joined, window};
use crate::model::{ColumnName, DbTable, Value};
use crate::parser::{Projection, Select};
use crate::sync::pg::binary::{self, oid};

/// One column of an answer: its name and its type at the source, what a
/// client needs to read the values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CachedColumn {
    pub name: ColumnName,
    pub type_oid: u32,
    /// Whether the type is an enum, whose values encode as their labels.
    pub enum_type: bool,
}

/// An answer: its columns and its rows, each row's values in column
/// order. The rows are a window on the projection the cache keeps, shared
/// by every read of it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CachedRows {
    pub columns: Arc<[CachedColumn]>,
    all: Arc<[Box<[Value]>]>,
    window: Range<usize>,
}

impl CachedRows {
    /// A copy owning its rows, for one thread to keep
    /// ([`super::local`]): its reads then write no shared reference
    /// count. `None` when the rows weigh more than `budget`.
    pub(super) fn local_copy(&self, budget: usize) -> Option<Self> {
        let bytes = self
            .rows()
            .iter()
            .fold(0usize, |bytes, row| {
                bytes.saturating_add(16 + row.iter().map(super::state::value_bytes).sum::<usize>())
            })
            .saturating_add(
                self.columns
                    .iter()
                    .map(|column| 32 + column.name.as_str().len())
                    .sum::<usize>(),
            );
        if bytes > budget {
            return None;
        }
        let rows = self.rows().to_vec();
        Some(Self {
            columns: self.columns.to_vec().into(),
            window: 0..rows.len(),
            all: rows.into(),
        })
    }

    /// The rows, in the statement's order.
    pub fn rows(&self) -> &[Box<[Value]>] {
        self.all.get(self.window.clone()).unwrap_or_default()
    }
}

/// One column of a [`Rows`]: its name and the OID of its type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    name: String,
    type_oid: u32,
}

impl Column {
    pub(crate) fn new(name: impl Into<String>, type_oid: u32) -> Self {
        Column {
            name: name.into(),
            type_oid,
        }
    }

    /// The column's name, as the statement's result names it.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The OID of the column's type at the source (a domain's base type).
    pub fn type_oid(&self) -> u32 {
        self.type_oid
    }
}

/// The rows a read returns, each value in Postgres's binary format for
/// its column's type: the same bytes Postgres sends for the statement,
/// whether the rows came from memory or from Postgres.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rows {
    columns: Arc<[Column]>,
    buffer: Vec<u8>,
    /// Each row's cells, `columns.len()` to a row: a range of `buffer`,
    /// `None` for `NULL`.
    cells: Vec<Option<Range<usize>>>,
}

impl Rows {
    pub(crate) fn from_parts(
        columns: Arc<[Column]>,
        buffer: Vec<u8>,
        cells: Vec<Option<Range<usize>>>,
    ) -> Self {
        Rows {
            columns,
            buffer,
            cells,
        }
    }

    /// The columns, in the statement's order.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// How many rows there are.
    pub fn len(&self) -> usize {
        self.cells
            .len()
            .checked_div(self.columns.len())
            .unwrap_or(0)
    }

    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The row at `index`.
    pub fn row(&self, index: usize) -> Option<Row<'_>> {
        (index < self.len()).then_some(Row { rows: self, index })
    }

    /// The rows in order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Row<'_>> {
        (0..self.len()).map(move |index| Row { rows: self, index })
    }

    /// The buffer every cell is a range of, and each row's cells: what an
    /// adapter hands its driver without copying.
    pub fn raw(&self) -> (&[u8], &[Option<Range<usize>>]) {
        (&self.buffer, &self.cells)
    }
}

/// One row of a [`Rows`].
#[derive(Debug, Clone, Copy)]
pub struct Row<'a> {
    rows: &'a Rows,
    index: usize,
}

impl<'a> Row<'a> {
    /// The value of column `column` in Postgres's binary format; `None`
    /// for `NULL` or a column the row does not have.
    pub fn bytes(&self, column: usize) -> Option<&'a [u8]> {
        let width = self.rows.columns.len();
        if column >= width {
            return None;
        }
        let range = self.rows.cells.get(self.index * width + column)?.clone()?;
        self.rows.buffer.get(range)
    }

    /// The cells of this row, as ranges of [`Rows::raw`]'s buffer.
    pub fn cells(&self) -> &'a [Option<Range<usize>>] {
        let width = self.rows.columns.len();
        let start = self.index * width;
        self.rows
            .cells
            .get(start..start + width)
            .unwrap_or_default()
    }
}

/// `rows` in Postgres's binary format: each value encoded for its column's
/// type into one buffer the rows share.
pub(super) fn encode(rows: &CachedRows) -> Result<Rows, Refusal> {
    let columns: Arc<[Column]> = rows
        .columns
        .iter()
        .map(|column| Column::new(column.name.as_str(), column.type_oid))
        .collect();
    let mut buffer = Vec::new();
    let mut cells = Vec::with_capacity(rows.rows().len() * columns.len());
    for row in rows.rows() {
        for (value, column) in row.iter().zip(rows.columns.iter()) {
            if value.is_null() {
                cells.push(None);
                continue;
            }
            let start = buffer.len();
            if !binary::encode_into(value, column.type_oid, column.enum_type, &mut buffer) {
                return Err(Refusal::Unavailable(format!(
                    "a value of `{}` does not encode as its column's type",
                    column.name
                )));
            }
            cells.push(Some(start..buffer.len()));
        }
    }
    Ok(Rows::from_parts(columns, buffer, cells))
}

/// A column of one of a statement's tables: the table's node, and its name.
type NodeColumn = (usize, ColumnName);

/// The columns `select` returns, each by its node and name, and as an
/// answer names them; a refusal when one is unknown or of a type the engine
/// does not hold exactly. A count returns none of the tables' columns.
pub(super) fn projected_columns(
    select: &Select,
    tables: &[&DbTable],
) -> Result<(Vec<NodeColumn>, Vec<CachedColumn>), Refusal> {
    let projected: Vec<NodeColumn> = match &select.projection {
        Projection::Columns(columns) => columns
            .iter()
            .map(|column| (column.node, column.column.clone()))
            .collect(),
        Projection::All => tables
            .iter()
            .enumerate()
            .flat_map(|(node, table)| {
                table
                    .declared()
                    .iter()
                    .map(move |column| (node, column.clone()))
            })
            .collect(),
        Projection::CountStar => Vec::new(),
    };
    let mut columns = Vec::with_capacity(projected.len());
    for (node, name) in &projected {
        let column = tables
            .get(*node)
            .and_then(|table| table.column(name.as_str()))
            .ok_or_else(|| Refusal::Unsupported(format!("unknown column `{name}`")))?;
        // These types are admitted for the incoming exact-value engine work.
        // Until its codecs are available, encoding still chooses fallback.
        let pending_exact = matches!(
            column.type_oid,
            oid::NUMERIC | oid::TIMESTAMP | oid::TIMESTAMPTZ
        );
        if !pending_exact && !binary::exact(column) {
            return Err(Refusal::Unsupported(format!(
                "`{name}` is of a type the engine does not hold exactly as Postgres does"
            )));
        }
        columns.push(CachedColumn {
            name: name.clone(),
            type_oid: column.type_oid,
            enum_type: column.enum_type,
        });
    }
    Ok((projected, columns))
}

/// A statement's columns of every joined row of its query, in order:
/// what every read of the statement slices.
#[derive(Debug)]
pub(super) struct Projected {
    columns: Arc<[CachedColumn]>,
    rows: Arc<[Box<[Value]>]>,
    bytes: usize,
}

impl Projected {
    /// What the projection weighs in the cache's budget.
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }
}

/// The answer to a `COUNT(*)` that matched `count` rows.
pub(super) fn count(count: usize, select: &Select) -> Result<CachedRows, Refusal> {
    let count = i64::try_from(count)
        .map_err(|_| Refusal::Unavailable("the count is out of range".to_owned()))?;
    Ok(slice(&project_count(count), select))
}

pub(super) fn project_count(count: i64) -> Projected {
    let all: Arc<[Box<[Value]>]> = Arc::new([Box::new([Value::Int(count)]) as Box<[Value]>]);
    Projected {
        columns: Arc::new([CachedColumn {
            name: ColumnName::from("count"),
            type_oid: oid::INT8,
            enum_type: false,
        }]),
        rows: all,
        bytes: 128,
    }
}

/// The read of `select` from its projection.
pub(super) fn slice(projected: &Projected, select: &Select) -> CachedRows {
    CachedRows {
        columns: projected.columns.clone(),
        all: projected.rows.clone(),
        window: window(projected.rows.len(), select),
    }
}

/// `select`'s columns of every row of `joined`, which is in its order.
/// `tables` are the `FROM`'s, in its order. Not for a `COUNT(*)`.
pub(super) fn project(
    joined: &[Joined],
    select: &Select,
    tables: &[&DbTable],
    max_bytes: usize,
) -> Result<Projected, Refusal> {
    let (projected, columns) = projected_columns(select, tables)?;
    let metadata_bytes = columns
        .iter()
        .map(|column| std::mem::size_of::<CachedColumn>() + column.name.as_str().len())
        .sum::<usize>();
    let (rows, bytes) = crate::compose::project(joined, &projected, metadata_bytes, max_bytes)
        .map_err(|error| Refusal::Unavailable(error.to_string()))?;
    Ok(Projected {
        columns: columns.into(),
        rows,
        bytes,
    })
}
