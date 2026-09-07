//! The storage boundary: where the engine reads rows it does not yet hold.
//!
//! Join maintenance fetches rows on demand (a join key becoming referenced
//! pulls the other side's rows in), so the engine needs a way to run a
//! single-table `SELECT` against the source of truth. [`Storage`] is that
//! seam: [`PgStorage`] is the Postgres implementation (a stub until the
//! Diesel connector lands — reads only, no writes ever go through here),
//! and [`MemoryStorage`] is an in-process implementation that makes the
//! join logic fully testable today.

use std::cell::RefCell;
use std::collections::HashMap;

use super::predicate::evaluate;
use super::window::order_cmp;
use crate::model::{
    DataFrameKey, DataFrameRow, Order, SingleTableReadQuery, TableName, Value, WriteQuery,
};

/// Read-only access to the source of truth for row data.
pub trait Storage {
    /// Rows of `query.table` whose full row image satisfies `query.filter`,
    /// as (identity, image) pairs — the same vocabulary the engine's
    /// operations speak. When `limit` is finite the result must honor
    /// `order_by` and `limit` (the engine's window maintenance depends on
    /// getting the *best* rows); an unlimited query may return rows in any
    /// order.
    fn select(&self, query: &SingleTableReadQuery) -> Vec<(DataFrameKey, DataFrameRow)>;
}

/// Postgres-backed storage. Stub until the Diesel connector lands: selects
/// return nothing, so fetches find no rows and joins stay empty when this
/// backend is used.
pub struct PgStorage;

impl Storage for PgStorage {
    /// Stub: always empty.
    fn select(&self, _query: &SingleTableReadQuery) -> Vec<(DataFrameKey, DataFrameRow)> {
        Vec::new()
    }
}

/// In-process storage: plain tables of rows, kept in insertion order so
/// selects (and therefore fetch-emitted operations) are deterministic.
///
/// Interior mutability lets tests hold a shared handle and mirror every
/// write into storage *before* notifying the engine — the same order of
/// events a real database produces (commit first, change feed second).
#[derive(Default)]
pub struct MemoryStorage {
    tables: RefCell<HashMap<TableName, Vec<(DataFrameKey, DataFrameRow)>>>,
}

impl MemoryStorage {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mirror one write into the store: insert/update upsert the row by
    /// primary key (full-row-image semantics, like the engine), delete
    /// removes it.
    pub fn apply(&self, write: &WriteQuery) {
        let mut tables = self.tables.borrow_mut();
        let rows = tables.entry(write.table().clone()).or_default();
        let position = rows.iter().position(|(key, _)| key == write.pkey_value());
        match write.new_row_image() {
            Some(image) => {
                let entry = (write.pkey_value().clone(), image.clone());
                match position {
                    Some(index) => rows[index] = entry,
                    None => rows.push(entry),
                }
            }
            None => {
                if let Some(index) = position {
                    rows.remove(index);
                }
            }
        }
    }
}

impl Storage for MemoryStorage {
    /// Linear scan of the table, filtering by the query's `Where` via the
    /// same predicate evaluation the engine routes with. A finite `limit`
    /// sorts by the `order_by` column (via the window module's total
    /// order) and truncates; an unlimited query keeps insertion order.
    fn select(&self, query: &SingleTableReadQuery) -> Vec<(DataFrameKey, DataFrameRow)> {
        let tables = self.tables.borrow();
        let Some(rows) = tables.get(&query.table) else {
            return Vec::new();
        };
        let mut selected: Vec<(DataFrameKey, DataFrameRow)> = rows
            .iter()
            .filter(|(_, row)| evaluate(&query.filter, &row.data, &mut 0))
            .cloned()
            .collect();
        if query.limit != u32::MAX {
            let column = query.order_by.column.as_str();
            selected.sort_by(|(_, a), (_, b)| {
                let ordering = order_cmp(
                    a.data.get(column).unwrap_or(&Value::Null),
                    b.data.get(column).unwrap_or(&Value::Null),
                );
                match query.order_by.direction {
                    Order::ASC => ordering,
                    Order::DESC => ordering.reverse(),
                }
            });
            selected.truncate(query.limit as usize);
        }
        selected
    }
}
