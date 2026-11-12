//! A read's answer, built from the rows a query keeps: in the statement's
//! order, its offset skipped, its limit applied, its projection taken.

use std::collections::HashMap;

use super::Refusal;
use crate::ivm::order_rows;
use crate::model::{ColumnName, DataFrameKey, DataFrameRow, DbTable, Value};
use crate::parser::{Projection, Select};
use crate::sync::pg::binary::oid;

/// One column of an answer: its name and its type at the source (and its
/// element type, for an array), what a client needs to read the values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedColumn {
    pub name: ColumnName,
    pub type_oid: u32,
    pub element_oid: u32,
}

/// An answer: its columns and its rows, each row's values in column
/// order.
#[derive(Debug, Clone, PartialEq)]
pub struct CachedRows {
    pub columns: Vec<CachedColumn>,
    pub rows: Vec<Vec<Value>>,
}

/// The answer to `select` over `table` from the rows its query keeps.
pub(super) fn build(
    kept: &HashMap<DataFrameKey, DataFrameRow>,
    select: &Select,
    table: &DbTable,
) -> Result<CachedRows, Refusal> {
    let start = usize::try_from(select.offset).unwrap_or(usize::MAX);
    let take = usize::try_from(select.limit).unwrap_or(usize::MAX);
    if select.projection == Projection::CountStar {
        let count = i64::try_from(kept.len())
            .map_err(|_| Refusal::Unsupported("the count is out of range".to_owned()))?;
        let rows = std::iter::once(vec![Value::Int(count)])
            .skip(start)
            .take(take)
            .collect();
        return Ok(CachedRows {
            columns: vec![CachedColumn {
                name: ColumnName::from("count"),
                type_oid: oid::INT8,
                element_oid: 0,
            }],
            rows,
        });
    }

    let names: Vec<ColumnName> = match &select.projection {
        Projection::Columns(names) => names.clone(),
        Projection::All | Projection::CountStar => table.declared().to_vec(),
    };
    let mut columns = Vec::with_capacity(names.len());
    for name in &names {
        let column = table
            .column(name.as_str())
            .ok_or_else(|| Refusal::Unsupported(format!("unknown column `{name}`")))?;
        if column.type_oid == 0 {
            return Err(Refusal::Unsupported(format!(
                "the type of `{name}` at the source is not known"
            )));
        }
        columns.push(CachedColumn {
            name: name.clone(),
            type_oid: column.type_oid,
            element_oid: column.element_oid,
        });
    }

    let mut ordered: Vec<(&DataFrameKey, &DataFrameRow)> = kept.iter().collect();
    ordered.sort_by(|a, b| {
        order_rows(&select.query.order_by, a.1, b.1)
            .then_with(|| crate::ivm::order_rows(&key_order(table), a.1, b.1))
    });
    let rows = ordered
        .into_iter()
        .skip(start)
        .take(take)
        .map(|(_, row)| {
            names
                .iter()
                .map(|name| row.data.get(name).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect();
    Ok(CachedRows { columns, rows })
}

/// The table's primary key ascending, which settles rows the statement's
/// order ties, so a repeated read returns them the same way.
fn key_order(table: &DbTable) -> Vec<crate::model::OrderBy> {
    table
        .pkey
        .iter()
        .map(|column| crate::model::OrderBy::new(column.clone(), crate::model::Order::ASC))
        .collect()
}
