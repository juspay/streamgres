//! A read's answer, built from the rows a query keeps: in the statement's
//! order, its offset skipped, its limit applied, its projection taken.
//!
//! The ordering is the costly part, so it is done once per change of a
//! query's rows, not once per read: [`ordered`] sorts a snapshot of the
//! rows, the entry keeps the result until its rows change again, and each
//! read only slices it ([`answer`]).

use std::sync::Arc;

use super::Refusal;
use crate::ivm::order_rows;
use crate::model::{ColumnName, DataFrameRow, DbTable, Order, OrderBy, Value};
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

/// `rows` in `order`, ties settled by `table`'s primary key ascending so a
/// repeated read returns them the same way.
pub(super) fn ordered(
    mut rows: Vec<Arc<DataFrameRow>>,
    order: &[OrderBy],
    table: &DbTable,
) -> Arc<[Arc<DataFrameRow>]> {
    let by_key: Vec<OrderBy> = table
        .pkey
        .iter()
        .map(|column| OrderBy::new(column.clone(), Order::ASC))
        .collect();
    rows.sort_unstable_by(|a, b| order_rows(order, a, b).then_with(|| order_rows(&by_key, a, b)));
    rows.into()
}

/// The answer to `select` over `table` from its query's rows in order,
/// `count` of them in all.
pub(super) fn answer(
    ordered: &[Arc<DataFrameRow>],
    count: usize,
    select: &Select,
    table: &DbTable,
) -> Result<CachedRows, Refusal> {
    let start = usize::try_from(select.offset).unwrap_or(usize::MAX);
    let take = usize::try_from(select.limit).unwrap_or(usize::MAX);
    if select.projection == Projection::CountStar {
        let count = i64::try_from(count)
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

    let names: &[ColumnName] = match &select.projection {
        Projection::Columns(names) => names,
        Projection::All | Projection::CountStar => table.declared(),
    };
    let mut columns = Vec::with_capacity(names.len());
    for name in names {
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
    let rows = ordered
        .iter()
        .skip(start)
        .take(take)
        .map(|row| {
            names
                .iter()
                .map(|name| match row.data.get(name) {
                    Some(Value::Json(json)) if !json.is_known() => Err(Refusal::Unsupported(
                        format!("the text Postgres has for `{name}` is not known"),
                    )),
                    Some(value) => Ok(value.clone()),
                    None => Ok(Value::Null),
                })
                .collect()
        })
        .collect::<Result<_, _>>()?;
    Ok(CachedRows { columns, rows })
}
