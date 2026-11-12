//! A read's answer, built from the rows a query keeps: the tables joined,
//! in the statement's order, its offset skipped, its limit applied, its
//! projection taken.
//!
//! Joining, ordering and projecting are the costly part, so they are done
//! once per change of a query's rows, not once per read: [`compose`] joins
//! and sorts a snapshot of the rows, [`project`] takes a statement's
//! columns of every joined row, the entry keeps both until its rows change
//! again, and each read only slices the projection ([`slice`]).

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use super::Refusal;
use crate::ivm::order_cmp;
use crate::model::{ColumnName, DataFrameRow, DbTable, Order, Value};
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
/// order. The rows are a window on the projection the cache keeps, shared
/// by every read of it.
#[derive(Debug, Clone, PartialEq)]
pub struct CachedRows {
    pub columns: Arc<[CachedColumn]>,
    all: Arc<[Box<[Value]>]>,
    window: Range<usize>,
}

impl CachedRows {
    /// The rows, in the statement's order.
    pub fn rows(&self) -> &[Box<[Value]>] {
        self.all.get(self.window.clone()).unwrap_or_default()
    }
}

/// A statement's columns of every joined row of its query, in order:
/// what every read of the statement slices.
#[derive(Debug)]
pub(super) struct Projected {
    columns: Arc<[CachedColumn]>,
    rows: Arc<[Box<[Value]>]>,
}

/// One row of the join: a row of each table of the `FROM`, in its order,
/// `None` where a `LEFT` join found none.
pub(super) type Joined = Box<[Option<Arc<DataFrameRow>>]>;

/// The rows of each table of the `FROM`, in its order, as the query kept
/// them.
pub(super) type Kept = Vec<Vec<Arc<DataFrameRow>>>;

/// The statement's joined rows from `kept`, in its order, ties settled by
/// each table's primary key ascending so a repeated read returns them the
/// same way. `tables` are the `FROM`'s, in its order.
pub(super) fn compose(kept: Kept, select: &Select, tables: &[&DbTable]) -> Arc<[Joined]> {
    let count = tables.len();
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); count];
    for (index, node) in select.nodes.iter().enumerate() {
        if let Some(edge) = &node.edge {
            children[edge.parent].push(index);
        }
    }
    let mut kept = kept.into_iter();
    let roots = kept.next().unwrap_or_default();
    let by_value: Vec<HashMap<Value, Vec<Arc<DataFrameRow>>>> = std::iter::once(HashMap::new())
        .chain(kept.zip(select.nodes.iter().skip(1)).map(|(rows, node)| {
            let mut index: HashMap<Value, Vec<Arc<DataFrameRow>>> = HashMap::new();
            if let Some(edge) = &node.edge {
                for row in rows {
                    if let Some(key) = row.data.get(&edge.column).and_then(Value::equality_key) {
                        index.entry(key).or_default().push(row);
                    }
                }
            }
            index
        }))
        .collect();

    let mut joined = Vec::with_capacity(roots.len());
    for root in roots {
        let mut start: Vec<Option<Arc<DataFrameRow>>> = vec![None; count];
        start[0] = Some(root);
        joined.extend(expand(0, start, select, &children, &by_value));
    }
    let key_order: Vec<(usize, ColumnName)> = tables
        .iter()
        .enumerate()
        .flat_map(|(node, table)| table.pkey.iter().map(move |column| (node, column.clone())))
        .collect();
    let value = |row: &Joined, node: usize, column: &ColumnName| -> Value {
        row.get(node)
            .and_then(Option::as_ref)
            .and_then(|row| row.data.get(column).cloned())
            .unwrap_or(Value::Null)
    };
    joined.sort_unstable_by(|a, b| {
        for clause in &select.order {
            let ordering = order_cmp(
                &value(a, clause.node, &clause.column),
                &value(b, clause.node, &clause.column),
            );
            let ordering = match clause.direction {
                Order::ASC => ordering,
                Order::DESC => ordering.reverse(),
            };
            if ordering.is_ne() {
                return ordering;
            }
        }
        for (node, column) in &key_order {
            let ordering = order_cmp(&value(a, *node, column), &value(b, *node, column));
            if ordering.is_ne() {
                return ordering;
            }
        }
        std::cmp::Ordering::Equal
    });
    joined.into()
}

/// The joined rows `partial` extends to under `node`, whose row it holds:
/// every combination of the rows each child joins. A child that joins no
/// row leaves nulls under a `LEFT` join and removes the row under an
/// `INNER` one, as SQL joins them.
fn expand(
    node: usize,
    partial: Vec<Option<Arc<DataFrameRow>>>,
    select: &Select,
    children: &[Vec<usize>],
    by_value: &[HashMap<Value, Vec<Arc<DataFrameRow>>>],
) -> Vec<Joined> {
    let mut rows = vec![partial];
    for &child in &children[node] {
        let Some(edge) = select.nodes[child].edge.as_ref() else {
            continue;
        };
        let mut next = Vec::new();
        for row in rows {
            let matches = row[node]
                .as_ref()
                .and_then(|parent| parent.data.get(&edge.parent_column))
                .and_then(Value::equality_key)
                .and_then(|key| by_value[child].get(&key));
            let mut extended = Vec::new();
            for matched in matches.into_iter().flatten() {
                let mut with = row.clone();
                with[child] = Some(matched.clone());
                extended.extend(
                    expand(child, with, select, children, by_value)
                        .into_iter()
                        .map(Vec::from),
                );
            }
            if extended.is_empty() && !edge.inner {
                extended.push(row);
            }
            next.extend(extended);
        }
        rows = next;
    }
    rows.into_iter().map(Vec::into_boxed_slice).collect()
}

/// The window of `len` rows `select`'s offset and limit leave.
fn window(len: usize, select: &Select) -> Range<usize> {
    let start = usize::try_from(select.offset).unwrap_or(usize::MAX).min(len);
    let take = usize::try_from(select.limit).unwrap_or(usize::MAX);
    start..start.saturating_add(take).min(len)
}

/// The answer to a `COUNT(*)` that matched `count` rows.
pub(super) fn count(count: usize, select: &Select) -> Result<CachedRows, Refusal> {
    let count = i64::try_from(count)
        .map_err(|_| Refusal::Unsupported("the count is out of range".to_owned()))?;
    let all: Arc<[Box<[Value]>]> = Arc::new([Box::new([Value::Int(count)]) as Box<[Value]>]);
    Ok(CachedRows {
        columns: Arc::new([CachedColumn {
            name: ColumnName::from("count"),
            type_oid: oid::INT8,
            element_oid: 0,
        }]),
        window: window(all.len(), select),
        all,
    })
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
) -> Result<Projected, Refusal> {
    let projected: Vec<(usize, ColumnName)> = match &select.projection {
        Projection::Columns(columns) => columns
            .iter()
            .map(|column| (column.node, column.column.clone()))
            .collect(),
        Projection::All | Projection::CountStar => tables
            .iter()
            .enumerate()
            .flat_map(|(node, table)| {
                table
                    .declared()
                    .iter()
                    .map(move |column| (node, column.clone()))
            })
            .collect(),
    };
    let mut columns = Vec::with_capacity(projected.len());
    for (node, name) in &projected {
        let column = tables
            .get(*node)
            .and_then(|table| table.column(name.as_str()))
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
    let rows = joined
        .iter()
        .map(|row| {
            projected
                .iter()
                .map(|(node, name)| {
                    match row
                        .get(*node)
                        .and_then(Option::as_ref)
                        .and_then(|row| row.data.get(name))
                    {
                        Some(Value::Json(json)) if !json.is_known() => Err(Refusal::Unsupported(
                            format!("the text Postgres has for `{name}` is not known"),
                        )),
                        Some(value) => Ok(value.clone()),
                        None => Ok(Value::Null),
                    }
                })
                .collect::<Result<Box<[Value]>, _>>()
        })
        .collect::<Result<_, _>>()?;
    Ok(Projected {
        columns: columns.into(),
        rows,
    })
}
