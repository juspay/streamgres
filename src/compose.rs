//! Shared result composition over engine row snapshots.
//!
//! Cache admission, retention, source encoding and client delivery belong to
//! callers. Comparisons retain the engine's existing value semantics.
use crate::ivm::order_cmp;
use crate::model::{ColumnName, DataFrameRow, DbTable, Order, RowData, Value};
use crate::parser::Select;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) enum Error {
    JoinBudget,
    ProjectionBudget,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::JoinBudget => "the joined result exceeds its row or byte budget",
            Self::ProjectionBudget => "the projection exceeds its byte budget",
        })
    }
}
impl std::error::Error for Error {}

/// One row of the join: a row of each table of the `FROM`, in its order,
/// `None` where a `LEFT` join found none.
pub(crate) type Joined = Box<[Option<Arc<DataFrameRow>>]>;

/// The rows of each table of the `FROM`, in its order, as the query kept
/// them.
pub(crate) type Snapshot = Vec<Vec<Arc<DataFrameRow>>>;

/// The statement's joined rows from `kept`, in its order, ties settled by
/// each table's primary key ascending so a repeated read returns them the
/// same way. `tables` are the `FROM`'s, in its order.
pub(crate) fn compose(
    kept: Snapshot,
    select: &Select,
    tables: &[&DbTable],
    max_bytes: usize,
    max_rows: usize,
) -> Result<Arc<[Joined]>, Error> {
    let count = tables.len();
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); count];
    for (index, node) in select.nodes.iter().enumerate() {
        if let Some(edge) = &node.edge {
            children[edge.parent].push(index);
        }
    }
    let roots = kept.get(select.root).cloned().unwrap_or_default();
    let by_value: Vec<HashMap<Value, Vec<Arc<DataFrameRow>>>> = kept
        .into_iter()
        .zip(&select.nodes)
        .map(|(rows, node)| {
            let mut index: HashMap<Value, Vec<Arc<DataFrameRow>>> = HashMap::new();
            if let Some(edge) = &node.edge {
                for row in rows {
                    if let Some(key) = edge
                        .column
                        .of(&row.data)
                        .and_then(|value| value.equality_key())
                    {
                        index.entry(key).or_default().push(row);
                    }
                }
            }
            index
        })
        .collect();

    let row_bytes = std::mem::size_of::<Joined>()
        .saturating_add(count.saturating_mul(std::mem::size_of::<Option<Arc<DataFrameRow>>>()));
    let limit = max_rows.min(max_bytes / row_bytes.max(1));
    let mut joined = Vec::with_capacity(roots.len().min(limit));
    let empty = RowData::from(HashMap::new());
    let mut visited = 0usize;
    for root in roots {
        let mut start: Vec<Option<Arc<DataFrameRow>>> = vec![None; count];
        start[select.root] = Some(root);
        expand(
            select.root,
            start,
            select,
            &children,
            &by_value,
            &mut |row| {
                if visited >= limit {
                    return Err(Error::JoinBudget);
                }
                visited += 1;
                if select.post_filters.iter().all(|(node, filter)| {
                    let data = row
                        .get(*node)
                        .and_then(Option::as_ref)
                        .map_or(&empty, |row| &row.data);
                    crate::ivm::evaluate(filter, data, &mut 0)
                }) {
                    joined.push(row.into_boxed_slice());
                }
                Ok(())
            },
        )?;
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
    Ok(joined.into())
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
    emit: &mut dyn FnMut(Vec<Option<Arc<DataFrameRow>>>) -> Result<(), Error>,
) -> Result<(), Error> {
    let mut visit = |row| expand_children(node, 0, row, select, children, by_value, emit);
    visit(partial)
}

/// Enumerates one combination at a time. A fan-out join never builds an
/// intermediate Cartesian product before the output budget is checked.
fn expand_children(
    node: usize,
    at: usize,
    row: Vec<Option<Arc<DataFrameRow>>>,
    select: &Select,
    children: &[Vec<usize>],
    by_value: &[HashMap<Value, Vec<Arc<DataFrameRow>>>],
    emit: &mut dyn FnMut(Vec<Option<Arc<DataFrameRow>>>) -> Result<(), Error>,
) -> Result<(), Error> {
    let Some(&child) = children[node].get(at) else {
        return emit(row);
    };
    let Some(edge) = select.nodes[child].edge.as_ref() else {
        return Ok(());
    };
    let matches = row[node]
        .as_ref()
        .and_then(|parent| edge.parent_column.of(&parent.data))
        .and_then(|value| value.equality_key())
        .and_then(|key| by_value[child].get(&key));
    let mut child_exists = false;
    for matched in matches.into_iter().flatten() {
        let mut with = row.clone();
        with[child] = Some(matched.clone());
        expand(child, with, select, children, by_value, &mut |branch| {
            child_exists = true;
            expand_children(node, at + 1, branch, select, children, by_value, emit)
        })?;
    }
    if !child_exists && !edge.inner {
        expand_children(node, at + 1, row, select, children, by_value, emit)?;
    }
    Ok(())
}

/// The shallow heap retained for a composed join; row images are counted
/// in the entry's parts already.
pub(crate) fn ordered_bytes(rows: &[Joined]) -> usize {
    rows.iter().fold(std::mem::size_of_val(rows), |total, row| {
        total.saturating_add(std::mem::size_of_val(row.as_ref()))
    })
}

/// The window of `len` rows `select`'s offset and limit leave.
pub(crate) fn window(len: usize, select: &Select) -> Range<usize> {
    let start = usize::try_from(select.offset)
        .unwrap_or(usize::MAX)
        .min(len);
    let take = usize::try_from(select.limit).unwrap_or(usize::MAX);
    start..start.saturating_add(take).min(len)
}

pub(crate) type Values = Arc<[Box<[Value]>]>;

/// Project values, including caller-owned column metadata in the budget.
pub(crate) fn project(
    joined: &[Joined],
    projected: &[(usize, ColumnName)],
    mut bytes: usize,
    max_bytes: usize,
) -> Result<(Values, usize), Error> {
    let rows = joined
        .iter()
        .map(|row| {
            let weight = projected.iter().fold(
                std::mem::size_of::<Box<[Value]>>(),
                |weight, (node, name)| {
                    weight.saturating_add(
                        row.get(*node)
                            .and_then(Option::as_ref)
                            .and_then(|row| row.data.get(name))
                            .map_or(std::mem::size_of::<Value>(), value_bytes),
                    )
                },
            );
            bytes = bytes.saturating_add(weight);
            if bytes > max_bytes {
                return Err(Error::ProjectionBudget);
            }
            projected
                .iter()
                .map(|(node, name)| {
                    match row
                        .get(*node)
                        .and_then(Option::as_ref)
                        .and_then(|row| row.data.get(name))
                    {
                        Some(value) => Ok(value.clone()),
                        None => Ok(Value::Null),
                    }
                })
                .collect::<Result<Box<[Value]>, _>>()
        })
        .collect::<Result<Arc<[Box<[Value]>]>, _>>()?;
    Ok((rows, bytes))
}

/// What a value weighs: its inline size and what it points at.
pub(crate) fn value_bytes(value: &Value) -> usize {
    let inline = std::mem::size_of::<Value>();
    inline
        + match value {
            Value::String(text) => text.len(),
            Value::List(items) => items.iter().map(value_bytes).sum(),
            _ => 0,
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Catalog, DbColumn, ValueType};

    #[test]
    fn fanout_is_stopped_before_a_cartesian_product_is_built() {
        let catalog = Catalog::new(vec![
            DbTable::new("a", ["id"], vec![DbColumn::new("id", ValueType::Int)]),
            DbTable::new(
                "b",
                ["id"],
                vec![
                    DbColumn::new("id", ValueType::Int),
                    DbColumn::new("a_id", ValueType::Int),
                ],
            ),
            DbTable::new(
                "c",
                ["id"],
                vec![
                    DbColumn::new("id", ValueType::Int),
                    DbColumn::new("a_id", ValueType::Int),
                ],
            ),
        ]);
        let select = crate::parser::parse_select(
            "SELECT a.id FROM a JOIN b ON b.a_id = a.id JOIN c ON c.a_id = a.id",
            &[],
            &catalog,
        )
        .unwrap();
        let row = |id, parent| {
            Arc::new(DataFrameRow::from(HashMap::from([
                ("id".into(), Value::Int(id)),
                ("a_id".into(), Value::Int(parent)),
            ])))
        };
        let snapshot = vec![
            vec![row(1, 0)],
            (0..1000).map(|n| row(n, 1)).collect(),
            (0..1000).map(|n| row(n, 1)).collect(),
        ];
        let tables: Vec<_> = select
            .nodes
            .iter()
            .map(|node| catalog.table(node.table.as_str()).unwrap())
            .collect();
        let answer = compose(snapshot, &select, &tables, 4096, 20);
        assert!(matches!(answer, Err(Error::JoinBudget)));
    }
}
