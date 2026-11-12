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
    /// Private per-thread ownership removes shared Arc counter writes from
    /// repeated reads of the same key. Copy only a bounded result window.
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

/// One row of the join: a row of each table of the `FROM`, in its order,
/// `None` where a `LEFT` join found none.
pub(super) type Joined = Box<[Option<Arc<DataFrameRow>>]>;

/// The rows of each table of the `FROM`, in its order, as the query kept
/// them.
pub(super) type Snapshot = Vec<Vec<Arc<DataFrameRow>>>;

/// The statement's joined rows from `kept`, in its order, ties settled by
/// each table's primary key ascending so a repeated read returns them the
/// same way. `tables` are the `FROM`'s, in its order.
pub(super) fn compose(
    kept: Snapshot,
    select: &Select,
    tables: &[&DbTable],
    max_bytes: usize,
    max_rows: usize,
) -> Result<Arc<[Joined]>, Refusal> {
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

    let row_bytes = std::mem::size_of::<Joined>()
        .saturating_add(count.saturating_mul(std::mem::size_of::<Option<Arc<DataFrameRow>>>()));
    let limit = max_rows.min(max_bytes / row_bytes.max(1));
    let mut joined = Vec::with_capacity(roots.len().min(limit));
    for root in roots {
        let mut start: Vec<Option<Arc<DataFrameRow>>> = vec![None; count];
        start[0] = Some(root);
        expand(0, start, select, &children, &by_value, &mut |row| {
            if joined.len() >= limit {
                return Err(Refusal::Unsupported(
                    "the joined result exceeds its row or byte budget".to_owned(),
                ));
            }
            joined.push(row.into_boxed_slice());
            Ok(())
        })?;
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
    emit: &mut dyn FnMut(Vec<Option<Arc<DataFrameRow>>>) -> Result<(), Refusal>,
) -> Result<(), Refusal> {
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
    emit: &mut dyn FnMut(Vec<Option<Arc<DataFrameRow>>>) -> Result<(), Refusal>,
) -> Result<(), Refusal> {
    let Some(&child) = children[node].get(at) else {
        return emit(row);
    };
    let Some(edge) = select.nodes[child].edge.as_ref() else {
        return Ok(());
    };
    let matches = row[node]
        .as_ref()
        .and_then(|parent| parent.data.get(&edge.parent_column))
        .and_then(Value::equality_key)
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
pub(super) fn ordered_bytes(rows: &[Joined]) -> usize {
    rows.iter().fold(std::mem::size_of_val(rows), |total, row| {
        total.saturating_add(std::mem::size_of_val(row.as_ref()))
    })
}

/// The window of `len` rows `select`'s offset and limit leave.
fn window(len: usize, select: &Select) -> Range<usize> {
    let start = usize::try_from(select.offset)
        .unwrap_or(usize::MAX)
        .min(len);
    let take = usize::try_from(select.limit).unwrap_or(usize::MAX);
    start..start.saturating_add(take).min(len)
}

/// The answer to a `COUNT(*)` that matched `count` rows.
pub(super) fn count(count: usize, select: &Select) -> Result<CachedRows, Refusal> {
    let count = i64::try_from(count)
        .map_err(|_| Refusal::Unsupported("the count is out of range".to_owned()))?;
    Ok(slice(&project_count(count), select))
}

pub(super) fn project_count(count: i64) -> Projected {
    let all: Arc<[Box<[Value]>]> = Arc::new([Box::new([Value::Int(count)]) as Box<[Value]>]);
    Projected {
        columns: Arc::new([CachedColumn {
            name: ColumnName::from("count"),
            type_oid: oid::INT8,
            element_oid: 0,
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
    let mut bytes = columns
        .iter()
        .map(|column| std::mem::size_of::<CachedColumn>() + column.name.as_str().len())
        .sum::<usize>();
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
                            .map_or(std::mem::size_of::<Value>(), super::state::value_bytes),
                    )
                },
            );
            bytes = bytes.saturating_add(weight);
            if bytes > max_bytes {
                return Err(Refusal::Unsupported(
                    "the projection exceeds its byte budget".to_owned(),
                ));
            }
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
        .collect::<Result<Arc<[Box<[Value]>]>, _>>()?;
    Ok(Projected {
        columns: columns.into(),
        rows,
        bytes,
    })
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
        assert!(matches!(answer, Err(Refusal::Unsupported(_))));
    }
    #[test]
    fn projection_budget_counts_expanded_json_as_well_as_wire_text() {
        let catalog = Catalog::new(vec![DbTable::new(
            "t",
            ["id"],
            vec![
                DbColumn::new("id", ValueType::Int).with_type_oid(20),
                DbColumn::new("j", ValueType::Json).with_type_oid(114),
            ],
        )]);
        let select = crate::parser::parse_select("SELECT j FROM t", &[], &catalog).unwrap();
        let json = crate::model::JsonText::new(format!("1{}", "0".repeat(9999)), "1e9999");
        let row = Arc::new(DataFrameRow::from(HashMap::from([
            (ColumnName::from("id"), Value::Int(1)),
            (ColumnName::from("j"), Value::Json(json)),
        ])));
        let joined = vec![vec![Some(row)].into_boxed_slice()];
        assert!(matches!(
            project(&joined, &select, &[catalog.table("t").unwrap()], 512),
            Err(Refusal::Unsupported(_))
        ));
    }
}
