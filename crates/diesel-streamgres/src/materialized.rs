//! A query's result kept current from the engine's rows, joins composed
//! here.
//!
//! The engine ships a subscription's rows per table: every column of
//! each row, and afterwards the rows that entered or changed and the keys
//! of those that left. [`Materialized`] keeps them by primary key per
//! table and, on demand, produces the result as Diesel rows: the join
//! composed from the root table down each edge (an inner edge drops a
//! parent row with no match, a left edge extends it with NULLs), ordered
//! as the query orders its root table, cut to its limit, projected to its
//! columns, each cell encoded for its Postgres type
//! ([`crate::encode`]).
//!
//! Ordering follows Postgres's defaults: `ASC` puts NULLs last, `DESC`
//! first. Strings compare bytewise, which is Postgres's `C` collation; a
//! column under a linguistic collation may order differently here, so a
//! query ordering by such a column should not be cached in rows mode.
//! Rows that tie on the ordering, and the child rows under one parent,
//! come out in primary-key order by value (9 before 10), which is what
//! `ORDER BY child.id` gives in Postgres; the engine accepts exactly
//! that ordering on a joined table, since the composition yields it.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value as Json;

use crate::encode::{Unsupported, encode, supports};
use crate::row::{Columns, OwnedRow, OwnedValue};
use crate::types::TableTypes;

/// One table of the query as the engine's `accepted` describes it.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeSnapshot {
    pub table: String,
    pub key: Vec<String>,
    /// The table this one joins to; `None` for the root.
    pub parent: Option<String>,
    pub parent_column: Option<String>,
    pub child_column: Option<String>,
    /// `false` for a LEFT join.
    pub inner: bool,
    pub rows: Vec<Json>,
}

/// What the engine's `accepted` carries in rows mode.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    /// Root first.
    pub nodes: Vec<NodeSnapshot>,
    /// The projected `(table, column)` pairs, `None` for `SELECT *`.
    pub columns: Option<Vec<(String, String)>>,
    /// What each projected column comes out as (its alias or its name);
    /// for a count, the one output name. `None` falls back to the
    /// columns' own names.
    pub names: Option<Vec<String>>,
    pub count: bool,
    /// Root-table column and whether ascending.
    pub order: Vec<(String, bool)>,
    pub limit: Option<usize>,
    /// Rows to skip before the limit; the engine hydrates `offset +
    /// limit` rows and the client cuts.
    pub offset: usize,
}

impl Snapshot {
    /// Reads an `accepted` body, its rows moved out of it; `None` when it
    /// carries no rows.
    pub fn from_json(body: &mut Json) -> Option<Snapshot> {
        let strings = |value: Option<&Json>| -> Option<Vec<String>> {
            value?
                .as_array()?
                .iter()
                .map(|item| item.as_str().map(str::to_owned))
                .collect()
        };
        let nodes = body
            .get_mut("nodes")?
            .as_array_mut()?
            .iter_mut()
            .map(|node| {
                let rows = match node.get_mut("rows").map(Json::take) {
                    Some(Json::Array(rows)) => rows,
                    _ => return None,
                };
                Some(NodeSnapshot {
                    table: node.get("table")?.as_str()?.to_owned(),
                    key: strings(node.get("key"))?,
                    parent: node.get("parent").and_then(Json::as_str).map(str::to_owned),
                    parent_column: node
                        .get("parent_column")
                        .and_then(Json::as_str)
                        .map(str::to_owned),
                    child_column: node
                        .get("child_column")
                        .and_then(Json::as_str)
                        .map(str::to_owned),
                    inner: node.get("inner").and_then(Json::as_bool).unwrap_or(true),
                    rows,
                })
            })
            .collect::<Option<Vec<_>>>()?;
        if nodes.is_empty() {
            return None;
        }
        let order = body
            .get("order")
            .and_then(Json::as_array)
            .map(|clauses| {
                clauses
                    .iter()
                    .filter_map(|clause| {
                        let pair = clause.as_array()?;
                        Some((
                            pair.first()?.as_str()?.to_owned(),
                            pair.get(1)?.as_str()? == "asc",
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let columns = match body.get("columns") {
            None | Some(Json::Null) => None,
            Some(columns) => Some(
                columns
                    .as_array()?
                    .iter()
                    .map(|pair| {
                        let pair = pair.as_array()?;
                        Some((
                            pair.first()?.as_str()?.to_owned(),
                            pair.get(1)?.as_str()?.to_owned(),
                        ))
                    })
                    .collect::<Option<Vec<_>>>()?,
            ),
        };
        Some(Snapshot {
            nodes,
            columns,
            names: match body.get("names") {
                None | Some(Json::Null) => None,
                other => Some(strings(other)?),
            },
            count: body.get("count").and_then(Json::as_bool).unwrap_or(false),
            order,
            offset: body
                .get("offset")
                .and_then(Json::as_u64)
                .map(|offset| offset as usize)
                .unwrap_or(0),
            limit: body
                .get("limit")
                .and_then(Json::as_u64)
                .map(|limit| limit as usize),
        })
    }

    /// The tables whose column types the store needs.
    pub fn tables(&self) -> impl Iterator<Item = &str> {
        self.nodes.iter().map(|node| node.table.as_str())
    }

    /// Every column of `table` the snapshot names: its key, the columns
    /// of its joins, the root's order, and the projection. The column
    /// types known for the table must cover them, or they are stale.
    pub fn columns_of<'a>(&'a self, table: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        let keys = self
            .nodes
            .iter()
            .filter(move |node| node.table == table)
            .flat_map(|node| {
                node.key
                    .iter()
                    .map(String::as_str)
                    .chain(node.child_column.as_deref())
            });
        let joins = self
            .nodes
            .iter()
            .filter(move |node| node.parent.as_deref() == Some(table))
            .filter_map(|node| node.parent_column.as_deref());
        let order = self
            .order
            .iter()
            .filter(move |_| self.nodes.first().is_some_and(|root| root.table == table))
            .map(|(name, _)| name.as_str());
        let projected = self
            .columns
            .iter()
            .flatten()
            .filter(move |(of, _)| of == table)
            .map(|(_, name)| name.as_str());
        keys.chain(joins).chain(order).chain(projected)
    }
}

/// What the result is made of.
enum Projection {
    /// One row, the count of the joined rows.
    Count,
    /// `(node, column index in the node's types)` per output column.
    Columns(Vec<(usize, usize)>),
}

/// One table of the query, with the rows the engine shipped for it.
struct Node {
    table: String,
    types: Arc<TableTypes>,
    key: Vec<usize>,
    /// Column index in the parent's types.
    parent_column: Option<usize>,
    /// Column index in this node's types.
    child_column: Option<usize>,
    inner: bool,
    children: Vec<usize>,
    rows: HashMap<String, Json>,
}

/// One query's rows and how to present them. The result is built from
/// the rows when it is asked for and kept until the next change, so a
/// burst of deltas costs one build, by whichever read comes next.
pub struct Materialized {
    nodes: Vec<Node>,
    projection: Projection,
    order: Vec<(usize, bool)>,
    limit: Option<usize>,
    offset: usize,
    columns: Columns,
    /// What the held rows weigh ([`Materialized::byte_len`]), kept as
    /// they come and go.
    bytes: usize,
    /// The result as last built, until a change.
    built: Option<Arc<[OwnedRow]>>,
}

impl Materialized {
    /// Checks the query can be served from engine rows (every projected
    /// column encodable, every named column and table known) and loads
    /// the snapshot. `types` must cover every table of the snapshot.
    /// Without a projection (`SELECT *`) the result is every column of
    /// every table, in `FROM` order.
    pub fn new(
        snapshot: Snapshot,
        types: &HashMap<String, Arc<TableTypes>>,
    ) -> Result<Materialized, Unsupported> {
        let mut nodes: Vec<Node> = Vec::with_capacity(snapshot.nodes.len());
        let node_index = |nodes: &[Node], table: &str| -> Result<usize, Unsupported> {
            nodes
                .iter()
                .position(|node| node.table == table)
                .ok_or_else(|| Unsupported(format!("`{table}` is not a table of the query")))
        };
        for node in &snapshot.nodes {
            let node_types = types.get(&node.table).cloned().ok_or_else(|| {
                Unsupported(format!("the types of `{}` are not known", node.table))
            })?;
            let column = |name: &str| -> Result<usize, Unsupported> {
                node_types
                    .column(name)
                    .map(|(index, _)| index)
                    .ok_or_else(|| {
                        Unsupported(format!(
                            "column `{name}` is not in the types of `{}`",
                            node.table
                        ))
                    })
            };
            let key = node
                .key
                .iter()
                .map(|name| column(name))
                .collect::<Result<Vec<_>, _>>()?;
            if key.is_empty() {
                return Err(Unsupported(format!("`{}` has no primary key", node.table)));
            }
            let (parent, parent_column, child_column) =
                match (&node.parent, &node.parent_column, &node.child_column) {
                    (Some(parent), Some(parent_column), Some(child_column)) => {
                        let parent = node_index(&nodes, parent)?;
                        let parent_column = nodes[parent]
                            .types
                            .column(parent_column)
                            .map(|(index, _)| index)
                            .ok_or_else(|| {
                                Unsupported(format!(
                                    "join column `{parent_column}` is not in the types of `{}`",
                                    nodes[parent].table
                                ))
                            })?;
                        (
                            Some(parent),
                            Some(parent_column),
                            Some(column(child_column)?),
                        )
                    }
                    (None, _, _) if nodes.is_empty() => (None, None, None),
                    _ => {
                        return Err(Unsupported(format!(
                            "`{}` has no join to a parent table",
                            node.table
                        )));
                    }
                };
            let index = nodes.len();
            if let Some(parent) = parent {
                nodes[parent].children.push(index);
            }
            nodes.push(Node {
                table: node.table.clone(),
                types: node_types,
                key,
                parent_column,
                child_column,
                inner: node.inner,
                children: Vec::new(),
                rows: HashMap::new(),
            });
        }

        let root = &nodes[0];
        let order = snapshot
            .order
            .iter()
            .map(|(name, ascending)| {
                root.types
                    .column(name)
                    .map(|(index, _)| (index, *ascending))
                    .ok_or_else(|| {
                        Unsupported(format!("order column `{name}` is not in the root's types"))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let (projection, columns): (Projection, Columns) = if snapshot.count {
            let name = snapshot
                .names
                .as_ref()
                .and_then(|names| names.first().cloned())
                .unwrap_or_else(|| "count".to_string());
            (Projection::Count, Arc::from(vec![name]))
        } else {
            let pairs: Vec<(usize, usize)> = match &snapshot.columns {
                Some(columns) => columns
                    .iter()
                    .map(|(table, name)| {
                        let node = node_index(&nodes, table)?;
                        let column = nodes[node]
                            .types
                            .column(name)
                            .map(|(index, _)| index)
                            .ok_or_else(|| {
                                Unsupported(format!(
                                    "column `{name}` is not in the types of `{table}`"
                                ))
                            })?;
                        Ok((node, column))
                    })
                    .collect::<Result<_, Unsupported>>()?,
                None => nodes
                    .iter()
                    .enumerate()
                    .flat_map(|(n, node)| (0..node.types.columns.len()).map(move |c| (n, c)))
                    .collect(),
            };
            for &(node, column) in &pairs {
                supports(&nodes[node].types.columns[column])?;
            }
            let names = match &snapshot.names {
                Some(names) if names.len() == pairs.len() => names.clone(),
                _ => pairs
                    .iter()
                    .map(|&(node, column)| nodes[node].types.columns[column].name.clone())
                    .collect::<Vec<_>>(),
            };
            (Projection::Columns(pairs), Arc::from(names))
        };

        let mut materialized = Materialized {
            nodes,
            projection,
            order,
            limit: snapshot.limit,
            offset: snapshot.offset,
            columns,
            bytes: 0,
            built: None,
        };
        for node in snapshot.nodes {
            for row in node.rows {
                materialized.put(&node.table, row);
            }
        }
        Ok(materialized)
    }

    /// A row of `table` entered or changed.
    pub fn put(&mut self, table: &str, row: Json) {
        if let Some(index) = self.nodes.iter().position(|node| node.table == table) {
            let key = self.key_of(index, &row);
            let (key_bytes, row_bytes) = (key.len(), json_bytes(&row));
            match self.nodes[index].rows.insert(key, row) {
                Some(old) => {
                    self.bytes = (self.bytes + row_bytes).saturating_sub(json_bytes(&old));
                }
                None => self.bytes += key_bytes + row_bytes,
            }
            self.built = None;
        }
    }

    /// A row of `table` left; `key` is the JSON object of its key columns.
    pub fn del(&mut self, table: &str, key: &Json) {
        if let Some(index) = self.nodes.iter().position(|node| node.table == table) {
            let key = self.key_of(index, key);
            if let Some(old) = self.nodes[index].rows.remove(&key) {
                self.bytes = self.bytes.saturating_sub(key.len() + json_bytes(&old));
            }
            self.built = None;
        }
    }

    /// What the store weighs, roughly: every row's JSON as it is held.
    pub fn byte_len(&self) -> usize {
        self.bytes
    }


    /// How many rows are held, over every table.
    pub fn len(&self) -> usize {
        self.nodes.iter().map(|node| node.rows.len()).sum()
    }

    /// Whether no row is held.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The row's primary key as one string: each key column's JSON text,
    /// each followed by U+001F. Two keys cannot run together into one,
    /// because JSON text never holds a raw U+001F: inside a string
    /// `serde_json` writes it as `\u001f`, and outside strings it is not
    /// a JSON character at all.
    fn key_of(&self, node: usize, object: &Json) -> String {
        let node = &self.nodes[node];
        let mut key = String::new();
        for &index in &node.key {
            let name = &node.types.columns[index].name;
            key.push_str(&object.get(name).cloned().unwrap_or(Json::Null).to_string());
            key.push('\u{1f}');
        }
        key
    }

    /// The result as Diesel rows, built if a change came since it last
    /// was. A count's `LIMIT` and `OFFSET` cut the one row it is, as
    /// Postgres applies them after counting.
    pub fn rows(&mut self) -> Result<Arc<[OwnedRow]>, Unsupported> {
        if let Some(built) = &self.built {
            return Ok(Arc::clone(built));
        }
        let built = self.build()?;
        self.built = Some(Arc::clone(&built));
        Ok(built)
    }

    /// The result built from the rows held now.
    fn build(&self) -> Result<Arc<[OwnedRow]>, Unsupported> {
        Ok(self
            .keyed()?
            .into_iter()
            .map(|(_, row)| row)
            .collect::<Vec<_>>()
            .into())
    }

    /// The result, each row with its identity: the primary keys of the
    /// table rows it was composed from (a NULL-extended table marked as
    /// such), so a row keeps its identity while its values change and no
    /// two rows of one result share one. A count is one row, of one
    /// identity.
    pub fn keyed(&self) -> Result<Vec<(String, OwnedRow)>, Unsupported> {
        let combos = self.assemble();
        let limit = self.limit.unwrap_or(usize::MAX);
        if let Projection::Count = &self.projection {
            let count = OwnedValue::new(
                std::num::NonZeroU32::new(20).expect("int8"),
                (combos.len() as i64).to_be_bytes().to_vec(),
            );
            let row = OwnedRow::new(Arc::clone(&self.columns), vec![Some(count)]);
            return Ok(std::iter::once((String::new(), row))
                .skip(self.offset)
                .take(limit)
                .collect());
        }
        combos
            .into_iter()
            .skip(self.offset)
            .take(limit)
            .map(|combo| Ok((self.identity(&combo), self.project(&combo)?)))
            .collect()
    }

    /// Whether the result is the root table's rows alone, uncut: one
    /// table, no `LIMIT` or `OFFSET`, not a count. A change of such a
    /// result is the change of the rows the engine ships, which
    /// [`Materialized::root_row`] and [`Materialized::project_root`] read
    /// without building it.
    pub fn is_plain(&self) -> bool {
        self.nodes.len() == 1
            && self.limit.is_none()
            && self.offset == 0
            && matches!(self.projection, Projection::Columns(_))
    }

    /// The identity, in [`Materialized::keyed`]'s terms, of the root
    /// table's row with these key columns (a row, or the key object a
    /// delete names); `None` for another table's.
    pub fn root_identity(&self, table: &str, object: &Json) -> Option<String> {
        (self.nodes[0].table == table).then(|| {
            let mut identity = self.key_of(0, object);
            identity.push('\u{1e}');
            identity
        })
    }

    /// The root table's row held under `identity`.
    pub fn root_row(&self, identity: &str) -> Option<&Json> {
        let key = identity.strip_suffix('\u{1e}')?;
        self.nodes[0].rows.get(key)
    }

    /// A root table's row as the result's row, for a plain result
    /// ([`Materialized::is_plain`]).
    pub fn project_root(&self, row: &Json) -> Result<OwnedRow, Unsupported> {
        self.project(&[Some(row)])
    }

    /// One result row's identity: each table's key, or a mark for a
    /// table the row has no row of, each closed by U+001E.
    fn identity(&self, combo: &[Option<&Json>]) -> String {
        let mut identity = String::new();
        for (node, row) in combo.iter().enumerate() {
            if let Some(row) = row {
                identity.push_str(&self.key_of(node, row));
            } else {
                identity.push('\u{1f}');
            }
            identity.push('\u{1e}');
        }
        identity
    }

    /// One result row: the projected columns of `combo`'s table rows,
    /// each encoded for its type.
    fn project(&self, combo: &[Option<&Json>]) -> Result<OwnedRow, Unsupported> {
        let Projection::Columns(pairs) = &self.projection else {
            return Err(Unsupported("a count has no rows to project".to_string()));
        };
        let values = pairs
            .iter()
            .map(|&(node, column)| {
                let column = &self.nodes[node].types.columns[column];
                match combo.get(node).copied().flatten() {
                    Some(row) => encode(row.get(&column.name).unwrap_or(&Json::Null), column),
                    None => Ok(None),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(OwnedRow::new(Arc::clone(&self.columns), values))
    }

    /// The joined rows: one entry per result row, holding each table's
    /// row or `None` where a left edge found no match. Root rows in the
    /// query's order; everything else in key order.
    ///
    /// Each child node's rows are indexed by the value of its join
    /// column, in primary-key order by value: `ORDER BY child.id` in
    /// Postgres puts 9 before 10, and so must this.
    fn assemble(&self) -> Vec<Vec<Option<&Json>>> {
        let mut by_value: Vec<HashMap<String, Vec<&Json>>> = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let mut index: HashMap<String, Vec<&Json>> = HashMap::new();
            if let Some(child_column) = node.child_column {
                let name = &node.types.columns[child_column].name;
                let mut keyed: Vec<(&String, &Json)> = node.rows.iter().collect();
                keyed.sort_by(|(_, a), (_, b)| self.key_order(node, a, b));
                for (_, row) in keyed {
                    match row.get(name) {
                        Some(value) if !value.is_null() => {
                            index.entry(value.to_string()).or_default().push(row);
                        }
                        _ => {}
                    }
                }
            }
            by_value.push(index);
        }

        let root = &self.nodes[0];
        let mut roots: Vec<(&String, &Json)> = root.rows.iter().collect();
        roots.sort_by(|(_, a), (_, b)| {
            for &(index, ascending) in &self.order {
                let name = &root.types.columns[index].name;
                let ordering = compare(a.get(name), b.get(name), ascending);
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            self.key_order(root, a, b)
        });

        let mut out = Vec::new();
        for (_, row) in roots {
            self.combos(0, row, &by_value, &mut out);
        }
        out
    }

    /// Two rows of `node` by their primary key, column by column, as
    /// Postgres orders them: numbers numerically, strings bytewise.
    fn key_order(&self, node: &Node, a: &Json, b: &Json) -> Ordering {
        for &index in &node.key {
            let name = &node.types.columns[index].name;
            let ordering = compare(a.get(name), b.get(name), true);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    }

    /// Every result row the subtree under `node` makes with `row` as that
    /// node's row, appended to `out`. An inner edge with no match makes
    /// none; a left edge with no match leaves its subtree NULL.
    fn combos<'a>(
        &'a self,
        node: usize,
        row: &'a Json,
        by_value: &[HashMap<String, Vec<&'a Json>>],
        out: &mut Vec<Vec<Option<&'a Json>>>,
    ) {
        let mut partial: Vec<Vec<Option<&'a Json>>> = vec![vec![None; self.nodes.len()]];
        partial[0][node] = Some(row);
        for &child in &self.nodes[node].children {
            let child_node = &self.nodes[child];
            let parent_column = child_node
                .parent_column
                .expect("a child has a parent column");
            let value = row.get(&self.nodes[node].types.columns[parent_column].name);
            let matches: &[&Json] = match value {
                Some(value) if !value.is_null() => by_value[child]
                    .get(&value.to_string())
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
                _ => &[],
            };
            let mut child_combos: Vec<Vec<Option<&'a Json>>> = Vec::new();
            for &child_row in matches {
                self.combos(child, child_row, by_value, &mut child_combos);
            }
            if child_combos.is_empty() {
                if child_node.inner {
                    return;
                }
                continue;
            }
            let mut next = Vec::with_capacity(partial.len() * child_combos.len());
            for left in &partial {
                for right in &child_combos {
                    let mut merged = left.clone();
                    for (slot, value) in merged.iter_mut().zip(right) {
                        if value.is_some() {
                            *slot = *value;
                        }
                    }
                    next.push(merged);
                }
            }
            partial = next;
        }
        out.extend(partial);
    }
}

/// A rough size of a JSON value in memory: what it holds plus what each
/// node costs.
fn json_bytes(value: &Json) -> usize {
    const NODE: usize = 32;
    match value {
        Json::Null | Json::Bool(_) | Json::Number(_) => NODE,
        Json::String(s) => NODE + s.len(),
        Json::Array(items) => NODE + items.iter().map(json_bytes).sum::<usize>(),
        Json::Object(fields) => {
            NODE + fields
                .iter()
                .map(|(k, v)| k.len() + NODE + json_bytes(v))
                .sum::<usize>()
        }
    }
}

/// Postgres's order for one clause: NULLs last ascending, first
/// descending (the reversal for descending applies to them as it does
/// to values); numbers numerically, strings bytewise, booleans false
/// first.
fn compare(a: Option<&Json>, b: Option<&Json>, ascending: bool) -> Ordering {
    let a = a.filter(|v| !v.is_null());
    let b = b.filter(|v| !v.is_null());
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if ascending {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(_), None) => {
            if ascending {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(a), Some(b)) => {
            let natural = match (a, b) {
                (Json::Number(x), Json::Number(y)) => match (x.as_i64(), y.as_i64()) {
                    (Some(x), Some(y)) => x.cmp(&y),
                    _ => x
                        .as_f64()
                        .partial_cmp(&y.as_f64())
                        .unwrap_or(Ordering::Equal),
                },
                (Json::String(x), Json::String(y)) => x.as_bytes().cmp(y.as_bytes()),
                (Json::Bool(x), Json::Bool(y)) => x.cmp(y),
                (x, y) => x.to_string().cmp(&y.to_string()),
            };
            if ascending {
                natural
            } else {
                natural.reverse()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnType, TypeKind};
    use diesel::deserialize::FromSqlRow;
    use diesel::pg::Pg;
    use diesel::sql_types::{BigInt, Integer, Nullable, Text};
    use serde_json::json;

    /// A base-type column.
    fn base(name: &str, oid: u32) -> ColumnType {
        ColumnType {
            name: name.into(),
            oid,
            typmod: -1,
            kind: TypeKind::Base,
        }
    }

    /// The types of `t`, `books` and `reviews`.
    fn types() -> HashMap<String, Arc<TableTypes>> {
        HashMap::from([
            (
                "t".to_string(),
                Arc::new(TableTypes {
                    columns: vec![base("id", 23), base("name", 25), base("rank", 23)],
                }),
            ),
            (
                "books".to_string(),
                Arc::new(TableTypes {
                    columns: vec![base("id", 23), base("author_id", 23), base("title", 25)],
                }),
            ),
            (
                "reviews".to_string(),
                Arc::new(TableTypes {
                    columns: vec![base("id", 23), base("book_id", 23), base("stars", 23)],
                }),
            ),
        ])
    }

    /// The root node, table `t`, holding `rows`.
    fn root(rows: Vec<Json>) -> NodeSnapshot {
        NodeSnapshot {
            table: "t".into(),
            key: vec!["id".into()],
            parent: None,
            parent_column: None,
            child_column: None,
            inner: true,
            rows,
        }
    }

    /// A node joined under `parent` on `parent_column = child_column`.
    fn child(
        table: &str,
        parent: &str,
        parent_column: &str,
        child_column: &str,
        inner: bool,
        rows: Vec<Json>,
    ) -> NodeSnapshot {
        NodeSnapshot {
            table: table.into(),
            key: vec!["id".into()],
            parent: Some(parent.into()),
            parent_column: Some(parent_column.into()),
            child_column: Some(child_column.into()),
            inner,
            rows,
        }
    }

    /// A snapshot of `t` alone, projecting `id` and `name`.
    fn snapshot(rows: Vec<Json>, order: Vec<(&str, bool)>, limit: Option<usize>) -> Snapshot {
        Snapshot {
            nodes: vec![root(rows)],
            columns: Some(vec![("t".into(), "id".into()), ("t".into(), "name".into())]),
            names: None,
            count: false,
            order: order.into_iter().map(|(c, a)| (c.to_string(), a)).collect(),
            limit,
            offset: 0,
        }
    }

    /// The rows as `(id, name)` pairs.
    fn decoded(rows: &Arc<[OwnedRow]>) -> Vec<(i32, String)> {
        rows.iter()
            .map(|row| {
                <(i32, String) as FromSqlRow<(Integer, Text), Pg>>::build_from_row(row).unwrap()
            })
            .collect()
    }

    /// `key_of` relies on this: a raw U+001F never appears in JSON
    /// text, so it can end each key column without a collision.
    #[test]
    fn json_text_never_holds_the_key_separator() {
        assert_eq!(Json::from("a\u{1f}b").to_string(), r#""a\u001fb""#);
    }

    /// An offset skips rows before the limit takes its rows.
    #[test]
    fn an_offset_skips_rows_before_the_limit() {
        let mut snap = snapshot(
            vec![
                json!({"id": 1, "key": "a", "value": "1"}),
                json!({"id": 2, "key": "b", "value": "2"}),
                json!({"id": 3, "key": "c", "value": "3"}),
                json!({"id": 4, "key": "d", "value": "4"}),
            ],
            vec![("id", true)],
            Some(2),
        );
        snap.offset = 1;
        let mut store = Materialized::new(snap, &types()).unwrap();
        let rows = store.rows().unwrap();
        let ids: Vec<i32> = rows
            .iter()
            .map(|row| i32::from_be_bytes(row.value(0).unwrap().bytes().try_into().unwrap()))
            .collect();
        assert_eq!(ids, vec![2, 3], "skipped one, took two");
    }

    /// Rows come out in the query's order, NULLs last ascending, cut to
    /// the limit and projected.
    #[test]
    fn rows_come_out_ordered_cut_and_projected() {
        let snap = snapshot(
            vec![
                json!({"id": 1, "name": "b", "rank": 2}),
                json!({"id": 2, "name": "a", "rank": null}),
                json!({"id": 3, "name": "c", "rank": 1}),
            ],
            vec![("rank", true), ("id", true)],
            Some(2),
        );
        let mut m = Materialized::new(snap, &types()).unwrap();
        assert_eq!(
            decoded(&m.rows().unwrap()),
            vec![(3, "c".into()), (1, "b".into())],
            "NULL rank last, cut to 2"
        );
    }

    /// Under `DESC` a NULL comes first, as Postgres puts it.
    #[test]
    fn descending_puts_nulls_first_as_postgres_does() {
        let snap = snapshot(
            vec![
                json!({"id": 1, "name": "b", "rank": 2}),
                json!({"id": 2, "name": "a", "rank": null}),
                json!({"id": 3, "name": "c", "rank": 1}),
            ],
            vec![("rank", false), ("id", true)],
            None,
        );
        let mut m = Materialized::new(snap, &types()).unwrap();
        assert_eq!(
            decoded(&m.rows().unwrap()),
            vec![(2, "a".into()), (1, "b".into()), (3, "c".into())],
            "NULL first under DESC, then 2, 1"
        );
    }

    /// A second, longer row weighs more.
    #[test]
    fn the_store_weighs_its_rows() {
        let snap = snapshot(vec![json!({"id": 1, "name": "b", "rank": 2})], vec![], None);
        let mut m = Materialized::new(snap, &types()).unwrap();
        let one = m.byte_len();
        assert!(one > 0);
        m.put(
            "t",
            json!({"id": 2, "name": "a much longer name than before", "rank": 1}),
        );
        assert!(
            m.byte_len() > one * 2 - 16,
            "a second, longer row weighs more"
        );
    }

    /// A joined table's rows come out in key order by value (9, 10, 100),
    /// not by text.
    #[test]
    fn child_rows_come_out_in_numeric_key_order() {
        let mut snap = snapshot(
            vec![json!({"id": 1, "name": "a", "rank": 1})],
            vec![("id", true)],
            None,
        );
        snap.nodes.push(NodeSnapshot {
            table: "books".to_string(),
            key: vec!["id".to_string()],
            parent: Some("t".to_string()),
            parent_column: Some("id".to_string()),
            child_column: Some("author_id".to_string()),
            inner: true,
            rows: vec![
                json!({"id": 10, "author_id": 1, "title": "ten"}),
                json!({"id": 9, "author_id": 1, "title": "nine"}),
                json!({"id": 100, "author_id": 1, "title": "hundred"}),
            ],
        });
        snap.columns = Some(vec![
            ("t".to_string(), "id".to_string()),
            ("books".to_string(), "title".to_string()),
        ]);
        snap.names = None;
        let mut m = Materialized::new(snap, &types()).unwrap();
        assert_eq!(
            decoded(&m.rows().unwrap()),
            vec![(1, "nine".into()), (1, "ten".into()), (1, "hundred".into())],
            "9, 10, 100: by value, not by text"
        );
    }

    /// Puts and deletes change the result.
    #[test]
    fn deltas_change_the_result() {
        let snap = snapshot(
            vec![
                json!({"id": 1, "name": "b", "rank": 2}),
                json!({"id": 2, "name": "a", "rank": 1}),
            ],
            vec![("name", false)],
            None,
        );
        let mut m = Materialized::new(snap, &types()).unwrap();
        assert_eq!(
            decoded(&m.rows().unwrap()),
            vec![(1, "b".into()), (2, "a".into())]
        );
        m.put("t", json!({"id": 1, "name": "z", "rank": 2}));
        m.put("t", json!({"id": 3, "name": "m", "rank": 0}));
        m.del("t", &json!({"id": 2}));
        assert_eq!(
            decoded(&m.rows().unwrap()),
            vec![(1, "z".into()), (3, "m".into())]
        );
        assert_eq!(m.len(), 2);
    }

    /// A count is one `int8` row, which counts every row whatever the
    /// limit.
    #[test]
    fn a_count_is_one_int8() {
        let mut snap = snapshot(vec![json!({"id": 1}), json!({"id": 2})], vec![], Some(1));
        snap.count = true;
        snap.columns = None;
        let mut m = Materialized::new(snap, &types()).unwrap();
        let rows = m.rows().unwrap();
        let count = <i64 as FromSqlRow<BigInt, Pg>>::build_from_row(&rows[0]).unwrap();
        assert_eq!(count, 2, "a count ignores the limit");
    }

    /// A count's LIMIT and OFFSET cut the one row it is, as Postgres
    /// applies them after counting: an OFFSET past it, or a LIMIT of
    /// zero, leaves no row.
    #[test]
    fn a_count_is_cut_by_its_limit_and_offset() {
        let counted = |limit: Option<usize>, offset: usize| {
            let mut snap = snapshot(vec![json!({"id": 1}), json!({"id": 2})], vec![], limit);
            snap.count = true;
            snap.columns = None;
            snap.offset = offset;
            Materialized::new(snap, &types()).unwrap().rows().unwrap().len()
        };
        assert_eq!(counted(Some(1), 0), 1);
        assert_eq!(counted(None, 1), 0);
        assert_eq!(counted(Some(0), 0), 0);
    }

    /// The result is built once per change: read again unchanged it is
    /// the same rows; a put or a delete builds it afresh; the weight
    /// follows the rows held.
    #[test]
    fn the_result_is_built_once_per_change() {
        let mut m = Materialized::new(
            snapshot(vec![json!({"id": 1, "name": "a"})], vec![("id", true)], None),
            &types(),
        )
        .unwrap();
        let first = m.rows().unwrap();
        assert!(Arc::ptr_eq(&first, &m.rows().unwrap()), "kept while unchanged");
        let one_row = m.byte_len();
        m.put("t", json!({"id": 2, "name": "b"}));
        let second = m.rows().unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(decoded(&second), vec![(1, "a".into()), (2, "b".into())]);
        assert!(m.byte_len() > one_row);
        m.put("t", json!({"id": 2, "name": "bee"}));
        m.del("t", &json!({"id": 2}));
        assert_eq!(decoded(&m.rows().unwrap()), vec![(1, "a".into())]);
        assert_eq!(m.byte_len(), one_row, "a row that came and went weighs nothing");
    }

    /// A projected column the client cannot encode refuses rows mode; an
    /// unprojected one does not matter.
    #[test]
    fn an_unencodable_projected_column_refuses_rows_mode() {
        let mut t = types();
        let mut with_created = (*t["t"]).clone();
        with_created.columns.push(base("created", 1186));
        t.insert("t".into(), Arc::new(with_created));
        let mut snap = snapshot(vec![], vec![], None);
        snap.columns = Some(vec![
            ("t".into(), "id".into()),
            ("t".into(), "created".into()),
        ]);
        assert!(Materialized::new(snap.clone(), &t).is_err());
        snap.columns = Some(vec![("t".into(), "id".into())]);
        assert!(
            Materialized::new(snap, &t).is_ok(),
            "unprojected columns do not matter"
        );
    }

    /// Output columns are named by the statement's aliases.
    #[test]
    fn output_names_follow_the_aliases() {
        use diesel::deserialize::QueryableByName;
        /// A row read by its output names.
        #[derive(QueryableByName, Debug, PartialEq)]
        struct Named {
            #[diesel(sql_type = Text)]
            n: String,
            #[diesel(sql_type = Integer)]
            i: i32,
        }
        let snap = Snapshot {
            columns: Some(vec![("t".into(), "name".into()), ("t".into(), "id".into())]),
            names: Some(vec!["n".into(), "i".into()]),
            ..snapshot(vec![json!({"id": 4, "name": "x", "rank": 1})], vec![], None)
        };
        let mut m = Materialized::new(snap, &types()).unwrap();
        let rows = m.rows().unwrap();
        assert_eq!(
            Named::build(&rows[0]).unwrap(),
            Named {
                n: "x".into(),
                i: 4
            }
        );
        assert_eq!(
            rows[0]
                .columns()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["n", "i"]
        );
    }

    /// A NULL decodes into a nullable column.
    #[test]
    fn a_nullable_projection_decodes() {
        let snap = Snapshot {
            columns: Some(vec![("t".into(), "rank".into())]),
            ..snapshot(
                vec![json!({"id": 1, "name": "x", "rank": null})],
                vec![],
                None,
            )
        };
        let mut m = Materialized::new(snap, &types()).unwrap();
        let rows = m.rows().unwrap();
        let rank =
            <Option<i32> as FromSqlRow<Nullable<Integer>, Pg>>::build_from_row(&rows[0]).unwrap();
        assert_eq!(rank, None);
    }

    type Joined = (String, Option<String>);
    type JoinedSql = (Text, Nullable<Text>);

    /// The rows as `(name, title)` pairs.
    fn joined(rows: &Arc<[OwnedRow]>) -> Vec<Joined> {
        rows.iter()
            .map(|row| <Joined as FromSqlRow<JoinedSql, Pg>>::build_from_row(row).unwrap())
            .collect()
    }

    /// Authors `t` joined to their `books`, inner or left, in id order.
    fn authors_books(inner: bool, limit: Option<usize>) -> Snapshot {
        Snapshot {
            nodes: vec![
                root(vec![
                    json!({"id": 1, "name": "ann", "rank": 1}),
                    json!({"id": 2, "name": "bob", "rank": 2}),
                    json!({"id": 3, "name": "cy", "rank": 3}),
                ]),
                child(
                    "books",
                    "t",
                    "id",
                    "author_id",
                    inner,
                    vec![
                        json!({"id": 10, "author_id": 1, "title": "a1"}),
                        json!({"id": 11, "author_id": 1, "title": "a2"}),
                        json!({"id": 12, "author_id": 3, "title": "c1"}),
                    ],
                ),
            ],
            columns: Some(vec![
                ("t".into(), "name".into()),
                ("books".into(), "title".into()),
            ]),
            names: None,
            count: false,
            order: vec![("id".into(), true)],
            limit,
            offset: 0,
        }
    }

    /// An inner join drops a parent with no match.
    #[test]
    fn an_inner_join_drops_parents_without_a_match() {
        let mut m = Materialized::new(authors_books(true, None), &types()).unwrap();
        assert_eq!(
            joined(&m.rows().unwrap()),
            vec![
                ("ann".into(), Some("a1".into())),
                ("ann".into(), Some("a2".into())),
                ("cy".into(), Some("c1".into())),
            ]
        );
    }

    /// A left join keeps a parent with no match, NULL-extended, and the
    /// limit counts joined rows.
    #[test]
    fn a_left_join_null_extends_and_the_limit_cuts_joined_rows() {
        let mut m = Materialized::new(authors_books(false, Some(3)), &types()).unwrap();
        assert_eq!(
            joined(&m.rows().unwrap()),
            vec![
                ("ann".into(), Some("a1".into())),
                ("ann".into(), Some("a2".into())),
                ("bob".into(), None),
            ],
            "bob has no books and the limit counts joined rows"
        );
    }

    /// Changes on either side of a join recompose it; a parent that
    /// leaves takes its rows with it.
    #[test]
    fn deltas_on_either_table_recompose_the_join() {
        let mut m = Materialized::new(authors_books(true, None), &types()).unwrap();
        m.put("books", json!({"id": 13, "author_id": 2, "title": "b1"}));
        m.del("books", &json!({"id": 11}));
        m.put("t", json!({"id": 3, "name": "cyrus", "rank": 3}));
        assert_eq!(
            joined(&m.rows().unwrap()),
            vec![
                ("ann".into(), Some("a1".into())),
                ("bob".into(), Some("b1".into())),
                ("cyrus".into(), Some("c1".into())),
            ]
        );
        m.del("t", &json!({"id": 1}));
        assert_eq!(
            joined(&m.rows().unwrap()).len(),
            2,
            "the parent left, its books are orphans"
        );
    }

    /// A join under a join, and a count of the rows it makes.
    #[test]
    fn a_nested_join_and_a_count_of_joined_rows() {
        let mut snap = authors_books(true, None);
        snap.nodes.push(child(
            "reviews",
            "books",
            "id",
            "book_id",
            true,
            vec![
                json!({"id": 100, "book_id": 10, "stars": 5}),
                json!({"id": 101, "book_id": 10, "stars": 3}),
                json!({"id": 102, "book_id": 12, "stars": 4}),
            ],
        ));
        snap.columns = Some(vec![
            ("t".into(), "name".into()),
            ("reviews".into(), "stars".into()),
        ]);
        let mut m = Materialized::new(snap.clone(), &types()).unwrap();
        let rows: Vec<(String, i32)> = m
            .rows()
            .unwrap()
            .iter()
            .map(|row| {
                <(String, i32) as FromSqlRow<(Text, Integer), Pg>>::build_from_row(row).unwrap()
            })
            .collect();
        assert_eq!(
            rows,
            vec![("ann".into(), 5), ("ann".into(), 3), ("cy".into(), 4)],
            "a2 has no reviews, so it is gone"
        );

        snap.count = true;
        snap.columns = None;
        let mut m = Materialized::new(snap, &types()).unwrap();
        let rows = m.rows().unwrap();
        assert_eq!(
            <i64 as FromSqlRow<BigInt, Pg>>::build_from_row(&rows[0]).unwrap(),
            3
        );
    }
}
