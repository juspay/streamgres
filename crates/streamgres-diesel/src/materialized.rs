//! A query's result kept current from the engine's rows.
//!
//! The engine ships a subscription's rows as JSON objects, every column
//! of each row, and afterwards the rows that entered or changed and the
//! keys of those that left. [`Materialized`] keeps them by primary key,
//! and on demand produces the result as Diesel rows: ordered as the
//! query orders, cut to its limit, projected to its columns, each cell
//! encoded for its Postgres type ([`crate::encode`]).
//!
//! Ordering follows Postgres's defaults: `ASC` puts NULLs last, `DESC`
//! first. Strings compare bytewise, which is Postgres's `C` collation; a
//! column under a linguistic collation may order differently here, so a
//! query ordering by such a column should not be cached in rows mode.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value as Json;

use crate::encode::{Unsupported, encode, supports};
use crate::row::{Columns, OwnedRow, OwnedValue};
use crate::types::TableTypes;

/// What the engine's `accepted` carries in rows mode.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub table: String,
    pub key: Vec<String>,
    /// The projected columns, `None` for `SELECT *`.
    pub columns: Option<Vec<String>>,
    /// What each projected column comes out as (its alias or its name);
    /// for a count, the one output name. `None` falls back to the
    /// columns' own names.
    pub names: Option<Vec<String>>,
    pub count: bool,
    /// Column and whether ascending.
    pub order: Vec<(String, bool)>,
    pub limit: Option<usize>,
    pub rows: Vec<Json>,
}

impl Snapshot {
    /// Reads an `accepted` body; `None` when it carries no rows.
    pub fn from_json(body: &Json) -> Option<Snapshot> {
        let rows = body.get("rows")?.as_array()?.clone();
        let strings = |value: Option<&Json>| -> Option<Vec<String>> {
            value?
                .as_array()?
                .iter()
                .map(|item| item.as_str().map(str::to_owned))
                .collect()
        };
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
        Some(Snapshot {
            table: body.get("table")?.as_str()?.to_owned(),
            key: strings(body.get("key"))?,
            columns: match body.get("columns") {
                None | Some(Json::Null) => None,
                other => Some(strings(other)?),
            },
            names: match body.get("names") {
                None | Some(Json::Null) => None,
                other => Some(strings(other)?),
            },
            count: body.get("count").and_then(Json::as_bool).unwrap_or(false),
            order,
            limit: body
                .get("limit")
                .and_then(Json::as_u64)
                .map(|limit| limit as usize),
            rows,
        })
    }
}

enum Projection {
    Count,
    Columns(Vec<usize>),
}

/// One query's rows and how to present them.
pub struct Materialized {
    types: Arc<TableTypes>,
    key: Vec<usize>,
    projection: Projection,
    order: Vec<(usize, bool)>,
    limit: Option<usize>,
    columns: Columns,
    rows: HashMap<String, Json>,
}

impl Materialized {
    /// Checks the query can be served from engine rows (every projected
    /// column encodable, every named column known) and loads the
    /// snapshot.
    pub fn new(snapshot: Snapshot, types: Arc<TableTypes>) -> Result<Materialized, Unsupported> {
        let index = |name: &str| -> Result<usize, Unsupported> {
            types
                .column(name)
                .map(|(index, _)| index)
                .ok_or_else(|| Unsupported(format!("column `{name}` is not in the table's types")))
        };
        let key = snapshot
            .key
            .iter()
            .map(|name| index(name))
            .collect::<Result<Vec<_>, _>>()?;
        if key.is_empty() {
            return Err(Unsupported("the table has no primary key".to_string()));
        }
        let order = snapshot
            .order
            .iter()
            .map(|(name, ascending)| index(name).map(|i| (i, *ascending)))
            .collect::<Result<Vec<_>, _>>()?;
        let (projection, columns): (Projection, Columns) = if snapshot.count {
            let name = snapshot
                .names
                .as_ref()
                .and_then(|names| names.first().cloned())
                .unwrap_or_else(|| "count".to_string());
            (Projection::Count, Arc::from(vec![name]))
        } else {
            let indices: Vec<usize> = match &snapshot.columns {
                Some(names) => names
                    .iter()
                    .map(|name| index(name))
                    .collect::<Result<_, _>>()?,
                None => (0..types.columns.len()).collect(),
            };
            for &i in &indices {
                supports(&types.columns[i])?;
            }
            let names = match &snapshot.names {
                Some(names) if names.len() == indices.len() => names.clone(),
                _ => indices
                    .iter()
                    .map(|&i| types.columns[i].name.clone())
                    .collect::<Vec<_>>(),
            };
            (Projection::Columns(indices), Arc::from(names))
        };
        let mut materialized = Materialized {
            types,
            key,
            projection,
            order,
            limit: snapshot.limit,
            columns,
            rows: HashMap::new(),
        };
        for row in snapshot.rows {
            materialized.put(row);
        }
        Ok(materialized)
    }

    /// A row entered or changed.
    pub fn put(&mut self, row: Json) {
        let key = self.key_of(&row);
        self.rows.insert(key, row);
    }

    /// A row left; `key` is the JSON object of its key columns.
    pub fn del(&mut self, key: &Json) {
        let key = self.key_of(key);
        self.rows.remove(&key);
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn key_of(&self, object: &Json) -> String {
        let mut key = String::new();
        for &index in &self.key {
            let name = &self.types.columns[index].name;
            key.push_str(&object.get(name).cloned().unwrap_or(Json::Null).to_string());
            key.push('\u{1f}');
        }
        key
    }

    /// The result as Diesel rows.
    pub fn rows(&self) -> Result<Arc<[OwnedRow]>, Unsupported> {
        if let Projection::Count = self.projection {
            let count = OwnedValue::new(
                std::num::NonZeroU32::new(20).expect("int8"),
                (self.rows.len() as i64).to_be_bytes().to_vec(),
            );
            return Ok(Arc::from(vec![OwnedRow::new(
                Arc::clone(&self.columns),
                vec![Some(count)],
            )]));
        }
        let Projection::Columns(indices) = &self.projection else {
            unreachable!()
        };
        let mut ordered: Vec<(&String, &Json)> = self.rows.iter().collect();
        ordered.sort_by(|(key_a, a), (key_b, b)| {
            for &(index, ascending) in &self.order {
                let name = &self.types.columns[index].name;
                let ordering = compare(a.get(name), b.get(name), ascending);
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            key_a.cmp(key_b)
        });
        let limit = self.limit.unwrap_or(usize::MAX);
        ordered
            .into_iter()
            .take(limit)
            .map(|(_, row)| {
                let values = indices
                    .iter()
                    .map(|&i| {
                        let column = &self.types.columns[i];
                        encode(row.get(&column.name).unwrap_or(&Json::Null), column)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(OwnedRow::new(Arc::clone(&self.columns), values))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Arc::from)
    }
}

/// Postgres's order for one clause: NULLs last ascending, first
/// descending; numbers numerically, strings bytewise, booleans false
/// first.
fn compare(a: Option<&Json>, b: Option<&Json>, ascending: bool) -> Ordering {
    let a = a.filter(|v| !v.is_null());
    let b = b.filter(|v| !v.is_null());
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
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
            if ascending { natural } else { natural.reverse() }
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

    fn types() -> Arc<TableTypes> {
        let base = |name: &str, oid: u32| ColumnType {
            name: name.into(),
            oid,
            typmod: -1,
            kind: TypeKind::Base,
        };
        Arc::new(TableTypes {
            columns: vec![base("id", 23), base("name", 25), base("rank", 23)],
        })
    }

    fn snapshot(rows: Vec<Json>, order: Vec<(&str, bool)>, limit: Option<usize>) -> Snapshot {
        Snapshot {
            table: "t".into(),
            key: vec!["id".into()],
            columns: Some(vec!["id".into(), "name".into()]),
            names: None,
            count: false,
            order: order.into_iter().map(|(c, a)| (c.to_string(), a)).collect(),
            limit,
            rows,
        }
    }

    fn decoded(rows: &Arc<[OwnedRow]>) -> Vec<(i32, String)> {
        rows.iter()
            .map(|row| <(i32, String) as FromSqlRow<(Integer, Text), Pg>>::build_from_row(row).unwrap())
            .collect()
    }

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
        let m = Materialized::new(snap, types()).unwrap();
        assert_eq!(decoded(&m.rows().unwrap()), vec![(3, "c".into()), (1, "b".into())], "NULL rank last, cut to 2");
    }

    #[test]
    fn deltas_change_the_result() {
        let snap = snapshot(
            vec![json!({"id": 1, "name": "b", "rank": 2}), json!({"id": 2, "name": "a", "rank": 1})],
            vec![("name", false)],
            None,
        );
        let mut m = Materialized::new(snap, types()).unwrap();
        assert_eq!(decoded(&m.rows().unwrap()), vec![(1, "b".into()), (2, "a".into())]);
        m.put(json!({"id": 1, "name": "z", "rank": 2}));
        m.put(json!({"id": 3, "name": "m", "rank": 0}));
        m.del(&json!({"id": 2}));
        assert_eq!(decoded(&m.rows().unwrap()), vec![(1, "z".into()), (3, "m".into())]);
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn a_count_is_one_int8() {
        let mut snap = snapshot(vec![json!({"id": 1}), json!({"id": 2})], vec![], Some(1));
        snap.count = true;
        snap.columns = None;
        let m = Materialized::new(snap, types()).unwrap();
        let rows = m.rows().unwrap();
        let count = <i64 as FromSqlRow<BigInt, Pg>>::build_from_row(&rows[0]).unwrap();
        assert_eq!(count, 2, "a count ignores the limit");
    }

    #[test]
    fn an_unencodable_projected_column_refuses_rows_mode() {
        let mut t = (*types()).clone();
        t.columns.push(ColumnType {
            name: "created".into(),
            oid: 1114,
            typmod: -1,
            kind: TypeKind::Base,
        });
        let mut snap = snapshot(vec![], vec![], None);
        snap.columns = Some(vec!["id".into(), "created".into()]);
        assert!(Materialized::new(snap.clone(), Arc::new(t.clone())).is_err());
        snap.columns = Some(vec!["id".into()]);
        assert!(Materialized::new(snap, Arc::new(t)).is_ok(), "unprojected columns do not matter");
    }

    #[test]
    fn output_names_follow_the_aliases() {
        use diesel::deserialize::QueryableByName;
        #[derive(QueryableByName, Debug, PartialEq)]
        struct Named {
            #[diesel(sql_type = Text)]
            n: String,
            #[diesel(sql_type = Integer)]
            i: i32,
        }
        let snap = Snapshot {
            columns: Some(vec!["name".into(), "id".into()]),
            names: Some(vec!["n".into(), "i".into()]),
            ..snapshot(vec![json!({"id": 4, "name": "x", "rank": 1})], vec![], None)
        };
        let m = Materialized::new(snap, types()).unwrap();
        let rows = m.rows().unwrap();
        assert_eq!(Named::build(&rows[0]).unwrap(), Named { n: "x".into(), i: 4 });
        assert_eq!(rows[0].columns().iter().map(String::as_str).collect::<Vec<_>>(), ["n", "i"]);
    }

    #[test]
    fn a_nullable_projection_decodes() {
        let snap = Snapshot {
            columns: Some(vec!["rank".into()]),
            ..snapshot(vec![json!({"id": 1, "name": "x", "rank": null})], vec![], None)
        };
        let m = Materialized::new(snap, types()).unwrap();
        let rows = m.rows().unwrap();
        let rank = <Option<i32> as FromSqlRow<Nullable<Integer>, Pg>>::build_from_row(&rows[0]).unwrap();
        assert_eq!(rank, None);
    }
}
