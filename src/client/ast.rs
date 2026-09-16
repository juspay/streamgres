//! Zero's query AST, as the application server returns it, translated
//! into the engine's query tree: `related` edges become LEFT joins,
//! `EXISTS` subqueries in the filter become INNER joins with an `EXISTS`
//! leaf in their place, a keyset `start` becomes the `WHERE` it means, the
//! root's `limit` becomes its window and a `related` node's `limit` its
//! window per parent row (the engine's driven windows), the primary key is
//! appended to the ordering when absent so pages are deterministic, and
//! every literal is coerced to its column's catalog type. Subqueries Zero
//! marks as permission checks are registered but their parts are not
//! shipped to the client, the way zero-cache withholds them.

use std::collections::HashSet;

use serde::Deserialize;
use serde_json::Value as Json;

use crate::ivm::QueryPart;
use crate::model::ComparisonOperator::{EQ, GT, GTE, IN, LT, LTE, NEQ, NOT_IN};
use crate::model::{
    Catalog, ComparisonOperator, DbTable, Join, MultiTableReadQuery, Order, OrderBy,
    SingleTableReadQuery, Value, ValueType, Where,
};

/// One query node.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ast {
    pub table: String,
    #[serde(default, rename = "where")]
    pub filter: Option<Box<Condition>>,
    #[serde(default)]
    pub related: Vec<CorrelatedSubquery>,
    #[serde(default)]
    pub limit: Option<f64>,
    #[serde(default)]
    pub order_by: Vec<(String, String)>,
    #[serde(default)]
    pub start: Option<Bound>,
}

/// A keyset cursor: the row to start after (or at).
#[derive(Debug, Clone, Deserialize)]
pub struct Bound {
    pub row: serde_json::Map<String, Json>,
    #[serde(default)]
    pub exclusive: bool,
}

/// A predicate node.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Condition {
    Simple {
        op: String,
        left: Operand,
        right: Operand,
    },
    And {
        conditions: Vec<Condition>,
    },
    Or {
        conditions: Vec<Condition>,
    },
    CorrelatedSubquery {
        related: CorrelatedSubquery,
        op: String,
    },
}

/// One side of a comparison.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Operand {
    Literal {
        value: Json,
    },
    Column {
        name: String,
    },
    Static {
        #[serde(default)]
        anchor: Option<String>,
        #[serde(default)]
        field: Json,
    },
}

/// A subquery joined to its parent on one column pair.
#[derive(Debug, Clone, Deserialize)]
pub struct CorrelatedSubquery {
    pub correlation: Correlation,
    pub subquery: Box<Ast>,
    #[serde(default)]
    pub hidden: Option<bool>,
    #[serde(default)]
    pub system: Option<String>,
}

/// The join columns of a subquery.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Correlation {
    pub parent_field: Vec<String>,
    pub child_field: Vec<String>,
}

/// A translated query: the tree to register, and the parts whose rows
/// stay on the server (permission subqueries).
#[derive(Debug, Clone)]
pub struct Translated {
    pub query: MultiTableReadQuery,
    pub hidden: HashSet<QueryPart>,
}

/// Translate `ast` against `catalog`; an error says what the engine
/// cannot run.
pub fn translate(ast: &Ast, catalog: &Catalog) -> Result<Translated, String> {
    let mut hidden = HashSet::new();
    let query = node(ast, Vec::new(), false, catalog, &mut hidden)?;
    Ok(Translated { query, hidden })
}

/// Build the tree below `ast` at `path`.
fn node(
    ast: &Ast,
    path: Vec<usize>,
    concealed: bool,
    catalog: &Catalog,
    hidden: &mut HashSet<QueryPart>,
) -> Result<MultiTableReadQuery, String> {
    let table = catalog
        .table(&ast.table)
        .ok_or_else(|| format!("unknown table `{}`", ast.table))?;
    if concealed {
        hidden.insert(QueryPart(path.clone()));
    }
    let mut inner: Vec<&CorrelatedSubquery> = Vec::new();
    let filter = match &ast.filter {
        Some(condition) => translate_condition(condition, table, &mut inner)?,
        None => Where::AND(Vec::new()),
    };
    let mut order = Vec::with_capacity(ast.order_by.len() + table.pkey.len());
    for (column, direction) in &ast.order_by {
        if table.column(column).is_none() {
            return Err(format!(
                "unknown column `{}.{column}` in ORDER BY",
                ast.table
            ));
        }
        let direction = match direction.as_str() {
            "asc" => Order::ASC,
            "desc" => Order::DESC,
            other => return Err(format!("unknown sort direction `{other}`")),
        };
        order.push(OrderBy::new(column.as_str(), direction));
    }
    for key in &table.pkey {
        if !order.iter().any(|clause| clause.column == *key) {
            order.push(OrderBy::new(key.clone(), Order::ASC));
        }
    }
    let filter = match &ast.start {
        Some(bound) => {
            let cursor = keyset(bound, &order, table)?;
            if matches!(&filter, Where::AND(children) if children.is_empty()) {
                cursor
            } else {
                collapse(vec![filter, cursor])
            }
        }
        None => filter,
    };
    let limit = match ast.limit {
        Some(limit) => limit.max(0.0).min(u32::MAX as f64) as u32,
        None => u32::MAX,
    };
    let main_table = SingleTableReadQuery::new(table.name.clone(), normalize(filter), order, limit);
    let left_count = ast.related.len();
    let mut left_joins = Vec::with_capacity(left_count);
    for (index, sub) in ast.related.iter().enumerate() {
        let mut child_path = path.clone();
        child_path.push(index);
        left_joins.push(edge(sub, child_path, concealed, catalog, hidden)?);
    }
    let mut inner_joins = Vec::with_capacity(inner.len());
    for (offset, sub) in inner.iter().enumerate() {
        let mut child_path = path.clone();
        child_path.push(left_count + offset);
        inner_joins.push(edge(sub, child_path, concealed, catalog, hidden)?);
    }
    Ok(MultiTableReadQuery {
        main_table,
        left_joins,
        right_joins: Vec::new(),
        inner_joins,
    })
}

/// One join edge and the subtree under it.
fn edge(
    sub: &CorrelatedSubquery,
    child_path: Vec<usize>,
    concealed_parent: bool,
    catalog: &Catalog,
    hidden: &mut HashSet<QueryPart>,
) -> Result<Join, String> {
    let (Some(parent_column), Some(child_column)) = (
        sub.correlation.parent_field.first(),
        sub.correlation.child_field.first(),
    ) else {
        return Err("a join without columns".to_owned());
    };
    if sub.correlation.parent_field.len() != 1 || sub.correlation.child_field.len() != 1 {
        return Err("compound join keys are not supported".to_owned());
    }
    let concealed = concealed_parent || sub.system.as_deref() == Some("permissions");
    let child = node(&sub.subquery, child_path, concealed, catalog, hidden)?;
    Ok(Join::new(
        child,
        parent_column.as_str(),
        child_column.as_str(),
    ))
}

/// A predicate node as a `Where`; `EXISTS` subqueries are collected into
/// `inner`, numbered in order of appearance.
fn translate_condition<'a>(
    condition: &'a Condition,
    table: &DbTable,
    inner: &mut Vec<&'a CorrelatedSubquery>,
) -> Result<Where, String> {
    match condition {
        Condition::Simple { op, left, right } => simple(op, left, right, table),
        Condition::And { conditions } => Ok(Where::AND(
            conditions
                .iter()
                .map(|child| translate_condition(child, table, inner))
                .collect::<Result<_, _>>()?,
        )),
        Condition::Or { conditions } => Ok(Where::OR(
            conditions
                .iter()
                .map(|child| translate_condition(child, table, inner))
                .collect::<Result<_, _>>()?,
        )),
        Condition::CorrelatedSubquery { related, op } => match op.as_str() {
            "EXISTS" => {
                let column = related
                    .correlation
                    .parent_field
                    .first()
                    .ok_or("an EXISTS without a join column")?;
                if table.column(column).is_none() {
                    return Err(format!(
                        "unknown column `{}.{column}` in EXISTS",
                        table.name
                    ));
                }
                let index = inner.len();
                inner.push(related);
                Ok(Where::exists(column.as_str(), index))
            }
            "NOT EXISTS" => Err("NOT EXISTS is not supported".to_owned()),
            other => Err(format!("unknown subquery operator `{other}`")),
        },
    }
}

/// One `column op literal` leaf.
fn simple(op: &str, left: &Operand, right: &Operand, table: &DbTable) -> Result<Where, String> {
    let Operand::Column { name } = left else {
        return Err("only a column can be compared".to_owned());
    };
    let column = table
        .column(name)
        .ok_or_else(|| format!("unknown column `{}.{name}`", table.name))?;
    let literal = match right {
        Operand::Literal { value } => value,
        Operand::Static { .. } => {
            return Err(format!(
                "an unresolved parameter reached the engine in a condition on `{name}`"
            ));
        }
        Operand::Column { .. } => {
            return Err("column-to-column comparisons are not supported".to_owned());
        }
    };
    let operator = match op {
        "=" => EQ,
        "!=" => NEQ,
        "<" => LT,
        ">" => GT,
        "<=" => LTE,
        ">=" => GTE,
        "IN" => IN,
        "NOT IN" => NOT_IN,
        "IS" => {
            return Ok(if literal.is_null() {
                Where::is_null(name.as_str())
            } else {
                Where::condition(name.as_str(), EQ, literal_value(literal, &column.r#type)?)
            });
        }
        "IS NOT" => {
            return Ok(if literal.is_null() {
                Where::is_not_null(name.as_str())
            } else {
                Where::condition(name.as_str(), NEQ, literal_value(literal, &column.r#type)?)
            });
        }
        "LIKE" | "ILIKE" | "NOT LIKE" | "NOT ILIKE" => {
            return Err(format!("{op} is not supported (a condition on `{name}`)"));
        }
        other => return Err(format!("unknown operator `{other}`")),
    };
    let value = if matches!(
        operator,
        ComparisonOperator::IN | ComparisonOperator::NOT_IN
    ) {
        let Json::Array(items) = literal else {
            return Err(format!("{op} on `{name}` needs a list"));
        };
        Value::List(
            items
                .iter()
                .map(|item| literal_value(item, &column.r#type))
                .collect::<Result<_, _>>()?,
        )
    } else {
        literal_value(literal, &column.r#type)?
    };
    Ok(Where::condition(name.as_str(), operator, value))
}

/// A JSON literal as the value a column of type `declared` compares
/// against.
fn literal_value(literal: &Json, declared: &ValueType) -> Result<Value, String> {
    Ok(match (literal, declared) {
        (Json::Null, _) => Value::Null,
        (Json::Bool(flag), _) => Value::Bool(*flag),
        (Json::Number(number), ValueType::Float) => {
            Value::Float(number.as_f64().unwrap_or(f64::NAN))
        }
        (Json::Number(number), ValueType::String | ValueType::Json) => {
            Value::String(number.to_string())
        }
        (Json::Number(number), _) => match number.as_i64() {
            Some(int) => Value::Int(int),
            None => Value::Float(number.as_f64().unwrap_or(f64::NAN)),
        },
        (Json::String(text), ValueType::Int | ValueType::Timestamp) => text
            .parse::<i64>()
            .map(Value::Int)
            .map_err(|_| format!("`{text}` is not a number"))?,
        (Json::String(text), ValueType::Float) => text
            .parse::<f64>()
            .map(Value::Float)
            .map_err(|_| format!("`{text}` is not a number"))?,
        (Json::String(text), _) => Value::String(text.clone()),
        (Json::Array(items), ValueType::List(inner)) => Value::List(
            items
                .iter()
                .map(|item| literal_value(item, inner))
                .collect::<Result<_, _>>()?,
        ),
        (Json::Array(items), _) => Value::List(
            items
                .iter()
                .map(|item| literal_value(item, declared))
                .collect::<Result<_, _>>()?,
        ),
        (Json::Object(_), ValueType::Json) => Value::String(literal.to_string()),
        (Json::Object(_), _) => return Err("an object literal in a comparison".to_owned()),
    })
}

/// The `WHERE` a keyset cursor means over `order`: one branch per sort
/// column the cursor names (a cursor may stop short of the trailing
/// columns, the appended primary key in particular, and is then compared
/// on the prefix it has), earlier columns tied and the column itself
/// strictly past the cursor, plus the cursor row itself when inclusive.
fn keyset(bound: &Bound, order: &[OrderBy], table: &DbTable) -> Result<Where, String> {
    let mut keys = Vec::with_capacity(order.len());
    for clause in order {
        let name = clause.column.as_str();
        let Some(literal) = bound.row.get(name) else {
            break;
        };
        let declared = &table
            .column(name)
            .ok_or_else(|| format!("unknown column `{name}`"))?
            .r#type;
        keys.push((name, clause.direction, literal_value(literal, declared)?));
    }
    if keys.is_empty() {
        return Err("the cursor names none of the sort columns".to_owned());
    }
    let mut branches = Vec::with_capacity(keys.len() + 1);
    for (index, (column, direction, value)) in keys.iter().enumerate() {
        let op = match direction {
            Order::ASC => GT,
            Order::DESC => LT,
        };
        let mut conjuncts: Vec<Where> = keys[..index]
            .iter()
            .map(|(earlier, _, earlier_value)| {
                Where::condition(*earlier, EQ, earlier_value.clone())
            })
            .collect();
        conjuncts.push(Where::condition(*column, op, value.clone()));
        branches.push(collapse(conjuncts));
    }
    if !bound.exclusive {
        branches.push(collapse(
            keys.iter()
                .map(|(column, _, value)| Where::condition(*column, EQ, value.clone()))
                .collect(),
        ));
    }
    Ok(if branches.len() == 1 {
        branches.pop().expect("one branch")
    } else {
        Where::OR(branches)
    })
}

/// One part is itself; several are an `AND`; none is `TRUE`.
fn collapse(mut parts: Vec<Where>) -> Where {
    if parts.len() == 1 {
        parts.pop().expect("one part")
    } else {
        Where::AND(parts)
    }
}

/// Unwrap every single-child `AND` / `OR`.
fn normalize(filter: Where) -> Where {
    match filter {
        Where::Condition(condition) => Where::Condition(condition),
        Where::AND(children) => collapse(children.into_iter().map(normalize).collect()),
        Where::OR(children) => {
            let mut children: Vec<Where> = children.into_iter().map(normalize).collect();
            if children.len() == 1 {
                children.pop().expect("one child")
            } else {
                Where::OR(children)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DbColumn, DbTable};

    /// A small catalog: messages in conversations in channels, with
    /// participants.
    fn catalog() -> Catalog {
        Catalog::new(vec![
            DbTable::new(
                "messages",
                ["messageId"],
                vec![
                    DbColumn::new("messageId", ValueType::String),
                    DbColumn::new("conversationId", ValueType::String),
                    DbColumn::new("createdAt", ValueType::Timestamp),
                    DbColumn::new("visibleTo", ValueType::String),
                    DbColumn::new("metadata", ValueType::Json),
                ],
            ),
            DbTable::new(
                "conversations",
                ["conversationId"],
                vec![
                    DbColumn::new("conversationId", ValueType::String),
                    DbColumn::new("channelId", ValueType::String),
                ],
            ),
            DbTable::new(
                "channel_participants",
                ["id"],
                vec![
                    DbColumn::new("id", ValueType::String),
                    DbColumn::new("channelId", ValueType::String),
                    DbColumn::new("userId", ValueType::String),
                ],
            ),
        ])
    }

    /// A message thread with a visibility rule, a related conversation,
    /// an EXISTS on participants, a compound order with a cursor and a
    /// limit, translated end to end.
    #[test]
    fn translates_a_thread_query() {
        let ast: Ast = serde_json::from_str(r#"{
            "table": "messages",
            "where": {"type": "and", "conditions": [
                {"type": "simple", "op": "=", "left": {"type": "column", "name": "conversationId"}, "right": {"type": "literal", "value": "cv1"}},
                {"type": "or", "conditions": [
                    {"type": "simple", "op": "IS", "left": {"type": "column", "name": "visibleTo"}, "right": {"type": "literal", "value": null}},
                    {"type": "simple", "op": "=", "left": {"type": "column", "name": "visibleTo"}, "right": {"type": "literal", "value": "me"}}
                ]},
                {"type": "correlatedSubquery", "op": "EXISTS", "related": {
                    "correlation": {"parentField": ["conversationId"], "childField": ["conversationId"]},
                    "system": "permissions",
                    "subquery": {"table": "conversations", "where": {"type": "correlatedSubquery", "op": "EXISTS", "related": {
                        "correlation": {"parentField": ["channelId"], "childField": ["channelId"]},
                        "subquery": {"table": "channel_participants", "where": {"type": "simple", "op": "=", "left": {"type": "column", "name": "userId"}, "right": {"type": "literal", "value": "me"}}}
                    }}}
                }}
            ]},
            "related": [{"correlation": {"parentField": ["conversationId"], "childField": ["conversationId"]}, "subquery": {"table": "conversations", "limit": 1}}],
            "orderBy": [["createdAt", "desc"]],
            "start": {"row": {"createdAt": 1000, "messageId": "m9"}, "exclusive": true},
            "limit": 20
        }"#).unwrap();
        let translated = translate(&ast, &catalog()).unwrap();
        let root = &translated.query;
        assert_eq!(root.main_table.limit, 20);
        assert_eq!(
            root.main_table.order_by.len(),
            2,
            "the primary key is appended"
        );
        assert_eq!(root.left_joins.len(), 1);
        assert_eq!(root.inner_joins.len(), 1);
        assert_eq!(
            root.left_joins[0].sub.main_table.limit, 1,
            "a related limit is kept, as a window per parent row"
        );
        assert_eq!(
            root.inner_joins[0].sub.inner_joins.len(),
            1,
            "nested EXISTS"
        );
        assert!(
            translated.hidden.contains(&QueryPart(vec![1])),
            "the permission part is hidden"
        );
        assert!(
            translated.hidden.contains(&QueryPart(vec![1, 0])),
            "and its subtree"
        );
        assert!(!translated.hidden.contains(&QueryPart(vec![0])));
        let rendered = format!("{:?}", root.main_table.filter);
        assert!(
            rendered.contains("EXISTS"),
            "the EXISTS leaf sits in the filter: {rendered}"
        );
        assert!(
            rendered.contains("LT") && rendered.contains("GT"),
            "the cursor is a WHERE: {rendered}"
        );
    }

    /// A cursor that stops short of the appended primary key is compared
    /// on the columns it has; one that names no sort column is refused.
    #[test]
    fn cursors_compare_on_their_prefix() {
        let ast: Ast = serde_json::from_str(r#"{"table": "messages", "orderBy": [["createdAt", "desc"]], "start": {"row": {"createdAt": 1000}, "exclusive": true}, "limit": 5}"#).unwrap();
        let translated = translate(&ast, &catalog()).unwrap();
        assert_eq!(
            translated.query.main_table.filter,
            Where::condition("createdAt", LT, Value::Int(1000))
        );
        let empty: Ast = serde_json::from_str(r#"{"table": "messages", "orderBy": [["createdAt", "desc"]], "start": {"row": {"other": 1}, "exclusive": true}}"#).unwrap();
        assert!(
            translate(&empty, &catalog())
                .unwrap_err()
                .contains("none of the sort columns")
        );
    }

    /// What the engine cannot run is refused by name.
    #[test]
    fn refuses_what_it_cannot_run() {
        let like: Ast = serde_json::from_str(r#"{"table": "messages", "where": {"type": "simple", "op": "ILIKE", "left": {"type": "column", "name": "visibleTo"}, "right": {"type": "literal", "value": "%x%"}}}"#).unwrap();
        assert!(translate(&like, &catalog()).unwrap_err().contains("ILIKE"));
        let unknown: Ast = serde_json::from_str(r#"{"table": "nope"}"#).unwrap();
        assert!(
            translate(&unknown, &catalog())
                .unwrap_err()
                .contains("nope")
        );
        let not_exists: Ast = serde_json::from_str(r#"{"table": "messages", "where": {"type": "correlatedSubquery", "op": "NOT EXISTS", "related": {"correlation": {"parentField": ["conversationId"], "childField": ["conversationId"]}, "subquery": {"table": "conversations"}}}}"#).unwrap();
        assert!(
            translate(&not_exists, &catalog())
                .unwrap_err()
                .contains("NOT EXISTS")
        );
    }
}
