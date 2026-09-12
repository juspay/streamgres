//! Rendering the query model as Postgres SQL: a [`SingleTableReadQuery`]
//! becomes one `SELECT` whose columns are cast to the types the catalog
//! declares (so every result decodes the same way), whose `WHERE` is the
//! filter tree rendered with the engine's two-valued semantics (every
//! leaf is wrapped in `IS TRUE`, so a comparison touching `NULL` is
//! false rather than unknown and `NOT` of it is not true either; an
//! empty `IN` list is false, a `NOT IN` with a `NULL` member is never
//! true), and whose `ORDER BY` / `LIMIT` are emitted only for a finite
//! limit.

use std::fmt::Write;

use crate::model::{
    ComparisonOperator, Condition, DbTable, Order, SingleTableReadQuery, Value, ValueType, Where,
};

/// The `SELECT` for `query` over `table`'s declared columns, in the
/// column order [`select_columns`] returns.
pub fn select_sql(query: &SingleTableReadQuery, table: &DbTable) -> String {
    let mut sql = String::from("SELECT ");
    let columns = select_columns(table);
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        }
        let declared = &table.columns[*column].r#type;
        let _ = write!(sql, "{}::{}", quote_ident(column.as_str()), cast_of(declared));
    }
    let _ = write!(sql, " FROM {} WHERE {}", quote_ident(query.table.as_str()), render_where(&query.filter));
    if query.limit != u32::MAX {
        let direction = match query.order_by.direction {
            Order::ASC => "ASC NULLS LAST",
            Order::DESC => "DESC NULLS FIRST",
        };
        let _ = write!(
            sql,
            " ORDER BY {} {direction} LIMIT {}",
            quote_ident(query.order_by.column.as_str()),
            query.limit
        );
    }
    sql
}

/// The table's columns in a fixed order: primary-key columns first (in
/// declaration order), then the rest sorted by name.
pub fn select_columns(table: &DbTable) -> Vec<&crate::model::ColumnName> {
    let mut columns: Vec<&crate::model::ColumnName> = table.pkey.iter().collect();
    let mut rest: Vec<&crate::model::ColumnName> = table
        .columns
        .keys()
        .filter(|column| !table.pkey.contains(column))
        .collect();
    rest.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    columns.extend(rest);
    columns
}

/// The Postgres type every column of a declared type is cast to on the
/// way out, so decoding is uniform whatever the column's own type.
pub fn cast_of(declared: &ValueType) -> &'static str {
    match declared {
        ValueType::Int => "int8",
        ValueType::Float => "float8",
        ValueType::String => "text",
        ValueType::Bool => "bool",
        ValueType::Date => "date",
        ValueType::Datetime => "timestamp",
        ValueType::List(_) | ValueType::Map(_, _) => "text",
    }
}

/// A double-quoted identifier.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The filter tree as a boolean expression.
fn render_where(filter: &Where) -> String {
    match filter {
        Where::Condition(condition) => render_condition(condition),
        Where::AND(children) if children.is_empty() => "TRUE".to_owned(),
        Where::OR(children) if children.is_empty() => "FALSE".to_owned(),
        Where::AND(children) => {
            let parts: Vec<String> = children.iter().map(render_where).collect();
            format!("({})", parts.join(" AND "))
        }
        Where::OR(children) => {
            let parts: Vec<String> = children.iter().map(render_where).collect();
            format!("({})", parts.join(" OR "))
        }
    }
}

/// One leaf, with the engine's `NULL` and list semantics made explicit.
fn render_condition(condition: &Condition) -> String {
    use ComparisonOperator::*;
    let column = quote_ident(condition.column.as_str());
    match condition.comparison_operator {
        IN | NOT_IN => {
            let members = members_of(&condition.value);
            let negated = condition.comparison_operator == NOT_IN;
            if negated && members.iter().any(Value::is_null) {
                return "FALSE".to_owned();
            }
            let literals: Vec<String> = members
                .iter()
                .filter(|value| !value.is_null())
                .map(literal)
                .collect();
            if literals.is_empty() {
                return if negated { "TRUE" } else { "FALSE" }.to_owned();
            }
            let keyword = if negated { "NOT IN" } else { "IN" };
            format!("{column} {keyword} ({}) IS TRUE", literals.join(", "))
        }
        _ if condition.value.is_null() => "FALSE".to_owned(),
        EQ => format!("{column} = {} IS TRUE", literal(&condition.value)),
        NEQ => format!("{column} <> {} IS TRUE", literal(&condition.value)),
        GT => format!("{column} > {} IS TRUE", literal(&condition.value)),
        GTE => format!("{column} >= {} IS TRUE", literal(&condition.value)),
        LT => format!("{column} < {} IS TRUE", literal(&condition.value)),
        LTE => format!("{column} <= {} IS TRUE", literal(&condition.value)),
    }
}

/// The members of an `IN` / `NOT IN` operand: a literal list's items, a
/// shared set's current members, or the lone value itself.
fn members_of(value: &Value) -> Vec<Value> {
    match value {
        Value::List(items) => items.clone(),
        Value::Set(set) => set.members(),
        other => vec![other.clone()],
    }
}

/// A value as a SQL literal.
fn literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::String(text) => quote_literal(text),
        Value::Int(int) => int.to_string(),
        Value::Float(float) if float.is_nan() => "'NaN'::float8".to_owned(),
        Value::Float(float) if float.is_infinite() && *float > 0.0 => "'Infinity'::float8".to_owned(),
        Value::Float(float) if float.is_infinite() => "'-Infinity'::float8".to_owned(),
        Value::Float(float) => format!("{float:?}::float8"),
        Value::Bool(true) => "TRUE".to_owned(),
        Value::Bool(false) => "FALSE".to_owned(),
        Value::Date(date) => format!("DATE '{}'", date.format("%Y-%m-%d")),
        Value::Datetime(datetime) => format!("TIMESTAMP '{}'", datetime.format("%Y-%m-%d %H:%M:%S%.6f")),
        Value::List(_) | Value::Map(_) | Value::Set(_) => "NULL".to_owned(),
    }
}

/// A single-quoted string literal, quotes doubled.
fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DbColumn, OrderBy, SharedSet};

    /// `tickets(id, status, points)`.
    fn tickets() -> DbTable {
        DbTable::new(
            "tickets",
            ["id"],
            vec![
                DbColumn::new("id", ValueType::Int),
                DbColumn::new("status", ValueType::String),
                DbColumn::new("points", ValueType::Int),
            ],
        )
    }

    /// Columns are cast, the filter is parenthesized, and an unbounded
    /// query carries no ORDER BY / LIMIT.
    #[test]
    fn renders_a_filtered_select() {
        let query = SingleTableReadQuery::new(
            "tickets",
            Where::AND(vec![
                Where::condition("status", ComparisonOperator::EQ, "it's open"),
                Where::condition("points", ComparisonOperator::GTE, 8),
            ]),
            OrderBy::new("id", Order::ASC),
            u32::MAX,
        );
        assert_eq!(
            select_sql(&query, &tickets()),
            "SELECT \"id\"::int8, \"points\"::int8, \"status\"::text FROM \"tickets\" WHERE (\"status\" = 'it''s open' IS TRUE AND \"points\" >= 8 IS TRUE)"
        );
    }

    /// A finite limit orders (nulls last, Postgres's default for ASC) and
    /// limits; a set-valued IN renders its current members; an empty IN
    /// is FALSE; a NULL comparison is FALSE; a NOT IN with a NULL member
    /// is FALSE.
    #[test]
    fn renders_windows_sets_and_null_semantics() {
        let set = SharedSet::default();
        set.insert(&Value::Int(7));
        set.insert(&Value::Int(9));
        let query = SingleTableReadQuery::new(
            "tickets",
            Where::OR(vec![
                Where::Condition(Condition::new("points", ComparisonOperator::IN, Value::Set(set))),
                Where::Condition(Condition::new("points", ComparisonOperator::IN, Value::List(Vec::new()))),
                Where::Condition(Condition::new("status", ComparisonOperator::EQ, Value::Null)),
                Where::Condition(Condition::new(
                    "status",
                    ComparisonOperator::NOT_IN,
                    Value::List(vec!["a".into(), Value::Null]),
                )),
            ]),
            OrderBy::new("points", Order::DESC),
            5,
        );
        let sql = select_sql(&query, &tickets());
        assert!(sql.contains("\"points\" IN (7, 9) IS TRUE") || sql.contains("\"points\" IN (9, 7) IS TRUE"), "{sql}");
        assert!(sql.contains(" OR FALSE OR FALSE OR FALSE)"), "{sql}");
        assert!(sql.ends_with("ORDER BY \"points\" DESC NULLS FIRST LIMIT 5"), "{sql}");
    }
}
