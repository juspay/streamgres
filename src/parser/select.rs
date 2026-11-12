//! A `SELECT` as an ORM renders it, over one table, with its bind
//! parameters, into the engine's query and what the statement returns.
//!
//! ```text
//! select     := SELECT projection FROM table [WHERE filter]
//!               [ORDER BY order (, order)*] [LIMIT count] [OFFSET count]
//! projection := '*' | table '.' '*' | COUNT '(' '*' ')' | column (, column)*
//! order      := column [ASC | DESC] [NULLS FIRST | NULLS LAST]
//! count      := integer | $n
//! ```
//!
//! Names may be quoted and qualified (`"schema"."table"."column"`), as
//! Diesel writes them, and the filter takes `$n` parameters wherever it
//! takes a literal, `= ANY($n)` and `!= ALL($n)` over an array parameter,
//! and `BETWEEN`.
//!
//! The engine must answer exactly what Postgres would, so a statement is
//! refused, with the reason, when it compares or orders a column the
//! engine cannot compare the way Postgres does ([`check_exact`]).

use super::{Bind, ParseError, Parser, Tok, default_order, lex};
use crate::model::{
    Catalog, ColumnName, ComparisonOperator, Condition, DbColumn, DbTable, Order, OrderBy,
    SingleTableReadQuery, Value, ValueType, Where,
};
use crate::sync::pg::binary::oid;

/// What a statement returns of the rows it reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    /// Every column, in the table's order.
    All,
    /// These columns, in this order.
    Columns(Vec<ColumnName>),
    /// One row holding the number of rows matched.
    CountStar,
}

/// A parsed `SELECT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Select {
    /// The rows the engine follows: the filter and order, and as many
    /// rows as the statement may return after skipping its offset
    /// (`limit + offset`; no limit for a count).
    pub query: SingleTableReadQuery,
    pub projection: Projection,
    /// The statement's own `LIMIT`, `u32::MAX` for none.
    pub limit: u32,
    /// Rows the statement skips.
    pub offset: u32,
}

/// Parses `sql` with its bind parameters against `catalog`.
pub fn parse_select(sql: &str, binds: &[Bind], catalog: &Catalog) -> Result<Select, ParseError> {
    let mut parser = Parser {
        tokens: lex(sql)?,
        pos: 0,
        depth: 0,
        binds: binds.to_vec(),
    };
    let select = parser.select(catalog)?;
    parser.eat_sym(";");
    if !matches!(parser.peek(), Tok::Eof) {
        return parser.err(format!("unexpected trailing input: {}", parser.describe()));
    }
    Ok(select)
}

impl Parser {
    fn select(&mut self, catalog: &Catalog) -> Result<Select, ParseError> {
        self.expect_word("SELECT")?;
        if self.is_word("DISTINCT") {
            return self.err("DISTINCT is not supported");
        }
        let projection_at = self.pos;
        self.skip_projection()?;
        self.expect_word("FROM")?;
        let table = self.table(catalog)?;
        if self.is_word("JOIN")
            || self.is_word("INNER")
            || self.is_word("LEFT")
            || self.is_word("RIGHT")
            || self.is_word("FULL")
            || self.is_word("CROSS")
            || self.is_sym(",")
        {
            return self.err("joins are not supported yet: one table per statement");
        }
        let after_from = self.pos;
        self.pos = projection_at;
        let projection = self.projection(table)?;
        self.pos = after_from;

        let filter = if self.eat_word("WHERE") {
            self.filter(table)?
        } else {
            Where::AND(Vec::new())
        };
        let order_by = if self.eat_word("ORDER") {
            self.expect_word("BY")?;
            let mut clauses = Vec::new();
            loop {
                clauses.push(self.order(table)?);
                if !self.eat_sym(",") {
                    break;
                }
            }
            clauses
        } else {
            vec![default_order(table, self.at())?]
        };
        let limit = if self.eat_word("LIMIT") {
            self.count(u32::MAX)?
        } else {
            u32::MAX
        };
        let offset = if self.eat_word("OFFSET") {
            self.count(0)?
        } else {
            0
        };
        if self.is_word("FOR") {
            return self.err("a locking read (FOR UPDATE, FOR SHARE) is not supported");
        }
        check_exact(table, &filter, &order_by, self.at())?;

        let followed = match projection {
            Projection::CountStar => u32::MAX,
            _ => limit.saturating_add(offset),
        };
        Ok(Select {
            query: SingleTableReadQuery::new(table.name.clone(), filter, order_by, followed),
            projection,
            limit,
            offset,
        })
    }

    /// Steps over the projection to the `FROM`, which it must come before.
    fn skip_projection(&mut self) -> Result<(), ParseError> {
        let mut depth = 0usize;
        loop {
            match self.peek() {
                Tok::Eof => return self.err("expected FROM"),
                Tok::Word(word) if depth == 0 && word.eq_ignore_ascii_case("FROM") => {
                    return Ok(());
                }
                Tok::Sym("(") => depth += 1,
                Tok::Sym(")") => depth = depth.saturating_sub(1),
                _ => {}
            }
            self.bump();
        }
    }

    /// The projection, its columns checked against `table`.
    fn projection(&mut self, table: &DbTable) -> Result<Projection, ParseError> {
        if self.eat_sym("*") {
            return Ok(Projection::All);
        }
        if self.is_word("COUNT") {
            self.bump();
            self.expect_sym("(")?;
            self.expect_sym("*")?;
            self.expect_sym(")")?;
            return Ok(Projection::CountStar);
        }
        let mut columns = Vec::new();
        loop {
            let at = self.pos;
            if self.qualified_star(table)? {
                if columns.is_empty() && self.is_word("FROM") {
                    return Ok(Projection::All);
                }
                return self.err("`table.*` must be the whole projection");
            }
            self.pos = at;
            if self
                .tokens
                .get(self.pos + 1)
                .is_some_and(|next| next.tok == Tok::Sym("("))
            {
                return self.err(format!(
                    "a projection is columns of the table, not {}(...)",
                    self.describe()
                ));
            }
            columns.push(self.column_name(table)?);
            if matches!(self.peek(), Tok::Quoted(_) | Tok::Word(_)) && !self.is_word("FROM") {
                return self.err("a projected column cannot be renamed or computed");
            }
            if !self.eat_sym(",") {
                break;
            }
        }
        if !self.is_word("FROM") {
            return self.err(format!(
                "a projection is columns of the table, found {}",
                self.describe()
            ));
        }
        Ok(Projection::Columns(columns))
    }

    /// Consumes `table.*` (or `schema.table.*`) when it is next.
    fn qualified_star(&mut self, table: &DbTable) -> Result<bool, ParseError> {
        let mut names = Vec::new();
        while matches!(self.peek(), Tok::Word(_) | Tok::Quoted(_)) {
            names.push(self.ident()?);
            if !self.eat_sym(".") {
                return Ok(false);
            }
            if self.eat_sym("*") {
                let named = match names.as_slice() {
                    [schema, name] if schema == "public" => name.clone(),
                    [schema, name] => format!("{schema}.{name}"),
                    _ => names.concat(),
                };
                if named != table.name.as_str() {
                    return self.err(format!("`{named}` is not the table read"));
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// One `ORDER BY` clause. Postgres puts `NULL`s last ascending and first
    /// descending, as the engine does; the other way round is refused.
    fn order(&mut self, table: &DbTable) -> Result<OrderBy, ParseError> {
        let column = self.column_name(table)?;
        let direction = if self.eat_word("DESC") {
            Order::DESC
        } else {
            self.eat_word("ASC");
            Order::ASC
        };
        if self.eat_word("NULLS") {
            let first = if self.eat_word("FIRST") {
                true
            } else {
                self.expect_word("LAST")?;
                false
            };
            if first != (direction == Order::DESC) {
                return self.err("NULLS FIRST ascending or NULLS LAST descending is not supported");
            }
        }
        Ok(OrderBy::new(column, direction))
    }

    /// A row count: a non-negative integer, or a parameter bound to one;
    /// `null` for a parameter bound to `NULL` (no limit, no offset).
    fn count(&mut self, null: u32) -> Result<u32, ParseError> {
        let at = self.at();
        let value = match self.peek().clone() {
            Tok::Num(number) => {
                self.bump();
                number.parse::<i64>().ok()
            }
            Tok::Param(number) => {
                self.bump();
                let bind = number
                    .checked_sub(1)
                    .and_then(|index| self.binds.get(index))
                    .ok_or(ParseError {
                        message: format!("no value is bound for ${number}"),
                        position: at,
                    })?;
                match &bind.bytes {
                    None => return Ok(null),
                    Some(bytes) => {
                        match crate::sync::pg::binary::decode_scalar(bind.type_oid, bytes) {
                            Ok(Value::Int(count)) => Some(count),
                            _ => None,
                        }
                    }
                }
            }
            _ => None,
        };
        value
            .and_then(|count| u32::try_from(count).ok())
            .ok_or(ParseError {
                message: "a LIMIT or OFFSET must be a row count".to_owned(),
                position: at,
            })
    }
}

/// How the engine compares a column's values against Postgres.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Comparable {
    /// Equal and ordered as Postgres has them.
    Fully,
    /// Equal as Postgres has them; ordered otherwise.
    Equality,
    /// Neither: Postgres's equality differs (case, padding, its own rules).
    Not,
}

/// How a column of `column`'s type compares in the engine, against how
/// Postgres compares it.
fn comparable(column: &DbColumn) -> Comparable {
    match &column.r#type {
        ValueType::Int
        | ValueType::Float
        | ValueType::Bool
        | ValueType::Numeric
        | ValueType::Timestamp
        | ValueType::Day
        | ValueType::Time
        | ValueType::Bytes
        | ValueType::Date
        | ValueType::Datetime => Comparable::Fully,
        ValueType::Json => Comparable::Equality,
        ValueType::String => match column.type_oid {
            oid::UUID => Comparable::Fully,
            oid::BPCHAR => Comparable::Not,
            oid::TEXT | oid::VARCHAR | oid::NAME | oid::CHAR | 0 => match &column.collation {
                Some(collation) if !collation.deterministic => Comparable::Not,
                Some(collation) if collation.orders_bytewise() => Comparable::Fully,
                _ => Comparable::Equality,
            },
            _ if column.collation.is_some() => Comparable::Not,
            _ => Comparable::Equality,
        },
        ValueType::List(_) | ValueType::Map(_, _) | ValueType::Raw => Comparable::Not,
    }
}

/// Refuses a filter or order the engine would answer otherwise than
/// Postgres: a comparison on a column whose equality differs from
/// Postgres's (`citext`, `char(n)`, a nondeterministic collation, a type
/// the engine does not interpret), a range or an `ORDER BY` on a column
/// it orders otherwise (text under a collation that does not order by
/// bytes, an enum, JSON), and a value of another type than the column's.
fn check_exact(
    table: &DbTable,
    filter: &Where,
    order: &[OrderBy],
    at: usize,
) -> Result<(), ParseError> {
    let refuse = |message: String| {
        Err(ParseError {
            message,
            position: at,
        })
    };
    let column_of = |name: &ColumnName| {
        table.column(name.as_str()).ok_or(ParseError {
            message: format!("unknown column `{name}`"),
            position: at,
        })
    };
    let mut conditions = Vec::new();
    collect_conditions(filter, &mut conditions);
    for condition in conditions {
        use ComparisonOperator::*;
        let column = column_of(&condition.column)?;
        let ranged = matches!(condition.comparison_operator, GT | GTE | LT | LTE);
        match (condition.comparison_operator, comparable(column)) {
            (IS | IS_NOT, _) if condition.value.is_null() => continue,
            (_, Comparable::Not) => {
                return refuse(format!(
                    "`{}` is compared by rules of Postgres's own the engine does not follow",
                    column.name
                ));
            }
            (_, Comparable::Equality) if ranged => {
                return refuse(format!(
                    "`{}` is ordered by Postgres in a way the engine does not follow",
                    column.name
                ));
            }
            _ => {}
        }
        let values: Vec<&Value> = match &condition.value {
            Value::List(items) => items.iter().collect(),
            other => vec![other],
        };
        for value in values {
            if !fits(value, &column.r#type) {
                return refuse(format!(
                    "`{}` is compared with a value of another type: {value:?}",
                    column.name
                ));
            }
        }
    }
    for clause in order {
        let column = column_of(&clause.column)?;
        if comparable(column) != Comparable::Fully {
            return refuse(format!(
                "ORDER BY `{}`: Postgres orders it in a way the engine does not follow",
                column.name
            ));
        }
    }
    Ok(())
}

/// Every condition of a filter tree.
fn collect_conditions<'a>(filter: &'a Where, out: &mut Vec<&'a Condition>) {
    match filter {
        Where::Condition(condition) => out.push(condition),
        Where::AND(parts) | Where::OR(parts) => {
            for part in parts {
                collect_conditions(part, out);
            }
        }
    }
}

/// Whether `value` is of the type a column of `declared` holds, so that
/// the engine compares the two as Postgres does.
fn fits(value: &Value, declared: &ValueType) -> bool {
    matches!(
        (value, declared),
        (Value::Null, _)
            | (
                Value::Int(_) | Value::Numeric(_),
                ValueType::Int | ValueType::Numeric
            )
            | (Value::Float(_), ValueType::Float)
            | (Value::Bool(_), ValueType::Bool)
            | (Value::String(_), ValueType::String)
            | (Value::Json(_), ValueType::Json)
            | (Value::Timestamp(_), ValueType::Timestamp)
            | (Value::Day(_), ValueType::Day)
            | (Value::Time(_), ValueType::Time)
            | (Value::Bytes(_), ValueType::Bytes)
            | (Value::Date(_), ValueType::Date)
            | (Value::Datetime(_), ValueType::Datetime)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Collation, Numeric, Timestamp};

    fn catalog() -> Catalog {
        let c = Collation {
            name: "C".to_owned(),
            deterministic: true,
        };
        let english = Collation {
            name: "en_US.UTF-8".to_owned(),
            deterministic: true,
        };
        Catalog::new(vec![
            DbTable::new(
                "orders",
                ["id"],
                vec![
                    DbColumn::new("id", ValueType::Int).with_type_oid(oid::INT8),
                    DbColumn::new("merchant_id", ValueType::String)
                        .with_type_oid(oid::TEXT)
                        .with_collation(english),
                    DbColumn::new("code", ValueType::String)
                        .with_type_oid(oid::TEXT)
                        .with_collation(c.clone()),
                    DbColumn::new("amount", ValueType::Numeric).with_type_oid(oid::NUMERIC),
                    DbColumn::new("created_at", ValueType::Timestamp)
                        .with_type_oid(oid::TIMESTAMPTZ),
                    DbColumn::new("status", ValueType::String).with_type_oid(99_001),
                    DbColumn::new("email", ValueType::String)
                        .with_type_oid(99_002)
                        .with_collation(c.clone()),
                    DbColumn::new("country", ValueType::String)
                        .with_type_oid(oid::BPCHAR)
                        .with_collation(c),
                    DbColumn::new("span", ValueType::Raw).with_type_oid(1186),
                ],
            ),
            DbTable::new(
                "billing.invoices",
                ["id"],
                vec![DbColumn::new("id", ValueType::Int).with_type_oid(oid::INT8)],
            ),
        ])
    }

    fn text(value: &str) -> Bind {
        Bind {
            type_oid: oid::TEXT,
            bytes: Some(value.as_bytes().to_vec()),
        }
    }

    fn int8(value: i64) -> Bind {
        Bind {
            type_oid: oid::INT8,
            bytes: Some(value.to_be_bytes().to_vec()),
        }
    }

    fn parse(sql: &str, binds: &[Bind]) -> Result<Select, ParseError> {
        parse_select(sql, binds, &catalog())
    }

    #[test]
    fn reads_a_diesel_select_with_binds() {
        let select = parse(
            r#"SELECT "orders"."id", "orders"."amount" FROM "orders" WHERE (("orders"."merchant_id" = $1) AND ("orders"."id" > $2)) ORDER BY "orders"."id" DESC LIMIT $3 OFFSET $4"#,
            &[text("m1"), int8(10), int8(20), int8(5)],
        )
        .unwrap();
        assert_eq!(
            select.projection,
            Projection::Columns(vec![ColumnName::from("id"), ColumnName::from("amount")])
        );
        assert_eq!(select.limit, 20);
        assert_eq!(select.offset, 5);
        assert_eq!(select.query.limit, 25);
        assert_eq!(select.query.order_by, vec![OrderBy::new("id", Order::DESC)]);
        let unbounded = parse(
            r#"SELECT "orders"."id" FROM "orders" LIMIT $1 OFFSET $2"#,
            &[
                Bind {
                    type_oid: oid::INT8,
                    bytes: None,
                },
                Bind {
                    type_oid: oid::INT8,
                    bytes: None,
                },
            ],
        )
        .unwrap();
        assert_eq!(
            (unbounded.limit, unbounded.offset),
            (u32::MAX, 0),
            "NULL is no limit, no offset"
        );
        assert_eq!(
            select.query.filter,
            Where::AND(vec![
                Where::condition("merchant_id", ComparisonOperator::EQ, "m1"),
                Where::condition("id", ComparisonOperator::GT, 10),
            ])
        );
    }

    #[test]
    fn reads_any_all_between_and_counts() {
        let ids = Bind {
            type_oid: 1016,
            bytes: Some(
                crate::sync::pg::binary::encode_array(&[Value::Int(1), Value::Int(2)], oid::INT8)
                    .unwrap(),
            ),
        };
        let select = parse(
            r#"SELECT COUNT(*) FROM "orders" WHERE ("orders"."id" = ANY($1)) AND "orders"."id" != ALL($2) AND "orders"."amount" BETWEEN $3 AND 100.50"#,
            &[
                ids.clone(),
                ids,
                Bind {
                    type_oid: oid::NUMERIC,
                    bytes: Some(Numeric::parse("1.5").unwrap().to_binary()),
                },
            ],
        )
        .unwrap();
        assert_eq!(select.projection, Projection::CountStar);
        assert_eq!(select.query.limit, u32::MAX);
        let list = Value::List(vec![Value::Int(1), Value::Int(2)]);
        assert_eq!(
            select.query.filter,
            Where::AND(vec![
                Where::condition("id", ComparisonOperator::IN, list.clone()),
                Where::condition("id", ComparisonOperator::NOT_IN, list),
                Where::AND(vec![
                    Where::condition(
                        "amount",
                        ComparisonOperator::GTE,
                        Value::Numeric(Numeric::parse("1.5").unwrap())
                    ),
                    Where::condition(
                        "amount",
                        ComparisonOperator::LTE,
                        Value::Numeric(Numeric::parse("100.50").unwrap())
                    ),
                ]),
            ])
        );
    }

    #[test]
    fn reads_qualified_names_and_whole_rows() {
        let select = parse(
            r#"SELECT "billing"."invoices".* FROM "billing"."invoices""#,
            &[],
        )
        .unwrap();
        assert_eq!(select.projection, Projection::All);
        assert_eq!(select.query.table.as_str(), "billing.invoices");
        assert!(
            parse(
                r#"SELECT * FROM "public"."orders" WHERE "orders"."id" = 1"#,
                &[]
            )
            .is_ok()
        );
        assert!(parse(r#"SELECT "other"."id" FROM "orders""#, &[]).is_err());
    }

    #[test]
    fn binds_take_the_column_type() {
        let at = Timestamp::from_unix_millis(1_000).unwrap();
        let select = parse(
            r#"SELECT * FROM "orders" WHERE "orders"."created_at" >= $1 AND "orders"."status" = $2"#,
            &[
                Bind {
                    type_oid: oid::TIMESTAMPTZ,
                    bytes: Some(at.0.to_be_bytes().to_vec()),
                },
                Bind {
                    type_oid: 99_001,
                    bytes: Some(b"paid".to_vec()),
                },
            ],
        )
        .unwrap();
        assert_eq!(
            select.query.filter,
            Where::AND(vec![
                Where::condition("created_at", ComparisonOperator::GTE, Value::Timestamp(at)),
                Where::condition("status", ComparisonOperator::EQ, "paid"),
            ])
        );
    }

    #[test]
    fn refuses_what_postgres_would_answer_otherwise() {
        for (sql, why) in [
            (
                r#"SELECT * FROM "orders" WHERE "orders"."merchant_id" > 'm'"#,
                "ordered",
            ),
            (
                r#"SELECT * FROM "orders" ORDER BY "orders"."merchant_id""#,
                "ORDER BY",
            ),
            (
                r#"SELECT * FROM "orders" ORDER BY "orders"."status""#,
                "ORDER BY",
            ),
            (
                r#"SELECT * FROM "orders" WHERE "orders"."email" = 'a@b'"#,
                "rules",
            ),
            (
                r#"SELECT * FROM "orders" WHERE "orders"."country" = 'IN'"#,
                "rules",
            ),
            (
                r#"SELECT * FROM "orders" WHERE "orders"."span" = 'x'"#,
                "rules",
            ),
            (
                r#"SELECT * FROM "orders" WHERE "orders"."id" = 'one'"#,
                "another type",
            ),
            (
                r#"SELECT * FROM "orders" ORDER BY "orders"."id" NULLS FIRST"#,
                "NULLS",
            ),
            (r#"SELECT DISTINCT "orders"."id" FROM "orders""#, "DISTINCT"),
            (
                r#"SELECT lower("orders"."code") FROM "orders""#,
                "projection",
            ),
            (r#"SELECT * FROM "orders" FOR UPDATE"#, "locking"),
            (
                r#"SELECT * FROM "orders" INNER JOIN "billing"."invoices" ON TRUE"#,
                "joins",
            ),
            (
                r#"SELECT * FROM "orders" WHERE "orders"."id" = $1"#,
                "no value",
            ),
        ] {
            let error = parse(sql, &[]).expect_err(sql);
            assert!(error.message.contains(why), "{sql}: {}", error.message);
        }
        assert!(
            parse(
                r#"SELECT * FROM "orders" WHERE "orders"."code" > 'a' ORDER BY "orders"."code""#,
                &[]
            )
            .is_ok()
        );
        assert!(
            parse(
                r#"SELECT * FROM "orders" WHERE "orders"."merchant_id" IN ('a', 'b')"#,
                &[]
            )
            .is_ok()
        );
        assert!(
            parse(
                r#"SELECT * FROM "orders" WHERE "orders"."email" IS NULL"#,
                &[]
            )
            .is_ok()
        );
    }
}
