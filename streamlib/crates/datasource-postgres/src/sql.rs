//! Turns a query into SQL that compares, sorts and handles nulls the way the engine does.

use streamlib_datasource::{
    Dir, Filter, Op, Pred, Query, Result, SourceError, Table, Value, ValueType,
};

pub fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote `schema.table` as `"schema"."table"`.
pub fn table_ident(name: &str) -> String {
    match name.split_once('.') {
        Some((schema, table)) => format!("{}.{}", ident(schema), ident(table)),
        None => ident(name),
    }
}

fn literal(text: &str) -> Result<String> {
    if text.contains('\0') {
        return Err(SourceError::Refused("NUL in a text value".into()));
    }
    Ok(format!("'{}'", text.replace('\'', "''")))
}

/// A column expression that compares the way the engine does.
fn expr(table: &Table, column: &str) -> Result<(String, ValueType)> {
    let col = table
        .column(column)
        .ok_or_else(|| SourceError::Refused(format!("no column `{column}` in `{}`", table.name)))?;
    let e = match col.ty {
        ValueType::Text => format!("({}::text COLLATE \"C\")", ident(column)),
        _ => ident(column),
    };
    Ok((e, col.ty))
}

fn value(v: &Value) -> Result<String> {
    Ok(match v {
        Value::Null => "NULL".into(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) if f.is_nan() => "'NaN'::float8".into(),
        Value::Float(f) if f.is_infinite() => {
            if *f > 0.0 {
                "'Infinity'::float8".into()
            } else {
                "'-Infinity'::float8".into()
            }
        }
        Value::Float(f) => format!("'{f:?}'::float8"),
        Value::Text(t) => literal(t)?,
    })
}

fn same_type(ty: ValueType, v: &Value) -> bool {
    matches!(
        (ty, v),
        (ValueType::Bool, Value::Bool(_))
            | (ValueType::Int, Value::Int(_))
            | (ValueType::Float, Value::Float(_))
            | (ValueType::Text, Value::Text(_))
    )
}

fn pred(table: &Table, p: &Pred) -> Result<String> {
    let (e, ty) = expr(table, p.column())?;
    Ok(match p {
        Pred::IsNull(_) => format!("{e} IS NULL"),
        Pred::IsNotNull(_) => format!("{e} IS NOT NULL"),
        Pred::Cmp { value: v, .. } if !same_type(ty, v) => "FALSE".into(),
        Pred::Cmp { op, value: v, .. } => {
            let op = match op {
                Op::Eq => "=",
                Op::Ne => "<>",
                Op::Lt => "<",
                Op::Le => "<=",
                Op::Gt => ">",
                Op::Ge => ">=",
            };
            format!("{e} {op} {}", value(v)?)
        }
    })
}

fn filter(table: &Table, f: &Filter) -> Result<String> {
    if f.0.is_empty() {
        return Ok("FALSE".into());
    }
    let ors =
        f.0.iter()
            .map(|conj| {
                if conj.is_empty() {
                    return Ok("TRUE".to_owned());
                }
                let ands = conj
                    .iter()
                    .map(|p| pred(table, p))
                    .collect::<Result<Vec<_>>>()?;
                Ok(format!("({})", ands.join(" AND ")))
            })
            .collect::<Result<Vec<_>>>()?;
    Ok(ors.join(" OR "))
}

/// Build a `SELECT` of every column, in catalog order.
pub fn select(table: &Table, q: &Query) -> Result<String> {
    let cols: Vec<_> = table.columns.iter().map(|c| ident(&c.name)).collect();
    let mut sql = format!(
        "SELECT {} FROM {} WHERE {}",
        cols.join(", "),
        table_ident(&table.name),
        filter(table, &q.filter)?
    );
    if !q.order.is_empty() {
        let keys = q
            .order
            .iter()
            .map(|(c, dir)| {
                let (e, _) = expr(table, c)?;
                Ok(match dir {
                    Dir::Asc => format!("{e} ASC NULLS FIRST"),
                    Dir::Desc => format!("{e} DESC NULLS LAST"),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        sql.push_str(&format!(" ORDER BY {}", keys.join(", ")));
    }
    // Postgres limits are bigint; anything larger means no limit.
    if let Some(n) = q.limit.filter(|n| i64::try_from(*n).is_ok()) {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;
    use streamlib_datasource::Column;

    fn table() -> Table {
        let col = |n: &str, ty| Column { name: n.into(), ty };
        Table {
            name: "public.items".into(),
            columns: vec![col("id", ValueType::Int), col("body", ValueType::Text)],
            key: vec!["id".into()],
        }
    }

    #[test]
    fn renders_engine_semantics() {
        let q = Query::table("public.items")
            .filter(Filter::and(vec![
                Pred::cmp("body", Op::Eq, "it's"),
                Pred::cmp("id", Op::Gt, 1),
            ]))
            .order("body", Dir::Asc)
            .limit(5);
        assert_eq!(
            select(&table(), &q).unwrap(),
            "SELECT \"id\", \"body\" FROM \"public\".\"items\" WHERE \
             ((\"body\"::text COLLATE \"C\") = 'it''s' AND \"id\" > 1) \
             ORDER BY (\"body\"::text COLLATE \"C\") ASC NULLS FIRST LIMIT 5"
        );
    }

    #[test]
    fn mismatched_type_or_null_is_false() {
        let q = Query::table("public.items").filter(Filter::and(vec![
            Pred::cmp("id", Op::Eq, "1"),
            Pred::cmp("id", Op::Eq, Value::Null),
        ]));
        assert!(
            select(&table(), &q)
                .unwrap()
                .ends_with("WHERE (FALSE AND FALSE)")
        );
    }

    #[test]
    fn huge_limit_means_no_limit() {
        let q = Query::table("public.items").limit(usize::MAX);
        assert!(!select(&table(), &q).unwrap().contains("LIMIT"));
    }

    #[test]
    fn refuses_unknown_column() {
        let q = Query::table("public.items").order("nope", Dir::Asc);
        assert!(matches!(select(&table(), &q), Err(SourceError::Refused(_))));
    }
}
