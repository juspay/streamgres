use std::cmp::Ordering;

use crate::catalog::{Key, Row, Table};
use crate::value::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Pred {
    Cmp {
        column: String,
        op: Op,
        value: Value,
    },
    IsNull(String),
    IsNotNull(String),
}

impl Pred {
    pub fn cmp(column: &str, op: Op, value: impl Into<Value>) -> Self {
        Pred::Cmp {
            column: column.to_owned(),
            op,
            value: value.into(),
        }
    }

    pub fn column(&self) -> &str {
        match self {
            Pred::Cmp { column, .. } | Pred::IsNull(column) | Pred::IsNotNull(column) => column,
        }
    }

    /// A missing column gives Unknown; a null never matches a comparison.
    pub fn eval(&self, row: &Row) -> Truth {
        let Some(cell) = row.get(self.column()) else {
            return Truth::Unknown;
        };
        let hit = match self {
            Pred::IsNull(_) => cell.is_null(),
            Pred::IsNotNull(_) => !cell.is_null(),
            Pred::Cmp { op, value, .. } => cell.compare(value).is_some_and(|o| match op {
                Op::Eq => o == Ordering::Equal,
                Op::Ne => o != Ordering::Equal,
                Op::Lt => o == Ordering::Less,
                Op::Le => o != Ordering::Greater,
                Op::Gt => o == Ordering::Greater,
                Op::Ge => o != Ordering::Less,
            }),
        };
        Truth::from(hit)
    }
}

/// Whether a row matches; Unknown means the row lacks a column the filter needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Truth {
    True,
    False,
    Unknown,
}

impl From<bool> for Truth {
    fn from(b: bool) -> Self {
        if b { Truth::True } else { Truth::False }
    }
}

impl Truth {
    fn and(self, other: Truth) -> Truth {
        match (self, other) {
            (Truth::False, _) | (_, Truth::False) => Truth::False,
            (Truth::True, Truth::True) => Truth::True,
            _ => Truth::Unknown,
        }
    }

    fn or(self, other: Truth) -> Truth {
        match (self, other) {
            (Truth::True, _) | (_, Truth::True) => Truth::True,
            (Truth::False, Truth::False) => Truth::False,
            _ => Truth::Unknown,
        }
    }
}

/// A filter written as OR of AND groups; a single empty group matches everything.
#[derive(Clone, Debug, PartialEq)]
pub struct Filter(pub Vec<Vec<Pred>>);

impl Filter {
    pub fn all() -> Self {
        Filter(vec![vec![]])
    }

    pub fn and(preds: Vec<Pred>) -> Self {
        Filter(vec![preds])
    }

    pub fn eval(&self, row: &Row) -> Truth {
        self.0.iter().fold(Truth::False, |any, conj| {
            any.or(conj.iter().fold(Truth::True, |all, p| all.and(p.eval(row))))
        })
    }

    pub fn columns(&self) -> impl Iterator<Item = &str> {
        self.0.iter().flatten().map(Pred::column)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dir {
    Asc,
    Desc,
}

/// A read of one table; with a limit, rows follow `order` using the engine's value order.
#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub table: String,
    pub filter: Filter,
    pub order: Vec<(String, Dir)>,
    pub limit: Option<usize>,
}

impl Query {
    pub fn table(name: &str) -> Self {
        Query {
            table: name.to_owned(),
            filter: Filter::all(),
            order: vec![],
            limit: None,
        }
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = filter;
        self
    }

    pub fn order(mut self, column: &str, dir: Dir) -> Self {
        self.order.push((column.to_owned(), dir));
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// One row by primary key.
    pub fn key(table: &Table, key: &Key) -> Self {
        let preds = table
            .key
            .iter()
            .zip(key)
            .map(|(c, v)| Pred::Cmp {
                column: c.clone(),
                op: Op::Eq,
                value: v.clone(),
            })
            .collect();
        Query::table(&table.name).filter(Filter::and(preds))
    }

    /// Sort rows by this query's order, using the engine's rules.
    pub fn sort(&self, rows: &mut [Row]) {
        rows.sort_by(|a, b| {
            for (column, dir) in &self.order {
                let o = a.get(column).cmp(&b.get(column));
                let o = if *dir == Dir::Desc { o.reverse() } else { o };
                if o != Ordering::Equal {
                    return o;
                }
            }
            Ordering::Equal
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: &[(&str, Value)]) -> Row {
        pairs
            .iter()
            .map(|(c, v)| (c.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn absent_column_is_unknown() {
        let f = Filter::and(vec![Pred::cmp("body", Op::Eq, "x")]);
        assert_eq!(f.eval(&row(&[("id", 1.into())])), Truth::Unknown);
    }

    #[test]
    fn false_beats_unknown_in_and() {
        let f = Filter::and(vec![
            Pred::cmp("id", Op::Eq, 2),
            Pred::cmp("body", Op::Eq, "x"),
        ]);
        assert_eq!(f.eval(&row(&[("id", 1.into())])), Truth::False);
    }

    #[test]
    fn true_beats_unknown_in_or() {
        let f = Filter(vec![
            vec![Pred::cmp("body", Op::Eq, "x")],
            vec![Pred::cmp("id", Op::Eq, 1)],
        ]);
        assert_eq!(f.eval(&row(&[("id", 1.into())])), Truth::True);
    }

    #[test]
    fn null_matches_no_comparison() {
        let r = row(&[("v", Value::Null)]);
        assert_eq!(Pred::cmp("v", Op::Ne, 1).eval(&r), Truth::False);
        assert_eq!(Pred::IsNull("v".into()).eval(&r), Truth::True);
    }
}
