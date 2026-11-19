use std::collections::BTreeMap;
use std::sync::Arc;

use crate::value::{Value, ValueType};

/// A row as column name to value; a partial row is missing some columns.
pub type Row = BTreeMap<String, Value>;

/// Primary key values, in the table's key column order.
pub type Key = Vec<Value>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: ValueType,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    /// Primary key column names, never empty.
    pub key: Vec<String>,
}

impl Table {
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// The row's key, if every key column is present.
    pub fn key_of(&self, row: &Row) -> Option<Key> {
        self.key.iter().map(|c| row.get(c).cloned()).collect()
    }

    /// True when the row has exactly the table's columns.
    pub fn is_complete(&self, row: &Row) -> bool {
        row.len() == self.columns.len() && self.columns.iter().all(|c| row.contains_key(&c.name))
    }

    /// Declared columns the row lacks.
    pub fn missing(&self, row: &Row) -> Vec<String> {
        self.columns
            .iter()
            .filter(|c| !row.contains_key(&c.name))
            .map(|c| c.name.clone())
            .collect()
    }

    /// Columns the row has that the table doesn't declare.
    pub fn undeclared<'a>(&self, row: &'a Row) -> Vec<&'a str> {
        row.keys()
            .filter(|c| self.column(c).is_none())
            .map(String::as_str)
            .collect()
    }
}

/// The tables a source serves.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Catalog {
    tables: BTreeMap<String, Arc<Table>>,
}

impl Catalog {
    pub fn new(tables: impl IntoIterator<Item = Table>) -> Self {
        Catalog {
            tables: tables
                .into_iter()
                .map(|t| (t.name.clone(), Arc::new(t)))
                .collect(),
        }
    }

    pub fn table(&self, name: &str) -> Option<&Arc<Table>> {
        self.tables.get(name)
    }

    pub fn tables(&self) -> impl Iterator<Item = &Arc<Table>> {
        self.tables.values()
    }
}

/// Fill a partial row's missing columns from an earlier copy of the row.
pub fn complete(row: &Row, earlier: &Row) -> Row {
    let mut full = earlier.clone();
    full.extend(row.iter().map(|(c, v)| (c.clone(), v.clone())));
    full
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> Table {
        Table {
            name: "t".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    ty: ValueType::Int,
                },
                Column {
                    name: "body".into(),
                    ty: ValueType::Text,
                },
            ],
            key: vec!["id".into()],
        }
    }

    #[test]
    fn partial_row_is_not_complete() {
        let row = Row::from([("id".into(), Value::Int(1))]);
        assert!(!table().is_complete(&row));
        assert_eq!(table().missing(&row), vec!["body".to_owned()]);
        assert_eq!(table().key_of(&row), Some(vec![Value::Int(1)]));
    }

    #[test]
    fn complete_keeps_new_values() {
        let old = Row::from([("id".into(), 1.into()), ("body".into(), "old".into())]);
        let new = Row::from([("id".into(), 1.into())]);
        assert_eq!(complete(&new, &old)["body"], Value::from("old"));
    }
}
