//! Table schema: the catalog, tables, columns, and a single materialized row.
//!
//! The schema has one authoritative home — the [`Catalog`] — and resolves by
//! name at every level: the catalog maps table names to [`DbTable`]s, and
//! each table maps column names to [`DbColumn`]s. Queries and records
//! reference their table **by name**; nothing else carries schema copies, so
//! a migration cannot leave two halves of the engine disagreeing about what
//! a table looks like.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use super::value::{unordered_map_hash, Value, ValueType};

/// A column: the table it belongs to, its name, and its static type.
///
/// `table` is stamped by [`DbTable::new`] when the column joins a table —
/// build columns with [`DbColumn::new`] (unqualified) and let the table
/// qualify them, so a column and its owning table can never disagree.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DbColumn {
    pub table: String,
    pub name: String,
    pub r#type: ValueType,
}

/// A table: its name, its columns resolvable by name, and the names of the
/// columns forming the primary key (in declaration order — the first pkey
/// entry drives the default `ORDER BY`).
///
/// `pkey` entries reference `columns` by name — the constructor enforces
/// that, along with uniqueness, so a `DbTable` built through [`DbTable::new`]
/// is always internally consistent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbTable {
    pub name: String,
    pub pkey: Vec<String>,
    pub columns: HashMap<String, DbColumn>,
}

/// A single full row of a table: primary-key values plus the remaining data.
#[derive(Debug, Clone, PartialEq)]
pub struct DbRecord {
    pub table: String,
    pub pkey_value: HashMap<String, Value>,
    pub data: HashMap<String, Value>,
}

impl Eq for DbRecord {}

impl Hash for DbRecord {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table.hash(state);
        unordered_map_hash(&self.pkey_value).hash(state);
        unordered_map_hash(&self.data).hash(state);
    }
}

/// The authoritative set of table schemas, by name.
///
/// The parser resolves and validates statements against it; anything else
/// needing schema (initial result sets, type checks) looks tables up here
/// rather than carrying copies.
#[derive(Debug, Clone, Default)]
pub struct Catalog {
    tables: HashMap<String, DbTable>,
}

impl Catalog {
    pub fn new(tables: impl IntoIterator<Item = DbTable>) -> Self {
        Catalog {
            tables: tables
                .into_iter()
                .map(|table| (table.name.clone(), table))
                .collect(),
        }
    }

    pub fn table(&self, name: &str) -> Option<&DbTable> {
        self.tables.get(name)
    }
}

impl DbColumn {
    /// An unqualified column definition; [`DbTable::new`] fills in `table`.
    pub fn new(name: impl Into<String>, r#type: ValueType) -> Self {
        DbColumn {
            table: String::new(),
            name: name.into(),
            r#type,
        }
    }
}

impl DbTable {
    /// Build a table, enforcing the schema invariants.
    ///
    /// # Panics
    ///
    /// On a duplicate column name, a duplicate pkey name, or a pkey name
    /// that is not in `columns` — a broken schema definition is a
    /// programming error, not a runtime condition.
    pub fn new(
        name: impl Into<String>,
        pkey: impl IntoIterator<Item = impl Into<String>>,
        columns: Vec<DbColumn>,
    ) -> Self {
        let name = name.into();
        let pkey: Vec<String> = pkey.into_iter().map(Into::into).collect();

        let mut by_name: HashMap<String, DbColumn> = HashMap::with_capacity(columns.len());
        for mut column in columns {
            column.table = name.clone();
            let column_name = column.name.clone();
            assert!(
                by_name.insert(column_name.clone(), column).is_none(),
                "table `{name}`: column `{column_name}` declared twice"
            );
        }
        for (index, key) in pkey.iter().enumerate() {
            assert!(
                by_name.contains_key(key),
                "table `{name}`: pkey column `{key}` is not in the column list"
            );
            assert!(
                !pkey[..index].contains(key),
                "table `{name}`: pkey column `{key}` declared twice"
            );
        }

        DbTable {
            name,
            pkey,
            columns: by_name,
        }
    }

    /// The definition of column `name`, if the table declares it.
    pub fn column(&self, name: &str) -> Option<&DbColumn> {
        self.columns.get(name)
    }

    /// Is `name` one of the primary-key columns?
    pub fn is_pkey(&self, name: &str) -> bool {
        self.pkey.iter().any(|key| key == name)
    }

    /// The primary-key columns' definitions, in declaration order.
    pub fn pkey_columns(&self) -> impl Iterator<Item = &DbColumn> {
        self.pkey.iter().filter_map(|key| self.column(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stamps_table_onto_columns_and_resolves_by_name() {
        let table = DbTable::new(
            "tickets",
            ["id"],
            vec![
                DbColumn::new("id", ValueType::Int),
                DbColumn::new("status", ValueType::String),
            ],
        );
        let status = table.column("status").expect("declared");
        assert_eq!(status.table, "tickets");
        assert_eq!(status.r#type, ValueType::String);
        assert!(table.is_pkey("id"));
        assert!(!table.is_pkey("status"));
        assert!(table.column("ghost").is_none());
    }

    #[test]
    #[should_panic(expected = "declared twice")]
    fn duplicate_column_names_are_rejected() {
        DbTable::new(
            "t",
            ["a"],
            vec![
                DbColumn::new("a", ValueType::Int),
                DbColumn::new("a", ValueType::String),
            ],
        );
    }

    #[test]
    #[should_panic(expected = "not in the column list")]
    fn pkey_must_reference_a_declared_column() {
        DbTable::new("t", ["ghost"], vec![DbColumn::new("a", ValueType::Int)]);
    }
}
