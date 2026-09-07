//! Table schema: the catalog, tables, and columns.
//!
//! The schema has one authoritative home — the [`Catalog`] — and resolves by
//! name at every level: the catalog maps table names to [`DbTable`]s, and
//! each table maps column names to [`DbColumn`]s. Queries reference their
//! table **by name**; nothing else carries schema copies, so a migration
//! cannot leave two halves of the engine disagreeing about what a table
//! looks like.

use std::collections::HashMap;

use super::value::ValueType;

/// The name of a table, as its own type so it can never be confused with
/// the other strings the engine passes around (subscription ids, column
/// names).
///
/// Constructed from any string-ish value; compares, orders, and hashes
/// exactly like the underlying name, maps keyed by `TableName` accept a
/// plain `&str` for lookups, and it compares directly against string
/// literals.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableName(String);

impl TableName {
    /// The name as a borrowed string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for TableName {
    /// Wraps a borrowed name.
    fn from(name: &str) -> Self {
        TableName(name.to_owned())
    }
}

impl From<String> for TableName {
    /// Wraps an owned name.
    fn from(name: String) -> Self {
        TableName(name)
    }
}

impl std::borrow::Borrow<str> for TableName {
    /// Lets maps keyed by [`TableName`] be queried with a plain `&str`.
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for TableName {
    /// Compares against a bare string name.
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for TableName {
    /// Compares against a bare string name.
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl std::fmt::Display for TableName {
    /// Renders as the bare name, honoring width/alignment format flags.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(&self.0)
    }
}

/// The name of a column, as its own type so it can never be confused with
/// the other strings the engine passes around (table names, subscription
/// ids).
///
/// Constructed from any string-ish value; compares, orders, and hashes
/// exactly like the underlying name, maps keyed by `ColumnName` accept a
/// plain `&str` for lookups, and it compares directly against string
/// literals.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnName(String);

impl ColumnName {
    /// The name as a borrowed string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ColumnName {
    /// Wraps a borrowed name.
    fn from(name: &str) -> Self {
        ColumnName(name.to_owned())
    }
}

impl From<String> for ColumnName {
    /// Wraps an owned name.
    fn from(name: String) -> Self {
        ColumnName(name)
    }
}

impl std::borrow::Borrow<str> for ColumnName {
    /// Lets maps keyed by [`ColumnName`] be queried with a plain `&str`.
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for ColumnName {
    /// Compares against a bare string name.
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for ColumnName {
    /// Compares against a bare string name.
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl std::fmt::Display for ColumnName {
    /// Renders as the bare name, honoring width/alignment format flags.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(&self.0)
    }
}

/// A column: the table it belongs to, its name, and its static type.
///
/// `table` is stamped by [`DbTable::new`] when the column joins a table —
/// build columns with [`DbColumn::new`] (unqualified) and let the table
/// qualify them, so a column and its owning table can never disagree.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DbColumn {
    pub table: TableName,
    pub name: ColumnName,
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
    pub name: TableName,
    pub pkey: Vec<ColumnName>,
    pub columns: HashMap<ColumnName, DbColumn>,
}

/// The authoritative set of table schemas, by name.
///
/// The parser resolves and validates statements against it; anything else
/// needing schema (initial result sets, type checks) looks tables up here
/// rather than carrying copies.
#[derive(Debug, Clone, Default)]
pub struct Catalog {
    tables: HashMap<TableName, DbTable>,
}

impl Catalog {
    /// Build a catalog from table definitions, indexed by table name.
    pub fn new(tables: impl IntoIterator<Item = DbTable>) -> Self {
        Catalog {
            tables: tables
                .into_iter()
                .map(|table| (table.name.clone(), table))
                .collect(),
        }
    }

    /// The schema of table `name`, if the catalog contains it.
    pub fn table(&self, name: &str) -> Option<&DbTable> {
        self.tables.get(name)
    }
}

impl DbColumn {
    /// An unqualified column definition; [`DbTable::new`] fills in `table`.
    pub fn new(name: impl Into<ColumnName>, r#type: ValueType) -> Self {
        DbColumn {
            table: TableName(String::new()),
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
        name: impl Into<TableName>,
        pkey: impl IntoIterator<Item = impl Into<ColumnName>>,
        columns: Vec<DbColumn>,
    ) -> Self {
        let name = name.into();
        let pkey: Vec<ColumnName> = pkey.into_iter().map(Into::into).collect();

        let mut by_name: HashMap<ColumnName, DbColumn> = HashMap::with_capacity(columns.len());
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
        self.pkey.iter().filter_map(|key| self.column(key.as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [`DbTable::new`] stamps the table name onto every column and resolves
    /// columns and pkey membership by name.
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

    /// Declaring the same column name twice panics in [`DbTable::new`].
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

    /// A pkey entry naming an undeclared column panics in [`DbTable::new`].
    #[test]
    #[should_panic(expected = "not in the column list")]
    fn pkey_must_reference_a_declared_column() {
        DbTable::new("t", ["ghost"], vec![DbColumn::new("a", ValueType::Int)]);
    }
}
