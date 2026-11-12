//! Who is reading, as far as it changes an answer.
//!
//! The engine reads every table as its own role, with each table at the
//! name the catalog gives it. A reader gets the same rows from Postgres
//! only when its connection resolves the table's name to that same table
//! and may read the whole of it (tables with row-level security are never
//! cached at all). [`ReaderCheck`] is the one query that finds, for a
//! reader's connection, the tables it would see otherwise; the [`Reader`]
//! built from its answer goes with every read, and the cache refuses a
//! read of one of those tables.
//!
//! The settings a session can change do not change an answer the engine
//! gives: values travel in binary format and every accepted comparison is
//! typed by its column, so the time zone, the date style and the float
//! digits play no part. Custom settings matter only through row-level
//! security or functions, both of which the engine refuses.

use std::collections::HashSet;
use std::sync::Arc;

use crate::model::{Catalog, TableName};
use crate::sync::pg::sql::{quote_ident, quote_table};

/// The query that checks a reader's connection, with its parameters.
///
/// Run it on the reader's connection with the three arrays as its `$1`,
/// `$2` and `$3`, both columns read as text, and hand the row to
/// [`Reader::new`] along with [`ReaderCheck::version`].
#[derive(Debug, Clone)]
pub struct ReaderCheck {
    pub sql: &'static str,
    /// Each table's catalog name.
    pub names: Vec<String>,
    /// Each table as a statement names it: bare in `public`.
    pub as_written: Vec<String>,
    /// Each table by its schema and name.
    pub qualified: Vec<String>,
    /// The catalog the check was made against.
    pub version: CatalogVersion,
}

/// Identifies one catalog the engine served by: a reader checked against
/// an older one must be checked again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogVersion(usize);

impl CatalogVersion {
    pub(super) fn of(catalog: &Arc<Catalog>) -> Self {
        CatalogVersion(Arc::as_ptr(catalog) as usize)
    }
}

/// The SQL of [`ReaderCheck`]: the reader's database, and the tables
/// whose name resolves elsewhere for it or that it may not read whole.
const CHECK_SQL: &str = "SELECT current_database()::text AS database,
       COALESCE(array_to_string(array_agg(t.name) FILTER (WHERE
           to_regclass(t.as_written) IS DISTINCT FROM to_regclass(t.qualified)
           OR NOT COALESCE(has_table_privilege(to_regclass(t.qualified), 'SELECT'), false)),
         chr(31)), '') AS excluded
FROM unnest($1::text[], $2::text[], $3::text[]) AS t(name, as_written, qualified)";

/// The separator of the excluded tables' names in the check's answer: a
/// control character no table name holds.
const SEPARATOR: char = '\u{1f}';

impl ReaderCheck {
    pub(super) fn of(catalog: &Arc<Catalog>) -> Self {
        let mut names = Vec::new();
        let mut as_written = Vec::new();
        let mut qualified = Vec::new();
        for table in catalog.tables() {
            let name = table.name.as_str();
            let full = match name.split_once('.') {
                Some(_) => quote_table(name),
                None => format!("{}.{}", quote_ident("public"), quote_ident(name)),
            };
            names.push(name.to_owned());
            as_written.push(quote_table(name));
            qualified.push(full);
        }
        ReaderCheck {
            sql: CHECK_SQL,
            names,
            as_written,
            qualified,
            version: CatalogVersion::of(catalog),
        }
    }
}

/// A reader's connection as checked: its database and the tables it
/// would read otherwise than the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reader {
    database: String,
    excluded: HashSet<TableName>,
    version: CatalogVersion,
}

impl Reader {
    /// The reader [`ReaderCheck`]'s row describes: its `database` and
    /// `excluded` columns, checked against catalog `version`.
    pub fn new(database: &str, excluded: &str, version: CatalogVersion) -> Self {
        Reader {
            database: database.to_owned(),
            excluded: excluded
                .split(SEPARATOR)
                .filter(|name| !name.is_empty())
                .map(TableName::from)
                .collect(),
            version,
        }
    }

    /// Whether this reader was checked against `version`.
    pub fn is_current(&self, version: CatalogVersion) -> bool {
        self.version == version
    }

    /// Why this reader cannot be answered for `table` in `database`, if
    /// it cannot.
    pub(super) fn refuses(&self, database: &str, table: &TableName) -> Option<String> {
        if self.database != database {
            return Some(format!(
                "the reader is on database `{}`, the engine on `{database}`",
                self.database
            ));
        }
        self.excluded.contains(table).then(|| {
            format!("`{table}` names another table for this reader, or one it may not read")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DbColumn, DbTable, ValueType};

    #[test]
    fn a_check_names_each_table_as_statements_do() {
        let catalog = Arc::new(Catalog::new(vec![
            DbTable::new("orders", ["id"], vec![DbColumn::new("id", ValueType::Int)]),
            DbTable::new(
                "billing.invoices",
                ["id"],
                vec![DbColumn::new("id", ValueType::Int)],
            ),
        ]));
        let check = ReaderCheck::of(&catalog);
        let at = check
            .names
            .iter()
            .position(|name| name == "orders")
            .unwrap();
        assert_eq!(check.as_written[at], "\"orders\"");
        assert_eq!(check.qualified[at], "\"public\".\"orders\"");
        let at = check
            .names
            .iter()
            .position(|name| name == "billing.invoices")
            .unwrap();
        assert_eq!(check.as_written[at], "\"billing\".\"invoices\"");
        assert_eq!(check.qualified[at], "\"billing\".\"invoices\"");
    }

    #[test]
    fn a_reader_is_refused_its_excluded_tables_and_other_databases() {
        let catalog = Arc::new(Catalog::new(Vec::new()));
        let version = CatalogVersion::of(&catalog);
        let reader = Reader::new(
            "app",
            &format!("orders{SEPARATOR}billing.invoices"),
            version,
        );
        assert!(reader.refuses("app", &TableName::from("orders")).is_some());
        assert!(
            reader
                .refuses("app", &TableName::from("billing.invoices"))
                .is_some()
        );
        assert!(reader.refuses("app", &TableName::from("users")).is_none());
        assert!(reader.refuses("other", &TableName::from("users")).is_some());
        assert!(
            Reader::new("app", "", version)
                .refuses("app", &TableName::from("orders"))
                .is_none()
        );
        assert!(reader.is_current(version));
    }
}
