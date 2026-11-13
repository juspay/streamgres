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
/// `$2` and `$3`, its three columns read as text, and hand the row to
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
/// an older one must be checked again. It holds the catalog, so no later
/// catalog can take its place in memory and pass for it.
#[derive(Debug, Clone)]
pub struct CatalogVersion(Arc<Catalog>);

impl CatalogVersion {
    pub(super) fn of(catalog: &Arc<Catalog>) -> Self {
        CatalogVersion(catalog.clone())
    }
}

impl PartialEq for CatalogVersion {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for CatalogVersion {}

impl std::hash::Hash for CatalogVersion {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

/// The SQL of [`ReaderCheck`]: the reader's cluster and database, and the
/// tables whose name resolves elsewhere for it or that it may not read
/// whole. Qualify the validation functions, types and equality operator:
/// the caller's search path must affect table resolution, never the check.
const CHECK_SQL: &str =
    "SELECT (SELECT system_identifier::pg_catalog.text FROM pg_catalog.pg_control_system()) AS cluster,
       pg_catalog.current_database()::pg_catalog.text AS database,
       COALESCE(pg_catalog.json_agg(t.name) FILTER (WHERE
           (pg_catalog.to_regclass(t.as_written) OPERATOR(pg_catalog.=) pg_catalog.to_regclass(t.qualified)) IS NOT TRUE
           OR NOT COALESCE(pg_catalog.has_table_privilege(pg_catalog.to_regclass(t.qualified), 'SELECT'), false)), '[]'::pg_catalog.json)::pg_catalog.text AS excluded
FROM ROWS FROM (
    pg_catalog.unnest($1::pg_catalog.text[]),
    pg_catalog.unnest($2::pg_catalog.text[]),
    pg_catalog.unnest($3::pg_catalog.text[])
) AS t(name, as_written, qualified)";

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

/// A reader's connection as checked: its cluster, its database and the
/// tables it would read otherwise than the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reader {
    cluster: String,
    database: String,
    excluded: Option<HashSet<TableName>>,
    version: CatalogVersion,
}

impl Reader {
    /// The reader [`ReaderCheck`]'s row describes: its `cluster`,
    /// `database` and `excluded` columns, checked against catalog
    /// `version`. `excluded` must be the JSON array of table names returned
    /// by the check. Invalid input refuses every table, never grants access.
    pub fn new(cluster: &str, database: &str, excluded: &str, version: CatalogVersion) -> Self {
        Reader {
            cluster: cluster.to_owned(),
            database: database.to_owned(),
            excluded: serde_json::from_str::<Vec<String>>(excluded)
                .ok()
                .map(|names| names.into_iter().map(TableName::from).collect()),
            version,
        }
    }

    /// Whether this reader was checked against `version`.
    pub fn is_current(&self, version: &CatalogVersion) -> bool {
        self.version == *version
    }

    /// Why this reader cannot be answered at all by an engine on
    /// `database` of `cluster`, if it cannot: it is connected elsewhere.
    /// A physical standby shares its primary's cluster.
    pub(super) fn elsewhere(&self, cluster: &str, database: &str) -> Option<String> {
        if self.cluster != cluster {
            return Some("the reader is on another database cluster than the engine".to_owned());
        }
        if self.database != database {
            return Some(format!(
                "the reader is on database `{}`, the engine on `{database}`",
                self.database
            ));
        }
        None
    }

    /// Why this reader cannot be answered for `table`, if it cannot.
    pub(super) fn refuses(&self, table: &TableName) -> Option<String> {
        match &self.excluded {
            None => Some("the reader check returned an invalid table exclusion list".to_owned()),
            Some(excluded) => excluded.contains(table).then(|| {
                format!("`{table}` names another table for this reader, or one it may not read")
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DbColumn, DbTable, ValueType};

    #[test]
    fn exclusions_preserve_every_character_and_fail_closed() {
        let version = CatalogVersion::of(&Arc::new(Catalog::new(Vec::new())));
        let name = "secret\u{1f}rows\"\\tail";
        let encoded = serde_json::to_string(&vec![name]).unwrap();
        let reader = Reader::new("1", "app", &encoded, version.clone());
        assert!(reader.refuses(&TableName::from(name)).is_some());
        assert!(reader.refuses(&TableName::from("other")).is_none());
        for invalid in ["", "null", "{}", "[null]", "secret\u{1f}rows"] {
            let reader = Reader::new("1", "app", invalid, version.clone());
            assert!(reader.refuses(&TableName::from("any_table")).is_some());
        }
    }

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
            "7",
            "app",
            r#"["orders", "billing.invoices"]"#,
            version.clone(),
        );
        assert!(reader.refuses(&TableName::from("orders")).is_some());
        assert!(
            reader
                .refuses(&TableName::from("billing.invoices"))
                .is_some()
        );
        assert!(reader.refuses(&TableName::from("users")).is_none());
        assert!(reader.elsewhere("7", "app").is_none());
        assert!(reader.elsewhere("7", "other").is_some());
        assert!(
            reader.elsewhere("8", "app").is_some(),
            "the same name on another cluster"
        );
        assert!(
            Reader::new("7", "app", "[]", version.clone())
                .refuses(&TableName::from("orders"))
                .is_none()
        );
        assert!(reader.is_current(&version));
    }
}
