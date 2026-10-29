//! Column types of tables outside `public`, under the engine's names for
//! them (`s.t`): through a blocking connection's lookup per table, and
//! through the async connection's lookup of every table at once.
//!
//! Needs a Postgres at `DATABASE_URL` (default: the local socket's
//! `diesel_streamgres_test`).

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_query;
use diesel_streamgres::types::{ALL_TABLES_SQL, TableAttribute, TypeLookup, all_table_types};

/// The test database's URL.
fn url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres:///diesel_streamgres_test?host=/tmp".to_string())
}

/// A schema of its own with one table, and a table of the same name in
/// `public` with other columns; set up once per process, as `IF NOT
/// EXISTS` is not safe against a concurrent create.
fn connection() -> PgConnection {
    static SETUP: std::sync::Once = std::sync::Once::new();
    let mut conn = PgConnection::establish(&url()).expect("connect");
    SETUP.call_once(|| {
        conn.batch_execute(
            "CREATE SCHEMA IF NOT EXISTS sg_types_ws;
             CREATE TABLE IF NOT EXISTS sg_types_ws.sg_types_items (id bigint PRIMARY KEY, label text, doc json);
             CREATE TABLE IF NOT EXISTS public.sg_types_items (id int PRIMARY KEY);",
        )
        .expect("schema");
    });
    conn
}

/// The names of a table's columns, in order.
fn names(types: &diesel_streamgres::types::TableTypes) -> Vec<&str> {
    types.columns.iter().map(|column| column.name.as_str()).collect()
}

/// A blocking connection finds `s.t` by its schema, and a bare name in
/// `public` as before.
#[test]
fn per_table_lookup_reads_schema_qualified_tables() {
    let mut conn = connection();
    let qualified = conn
        .table_types("sg_types_ws.sg_types_items")
        .expect("schema-qualified table");
    assert_eq!(names(&qualified), ["id", "label", "doc"]);
    let bare = conn.table_types("sg_types_items").expect("public table");
    assert_eq!(names(&bare), ["id"]);
}

/// The all-tables lookup lists `s.t` under that name beside the bare
/// names of the search path.
#[test]
fn all_tables_lookup_lists_schema_qualified_tables() {
    let mut conn = connection();
    let rows = sql_query(ALL_TABLES_SQL)
        .load::<TableAttribute>(&mut conn)
        .expect("all tables");
    let tables = all_table_types(rows);
    assert_eq!(
        names(tables.get("sg_types_ws.sg_types_items").expect("schema-qualified table")),
        ["id", "label", "doc"]
    );
    assert_eq!(names(tables.get("sg_types_items").expect("public table")), ["id"]);
}
