//! A captured row decodes exactly like the live row it was copied from.

use diesel::connection::{DefaultLoadingMode, LoadConnection, SimpleConnection};
use diesel::pg::Pg;
use diesel::row::Row;
use diesel::deserialize::{FromSqlRow, QueryableByName};
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{Integer, Nullable, Text};
use streamgres_diesel::SyncedConnection;
use streamgres_diesel::row::{OwnedCursor, OwnedRow, capture_all};

diesel::table! {
    sg_items (id) {
        id -> Int4,
        name -> Text,
        qty -> Int4,
        note -> Nullable<Text>,
        tags -> Array<Text>,
        payload -> Jsonb,
        created -> Timestamptz,
    }
}

#[derive(Queryable, Selectable, Insertable, Debug, PartialEq, Clone)]
#[diesel(table_name = sg_items)]
struct Item {
    id: i32,
    name: String,
    qty: i32,
    note: Option<String>,
    tags: Vec<String>,
    payload: serde_json::Value,
    created: chrono::DateTime<chrono::Utc>,
}

fn url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres:///streamgres_diesel_test?host=/tmp".to_string())
}

fn conn() -> SyncedConnection<PgConnection> {
    static SCHEMA_READY: std::sync::Once = std::sync::Once::new();
    SCHEMA_READY.call_once(|| {
        let mut conn = PgConnection::establish(&url()).expect("connect");
        conn.batch_execute(
            "CREATE TABLE IF NOT EXISTS sg_items (
                id serial PRIMARY KEY, name text NOT NULL, qty int NOT NULL, note text,
                tags text[] NOT NULL DEFAULT '{}', payload jsonb NOT NULL DEFAULT '{}',
                created timestamptz NOT NULL DEFAULT now())",
        )
        .expect("schema");
    });
    let mut conn = SyncedConnection::<PgConnection>::establish(&url()).expect("connect");
    conn.begin_test_transaction().expect("test transaction");
    conn
}

fn seed(conn: &mut SyncedConnection<PgConnection>) -> Vec<Item> {
    conn.batch_execute(
        "INSERT INTO sg_items (name, qty, note, tags, payload, created) VALUES
         ('bolt', 3, 'm6', '{metal,small}', '{\"len_mm\": 20, \"zinc\": true}', '2026-10-13 11:22:33.123456+05:30'),
         ('washer', 0, NULL, '{}', '[1, 2.5, \"x\", null]', '1999-12-31 23:59:59Z'),
         ('nut', -7, '', '{\"a b\",\"c,d\",\"\"}', '{}', '2026-01-01 00:00:00Z')",
    )
    .expect("seed");
    sg_items::table
        .order(sg_items::id)
        .select(Item::as_select())
        .load(conn)
        .expect("live rows")
}

/// Loads the whole table as owned rows through Diesel's own cursor.
fn captured(conn: &mut SyncedConnection<PgConnection>) -> Vec<OwnedRow> {
    let cursor = <SyncedConnection<PgConnection> as LoadConnection<DefaultLoadingMode>>::load(
        conn,
        sg_items::table.order(sg_items::id),
    )
    .expect("load");
    capture_all(cursor).expect("capture")
}

#[test]
fn a_captured_row_decodes_like_the_live_one() {
    let mut conn = conn();
    let live = seed(&mut conn);
    let rows = captured(&mut conn);
    assert_eq!(rows.len(), 3);

    let decoded: Vec<Item> = rows
        .iter()
        .map(|row| <Item as FromSqlRow<sg_items::SqlType, Pg>>::build_from_row(row).expect("decode"))
        .collect();
    assert_eq!(decoded, live);
    assert_eq!(decoded[1].note, None);
    assert_eq!(decoded[2].tags, vec!["a b", "c,d", ""]);
    assert_eq!(decoded[0].created.timestamp_subsec_micros(), 123456);
}

#[test]
fn rows_of_one_result_share_their_column_names() {
    let mut conn = conn();
    seed(&mut conn);
    let rows = captured(&mut conn);
    assert!(std::sync::Arc::ptr_eq(rows[0].columns(), rows[2].columns()));
    assert_eq!(
        rows[0].columns().iter().map(String::as_str).collect::<Vec<_>>(),
        ["id", "name", "qty", "note", "tags", "payload", "created"]
    );
    assert!(rows[1].value(3).is_none(), "the washer's note is NULL");
    assert!(rows[0].byte_len() > 0);
}

#[derive(QueryableByName, Debug, PartialEq)]
struct NameNote {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Nullable<Text>)]
    note: Option<String>,
    #[diesel(sql_type = Integer)]
    qty: i32,
}

#[test]
fn a_captured_row_decodes_by_column_name() {
    let mut conn = conn();
    seed(&mut conn);
    let cursor = <SyncedConnection<PgConnection> as LoadConnection<DefaultLoadingMode>>::load(
        &mut conn,
        sql_query("SELECT qty, note, name FROM sg_items ORDER BY id"),
    )
    .expect("load");
    let rows = capture_all(cursor).expect("capture");
    let decoded: Vec<NameNote> = rows
        .iter()
        .map(|row| NameNote::build(row).expect("by name"))
        .collect();
    assert_eq!(
        decoded,
        vec![
            NameNote { name: "bolt".into(), note: Some("m6".into()), qty: 3 },
            NameNote { name: "washer".into(), note: None, qty: 0 },
            NameNote { name: "nut".into(), note: Some("".into()), qty: -7 },
        ]
    );
}

#[test]
fn an_owned_cursor_yields_the_rows_in_order() {
    let mut conn = conn();
    seed(&mut conn);
    let rows = captured(&mut conn);
    let cursor = OwnedCursor::new(rows.clone().into());
    assert_eq!(cursor.len(), 3);
    let back: Vec<OwnedRow> = cursor.map(|row| row.expect("row")).collect();
    assert_eq!(back, rows);
}

#[test]
fn nested_tuples_read_partial_rows() {
    let mut conn = conn();
    seed(&mut conn);
    let rows = captured(&mut conn);
    type Nested = (i32, (String, i32), Option<String>);
    type NestedSql = (
        diesel::sql_types::Int4,
        (Text, Integer),
        Nullable<Text>,
    );
    let first: Nested = {
        let row = rows[0].partial_row(0..4);
        <Nested as FromSqlRow<NestedSql, Pg>>::build_from_row(&row).expect("nested")
    };
    assert_eq!(first.1, ("bolt".to_string(), 3));
    assert_eq!(first.2, Some("m6".to_string()));
}
