//! The wrapper is transparent: every way Diesel reads and writes through a
//! `PgConnection` behaves the same through `SyncedConnection<PgConnection>`.
//!
//! Needs a Postgres at `DATABASE_URL` (default: the local socket database
//! `diesel_streamgres_test`). Every test runs inside a test transaction, so
//! nothing it writes outlives it.

use diesel::connection::{Instrumentation, InstrumentationEvent, SimpleConnection};
use diesel::pg::{PgMetadataLookup, PgRowByRowLoadingMode};
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::sql_query;
use diesel::sql_types::{Integer, Text};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use diesel_streamgres::SyncedConnection;

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

#[derive(Queryable, Selectable, Debug, PartialEq, Clone)]
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

#[derive(Insertable)]
#[diesel(table_name = sg_items)]
struct NewItem<'a> {
    name: &'a str,
    qty: i32,
    note: Option<&'a str>,
    tags: Vec<&'a str>,
    payload: serde_json::Value,
}

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS sg_items (
        id serial PRIMARY KEY,
        name text NOT NULL,
        qty int NOT NULL,
        note text,
        tags text[] NOT NULL DEFAULT '{}',
        payload jsonb NOT NULL DEFAULT '{}',
        created timestamptz NOT NULL DEFAULT now()
    );
";

fn url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres:///diesel_streamgres_test?host=/tmp".to_string())
}

/// Creates the table once per process: the tests run in parallel, and
/// concurrent `CREATE TABLE IF NOT EXISTS` on a missing table races in
/// Postgres.
fn ensure_schema() {
    static SCHEMA_READY: std::sync::Once = std::sync::Once::new();
    SCHEMA_READY.call_once(|| {
        let mut conn = PgConnection::establish(&url()).expect("connect");
        conn.batch_execute(SCHEMA).expect("schema");
    });
}

/// A wrapped connection inside a test transaction with the table present.
fn conn() -> SyncedConnection<PgConnection> {
    ensure_schema();
    let mut conn = SyncedConnection::<PgConnection>::establish(&url()).expect("connect");
    conn.begin_test_transaction().expect("test transaction");
    conn
}

fn seed(conn: &mut SyncedConnection<PgConnection>) -> Vec<Item> {
    let rows = vec![
        NewItem {
            name: "bolt",
            qty: 3,
            note: Some("m6"),
            tags: vec!["metal", "small"],
            payload: serde_json::json!({"len_mm": 20, "zinc": true}),
        },
        NewItem {
            name: "washer",
            qty: 0,
            note: None,
            tags: vec![],
            payload: serde_json::json!([1, 2.5, "x", null]),
        },
        NewItem {
            name: "nut",
            qty: 7,
            note: Some("m6"),
            tags: vec!["metal"],
            payload: serde_json::json!({}),
        },
    ];
    diesel::insert_into(sg_items::table)
        .values(&rows)
        .get_results::<Item>(conn)
        .expect("seed")
}

#[test]
fn loads_rows_exactly_like_the_inner_connection() {
    let mut conn = conn();
    let inserted = seed(&mut conn);

    let through_wrapper = sg_items::table
        .filter(sg_items::note.eq("m6"))
        .order(sg_items::id)
        .select(Item::as_select())
        .load(&mut conn)
        .expect("wrapped load");
    let through_inner = sg_items::table
        .filter(sg_items::note.eq("m6"))
        .order(sg_items::id)
        .select(Item::as_select())
        .load(conn.inner())
        .expect("inner load");

    assert_eq!(through_wrapper, through_inner);
    assert_eq!(through_wrapper.len(), 2);
    assert_eq!(through_wrapper[0], inserted[0]);
    assert_eq!(through_wrapper[1], inserted[2]);
    assert_eq!(through_wrapper[0].tags, vec!["metal", "small"]);
    assert_eq!(through_wrapper[0].payload["len_mm"], 20);
}

#[test]
fn first_and_count_go_through() {
    let mut conn = conn();
    seed(&mut conn);

    let count: i64 = sg_items::table.count().get_result(&mut conn).expect("count");
    assert_eq!(count, 3);

    let washer: Item = sg_items::table
        .filter(sg_items::name.eq("washer"))
        .select(Item::as_select())
        .first(&mut conn)
        .expect("first");
    assert_eq!(washer.note, None);
    assert_eq!(washer.tags, Vec::<String>::new());

    let missing = sg_items::table
        .filter(sg_items::name.eq("gear"))
        .select(Item::as_select())
        .first(&mut conn)
        .optional()
        .expect("optional");
    assert_eq!(missing, None);
}

#[test]
fn writes_return_counts_and_returning_rows() {
    let mut conn = conn();
    seed(&mut conn);

    let updated = diesel::update(sg_items::table.filter(sg_items::note.eq("m6")))
        .set(sg_items::qty.eq(sg_items::qty + 1))
        .execute(&mut conn)
        .expect("update");
    assert_eq!(updated, 2);

    let renamed: Item = diesel::update(sg_items::table.filter(sg_items::name.eq("washer")))
        .set(sg_items::name.eq("spring washer"))
        .get_result(&mut conn)
        .expect("update returning");
    assert_eq!(renamed.name, "spring washer");

    let deleted = diesel::delete(sg_items::table.filter(sg_items::qty.gt(5)))
        .execute(&mut conn)
        .expect("delete");
    assert_eq!(deleted, 1, "only the nut had qty > 5 after the bump");

    let left: Vec<String> = sg_items::table
        .order(sg_items::id)
        .select(sg_items::name)
        .load(&mut conn)
        .expect("load");
    assert_eq!(left, vec!["bolt", "spring washer"]);
}

#[test]
fn transactions_roll_back_and_nest_through_the_wrapper() {
    let mut conn = conn();
    seed(&mut conn);

    let failed: Result<(), diesel::result::Error> = conn.transaction(|c| {
        diesel::delete(sg_items::table).execute(c)?;
        Err(diesel::result::Error::RollbackTransaction)
    });
    assert!(matches!(
        failed,
        Err(diesel::result::Error::RollbackTransaction)
    ));
    let count: i64 = sg_items::table.count().get_result(&mut conn).expect("count");
    assert_eq!(count, 3, "the inner transaction's delete was rolled back");

    // A nested savepoint that succeeds inside one that fails.
    let outer: Result<(), diesel::result::Error> = conn.transaction(|c| {
        c.transaction(|c| {
            diesel::insert_into(sg_items::table)
                .values(NewItem {
                    name: "gear",
                    qty: 1,
                    note: None,
                    tags: vec![],
                    payload: serde_json::json!(null),
                })
                .execute(c)?;
            Ok::<_, diesel::result::Error>(())
        })?;
        let n: i64 = sg_items::table.count().get_result(c)?;
        assert_eq!(n, 4);
        Err(diesel::result::Error::RollbackTransaction)
    });
    assert!(outer.is_err());
    let count: i64 = sg_items::table.count().get_result(&mut conn).expect("count");
    assert_eq!(count, 3, "the outer rollback also undid the nested insert");
}

#[derive(QueryableByName, Debug, PartialEq)]
struct NameQty {
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Integer)]
    qty: i32,
}

#[test]
fn raw_sql_queries_by_name_go_through() {
    let mut conn = conn();
    seed(&mut conn);

    let rows = sql_query("SELECT name, qty FROM sg_items WHERE qty >= $1 ORDER BY qty")
        .bind::<Integer, _>(3)
        .load::<NameQty>(&mut conn)
        .expect("sql_query");
    assert_eq!(
        rows,
        vec![
            NameQty { name: "bolt".into(), qty: 3 },
            NameQty { name: "nut".into(), qty: 7 },
        ]
    );
}

#[test]
fn row_by_row_loading_mode_goes_through() {
    let mut conn = conn();
    seed(&mut conn);

    let names: Vec<String> = sg_items::table
        .order(sg_items::id)
        .select(sg_items::name)
        .load_iter::<String, PgRowByRowLoadingMode>(&mut conn)
        .expect("load_iter")
        .collect::<QueryResult<_>>()
        .expect("rows");
    assert_eq!(names, vec!["bolt", "washer", "nut"]);
}

#[test]
fn a_pool_hands_out_wrapped_connections() {
    let pool: Pool<ConnectionManager<SyncedConnection<PgConnection>>> = Pool::builder()
        .max_size(2)
        .build(ConnectionManager::new(url()))
        .expect("pool");

    let mut a = pool.get().expect("conn a");
    let mut b = pool.get().expect("conn b");
    let one: i32 = diesel::select(diesel::dsl::sql::<Integer>("1"))
        .get_result(&mut a)
        .expect("select 1");
    let two: i32 = diesel::select(diesel::dsl::sql::<Integer>("2"))
        .get_result(&mut b)
        .expect("select 2");
    assert_eq!((one, two), (1, 2));
    assert!(pool.get_timeout(std::time::Duration::from_millis(50)).is_err());
    drop(a);
    assert!(pool.get_timeout(std::time::Duration::from_millis(500)).is_ok());
}

struct CountQueries(Arc<AtomicUsize>);

impl Instrumentation for CountQueries {
    fn on_connection_event(&mut self, event: InstrumentationEvent<'_>) {
        if let InstrumentationEvent::StartQuery { .. } = event {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[test]
fn instrumentation_reaches_the_inner_connection() {
    let mut conn = conn();
    let started = Arc::new(AtomicUsize::new(0));
    conn.set_instrumentation(CountQueries(started.clone()));

    seed(&mut conn);
    let _: i64 = sg_items::table.count().get_result(&mut conn).expect("count");

    assert_eq!(started.load(Ordering::SeqCst), 2, "one insert, one count");
}

#[test]
fn the_wrapper_resolves_type_metadata_like_the_inner_connection() {
    let mut conn = conn();
    let through_wrapper = PgMetadataLookup::lookup_type(&mut conn, "int4", None);
    let through_inner = PgMetadataLookup::lookup_type(conn.inner(), "int4", None);
    assert_eq!(through_wrapper.oid().ok(), through_inner.oid().ok());
    assert_eq!(through_wrapper.oid().ok(), Some(23));
    assert_eq!(through_wrapper.array_oid().ok(), Some(1007));
}
