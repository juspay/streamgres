//! The async wrapper against a running engine: rows mode end to end,
//! including the one-shot load of every table's types.
//!
//! Needs `STREAMGRES_S2S_URL` and `DATABASE_URL`; skips itself otherwise.

#![cfg(feature = "async")]

mod transport;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use diesel_streamgres::cache::Cache;
use diesel_streamgres::{AsyncSyncedConnection, Route};

/// diesel-async's `RunQueryDsl::load` shadows `AtomicU64::load`.
fn stat(counter: &AtomicU64) -> u64 {
    AtomicU64::load(counter, Ordering::Relaxed)
}

diesel::table! {
    sg_configs (id) {
        id -> Int4,
        key -> Text,
        value -> Text,
    }
}

/// Sets `key` to `value` on a plain connection, as another service
/// would.
async fn put(other: &mut AsyncPgConnection, key: &str, value: &str) {
    diesel::insert_into(sg_configs::table)
        .values((sg_configs::key.eq(key), sg_configs::value.eq(value)))
        .on_conflict(sg_configs::key)
        .do_update()
        .set(sg_configs::value.eq(value))
        .execute(other)
        .await
        .expect("put");
}

/// `key`'s value, through the wrapper.
async fn read(conn: &mut AsyncSyncedConnection, key: &str) -> Option<String> {
    sg_configs::table
        .filter(sg_configs::key.eq(key))
        .select(sg_configs::value)
        .first::<String>(conn)
        .await
        .optional()
        .expect("read")
}

/// Rows mode through the async wrapper against a live engine: the first
/// read is served from the engine's rows and the second from the cache.
///
/// A change arrives as a delta and replaces the entry; no read.
///
/// A second query on the same table: the types are known, no catalog load,
/// still rows mode.
#[tokio::test]
async fn rows_mode_through_the_async_wrapper() {
    let Some(engine) = transport::client(diesel_streamgres::engine::Config::from_env()) else {
        return;
    };
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let cache = Arc::new(Cache::new(64 * 1024 * 1024, Arc::new(engine.clone())));
    engine.attach(&cache);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !engine.is_connected() {
        assert!(Instant::now() < deadline, "the engine did not connect");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut other = AsyncPgConnection::establish(&database)
        .await
        .expect("connect");
    other
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS sg_configs (
                id serial PRIMARY KEY, key text NOT NULL UNIQUE, value text NOT NULL)",
        )
        .await
        .expect("schema");
    let adopting = engine.clone();
    let url = database.clone();
    tokio::task::spawn_blocking(move || {
        let mut conn = diesel::PgConnection::establish(&url).expect("connect");
        transport::adopted(&adopting, &mut conn, sg_configs::table.select(sg_configs::id));
    })
    .await
    .expect("the engine takes the table in");
    let prefix = format!("async-live-{}-", std::process::id());
    let key = format!("{prefix}r");
    put(&mut other, &key, "1").await;

    let inner = AsyncPgConnection::establish(&database)
        .await
        .expect("connect");
    let mut conn = AsyncSyncedConnection::with_cache(inner, cache.clone(), Route::Streamgres);
    let one: i32 = diesel_async::RunQueryDsl::get_result(
        diesel::select(diesel::dsl::sql::<diesel::sql_types::Integer>("1")),
        &mut conn,
    )
    .await
    .expect("a first read, which reads what the connection reads as");
    assert_eq!(one, 1);
    assert_eq!(read(&mut conn, &key).await.as_deref(), Some("1"));
    assert_eq!(
        engine.materialized(),
        1,
        "served from the engine's rows: {:?}",
        engine.fallbacks()
    );
    assert_eq!(engine.subscribed(), 1);
    let hits = stat(&cache.stats.hits);
    assert_eq!(read(&mut conn, &key).await.as_deref(), Some("1"));
    assert_eq!(stat(&cache.stats.hits), hits + 1);

    put(&mut other, &key, "2").await;
    let started = Instant::now();
    loop {
        if read(&mut conn, &key).await.as_deref() == Some("2") {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the change did not arrive"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    eprintln!(
        "ASYNC rows mode commit to replaced entry: {:?}",
        started.elapsed()
    );
    assert!(stat(&cache.stats.replaced) >= 1);
    assert_eq!(stat(&cache.stats.touched), 0);

    let key2 = format!("{prefix}r2");
    diesel::insert_into(sg_configs::table)
        .values((sg_configs::key.eq(&key2), sg_configs::value.eq("x")))
        .execute(&mut other)
        .await
        .unwrap();
    let got: Option<String> = sg_configs::table
        .filter(sg_configs::key.eq(&key2))
        .select(sg_configs::value)
        .first(&mut conn)
        .await
        .optional()
        .unwrap();
    assert_eq!(got.as_deref(), Some("x"));
    assert_eq!(engine.materialized(), 2, "{:?}", engine.fallbacks());

    let _ = diesel::delete(sg_configs::table.filter(sg_configs::key.like(format!("{prefix}%"))))
        .execute(&mut other)
        .await;
}

/// Counts the query starts and finishes a connection's instrumentation
/// hears.
struct Count(Arc<(AtomicU64, AtomicU64)>);

impl diesel::connection::Instrumentation for Count {
    fn on_connection_event(&mut self, event: diesel::connection::InstrumentationEvent<'_>) {
        match event {
            diesel::connection::InstrumentationEvent::StartQuery { .. } => {
                self.0.0.fetch_add(1, Ordering::SeqCst);
            }
            diesel::connection::InstrumentationEvent::FinishQuery { .. } => {
                self.0.1.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}

/// Every query the instrumentation hears start, it hears finish, however
/// the read went: a miss the engine serves from its own rows (whose
/// futures for Postgres were built and never run), a hit, a read the
/// engine refuses and Postgres answers, and a miss dropped while it
/// subscribes. A miss the engine serves tells it of no read of the row.
#[tokio::test]
async fn the_instrumentation_hears_every_query_it_starts_finish() {
    let Some(engine) = transport::client(diesel_streamgres::engine::Config::from_env()) else {
        return;
    };
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let cache = Arc::new(Cache::new(64 * 1024 * 1024, Arc::new(engine.clone())));
    engine.attach(&cache);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !engine.is_connected() {
        assert!(Instant::now() < deadline, "the engine did not connect");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut other = AsyncPgConnection::establish(&database).await.expect("connect");
    other
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS sg_configs (
                id serial PRIMARY KEY, key text NOT NULL UNIQUE, value text NOT NULL)",
        )
        .await
        .expect("schema");
    let adopting = engine.clone();
    let url = database.clone();
    tokio::task::spawn_blocking(move || {
        let mut conn = diesel::PgConnection::establish(&url).expect("connect");
        transport::adopted(&adopting, &mut conn, sg_configs::table.select(sg_configs::id));
    })
    .await
    .expect("the engine takes the table in");
    let prefix = format!("async-instrumented-{}-", std::process::id());
    for name in ["a", "b", "c"] {
        put(&mut other, &format!("{prefix}{name}"), name).await;
    }

    let counts = Arc::new((AtomicU64::new(0), AtomicU64::new(0)));
    let inner = AsyncPgConnection::establish(&database).await.expect("connect");
    let mut conn = AsyncSyncedConnection::with_cache(inner, cache.clone(), Route::Streamgres);
    conn.set_instrumentation(Count(Arc::clone(&counts)));
    let heard = || (stat(&counts.0), stat(&counts.1));
    let balanced = |what: &str| {
        let (starts, finishes) = heard();
        assert_eq!(starts, finishes, "{what}: {starts} started, {finishes} finished");
        starts
    };

    let one: i32 = diesel_async::RunQueryDsl::get_result(
        diesel::select(diesel::dsl::sql::<diesel::sql_types::Integer>("1")),
        &mut conn,
    )
    .await
    .expect("a first read, which reads what the connection reads as");
    assert_eq!(one, 1);
    balanced("the first read, from Postgres");

    let before = balanced("before the miss");
    assert_eq!(read(&mut conn, &format!("{prefix}a")).await.as_deref(), Some("a"));
    assert_eq!(
        conn.last_served(),
        Some(diesel_streamgres::Served::Engine),
        "{:?}",
        engine.fallbacks()
    );
    let after = balanced("a miss the engine served");
    assert!(
        after - before <= 1,
        "at most the one-time lookup of every table's types ran, not the read itself: {}",
        after - before
    );

    assert_eq!(read(&mut conn, &format!("{prefix}a")).await.as_deref(), Some("a"));
    assert_eq!(conn.last_served(), Some(diesel_streamgres::Served::Cache));
    assert_eq!(balanced("a hit"), after, "a hit runs nothing");

    let refused: Vec<String> = sg_configs::table
        .filter(sg_configs::key.eq(format!("{prefix}b")))
        .filter(diesel::dsl::sql::<diesel::sql_types::Bool>("length(value) > 0"))
        .select(sg_configs::value)
        .load(&mut conn)
        .await
        .expect("a read the engine refuses");
    assert_eq!(refused, ["b"]);
    assert_eq!(conn.last_served(), Some(diesel_streamgres::Served::Postgres));
    assert!(balanced("a refused read") > after, "Postgres answered it");

    let dropped = futures_util::FutureExt::now_or_never(read(&mut conn, &format!("{prefix}c")));
    assert!(dropped.is_none(), "dropped while it subscribes");
    balanced("a miss dropped while it subscribes");
    assert_eq!(read(&mut conn, &format!("{prefix}c")).await.as_deref(), Some("c"));
    balanced("the same miss read again");
}

/// A read the engine refuses waits for a fallback permit before it asks
/// Postgres. Turned away for want of one, or cancelled while it waits,
/// it started no query, and the instrumentation hears of none; cancelled
/// in the middle of its query, it hears the query finish with an error.
/// Every start has its finish.
#[tokio::test]
async fn a_read_turned_away_or_cancelled_finishes_what_it_started() {
    let Some(engine) = transport::client(diesel_streamgres::engine::Config::from_env()) else {
        return;
    };
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let limits = diesel_streamgres::cache::Limits {
        fallback_max_concurrent: 1,
        fallback_wait: Duration::from_millis(50),
        ..Default::default()
    };
    let cache = Arc::new(Cache::with_limits(limits, Arc::new(engine.clone())));
    engine.attach(&cache);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !engine.is_connected() {
        assert!(Instant::now() < deadline, "the engine did not connect");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut other = AsyncPgConnection::establish(&database).await.expect("connect");
    other
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS sg_configs (
                id serial PRIMARY KEY, key text NOT NULL UNIQUE, value text NOT NULL)",
        )
        .await
        .expect("schema");
    let prefix = format!("async-admission-{}-", std::process::id());
    put(&mut other, &format!("{prefix}a"), "a").await;

    let connect = async || {
        let inner = AsyncPgConnection::establish(&database).await.expect("connect");
        let mut conn = AsyncSyncedConnection::with_cache(inner, cache.clone(), Route::Streamgres);
        let counts = Arc::new((AtomicU64::new(0), AtomicU64::new(0)));
        conn.set_instrumentation(Count(Arc::clone(&counts)));
        let one: i32 = diesel_async::RunQueryDsl::get_result(
            diesel::select(diesel::dsl::sql::<diesel::sql_types::Integer>("1")),
            &mut conn,
        )
        .await
        .expect("a first read, which reads what the connection reads as");
        assert_eq!(one, 1);
        (conn, counts)
    };
    let (mut holder, _) = connect().await;
    let (mut reader, counts) = connect().await;
    let heard = || (stat(&counts.0), stat(&counts.1));
    let refused = |filter: &'static str| {
        sg_configs::table
            .filter(sg_configs::key.eq(format!("{prefix}a")))
            .filter(diesel::dsl::sql::<diesel::sql_types::Bool>(filter))
            .select(sg_configs::value)
    };

    let held = diesel_async::RunQueryDsl::load_stream::<String>(refused("length(value) > 0"), &mut holder)
        .await
        .expect("a fallback whose rows hold the one permit");
    let before = heard();
    let turned_away = diesel_async::RunQueryDsl::load::<String>(refused("length(value) > 1"), &mut reader).await;
    let error = turned_away.expect_err("no permit to be had");
    assert!(error.to_string().contains("fallback concurrency limit"), "{error}");
    assert_eq!(heard(), before, "turned away: no query started");

    let waiting = tokio::time::timeout(
        Duration::from_millis(10),
        diesel_async::RunQueryDsl::load::<String>(refused("length(value) > 2"), &mut reader),
    )
    .await;
    assert!(waiting.is_err(), "cancelled while it waits for a permit");
    assert_eq!(heard(), before, "cancelled while it waits: no query started");
    drop(held);

    let cancelled = tokio::time::timeout(
        Duration::from_millis(300),
        diesel_async::RunQueryDsl::load::<String>(refused("(SELECT true FROM pg_sleep(1))"), &mut reader),
    )
    .await;
    assert!(cancelled.is_err(), "cancelled in the middle of its query");
    let (starts, finishes) = heard();
    assert!(starts > before.0, "the query started");
    assert_eq!(starts, finishes, "and is told finished");
}
