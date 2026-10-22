//! The async wrapper against a running engine: rows mode end to end,
//! including the one-shot load of every table's types.
//!
//! Needs `STREAMGRES_S2S_URL` and `DATABASE_URL`; skips itself otherwise.

#![cfg(feature = "async")]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use diesel_streamgres::cache::Cache;
use diesel_streamgres::engine::EngineClient;
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
    let Ok(url) = std::env::var("STREAMGRES_S2S_URL") else {
        eprintln!("STREAMGRES_S2S_URL not set; skipping");
        return;
    };
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let engine = EngineClient::connect(url);
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
    let prefix = format!("async-live-{}-", std::process::id());
    let key = format!("{prefix}r");
    put(&mut other, &key, "1").await;

    let inner = AsyncPgConnection::establish(&database)
        .await
        .expect("connect");
    let mut conn = AsyncSyncedConnection::with_cache(inner, cache.clone(), Route::Streamgres);
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
