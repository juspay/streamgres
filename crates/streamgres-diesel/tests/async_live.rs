#![cfg(feature = "async")]
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, SimpleAsyncConnection};
use std::sync::Arc;
use std::time::Duration;
use streamgres_diesel::{
    AsyncLoadCached, Cache, CacheAttempt, CacheConfig, Tracker, wait_for_writes_async,
};
diesel::table! { async_cache_rows(id) { id -> Int8, name -> Text, } }
#[test]
fn async_reads_hit_fall_back_and_keep_transactions_on_postgres() {
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        return;
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut conn = AsyncPgConnection::establish(&dsn).await.unwrap();
        let mut config = CacheConfig::new(&dsn, "async_cache_pub");
        config.app = "async_cache_test".into();
        conn.batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS async_cache_pub; DROP TABLE IF EXISTS async_cache_rows;
            CREATE TABLE async_cache_rows(id int8 PRIMARY KEY, name text COLLATE \"C\");
            INSERT INTO async_cache_rows VALUES(1, 'one'), (2, 'two');
            CREATE PUBLICATION async_cache_pub FOR TABLE async_cache_rows; {}",
            config.setup_sql()
        ))
        .await
        .unwrap();
        let teardown = config.teardown_sql();
        let cache = Arc::new(Cache::start(config));
        let waiting = cache.clone();
        tokio::task::spawn_blocking(move || waiting.wait_ready(Duration::from_secs(10)))
            .await
            .unwrap()
            .unwrap();
        conn.set_instrumentation(Tracker::new());
        let first: CacheAttempt<Vec<i64>> = async_cache_rows::table
            .select(async_cache_rows::id)
            .order(async_cache_rows::id)
            .inspect_cached_async(cache.clone(), &mut conn)
            .await
            .unwrap();
        assert!(matches!(first, CacheAttempt::Hit(rows) if rows == vec![1, 2]));
        // The extension future can be spawned on a multithreaded executor.
        let worker_cache = cache.clone();
        let worker_dsn = dsn.clone();
        let rows = tokio::spawn(async move {
            let mut conn = AsyncPgConnection::establish(&worker_dsn).await.unwrap();
            conn.set_instrumentation(Tracker::new());
            let rows: Vec<i64> = async_cache_rows::table
                .select(async_cache_rows::id)
                .order(async_cache_rows::id)
                .load_cached_async(worker_cache, &mut conn)
                .await
                .unwrap();
            rows
        })
        .await
        .unwrap();
        assert_eq!(rows, vec![1, 2]);
        conn.batch_execute("BEGIN; INSERT INTO async_cache_rows VALUES(3, 'three')")
            .await
            .unwrap();
        let rows: Vec<i64> = async_cache_rows::table
            .select(async_cache_rows::id)
            .order(async_cache_rows::id)
            .load_cached_async(cache.clone(), &mut conn)
            .await
            .unwrap();
        assert_eq!(rows, vec![1, 2, 3]);
        conn.batch_execute("ROLLBACK; INSERT INTO async_cache_rows VALUES(4, 'four')")
            .await
            .unwrap();
        assert!(
            wait_for_writes_async(cache.clone(), &mut conn, Duration::from_secs(10))
                .await
                .unwrap()
        );
        let rows: Vec<i64> = async_cache_rows::table
            .select(async_cache_rows::id)
            .order(async_cache_rows::id)
            .load_cached_async(cache.clone(), &mut conn)
            .await
            .unwrap();
        assert_eq!(rows, vec![1, 2, 4]);
        let refused: CacheAttempt<Vec<i64>> = async_cache_rows::table
            .select(async_cache_rows::id)
            .distinct()
            .inspect_cached_async(cache.clone(), &mut conn)
            .await
            .unwrap();
        assert!(matches!(refused, CacheAttempt::Refused(_)));
        let direct: Vec<i64> = async_cache_rows::table
            .select(async_cache_rows::id)
            .distinct()
            .order(async_cache_rows::id)
            .load_cached_async(cache.clone(), &mut conn)
            .await
            .unwrap();
        assert_eq!(direct, vec![1, 2, 4]);
        tokio::task::spawn_blocking(move || cache.stop())
            .await
            .unwrap();
        conn.batch_execute(&format!(
            "{teardown} DROP PUBLICATION async_cache_pub; DROP TABLE async_cache_rows"
        ))
        .await
        .unwrap();
    });
}
