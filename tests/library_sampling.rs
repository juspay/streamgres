//! Freshness sampling against a live Postgres, in a test binary of its
//! own: other tests' schema changes would force reader checks that need
//! the very pool this test holds.
//!
//! Set `STREAMGRES_PG_DSN` (a database with `wal_level = logical`);
//! without it the test is skipped.

use std::time::{Duration, Instant};

use streamgres::cache::{OnUnsupported, Served, Sql, Streamgres};

async fn admin(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    client
}

/// A pool held by a slow fallback read does not starve the freshness
/// samples: a query already in memory keeps answering from memory for
/// longer than `max_lag`.
#[tokio::test(flavor = "multi_thread")]
async fn a_saturated_pool_leaves_warm_queries_in_memory() {
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        eprintln!("STREAMGRES_PG_DSN not set: sampling scenario skipped");
        return;
    };
    let builder = Streamgres::connect(&dsn)
        .schemas(["lib_sample"])
        .publication("lib_sample_pub")
        .app("libsample", 0)
        .pool_size(1)
        .max_lag(Duration::from_secs(1))
        .on_unsupported(OnUnsupported::Fallback);
    let client = admin(&dsn).await;
    client
        .batch_execute(
            "DROP PUBLICATION IF EXISTS lib_sample_pub;
             DROP SCHEMA IF EXISTS lib_sample CASCADE;
             CREATE SCHEMA lib_sample;
             CREATE TABLE lib_sample.t (id bigint PRIMARY KEY);
             INSERT INTO lib_sample.t VALUES (1);
             CREATE PUBLICATION lib_sample_pub FOR TABLES IN SCHEMA lib_sample;",
        )
        .await
        .expect("prepare");
    client
        .batch_execute(&builder.setup_sql())
        .await
        .expect("trigger stack");
    let lib = builder.start().await.unwrap();

    let warm = Sql::new("SELECT id FROM lib_sample.t");
    let deadline = Instant::now() + Duration::from_secs(10);
    while lib.load_and_sync(warm.clone()).inspect().await.1.served != Served::Memory {
        assert!(Instant::now() < deadline, "never answered from memory");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Holds the pool's only connection for four seconds.
    let holder = {
        let lib = lib.clone();
        tokio::spawn(async move { lib.load_and_sync(Sql::new("SELECT pg_sleep(4)")).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let until = Instant::now() + Duration::from_secs(3);
    while Instant::now() < until {
        let (read, trace) = lib.load_and_sync(warm.clone()).inspect().await;
        assert_eq!(trace.served, Served::Memory, "{trace}");
        assert_eq!(read.unwrap().len(), 1);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    holder.await.unwrap().expect("the slow read finishes");

    lib.shutdown().await;
    client
        .batch_execute(
            "DROP PUBLICATION lib_sample_pub;
             DROP SCHEMA lib_sample CASCADE;
             DROP SCHEMA libsample_0 CASCADE;",
        )
        .await
        .expect("clean up");
}
