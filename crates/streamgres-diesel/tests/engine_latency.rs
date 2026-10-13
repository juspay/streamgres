//! Commit-to-cache latency against a running engine, on any table.
//!
//! Flips one text column of one existing row back and forth from a
//! separate connection and measures how long a cached read of that row
//! takes to show each new value: that is the engine's propagation (WAL
//! decode, routing, the touch over the socket) plus one Postgres refill.
//!
//! Driven by environment so it runs against a real schema:
//! - `STREAMGRES_S2S_URL`, `DATABASE_URL`
//! - `STREAMGRES_LATENCY_TABLE`, `STREAMGRES_LATENCY_KEY_COLUMN`,
//!   `STREAMGRES_LATENCY_VALUE_COLUMN`, `STREAMGRES_LATENCY_ROW_KEY`
//!   (an existing row; its value column is overwritten and restored)
//! - `STREAMGRES_LATENCY_SAMPLES` (default 25)
//!
//! Skips itself when any is missing.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::Text;
use streamgres_diesel::cache::Cache;
use streamgres_diesel::engine::EngineClient;
use streamgres_diesel::{Consistency, SyncedConnection};

#[derive(QueryableByName)]
struct Val {
    #[diesel(sql_type = Text)]
    v: String,
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

#[test]
fn commit_to_refilled_read_latency() {
    let (Some(url), Some(database), Some(table), Some(key_column), Some(value_column), Some(row_key)) = (
        env("STREAMGRES_S2S_URL"),
        env("DATABASE_URL"),
        env("STREAMGRES_LATENCY_TABLE"),
        env("STREAMGRES_LATENCY_KEY_COLUMN"),
        env("STREAMGRES_LATENCY_VALUE_COLUMN"),
        env("STREAMGRES_LATENCY_ROW_KEY"),
    ) else {
        eprintln!("latency env not set; skipping");
        return;
    };
    let samples: usize = env("STREAMGRES_LATENCY_SAMPLES")
        .and_then(|value| value.parse().ok())
        .unwrap_or(25);

    let engine = EngineClient::connect(url);
    let cache = Arc::new(Cache::new(64 * 1024 * 1024, Arc::new(engine.clone())));
    engine.attach(&cache);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !engine.is_connected() {
        assert!(Instant::now() < deadline, "the engine did not connect");
        std::thread::sleep(Duration::from_millis(50));
    }
    let mut writer = PgConnection::establish(&database).expect("connect");
    let mut conn = SyncedConnection::with_cache(
        PgConnection::establish(&database).expect("connect"),
        cache.clone(),
        Consistency::Eventual,
    );

    let select = format!(
        "SELECT \"{value_column}\" AS v FROM \"{table}\" WHERE \"{key_column}\" = $1"
    );
    let update = format!(
        "UPDATE \"{table}\" SET \"{value_column}\" = $1 WHERE \"{key_column}\" = $2"
    );
    let read = |conn: &mut SyncedConnection<PgConnection>| -> String {
        sql_query(&select)
            .bind::<Text, _>(&row_key)
            .load::<Val>(conn)
            .expect("read")
            .pop()
            .expect("the row exists")
            .v
    };
    let original = read(&mut conn);
    assert_eq!(engine.subscribed(), 1, "the engine accepted the read");
    assert_eq!(read(&mut conn), original);
    assert_eq!(cache.stats.hits.load(Ordering::Relaxed), 1, "the second read hit");

    let mut took = Vec::with_capacity(samples);
    for sample in 0..samples {
        let next = format!("sg-latency-{sample}");
        let started = Instant::now();
        sql_query(&update)
            .bind::<Text, _>(&next)
            .bind::<Text, _>(&row_key)
            .execute(&mut writer)
            .expect("update");
        let committed = Instant::now();
        loop {
            if read(&mut conn) == next {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "sample {sample} never arrived"
            );
            std::thread::sleep(Duration::from_micros(200));
        }
        took.push((committed.elapsed(), started.elapsed()));
    }
    // Restore the row.
    sql_query(&update)
        .bind::<Text, _>(&original)
        .bind::<Text, _>(&row_key)
        .execute(&mut writer)
        .expect("restore");

    let mut after_commit: Vec<Duration> = took.iter().map(|(a, _)| *a).collect();
    let mut with_write: Vec<Duration> = took.iter().map(|(_, b)| *b).collect();
    after_commit.sort();
    with_write.sort();
    let at = |sorted: &[Duration], q: f64| sorted[((sorted.len() - 1) as f64 * q).round() as usize];
    eprintln!(
        "LATENCY samples={samples} table={table} commit->refilled read: p50 {:?} p90 {:?} max {:?}; including the write: p50 {:?} p90 {:?} max {:?}",
        at(&after_commit, 0.5),
        at(&after_commit, 0.9),
        after_commit[after_commit.len() - 1],
        at(&with_write, 0.5),
        at(&with_write, 0.9),
        with_write[with_write.len() - 1],
    );
    assert!(cache.stats.touched.load(Ordering::Relaxed) as usize >= samples);
    assert_eq!(engine.subscribed(), 1);
}
