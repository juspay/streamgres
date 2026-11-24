//! Warm reads through the public API under concurrency.
//!
//! ```sh
//! cargo run --release --example warm_reads -- <dsn> [callers] [seconds] [async|blocking]
//! ```
//!
//! Sets up a table in schema `bench_warm`, warms every query it will time
//! until memory answers each one, then runs `callers` concurrent readers
//! for `seconds` (async tasks, or plain threads calling `.wait()`). It
//! reports throughput, p50 and p99 latency, and the share of reads memory
//! answered: below 100% means the run did not measure warm reads alone.
//! The database needs `wal_level = logical`; the schema is dropped after.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use streamgres::cache::{Served, Sql, Streamgres};

const SCHEMA: &str = "bench_warm";
const GROUPS: i64 = 16;
const ROWS_PER_GROUP: i64 = 50;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let dsn = args
        .next()
        .ok_or("usage: warm_reads <dsn> [callers] [seconds] [async|blocking]")?;
    let callers: usize = args.next().map_or(Ok(8), |arg| arg.parse())?;
    let seconds: u64 = args.next().map_or(Ok(5), |arg| arg.parse())?;
    let blocking = match args.next().as_deref() {
        None | Some("async") => false,
        Some("blocking") => true,
        Some(other) => return Err(format!("unknown mode `{other}`").into()),
    };

    let (admin, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await?;
    tokio::spawn(connection);
    let builder = Streamgres::connect(&dsn)
        .schemas([SCHEMA])
        .publication(format!("{SCHEMA}_pub"))
        .app("benchwarm", 0);
    admin
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {SCHEMA}_pub;
             DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;
             CREATE SCHEMA {SCHEMA};
             CREATE TABLE {SCHEMA}.t (id bigint PRIMARY KEY, grp bigint NOT NULL, name text NOT NULL);
             INSERT INTO {SCHEMA}.t
               SELECT n, n % {GROUPS}, 'row ' || n FROM generate_series(1, {}) AS n;
             CREATE PUBLICATION {SCHEMA}_pub FOR TABLES IN SCHEMA {SCHEMA};",
            GROUPS * ROWS_PER_GROUP
        ))
        .await?;
    admin.batch_execute(&builder.setup_sql()).await?;
    let lib = builder.start().await?;

    let query = |group: i64| {
        Sql::new(format!(
            "SELECT id, name FROM {SCHEMA}.t WHERE grp = $1 ORDER BY id"
        ))
        .bind(group)
    };
    for group in 0..GROUPS {
        let deadline = Instant::now() + Duration::from_secs(30);
        while lib.load_and_sync(query(group)).inspect().await.1.served != Served::Memory {
            if Instant::now() > deadline {
                return Err(format!("group {group} never warmed").into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    let before = lib.metrics();
    let stop = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let mut latencies: Vec<Duration> = Vec::new();
    if blocking {
        let workers: Vec<_> = (0..callers)
            .map(|caller| {
                let (lib, stop) = (lib.clone(), stop.clone());
                std::thread::spawn(move || {
                    let mut own = Vec::new();
                    let mut group = caller as i64 % GROUPS;
                    while !stop.load(Ordering::Relaxed) {
                        let at = Instant::now();
                        lib.load_and_sync(query(group)).wait().expect("read");
                        own.push(at.elapsed());
                        group = (group + 1) % GROUPS;
                    }
                    own
                })
            })
            .collect();
        tokio::time::sleep(Duration::from_secs(seconds)).await;
        stop.store(true, Ordering::Relaxed);
        for worker in workers {
            latencies.extend(worker.join().map_err(|_| "a reader thread panicked")?);
        }
    } else {
        let workers: Vec<_> = (0..callers)
            .map(|caller| {
                let (lib, stop) = (lib.clone(), stop.clone());
                tokio::spawn(async move {
                    let mut own = Vec::new();
                    let mut group = caller as i64 % GROUPS;
                    while !stop.load(Ordering::Relaxed) {
                        let at = Instant::now();
                        lib.load_and_sync(query(group)).await.expect("read");
                        own.push(at.elapsed());
                        group = (group + 1) % GROUPS;
                    }
                    own
                })
            })
            .collect();
        tokio::time::sleep(Duration::from_secs(seconds)).await;
        stop.store(true, Ordering::Relaxed);
        for worker in workers {
            latencies.extend(worker.await?);
        }
    }
    let elapsed = started.elapsed();
    let after = lib.metrics();

    let reads = latencies.len();
    let memory = after.answered.saturating_sub(before.answered);
    latencies.sort_unstable();
    let percentile = |p: f64| {
        latencies
            .get(((reads as f64 * p) as usize).min(reads.saturating_sub(1)))
            .copied()
            .unwrap_or_default()
    };
    println!(
        "{} callers ({}), {:.1} s: {} reads, {:.0} reads/s, p50 {:?}, p99 {:?}, memory {:.2}% ({} of {}), fallbacks {}",
        callers,
        if blocking { "blocking" } else { "async" },
        elapsed.as_secs_f64(),
        reads,
        reads as f64 / elapsed.as_secs_f64(),
        percentile(0.50),
        percentile(0.99),
        100.0 * memory as f64 / reads.max(1) as f64,
        memory,
        reads,
        after.fallbacks.saturating_sub(before.fallbacks),
    );

    lib.shutdown().await;
    admin
        .batch_execute(&format!(
            "DROP PUBLICATION {SCHEMA}_pub; DROP SCHEMA {SCHEMA} CASCADE; DROP SCHEMA benchwarm_0 CASCADE;"
        ))
        .await?;
    Ok(())
}
