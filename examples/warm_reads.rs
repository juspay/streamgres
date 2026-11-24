//! Warm reads through the public API under concurrency.
//!
//! ```sh
//! cargo run --release --example warm_reads -- <dsn> [callers] [seconds] [async|blocking]
//! ```
//!
//! Creates a schema for each run, warms every query it will time
//! until memory answers each one, then runs `callers` concurrent readers
//! for `seconds` (async tasks, or plain threads calling `.wait()`). It
//! reports throughput, p50 and p99 latency, and the share of reads memory
//! answered: below 100% means the run did not measure warm reads alone.
//! Each caller stops at its own deadline. The database needs
//! `wal_level = logical`; the schema and trigger stack are dropped after.

use std::time::{Duration, Instant};

use streamgres::cache::{Served, Sql, Streamgres};

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

    if callers == 0 || seconds == 0 {
        return Err("callers and seconds must be positive".into());
    }
    let run = uuid::Uuid::new_v4().simple();
    let schema = format!("bench_warm_{run}");
    let app = format!("benchwarm{run}");

    let (mut admin, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await?;
    tokio::spawn(connection);
    let builder = Streamgres::connect(&dsn)
        .schemas([schema.as_str()])
        .publication(format!("{schema}_pub"))
        .app(&app, 0);
    // Reserve both schema names in the same transaction as setup. A name
    // conflict rolls back setup before this run owns any fixtures.
    let setup = admin.transaction().await?;
    setup
        .batch_execute(&format!(
            "CREATE SCHEMA {schema};
             CREATE SCHEMA {app}_0;
             CREATE TABLE {schema}.t (id bigint PRIMARY KEY, grp bigint NOT NULL, name text NOT NULL);
             INSERT INTO {schema}.t
               SELECT n, n % {GROUPS}, 'row ' || n FROM generate_series(1, {}) AS n;
             CREATE PUBLICATION {schema}_pub FOR TABLES IN SCHEMA {schema};",
            GROUPS * ROWS_PER_GROUP
        ))
        .await?;
    setup.batch_execute(&builder.setup_sql()).await?;
    setup.commit().await?;

    let result = async {
        let lib = builder.start().await?;
        let result = measure(
            &lib,
            &schema,
            callers,
            Duration::from_secs(seconds),
            blocking,
        )
        .await;
        lib.shutdown().await;
        result
    }
    .await;
    let cleanup = admin
        .batch_execute(&format!(
            "DROP PUBLICATION {schema}_pub; DROP SCHEMA {schema} CASCADE; DROP SCHEMA {app}_0 CASCADE;"
        ))
        .await;
    if let Err(error) = &cleanup
        && result.is_err()
    {
        eprintln!("could not remove benchmark fixtures for {schema}: {error}");
    }
    result?;
    cleanup?;
    Ok(())
}

async fn measure(
    lib: &Streamgres,
    schema: &str,
    callers: usize,
    duration: Duration,
    blocking: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let text: std::sync::Arc<str> =
        format!("SELECT id, name FROM {schema}.t WHERE grp = $1 ORDER BY id").into();
    let query = move |group: i64| Sql::new(&*text).bind(group);
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
    let started = Instant::now();
    // Every caller stops at this deadline on its own: a memory hit does not
    // yield, so a timer task on the same runtime could wait behind them.
    let until = started
        .checked_add(duration)
        .ok_or("benchmark duration is too large")?;
    let mut latencies: Vec<Duration> = Vec::new();
    let mut active_callers = 0;
    if blocking {
        let workers: Vec<_> = (0..callers)
            .map(|caller| {
                let (lib, query) = (lib.clone(), query.clone());
                std::thread::spawn(move || {
                    let mut own = Vec::new();
                    let mut group = caller as i64 % GROUPS;
                    while Instant::now() < until {
                        let at = Instant::now();
                        lib.load_and_sync(query(group)).wait().expect("read");
                        own.push(at.elapsed());
                        group = (group + 1) % GROUPS;
                    }
                    own
                })
            })
            .collect();
        for worker in workers {
            let samples = worker.join().map_err(|_| "a reader thread panicked")?;
            active_callers += usize::from(!samples.is_empty());
            latencies.extend(samples);
        }
    } else {
        let workers: Vec<_> = (0..callers)
            .map(|caller| {
                let (lib, query) = (lib.clone(), query.clone());
                tokio::spawn(async move {
                    let mut own = Vec::new();
                    let mut group = caller as i64 % GROUPS;
                    while Instant::now() < until {
                        let at = Instant::now();
                        lib.load_and_sync(query(group)).await.expect("read");
                        own.push(at.elapsed());
                        group = (group + 1) % GROUPS;
                        // Warm reads can complete without yielding.
                        if own.len() % 64 == 0 {
                            tokio::task::yield_now().await;
                        }
                    }
                    own
                })
            })
            .collect();
        for worker in workers {
            let samples = worker.await?;
            active_callers += usize::from(!samples.is_empty());
            latencies.extend(samples);
        }
    }
    let elapsed = started.elapsed();
    let after = lib.metrics();

    if active_callers != callers {
        return Err(format!("only {active_callers} of {callers} callers recorded samples").into());
    }
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

    Ok(())
}
