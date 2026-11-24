//! `demo serve | seed | verify | bench | migrate`.
//!
//! `DATABASE_URL` names the Postgres Streamgres follows (`wal_level =
//! logical`; `seed` and `serve --init` install the publication and the
//! trigger stack, so they need a superuser). `DEMO_SCALE` multiplies the
//! seed's rows.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use streamgres_demo::migrate::Step;
use streamgres_demo::queries::{Lookup, Route};
use streamgres_demo::verify::{self, Verification};
use streamgres_demo::{bench, db, seed, writer};

#[derive(Parser)]
#[command(
    name = "demo",
    about = "A ticket desk whose reads go through Streamgres"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Runs the ticket desk, its comparisons and its lab.
    Serve(streamgres_demo::service::Options),
    /// Creates the schema, the publication and the trigger stack, and
    /// (re)loads the dataset.
    Seed,
    /// Compares every lookup through Streamgres and through Postgres
    /// while the writer runs; exits 1 on any problem.
    Verify {
        /// Seconds to run for.
        #[arg(long = "for", default_value_t = 30)]
        seconds: u64,
        /// Milliseconds a differing pair may take to agree.
        #[arg(long, default_value_t = 2000)]
        settle: u64,
        /// The writer's tick in milliseconds; 0 for none.
        #[arg(long, default_value_t = 10)]
        writer_ms: u64,
    },
    /// Times a lookup on each route.
    Bench {
        #[arg(long, default_value = "queue_cards")]
        lookup: String,
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
        #[arg(long, default_value_t = 5)]
        seconds: u64,
    },
    /// Runs a schema change: `widen`, `new-type` or `drop`.
    Migrate { step: String },
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("demo: {error}");
        std::process::exit(1);
    }
}

fn database_url() -> Result<String, String> {
    std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL is not set".to_owned())
}

async fn run() -> Result<(), String> {
    match Cli::parse().command {
        Command::Serve(options) => streamgres_demo::service::serve(database_url()?, options).await,
        Command::Seed => {
            let pool = db::pool(&database_url()?, 1).await?;
            let mut conn = pool.get().await.map_err(|error| error.to_string())?;
            db::install(&mut conn, true)
                .await
                .map_err(|error| error.to_string())?;
            println!("seeded: {}", seed::summary(seed::scale()));
            Ok(())
        }
        Command::Migrate { step } => {
            let step = Step::parse(&step).ok_or("the steps are widen, new-type and drop")?;
            let pool = db::pool(&database_url()?, 1).await?;
            let mut conn = pool.get().await.map_err(|error| error.to_string())?;
            step.run(&mut conn)
                .await
                .map_err(|error| error.to_string())?;
            println!("{step:?}: {}", step.expectation());
            Ok(())
        }
        Command::Verify {
            seconds,
            settle,
            writer_ms,
        } => verify_for(seconds, settle, writer_ms).await,
        Command::Bench {
            lookup,
            concurrency,
            seconds,
        } => {
            let url = database_url()?;
            let pool = db::pool(&url, concurrency as u32 + 2).await?;
            let lib = db::start(&url, &db::Settings::default()).await?;
            let lookup = Lookup::named(&lookup).ok_or("no such lookup")?;
            let stop = Arc::new(AtomicBool::new(false));
            for route in [Route::Streamgres, Route::Postgres] {
                let run = bench::run(
                    &lib,
                    &pool,
                    &lookup,
                    route,
                    concurrency,
                    Duration::from_secs(seconds),
                    stop.clone(),
                )
                .await;
                println!(
                    "{:<10} {:>9.0} req/s  p50 {:>8.1} µs  p99 {:>8.1} µs  memory {:>3}%  errors {}",
                    format!("{route:?}"),
                    run.requests_per_second,
                    run.p50_us,
                    run.p99_us,
                    run.memory * 100 / run.requests.max(1),
                    run.errors,
                );
            }
            lib.shutdown().await;
            Ok(())
        }
    }
}

/// Verification passes for `seconds` under the writer; an error naming
/// every problem found.
async fn verify_for(seconds: u64, settle: u64, writer_ms: u64) -> Result<(), String> {
    let url = database_url()?;
    let pool = db::pool(&url, 6).await?;
    let lib = db::start(&url, &db::Settings::default()).await?;
    let stop = Arc::new(AtomicBool::new(false));
    let counts = Arc::new(writer::Counts::default());
    let writing = (writer_ms > 0).then(|| {
        let (pool, stop, counts) = (pool.clone(), stop.clone(), counts.clone());
        tokio::spawn(async move {
            let mut session = writer::Session::default();
            let mix = writer::mix_from_env();
            while !stop.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(writer_ms)).await;
                let op = mix[rand::random_range(0..mix.len())];
                if let Ok(mut conn) = pool.get().await
                    && let Err(error) = session.run(op, &mut conn, &counts).await
                {
                    eprintln!("writer: {op}: {error}");
                }
            }
        })
    });
    let lookups = Lookup::demo();
    let tallies = std::sync::Mutex::new(Verification::default());
    let deadline = Instant::now() + Duration::from_secs(seconds);
    while Instant::now() < deadline {
        verify::pass(
            &lib,
            &pool,
            &lookups,
            Duration::from_millis(settle),
            &stop,
            &tallies,
        )
        .await;
    }
    stop.store(true, Ordering::Release);
    if let Some(writing) = writing {
        let _ = writing.await;
    }
    lib.shutdown().await;
    let tallies = tallies
        .into_inner()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    println!(
        "{:<16} {:>8} {:>7} {:>7} {:>7} {:>7} {:>6}  expected",
        "lookup", "compared", "memory", "pg", "lagged", "live≠", "wrong"
    );
    for lookup in &lookups {
        let Some(tally) = tallies.by_lookup.get(lookup.name()) else {
            continue;
        };
        println!(
            "{:<16} {:>8} {:>7} {:>7} {:>7} {:>7} {:>6}  {:?}",
            lookup.name(),
            tally.compared,
            tally.from_memory,
            tally.from_postgres,
            tally.lagged,
            tally.live_disagreed,
            tally.mismatches,
            lookup.expected(),
        );
    }
    println!(
        "writer: {} operations, {} errors",
        counts.total(),
        counts.errors()
    );
    if let Some(mismatch) = &tallies.last_mismatch {
        println!("last mismatch: {mismatch}");
    }
    let problems = tallies.problems(&lookups);
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems.join("; "))
    }
}
