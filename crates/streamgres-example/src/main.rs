//! `example serve | seed | bench | verify | lag | migrate`.
//!
//! Environment:
//!
//! | variable              | meaning                                        | default        |
//! |-----------------------|------------------------------------------------|----------------|
//! | `DATABASE_URL`        | the Postgres the engine follows                | required       |
//! | `STREAMGRES_S2S_URL`  | `ws://host:port/<base>/s2s/v1`                 | required       |
//! | `EXAMPLE_ADDR`        | where `serve` listens                          | `0.0.0.0:8080` |
//! | `EXAMPLE_WRITER_MS`   | the background writer's tick; 0 turns it off   | `1000`         |
//! | `EXAMPLE_WRITER_MIX`  | the writer's operations, comma separated       | all of them    |
//! | `EXAMPLE_VERIFY_MS`   | `serve` also verifies every lookup this often; 0 turns it off | `0` |
//! | `EXAMPLE_SCALE`       | multiplies the seed's tickets, comments and attachments | `1`  |
//!
//! Plus the wrapper's own `STREAMGRES_CACHE_*`, `STREAMGRES_S2S_*` and
//! `STREAMGRES_READ_YOUR_WRITES_MS`.

use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use streamgres_diesel::Route;
use streamgres_example::migrate::Step;
use streamgres_example::pools::Stack;
use streamgres_example::queries::Lookup;
use streamgres_example::{api, bench, lag, seed, verify, writer};

#[derive(Parser)]
#[command(
    name = "example",
    about = "A ticket desk on the Streamgres Diesel wrapper"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Serves the HTTP API and the stats page.
    Serve,
    /// Creates the schema and loads the dataset; safe to run again.
    Seed,
    /// Times a lookup on each route, in process.
    Bench {
        /// A lookup by name (`example bench --list`).
        #[arg(long, default_value = "queue")]
        lookup: String,
        /// `streamgres`, `postgres` or `both`.
        #[arg(long, default_value = "both")]
        route: String,
        #[arg(long, default_value_t = 16)]
        concurrency: usize,
        #[arg(long, default_value_t = 5000)]
        requests: usize,
        /// Lists the lookups and exits.
        #[arg(long)]
        list: bool,
    },
    /// Reads every lookup through the cache and through Postgres,
    /// repeatedly, and reports every pair that never agreed. Exits 1
    /// when any did not.
    Verify {
        /// Milliseconds between passes.
        #[arg(long, default_value_t = 250)]
        every: u64,
        /// Seconds to run for.
        #[arg(long = "for", default_value_t = 30)]
        seconds: u64,
        /// Milliseconds a differing pair may take to agree before it is
        /// a mismatch.
        #[arg(long, default_value_t = 2000)]
        settle: u64,
        /// Run the background writer meanwhile, at this tick in
        /// milliseconds; 0 for none.
        #[arg(long, default_value_t = 50)]
        writer_ms: u64,
    },
    /// Measures how long a commit takes to reach the cached answer,
    /// under a write rate.
    Lag {
        /// Background writes per second.
        #[arg(long, default_value_t = 20)]
        rate: u32,
        /// Seconds to measure for.
        #[arg(long = "for", default_value_t = 10)]
        seconds: u64,
    },
    /// Runs a schema change: `widen`, `new-type` or `drop`.
    Migrate { step: String },
}

fn env(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("{name} is not set"))
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("example: {error}");
        std::process::exit(1);
    }
}

/// A connected stack, or why not.
async fn connect() -> Result<Stack, String> {
    let stack = Stack::connect(&env("DATABASE_URL")?, &env("STREAMGRES_S2S_URL")?).await?;
    if !stack.wait_for_engine(Duration::from_secs(10)).await {
        return Err("the engine is not reachable".to_string());
    }
    Ok(stack)
}

/// A number from the environment, or the default.
fn env_number(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

async fn run() -> Result<(), String> {
    match Cli::parse().command {
        Command::Seed => {
            let database_url = env("DATABASE_URL")?;
            let mut conn = Stack::plain_sync(&database_url).map_err(|error| error.to_string())?;
            seed::run(&mut conn).map_err(|error| error.to_string())?;
            println!("seeded: {}", seed::summary(seed::scale()));
            Ok(())
        }
        Command::Serve => {
            let stack =
                Arc::new(Stack::connect(&env("DATABASE_URL")?, &env("STREAMGRES_S2S_URL")?).await?);
            if !stack.wait_for_engine(Duration::from_secs(10)).await {
                eprintln!(
                    "example: the engine is not reachable yet; reads go to Postgres until it is"
                );
            }
            let every = env_number("EXAMPLE_WRITER_MS", 1000);
            if every > 0 {
                writer::spawn(
                    stack.plain.clone(),
                    Duration::from_millis(every),
                    writer::mix_from_env(),
                    Arc::clone(&stack.writer),
                );
            }
            let verify_every = env_number("EXAMPLE_VERIFY_MS", 0);
            if verify_every > 0 {
                let stack = Arc::clone(&stack);
                tokio::spawn(async move {
                    let lookups = Lookup::demo();
                    loop {
                        tokio::time::sleep(Duration::from_millis(verify_every)).await;
                        let outcome = verify::once(&stack, &lookups, Duration::from_secs(2)).await;
                        for mismatch in &outcome.mismatches {
                            eprintln!("verify: {} never agreed", mismatch.lookup);
                        }
                    }
                });
            }
            let addr = std::env::var("EXAMPLE_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
            let listener = tokio::net::TcpListener::bind(&addr)
                .await
                .map_err(|error| format!("bind {addr}: {error}"))?;
            println!("example: serving on http://{addr} (stats at /stats, a check at /verify)");
            axum::serve(listener, api::router(stack))
                .await
                .map_err(|error| error.to_string())
        }
        Command::Bench {
            lookup,
            route,
            concurrency,
            requests,
            list,
        } => {
            if list {
                for lookup in Lookup::demo() {
                    println!("{:<16} {}", lookup.name(), lookup.exercises());
                }
                return Ok(());
            }
            let lookup = Lookup::named(&lookup)
                .ok_or_else(|| format!("no lookup `{lookup}`; see --list"))?;
            let stack = connect().await?;
            let routes: Vec<Route> = match route.as_str() {
                "streamgres" => vec![Route::Streamgres],
                "postgres" => vec![Route::Postgres],
                _ => vec![Route::Postgres, Route::Streamgres],
            };
            println!(
                "{} ({}), {concurrency} tasks, {requests} requests per route",
                lookup.name(),
                lookup.exercises()
            );
            println!(
                "{:<12} {:>9} {:>9} {:>9} {:>9} {:>10}",
                "route", "p50", "p95", "p99", "max", "req/s"
            );
            for route in routes {
                let report = bench::run(&stack, &lookup, route, concurrency, requests).await?;
                println!(
                    "{:<12} {:>9} {:>9} {:>9} {:>9} {:>10.0}",
                    format!("{route:?}").to_lowercase(),
                    micros(report.p50),
                    micros(report.p95),
                    micros(report.p99),
                    micros(report.max),
                    report.per_second()
                );
            }
            let stats = api::stats_json(&stack);
            println!("cache {}", stats["cache"]);
            Ok(())
        }
        Command::Verify {
            every,
            seconds,
            settle,
            writer_ms,
        } => {
            let stack = connect().await?;
            if writer_ms > 0 {
                writer::spawn(
                    stack.plain.clone(),
                    Duration::from_millis(writer_ms),
                    writer::mix_from_env(),
                    Arc::clone(&stack.writer),
                );
            }
            let lookups = Lookup::demo();
            let started = std::time::Instant::now();
            let mut mismatches = 0u64;
            let mut last_report = std::time::Instant::now();
            while started.elapsed() < Duration::from_secs(seconds) {
                let outcome = verify::once(&stack, &lookups, Duration::from_millis(settle)).await;
                for mismatch in &outcome.mismatches {
                    mismatches += 1;
                    println!(
                        "MISMATCH {}: cache {} | postgres {}",
                        mismatch.lookup,
                        truncate(&mismatch.cached.to_string()),
                        truncate(&mismatch.postgres.to_string())
                    );
                }
                if last_report.elapsed() >= Duration::from_secs(5) {
                    last_report = std::time::Instant::now();
                    println!(
                        "{:>4}s  verify {}  writer {}",
                        started.elapsed().as_secs(),
                        stack.verify.json(),
                        stack.writer.json()
                    );
                }
                tokio::time::sleep(Duration::from_millis(every)).await;
            }
            println!("verify {}", stack.verify.json());
            println!("writer {}", stack.writer.json());
            if mismatches > 0 {
                return Err(format!("{mismatches} pair(s) never agreed"));
            }
            println!("every pair agreed");
            Ok(())
        }
        Command::Lag { rate, seconds } => {
            let stack = connect().await?;
            let report = lag::run(&stack, rate, Duration::from_secs(seconds)).await?;
            println!(
                "commit to cached answer over {} samples, {:.0} writes/s of load:",
                report.samples,
                report.writes_per_second()
            );
            println!(
                "p50 {}  p95 {}  p99 {}  max {}",
                micros(report.p50),
                micros(report.p95),
                micros(report.p99),
                micros(report.max)
            );
            Ok(())
        }
        Command::Migrate { step } => {
            let step = Step::parse(&step).ok_or_else(|| {
                format!("no migration `{step}`; the steps are widen, new-type, drop")
            })?;
            let mut conn =
                Stack::plain_sync(&env("DATABASE_URL")?).map_err(|error| error.to_string())?;
            step.run(&mut conn).map_err(|error| error.to_string())?;
            println!("{step:?} done: {}", step.expectation());
            Ok(())
        }
    }
}

fn micros(duration: Duration) -> String {
    format!("{}µs", duration.as_micros())
}

/// The first part of a long JSON answer, for a report line.
fn truncate(text: &str) -> String {
    if text.len() <= 200 {
        return text.to_string();
    }
    let mut end = 200;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}
