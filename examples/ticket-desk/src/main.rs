//! `demo seed | explain | verify | bench | lag | schema | migrate`.
//!
//! `DATABASE_URL` names the Postgres the cache follows (`wal_level =
//! logical`; `seed` installs the publication and the trigger stack, so it
//! needs a superuser). `DEMO_SCALE` multiplies the seed's rows.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use clap::{Parser, Subcommand};
use diesel::{Connection, PgConnection};
use streamgres_demo::migrate::Step;
use streamgres_demo::queries::{Lookup, Route};
use streamgres_demo::stack::{self, Stack};
use streamgres_demo::verify::{self, Tallies};
use streamgres_demo::{bench, lag, seed, writer};

#[derive(Parser)]
#[command(
    name = "demo",
    about = "The ticket desk on the embedded Streamgres cache"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Runs the local ticket desk, live comparisons and reliability lab.
    Serve(streamgres_demo::service::Options),
    /// Creates the schema, the publication and the trigger stack, and
    /// loads the dataset; safe to run again.
    Seed,
    /// Says, for every lookup, whether the cache answers it and why not.
    Explain,
    /// Compares every lookup through the cache and through Postgres
    /// under the writer; exits 1 on any problem.
    Verify {
        #[arg(long, default_value_t = 4)]
        readers: usize,
        /// Seconds to run for.
        #[arg(long = "for", default_value_t = 30)]
        seconds: u64,
        /// Milliseconds between a reader's passes.
        #[arg(long, default_value_t = 20)]
        every: u64,
        /// Milliseconds a differing pair may take to agree.
        #[arg(long, default_value_t = 2000)]
        settle: u64,
        /// The writer's tick in milliseconds; 0 for none.
        #[arg(long, default_value_t = 10)]
        writer_ms: u64,
    },
    /// Times a lookup on each route.
    Bench {
        #[arg(long, default_value = "queue")]
        lookup: String,
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
        #[arg(long, default_value_t = 20000)]
        requests: usize,
        /// Times every lookup, one line each.
        #[arg(long)]
        all: bool,
    },
    /// How long a commit takes to reach the cached answer.
    Lag {
        #[arg(long, default_value_t = 200)]
        samples: usize,
        /// The writer's tick in milliseconds; 0 for none.
        #[arg(long, default_value_t = 10)]
        writer_ms: u64,
    },
    /// Every kind of schema change under a running cache, each followed
    /// by a verify pass.
    Schema {
        /// Seconds of verify after each step.
        #[arg(long = "for", default_value_t = 8)]
        seconds: u64,
    },
    /// Runs a schema change: `widen`, `new-type` or `drop`.
    Migrate { step: String },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("demo: {error}");
        std::process::exit(1);
    }
}

fn database_url() -> Result<String, String> {
    std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL is not set".to_owned())
}

fn start() -> Result<Arc<Stack>, String> {
    let stack = Stack::start(&database_url()?, Duration::from_secs(60))?;
    Ok(Arc::new(stack))
}

/// The writer, running until the returned flag is set.
fn start_writer(stack: &Stack, every_ms: u64) -> (Arc<AtomicBool>, Arc<writer::Counts>) {
    let stop = Arc::new(AtomicBool::new(false));
    let counts = Arc::new(writer::Counts::default());
    if every_ms > 0 {
        writer::spawn(
            stack.database_url.clone(),
            Duration::from_millis(every_ms),
            writer::mix_from_env(),
            Arc::clone(&counts),
            Arc::clone(&stop),
        );
    }
    (stop, counts)
}

fn print_tallies(lookups: &[Lookup], tallies: &Tallies) {
    println!(
        "{:<16} {:>8} {:>7} {:>7} {:>7} {:>8} {:>6} {:>9}  expected",
        "lookup", "compared", "cache", "pg", "lagged", "live≠", "wrong", "settle"
    );
    for lookup in lookups {
        let Some(tally) = tallies.get(lookup.name()) else {
            continue;
        };
        println!(
            "{:<16} {:>8} {:>7} {:>7} {:>7} {:>8} {:>6} {:>7}ms  {:?}",
            lookup.name(),
            tally.compared,
            tally.from_cache,
            tally.from_postgres,
            tally.lagged,
            tally.live_disagreed,
            tally.mismatches,
            tally.max_settle.as_millis(),
            lookup.expected(),
        );
        if let Some(mismatch) = &tally.first_mismatch {
            println!("    cached:   {}", mismatch.cached);
            println!("    postgres: {}", mismatch.postgres);
        }
    }
}

/// A verify pass of `lookups`; its problems.
fn verify_pass(
    stack: &Arc<Stack>,
    lookups: &[Lookup],
    seconds: u64,
) -> Result<Vec<String>, String> {
    let tallies = verify::run(
        stack,
        lookups,
        4,
        Duration::from_secs(seconds),
        Duration::from_millis(20),
        Duration::from_secs(2),
    )?;
    print_tallies(lookups, &tallies);
    let problems = verify::problems(lookups, &tallies);
    if !problems.is_empty() {
        for (name, reason) in verify::explain(stack, lookups)? {
            println!("   refused {name}: {reason}");
        }
    }
    Ok(problems)
}

fn run() -> Result<(), String> {
    match Cli::parse().command {
        Command::Serve(options) => {
            let url = database_url()?;
            tokio::runtime::Runtime::new()
                .map_err(|error| error.to_string())?
                .block_on(streamgres_demo::service::serve(url, options))
        }
        Command::Seed => {
            let url = database_url()?;
            let mut conn = PgConnection::establish(&url).map_err(|error| error.to_string())?;
            seed::run(&mut conn).map_err(|error| error.to_string())?;
            stack::install_database(&mut conn, &stack::config(&url))?;
            println!("seeded: {}", seed::summary(seed::scale()));
            Ok(())
        }
        Command::Explain => {
            let stack = start()?;
            let lookups = Lookup::demo();
            let refusals = verify::explain(&stack, &lookups)?;
            for lookup in &lookups {
                match refusals.get(lookup.name()) {
                    None => println!("{:<16} cache      {}", lookup.name(), lookup.exercises()),
                    Some(reason) => println!(
                        "{:<16} postgres   {} — {reason}",
                        lookup.name(),
                        lookup.exercises()
                    ),
                }
            }
            Ok(())
        }
        Command::Verify {
            readers,
            seconds,
            every,
            settle,
            writer_ms,
        } => {
            let stack = start()?;
            let (stop, counts) = start_writer(&stack, writer_ms);
            let lookups = Lookup::demo();
            let tallies = verify::run(
                &stack,
                &lookups,
                readers,
                Duration::from_secs(seconds),
                Duration::from_millis(every),
                Duration::from_millis(settle),
            )?;
            stop.store(true, Ordering::Relaxed);
            print_tallies(&lookups, &tallies);
            println!("writer: {} ({} errors)", counts.json(), counts.errors());
            let problems = verify::problems(&lookups, &tallies);
            if problems.is_empty() {
                println!("verify: passed");
                Ok(())
            } else {
                Err(format!("verify failed:\n  {}", problems.join("\n  ")))
            }
        }
        Command::Bench {
            lookup,
            concurrency,
            requests,
            all,
        } => {
            let stack = start()?;
            streamgres_demo::queries::ROWS_ONLY.store(true, Ordering::Relaxed);
            let lookups = if all {
                Lookup::demo()
            } else {
                vec![Lookup::named(&lookup).ok_or_else(|| format!("no lookup `{lookup}`"))?]
            };
            println!(
                "{:<16} {:<9} {:>10} {:>10} {:>10} {:>10} {:>11} {:>7}",
                "lookup", "route", "p50", "p95", "p99", "max", "req/s", "cache%"
            );
            for lookup in &lookups {
                let routes = match std::env::var("DEMO_BENCH_ROUTE").as_deref() {
                    Ok("cached") => vec![Route::Cached],
                    Ok("postgres") => vec![Route::Postgres],
                    _ => vec![Route::Postgres, Route::Cached],
                };
                for route in routes {
                    let report = bench::run(&stack, lookup, route, concurrency, requests)?;
                    println!(
                        "{:<16} {:<9} {:>10.1?} {:>10.1?} {:>10.1?} {:>10.1?} {:>11.0} {:>6.0}%",
                        lookup.name(),
                        format!("{route:?}"),
                        report.p50,
                        report.p95,
                        report.p99,
                        report.max,
                        report.per_second(),
                        100.0 * report.from_cache as f64 / report.requests.max(1) as f64,
                    );
                }
            }
            Ok(())
        }
        Command::Lag { samples, writer_ms } => {
            let stack = start()?;
            let (stop, _counts) = start_writer(&stack, writer_ms);
            let elsewhere = lag::from_elsewhere(&stack, samples)?;
            let own = lag::own_writes(&stack, samples)?;
            stop.store(true, Ordering::Relaxed);
            for (name, report) in [("another service", &elsewhere), ("own write", &own)] {
                println!(
                    "{name:<16} n={:<4} p50 {:>9.1?} p95 {:>9.1?} p99 {:>9.1?} max {:>9.1?}  refused {} stale {} timeouts {}",
                    report.samples.len(),
                    report.at(0.50),
                    report.at(0.95),
                    report.at(0.99),
                    report.at(1.0),
                    report.refused,
                    report.stale,
                    report.timeouts,
                );
            }
            if own.stale > 0 || own.timeouts > 0 {
                return Err("a read after wait_for_writes missed the write".to_owned());
            }
            Ok(())
        }
        Command::Schema { seconds } => {
            let url = database_url()?;
            let mut admin = PgConnection::establish(&url).map_err(|error| error.to_string())?;
            Step::Drop
                .run(&mut admin)
                .map_err(|error| error.to_string())?;
            let stack = start()?;
            let (stop, _counts) = start_writer(&stack, 20);
            let mut problems = Vec::new();
            let lookups = Lookup::demo();
            println!("== before any change");
            problems.extend(verify_pass(&stack, &lookups, seconds)?);
            for step in [Step::Widen, Step::NewType] {
                println!("== {step:?}: {}", step.expectation());
                step.run(&mut admin).map_err(|error| error.to_string())?;
                let mut lookups = lookups.clone();
                if step == Step::NewType {
                    lookups.push(Lookup::named("of_kind").ok_or("no of_kind")?);
                }
                problems.extend(
                    verify_pass(&stack, &lookups, seconds)?
                        .into_iter()
                        .map(|problem| format!("after {step:?}: {problem}")),
                );
            }
            println!("== Drop: {}", Step::Drop.expectation());
            Step::Drop
                .run(&mut admin)
                .map_err(|error| error.to_string())?;
            let ready = stack.cache.wait_ready(Duration::from_secs(60));
            println!("   the cache is ready again: {ready:?}");
            problems.extend(
                verify_pass(&stack, &lookups, seconds)?
                    .into_iter()
                    .map(|problem| format!("after Drop: {problem}")),
            );
            stop.store(true, Ordering::Relaxed);
            if problems.is_empty() {
                println!("schema: passed");
                Ok(())
            } else {
                Err(format!("schema failed:\n  {}", problems.join("\n  ")))
            }
        }
        Command::Migrate { step } => {
            let step = Step::parse(&step).ok_or_else(|| format!("no step `{step}`"))?;
            let mut conn =
                PgConnection::establish(&database_url()?).map_err(|error| error.to_string())?;
            step.run(&mut conn).map_err(|error| error.to_string())?;
            println!("{step:?}: {}", step.expectation());
            Ok(())
        }
    }
}
