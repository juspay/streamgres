//! `example serve | seed | bench`.
//!
//! Environment:
//!
//! | variable               | meaning                                   | default        |
//! |------------------------|-------------------------------------------|----------------|
//! | `DATABASE_URL`         | the Postgres the engine follows           | required       |
//! | `STREAMGRES_S2S_URL`   | `ws://host:port/<base>/s2s/v1`            | required       |
//! | `EXAMPLE_ADDR`         | where `serve` listens                     | `0.0.0.0:8080` |
//! | `EXAMPLE_WRITER_MS`    | the background writer's tick; 0 turns it off | `1000`      |
//!
//! Plus the wrapper's own `STREAMGRES_CACHE_*`, `STREAMGRES_S2S_*` and
//! `STREAMGRES_READ_YOUR_WRITES_MS`.

use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use streamgres_diesel::Route;
use streamgres_example::pools::Stack;
use streamgres_example::queries::Lookup;
use streamgres_example::{api, bench, seed, writer};

#[derive(Parser)]
#[command(name = "example", about = "A ticket desk on the Streamgres Diesel wrapper")]
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

async fn run() -> Result<(), String> {
    match Cli::parse().command {
        Command::Seed => {
            let database_url = env("DATABASE_URL")?;
            let mut conn = Stack::plain_sync(&database_url).map_err(|error| error.to_string())?;
            seed::run(&mut conn).map_err(|error| error.to_string())?;
            println!("seeded: 3 organizations, 12 agents, 180 tickets, 120 comments, 18 attachments");
            Ok(())
        }
        Command::Serve => {
            let stack = Arc::new(Stack::connect(&env("DATABASE_URL")?, &env("STREAMGRES_S2S_URL")?).await?);
            if !stack.wait_for_engine(Duration::from_secs(10)).await {
                eprintln!("example: the engine is not reachable yet; reads go to Postgres until it is");
            }
            let every = std::env::var("EXAMPLE_WRITER_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(1000);
            if every > 0 {
                writer::spawn(stack.plain.clone(), Duration::from_millis(every));
            }
            let addr = std::env::var("EXAMPLE_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
            let listener = tokio::net::TcpListener::bind(&addr).await.map_err(|error| format!("bind {addr}: {error}"))?;
            println!("example: serving on http://{addr} (stats at /stats)");
            axum::serve(listener, api::router(stack)).await.map_err(|error| error.to_string())
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
                    println!("{:<12} {}", lookup.name(), lookup.exercises());
                }
                return Ok(());
            }
            let lookup = Lookup::named(&lookup).ok_or_else(|| format!("no lookup `{lookup}`; see --list"))?;
            let stack = Stack::connect(&env("DATABASE_URL")?, &env("STREAMGRES_S2S_URL")?).await?;
            if !stack.wait_for_engine(Duration::from_secs(10)).await {
                return Err("the engine is not reachable".to_string());
            }
            let routes: Vec<Route> = match route.as_str() {
                "streamgres" => vec![Route::Streamgres],
                "postgres" => vec![Route::Postgres],
                _ => vec![Route::Postgres, Route::Streamgres],
            };
            println!("{} ({}), {concurrency} tasks, {requests} requests per route", lookup.name(), lookup.exercises());
            println!("{:<12} {:>9} {:>9} {:>9} {:>9} {:>10}", "route", "p50", "p95", "p99", "max", "req/s");
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
    }
}

fn micros(duration: Duration) -> String {
    format!("{}µs", duration.as_micros())
}
