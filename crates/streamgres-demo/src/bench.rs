//! Latency per route, in process: no HTTP in the way, just the wrapper.
//!
//! Every task takes a pooled connection, pins the route on it, and runs
//! the lookup `requests / concurrency` times, timing each. The first
//! run per route is the warm-up (the subscribe and the fill) and is
//! not counted.

use std::time::{Duration, Instant};

use diesel_streamgres::Route;

use crate::pools::Stack;
use crate::queries::Lookup;

/// One route's latencies over a run.
#[derive(Debug, Clone)]
pub struct Report {
    pub route: Route,
    pub requests: usize,
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub max: Duration,
    pub elapsed: Duration,
}

impl Report {
    /// Requests per second over the run.
    pub fn per_second(&self) -> f64 {
        self.requests as f64 / self.elapsed.as_secs_f64().max(1e-9)
    }
}

/// Times `requests` reads of `lookup` on `route`, `concurrency` at a
/// time, after one warm-up read (the subscribe and the fill) so the
/// measurement is of the steady state on both routes.
pub async fn run(
    stack: &Stack,
    lookup: &Lookup,
    route: Route,
    concurrency: usize,
    requests: usize,
) -> Result<Report, String> {
    let per_task = requests.div_ceil(concurrency.max(1));
    {
        let mut conn = stack.main.get().await.map_err(|error| error.to_string())?;
        let pool_route = conn.route();
        conn.set_route(route);
        let warmed = lookup.run_async(&mut conn).await;
        conn.set_route(pool_route);
        warmed.map_err(|error| error.to_string())?;
    }
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(concurrency);
    for _ in 0..concurrency.max(1) {
        let pool = stack.main.clone();
        let lookup = lookup.clone();
        tasks.push(tokio::spawn(async move {
            let mut samples = Vec::with_capacity(per_task);
            let mut conn = pool.get().await.map_err(|error| error.to_string())?;
            let pool_route = conn.route();
            conn.set_route(route);
            let mut outcome = Ok(());
            for _ in 0..per_task {
                let at = Instant::now();
                if let Err(error) = lookup.run_async(&mut conn).await {
                    outcome = Err(error.to_string());
                    break;
                }
                samples.push(at.elapsed());
            }
            conn.set_route(pool_route);
            outcome.map(|()| samples)
        }));
    }
    let mut samples = Vec::with_capacity(requests);
    for task in tasks {
        samples.extend(task.await.map_err(|error| error.to_string())??);
    }
    let elapsed = started.elapsed();
    samples.sort();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
    Ok(Report {
        route,
        requests: samples.len(),
        p50: at(0.50),
        p95: at(0.95),
        p99: at(0.99),
        max: *samples.last().unwrap_or(&Duration::ZERO),
        elapsed,
    })
}
