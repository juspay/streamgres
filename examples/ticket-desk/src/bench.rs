//! Latency per route, in process: `load_cached` against `.load`.
//!
//! Each thread opens its own connection and runs the lookup
//! `requests / concurrency` times, timing each, after one warm-up read
//! (the subscribe and the fill) that is not counted.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::queries::{Ctx, Lookup, Route, Served};
use crate::stack::Stack;

/// One route's latencies over a run.
#[derive(Debug, Clone)]
pub struct Report {
    pub route: Route,
    pub requests: usize,
    /// How many of them the cache answered.
    pub from_cache: usize,
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

/// Times `requests` reads of `lookup` on `route`, `concurrency` at a time.
pub fn run(
    stack: &Arc<Stack>,
    lookup: &Lookup,
    route: Route,
    concurrency: usize,
    requests: usize,
) -> Result<Report, String> {
    let concurrency = concurrency.max(1);
    let per_thread = requests.div_ceil(concurrency);
    let mut conns = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let mut conn = stack.connect()?;
        lookup
            .run(&mut Ctx::new(&mut conn, &stack.cache, route))
            .map_err(|error| error.to_string())?;
        conns.push(conn);
    }
    let started = Instant::now();
    let threads: Vec<_> = conns
        .into_iter()
        .map(|mut conn| {
            let stack = Arc::clone(stack);
            let lookup = lookup.clone();
            std::thread::spawn(move || -> Result<(Vec<Duration>, usize), String> {
                let mut samples = Vec::with_capacity(per_thread);
                let mut from_cache = 0;
                for _ in 0..per_thread {
                    let at = Instant::now();
                    let mut ctx = Ctx::new(&mut conn, &stack.cache, route);
                    lookup.run(&mut ctx).map_err(|error| error.to_string())?;
                    samples.push(at.elapsed());
                    if ctx.served == Served::Cache {
                        from_cache += 1;
                    }
                }
                Ok((samples, from_cache))
            })
        })
        .collect();
    let mut samples = Vec::with_capacity(requests);
    let mut from_cache = 0;
    for thread in threads {
        let (own, cached) = thread
            .join()
            .map_err(|_| "a bench thread panicked".to_owned())??;
        samples.extend(own);
        from_cache += cached;
    }
    let elapsed = started.elapsed();
    samples.sort();
    let at = |q: f64| {
        let index = ((samples.len().saturating_sub(1)) as f64 * q) as usize;
        samples.get(index).copied().unwrap_or_default()
    };
    Ok(Report {
        route,
        requests: samples.len(),
        from_cache,
        p50: at(0.50),
        p95: at(0.95),
        p99: at(0.99),
        max: samples.last().copied().unwrap_or_default(),
        elapsed,
    })
}
