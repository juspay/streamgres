//! Latency per route, in process: `load_and_sync` against `.load`.
//!
//! Each route runs the same lookup from `concurrency` tasks for
//! `duration`; the result is throughput and percentiles of the read alone
//! (Diesel decoding included, serialization and pool waits excluded).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;
use streamgres_diesel::Streamgres;
use tokio::task::JoinSet;

use crate::db::Pool;
use crate::queries::{self, Lookup, Route};

/// How one route did.
#[derive(Debug, Clone, Serialize)]
pub struct Run {
    pub route: Route,
    pub requests: u64,
    pub errors: u64,
    /// Requests memory answered.
    pub memory: u64,
    pub requests_per_second: f64,
    pub p50_us: f64,
    pub p99_us: f64,
    pub sample_count: usize,
    pub elapsed_ms: f64,
}

/// Samples kept per task for the percentiles.
const SAMPLES_PER_TASK: usize = 512;

/// Runs `lookup` on `route` from `concurrency` tasks for `duration`, or
/// until `stop`.
pub async fn run(
    lib: &Streamgres,
    pool: &Pool,
    lookup: &Lookup,
    route: Route,
    concurrency: usize,
    duration: Duration,
    stop: Arc<AtomicBool>,
) -> Run {
    let started = Instant::now();
    let deadline = started + duration;
    let mut tasks = JoinSet::new();
    for _ in 0..concurrency.max(1) {
        let (lib, pool, lookup, stop) = (lib.clone(), pool.clone(), lookup.clone(), stop.clone());
        tasks.spawn(async move {
            let (mut requests, mut errors, mut memory) = (0u64, 0u64, 0u64);
            let mut samples = std::collections::VecDeque::with_capacity(SAMPLES_PER_TASK);
            while Instant::now() < deadline && !stop.load(Ordering::Acquire) {
                match queries::read(&lib, &pool, &lookup, route).await {
                    Ok(answer) => {
                        requests += 1;
                        memory += u64::from(answer.served == "cache");
                        if samples.len() == SAMPLES_PER_TASK {
                            samples.pop_front();
                        }
                        samples.push_back(answer.elapsed_us);
                    }
                    Err(_) => {
                        errors += 1;
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
            (requests, errors, memory, samples)
        });
    }
    let (mut requests, mut errors, mut memory, mut samples) = (0, 0, 0, Vec::new());
    while let Some(Ok((n, e, m, own))) = tasks.join_next().await {
        requests += n;
        errors += e;
        memory += m;
        samples.extend(own);
    }
    let elapsed = started.elapsed();
    samples.sort_by(f64::total_cmp);
    let at = |q: usize| {
        samples
            .get(samples.len().saturating_sub(1) * q / 100)
            .copied()
            .unwrap_or(0.0)
    };
    Run {
        route,
        requests,
        errors,
        memory,
        requests_per_second: requests as f64 / elapsed.as_secs_f64().max(1e-6),
        p50_us: at(50),
        p99_us: at(99),
        sample_count: samples.len(),
        elapsed_ms: elapsed.as_secs_f64() * 1e3,
    }
}
