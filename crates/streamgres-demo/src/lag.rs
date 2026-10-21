//! Propagation lag: how long a commit takes to reach the cached answer,
//! under a write rate.
//!
//! A sentinel is written to one ticket through the plain pool, and the
//! cached lookup of that ticket is polled until it carries it; the
//! elapsed time is one sample. Meanwhile a load task cycles the status
//! of random tickets of the second organization at `rate` per second,
//! so the engine has other changes to carry. This is the number people
//! ask about ("how stale is the cache?"), measured instead of estimated.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use diesel::sql_query;
use diesel::sql_types::{BigInt, Text};
use diesel_async::RunQueryDsl;
use rand::Rng;

use crate::pools::Stack;
use crate::queries::Lookup;
use crate::seed;

/// The lag samples, and the load that ran under them.
#[derive(Debug, Clone)]
pub struct Report {
    pub samples: usize,
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub max: Duration,
    pub writes: u64,
    pub elapsed: Duration,
}

impl Report {
    /// The load actually applied, in writes per second.
    pub fn writes_per_second(&self) -> f64 {
        self.writes as f64 / self.elapsed.as_secs_f64().max(1e-9)
    }
}

/// The ticket the sentinel goes to.
const SENTINEL: i64 = 1;

/// Measures for `duration` under `rate` background writes per second.
pub async fn run(stack: &Stack, rate: u32, duration: Duration) -> Result<Report, String> {
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicU64::new(0));
    let load = (rate > 0).then(|| {
        let pool = stack.plain.clone();
        let stop = Arc::clone(&stop);
        let writes = Arc::clone(&writes);
        let per_org = seed::tickets_per_org(seed::scale());
        let every = Duration::from_secs_f64(1.0 / f64::from(rate));
        tokio::spawn(async move {
            let mut next = tokio::time::Instant::now();
            while !AtomicBool::load(&stop, Ordering::Relaxed) {
                next += every;
                tokio::time::sleep_until(next).await;
                let Ok(mut conn) = pool.get().await else {
                    continue;
                };
                let id = per_org + rand::rng().random_range(1..=per_org);
                let done = sql_query(
                    "UPDATE tickets SET status = (CASE status
                        WHEN 'open' THEN 'pending' WHEN 'pending' THEN 'closed' ELSE 'open'
                     END)::ticket_status, updated_at = now() WHERE id = $1",
                )
                .bind::<BigInt, _>(id)
                .execute(&mut conn)
                .await;
                if done.is_ok() {
                    writes.fetch_add(1, Ordering::Relaxed);
                }
            }
        })
    });

    let lookup = Lookup::Ticket { id: SENTINEL };
    let mut reader = stack.main.get().await.map_err(|error| error.to_string())?;
    let mut writer = stack.plain.get().await.map_err(|error| error.to_string())?;
    lookup
        .run_async(&mut reader)
        .await
        .map_err(|error| error.to_string())?;
    let started = Instant::now();
    let mut samples = Vec::new();
    let mut n = 0u64;
    while started.elapsed() < duration {
        n += 1;
        let title = format!("lag-{}-{n}", std::process::id());
        sql_query("UPDATE tickets SET title = $1, updated_at = now() WHERE id = $2")
            .bind::<Text, _>(&title)
            .bind::<BigInt, _>(SENTINEL)
            .execute(&mut writer)
            .await
            .map_err(|error| error.to_string())?;
        let committed = Instant::now();
        loop {
            let rows = lookup
                .run_async(&mut reader)
                .await
                .map_err(|error| error.to_string())?;
            if rows[0]["title"].as_str() == Some(title.as_str()) {
                samples.push(committed.elapsed());
                break;
            }
            if committed.elapsed() > Duration::from_secs(10) {
                return Err(format!("the sentinel {title} never reached the cache"));
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    stop.store(true, Ordering::Relaxed);
    if let Some(load) = load {
        let _ = load.await;
    }
    samples.sort();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
    Ok(Report {
        samples: samples.len(),
        p50: at(0.50),
        p95: at(0.95),
        p99: at(0.99),
        max: *samples.last().unwrap_or(&Duration::ZERO),
        writes: AtomicU64::load(&writes, Ordering::Relaxed),
        elapsed: started.elapsed(),
    })
}
