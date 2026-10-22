//! The demo checking itself: every lookup read through the cache and
//! through Postgres, compared.
//!
//! A cached answer may lag a commit by the engine's propagation delay,
//! so a difference is not a mismatch until it has had time to settle:
//! the pair is re-read until the two agree or `settle` runs out. A pair
//! that agreed on a later read counts as `lagged`; one that never did is
//! a mismatch, kept with both answers. The verify loop (`demo
//! verify`) and `/verify` run this; `/stats` shows the counters.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::Value;

use crate::pools::Stack;
use crate::queries::Lookup;

/// A pair that never agreed.
#[derive(Debug, Clone, Serialize)]
pub struct Mismatch {
    pub lookup: String,
    pub cached: Value,
    pub postgres: Value,
}

/// What one pass found.
#[derive(Debug, Default, Serialize)]
pub struct Outcome {
    pub compared: u64,
    pub lagged: u64,
    pub mismatches: Vec<Mismatch>,
}

/// The counters over every pass so far, and the last mismatch.
#[derive(Default)]
pub struct Counts {
    pub runs: AtomicU64,
    pub compared: AtomicU64,
    pub lagged: AtomicU64,
    pub mismatches: AtomicU64,
    pub errors: AtomicU64,
    pub last: Mutex<Option<Mismatch>>,
}

impl Counts {
    /// The counters as JSON, with the last mismatch's lookup name.
    pub fn json(&self) -> Value {
        let last = self
            .last
            .lock()
            .ok()
            .and_then(|last| last.as_ref().map(|mismatch| mismatch.lookup.clone()));
        serde_json::json!({
            "runs": AtomicU64::load(&self.runs, Ordering::Relaxed),
            "compared": AtomicU64::load(&self.compared, Ordering::Relaxed),
            "lagged": AtomicU64::load(&self.lagged, Ordering::Relaxed),
            "mismatches": AtomicU64::load(&self.mismatches, Ordering::Relaxed),
            "errors": AtomicU64::load(&self.errors, Ordering::Relaxed),
            "last_mismatch": last,
        })
    }

    /// Folds a pass into the counters.
    fn record(&self, outcome: &Outcome) {
        self.runs.fetch_add(1, Ordering::Relaxed);
        self.compared.fetch_add(outcome.compared, Ordering::Relaxed);
        self.lagged.fetch_add(outcome.lagged, Ordering::Relaxed);
        self.mismatches
            .fetch_add(outcome.mismatches.len() as u64, Ordering::Relaxed);
        if let (Some(last), Ok(mut slot)) = (outcome.mismatches.last(), self.last.lock()) {
            *slot = Some(last.clone());
        }
    }
}

/// Reads `lookup` through the cache and through Postgres until the two
/// agree or `settle` runs out: the answers, and whether a later read
/// was needed.
async fn compare(
    stack: &Stack,
    lookup: &Lookup,
    settle: Duration,
) -> Result<(Value, Value, bool), String> {
    let mut cached_conn = stack.main.get().await.map_err(|error| error.to_string())?;
    let mut truth_conn = stack
        .reporting
        .get()
        .await
        .map_err(|error| error.to_string())?;
    let started = Instant::now();
    let mut lagged = false;
    loop {
        let cached = lookup
            .run_async(&mut cached_conn)
            .await
            .map_err(|error| format!("{}: cached read: {error}", lookup.name()))?;
        let truth = lookup
            .run_async(&mut truth_conn)
            .await
            .map_err(|error| format!("{}: postgres read: {error}", lookup.name()))?;
        if cached == truth || started.elapsed() >= settle {
            return Ok((cached, truth, lagged));
        }
        lagged = true;
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// One pass over `lookups`, recorded in the stack's counters.
pub async fn once(stack: &Stack, lookups: &[Lookup], settle: Duration) -> Outcome {
    let mut outcome = Outcome::default();
    for lookup in lookups {
        match compare(stack, lookup, settle).await {
            Ok((cached, postgres, lagged)) => {
                outcome.compared += 1;
                if lagged {
                    outcome.lagged += 1;
                }
                if cached != postgres {
                    outcome.mismatches.push(Mismatch {
                        lookup: lookup.name().to_string(),
                        cached,
                        postgres,
                    });
                }
            }
            Err(error) => {
                stack.verify.errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("verify: {error}");
            }
        }
    }
    stack.verify.record(&outcome);
    outcome
}
