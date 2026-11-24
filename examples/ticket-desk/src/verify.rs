//! The desk checking itself: every lookup read through Streamgres and
//! through Postgres, compared, while the writer runs.
//!
//! An answer from memory may lag a commit by the engine's propagation
//! delay, so a difference is not a mismatch until it has had time to
//! settle: the pair is read again until the two agree or `settle` runs
//! out. A pair that agreed on a later read counts as `lagged`; one that
//! never did is a mismatch, kept with both answers. A pair whose
//! Streamgres side came from Postgres (a shape the engine does not answer)
//! says nothing about memory: two live reads a moment apart can differ
//! under steady writes, so it counts as `live_disagreed`.
//!
//! Each lookup is also held to who is expected to answer it
//! ([`crate::queries::Lookup::expected`]).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::{Value, json};
use streamgres_diesel::Streamgres;

use crate::db::Pool;
use crate::queries::{self, Answer, Expected, Lookup, Route};

/// Both sides of one comparison.
#[derive(Debug, Clone, Serialize)]
pub struct Comparison {
    pub cached: Answer,
    pub postgres: Answer,
    pub equal: bool,
    /// Whether the pair had to be read again to agree.
    pub settled: bool,
    pub observed_ms: f64,
}

/// Reads `lookup` through Streamgres and through Postgres until the two
/// agree, `settle` runs out, or `stop` is set.
pub async fn compare(
    lib: &Streamgres,
    pool: &Pool,
    lookup: &Lookup,
    settle: Duration,
    stop: &AtomicBool,
) -> diesel::QueryResult<Comparison> {
    let started = Instant::now();
    let mut settled = false;
    loop {
        let cached = queries::read(lib, pool, lookup, Route::Streamgres).await?;
        let postgres = queries::read(lib, pool, lookup, Route::Postgres).await?;
        // Refused as unsupported: there are no rows through Streamgres to
        // compare, and re-reading will not change that.
        let equal = cached.served == "unsupported" || cached.data == postgres.data;
        if equal || started.elapsed() >= settle || stop.load(Ordering::Acquire) {
            return Ok(Comparison {
                cached,
                postgres,
                equal,
                settled,
                observed_ms: started.elapsed().as_secs_f64() * 1e3,
            });
        }
        settled = true;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// What the checks found for one lookup.
#[derive(Debug, Default, Clone, Serialize)]
pub struct Tally {
    pub compared: u64,
    pub from_memory: u64,
    pub from_postgres: u64,
    pub unsupported: u64,
    pub lagged: u64,
    pub live_disagreed: u64,
    pub mismatches: u64,
    pub errors: u64,
}

/// What the checks found so far.
#[derive(Debug, Default, Clone, Serialize)]
pub struct Verification {
    pub running: bool,
    pub busy: bool,
    pub comparisons: u64,
    pub matches: u64,
    pub mismatches: u64,
    pub live_disagreements: u64,
    pub errors: u64,
    pub lagged: u64,
    pub from_memory: u64,
    pub from_postgres: u64,
    pub unsupported: u64,
    pub last_mismatch: Option<Value>,
    pub last_error: Option<String>,
    pub by_lookup: BTreeMap<String, Tally>,
}

impl Verification {
    /// Counts one comparison of `lookup`.
    pub fn record(&mut self, lookup: &Lookup, result: diesel::QueryResult<Comparison>) {
        let tally = self.by_lookup.entry(lookup.name().to_owned()).or_default();
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                tally.errors += 1;
                self.errors += 1;
                self.last_error = Some(format!("{}: {error}", lookup.name()));
                return;
            }
        };
        tally.compared += 1;
        self.comparisons += 1;
        // Refused as unsupported: no rows through Streamgres to compare.
        if result.cached.served == "unsupported" {
            tally.unsupported += 1;
            self.unsupported += 1;
            return;
        }
        let memory = result.cached.served == "cache";
        if memory {
            tally.from_memory += 1;
            self.from_memory += 1;
        } else {
            tally.from_postgres += 1;
            self.from_postgres += 1;
        }
        if result.settled {
            tally.lagged += 1;
            self.lagged += 1;
        }
        if result.equal {
            self.matches += 1;
        } else if !memory {
            tally.live_disagreed += 1;
            self.live_disagreements += 1;
        } else {
            tally.mismatches += 1;
            self.mismatches += 1;
            self.last_mismatch = Some(json!({
                "lookup": lookup.name(),
                "cached": result.cached.data,
                "postgres": result.postgres.data,
            }));
        }
    }

    /// What is wrong: a mismatch or an error, a shape expected from memory
    /// that memory never answered, one expected from Postgres that memory
    /// did answer, or a verdict of unsupported that was not expected (or
    /// one expected that was not given).
    pub fn problems(&self, lookups: &[Lookup]) -> Vec<String> {
        let mut problems = Vec::new();
        for lookup in lookups {
            let Some(tally) = self.by_lookup.get(lookup.name()) else {
                continue;
            };
            if tally.mismatches > 0 {
                problems.push(format!(
                    "{}: {} mismatches",
                    lookup.name(),
                    tally.mismatches
                ));
            }
            if tally.errors > 0 {
                problems.push(format!("{}: {} errors", lookup.name(), tally.errors));
            }
            match lookup.expected() {
                Expected::Cache if tally.compared > 0 && tally.from_memory == 0 => {
                    problems.push(format!(
                        "{}: expected from memory, never was",
                        lookup.name()
                    ));
                }
                Expected::Postgres if tally.from_memory > 0 => {
                    problems.push(format!(
                        "{}: expected from Postgres, memory answered",
                        lookup.name()
                    ));
                }
                Expected::Unsupported if tally.unsupported < tally.compared => {
                    problems.push(format!(
                        "{}: expected unsupported, was answered",
                        lookup.name()
                    ));
                }
                Expected::Cache | Expected::Postgres if tally.unsupported > 0 => {
                    problems.push(format!("{}: refused as unsupported", lookup.name()));
                }
                _ => {}
            }
        }
        problems
    }
}

/// One pass over `lookups`, each counted into `into`.
pub async fn pass(
    lib: &Streamgres,
    pool: &Pool,
    lookups: &[Lookup],
    settle: Duration,
    stop: &AtomicBool,
    into: &std::sync::Mutex<Verification>,
) {
    for lookup in lookups {
        if stop.load(Ordering::Acquire) {
            break;
        }
        let result = compare(lib, pool, lookup, settle, stop).await;
        into.lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .record(lookup, result);
    }
}
