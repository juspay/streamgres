//! The demo checking itself: every lookup read through `load_cached` and
//! through Postgres, compared, by several readers at once while the
//! writer runs.
//!
//! A cached answer may lag a commit by the engine's propagation delay,
//! so a difference is not a mismatch until it has had time to settle:
//! the pair is re-read until the two agree or `settle` runs out. A pair
//! that agreed on a later read counts as `lagged`; one that never did is
//! a mismatch, kept with both answers. A pair whose cached side came from
//! Postgres (a refused shape) says nothing about the cache: two live
//! reads a moment apart can differ under steady writes, so it counts as
//! `live_disagreed`.
//!
//! Each lookup is also held to who is expected to answer it: a shape
//! expected from the cache that the cache never answered, or one
//! expected from Postgres that the cache did answer, fails the run.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::Value;

use crate::queries::{Ctx, Expected, Lookup, Route, Served};
use crate::stack::Stack;

/// A pair that never agreed.
#[derive(Debug, Clone, Serialize)]
pub struct Mismatch {
    pub cached: Value,
    pub postgres: Value,
}

/// What the run found for one lookup.
#[derive(Debug, Default, Clone, Serialize)]
pub struct Tally {
    pub compared: u64,
    pub from_cache: u64,
    pub from_postgres: u64,
    pub lagged: u64,
    pub live_disagreed: u64,
    pub errors: u64,
    pub mismatches: u64,
    pub first_mismatch: Option<Mismatch>,
    pub max_settle: Duration,
}

/// What a run found, by lookup.
pub type Tallies = BTreeMap<&'static str, Tally>;

/// What one comparison found.
struct Compared {
    cached: Value,
    postgres: Value,
    served: Served,
    lagged: bool,
    took: Duration,
}

/// Reads `lookup` through the cache and through Postgres until the two
/// agree or `settle` runs out.
fn compare(
    stack: &Stack,
    cached_conn: &mut diesel::PgConnection,
    truth_conn: &mut diesel::PgConnection,
    lookup: &Lookup,
    settle: Duration,
) -> Result<Compared, String> {
    let started = Instant::now();
    let mut lagged = false;
    loop {
        let mut ctx = Ctx::new(cached_conn, &stack.cache, Route::Cached);
        let cached = lookup
            .run(&mut ctx)
            .map_err(|error| format!("{}: cached read: {error}", lookup.name()))?;
        let served = ctx.served;
        let mut truth_ctx = Ctx::new(truth_conn, &stack.cache, Route::Postgres);
        let postgres = lookup
            .run(&mut truth_ctx)
            .map_err(|error| format!("{}: postgres read: {error}", lookup.name()))?;
        if cached == postgres || started.elapsed() >= settle {
            return Ok(Compared {
                cached,
                postgres,
                served,
                lagged,
                took: started.elapsed(),
            });
        }
        lagged = true;
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Why the cache refuses each lookup, asked once on a fresh connection;
/// absent for those it answers.
pub fn explain(
    stack: &Stack,
    lookups: &[Lookup],
) -> Result<BTreeMap<&'static str, String>, String> {
    let mut conn = stack.connect()?;
    let mut refusals = BTreeMap::new();
    for lookup in lookups {
        let mut ctx = Ctx::new(&mut conn, &stack.cache, Route::Cached);
        ctx.explain = true;
        lookup
            .run(&mut ctx)
            .map_err(|error| format!("{}: {error}", lookup.name()))?;
        if let Some(reason) = ctx.refusal {
            refusals.insert(lookup.name(), reason);
        }
    }
    Ok(refusals)
}

/// `readers` threads comparing every lookup for `run_for`, each pass
/// `every` apart.
pub fn run(
    stack: &Arc<Stack>,
    lookups: &[Lookup],
    readers: usize,
    run_for: Duration,
    every: Duration,
    settle: Duration,
) -> Result<Tallies, String> {
    let tallies: Arc<Mutex<Tallies>> = Arc::default();
    let deadline = Instant::now() + run_for;
    let mut threads = Vec::new();
    for _ in 0..readers.max(1) {
        let stack = Arc::clone(stack);
        let lookups = lookups.to_vec();
        let tallies = Arc::clone(&tallies);
        let mut cached_conn = stack.connect()?;
        let mut truth_conn = stack.connect()?;
        threads.push(std::thread::spawn(move || {
            while Instant::now() < deadline {
                for lookup in &lookups {
                    let outcome =
                        compare(&stack, &mut cached_conn, &mut truth_conn, lookup, settle);
                    let mut tallies = tallies.lock().unwrap_or_else(|poison| poison.into_inner());
                    let tally = tallies.entry(lookup.name()).or_default();
                    match outcome {
                        Ok(compared) => {
                            tally.compared += 1;
                            match compared.served {
                                Served::Cache => tally.from_cache += 1,
                                Served::Postgres => tally.from_postgres += 1,
                            }
                            if compared.lagged {
                                tally.lagged += 1;
                                tally.max_settle = tally.max_settle.max(compared.took);
                            }
                            let static_moved =
                                lookup.is_static() && compared.cached != Value::Array(Vec::new());
                            if compared.cached == compared.postgres && !static_moved {
                                continue;
                            }
                            if compared.served == Served::Postgres && !static_moved {
                                tally.live_disagreed += 1;
                            } else {
                                tally.mismatches += 1;
                                tally.first_mismatch.get_or_insert(Mismatch {
                                    cached: compared.cached,
                                    postgres: compared.postgres,
                                });
                            }
                        }
                        Err(error) => {
                            tally.errors += 1;
                            eprintln!("verify: {error}");
                        }
                    }
                }
                std::thread::sleep(every);
            }
        }));
    }
    for thread in threads {
        thread
            .join()
            .map_err(|_| "a reader thread panicked".to_owned())?;
    }
    let tallies = tallies.lock().unwrap_or_else(|poison| poison.into_inner());
    Ok(tallies.clone())
}

/// The problems in a run's tallies; none when it passed.
pub fn problems(lookups: &[Lookup], tallies: &Tallies) -> Vec<String> {
    let mut problems = Vec::new();
    for lookup in lookups {
        let name = lookup.name();
        let Some(tally) = tallies.get(name) else {
            problems.push(format!("{name}: never compared"));
            continue;
        };
        if tally.mismatches > 0 {
            problems.push(format!("{name}: {} mismatches", tally.mismatches));
        }
        if tally.errors > 0 {
            problems.push(format!("{name}: {} errors", tally.errors));
        }
        match lookup.expected() {
            Expected::Cache if tally.from_cache == 0 => {
                problems.push(format!(
                    "{name}: expected from the cache, never answered by it"
                ));
            }
            Expected::Postgres if tally.from_cache > 0 => problems.push(format!(
                "{name}: expected from Postgres, answered by the cache {} times",
                tally.from_cache
            )),
            Expected::Cache | Expected::Postgres => {}
        }
    }
    problems
}
