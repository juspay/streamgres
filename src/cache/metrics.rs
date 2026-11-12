//! What a cache has done and what it holds, for the application's own
//! metrics: counters since the cache started, and gauges read now.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

const SHARDS: usize = 16;
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);
thread_local! { static SHARD: usize = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARDS; }
#[repr(align(128))]
struct Cell(AtomicU64);
/// Per-cache counters spread updates across cache lines. The thread-local
/// index is only a placement hint; no measurements are process-global.
pub(super) struct Counter([Cell; SHARDS]);
impl Default for Counter {
    fn default() -> Self {
        Self(std::array::from_fn(|_| Cell(AtomicU64::new(0))))
    }
}
impl Counter {
    pub fn fetch_add(&self, value: u64, order: Ordering) {
        SHARD.with(|slot| {
            self.0[*slot].0.fetch_add(value, order);
        });
    }
    pub fn load(&self, order: Ordering) -> u64 {
        self.0
            .iter()
            .map(|cell| cell.0.load(order))
            .fold(0, u64::saturating_add)
    }
}

/// The counters, bumped where the things they count happen.
#[derive(Default)]
pub(super) struct Counters {
    pub answered: Counter,
    pub filled: Counter,
    pub unsupported: Counter,
    pub not_ready: Counter,
    pub not_admitted: Counter,
    pub unusable: Counter,
    pub evicted: Counter,
    pub restarts: Counter,
}

impl Counters {
    pub fn bump(counter: &Counter) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// A cache's measurements at one moment ([`super::Cache::metrics`]).
///
/// Counters, since the cache started:
/// - `answered`: reads answered from the cache.
/// - `filled`: reads that initiated or joined a new subscription; its
///   first rows may subsequently arrive or be refused.
/// - `unsupported`: reads refused because the engine cannot answer the
///   query exactly, or not for this reader.
/// - `not_ready`: reads refused because the engine was starting,
///   restarting, behind, or the query's rows were not complete.
/// - `not_admitted`: reads refused because their query had not been read
///   often enough to be kept.
/// - `unusable`: answers an adapter could not hand to its caller, who
///   read the database instead.
/// - `evicted`: queries dropped to keep the cache within its byte budget.
/// - `restarts`: engines started after the first.
///
/// Gauges, now:
/// - `running`: whether an engine is up.
/// - `entries`: queries kept current.
/// - `bytes`: what their rows weigh, as the budget counts them.
/// - `lag`: how stale an answer may be; `None` while no engine is up or
///   it has not yet heard the database.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Metrics {
    pub answered: u64,
    pub filled: u64,
    pub unsupported: u64,
    pub not_ready: u64,
    pub not_admitted: u64,
    pub unusable: u64,
    pub evicted: u64,
    pub restarts: u64,
    pub running: bool,
    pub entries: usize,
    pub bytes: usize,
    pub lag: Option<Duration>,
}

impl Metrics {
    pub(super) fn of(
        counters: &Counters,
        running: bool,
        held: (usize, usize),
        lag: Option<Duration>,
    ) -> Self {
        let read = |counter: &Counter| counter.load(Ordering::Relaxed);
        Metrics {
            answered: read(&counters.answered),
            filled: read(&counters.filled),
            unsupported: read(&counters.unsupported),
            not_ready: read(&counters.not_ready),
            not_admitted: read(&counters.not_admitted),
            unusable: read(&counters.unusable),
            evicted: read(&counters.evicted),
            restarts: read(&counters.restarts),
            running,
            entries: held.0,
            bytes: held.1,
            lag,
        }
    }
}
