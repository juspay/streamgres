//! What the server measures about itself: how long each stage of the
//! pipeline takes for a committed transaction, from the feed thread
//! decoding it to the last frame of the poke it produced being written,
//! and the counters that say how much work went through. Every stage
//! records into a lock-free histogram; the client side serves the whole
//! picture at `/stats`, so the server's own share of a delivery delay can
//! be read apart from PostgreSQL's and the client's.
//!
//! A histogram has four buckets per power of two, so a percentile is
//! exact to within a fifth of its value, and it costs one atomic add per
//! sample.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value as Json, json};

use crate::ivm::IvmStats;
use crate::sync::SyncStats;

/// How many buckets a histogram has: four per power of two of a `u64`,
/// plus one for zero.
const BUCKETS: usize = 1 + 64 * 4;

/// A distribution of durations in microseconds, four buckets per octave.
pub struct Histogram {
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    sum: AtomicU64,
    max: AtomicU64,
}

/// What a histogram says when read: the count, the mean and the
/// percentiles (each the lower bound of its bucket, in microseconds).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Summary {
    pub count: u64,
    pub mean_us: u64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

impl Default for Histogram {
    /// [`Histogram::new`].
    fn default() -> Self {
        Self::new()
    }
}

impl Histogram {
    /// An empty histogram.
    pub fn new() -> Self {
        Histogram {
            buckets: (0..BUCKETS).map(|_| AtomicU64::new(0)).collect(),
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }

    /// Record one duration.
    pub fn record(&self, elapsed: Duration) {
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.buckets[bucket_of(micros)].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(micros, Ordering::Relaxed);
        self.max.fetch_max(micros, Ordering::Relaxed);
    }

    /// The distribution so far.
    pub fn summary(&self) -> Summary {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return Summary::default();
        }
        let counts: Vec<u64> = self
            .buckets
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .collect();
        let percentile = |fraction: f64| -> u64 {
            let target = ((count as f64) * fraction).ceil().max(1.0) as u64;
            let mut seen = 0u64;
            for (index, bucket) in counts.iter().enumerate() {
                seen += bucket;
                if seen >= target {
                    return lower_bound(index);
                }
            }
            lower_bound(BUCKETS - 1)
        };
        Summary {
            count,
            mean_us: self.sum.load(Ordering::Relaxed) / count,
            p50_us: percentile(0.50),
            p90_us: percentile(0.90),
            p99_us: percentile(0.99),
            max_us: self.max.load(Ordering::Relaxed),
        }
    }

    /// Forget every sample.
    pub fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
        self.count.store(0, Ordering::Relaxed);
        self.sum.store(0, Ordering::Relaxed);
        self.max.store(0, Ordering::Relaxed);
    }
}

/// The bucket of `micros`: zero in the first, then four per octave (the
/// two lowest octaves have one value each and use their first quarter).
fn bucket_of(micros: u64) -> usize {
    if micros == 0 {
        return 0;
    }
    let exponent = 63 - micros.leading_zeros() as usize;
    let quarter = if exponent >= 2 {
        ((micros >> (exponent - 2)) & 3) as usize
    } else {
        0
    };
    1 + exponent * 4 + quarter
}

/// The smallest duration a bucket holds.
fn lower_bound(index: usize) -> u64 {
    if index == 0 {
        return 0;
    }
    let exponent = (index - 1) / 4;
    let quarter = ((index - 1) % 4) as u64;
    if exponent >= 2 {
        (4 + quarter) << (exponent - 2)
    } else {
        1 << exponent
    }
}

/// The engine's own counters, as last published by the service.
#[derive(Default, Clone)]
struct Engine {
    ivm: IvmStats,
    sync: SyncStats,
}

/// Every measurement of the server, shared by the threads that take them.
///
/// Durations, per committed transaction unless said otherwise:
/// - `feed_to_engine`: from the feed thread decoding it to the engine
///   thread taking it up.
/// - `engine_step`: the engine routing it (every write, the reads asked
///   for, the deltas handed out).
/// - `engine_to_groups`: from the engine sending its commit event to a
///   group thread taking it up.
/// - `groups_flush`: one flush of a group thread (every poke it built).
/// - `groups_to_socket`: from a poke being handed to a connection's
///   writer to its last frame written (per poke and connection).
/// - `end_to_end`: from the feed decoding the oldest transaction a poke
///   carries to that poke's last frame written (per poke and connection).
///
/// Durations of the query path, so a hydration's time splits into the
/// application server's, PostgreSQL's and the engine's own:
/// - `transform`: one round trip to the application server for the ASTs
///   of a desired-queries change (per change that named custom queries).
/// - `plan`: translating and planning one query on the connection task
///   (a cache hit is microseconds; a miss counts on the reads pool).
/// - `hydrate_cold` / `hydrate_warm`: from a query's registration being
///   sent to the engine to its first rows all present, per query; cold
///   when the registration issued storage reads, warm when the frames
///   already held answered it.
/// - `register_step`: the engine registering one query (compute).
/// - `unregister_step`: the engine releasing one query or one client's
///   queries, the rows only they held dropped (compute).
/// - `read_io`: one storage read from being issued to its rows being back
///   on the engine thread (the pool's queue, PostgreSQL, decoding).
/// - `land_step`: the engine landing one read's rows (compute).
///
/// Counts: transactions and writes routed, pokes and frames written, rows
/// serialized and rows found already serialized in the same flush.
pub struct Stats {
    started: Instant,
    pub feed_to_engine: Histogram,
    pub engine_step: Histogram,
    pub engine_to_groups: Histogram,
    pub groups_flush: Histogram,
    pub groups_to_socket: Histogram,
    pub end_to_end: Histogram,
    pub transform: Histogram,
    pub plan: Histogram,
    pub hydrate_cold: Histogram,
    pub hydrate_warm: Histogram,
    pub register_step: Histogram,
    pub unregister_step: Histogram,
    pub read_io: Histogram,
    pub land_step: Histogram,
    pub transactions: AtomicU64,
    pub writes: AtomicU64,
    pub pokes: AtomicU64,
    pub frames: AtomicU64,
    pub rows_serialized: AtomicU64,
    pub rows_shared: AtomicU64,
    engine: Mutex<Engine>,
}

impl Default for Stats {
    /// [`Stats::new`].
    fn default() -> Self {
        Self::new()
    }
}

impl Stats {
    /// Fresh measurements, the clock started now.
    pub fn new() -> Self {
        Stats {
            started: Instant::now(),
            feed_to_engine: Histogram::new(),
            engine_step: Histogram::new(),
            engine_to_groups: Histogram::new(),
            groups_flush: Histogram::new(),
            groups_to_socket: Histogram::new(),
            end_to_end: Histogram::new(),
            transform: Histogram::new(),
            plan: Histogram::new(),
            hydrate_cold: Histogram::new(),
            hydrate_warm: Histogram::new(),
            register_step: Histogram::new(),
            unregister_step: Histogram::new(),
            read_io: Histogram::new(),
            land_step: Histogram::new(),
            transactions: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            pokes: AtomicU64::new(0),
            frames: AtomicU64::new(0),
            rows_serialized: AtomicU64::new(0),
            rows_shared: AtomicU64::new(0),
            engine: Mutex::new(Engine::default()),
        }
    }

    /// Fresh measurements behind a shared handle.
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Publish the engine's counters (the service does, once per commit).
    pub fn publish_engine(&self, ivm: &IvmStats, sync: &SyncStats) {
        if let Ok(mut engine) = self.engine.lock() {
            engine.ivm = ivm.clone();
            engine.sync = sync.clone();
        }
    }

    /// Forget every duration recorded so far; the counters stay.
    pub fn reset(&self) {
        for histogram in [
            &self.feed_to_engine,
            &self.engine_step,
            &self.engine_to_groups,
            &self.groups_flush,
            &self.groups_to_socket,
            &self.end_to_end,
            &self.transform,
            &self.plan,
            &self.hydrate_cold,
            &self.hydrate_warm,
            &self.register_step,
            &self.unregister_step,
            &self.read_io,
            &self.land_step,
        ] {
            histogram.reset();
        }
    }

    /// Everything, as the `/stats` endpoint serves it.
    pub fn json(&self) -> Json {
        let summary = |histogram: &Histogram| {
            let s = histogram.summary();
            json!({
                "count": s.count, "mean_us": s.mean_us, "p50_us": s.p50_us,
                "p90_us": s.p90_us, "p99_us": s.p99_us, "max_us": s.max_us,
            })
        };
        let engine = self
            .engine
            .lock()
            .map(|engine| engine.clone())
            .unwrap_or_default();
        json!({
            "uptime_s": self.started.elapsed().as_secs(),
            "stages_us": {
                "feed_to_engine": summary(&self.feed_to_engine),
                "engine_step": summary(&self.engine_step),
                "engine_to_groups": summary(&self.engine_to_groups),
                "groups_flush": summary(&self.groups_flush),
                "groups_to_socket": summary(&self.groups_to_socket),
                "end_to_end": summary(&self.end_to_end),
                "transform": summary(&self.transform),
                "plan": summary(&self.plan),
                "hydrate_cold": summary(&self.hydrate_cold),
                "hydrate_warm": summary(&self.hydrate_warm),
                "register_step": summary(&self.register_step),
                "unregister_step": summary(&self.unregister_step),
                "read_io": summary(&self.read_io),
                "land_step": summary(&self.land_step),
            },
            "counts": {
                "transactions": self.transactions.load(Ordering::Relaxed),
                "writes": self.writes.load(Ordering::Relaxed),
                "pokes": self.pokes.load(Ordering::Relaxed),
                "frames": self.frames.load(Ordering::Relaxed),
                "rows_serialized": self.rows_serialized.load(Ordering::Relaxed),
                "rows_shared": self.rows_shared.load(Ordering::Relaxed),
            },
            "engine": {
                "writes_processed": engine.ivm.writes_processed,
                "queries_registered": engine.ivm.queries_registered,
                "queries_impacted": engine.ivm.queries_impacted,
                "conditions_evaluated": engine.ivm.conditions_evaluated,
                "disjunct_increments": engine.ivm.disjunct_increments,
                "ops_add": engine.ivm.ops_add,
                "ops_delete": engine.ivm.ops_delete,
                "storage_reads": engine.ivm.storage_reads,
                "window_evictions": engine.ivm.window_evictions,
                "window_refills": engine.ivm.window_refills,
                "snapshots_shared": engine.ivm.snapshots_shared,
                "reads_issued": engine.sync.reads_issued,
                "reads_landed": engine.sync.reads_landed,
                "reads_refused": engine.sync.reads_refused,
                "rows_dropped": engine.sync.rows_dropped,
                "rows_refreshed": engine.sync.rows_refreshed,
                "rows_added": engine.sync.rows_added,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Buckets are four per octave and their lower bounds are monotonic,
    /// so a percentile lands within a fifth of the true value.
    #[test]
    fn buckets_are_quarter_octaves() {
        assert_eq!(bucket_of(0), 0);
        assert_eq!(bucket_of(1), 1);
        assert_eq!(bucket_of(2), 5);
        assert_eq!(bucket_of(3), 5);
        assert_eq!(bucket_of(4), 9);
        assert_eq!(bucket_of(5), 10);
        assert_eq!(bucket_of(7), 12);
        assert_eq!(bucket_of(8), 13);
        let used = |index: usize| (index - 1) / 4 >= 2 || (index - 1) % 4 == 0;
        let mut previous = 0;
        for index in (1..BUCKETS).filter(|index| used(*index)) {
            assert!(lower_bound(index) > previous, "at {index}");
            assert_eq!(bucket_of(lower_bound(index)), index, "at {index}");
            previous = lower_bound(index);
        }
        let histogram = Histogram::new();
        for micros in 1..=1000u64 {
            histogram.record(Duration::from_micros(micros));
        }
        let summary = histogram.summary();
        assert_eq!(summary.count, 1000);
        assert!((448..=512).contains(&summary.p50_us), "{summary:?}");
        assert!((896..=1000).contains(&summary.p99_us), "{summary:?}");
        assert_eq!(summary.max_us, 1000);
        histogram.reset();
        assert_eq!(histogram.summary().count, 0);
    }
}
