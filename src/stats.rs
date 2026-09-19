//! What the server measures about itself: how long each stage of the
//! pipeline takes for a committed transaction, from the feed thread
//! decoding it to the last frame of the poke it produced being written;
//! the query path's stages, so a hydration's time splits into the
//! application server's, PostgreSQL's and the engine's own; the counters
//! that say how much work went through; and the gauges that say what the
//! process holds. Every duration and count is recorded into lock-free
//! atomics by the thread doing the work (one `fetch_add` a sample); the
//! reading, as `/stats` JSON, `/metrics` in Prometheus exposition format,
//! or the periodic summary line, is done elsewhere, from the atomics.
//!
//! A histogram has four buckets per power of two, so a percentile is
//! exact to within a fifth of its value, and it costs one atomic add per
//! sample. `docs/observability.md` is the catalogue.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::{Value as Json, json};

use crate::ivm::{Footprint, IvmStats};
use crate::sync::SyncStats;

/// How many buckets a histogram has: four per power of two of a `u64`,
/// plus one for zero.
const BUCKETS: usize = 1 + 64 * 4;

/// A distribution of durations in microseconds (or of counts), four
/// buckets per octave.
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
        self.record_value(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX));
    }

    /// Record one value (microseconds, or a count for a histogram of counts).
    pub fn record_value(&self, value: u64) {
        self.buckets[bucket_of(value)].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.max.fetch_max(value, Ordering::Relaxed);
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

    /// The count and the sum of every sample so far.
    pub fn count_and_sum(&self) -> (u64, u64) {
        (
            self.count.load(Ordering::Relaxed),
            self.sum.load(Ordering::Relaxed),
        )
    }

    /// How many samples were at most `limit` (a bucket straddling the
    /// limit counts whole, so the answer is exact to within one bucket).
    fn at_most(&self, limit: u64) -> u64 {
        self.buckets
            .iter()
            .enumerate()
            .take_while(|(index, _)| lower_bound(*index) <= limit)
            .map(|(_, bucket)| bucket.load(Ordering::Relaxed))
            .sum()
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

/// The bucket of `value`: zero in the first, then four per octave (the
/// two lowest octaves have one value each and use their first quarter).
fn bucket_of(value: u64) -> usize {
    if value == 0 {
        return 0;
    }
    let exponent = 63 - value.leading_zeros() as usize;
    let quarter = if exponent >= 2 {
        ((value >> (exponent - 2)) & 3) as usize
    } else {
        0
    };
    1 + exponent * 4 + quarter
}

/// The smallest value a bucket holds.
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

/// The engine's own counters and what it holds, as last published by
/// the service.
#[derive(Default, Clone)]
struct Engine {
    ivm: IvmStats,
    sync: SyncStats,
    footprint: Footprint,
}

/// A push awaiting the `lastMutationIDChanges` that acknowledges it.
struct PendingPush {
    mutation: i64,
    at: Instant,
}

/// What only sampling or the group threads can report.
#[derive(Default, Clone)]
struct Sampled {
    thread_cpu: Vec<(String, f64)>,
    groups_inbox: Vec<u64>,
}

static GLOBAL: OnceLock<Arc<Stats>> = OnceLock::new();

/// Every measurement of the server, shared by the threads that take them.
///
/// Durations, per committed transaction unless said otherwise:
/// - `feed_decode`: the feed thread decoding it.
/// - `feed_lag`: from PostgreSQL's commit time to the engine taking it up.
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
/// - `count_io`: one planner count on PostgreSQL.
/// - `hydrate_cold` / `hydrate_warm`: from a query's registration being
///   sent to the engine to its first rows all present, per query; cold
///   when the registration issued storage reads, warm when the frames
///   already held answered it.
/// - `register_step`: the engine registering one query (compute).
/// - `unregister_step`: the engine releasing one query or one client's
///   queries, the rows only they held dropped (compute).
/// - `read_io`: one storage read from being issued to its rows being back
///   on the engine thread (the pool's queue, PostgreSQL, decoding).
/// - `read_rows`: rows per storage read.
/// - `land_step`: the engine landing one read's rows (compute).
///
/// Durations of a mutation: `push`, the application server's round trip,
/// and `mutation_ack`, from the push to the poke that acknowledges it.
///
/// Counts: transactions and writes routed, pokes and frames written, rows
/// serialized and rows found already serialized in the same flush,
/// transforms answered from the cache and not, pushes by outcome,
/// connections opened and closed by reason. Gauges: what is open, held
/// and queued right now.
pub struct Stats {
    started: Instant,
    pub feed_decode: Histogram,
    pub feed_lag: Histogram,
    pub feed_to_engine: Histogram,
    pub engine_step: Histogram,
    pub engine_to_groups: Histogram,
    pub groups_flush: Histogram,
    pub groups_to_socket: Histogram,
    pub end_to_end: Histogram,
    pub transform: Histogram,
    pub plan: Histogram,
    pub count_io: Histogram,
    pub hydrate_cold: Histogram,
    pub hydrate_warm: Histogram,
    pub register_step: Histogram,
    pub unregister_step: Histogram,
    pub read_io: Histogram,
    pub read_rows: Histogram,
    pub land_step: Histogram,
    pub push: Histogram,
    pub mutation_ack: Histogram,
    pub transactions: AtomicU64,
    pub writes: AtomicU64,
    pub pokes: AtomicU64,
    pub frames: AtomicU64,
    pub rows_serialized: AtomicU64,
    pub rows_shared: AtomicU64,
    pub rows_read: AtomicU64,
    pub transform_hits: AtomicU64,
    pub transform_misses: AtomicU64,
    pub transform_errors: AtomicU64,
    pub refused_unsupported: AtomicU64,
    pub refused_plan_limit: AtomicU64,
    pub refused_read_limit: AtomicU64,
    pub refused_other: AtomicU64,
    pub plans_page_driven: AtomicU64,
    pub pushes_ok: AtomicU64,
    pub pushes_failed: AtomicU64,
    pub connections_opened: AtomicU64,
    pub connections_closed_by_client: AtomicU64,
    pub connections_closed_by_error: AtomicU64,
    pub connections_closed_by_server: AtomicU64,
    pub connections_open: AtomicU64,
    pub client_groups: AtomicU64,
    pub clients: AtomicU64,
    pub engine_inbox: AtomicU64,
    pub plan_cache_entries: AtomicU64,
    pub transform_cache_entries: AtomicU64,
    pub warm_shapes: AtomicU64,
    pub feed_lsn: AtomicU64,
    pub feed_last_message_ms: AtomicU64,
    pub process_rss_bytes: AtomicU64,
    engine: Mutex<Engine>,
    pushes: Mutex<HashMap<String, Vec<PendingPush>>>,
    sampled: Mutex<Sampled>,
    refused: Mutex<HashMap<String, RefusedQuery>>,
}

/// One query name the server refused: why (the class and the last reason
/// in full), how often, and when last. The report of the queries the
/// application should rewrite.
#[derive(Debug, Clone)]
pub struct RefusedQuery {
    pub kind: &'static str,
    pub reason: String,
    pub count: u64,
    pub last_ms: u64,
}

/// How many refused query names are kept; past it new names are counted
/// but not listed.
const REFUSED_NAMES: usize = 256;

/// The class of a refusal, from its reason: `unsupported` (something the
/// translation does not express: `LIKE`, `NOT EXISTS`, a compound join
/// key), `plan_limit` (the planner found no side of a join small enough
/// to read), `read_limit` (a read came back larger than the row limit),
/// `other` (a count that failed, an AST that does not parse).
pub fn refusal_kind(reason: &str) -> &'static str {
    if reason.contains("would read more than") {
        "plan_limit"
    } else if reason.contains("returned more than") {
        "read_limit"
    } else if reason.contains("not supported")
        || reason.contains("unknown ")
        || reason.contains("nests deeper")
        || reason.contains("without columns")
    {
        "unsupported"
    } else {
        "other"
    }
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
            feed_decode: Histogram::new(),
            feed_lag: Histogram::new(),
            feed_to_engine: Histogram::new(),
            engine_step: Histogram::new(),
            engine_to_groups: Histogram::new(),
            groups_flush: Histogram::new(),
            groups_to_socket: Histogram::new(),
            end_to_end: Histogram::new(),
            transform: Histogram::new(),
            plan: Histogram::new(),
            count_io: Histogram::new(),
            hydrate_cold: Histogram::new(),
            hydrate_warm: Histogram::new(),
            register_step: Histogram::new(),
            unregister_step: Histogram::new(),
            read_io: Histogram::new(),
            read_rows: Histogram::new(),
            land_step: Histogram::new(),
            push: Histogram::new(),
            mutation_ack: Histogram::new(),
            transactions: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            pokes: AtomicU64::new(0),
            frames: AtomicU64::new(0),
            rows_serialized: AtomicU64::new(0),
            rows_shared: AtomicU64::new(0),
            rows_read: AtomicU64::new(0),
            transform_hits: AtomicU64::new(0),
            transform_misses: AtomicU64::new(0),
            transform_errors: AtomicU64::new(0),
            refused_unsupported: AtomicU64::new(0),
            refused_plan_limit: AtomicU64::new(0),
            refused_read_limit: AtomicU64::new(0),
            refused_other: AtomicU64::new(0),
            plans_page_driven: AtomicU64::new(0),
            pushes_ok: AtomicU64::new(0),
            pushes_failed: AtomicU64::new(0),
            connections_opened: AtomicU64::new(0),
            connections_closed_by_client: AtomicU64::new(0),
            connections_closed_by_error: AtomicU64::new(0),
            connections_closed_by_server: AtomicU64::new(0),
            connections_open: AtomicU64::new(0),
            client_groups: AtomicU64::new(0),
            clients: AtomicU64::new(0),
            engine_inbox: AtomicU64::new(0),
            plan_cache_entries: AtomicU64::new(0),
            transform_cache_entries: AtomicU64::new(0),
            warm_shapes: AtomicU64::new(0),
            feed_lsn: AtomicU64::new(0),
            feed_last_message_ms: AtomicU64::new(0),
            process_rss_bytes: AtomicU64::new(0),
            engine: Mutex::new(Engine::default()),
            pushes: Mutex::new(HashMap::new()),
            sampled: Mutex::new(Sampled::default()),
            refused: Mutex::new(HashMap::new()),
        }
    }

    /// Fresh measurements behind a shared handle.
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Make `stats` the process's measurements, reachable from code that
    /// has no handle (the reads pool, the planner); the first call wins.
    pub fn install(stats: &Arc<Self>) {
        let _ = GLOBAL.set(stats.clone());
    }

    /// The process's measurements, if installed.
    pub fn global() -> Option<&'static Arc<Self>> {
        GLOBAL.get()
    }

    /// Publish the engine's counters (the service does, once per commit).
    pub fn publish_engine(&self, ivm: &IvmStats, sync: &SyncStats) {
        if let Ok(mut engine) = self.engine.lock() {
            engine.ivm = ivm.clone();
            engine.sync = sync.clone();
        }
    }

    /// Publish what the engine holds (the service does, once per commit).
    pub fn publish_footprint(&self, footprint: Footprint) {
        if let Ok(mut engine) = self.engine.lock() {
            engine.footprint = footprint;
        }
    }

    /// Note a push of `mutation` by `client`, awaiting its acknowledgement.
    pub fn push_sent(&self, client: &str, mutation: i64) {
        if let Ok(mut pushes) = self.pushes.lock() {
            let pending = pushes.entry(client.to_owned()).or_default();
            pending.retain(|push| push.at.elapsed() < Duration::from_secs(120));
            pending.push(PendingPush {
                mutation,
                at: Instant::now(),
            });
        }
    }

    /// Note that `client`'s last mutation id reached `lmid`: every push of
    /// a mutation at or below it is acknowledged now.
    pub fn lmid_seen(&self, client: &str, lmid: i64) {
        if let Ok(mut pushes) = self.pushes.lock() {
            let Some(pending) = pushes.get_mut(client) else {
                return;
            };
            let now = Instant::now();
            pending.retain(|push| {
                if push.mutation <= lmid {
                    self.mutation_ack.record(now.duration_since(push.at));
                    false
                } else {
                    true
                }
            });
            if pending.is_empty() {
                pushes.remove(client);
            }
        }
    }

    /// Publish the CPU seconds by thread name the sampler read.
    pub fn publish_thread_cpu(&self, thread_cpu: Vec<(String, f64)>) {
        if let Ok(mut sampled) = self.sampled.lock() {
            sampled.thread_cpu = thread_cpu;
        }
    }

    /// Publish one group thread's inbox depth.
    pub fn set_groups_inbox(&self, shard: usize, depth: u64) {
        if let Ok(mut sampled) = self.sampled.lock() {
            if sampled.groups_inbox.len() <= shard {
                sampled.groups_inbox.resize(shard + 1, 0);
            }
            sampled.groups_inbox[shard] = depth;
        }
    }

    /// Seconds since the measurements started.
    pub fn uptime(&self) -> Duration {
        self.started.elapsed()
    }

    /// The engine's busy time so far: the sum of its steps.
    pub fn engine_busy(&self) -> Duration {
        let micros: u64 = [
            &self.engine_step,
            &self.register_step,
            &self.unregister_step,
            &self.land_step,
        ]
        .iter()
        .map(|histogram| histogram.count_and_sum().1)
        .sum();
        Duration::from_micros(micros)
    }

    /// Forget every duration recorded so far; the counters stay.
    pub fn reset(&self) {
        for (_, histogram) in self.histograms() {
            histogram.reset();
        }
    }

    /// Every histogram with its `/stats` name.
    fn histograms(&self) -> Vec<(&'static str, &Histogram)> {
        vec![
            ("feed_decode", &self.feed_decode),
            ("feed_lag", &self.feed_lag),
            ("feed_to_engine", &self.feed_to_engine),
            ("engine_step", &self.engine_step),
            ("engine_to_groups", &self.engine_to_groups),
            ("groups_flush", &self.groups_flush),
            ("groups_to_socket", &self.groups_to_socket),
            ("end_to_end", &self.end_to_end),
            ("transform", &self.transform),
            ("plan", &self.plan),
            ("count_io", &self.count_io),
            ("hydrate_cold", &self.hydrate_cold),
            ("hydrate_warm", &self.hydrate_warm),
            ("register_step", &self.register_step),
            ("unregister_step", &self.unregister_step),
            ("read_io", &self.read_io),
            ("read_rows", &self.read_rows),
            ("land_step", &self.land_step),
            ("push", &self.push),
            ("mutation_ack", &self.mutation_ack),
        ]
    }

    /// Every counter with its `/stats` name.
    fn counters(&self) -> Vec<(&'static str, u64)> {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        vec![
            ("transactions", load(&self.transactions)),
            ("writes", load(&self.writes)),
            ("pokes", load(&self.pokes)),
            ("frames", load(&self.frames)),
            ("rows_serialized", load(&self.rows_serialized)),
            ("rows_shared", load(&self.rows_shared)),
            ("rows_read", load(&self.rows_read)),
            ("transform_hits", load(&self.transform_hits)),
            ("transform_misses", load(&self.transform_misses)),
            ("transform_errors", load(&self.transform_errors)),
            ("refused_unsupported", load(&self.refused_unsupported)),
            ("refused_plan_limit", load(&self.refused_plan_limit)),
            ("refused_read_limit", load(&self.refused_read_limit)),
            ("refused_other", load(&self.refused_other)),
            ("plans_page_driven", load(&self.plans_page_driven)),
            ("pushes_ok", load(&self.pushes_ok)),
            ("pushes_failed", load(&self.pushes_failed)),
            ("connections_opened", load(&self.connections_opened)),
            (
                "connections_closed_by_client",
                load(&self.connections_closed_by_client),
            ),
            (
                "connections_closed_by_error",
                load(&self.connections_closed_by_error),
            ),
            (
                "connections_closed_by_server",
                load(&self.connections_closed_by_server),
            ),
            ("log_dropped", crate::log::dropped()),
        ]
    }

    /// Every gauge with its `/stats` name.
    fn gauges(&self) -> Vec<(&'static str, u64)> {
        let load = |gauge: &AtomicU64| gauge.load(Ordering::Relaxed);
        let last = load(&self.feed_last_message_ms);
        let heartbeat_age_ms = if last == 0 {
            0
        } else {
            now_ms().saturating_sub(last)
        };
        vec![
            ("connections_open", load(&self.connections_open)),
            ("client_groups", load(&self.client_groups)),
            ("clients", load(&self.clients)),
            ("engine_inbox", load(&self.engine_inbox)),
            ("plan_cache_entries", load(&self.plan_cache_entries)),
            (
                "transform_cache_entries",
                load(&self.transform_cache_entries),
            ),
            ("warm_shapes", load(&self.warm_shapes)),
            ("feed_lsn", load(&self.feed_lsn)),
            ("feed_heartbeat_age_ms", heartbeat_age_ms),
            ("process_rss_bytes", load(&self.process_rss_bytes)),
        ]
    }

    /// Count one refusal of the query `name` for `reason`, returning its
    /// class: the counter of the class, and the per-name report.
    pub fn note_refusal(&self, name: &str, reason: &str) -> &'static str {
        let kind = refusal_kind(reason);
        let counter = match kind {
            "unsupported" => &self.refused_unsupported,
            "plan_limit" => &self.refused_plan_limit,
            "read_limit" => &self.refused_read_limit,
            _ => &self.refused_other,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut refused) = self.refused.lock() {
            let room = refused.len() < REFUSED_NAMES;
            match refused.get_mut(name) {
                Some(entry) => {
                    entry.kind = kind;
                    entry.reason = reason.to_owned();
                    entry.count += 1;
                    entry.last_ms = now_ms();
                }
                None if room => {
                    refused.insert(
                        name.to_owned(),
                        RefusedQuery {
                            kind,
                            reason: reason.to_owned(),
                            count: 1,
                            last_ms: now_ms(),
                        },
                    );
                }
                None => {}
            }
        }
        kind
    }

    /// The refused query names, most refused first.
    pub fn refused_queries(&self) -> Vec<(String, RefusedQuery)> {
        let mut refused: Vec<(String, RefusedQuery)> = self
            .refused
            .lock()
            .map(|refused| {
                refused
                    .iter()
                    .map(|(name, entry)| (name.clone(), entry.clone()))
                    .collect()
            })
            .unwrap_or_default();
        refused.sort_by(|a, b| b.1.count.cmp(&a.1.count).then_with(|| a.0.cmp(&b.0)));
        refused
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
        let sampled = self
            .sampled
            .lock()
            .map(|sampled| sampled.clone())
            .unwrap_or_default();
        let stages: serde_json::Map<String, Json> = self
            .histograms()
            .into_iter()
            .map(|(name, histogram)| (name.to_owned(), summary(histogram)))
            .collect();
        let counts: serde_json::Map<String, Json> = self
            .counters()
            .into_iter()
            .map(|(name, value)| (name.to_owned(), json!(value)))
            .collect();
        let gauges: serde_json::Map<String, Json> = self
            .gauges()
            .into_iter()
            .map(|(name, value)| (name.to_owned(), json!(value)))
            .collect();
        let rows_held: serde_json::Map<String, Json> = engine
            .footprint
            .rows_by_table
            .iter()
            .map(|(table, rows)| (table.clone(), json!(rows)))
            .collect();
        let thread_cpu: serde_json::Map<String, Json> = sampled
            .thread_cpu
            .iter()
            .map(|(name, seconds)| (name.clone(), json!(seconds)))
            .collect();
        let refused: Vec<Json> = self
            .refused_queries()
            .into_iter()
            .map(|(name, entry)| {
                json!({
                    "name": name,
                    "kind": entry.kind,
                    "count": entry.count,
                    "last_ms": entry.last_ms,
                    "reason": entry.reason,
                })
            })
            .collect();
        json!({
            "uptime_s": self.started.elapsed().as_secs(),
            "stages_us": stages,
            "counts": counts,
            "gauges": gauges,
            "engine_busy_s": self.engine_busy().as_secs_f64(),
            "held": {
                "subscriptions": engine.footprint.subscriptions,
                "trees": engine.footprint.trees,
                "rows_by_table": rows_held,
            },
            "threads_cpu_s": thread_cpu,
            "groups_inbox": sampled.groups_inbox,
            "refused_queries": refused,
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
                "window_rejections": engine.ivm.window_rejections,
                "window_capped": engine.ivm.window_capped,
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

    /// Everything, in Prometheus exposition format: durations in seconds
    /// with cumulative buckets (exact to within one of our quarter-octave
    /// buckets), counts as `_total`, the rest as gauges.
    pub fn prometheus(&self) -> String {
        let mut out = String::with_capacity(16 * 1024);
        let engine = self
            .engine
            .lock()
            .map(|engine| engine.clone())
            .unwrap_or_default();
        let sampled = self
            .sampled
            .lock()
            .map(|sampled| sampled.clone())
            .unwrap_or_default();
        let mut typed: Vec<&str> = Vec::new();
        for (name, histogram) in self.histograms() {
            let (metric, seconds) = if name == "read_rows" {
                ("xyne_sync_read_rows", false)
            } else {
                (metric_of(name), true)
            };
            if !typed.contains(&metric) {
                typed.push(metric);
                let _ = writeln!(out, "# HELP {metric} {}", stage_help(name));
                let _ = writeln!(out, "# TYPE {metric} histogram");
            }
            let labels = stage_labels(name);
            let plain = labels.trim_end_matches(',');
            let bounds: &[u64] = if seconds {
                &SECONDS_BOUNDS_US
            } else {
                &COUNT_BOUNDS
            };
            for bound in bounds {
                let le = if seconds {
                    format!("{}", *bound as f64 / 1_000_000.0)
                } else {
                    bound.to_string()
                };
                let _ = writeln!(
                    out,
                    "{metric}_bucket{{{labels}le=\"{le}\"}} {}",
                    histogram.at_most(*bound)
                );
            }
            let (count, sum) = histogram.count_and_sum();
            let _ = writeln!(out, "{metric}_bucket{{{labels}le=\"+Inf\"}} {count}");
            let braced = if plain.is_empty() {
                String::new()
            } else {
                format!("{{{plain}}}")
            };
            if seconds {
                let _ = writeln!(out, "{metric}_sum{braced} {}", sum as f64 / 1_000_000.0);
            } else {
                let _ = writeln!(out, "{metric}_sum{braced} {sum}");
            }
            let _ = writeln!(out, "{metric}_count{braced} {count}");
        }
        for (name, value) in self.counters() {
            let (metric, labels) = counter_name(name);
            if !typed.contains(&metric.as_str()) {
                let _ = writeln!(out, "# TYPE {metric} counter");
            }
            let _ = writeln!(out, "{metric}{labels} {value}");
            typed.push(Box::leak(metric.into_boxed_str()));
        }
        for (name, value) in self.gauges() {
            if name == "feed_heartbeat_age_ms" {
                let _ = writeln!(out, "# TYPE xyne_sync_feed_heartbeat_age_seconds gauge");
                let _ = writeln!(
                    out,
                    "xyne_sync_feed_heartbeat_age_seconds {}",
                    value as f64 / 1000.0
                );
            } else {
                let _ = writeln!(out, "# TYPE xyne_sync_{name} gauge");
                let _ = writeln!(out, "xyne_sync_{name} {value}");
            }
        }
        let _ = writeln!(out, "# TYPE xyne_sync_uptime_seconds gauge");
        let _ = writeln!(
            out,
            "xyne_sync_uptime_seconds {}",
            self.started.elapsed().as_secs()
        );
        let _ = writeln!(out, "# TYPE xyne_sync_engine_busy_seconds_total counter");
        let _ = writeln!(
            out,
            "xyne_sync_engine_busy_seconds_total {}",
            self.engine_busy().as_secs_f64()
        );
        let _ = writeln!(out, "# TYPE xyne_sync_subscriptions gauge");
        let _ = writeln!(
            out,
            "xyne_sync_subscriptions {}",
            engine.footprint.subscriptions
        );
        let _ = writeln!(out, "# TYPE xyne_sync_trees gauge");
        let _ = writeln!(out, "xyne_sync_trees {}", engine.footprint.trees);
        let _ = writeln!(out, "# TYPE xyne_sync_rows_held gauge");
        for (table, rows) in &engine.footprint.rows_by_table {
            let _ = writeln!(out, "xyne_sync_rows_held{{table=\"{table}\"}} {rows}");
        }
        let _ = writeln!(out, "# TYPE xyne_sync_thread_cpu_seconds_total counter");
        for (thread, seconds) in &sampled.thread_cpu {
            let _ = writeln!(
                out,
                "xyne_sync_thread_cpu_seconds_total{{thread=\"{thread}\"}} {seconds}"
            );
        }
        let _ = writeln!(out, "# TYPE xyne_sync_groups_inbox gauge");
        for (shard, depth) in sampled.groups_inbox.iter().enumerate() {
            let _ = writeln!(out, "xyne_sync_groups_inbox{{shard=\"{shard}\"}} {depth}");
        }
        for (name, value) in [
            ("writes_impacting_total", engine.ivm.queries_impacted),
            ("narrowed_reads_total", engine.ivm.storage_reads),
            ("reads_issued_total", engine.sync.reads_issued),
            ("reads_landed_total", engine.sync.reads_landed),
            ("reads_refused_total", engine.sync.reads_refused),
            ("reads_shared_total", engine.ivm.snapshots_shared),
            ("window_refills_total", engine.ivm.window_refills),
            ("page_rows_rejected_total", engine.ivm.window_rejections),
            ("pages_capped_total", engine.ivm.window_capped),
            ("registrations_total", engine.ivm.queries_registered),
            ("client_updates_add_total", engine.ivm.ops_add),
            ("client_updates_delete_total", engine.ivm.ops_delete),
        ] {
            let _ = writeln!(out, "# TYPE xyne_sync_{name} counter");
            let _ = writeln!(out, "xyne_sync_{name} {value}");
        }
        out
    }

    /// One line that says how the server is doing, for the log every
    /// minute: what is open and held, the engine's share of a core since
    /// the previous snapshot, the feed's lag, the caches' hit rates and
    /// the rates of work.
    pub fn summary_line(
        &self,
        previous: &Snapshot,
        since: Duration,
    ) -> (String, Vec<(&'static str, String)>) {
        let now = self.snapshot();
        let secs = since.as_secs_f64().max(0.001);
        let rate = |a: u64, b: u64| (a.saturating_sub(b)) as f64 / secs;
        let engine_busy = (now.engine_busy_us.saturating_sub(previous.engine_busy_us)) as f64
            / 1_000_000.0
            / secs;
        let hits = now.transform_hits.saturating_sub(previous.transform_hits);
        let misses = now
            .transform_misses
            .saturating_sub(previous.transform_misses);
        let transform_hit_rate = if hits + misses == 0 {
            0.0
        } else {
            100.0 * hits as f64 / (hits + misses) as f64
        };
        let engine = self
            .engine
            .lock()
            .map(|engine| engine.clone())
            .unwrap_or_default();
        let feed_lag = self.feed_lag.summary();
        let fields = vec![
            ("connections", now.connections_open.to_string()),
            ("groups", now.client_groups.to_string()),
            ("clients", now.clients.to_string()),
            ("subscriptions", engine.footprint.subscriptions.to_string()),
            ("trees", engine.footprint.trees.to_string()),
            (
                "rows_held",
                engine
                    .footprint
                    .rows_by_table
                    .iter()
                    .map(|(_, rows)| rows)
                    .sum::<u64>()
                    .to_string(),
            ),
            ("rss_mb", (now.rss_bytes / (1024 * 1024)).to_string()),
            ("engine_busy_pct", format!("{:.1}", 100.0 * engine_busy)),
            ("engine_inbox", now.engine_inbox.to_string()),
            (
                "feed_lag_p50_ms",
                format!("{:.1}", feed_lag.p50_us as f64 / 1000.0),
            ),
            (
                "registrations_per_s",
                format!("{:.1}", rate(now.registrations, previous.registrations)),
            ),
            (
                "releases_per_s",
                format!("{:.1}", rate(now.releases, previous.releases)),
            ),
            (
                "transactions_per_s",
                format!("{:.1}", rate(now.transactions, previous.transactions)),
            ),
            (
                "pokes_per_s",
                format!("{:.1}", rate(now.pokes, previous.pokes)),
            ),
            (
                "rows_sent_per_s",
                format!("{:.0}", rate(now.rows_serialized, previous.rows_serialized)),
            ),
            ("transform_hit_pct", format!("{transform_hit_rate:.1}")),
            (
                "pushes",
                now.pushes.saturating_sub(previous.pushes).to_string(),
            ),
            (
                "queries_refused",
                now.refused.saturating_sub(previous.refused).to_string(),
            ),
            (
                "page_rows_rejected",
                engine
                    .ivm
                    .window_rejections
                    .saturating_sub(previous.page_rows_rejected)
                    .to_string(),
            ),
            ("log_dropped", now.log_dropped.to_string()),
        ];
        ("summary".to_owned(), fields)
    }

    /// The counters the summary line differences.
    pub fn snapshot(&self) -> Snapshot {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        Snapshot {
            engine_busy_us: u64::try_from(self.engine_busy().as_micros()).unwrap_or(u64::MAX),
            registrations: self.register_step.count_and_sum().0,
            releases: self.unregister_step.count_and_sum().0,
            transactions: load(&self.transactions),
            pokes: load(&self.pokes),
            rows_serialized: load(&self.rows_serialized),
            transform_hits: load(&self.transform_hits),
            transform_misses: load(&self.transform_misses),
            pushes: load(&self.pushes_ok) + load(&self.pushes_failed),
            connections_open: load(&self.connections_open),
            client_groups: load(&self.client_groups),
            clients: load(&self.clients),
            engine_inbox: load(&self.engine_inbox),
            rss_bytes: load(&self.process_rss_bytes),
            log_dropped: crate::log::dropped(),
            refused: load(&self.refused_unsupported)
                + load(&self.refused_plan_limit)
                + load(&self.refused_read_limit)
                + load(&self.refused_other),
            page_rows_rejected: self
                .engine
                .lock()
                .map(|engine| engine.ivm.window_rejections)
                .unwrap_or(0),
        }
    }
}

/// The counters a summary line is computed from.
#[derive(Debug, Clone, Copy, Default)]
pub struct Snapshot {
    pub engine_busy_us: u64,
    pub registrations: u64,
    pub releases: u64,
    pub transactions: u64,
    pub pokes: u64,
    pub rows_serialized: u64,
    pub transform_hits: u64,
    pub transform_misses: u64,
    pub pushes: u64,
    pub connections_open: u64,
    pub client_groups: u64,
    pub clients: u64,
    pub engine_inbox: u64,
    pub rss_bytes: u64,
    pub log_dropped: u64,
    pub refused: u64,
    pub page_rows_rejected: u64,
}

/// Milliseconds since the epoch.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// The `le` boundaries of a duration histogram, in microseconds.
const SECONDS_BOUNDS_US: [u64; 18] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 5_000_000, 10_000_000, 30_000_000, 60_000_000,
];

/// The `le` boundaries of a histogram of counts.
const COUNT_BOUNDS: [u64; 10] = [1, 10, 50, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000];

/// A stage's Prometheus metric name; stages sharing a metric differ by a
/// label ([`stage_labels`]).
fn metric_of(stage: &str) -> &'static str {
    match stage {
        "feed_decode" => "xyne_sync_feed_decode_seconds",
        "feed_lag" => "xyne_sync_feed_lag_seconds",
        "feed_to_engine" => "xyne_sync_feed_to_engine_seconds",
        "engine_step" | "register_step" | "unregister_step" | "land_step" => {
            "xyne_sync_engine_step_seconds"
        }
        "engine_to_groups" => "xyne_sync_engine_to_groups_seconds",
        "groups_flush" => "xyne_sync_groups_flush_seconds",
        "groups_to_socket" => "xyne_sync_groups_to_socket_seconds",
        "end_to_end" => "xyne_sync_end_to_end_seconds",
        "transform" => "xyne_sync_transform_seconds",
        "plan" => "xyne_sync_plan_seconds",
        "count_io" => "xyne_sync_count_seconds",
        "hydrate_cold" | "hydrate_warm" => "xyne_sync_hydrate_seconds",
        "read_io" => "xyne_sync_read_seconds",
        "push" => "xyne_sync_push_seconds",
        "mutation_ack" => "xyne_sync_mutation_ack_seconds",
        _ => "xyne_sync_unknown_seconds",
    }
}

/// The label that tells apart stages sharing a metric, with its trailing
/// comma, or nothing.
fn stage_labels(stage: &str) -> &'static str {
    match stage {
        "engine_step" => "step=\"write\",",
        "register_step" => "step=\"register\",",
        "unregister_step" => "step=\"unregister\",",
        "land_step" => "step=\"land\",",
        "hydrate_cold" => "kind=\"cold\",",
        "hydrate_warm" => "kind=\"warm\",",
        _ => "",
    }
}

/// One line of help per stage.
fn stage_help(stage: &str) -> &'static str {
    match stage {
        "feed_decode" => "the feed thread decoding one transaction",
        "feed_lag" => "PostgreSQL's commit time to the engine taking the transaction up",
        "feed_to_engine" => "a decoded transaction's wait for the engine thread",
        "engine_step" | "register_step" | "unregister_step" | "land_step" => {
            "the engine's own compute per step"
        }
        "engine_to_groups" => "the engine's commit event's wait for a group thread",
        "groups_flush" => "one flush of a group thread",
        "groups_to_socket" => "a poke's wait from the writer to its last frame written",
        "end_to_end" => "the oldest transaction of a poke to its last frame written",
        "transform" => "one round trip to the application server for ASTs",
        "plan" => "translating and planning one query",
        "count_io" => "one planner count on PostgreSQL",
        "hydrate_cold" | "hydrate_warm" => "a query's registration to its rows present",
        "read_io" => "a storage read from issue to its rows back on the engine thread",
        "read_rows" => "rows per storage read",
        "push" => "the application server's push round trip",
        "mutation_ack" => "a push to the poke acknowledging it",
        _ => "",
    }
}

/// A counter's Prometheus name and labels.
fn counter_name(name: &str) -> (String, &'static str) {
    match name {
        "transform_hits" => ("xyne_sync_transforms_total".to_owned(), "{result=\"hit\"}"),
        "transform_misses" => ("xyne_sync_transforms_total".to_owned(), "{result=\"miss\"}"),
        "transform_errors" => (
            "xyne_sync_transforms_total".to_owned(),
            "{result=\"error\"}",
        ),
        "refused_unsupported" => (
            "xyne_sync_queries_refused_total".to_owned(),
            "{reason=\"unsupported\"}",
        ),
        "refused_plan_limit" => (
            "xyne_sync_queries_refused_total".to_owned(),
            "{reason=\"plan_limit\"}",
        ),
        "refused_read_limit" => (
            "xyne_sync_queries_refused_total".to_owned(),
            "{reason=\"read_limit\"}",
        ),
        "refused_other" => (
            "xyne_sync_queries_refused_total".to_owned(),
            "{reason=\"other\"}",
        ),
        "plans_page_driven" => (
            "xyne_sync_plans_total".to_owned(),
            "{kind=\"page_drives\"}",
        ),
        "pushes_ok" => ("xyne_sync_pushes_total".to_owned(), "{result=\"ok\"}"),
        "pushes_failed" => ("xyne_sync_pushes_total".to_owned(), "{result=\"failed\"}"),
        "connections_opened" => (
            "xyne_sync_connections_total".to_owned(),
            "{event=\"opened\"}",
        ),
        "connections_closed_by_client" => (
            "xyne_sync_connections_total".to_owned(),
            "{event=\"closed\",reason=\"client\"}",
        ),
        "connections_closed_by_error" => (
            "xyne_sync_connections_total".to_owned(),
            "{event=\"closed\",reason=\"error\"}",
        ),
        "connections_closed_by_server" => (
            "xyne_sync_connections_total".to_owned(),
            "{event=\"closed\",reason=\"server\"}",
        ),
        "transactions" => ("xyne_sync_feed_transactions_total".to_owned(), ""),
        "writes" => ("xyne_sync_feed_writes_total".to_owned(), ""),
        other => (format!("xyne_sync_{other}_total"), ""),
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
        assert_eq!(histogram.at_most(1000), 1000);
        assert!(
            (500..=640).contains(&histogram.at_most(500)),
            "{}",
            histogram.at_most(500)
        );
        histogram.reset();
        assert_eq!(histogram.summary().count, 0);
    }

    #[test]
    fn the_exposition_has_cumulative_buckets_counters_and_gauges() {
        let stats = Stats::new();
        for ms in [1u64, 5, 20, 400] {
            stats.hydrate_cold.record(Duration::from_millis(ms));
        }
        stats.read_rows.record_value(75);
        stats.transform_hits.fetch_add(3, Ordering::Relaxed);
        stats.connections_open.store(7, Ordering::Relaxed);
        stats.publish_footprint(Footprint {
            subscriptions: 12,
            trees: 4,
            rows_by_table: vec![("messages".to_owned(), 900)],
        });
        let text = stats.prometheus();
        let line = |needle: &str| {
            text.lines()
                .find(|line| line.starts_with(needle))
                .unwrap_or_else(|| panic!("no line starting with {needle}\n{text}"))
                .to_owned()
        };
        assert_eq!(
            line("xyne_sync_hydrate_seconds_bucket{kind=\"cold\",le=\"0.0025\"}"),
            "xyne_sync_hydrate_seconds_bucket{kind=\"cold\",le=\"0.0025\"} 1"
        );
        assert_eq!(
            line("xyne_sync_hydrate_seconds_bucket{kind=\"cold\",le=\"0.025\"}"),
            "xyne_sync_hydrate_seconds_bucket{kind=\"cold\",le=\"0.025\"} 3"
        );
        assert_eq!(
            line("xyne_sync_hydrate_seconds_bucket{kind=\"cold\",le=\"+Inf\"}"),
            "xyne_sync_hydrate_seconds_bucket{kind=\"cold\",le=\"+Inf\"} 4"
        );
        assert_eq!(
            line("xyne_sync_hydrate_seconds_count{kind=\"cold\"}"),
            "xyne_sync_hydrate_seconds_count{kind=\"cold\"} 4"
        );
        assert_eq!(
            line("xyne_sync_hydrate_seconds_sum{kind=\"cold\"}"),
            "xyne_sync_hydrate_seconds_sum{kind=\"cold\"} 0.426"
        );
        assert_eq!(
            line("xyne_sync_read_rows_bucket{le=\"100\"}"),
            "xyne_sync_read_rows_bucket{le=\"100\"} 1"
        );
        assert_eq!(
            line("xyne_sync_read_rows_bucket{le=\"50\"}"),
            "xyne_sync_read_rows_bucket{le=\"50\"} 0"
        );
        assert_eq!(
            line("xyne_sync_transforms_total{result=\"hit\"}"),
            "xyne_sync_transforms_total{result=\"hit\"} 3"
        );
        assert_eq!(
            line("xyne_sync_connections_open "),
            "xyne_sync_connections_open 7"
        );
        assert_eq!(
            line("xyne_sync_subscriptions "),
            "xyne_sync_subscriptions 12"
        );
        assert_eq!(
            line("xyne_sync_rows_held{table=\"messages\"}"),
            "xyne_sync_rows_held{table=\"messages\"} 900"
        );
        let cumulative: Vec<u64> = text
            .lines()
            .filter(|line| line.starts_with("xyne_sync_hydrate_seconds_bucket{kind=\"cold\""))
            .map(|line| line.rsplit(' ').next().unwrap().parse().unwrap())
            .collect();
        assert!(
            cumulative.windows(2).all(|pair| pair[0] <= pair[1]),
            "{cumulative:?}"
        );
        assert_eq!(
            text.matches("# TYPE xyne_sync_engine_step_seconds histogram")
                .count(),
            1,
            "one TYPE line per metric"
        );
        let json = stats.json();
        assert_eq!(json["held"]["subscriptions"], 12);
        assert_eq!(json["counts"]["transform_hits"], 3);
        assert_eq!(json["gauges"]["connections_open"], 7);
    }

    #[test]
    fn a_push_is_acknowledged_by_the_mutation_id_that_covers_it() {
        let stats = Stats::new();
        stats.push_sent("c1", 3);
        stats.push_sent("c1", 4);
        stats.push_sent("c2", 1);
        stats.lmid_seen("c1", 3);
        assert_eq!(
            stats.mutation_ack.summary().count,
            1,
            "only the mutation at or below the id"
        );
        stats.lmid_seen("c1", 9);
        assert_eq!(stats.mutation_ack.summary().count, 2);
        stats.lmid_seen("c3", 9);
        assert_eq!(
            stats.mutation_ack.summary().count,
            2,
            "an unknown client acknowledges nothing"
        );
        stats.lmid_seen("c2", 1);
        assert_eq!(stats.mutation_ack.summary().count, 3);
        assert!(
            stats.pushes.lock().unwrap().is_empty(),
            "everything acknowledged is forgotten"
        );
    }

    #[test]
    fn the_summary_line_reports_rates_since_the_previous_snapshot() {
        let stats = Stats::new();
        let before = stats.snapshot();
        stats.transactions.fetch_add(20, Ordering::Relaxed);
        stats.transform_hits.fetch_add(3, Ordering::Relaxed);
        stats.transform_misses.fetch_add(1, Ordering::Relaxed);
        stats.engine_step.record(Duration::from_millis(500));
        let (message, fields) = stats.summary_line(&before, Duration::from_secs(10));
        assert_eq!(message, "summary");
        let field = |key: &str| {
            fields
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| v.clone())
                .unwrap()
        };
        assert_eq!(field("transactions_per_s"), "2.0");
        assert_eq!(field("transform_hit_pct"), "75.0");
        assert_eq!(field("engine_busy_pct"), "5.0");
    }
    /// A refusal is classed by its reason, counted under its class, and
    /// reported by query name with its last reason, most refused first.
    #[test]
    fn refusals_are_classed_counted_and_reported_by_name() {
        let stats = Stats::new();
        let plan = "the query would read more than 100000 rows into memory (messages holds more than 100000)";
        let read = "a read on `collection_items` returned more than 100000 rows";
        assert_eq!(stats.note_refusal("bigJoin", plan), "plan_limit");
        assert_eq!(stats.note_refusal("kbRoot", read), "read_limit");
        assert_eq!(stats.note_refusal("kbRoot", read), "read_limit");
        assert_eq!(
            stats.note_refusal("search", "ILIKE is not supported (a condition on `name`)"),
            "unsupported"
        );
        assert_eq!(
            stats.note_refusal("odd", "counting the rows of tickets failed: timeout"),
            "other"
        );
        let refused = stats.refused_queries();
        assert_eq!(refused[0].0, "kbRoot");
        assert_eq!(refused[0].1.count, 2);
        assert_eq!(refused[0].1.kind, "read_limit");
        assert_eq!(refused.len(), 4);
        let text = stats.prometheus();
        assert!(text.contains("xyne_sync_queries_refused_total{reason=\"read_limit\"} 2"));
        assert!(text.contains("xyne_sync_queries_refused_total{reason=\"plan_limit\"} 1"));
        assert!(text.contains("xyne_sync_queries_refused_total{reason=\"unsupported\"} 1"));
        let json = stats.json();
        assert_eq!(json["refused_queries"][0]["name"], "kbRoot");
        assert_eq!(json["refused_queries"][0]["reason"], read);
    }
}
