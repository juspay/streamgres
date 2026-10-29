//! The engine side of the cache: a client of the Streamgres S2S endpoint.
//!
//! A process holds a small pool of WebSockets to the engine, each on its
//! own thread with its own runtime, and spreads its subscriptions across
//! them. A read that misses asks [`EngineClient::subscribe`], which picks
//! the connection with the fewest subscriptions, sends the query's SQL
//! and bind bytes and waits for the engine's `accepted` or `refused`; the
//! engine's `touched` frames are turned into [`Cache::touched`] calls.
//!
//! A backend subscribes to whatever its code reads, with no screen to
//! bound the set, so the pool is capped: [`Config::max_subscriptions`]
//! refuses the subscribe past the limit and the read is served live,
//! which is the same thing that happens when the engine is down. Both
//! the pool size and the cap come from the environment
//! ([`Config::from_env`]).
//!
//! Every subscription asks for the rows. When the engine's `accepted`
//! carries them and every projected column has a type the client encodes
//! exactly, the rows are kept as a [`Materialized`] store that the cache
//! entry holds ([`crate::cache::Live`]): the engine's `delta` frames
//! update the store in place, and the next read builds the result from
//! it. Postgres is never read for that query, and the socket's thread
//! only moves rows in and out, never builds a result. When a column
//! cannot be encoded (a `numeric`, an interval, a range) the
//! subscription falls back to touches: the cache reads Postgres and the
//! engine's frames mark the entry stale.
//!
//! The engine is pinged while the socket is quiet; a socket that stays
//! silent past [`Config::silence`] is taken for dead (an engine killed without a
//! close, a network gone without a reset), so its keys go stale rather
//! than being served from rows nobody updates any more.
//!
//! The token travels in the upgrade request. Over `ws://` that is plain
//! text: use `wss://` (TLS, with the system's roots) or a TLS-terminating
//! proxy for anything but a loopback or a private network.
//!
//! The wrapper never needs an async context: a subscribe blocks the
//! calling thread on a channel until the reply lands or the wait runs
//! out. When the engine is unreachable every subscribe is refused at
//! once, so an engine outage costs the application nothing but its cache.
//!
//! # Reconnecting
//!
//! Dropping one socket drops the subscriptions the engine held for it.
//! The worker refuses every subscribe waiting on it, drops the cache
//! entries of its keys (a change may land while it is down, and until it
//! is back those keys are served live), and on reconnect resubscribes
//! every key it still holds under the same ids. A rows-mode key refills
//! from the engine's snapshot with no Postgres read; a refill-mode key
//! refills on its next read. Keys the engine then refuses are forgotten.
//! Failed attempts use exponential backoff with jitter: waits range from
//! half a second to one second initially, doubling to a fifteen-to-thirty
//! second range. A greeted connection alive for thirty seconds resets that
//! range; a handshake immediately followed by another disconnect does not.
//! Dropping the pool wakes a worker waiting to reconnect. Restored queries
//! are sent in batches of at most 32 per ten milliseconds per connection,
//! interleaved with incoming frames, pings and outgoing requests.

use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value as Json, json};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::Connector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;

use crate::cache::{Cache, Database, Live};
use crate::key::QueryKey;
use crate::listen::Listening;
use crate::row::OwnedRow;
use crate::materialized::{Materialized, Snapshot};
use crate::subscriptions::{Outcome, Subscriptions, Verdict};
use crate::types::TableTypes;

/// How long a subscribe waits for the engine's verdict before the read is
/// served live instead.
pub const DEFAULT_VERDICT_TIMEOUT: Duration = Duration::from_secs(5);

/// Sockets to the engine per process when the environment says nothing.
pub const DEFAULT_CONNECTIONS: usize = 2;

/// Subscriptions a process may hold across its sockets when the
/// environment says nothing.
pub const DEFAULT_MAX_SUBSCRIPTIONS: usize = 10_000;

/// A stream must have stayed open this long before a later disconnect
/// starts again with the shortest delay. A successful handshake alone
/// does not reset the budget: a server that keeps closing new connections
/// must not make every client retry once a second forever.
const STABLE_CONNECTION: Duration = Duration::from_secs(30);

/// The ceiling doubles from one second to thirty. Each wait is chosen
/// between half that ceiling and the ceiling, so clients spread their
/// attempts without either a tight retry loop or an unbounded recovery wait.
struct Backoff {
    ceiling_ms: u64,
}

impl Backoff {
    /// The first attempt is immediate; this is the delay after its failure.
    fn new() -> Self {
        Self { ceiling_ms: 1_000 }
    }

    /// A fresh per-attempt sample, independent across workers and processes.
    /// It only staggers retries; it is not a cryptographic secret.
    fn next_delay(&mut self) -> Duration {
        self.with_sample(RandomState::new().hash_one(()))
    }

    /// The delay for a sample, kept separate so the bounds can be tested
    /// without sleeping or depending on the random samples a run receives.
    fn with_sample(&mut self, sample: u64) -> Duration {
        let floor = self.ceiling_ms / 2;
        let delay = floor + sample % (self.ceiling_ms - floor + 1);
        self.ceiling_ms = (self.ceiling_ms * 2).min(30_000);
        Duration::from_millis(delay)
    }

    /// A stable connection earns a short retry after its next disconnect.
    fn reset(&mut self) {
        self.ceiling_ms = 1_000;
    }
}

/// How long one frame may take to go out before the socket is given up
/// on: an engine that holds the socket open but stops reading would
/// otherwise block the worker, and its thread, for good.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a socket may stay silent, pings unanswered, before it is
/// taken for dead and its keys go stale, when the environment says
/// nothing. A quiet socket is pinged three times in that time.
pub const DEFAULT_SILENCE: Duration = Duration::from_secs(10);

/// How far behind the database the engine may be, by its own heartbeats,
/// before reads stop trusting the cache, when the environment says
/// nothing.
pub const DEFAULT_MAX_LAG: Duration = Duration::from_secs(2);

/// How far a store's weight may drift from what its cache entry was
/// weighed at, as a share of it (a quarter) and at least this many
/// bytes, before the entry is weighed again.
const REWEIGH_BYTES: usize = 64 * 1024;

/// The pool's shape. [`Config::from_env`] reads it from the environment:
///
/// | variable                            | meaning                            | default |
/// |-------------------------------------|------------------------------------|---------|
/// | `STREAMGRES_S2S_CONNECTIONS`        | sockets to the engine              | 2       |
/// | `STREAMGRES_S2S_MAX_SUBSCRIPTIONS`  | subscriptions held, all sockets    | 10000   |
/// | `STREAMGRES_S2S_VERDICT_TIMEOUT_MS` | wait for `accepted` or `refused`   | 5000    |
/// | `STREAMGRES_S2S_SILENCE_MS`         | a silent socket is taken for dead  | 10000   |
/// | `STREAMGRES_S2S_MAX_LAG_MS`         | the engine's lag the cache allows; 0 trusts it | 2000 |
/// | `STREAMGRES_S2S_TOKEN`              | bearer token the engine asks for   | none    |
///
/// A number that is missing, not a number, or zero leaves the default;
/// an empty token is no token.
#[derive(Clone)]
pub struct Config {
    pub connections: usize,
    pub max_subscriptions: usize,
    pub verdict_timeout: Duration,
    /// How long a socket may stay silent before it is taken for dead.
    pub silence: Duration,
    /// How far behind the database the engine may be before reads go to
    /// Postgres; zero trusts the cache whatever the engine says.
    pub max_lag: Duration,
    /// Sent as `Authorization: Bearer <token>` when each socket opens,
    /// for an engine started with `STREAMGRES_S2S_TOKEN`.
    pub token: Option<String>,
}

impl std::fmt::Debug for Config {
    /// The configuration with its token masked.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("connections", &self.connections)
            .field("max_subscriptions", &self.max_subscriptions)
            .field("verdict_timeout", &self.verdict_timeout)
            .field("silence", &self.silence)
            .field("max_lag", &self.max_lag)
            .field("token", &self.token.as_ref().map(|_| ".."))
            .finish()
    }
}

impl Default for Config {
    /// The documented defaults, with no token.
    fn default() -> Config {
        Config {
            connections: DEFAULT_CONNECTIONS,
            max_subscriptions: DEFAULT_MAX_SUBSCRIPTIONS,
            verdict_timeout: DEFAULT_VERDICT_TIMEOUT,
            silence: DEFAULT_SILENCE,
            max_lag: DEFAULT_MAX_LAG,
            token: None,
        }
    }
}

impl Config {
    /// The pool's shape from the environment (see [`Config`]).
    pub fn from_env() -> Config {
        let defaults = Config::default();
        Config {
            connections: env_number("STREAMGRES_S2S_CONNECTIONS").unwrap_or(defaults.connections),
            max_subscriptions: env_number("STREAMGRES_S2S_MAX_SUBSCRIPTIONS")
                .unwrap_or(defaults.max_subscriptions),
            verdict_timeout: env_number("STREAMGRES_S2S_VERDICT_TIMEOUT_MS")
                .map(|millis| Duration::from_millis(millis as u64))
                .unwrap_or(defaults.verdict_timeout),
            silence: env_number("STREAMGRES_S2S_SILENCE_MS")
                .map(|millis| Duration::from_millis(millis as u64))
                .unwrap_or(defaults.silence),
            max_lag: match std::env::var("STREAMGRES_S2S_MAX_LAG_MS").as_deref().map(str::trim) {
                Ok("0") => Duration::ZERO,
                _ => env_number("STREAMGRES_S2S_MAX_LAG_MS")
                    .map(|millis| Duration::from_millis(millis as u64))
                    .unwrap_or(defaults.max_lag),
            },
            token: std::env::var("STREAMGRES_S2S_TOKEN")
                .ok()
                .filter(|token| !token.is_empty()),
        }
    }
}

/// A positive number from the environment.
fn env_number(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()?
        .trim()
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
}

/// What a connection task is asked to send.
enum Outgoing {
    Subscribe {
        id: u64,
        sql: String,
        binds: Vec<Json>,
    },
    Unsubscribe {
        id: u64,
    },
    /// The pool is gone: close the socket and end the thread.
    Stop,
}

/// What the reader hands a waiting subscribe.
enum Reply {
    Verdict(Verdict),
    /// Accepted with rows: the caller, which has a connection, builds the
    /// store.
    Snapshot(Snapshot),
}

/// What every worker shares: the cache, the types learnt, the fallback
/// log and the one map saying which worker holds which key.
struct Shared {
    /// Which worker holds each key. Also the count against the cap.
    by_key: Mutex<HashMap<QueryKey, usize>>,
    max_subscriptions: usize,
    /// The bearer token each socket opens with, if the engine asks for one.
    token: Option<String>,
    /// How long a socket may stay silent before it is taken for dead.
    silence: Duration,
    /// How far behind the engine may be before reads go to Postgres.
    max_lag: Duration,
    /// Column types by table, learnt on the first rows-mode subscribe of
    /// a table and reused for every delta and resubscribe after; looked
    /// up again when a snapshot names a column they lack, which is how a
    /// column a migration added becomes known.
    types: Mutex<HashMap<String, Arc<TableTypes>>>,
    /// Why the latest subscriptions fell back from rows to refills, most
    /// recent last, at most a few.
    fallbacks: Mutex<Vec<String>>,
    /// Why the latest subscriptions were refused, by the engine or by
    /// this client, most recent last, at most a few.
    refusals: Mutex<Vec<String>>,
    /// The cache this client feeds, once one is attached.
    cache: Mutex<Option<Weak<Cache>>>,
    /// The keys followed for a callback ([`EngineClient::subscribe`]),
    /// not for the cache: their stores never reach it, and their deltas
    /// are handed to the subscription as changes.
    listeners: Mutex<HashMap<QueryKey, Arc<Listening>>>,
    /// Set when the pool is dropped; the worker threads then end.
    stopped: AtomicBool,
    /// Wakes retry waits when the pool is dropped.
    stop: tokio::sync::Notify,
    /// The shared plain-SQL subscriptions ([`EngineClient::watch_sql`]).
    watches: crate::listen::Watches,
    /// Whether the engine was last seen past the lag reads may trust, to
    /// log the crossing each way once.
    was_behind: AtomicBool,
}

impl Shared {
    /// The attached cache, while it lives.
    fn cache(&self) -> Option<Arc<Cache>> {
        self.cache.lock().expect("cache").as_ref()?.upgrade()
    }

    /// The tables of `snapshot` whose column types are not known, or are
    /// known without a column it names: types learnt before a migration
    /// added it.
    fn missing_types(&self, snapshot: &Snapshot) -> Vec<String> {
        let types = self.types.lock().expect("types");
        snapshot
            .tables()
            .filter(|table| {
                types.get(*table).is_none_or(|known| {
                    snapshot
                        .columns_of(table)
                        .any(|column| known.column(column).is_none())
                })
            })
            .map(str::to_string)
            .collect()
    }

    /// The subscription following `key` for a callback, if one does.
    fn listening(&self, key: &QueryKey) -> Option<Arc<Listening>> {
        self.listeners.lock().expect("listeners").get(key).cloned()
    }

    /// Notes why a subscription fell back from rows to refills.
    fn fell_back(&self, reason: String) {
        crate::metrics::note(
            log::Level::Warn,
            &crate::metrics::reason_key("fallback", &reason),
            format!("a query fell back from the engine's rows to Postgres refills: {reason}"),
        );
        crate::metrics::report(|metrics| metrics.fallback(&reason));
        Self::remember(&self.fallbacks, reason);
    }

    /// Notes a refusal and returns it as the verdict.
    fn refused(&self, reason: String) -> Verdict {
        crate::metrics::note(
            log::Level::Info,
            &crate::metrics::reason_key("refused", &reason),
            format!("a query is read from Postgres: {reason}"),
        );
        crate::metrics::report(|metrics| metrics.refused(&reason));
        Self::remember(&self.refusals, reason.clone());
        Verdict::Refused(reason)
    }

    /// The reasons this client refuses on its own, all of which pass.
    const TRANSIENT: [&'static str; 10] = [
        crate::subscriptions::STALE_TYPES,
        "the engine has not said which database it streams",
        "the engine is not connected",
        "the engine client has stopped",
        "the engine did not answer in time",
        "the engine connection dropped",
        "the subscription limit of ",
        "the subscription is pending",
        "the subscription ended before its rows were built",
        "the subscription's rows are being built",
    ];

    /// Appends `reason` to `log`, keeping the last eight.
    fn remember(log: &Mutex<Vec<String>>, reason: String) {
        let mut log = log.lock().expect("log");
        if log.len() >= 8 {
            log.remove(0);
        }
        log.push(reason);
    }

    /// Drops `key` from the routing map if `worker` is the one holding
    /// it.
    fn release(&self, key: &QueryKey, worker: usize) {
        let mut by_key = self.by_key.lock().expect("by_key");
        if by_key.get(key) == Some(&worker) {
            by_key.remove(key);
        }
    }
}

/// What one worker holds, under its lock.
struct WorkerState {
    /// The id the next subscription is sent under; never reused.
    next_id: u64,
    /// Each held key's subscription id.
    by_key: HashMap<QueryKey, u64>,
    /// Each subscription id's key.
    by_id: HashMap<u64, QueryKey>,
    /// Subscriptions the engine has not accepted yet: a subscribe
    /// waiting for its verdict, with where to deliver it, or `None` for
    /// a key resubscribed after a reconnect, which nobody waits for. A
    /// read of a pending key goes live: until the engine accepts, a
    /// commit can land that no frame reports.
    pending: HashMap<u64, Option<SyncSender<Reply>>>,
    /// Subscriptions served from the engine's rows, each behind its own
    /// lock so a delta to one is applied without holding up the rest.
    materialized: HashMap<u64, Arc<LiveStore>>,
    /// The position of the last change the engine will not report for
    /// each subscription, as its `accepted` and `touched` frames name
    /// it: what a refill from a standby must have replayed past.
    since: HashMap<u64, u64>,
}

/// A rows-mode subscription's store, from the moment the engine's
/// snapshot lands.
enum Store {
    /// The snapshot is with the caller, which is looking the column
    /// types up; the deltas that land meanwhile wait here and are
    /// applied once the store is built, so none is lost.
    Building(Vec<Json>),
    Ready(Materialized),
}

/// A rows-mode subscription's store as the worker and the cache entry
/// share it: the worker applies the engine's deltas, a read builds the
/// result.
struct LiveStore {
    id: u64,
    key: QueryKey,
    store: Mutex<Store>,
    worker: Weak<Worker>,
    /// What the store weighed when its cache entry was last weighed.
    weighed: AtomicUsize,
}

impl LiveStore {
    /// Whether the store is still waiting for its rows: a read of its
    /// key then goes to Postgres and fills nothing. A refill would race
    /// the read building it, and could land after the store's entry and
    /// replace it with rows no delta ever updates.
    fn is_building(&self) -> bool {
        matches!(*self.store.lock().expect("store"), Store::Building(_))
    }

    /// An empty store for subscription `id`, waiting for its snapshot.
    fn building(id: u64, key: QueryKey, worker: Weak<Worker>) -> Arc<LiveStore> {
        Arc::new(LiveStore {
            id,
            key,
            store: Mutex::new(Store::Building(Vec::new())),
            worker,
            weighed: AtomicUsize::new(0),
        })
    }

    /// Whether the store, now `bytes`, has drifted from what its entry
    /// was weighed at by more than [`REWEIGH_BYTES`] allows; if so it
    /// counts as weighed at `bytes` from here.
    fn drifted(&self, bytes: usize) -> bool {
        let weighed = self.weighed.load(Ordering::Relaxed);
        if bytes.abs_diff(weighed) > (weighed / 4).max(REWEIGH_BYTES) {
            self.weighed.store(bytes, Ordering::Relaxed);
            true
        } else {
            false
        }
    }
}

impl std::fmt::Debug for LiveStore {
    /// The store by its subscription id; its rows are not printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveStore").field("id", &self.id).finish()
    }
}

impl Live for LiveStore {
    /// Built under the store's lock, which the socket's thread waits on
    /// for at most this one build. A value that cannot be encoded falls
    /// the query back to refills, once the lock is let go.
    fn rows(&self) -> Option<Arc<[OwnedRow]>> {
        let built = match &mut *self.store.lock().expect("store") {
            Store::Ready(materialized) => materialized.rows(),
            Store::Building(_) => return None,
        };
        match built {
            Ok(rows) => Some(rows),
            Err(error) => {
                if let Some(worker) = self.worker.upgrade() {
                    worker.fall_back(self.id, &self.key, error.0);
                }
                None
            }
        }
    }

    /// What the store weighs now; nothing while it is building.
    fn bytes(&self) -> usize {
        match &*self.store.lock().expect("store") {
            Store::Ready(materialized) => materialized.byte_len(),
            Store::Building(_) => 0,
        }
    }
}

/// What an engine's `hello` said of the database it streams.
#[derive(Debug, Clone, PartialEq)]
enum Greeting {
    /// No `hello` on this socket yet.
    Waiting,
    /// A `hello` that names no database: an engine from before it did,
    /// or one that could not read it. Its rows are taken on trust.
    Unnamed,
    /// The database the engine streams.
    Named(Database),
}

/// What a worker connects to.
#[derive(Clone)]
enum Target {
    /// An engine's S2S endpoint, at this URL.
    Remote(String),
    /// An engine in this process.
    #[cfg(feature = "embedded")]
    Local(streamgres::embedded::Engine),
}

impl Target {
    /// The remote engine's URL; `None` for one in this process.
    fn url(&self) -> Option<&str> {
        match self {
            Target::Remote(url) => Some(url),
            #[cfg(feature = "embedded")]
            Target::Local(_) => None,
        }
    }
}

/// One connection to the engine and the subscriptions it carries.
struct Worker {
    index: usize,
    /// The worker itself, for the stores it hands the cache.
    this: Weak<Worker>,
    target: Target,
    verdict_timeout: Duration,
    outgoing: mpsc::UnboundedSender<Outgoing>,
    connected: AtomicBool,
    /// How many keys this worker holds, kept beside the state so the
    /// pool can pick the least loaded worker without locking each one.
    held: AtomicUsize,
    /// The most subscriptions the engine takes on one connection, from
    /// its `hello`; 0 when it named no limit.
    room: AtomicUsize,
    /// Why the latest attempt to connect failed, until one succeeds.
    connect_error: Mutex<Option<String>>,
    /// What the engine on the socket said it streams, from its `hello`.
    greeting: Mutex<Greeting>,
    /// The engine's lag as its latest `lag` frame said, and when that
    /// came; `None` until one has, and while the engine measures none.
    lag: Mutex<Option<(Duration, Instant)>>,
    /// The position up to which every change has reached this worker, as
    /// the engine's latest `lag` frame said (frames arrive in order, so
    /// every change before it has been applied here); 0 until one has,
    /// and while the socket is down.
    position: AtomicU64,
    state: Mutex<WorkerState>,
    shared: Arc<Shared>,
}

/// The workers, and what they share, for the life of the client.
struct Pool {
    workers: Vec<Arc<Worker>>,
    shared: Arc<Shared>,
    config: Config,
}

impl Drop for Pool {
    /// Stops every worker's thread.
    fn drop(&mut self) {
        self.shared.stopped.store(true, Ordering::Release);
        self.shared.stop.notify_waiters();
        for worker in &self.workers {
            let _ = worker.outgoing.send(Outgoing::Stop);
        }
    }
}

/// A client of the engine's S2S endpoint, shared by every connection of a
/// process.
#[derive(Clone)]
pub struct EngineClient {
    pool: Arc<Pool>,
}

impl EngineClient {
    /// A client of the engine the environment names: with the
    /// `embedded` feature and `STREAMGRES_ENGINE=embedded`, one this
    /// process starts, configured from the server's own variables
    /// ([`streamgres::embedded::Config::from_env`]); otherwise the one at
    /// `STREAMGRES_S2S_URL`. Either way the pool's shape is
    /// [`Config::from_env`]'s. Why not, when neither is named.
    pub fn from_env() -> Result<EngineClient, String> {
        let config = Config::from_env();
        let mode = std::env::var("STREAMGRES_ENGINE").unwrap_or_default();
        if mode == "embedded" {
            #[cfg(feature = "embedded")]
            {
                let engine = streamgres::embedded::Engine::try_start(
                    streamgres::embedded::Config::from_env()?,
                )
                .map_err(|error| error.to_string())?;
                return Ok(EngineClient::embedded(engine, config));
            }
            #[cfg(not(feature = "embedded"))]
            return Err(
                "STREAMGRES_ENGINE=embedded needs diesel-streamgres built with its `embedded` feature"
                    .to_owned(),
            );
        }
        let url = std::env::var("STREAMGRES_S2S_URL")
            .map_err(|_| "STREAMGRES_S2S_URL must name the engine, or STREAMGRES_ENGINE=embedded run one in this process".to_owned())?;
        Ok(EngineClient::with_config(url, config))
    }

    /// Starts the pool for `url` (`ws://host:port/<base>/s2s/v1`) with
    /// [`Config::from_env`]. Returns at once; the sockets connect in the
    /// background and subscribes are refused until one has.
    pub fn connect(url: impl Into<String>) -> EngineClient {
        Self::with_config(url, Config::from_env())
    }

    /// [`Config::from_env`] with another verdict timeout.
    pub fn connect_with_timeout(url: impl Into<String>, verdict_timeout: Duration) -> EngineClient {
        Self::with_config(
            url,
            Config {
                verdict_timeout,
                ..Config::from_env()
            },
        )
    }

    /// A client with `config` (zero connections taken as one). Returns at
    /// once, as [`EngineClient::connect`] does.
    pub fn with_config(url: impl Into<String>, config: Config) -> EngineClient {
        Self::with_target(Target::Remote(url.into()), config)
    }

    /// A client of `engine`, an engine this process runs, with `config`
    /// (its token unused): the same frames as over a socket, carried by
    /// channels, and the same cache, routing and checks on top. Returns
    /// at once; each connection waits for the engine to serve.
    #[cfg(feature = "embedded")]
    pub fn embedded(engine: streamgres::embedded::Engine, config: Config) -> EngineClient {
        Self::with_target(Target::Local(engine), config)
    }

    /// A client of `target`, its connections started.
    fn with_target(target: Target, config: Config) -> EngineClient {
        let config = Config {
            connections: config.connections.max(1),
            ..config
        };
        let shared = Arc::new(Shared {
            by_key: Mutex::new(HashMap::new()),
            max_subscriptions: config.max_subscriptions,
            token: config.token.clone(),
            silence: config.silence,
            max_lag: config.max_lag,
            types: Mutex::new(HashMap::new()),
            fallbacks: Mutex::new(Vec::new()),
            refusals: Mutex::new(Vec::new()),
            cache: Mutex::new(None),
            listeners: Mutex::new(HashMap::new()),
            stopped: AtomicBool::new(false),
            stop: tokio::sync::Notify::new(),
            watches: Default::default(),
            was_behind: AtomicBool::new(false),
        });
        let workers = (0..config.connections)
            .map(|index| {
                Worker::start(
                    index,
                    target.clone(),
                    config.verdict_timeout,
                    Arc::clone(&shared),
                )
            })
            .collect();
        EngineClient {
            pool: Arc::new(Pool {
                workers,
                shared,
                config,
            }),
        }
    }

    /// The cache whose entries this client's `touched` frames drop. Set
    /// once the cache exists; it holds the client, so the client holds it
    /// weakly.
    pub fn attach(&self, cache: &Arc<Cache>) {
        *self.pool.shared.cache.lock().expect("cache") = Some(Arc::downgrade(cache));
    }

    /// Follows `key` for `listening` from here on: its rows never go to
    /// the cache, and its changes go to the subscription.
    pub(crate) fn start_listening(&self, key: &QueryKey, listening: Arc<Listening>) {
        self.pool
            .shared
            .listeners
            .lock()
            .expect("listeners")
            .insert(key.clone(), listening);
    }

    /// Stops following `key` for a callback, here and with the engine.
    pub(crate) fn stop_listening(&self, key: &QueryKey) {
        let held = self
            .pool
            .shared
            .listeners
            .lock()
            .expect("listeners")
            .remove(key);
        if held.is_some() {
            self.unsubscribe(key);
        }
    }

    /// The configuration the client runs with.
    pub fn config(&self) -> &Config {
        &self.pool.config
    }

    /// Whether any socket to the engine is up.
    pub fn is_connected(&self) -> bool {
        self.connections() > 0
    }

    /// Wait for startup and a client connection within one deadline.
    pub async fn ready(&self, timeout: Duration) -> Result<(), crate::SubscribeError> {
        tokio::time::timeout(timeout, async {
            #[cfg(feature = "embedded")]
            if let Some(worker) = self.pool.workers.first()
                && let Target::Local(engine) = &worker.target
            {
                engine
                    .ready(timeout)
                    .await
                    .map_err(|error| crate::SubscribeError::Unavailable(error.to_string()))?;
            }
            while !self.pool.workers.iter().any(|worker| {
                worker.connected.load(Ordering::Acquire)
                    && !matches!(
                        *worker.greeting.lock().expect("greeting"),
                        Greeting::Waiting
                    )
            }) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(())
        })
        .await
        .map_err(|_| {
            crate::SubscribeError::Unavailable("timed out waiting for an engine connection".into())
        })?
    }

    /// The engine's own statistics, when it runs in this process
    /// ([`EngineClient::embedded`]): its uptime, how busy its thread is,
    /// and the counters the server would export, for the application's
    /// metrics to read. `None` for an engine over a socket, which exports
    /// its own.
    #[cfg(feature = "embedded")]
    pub fn engine_stats(&self) -> Option<Arc<streamgres::stats::Stats>> {
        self.pool.workers.first().and_then(|worker| match &worker.target {
            Target::Local(engine) => Some(Arc::clone(engine.stats())),
            Target::Remote(_) => None,
        })
    }

    /// The shared plain-SQL subscriptions of this client.
    pub(crate) fn watches(&self) -> &crate::listen::Watches {
        &self.pool.shared.watches
    }

    /// How many of the pool's sockets are up.
    pub fn connections(&self) -> usize {
        self.pool
            .workers
            .iter()
            .filter(|worker| worker.connected.load(Ordering::Acquire))
            .count()
    }

    /// How many subscriptions the engine holds for this client, over
    /// every socket.
    pub fn subscribed(&self) -> usize {
        self.pool.shared.by_key.lock().expect("by_key").len()
    }

    /// How many subscriptions each socket carries, in pool order.
    pub fn per_worker(&self) -> Vec<usize> {
        self.pool
            .workers
            .iter()
            .map(|worker| worker.held.load(Ordering::Relaxed))
            .collect()
    }

    /// What the rows-mode stores weigh, roughly, over every socket.
    pub fn store_bytes(&self) -> usize {
        self.pool
            .workers
            .iter()
            .map(|worker| {
                let stores: Vec<Arc<LiveStore>> = worker
                    .state
                    .lock()
                    .expect("state")
                    .materialized
                    .values()
                    .cloned()
                    .collect();
                stores.iter().map(|store| store.bytes()).sum::<usize>()
            })
            .sum()
    }

    /// How many subscriptions are served from the engine's rows rather
    /// than refilled from Postgres.
    pub fn materialized(&self) -> usize {
        self.pool
            .workers
            .iter()
            .map(|worker| worker.state.lock().expect("state").materialized.len())
            .sum()
    }

    /// What the application should know about how the client is set
    /// up, for its log: a bearer token that would cross the network in
    /// plain text, over `ws://` to another host.
    pub fn warnings(&self) -> Vec<String> {
        let url = self.pool.workers.first().and_then(|worker| worker.target.url());
        match (url, &self.pool.shared.token) {
            (Some(url), Some(_)) if token_in_clear(url) => vec![format!(
                "STREAMGRES_S2S_TOKEN goes to `{url}` in plain text; use wss:// or a TLS-terminating proxy"
            )],
            _ => Vec::new(),
        }
    }

    /// Why recent subscriptions fell back from rows to refills, oldest
    /// first; the last few only.
    pub fn fallbacks(&self) -> Vec<String> {
        self.pool
            .shared
            .fallbacks
            .lock()
            .expect("fallbacks")
            .clone()
    }

    /// Why recent subscriptions were refused, by the engine or by this
    /// client (not connected, over the cap, no answer in time), oldest
    /// first; the last few only.
    pub fn refusals(&self) -> Vec<String> {
        self.pool.shared.refusals.lock().expect("refusals").clone()
    }

    /// The connected worker carrying the fewest subscriptions among those
    /// the engine has room on; else why there is none.
    fn least_loaded(&self) -> Result<&Arc<Worker>, String> {
        let workers = &self.pool.workers;
        let connected = || {
            workers
                .iter()
                .filter(|worker| worker.connected.load(Ordering::Acquire))
        };
        if let Some(worker) = connected()
            .filter(|worker| worker.has_room())
            .min_by_key(|worker| worker.held.load(Ordering::Relaxed))
        {
            return Ok(worker);
        }
        match connected().next() {
            Some(full) => Err(format!(
                "the subscription limit of {} per connection is reached on every socket",
                full.room.load(Ordering::Relaxed)
            )),
            None => Err(workers
                .iter()
                .find(|worker| {
                    worker
                        .connect_error
                        .lock()
                        .expect("connect_error")
                        .is_some()
                })
                .or(workers.first())
                .map_or_else(
                    || "the engine is not connected".to_string(),
                    |worker| worker.not_connected(),
                )),
        }
    }

    /// How far behind the database the engine is, by its heartbeats: the
    /// most any connected socket says, `None` when none says.
    pub fn lag(&self) -> Option<Duration> {
        self.pool.workers.iter().filter_map(|worker| worker.lag()).max()
    }

    /// Why each socket that is down failed to connect, for those that
    /// have tried and failed.
    pub fn connect_errors(&self) -> Vec<String> {
        self.pool
            .workers
            .iter()
            .filter(|worker| !worker.connected.load(Ordering::Acquire))
            .filter_map(|worker| worker.connect_error.lock().expect("connect_error").clone())
            .collect()
    }
}

impl Subscriptions for EngineClient {
    /// Routes the key to the worker already holding it, else the least
    /// loaded one, unless the cap is reached. The key is claimed in the
    /// routing map before the worker is asked, so concurrent subscribes
    /// count against the cap and land on one worker.
    fn subscribe(&self, key: &QueryKey) -> Outcome {
        let shared = &self.pool.shared;
        let worker = {
            let mut by_key = shared.by_key.lock().expect("by_key");
            match by_key.get(key) {
                Some(index) => Arc::clone(&self.pool.workers[*index]),
                None => {
                    if by_key.len() >= shared.max_subscriptions {
                        return Outcome::Verdict(shared.refused(format!(
                            "the subscription limit of {} is reached",
                            shared.max_subscriptions
                        )));
                    }
                    let worker = match self.least_loaded() {
                        Ok(worker) => worker,
                        Err(reason) => return Outcome::Verdict(shared.refused(reason)),
                    };
                    by_key.insert(key.clone(), worker.index);
                    Arc::clone(worker)
                }
            }
        };
        worker.subscribe(key)
    }

    /// Learns the looked-up `types` and builds the key's store on the
    /// worker holding it.
    fn materialize(
        &self,
        key: &QueryKey,
        snapshot: Snapshot,
        types: HashMap<String, Arc<TableTypes>>,
    ) -> Verdict {
        self.pool.shared.types.lock().expect("types").extend(types);
        let worker = self
            .pool
            .shared
            .by_key
            .lock()
            .expect("by_key")
            .get(key)
            .map(|index| Arc::clone(&self.pool.workers[*index]));
        match worker {
            Some(worker) => worker.materialize_key(key, snapshot),
            None => {
                Verdict::Refused("the subscription ended before its rows were built".to_string())
            }
        }
    }

    /// Every refusal but this client's own transient ones.
    fn refusal_is_final(&self, reason: &str) -> bool {
        !Shared::TRANSIENT
            .iter()
            .any(|transient| reason.starts_with(transient))
    }

    /// Whether the engine is further behind the database than
    /// [`Config::max_lag`] allows, on any socket that says how far.
    fn behind(&self) -> bool {
        let max = self.pool.shared.max_lag;
        let lag = self.lag();
        let behind = !max.is_zero() && lag.is_some_and(|lag| lag > max);
        if self.pool.shared.was_behind.swap(behind, Ordering::AcqRel) != behind {
            if behind {
                crate::metrics::note(
                    log::Level::Warn,
                    "behind",
                    format!(
                        "the engine is {:?} behind, past {max:?}: reads go to Postgres until it catches up",
                        lag.unwrap_or_default()
                    ),
                );
            } else {
                crate::metrics::note(
                    log::Level::Info,
                    "caught-up",
                    "the engine caught up: reads are served from the cache again".to_string(),
                );
            }
        }
        behind
    }

    /// The least position every connected socket has received every
    /// change up to; `None` with no socket up, or while one has not
    /// said, as an engine from before positions never does.
    fn position(&self) -> Option<u64> {
        let mut least: Option<u64> = None;
        for worker in &self.pool.workers {
            if !worker.connected.load(Ordering::Acquire) {
                continue;
            }
            match worker.position.load(Ordering::Acquire) {
                0 => return None,
                position => least = Some(least.map_or(position, |least| least.min(position))),
            }
        }
        least
    }

    /// Releases the key from the routing map and its worker.
    fn unsubscribe(&self, key: &QueryKey) {
        let worker = self
            .pool
            .shared
            .by_key
            .lock()
            .expect("by_key")
            .remove(key)
            .map(|index| Arc::clone(&self.pool.workers[index]));
        if let Some(worker) = worker {
            worker.unsubscribe(key);
        }
    }
}

impl Worker {
    /// A worker and the receiving end of its outgoing frames, not yet
    /// running.
    fn new(
        index: usize,
        target: Target,
        verdict_timeout: Duration,
        shared: Arc<Shared>,
    ) -> (Arc<Worker>, mpsc::UnboundedReceiver<Outgoing>) {
        let (outgoing, outgoing_rx) = mpsc::unbounded_channel();
        let worker = Arc::new_cyclic(|this| Worker {
            index,
            this: this.clone(),
            target,
            verdict_timeout,
            outgoing,
            connected: AtomicBool::new(false),
            held: AtomicUsize::new(0),
            room: AtomicUsize::new(0),
            connect_error: Mutex::new(None),
            greeting: Mutex::new(Greeting::Waiting),
            lag: Mutex::new(None),
            position: AtomicU64::new(0),
            state: Mutex::new(WorkerState {
                next_id: 1,
                by_key: HashMap::new(),
                by_id: HashMap::new(),
                pending: HashMap::new(),
                materialized: HashMap::new(),
                since: HashMap::new(),
            }),
            shared,
        });
        (worker, outgoing_rx)
    }

    /// A worker running its connection on a thread of its own.
    fn start(
        index: usize,
        target: Target,
        verdict_timeout: Duration,
        shared: Arc<Shared>,
    ) -> Arc<Worker> {
        let (worker, outgoing_rx) = Worker::new(index, target, verdict_timeout, shared);
        let task = Arc::clone(&worker);
        thread::Builder::new()
            .name(format!("streamgres-engine-{index}"))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("engine client runtime");
                runtime.block_on(connection_loop(task, outgoing_rx));
            })
            .expect("engine client thread");
        worker
    }

    /// Why this socket's engine must not be asked for `key`'s rows: it has
    /// not greeted yet, or it streams another database than the cache's
    /// reads go to (or, for a key followed for a callback, than the
    /// subscription's), whose rows would be served as this one's.
    fn wrong_database(&self, key: &QueryKey) -> Option<String> {
        let greeting = self.greeting.lock().expect("greeting").clone();
        let ours = match self.shared.listening(key) {
            Some(listening) => Some(listening.database.clone()),
            None => self.shared.cache().and_then(|cache| cache.database().cloned()),
        };
        match (greeting, ours.as_ref()) {
            (Greeting::Waiting, _) => Some("the engine has not said which database it streams".to_string()),
            (Greeting::Named(theirs), Some(ours)) if theirs != *ours => Some(format!(
                "the engine streams database `{}` of system {}, but reads here go to `{}` of system {}",
                theirs.name, theirs.system, ours.name, ours.system
            )),
            _ => None,
        }
    }

    /// Subscribes `key` on this worker's socket and waits for the
    /// engine's verdict.
    ///
    /// While the socket is down, a key this worker holds stays routed to
    /// it, to be resubscribed when the socket is back; a new one is
    /// released for another worker or another time. A held key whose
    /// verdict another read is waiting for is refused for now: accepting
    /// would fill an entry that a refusal or a timeout then leaves
    /// without a subscription, so this read goes live and the next one
    /// finds the answer. A key already subscribed (a refill after a
    /// touch) gets the rows if its store has them.
    ///
    /// The reader thread puts a `Building` store in place before it
    /// hands a snapshot over, so every delta from that moment on queues
    /// there while the snapshot's types are looked up.
    fn subscribe(&self, key: &QueryKey) -> Outcome {
        let (reply, verdict) = sync_channel(1);
        let id = {
            let mut state = self.state.lock().expect("state");
            let held = state.by_key.get(key).copied();
            if !self.connected.load(Ordering::Acquire) {
                drop(state);
                if held.is_none() {
                    self.shared.release(key, self.index);
                }
                return Outcome::Verdict(self.shared.refused(self.not_connected()));
            }
            if let Some(reason) = self.wrong_database(key) {
                drop(state);
                if held.is_none() {
                    self.shared.release(key, self.index);
                }
                return Outcome::Verdict(self.shared.refused(reason));
            }
            if let Some(id) = held {
                if state.pending.contains_key(&id) {
                    return Outcome::Verdict(
                        self.shared
                            .refused("the subscription is pending".to_string()),
                    );
                }
                let store = state.materialized.get(&id).cloned();
                if store.as_ref().is_some_and(|store| store.is_building()) {
                    return Outcome::Verdict(
                        self.shared
                            .refused("the subscription's rows are being built".to_string()),
                    );
                }
                let since = state.since.get(&id).copied();
                drop(state);
                return Outcome::Verdict(held_rows(store, since));
            }
            let id = state.next_id;
            state.next_id += 1;
            state.by_key.insert(key.clone(), id);
            state.by_id.insert(id, key.clone());
            state.pending.insert(id, Some(reply));
            self.note_held(&state);
            id
        };
        if self
            .outgoing
            .send(Outgoing::Subscribe {
                id,
                sql: key.sql.clone(),
                binds: key.binds.iter().map(bind_json).collect(),
            })
            .is_err()
        {
            self.forget(id);
            return Outcome::Verdict(
                self.shared
                    .refused("the engine client has stopped".to_string()),
            );
        }
        match verdict.recv_timeout(self.verdict_timeout) {
            Ok(Reply::Verdict(verdict)) => Outcome::Verdict(verdict),
            Ok(Reply::Snapshot(snapshot)) => {
                let missing = self.shared.missing_types(&snapshot);
                if missing.is_empty() {
                    Outcome::Verdict(self.materialize(id, snapshot))
                } else {
                    Outcome::NeedsTypes { snapshot, missing }
                }
            }
            Err(_) => {
                self.forget(id);
                let _ = self.outgoing.send(Outgoing::Unsubscribe { id });
                Outcome::Verdict(
                    self.shared
                        .refused("the engine did not answer in time".to_string()),
                )
            }
        }
    }

    /// The second step of a subscribe, once the caller has looked the
    /// missing types up and put them in the shared map.
    fn materialize_key(&self, key: &QueryKey, snapshot: Snapshot) -> Verdict {
        let id = self.state.lock().expect("state").by_key.get(key).copied();
        match id {
            Some(id) => self.materialize(id, snapshot),
            None => {
                Verdict::Refused("the subscription ended before its rows were built".to_string())
            }
        }
    }

    /// Ends the key's subscription on this worker and with the engine.
    fn unsubscribe(&self, key: &QueryKey) {
        let id = {
            let mut state = self.state.lock().expect("state");
            let Some(id) = state.by_key.get(key).copied() else {
                return;
            };
            self.drop_id(&mut state, id);
            id
        };
        let _ = self.outgoing.send(Outgoing::Unsubscribe { id });
    }

    /// Removes a subscription from the worker's maps, under the state
    /// lock the caller holds, and fails any rows in flight for its key
    /// in the cache: an engine delta or snapshot that found the key held
    /// a moment ago must not land after this.
    fn drop_id(&self, state: &mut WorkerState, id: u64) -> Option<QueryKey> {
        let key = state.by_id.remove(&id);
        if let Some(key) = &key {
            state.by_key.remove(key);
            if let Some(cache) = self.shared.cache() {
                cache.bump_generation(key);
            }
        }
        state.pending.remove(&id);
        state.materialized.remove(&id);
        state.since.remove(&id);
        self.note_held(state);
        key
    }

    /// Builds the store for an accepted subscription from its snapshot
    /// and the types already known, applies the deltas that landed
    /// while the types were looked up, and installs it. Falls back to
    /// touches when a table's types are not known, the rows cannot be
    /// encoded, or the context the subscription is read in may not read
    /// one of its tables whole ([`Worker::unreadable`]). A subscription
    /// that ended meanwhile gets nothing.
    ///
    /// The store is transitioned in place, under the state lock and its
    /// own: the one `Arc` lives for the subscription, so a delta handler
    /// that took a clone of it before this point finds it `Ready` and
    /// applies to the live store, and no delta lands between taking the
    /// queue and installing the rows.
    fn materialize(&self, id: u64, snapshot: Snapshot) -> Verdict {
        self.build(id, snapshot, None)
    }

    /// [`Worker::materialize`], into the store `expected` when one is
    /// named: a snapshot whose types were looked up off the socket's
    /// thread is built only if the store it was accepted into is still
    /// the one held, not after the socket dropped and came back.
    ///
    /// A key followed for a callback ([`Shared::listeners`]) is read
    /// whole, as its subscription's own code reads the engine's rows; its
    /// store gives the subscription its first rows, or its changes since
    /// the socket dropped, and the deltas queued meanwhile as changes.
    fn build(&self, id: u64, snapshot: Snapshot, expected: Option<&Arc<LiveStore>>) -> Verdict {
        let shared = &self.shared;
        let key = self.state.lock().expect("state").by_id.get(&id).cloned();
        let listening = key.as_ref().and_then(|key| shared.listening(key));
        let fallback = |reason: String| {
            let mut state = self.state.lock().expect("state");
            state.materialized.remove(&id);
            let since = state.since.get(&id).copied();
            drop(state);
            if let Some(listening) = &listening {
                listening.end(format!("the engine no longer serves the query's rows: {reason}"));
            }
            shared.fell_back(reason);
            Verdict::Accepted { since }
        };
        if listening.is_none()
            && let Some(table) = self.unreadable(id, &snapshot)
        {
            return fallback(format!(
                "the reading role may not read `{table}` whole, so its rows come from Postgres"
            ));
        }
        let types: HashMap<String, Arc<TableTypes>> = {
            let known = shared.types.lock().expect("types");
            let mut types = HashMap::new();
            for table in snapshot.tables() {
                match known.get(table) {
                    Some(found) => {
                        types.insert(table.to_string(), Arc::clone(found));
                    }
                    None => {
                        drop(known);
                        return fallback(format!("types of `{table}` unavailable"));
                    }
                }
            }
            types
        };
        let mut materialized = match Materialized::new(snapshot, &types) {
            Ok(materialized) => materialized,
            Err(error) => return fallback(error.0),
        };
        let mut state = self.state.lock().expect("state");
        let Some(key) = state.by_id.get(&id).cloned() else {
            state.materialized.remove(&id);
            return Verdict::Refused(
                "the subscription ended before its rows were built".to_string(),
            );
        };
        if let Some(expected) = expected
            && !state
                .materialized
                .get(&id)
                .is_some_and(|held| Arc::ptr_eq(held, expected))
        {
            return Verdict::Refused("the engine connection dropped".to_string());
        }
        let live = Arc::clone(
            state
                .materialized
                .entry(id)
                .or_insert_with(|| LiveStore::building(id, key, self.this.clone())),
        );
        let mut store = live.store.lock().expect("store");
        let queued = match &mut *store {
            Store::Building(queued) => std::mem::take(queued),
            Store::Ready(_) => Vec::new(),
        };
        if let Some(listening) = &listening {
            if let Err(reason) = listening.built(&materialized) {
                drop(store);
                drop(state);
                return fallback(reason);
            }
            for body in queued {
                listening.apply(&mut materialized, body, apply_delta);
            }
            live.weighed
                .store(materialized.byte_len(), Ordering::Relaxed);
            *store = Store::Ready(materialized);
            drop(store);
            return Verdict::Rows {
                rows: Arc::from(Vec::new()),
                live,
            };
        }
        for body in queued {
            apply_delta(&mut materialized, body);
        }
        match materialized.rows() {
            Ok(rows) => {
                live.weighed
                    .store(materialized.byte_len(), Ordering::Relaxed);
                *store = Store::Ready(materialized);
                drop(store);
                Verdict::Rows { rows, live }
            }
            Err(error) => {
                drop(store);
                state.materialized.remove(&id);
                let since = state.since.get(&id).copied();
                drop(state);
                shared.fell_back(error.0);
                Verdict::Accepted { since }
            }
        }
    }

    /// A table of `snapshot` that the context subscription `id` is read in
    /// may not read whole ([`crate::cache::Context::reads_whole`]); every
    /// table, for a context no longer current (retired when its
    /// privileges changed). The engine read its rows with its own role;
    /// served to a reader whose role may not read them, they would hand
    /// over what Postgres would refuse. Its reads refill from Postgres on
    /// the reader's connection instead, where the role's privileges apply.
    fn unreadable(&self, id: u64, snapshot: &Snapshot) -> Option<String> {
        let key = self.state.lock().expect("state").by_id.get(&id).cloned()?;
        let cache = self.shared.cache()?;
        let context = cache.context(key.context);
        snapshot
            .tables()
            .find(|table| context.as_ref().is_none_or(|context| !context.reads_whole(table)))
            .map(str::to_owned)
    }

    /// Drops a subscription from this worker and from the routing map.
    fn forget(&self, id: u64) {
        let key = {
            let mut state = self.state.lock().expect("state");
            self.drop_id(&mut state, id)
        };
        if let Some(key) = key {
            self.shared.release(&key, self.index);
        }
    }

    /// A rows-mode subscription's rows can no longer be served (a value
    /// arrived that cannot be encoded): its store goes, and its key is
    /// stale, so the next read refills from Postgres and the engine's
    /// frames touch it from then on.
    fn fall_back(&self, id: u64, key: &QueryKey, reason: String) {
        self.state.lock().expect("state").materialized.remove(&id);
        self.shared.fell_back(reason);
        if let Some(cache) = self.shared.cache() {
            cache.touched(key);
        }
    }

    /// Publishes how many keys this worker holds, for the pool's choice.
    fn note_held(&self, state: &WorkerState) {
        self.held.store(state.by_key.len(), Ordering::Relaxed);
    }

    /// Whether the engine takes another subscription on this socket: it
    /// counts the ones still registering too, as `held` does.
    fn has_room(&self) -> bool {
        let room = self.room.load(Ordering::Relaxed);
        room == 0 || self.held.load(Ordering::Relaxed) < room
    }

    /// The refusal while this socket is down, with why it last failed to
    /// connect when it has.
    fn not_connected(&self) -> String {
        match &*self.connect_error.lock().expect("connect_error") {
            Some(error) => format!("the engine is not connected: {error}"),
            None => "the engine is not connected".to_string(),
        }
    }

    /// The socket is up: the worker counts as connected, and returns what
    /// it still holds, to resubscribe under the same ids. Their cache
    /// entries went when the socket did, and each is pending again until
    /// the engine accepts it, so nothing stale is served meanwhile.
    fn up(&self) -> Vec<(u64, QueryKey)> {
        crate::metrics::note(
            log::Level::Info,
            &format!("up:{}", self.index),
            format!("engine socket {} is up", self.index),
        );
        crate::metrics::report(|metrics| metrics.connection(self.index, true));
        let mut state = self.state.lock().expect("state");
        self.connected.store(true, Ordering::Release);
        let held: Vec<(u64, QueryKey)> = state
            .by_id
            .iter()
            .map(|(id, key)| (*id, key.clone()))
            .collect();
        for (id, _) in &held {
            state.pending.insert(*id, None);
        }
        held
    }

    /// How far behind the engine on this socket is: what it last said,
    /// and the time since it said it, so an engine that stops saying
    /// anything (frozen, or its frames held up) counts as falling further
    /// behind; `None` when it has said nothing measurable.
    fn lag(&self) -> Option<Duration> {
        if !self.connected.load(Ordering::Acquire) {
            return None;
        }
        let (lag, heard) = (*self.lag.lock().expect("lag"))?;
        Some(lag + heard.elapsed())
    }

    /// Records why the latest connect failed, or that one succeeded.
    fn connect_failed(&self, error: Option<String>) {
        if let Some(error) = &error {
            crate::metrics::note(
                log::Level::Warn,
                &format!("connect:{}", self.index),
                format!("engine socket {} could not connect: {error}", self.index),
            );
        }
        *self.connect_error.lock().expect("connect_error") = error;
    }
}

/// The verdict for a key already subscribed: its store's rows when it
/// has them, else accepted, for the caller to read Postgres past
/// `since`.
fn held_rows(store: Option<Arc<LiveStore>>, since: Option<u64>) -> Verdict {
    if let Some(live) = store
        && let Some(rows) = live.rows()
    {
        return Verdict::Rows { rows, live };
    }
    Verdict::Accepted { since }
}

/// Raises subscription `id`'s position to `at`: no change at or before
/// it goes unreported ([`WorkerState::since`]).
fn raise_since(state: &mut WorkerState, id: u64, at: u64) {
    state
        .since
        .entry(id)
        .and_modify(|since| *since = (*since).max(at))
        .or_insert(at);
}

/// Applies one `delta` body to a store: every table's departures, then
/// its arrivals, the rows moved in rather than copied. The engine
/// coalesces a frame per key, so no key is in both lists and the order
/// between them does not matter.
fn apply_delta(materialized: &mut Materialized, mut body: Json) {
    let Some(Json::Array(changes)) = body.get_mut("changes").map(Json::take) else {
        return;
    };
    for mut change in changes {
        let Some(table) = change.get("table").and_then(Json::as_str).map(str::to_owned) else {
            continue;
        };
        if let Some(Json::Array(keys)) = change.get_mut("del").map(Json::take) {
            for key in &keys {
                materialized.del(&table, key);
            }
        }
        if let Some(Json::Array(rows)) = change.get_mut("put").map(Json::take) {
            for row in rows {
                materialized.put(&table, row);
            }
        }
    }
}

/// The bind as the subscribe frame carries it.
fn bind_json(bind: &crate::key::Bind) -> Json {
    json!({
        "oid": bind.type_oid,
        "bytes": bind.bytes.as_ref().map(|bytes| BASE64.encode(bytes)),
    })
}

/// The upgrade request for `url`, with the bearer token when there is
/// one; else why neither can go in a request. The worker then keeps
/// retrying like any other failed connect, every read is served live,
/// and the reason is in each refusal.
fn upgrade_request(url: &str, token: Option<&str>) -> Result<Request, String> {
    let mut request = url
        .into_client_request()
        .map_err(|error| format!("`{url}` is not an engine URL: {error}"))?;
    if let Some(token) = token {
        let value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
            "STREAMGRES_S2S_TOKEN holds a character a header cannot carry".to_string()
        })?;
        request.headers_mut().insert(AUTHORIZATION, value);
    }
    Ok(request)
}

/// The TLS a `wss://` socket opens with: rustls with the ring provider
/// and the system's root certificates, built once and given to each
/// socket rather than installed for the whole process, which belongs to
/// the application. `None` when the roots cannot be loaded; a `wss://`
/// connect then fails, and says so in the refusals. A `ws://` socket
/// ignores it.
fn tls() -> Option<Connector> {
    static CONFIG: OnceLock<Option<Arc<rustls::ClientConfig>>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let mut roots = rustls::RootCertStore::empty();
            for certificate in rustls_native_certs::load_native_certs().certs {
                let _ = roots.add(certificate);
            }
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let config = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .ok()?
                .with_root_certificates(roots)
                .with_no_client_auth();
            Some(Arc::new(config))
        })
        .clone()
        .map(Connector::Rustls)
}

/// Whether a bearer token sent to `url` would cross the network in plain
/// text: a `ws://` URL whose host is not this machine.
fn token_in_clear(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("ws://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = match authority.rsplit_once(':') {
        Some((host, port)) if port.bytes().all(|byte| byte.is_ascii_digit()) => host,
        _ => authority,
    };
    !matches!(host, "localhost" | "127.0.0.1" | "[::1]")
}

/// Why a connect failed, as a refusal names it.
fn connect_failure(error: &tokio_tungstenite::tungstenite::Error) -> String {
    use tokio_tungstenite::tungstenite::Error;
    match error {
        Error::Http(response) if response.status() == 401 => {
            "the engine refused the token (check STREAMGRES_S2S_TOKEN)".to_string()
        }
        Error::Http(response) if response.status() == 404 => {
            "the engine serves no S2S endpoint at this URL (it serves one only with STREAMGRES_S2S_TOKEN set, and only on STREAMGRES_S2S_ADDR when that is set)".to_string()
        }
        Error::Http(response) if response.status() == 503 => {
            "the engine is not serving yet, or has no free connection".to_string()
        }
        Error::Http(response) => format!("the engine answered {}", response.status()),
        other => other.to_string(),
    }
}

/// The socket a remote engine is reached on.
type Socket = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

/// The sending half of a connection to an engine.
enum Sender {
    Socket(futures_util::stream::SplitSink<Socket, Message>),
    #[cfg(feature = "embedded")]
    Local(mpsc::Sender<String>),
}

/// The receiving half of a connection to an engine.
enum Receiver {
    Socket(futures_util::stream::SplitStream<Socket>),
    #[cfg(feature = "embedded")]
    Local(streamgres::embedded::Session),
}

/// What a connection brought.
enum Heard {
    /// A frame: a socket's text stays in the socket's buffer.
    Socket(tokio_tungstenite::tungstenite::Utf8Bytes),
    #[cfg(feature = "embedded")]
    Local(String),
    /// Something else the socket carries (a ping, a pong, binary).
    Other,
    /// The connection ended.
    Closed,
}

impl Heard {
    /// The frame's text, for a frame.
    fn text(&self) -> Option<&str> {
        match self {
            Heard::Socket(text) => Some(text.as_str()),
            #[cfg(feature = "embedded")]
            Heard::Local(text) => Some(text),
            Heard::Other | Heard::Closed => None,
        }
    }
}

/// Opens a connection to `worker`'s engine: a socket to a remote one, a
/// session with one in this process. Why not, when it cannot.
async fn open(worker: &Worker) -> Result<(Sender, Receiver), String> {
    match &worker.target {
        Target::Remote(url) => {
            let request = upgrade_request(url, worker.shared.token.as_deref())?;
            let (socket, _) =
                tokio_tungstenite::connect_async_tls_with_config(request, None, false, tls())
                    .await
                    .map_err(|error| connect_failure(&error))?;
            let (sink, stream) = socket.split();
            Ok((Sender::Socket(sink), Receiver::Socket(stream)))
        }
        #[cfg(feature = "embedded")]
        Target::Local(engine) => {
            let session = engine.connect()?;
            Ok((Sender::Local(session.sender()), Receiver::Local(session)))
        }
    }
}

impl Sender {
    /// Sends one frame, giving the connection up if it does not take it
    /// in time.
    async fn send(&mut self, frame: String) -> bool {
        match self {
            Sender::Socket(sink) => matches!(
                timeout(SEND_TIMEOUT, sink.send(Message::Text(frame.into()))).await,
                Ok(Ok(()))
            ),
            #[cfg(feature = "embedded")]
            Sender::Local(engine) => {
                matches!(timeout(SEND_TIMEOUT, engine.send(frame)).await, Ok(Ok(())))
            }
        }
    }

    /// Closes the connection.
    async fn close(&mut self) {
        match self {
            Sender::Socket(sink) => {
                let _ = sink.close().await;
            }
            #[cfg(feature = "embedded")]
            Sender::Local(_) => {}
        }
    }
}

impl Receiver {
    /// What the connection brings next; cancelling it loses nothing.
    async fn next(&mut self) -> Heard {
        match self {
            Receiver::Socket(stream) => match stream.next().await {
                Some(Ok(Message::Text(text))) => Heard::Socket(text),
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => Heard::Closed,
                Some(Ok(_)) => Heard::Other,
            },
            #[cfg(feature = "embedded")]
            Receiver::Local(session) => session.recv().await.map_or(Heard::Closed, Heard::Local),
        }
    }
}

/// Connects, serves until the connection breaks either way, reconnects,
/// until the pool stops: one worker's connection, a socket to a remote
/// engine or a session with one in this process, the same frames over
/// either. A quiet connection is pinged every third of the silence
/// window, and how long it has been silent is looked at every thirtieth
/// (a second at most), so it is taken for dead within that of the
/// window, not at the next ping.
async fn connection_loop(worker: Arc<Worker>, mut outgoing: mpsc::UnboundedReceiver<Outgoing>) {
    let mut first = true;
    let mut backoff = Backoff::new();
    loop {
        if !first && !retry_wait(&worker.shared, backoff.next_delay()).await {
            return;
        }
        first = false;
        if worker.shared.stopped.load(Ordering::Acquire) {
            return;
        }
        let (mut sender, mut receiver) = match open(&worker).await {
            Ok(link) => {
                worker.connect_failed(None);
                link
            }
            Err(error) => {
                worker.connect_failed(Some(error));
                continue;
            }
        };
        let opened = Instant::now();
        let mut heard = opened;
        let silence = worker.shared.silence;
        let every = (silence / 3).max(Duration::from_millis(1));
        let mut ping = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let glance = (silence / 30).clamp(Duration::from_millis(1), Duration::from_secs(1));
        let mut check = tokio::time::interval_at(tokio::time::Instant::now() + glance, glance);
        check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut resubscribe: std::collections::VecDeque<_> = worker.up().into();
        let mut restore = tokio::time::interval(Duration::from_millis(10));
        restore.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut up = true;
        while up {
            up = tokio::select! {
                _ = restore.tick(), if !resubscribe.is_empty() => {
                    let mut sent = true;
                    for _ in 0..32 {
                        let Some((id, key)) = resubscribe.pop_front() else { break };
                        // An unsubscribe processed while restoring must not
                        // resurrect a query that no consumer holds anymore.
                        let held = worker.state.lock().expect("state").by_id.get(&id) == Some(&key);
                        if held && !sender.send(subscribe_frame(id, &key.sql, &key.binds.iter().map(bind_json).collect::<Vec<_>>())).await {
                            sent = false;
                            break;
                        }
                    }
                    sent
                },
                frame = receiver.next() => {
                    heard = Instant::now();
                    if let Some(text) = frame.text() {
                        inbound(&worker, text);
                    }
                    !matches!(frame, Heard::Closed)
                },
                _ = ping.tick() => {
                    heard.elapsed() < silence && sender.send(json!(["ping"]).to_string()).await
                },
                _ = check.tick() => heard.elapsed() < silence,
                message = outgoing.recv() => match message {
                    Some(Outgoing::Subscribe { id, sql, binds }) => {
                        sender.send(subscribe_frame(id, &sql, &binds)).await
                    }
                    Some(Outgoing::Unsubscribe { id }) => {
                        sender.send(json!(["unsubscribe", {"id": id}]).to_string()).await
                    }
                    Some(Outgoing::Stop) | None => {
                        sender.close().await;
                        return;
                    }
                },
            };
        }
        if opened.elapsed() >= STABLE_CONNECTION
            && !matches!(
                *worker.greeting.lock().expect("greeting"),
                Greeting::Waiting
            )
        {
            backoff.reset();
        }
        disconnected(&worker);
    }
}

/// Waits for the next attempt without holding the worker alive through
/// shutdown. Register the notification before checking the flag so a
/// pool dropped at that boundary cannot leave a thirty-second sleep.
async fn retry_wait(shared: &Shared, delay: Duration) -> bool {
    let stopped = shared.stop.notified();
    tokio::pin!(stopped);
    stopped.as_mut().enable();
    if shared.stopped.load(Ordering::Acquire) {
        return false;
    }
    tokio::select! {
        _ = tokio::time::sleep(delay) => !shared.stopped.load(Ordering::Acquire),
        _ = stopped => false,
    }
}

/// The socket is down. Every wait ends refused, and what it waited for
/// is forgotten: the engine never accepted it, so there is nothing to
/// resubscribe, and a frame for it still queued would otherwise
/// register it twice. The stores go, the keys the engine had accepted
/// stay held, pending again, and leave the cache until the engine is
/// back. The other workers' keys are untouched. The cache's memory of
/// refusals goes too, and the column types learnt: the engine coming
/// back may be a restarted one, after a migration that changed a
/// column's type to one of the same width.
fn disconnected(worker: &Worker) {
    crate::metrics::note(
        log::Level::Warn,
        &format!("down:{}", worker.index),
        format!(
            "engine socket {} is down: its queries are read from Postgres until it is back",
            worker.index
        ),
    );
    crate::metrics::report(|metrics| metrics.connection(worker.index, false));
    let (waiting, released, keys) = {
        let mut state = worker.state.lock().expect("state");
        worker.connected.store(false, Ordering::Release);
        *worker.greeting.lock().expect("greeting") = Greeting::Waiting;
        *worker.lag.lock().expect("lag") = None;
        worker.position.store(0, Ordering::Release);
        for (id, live) in std::mem::take(&mut state.materialized) {
            let listening = state
                .by_id
                .get(&id)
                .and_then(|key| worker.shared.listening(key));
            if let Some(listening) = listening
                && let Store::Ready(materialized) = std::mem::replace(
                    &mut *live.store.lock().expect("store"),
                    Store::Building(Vec::new()),
                )
            {
                listening.keep(materialized);
            }
        }
        let mut waiting: Vec<SyncSender<Reply>> = Vec::new();
        let mut released: Vec<QueryKey> = Vec::new();
        for (id, reply) in state.pending.drain().collect::<Vec<_>>() {
            if let Some(reply) = reply {
                waiting.push(reply);
                if let Some(key) = worker.drop_id(&mut state, id) {
                    released.push(key);
                }
            }
        }
        let held: Vec<u64> = state.by_id.keys().copied().collect();
        for id in held {
            state.pending.insert(id, None);
        }
        let keys: Vec<QueryKey> = state.by_key.keys().cloned().collect();
        (waiting, released, keys)
    };
    for key in &released {
        worker.shared.release(key, worker.index);
    }
    for reply in &waiting {
        let _ = reply.try_send(Reply::Verdict(
            worker
                .shared
                .refused("the engine connection dropped".to_string()),
        ));
    }
    if let Some(cache) = worker.shared.cache() {
        for key in &keys {
            cache.touched(key);
        }
        cache.forget_refusals();
    }
    worker.shared.types.lock().expect("types").clear();
}

/// A subscribe frame asking for the rows.
fn subscribe_frame(id: u64, sql: &str, binds: &[Json]) -> String {
    json!(["subscribe", {"id": id, "sql": sql, "binds": binds, "rows": true}]).to_string()
}

/// One frame from the engine, on one worker's socket.
fn inbound(worker: &Worker, text: &str) {
    let Ok(Json::Array(mut array)) = serde_json::from_str::<Json>(text) else {
        return;
    };
    let body = if array.len() > 1 {
        array.swap_remove(1)
    } else {
        Json::Null
    };
    match array.first().and_then(Json::as_str).unwrap_or("") {
        "accepted" => on_accepted(worker, body),
        "hello" => on_hello(worker, &body),
        "refused" => on_refused(worker, &body),
        "delta" => on_delta(worker, body),
        "touched" => on_touched(worker, &body),
        "lag" => on_lag(worker, &body),
        _ => {}
    }
}

/// `accepted`: the wait ends, and for a rows-mode subscription the
/// `Building` store is in place, under the same lock, before anyone can
/// see the snapshot, so a delta right behind this frame queues there
/// rather than being lost.
///
/// When nobody holds the id (the wait timed out, and the frame for it
/// was still queued) the engine must not keep following it. A
/// resubscribe after a reconnect has no caller waiting: in rows mode
/// the store is built from the types already known and the cache
/// refilled without a read, the key reserved before the rows are built
/// as a Postgres read reserves it; in touch mode it is accepted, and the
/// next read refills it. The types are forgotten with the socket, and
/// with no connection here to look them up, a resubscribe whose tables
/// lack them is let go ([`let_go`]) rather than kept refilling from
/// Postgres: the next read subscribes it afresh, looks them up, and is
/// served from rows again.
fn on_accepted(worker: &Worker, mut body: Json) {
    let shared = &worker.shared;
    let Some(id) = body.get("id").and_then(Json::as_u64) else {
        return;
    };
    let carried_rows = body.get("nodes").is_some();
    let snapshot = Snapshot::from_json(&mut body);
    if snapshot.is_none() && carried_rows {
        shared.fell_back("the accepted frame was not understood".to_string());
    }
    let since = body.get("lsn").and_then(Json::as_u64);
    let (waiter, key) = {
        let mut state = worker.state.lock().expect("state");
        let waiter = state.pending.remove(&id);
        let key = state.by_id.get(&id).cloned();
        if key.is_some()
            && let Some(since) = since
        {
            raise_since(&mut state, id, since);
        }
        if let Some(key) = &key
            && snapshot.is_some()
        {
            state
                .materialized
                .entry(id)
                .or_insert_with(|| LiveStore::building(id, key.clone(), worker.this.clone()));
        }
        (waiter, key)
    };
    let Some(key) = key else {
        let _ = worker.outgoing.send(Outgoing::Unsubscribe { id });
        return;
    };
    if !matches!(waiter, Some(Some(_)))
        && let Some(reason) = worker.wrong_database(&key)
    {
        resubscribed_elsewhere(worker, id, &key, reason);
        return;
    }
    match (waiter, snapshot) {
        (Some(Some(reply)), Some(snapshot)) => {
            let _ = reply.try_send(Reply::Snapshot(snapshot));
        }
        (Some(Some(reply)), None) => {
            let _ = reply.try_send(Reply::Verdict(Verdict::Accepted { since }));
        }
        (Some(None) | None, Some(snapshot)) => {
            let missing = shared.missing_types(&snapshot);
            if let Some(listening) = shared.listening(&key) {
                if missing.is_empty() {
                    worker.materialize(id, snapshot);
                } else {
                    rebuild_off_thread(worker, id, snapshot, missing, listening);
                }
                return;
            }
            if !missing.is_empty() {
                let_go(worker, id, &key);
                return;
            }
            let reservation = shared.cache().map(|cache| cache.reserve(&key));
            if let Verdict::Rows { live, .. } = worker.materialize(id, snapshot)
                && let (Some(cache), Some(reservation)) = (shared.cache(), reservation)
            {
                cache.live(reservation, live);
            }
        }
        (Some(None) | None, None) => {}
    }
}

/// A key followed for a callback, resubscribed after a reconnect, whose
/// tables' types were forgotten with the socket: they are looked up on a
/// connection of the subscription's own, on a thread of its own, while
/// the deltas queue in the store being built, which is then built from
/// them. A lookup that fails lets the key go, and the subscription says
/// why.
fn rebuild_off_thread(
    worker: &Worker,
    id: u64,
    snapshot: Snapshot,
    missing: Vec<String>,
    listening: Arc<Listening>,
) {
    let Some(this) = worker.this.upgrade() else {
        return;
    };
    let Some(expected) = worker.state.lock().expect("state").materialized.get(&id).cloned() else {
        return;
    };
    let spawned = thread::Builder::new()
        .name("streamgres-listen-types".to_string())
        .spawn(move || {
            let key = this.state.lock().expect("state").by_id.get(&id).cloned();
            match listening.lookup_types(&missing) {
                Ok(types) => {
                    this.shared.types.lock().expect("types").extend(types);
                    this.build(id, snapshot, Some(&expected));
                }
                Err(reason) => {
                    listening.end(reason);
                    if let Some(key) = key {
                        let_go(&this, id, &key);
                    }
                }
            }
        });
    if spawned.is_err()
        && let Some(key) = worker.state.lock().expect("state").by_id.get(&id).cloned()
    {
        let_go(worker, id, &key);
    }
}

/// A key resubscribed after a reconnect, accepted by an engine that
/// streams another database than the cache's reads go to: its rows are
/// not this database's, so the key is let go as a refusal would, and its
/// entry goes stale for the next read to take from Postgres.
fn resubscribed_elsewhere(worker: &Worker, id: u64, key: &QueryKey, reason: String) {
    let _ = worker.shared.refused(reason);
    let_go(worker, id, key);
}

/// Ends a resubscribed key's subscription here and with the engine, and
/// leaves its entry stale, so the next read takes it as a miss and
/// subscribes it afresh.
fn let_go(worker: &Worker, id: u64, key: &QueryKey) {
    let shared = &worker.shared;
    if let Some(listening) = shared.listening(key) {
        listening.end("the engine stopped following the query".to_string());
    }
    {
        let mut state = worker.state.lock().expect("state");
        worker.drop_id(&mut state, id);
    }
    shared.release(key, worker.index);
    let _ = worker.outgoing.send(Outgoing::Unsubscribe { id });
    if let Some(cache) = shared.cache() {
        cache.touched(key);
    }
}

/// `hello`: how many subscriptions the engine takes on this socket, and
/// which database it streams.
fn on_hello(worker: &Worker, body: &Json) {
    let room = body
        .get("max_subscriptions")
        .and_then(Json::as_u64)
        .map_or(0, |room| usize::try_from(room).unwrap_or(usize::MAX));
    worker.room.store(room, Ordering::Relaxed);
    let field = |name: &str| {
        body.get("database")
            .and_then(|database| database.get(name))
            .and_then(Json::as_str)
            .map(str::to_owned)
    };
    *worker.greeting.lock().expect("greeting") = match (field("system"), field("name")) {
        (Some(system), Some(name)) => Greeting::Named(Database { system, name }),
        _ => Greeting::Unnamed,
    };
}

/// `refused`: the waiting subscribe gets the reason. A refusal after
/// acceptance (the engine stopped following the query), or of a
/// resubscribe, has nobody waiting, and the cache must not serve the
/// key any more.
fn on_refused(worker: &Worker, body: &Json) {
    let shared = &worker.shared;
    let Some(id) = body.get("id").and_then(Json::as_u64) else {
        return;
    };
    let reason = body
        .get("reason")
        .and_then(Json::as_str)
        .unwrap_or("refused by the engine")
        .to_string();
    let (reply, key) = {
        let mut state = worker.state.lock().expect("state");
        let reply = state.pending.remove(&id);
        let key = worker.drop_id(&mut state, id);
        (reply, key)
    };
    if let Some(key) = &key {
        shared.release(key, worker.index);
    }
    match reply {
        Some(Some(reply)) => {
            let _ = reply.try_send(Reply::Verdict(shared.refused(reason)));
        }
        Some(None) | None => {
            if let Some(listening) = key.as_ref().and_then(|key| shared.listening(key)) {
                listening.end(format!("the engine stopped following the query: {reason}"));
            }
            if let (Some(key), Some(cache)) = (key, shared.cache()) {
                cache.touched(&key);
            }
        }
    }
}

/// `delta`: applied to the subscription's store, or queued while the
/// store is building; a subscription without a store is touched. When
/// the store's weight drifted, its cache entry is weighed again: the key
/// is reserved first, and the store checked to still be the one held,
/// so a subscription ended in between leaves the key alone.
fn on_delta(worker: &Worker, body: Json) {
    let shared = &worker.shared;
    let Some(id) = body.get("id").and_then(Json::as_u64) else {
        return;
    };
    let (key, live) = {
        let state = worker.state.lock().expect("state");
        let Some(key) = state.by_id.get(&id).cloned() else {
            return;
        };
        (key, state.materialized.get(&id).cloned())
    };
    let listening = shared.listening(&key);
    let Some(live) = live else {
        if let Some(cache) = shared.cache() {
            cache.touched(&key);
        }
        return;
    };
    if let Some(listening) = listening {
        let mut store = live.store.lock().expect("store");
        match &mut *store {
            Store::Building(queued) => queued.push(body),
            Store::Ready(materialized) => listening.apply(materialized, body, apply_delta),
        }
        return;
    }
    let reweigh = {
        let mut store = live.store.lock().expect("store");
        match &mut *store {
            Store::Building(queued) => {
                queued.push(body);
                return;
            }
            Store::Ready(materialized) => {
                apply_delta(materialized, body);
                live.drifted(materialized.byte_len())
            }
        }
    };
    let Some(cache) = shared.cache() else {
        return;
    };
    cache.changed();
    if reweigh {
        let reservation = cache.reserve(&key);
        let held = worker
            .state
            .lock()
            .expect("state")
            .materialized
            .get(&id)
            .is_some_and(|held| Arc::ptr_eq(held, &live));
        if held {
            cache.live(reservation, live);
        }
    }
}

/// `lag`: how far behind the database the engine is, by its heartbeats
/// (`null` while it measures none), and the position up to which every
/// change has been sent on this socket (an engine from before positions
/// sends none).
fn on_lag(worker: &Worker, body: &Json) {
    let lag = body
        .get("ms")
        .and_then(Json::as_u64)
        .map(|ms| (Duration::from_millis(ms), Instant::now()));
    if let Some((lag, _)) = lag {
        crate::metrics::report(|metrics| metrics.lag(lag));
    }
    *worker.lag.lock().expect("lag") = lag;
    if let Some(position) = body.get("lsn").and_then(Json::as_u64) {
        worker.position.fetch_max(position, Ordering::AcqRel);
    }
}

/// `touched`: every named subscription's key goes stale, and a refill
/// from a standby must have replayed past the change's position.
fn on_touched(worker: &Worker, body: &Json) {
    let Some(ids) = body.get("ids").and_then(Json::as_array) else {
        return;
    };
    let position = body.get("lsn").and_then(Json::as_u64);
    let keys: Vec<QueryKey> = {
        let mut state = worker.state.lock().expect("state");
        let mut keys = Vec::new();
        for id in ids.iter().filter_map(Json::as_u64) {
            let Some(key) = state.by_id.get(&id).cloned() else {
                continue;
            };
            if let Some(position) = position {
                raise_since(&mut state, id, position);
            }
            keys.push(key);
        }
        keys
    };
    if let Some(cache) = worker.shared.cache() {
        for key in &keys {
            cache.touched(key);
        }
    }
}

#[cfg(test)]
mod tests {
    /// Back-to-back failures grow the range, cap it, and never retry at zero.
    #[test]
    fn failures_back_off_with_bounded_jitter() {
        let mut low = Backoff::new();
        let mut high = Backoff::new();
        for ceiling in [1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000] {
            assert_eq!(low.with_sample(0), Duration::from_millis(ceiling / 2));
            assert_eq!(
                high.with_sample(ceiling / 2),
                Duration::from_millis(ceiling)
            );
        }
        for _ in 0..100 {
            let delay = high.next_delay();
            assert!((Duration::from_secs(15)..=Duration::from_secs(30)).contains(&delay));
        }
    }

    /// A recovered stream restores the initial retry range.
    #[test]
    fn a_stable_connection_resets_the_retry_range() {
        let mut backoff = Backoff::new();
        for _ in 0..20 {
            backoff.with_sample(0);
        }
        backoff.reset();
        assert_eq!(backoff.with_sample(0), Duration::from_millis(500));
    }

    use std::time::Instant;

    use super::*;
    use crate::row::OwnedRow;
    use crate::types::{ColumnType, TypeKind};
    use diesel::deserialize::FromSqlRow;
    use diesel::pg::Pg;
    use diesel::sql_types::{Integer, Text};

    /// Dropping the pool wakes a long retry wait, including a drop that
    /// happens before the worker has registered its notification.
    #[tokio::test]
    async fn shutdown_interrupts_reconnect_backoff() {
        let (_, _, shared) = worker();
        let waiting = Arc::clone(&shared);
        let task = tokio::spawn(async move { retry_wait(&waiting, Duration::from_secs(30)).await });
        tokio::task::yield_now().await;
        shared.stopped.store(true, Ordering::Release);
        shared.stop.notify_waiters();
        assert!(
            !tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
        );
        assert!(!retry_wait(&shared, Duration::from_secs(30)).await);
    }

    /// A worker with no socket: `connected` is set by hand and frames are
    /// fed to `inbound` directly, so the orderings the reader thread and
    /// the subscribing thread can take are forced rather than hoped for.
    fn worker() -> (Arc<Worker>, mpsc::UnboundedReceiver<Outgoing>, Arc<Shared>) {
        worker_waiting(Duration::from_secs(5))
    }

    /// As [`worker`], giving a verdict `wait` to arrive.
    fn worker_waiting(
        wait: Duration,
    ) -> (Arc<Worker>, mpsc::UnboundedReceiver<Outgoing>, Arc<Shared>) {
        let shared = Arc::new(Shared {
            by_key: Mutex::new(HashMap::new()),
            max_subscriptions: 100,
            token: None,
            silence: DEFAULT_SILENCE,
            max_lag: DEFAULT_MAX_LAG,
            types: Mutex::new(HashMap::new()),
            fallbacks: Mutex::new(Vec::new()),
            refusals: Mutex::new(Vec::new()),
            cache: Mutex::new(None),
            listeners: Mutex::new(HashMap::new()),
            stopped: AtomicBool::new(false),
            stop: tokio::sync::Notify::new(),
            watches: Default::default(),
            was_behind: AtomicBool::new(false),
        });
        let (worker, outgoing) = Worker::new(
            0,
            Target::Remote("ws://127.0.0.1:1/never".to_string()),
            wait,
            Arc::clone(&shared),
        );
        greeted(&worker);
        (worker, outgoing, shared)
    }

    /// The socket is up and the engine has greeted, naming no database.
    fn greeted(worker: &Worker) {
        worker.connected.store(true, Ordering::Release);
        *worker.greeting.lock().unwrap() = Greeting::Unnamed;
    }

    /// The types of table `t`: an `int4` id and a `text` name.
    fn t_types() -> Arc<TableTypes> {
        let base = |name: &str, oid: u32| ColumnType {
            name: name.into(),
            oid,
            typmod: -1,
            kind: TypeKind::Base,
        };
        Arc::new(TableTypes {
            columns: vec![base("id", 23), base("name", 25)],
        })
    }

    /// The key every test subscribes.
    fn key() -> QueryKey {
        QueryKey {
            sql: "SELECT id, name FROM t".to_string(),
            binds: Vec::new(),
            context: 0,
        }
    }

    /// An `accepted` for `id` carrying row 1 of `t`.
    fn accepted_with_rows(id: u64) -> String {
        json!(["accepted", {
            "id": id,
            "nodes": [{"table": "t", "key": ["id"], "parent": null, "rows": [{"id": 1, "name": "a"}]}],
            "columns": [["t", "id"], ["t", "name"]],
            "names": ["id", "name"],
            "count": false,
            "order": [["id", "asc"]],
            "limit": null,
            "offset": 0
        }])
        .to_string()
    }

    /// A `delta` for `id` putting row 2 of `t`.
    fn delta(id: u64) -> String {
        json!(["delta", {"id": id, "changes": [{"table": "t", "put": [{"id": 2, "name": "b"}], "del": []}]}]).to_string()
    }

    /// The rows as `(id, name)` pairs.
    fn decoded(rows: &Arc<[OwnedRow]>) -> Vec<(i32, String)> {
        rows.iter()
            .map(|row| {
                <(i32, String) as FromSqlRow<(Integer, Text), Pg>>::build_from_row(row).unwrap()
            })
            .collect()
    }

    /// Starts a subscribe on another thread and returns once the worker
    /// has it pending.
    fn subscribing(worker: &Arc<Worker>) -> std::thread::JoinHandle<Outcome> {
        let subscriber = Arc::clone(worker);
        let handle = std::thread::spawn(move || subscriber.subscribe(&key()));
        let started = Instant::now();
        while worker.state.lock().unwrap().pending.is_empty() {
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "the subscribe never became pending"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        handle
    }

    /// An `accepted` for `id` carrying `rows` of `t`.
    fn accepted_holding(id: u64, rows: Json) -> String {
        let mut frame: Json = serde_json::from_str(&accepted_with_rows(id)).unwrap();
        frame[1]["nodes"][0]["rows"] = rows;
        frame.to_string()
    }

    /// A `delta` for `id` with `put` rows and `del` keys of `t`.
    fn delta_of(id: u64, put: Json, del: Json) -> String {
        json!(["delta", {"id": id, "changes": [{"table": "t", "put": put, "del": del}]}]).to_string()
    }

    /// `key()` followed for a callback, through `listening`, its tables'
    /// types known.
    fn listened(shared: &Shared, url: &str) -> Arc<Listening> {
        shared.types.lock().unwrap().insert("t".to_string(), t_types());
        let listening = Listening::new(
            url.to_string(),
            crate::cache::Database {
                system: "1".to_string(),
                name: "test".to_string(),
            },
        );
        shared
            .listeners
            .lock()
            .unwrap()
            .insert(key(), Arc::clone(&listening));
        listening
    }

    /// A subscription's changes: its first rows as present, then each
    /// delta's rows as added, updated and removed; after a reconnect,
    /// what differs between the result before and the engine's new
    /// snapshot. Its store never reaches a cache.
    #[test]
    fn a_listened_query_hands_its_changes_over() {
        use crate::listen::Change;
        let (worker, _outgoing, shared) = worker();
        let listening = listened(&shared, "postgres://unused");
        let changes = || listening.take::<(Integer, Text), (i32, String)>();
        let handle = subscribing(&worker);
        inbound(&worker, &accepted_with_rows(1));
        let Outcome::Verdict(Verdict::Rows { .. }) = handle.join().unwrap() else {
            panic!("rows mode");
        };
        assert_eq!(changes(), vec![Change::Present((1, "a".into()))]);

        inbound(&worker, &delta(1));
        inbound(&worker, &delta_of(1, json!([{"id": 1, "name": "z"}]), json!([])));
        inbound(&worker, &delta_of(1, json!([]), json!([{"id": 2}])));
        assert_eq!(
            changes(),
            vec![
                Change::Added((2, "b".into())),
                Change::Updated {
                    old: (1, "a".into()),
                    new: (1, "z".into())
                },
                Change::Removed((2, "b".into())),
            ]
        );

        disconnected(&worker);
        worker.up();
        greeted(&worker);
        shared.types.lock().unwrap().insert("t".to_string(), t_types());
        inbound(
            &worker,
            &accepted_holding(1, json!([{"id": 1, "name": "y"}, {"id": 3, "name": "c"}])),
        );
        assert_eq!(
            changes(),
            vec![
                Change::Updated {
                    old: (1, "z".into()),
                    new: (1, "y".into())
                },
                Change::Added((3, "c".into())),
            ],
            "the difference across the reconnect"
        );
        inbound(&worker, &delta_of(1, json!([]), json!([{"id": 3}])));
        assert_eq!(changes(), vec![Change::Removed((3, "c".into()))], "followed again");
    }

    /// A subscription is checked against the database it was rendered
    /// on, whatever a cache's is: an engine streaming another is not
    /// asked.
    #[test]
    fn a_listened_query_is_not_asked_of_an_engine_on_another_database() {
        let hello = |system: &str, name: &str| json!({"database": {"system": system, "name": name}});
        let (worker, mut outgoing, shared) = worker();
        listened(&shared, "postgres://unused");
        on_hello(&worker, &hello("1", "elsewhere"));
        let Outcome::Verdict(Verdict::Refused(reason)) = worker.subscribe(&key()) else {
            panic!("refused");
        };
        assert!(reason.contains("reads here go to `test` of system 1"), "{reason}");
        assert!(outgoing.try_recv().is_err(), "the engine was not asked");
        on_hello(&worker, &hello("1", "test"));
        assert!(worker.wrong_database(&key()).is_none());
    }

    /// After a reconnect, a subscription whose tables' types were
    /// forgotten looks them up on its own connection; one that cannot
    /// lets the key go and says why, rather than stopping silently.
    #[test]
    fn a_listened_query_that_cannot_relearn_its_types_says_so() {
        let (worker, _outgoing, shared) = worker();
        let listening = listened(&shared, "postgresql://nobody@127.0.0.1:1/none");
        let handle = subscribing(&worker);
        inbound(&worker, &accepted_with_rows(1));
        let Outcome::Verdict(Verdict::Rows { .. }) = handle.join().unwrap() else {
            panic!("rows mode");
        };
        disconnected(&worker);
        worker.up();
        greeted(&worker);
        inbound(&worker, &accepted_with_rows(1));
        let started = Instant::now();
        while worker.state.lock().unwrap().by_key.contains_key(&key()) {
            assert!(started.elapsed() < Duration::from_secs(10), "never let go");
            std::thread::sleep(Duration::from_millis(5));
        }
        let errors = listening.errors();
        assert!(
            errors.iter().any(|error| error.contains("connecting to look the column types up")),
            "{errors:?}"
        );
    }

    /// The reader thread handles `accepted` and a delta back to back;
    /// whether the subscribing thread has woken up in between must not
    /// matter: the store has the delta's row.
    #[test]
    fn a_delta_right_behind_accepted_is_in_the_store() {
        let (worker, _outgoing, shared) = worker();
        shared
            .types
            .lock()
            .unwrap()
            .insert("t".to_string(), t_types());
        let handle = subscribing(&worker);
        inbound(&worker, &accepted_with_rows(1));
        inbound(&worker, &delta(1));
        let Outcome::Verdict(Verdict::Rows { .. }) = handle.join().unwrap() else {
            panic!("rows mode");
        };
        let Outcome::Verdict(Verdict::Rows { rows, .. }) = worker.subscribe(&key()) else {
            panic!("held in rows mode");
        };
        assert_eq!(
            decoded(&rows),
            vec![(1, "a".into()), (2, "b".into())],
            "the store has the delta's row"
        );
    }

    /// While the first reader looks the types up, the store waits for
    /// its rows: a second read of the key goes to Postgres and must fill
    /// nothing, or its rows could land after the store's entry and
    /// replace it with rows no delta ever updates. Once built, the store
    /// serves.
    #[test]
    fn a_read_while_the_store_is_built_fills_nothing() {
        let (worker, _outgoing, _shared) = worker();
        let handle = subscribing(&worker);
        inbound(&worker, &accepted_with_rows(1));
        let Outcome::NeedsTypes { snapshot, .. } = handle.join().unwrap() else {
            panic!("the types are looked up");
        };
        let Outcome::Verdict(Verdict::Refused(reason)) = worker.subscribe(&key()) else {
            panic!("not a refill while the store is built");
        };
        assert!(reason.contains("being built"), "{reason}");
        assert!(
            Shared::TRANSIENT.iter().any(|transient| reason.starts_with(transient)),
            "not final: asked again next time"
        );
        worker
            .shared
            .types
            .lock()
            .unwrap()
            .insert("t".to_string(), t_types());
        let Verdict::Rows { .. } = worker.materialize_key(&key(), snapshot) else {
            panic!("rows mode");
        };
        let Outcome::Verdict(Verdict::Rows { .. }) = worker.subscribe(&key()) else {
            panic!("held in rows mode");
        };
    }

    /// Types learnt before a migration lack the column it added: a
    /// snapshot naming that column asks for the table's types again,
    /// and the fresh ones serve the rows.
    #[test]
    fn a_column_the_known_types_lack_asks_for_the_types_again() {
        let (worker, _outgoing, shared) = worker();
        let stale = Arc::new(TableTypes {
            columns: vec![ColumnType {
                name: "id".into(),
                oid: 23,
                typmod: -1,
                kind: TypeKind::Base,
            }],
        });
        shared.types.lock().unwrap().insert("t".to_string(), stale);
        let handle = subscribing(&worker);
        inbound(&worker, &accepted_with_rows(1));
        let Outcome::NeedsTypes { snapshot, missing } = handle.join().unwrap() else {
            panic!("stale types are looked up again");
        };
        assert_eq!(missing, vec!["t".to_string()]);
        shared
            .types
            .lock()
            .unwrap()
            .insert("t".to_string(), t_types());
        let Verdict::Rows { .. } = worker.materialize_key(&key(), snapshot) else {
            panic!("rows mode with the fresh types");
        };
    }

    /// A delta that lands while the caller is off looking the types up
    /// queues and is applied once the store is built; a delta after that
    /// reaches the same store.
    #[test]
    fn deltas_during_the_type_lookup_queue_and_apply_once() {
        let (worker, _outgoing, shared) = worker();
        let handle = subscribing(&worker);
        inbound(&worker, &accepted_with_rows(1));
        let Outcome::NeedsTypes { snapshot, missing } = handle.join().unwrap() else {
            panic!("the types of t are not known yet");
        };
        assert_eq!(missing, vec!["t".to_string()]);
        inbound(&worker, &delta(1));
        shared
            .types
            .lock()
            .unwrap()
            .insert("t".to_string(), t_types());
        let Verdict::Rows { rows, .. } = worker.materialize_key(&key(), snapshot) else {
            panic!(
                "rows mode once the types are in: {:?}",
                shared.fallbacks.lock().unwrap()
            );
        };
        assert_eq!(decoded(&rows), vec![(1, "a".into()), (2, "b".into())]);
        inbound(&worker, &json!(["delta", {"id": 1, "changes": [{"table": "t", "put": [], "del": [{"id": 1}]}]}]).to_string());
        let Outcome::Verdict(Verdict::Rows { rows, .. }) = worker.subscribe(&key()) else {
            panic!("held in rows mode");
        };
        assert_eq!(decoded(&rows), vec![(2, "b".into())]);
    }

    /// A subscribe whose socket drops is refused and forgotten. Its frame
    /// was still queued and went out on the next socket, so the engine's
    /// acceptance names an id nobody holds: the engine is told to drop
    /// it.
    #[test]
    fn a_wait_cut_by_a_socket_drop_is_forgotten_and_a_late_accept_is_unsubscribed() {
        let (worker, mut outgoing, _shared) = worker();
        let handle = subscribing(&worker);
        disconnected(&worker);
        let Outcome::Verdict(Verdict::Refused(reason)) = handle.join().unwrap() else {
            panic!("refused");
        };
        assert_eq!(reason, "the engine connection dropped");
        assert!(
            worker.state.lock().unwrap().by_id.is_empty(),
            "nothing to resubscribe"
        );
        assert_eq!(worker.held.load(Ordering::Relaxed), 0);
        greeted(&worker);
        inbound(&worker, &accepted_with_rows(1));
        let mut unsubscribed = false;
        while let Ok(message) = outgoing.try_recv() {
            if matches!(message, Outgoing::Unsubscribe { id: 1 }) {
                unsubscribed = true;
            }
        }
        assert!(unsubscribed, "the engine is told to drop it");
        assert!(
            worker.state.lock().unwrap().materialized.is_empty(),
            "no store for an id nobody holds"
        );
    }

    /// When the socket drops and comes back, an accepted key is held,
    /// resubscribed, and not to be trusted until the engine says so: a
    /// read in the window goes live.
    #[test]
    fn a_resubscribed_key_is_pending_until_the_engine_accepts_it() {
        let (worker, _outgoing, _shared) = worker();
        let handle = subscribing(&worker);
        inbound(&worker, &json!(["accepted", {"id": 1}]).to_string());
        assert!(matches!(
            handle.join().unwrap(),
            Outcome::Verdict(Verdict::Accepted { since: None })
        ));
        disconnected(&worker);
        assert!(
            worker.state.lock().unwrap().by_id.contains_key(&1),
            "an accepted key stays held"
        );
        greeted(&worker);
        let Outcome::Verdict(Verdict::Refused(reason)) = worker.subscribe(&key()) else {
            panic!("a read in the window goes live");
        };
        assert_eq!(reason, "the subscription is pending");
        inbound(&worker, &json!(["accepted", {"id": 1}]).to_string());
        assert!(matches!(
            worker.subscribe(&key()),
            Outcome::Verdict(Verdict::Accepted { since: None })
        ));
    }

    /// A dropped socket forgets the types, so the rows a resubscribe
    /// brings back cannot be built: the key is let go, not kept
    /// refilling from Postgres, and the next read subscribes it afresh
    /// and is asked for the types. With the types known, a resubscribe
    /// is served from rows again at once.
    #[test]
    fn a_resubscribe_without_types_lets_the_key_go() {
        let (worker, mut outgoing, shared) = worker();
        shared.types.lock().unwrap().insert("t".to_string(), t_types());
        let handle = subscribing(&worker);
        inbound(&worker, &accepted_with_rows(1));
        assert!(matches!(handle.join().unwrap(), Outcome::Verdict(Verdict::Rows { .. })));

        disconnected(&worker);
        greeted(&worker);
        shared.types.lock().unwrap().insert("t".to_string(), t_types());
        inbound(&worker, &accepted_with_rows(1));
        assert!(
            worker.state.lock().unwrap().materialized.contains_key(&1),
            "types known: rows again"
        );

        disconnected(&worker);
        greeted(&worker);
        while outgoing.try_recv().is_ok() {}
        inbound(&worker, &accepted_with_rows(1));
        assert!(matches!(outgoing.try_recv(), Ok(Outgoing::Unsubscribe { id: 1 })));
        {
            let state = worker.state.lock().unwrap();
            assert!(state.by_id.is_empty(), "the key is let go");
            assert!(state.materialized.is_empty());
        }
        assert!(shared.fallbacks.lock().unwrap().is_empty(), "not turned to refills");

        let handle = subscribing(&worker);
        inbound(&worker, &accepted_with_rows(2));
        let Outcome::NeedsTypes { missing, .. } = handle.join().unwrap() else {
            panic!("the next read looks the types up");
        };
        assert_eq!(missing, vec!["t".to_string()]);
    }

    /// An engine that takes the subscribe and never answers: the wait
    /// ends at the verdict timeout, refused for a reason that passes,
    /// and the id is withdrawn, so a late `accepted` registers nothing.
    #[test]
    fn a_subscribe_nobody_answers_is_given_up_in_time() {
        let (worker, mut outgoing, shared) = worker_waiting(Duration::from_millis(50));
        let started = Instant::now();
        let Outcome::Verdict(Verdict::Refused(reason)) = worker.subscribe(&key()) else {
            panic!("refused");
        };
        assert!(started.elapsed() >= Duration::from_millis(50));
        assert_eq!(reason, "the engine did not answer in time");
        assert!(Shared::TRANSIENT.contains(&reason.as_str()));
        assert!(matches!(
            outgoing.try_recv(),
            Ok(Outgoing::Subscribe { id: 1, .. })
        ));
        assert!(matches!(
            outgoing.try_recv(),
            Ok(Outgoing::Unsubscribe { id: 1 })
        ));
        let state = worker.state.lock().unwrap();
        assert!(state.pending.is_empty(), "nothing waits");
        assert!(
            state.by_key.is_empty() && state.by_id.is_empty(),
            "nothing is held"
        );
        assert!(
            shared.by_key.lock().unwrap().is_empty(),
            "the key is not counted against the cap"
        );
    }

    /// The engine that comes back after a dropped socket may know a type
    /// it refused before, so its refusals are forgotten with the socket.
    #[test]
    fn a_dropped_socket_forgets_the_engines_refusals() {
        let (worker, _outgoing, shared) = worker();
        let cache = Arc::new(Cache::new(
            1 << 20,
            Arc::new(crate::subscriptions::Unsynced),
        ));
        *shared.cache.lock().unwrap() = Some(Arc::downgrade(&cache));
        cache.refused(&key(), "`$2`: a bind of type oid 16406 is not supported");
        assert!(
            cache.refused_recently(&key()).is_some(),
            "remembered while the engine is the same"
        );
        disconnected(&worker);
        assert!(
            cache.refused_recently(&key()).is_none(),
            "forgotten with the socket"
        );
    }

    /// An engine that streams another database than the cache's reads go
    /// to is not asked for rows, which would be served as this one's;
    /// the right one is, and one that names no database is taken on
    /// trust. A key resubscribed after a reconnect to the wrong one is
    /// let go and its entry left stale.
    #[test]
    fn an_engine_on_another_database_is_not_asked() {
        let ours = Database {
            system: "7699115118699778086".into(),
            name: "tickets".into(),
        };
        let hello = |system: &str, name: &str| json!({"database": {"system": system, "name": name}});
        let (worker, mut outgoing, shared) = worker_waiting(Duration::from_millis(50));
        let cache = Arc::new(Cache::new(1 << 20, Arc::new(crate::subscriptions::Unsynced)));
        *shared.cache.lock().unwrap() = Some(Arc::downgrade(&cache));
        cache.learn_database(Some(ours.clone()));

        on_hello(&worker, &hello("7699114362382213158", "tickets"));
        let Outcome::Verdict(Verdict::Refused(reason)) = worker.subscribe(&key()) else {
            panic!("refused");
        };
        assert!(reason.contains("streams database `tickets` of system 7699114362382213158"), "{reason}");
        assert!(outgoing.try_recv().is_err(), "the engine was not asked");

        on_hello(&worker, &json!({}));
        assert!(worker.wrong_database(&key()).is_none(), "an engine naming no database is trusted");
        on_hello(&worker, &hello(&ours.system, &ours.name));
        assert!(worker.wrong_database(&key()).is_none());
        let handle = subscribing(&worker);
        inbound(&worker, &json!(["accepted", {"id": 1}]).to_string());
        assert!(matches!(handle.join().unwrap(), Outcome::Verdict(Verdict::Accepted { .. })));
        assert!(matches!(outgoing.try_recv(), Ok(Outgoing::Subscribe { id: 1, .. })));

        disconnected(&worker);
        assert!(worker.state.lock().unwrap().by_id.contains_key(&1), "held to resubscribe");
        greeted(&worker);
        on_hello(&worker, &hello(&ours.system, "staging"));
        inbound(&worker, &json!(["accepted", {"id": 1}]).to_string());
        assert!(matches!(outgoing.try_recv(), Ok(Outgoing::Unsubscribe { id: 1 })));
        assert!(worker.state.lock().unwrap().by_id.is_empty(), "the key is let go");
    }

    /// The engine's lag is what its latest frame said plus the time since,
    /// so an engine that stops reporting counts as falling further behind;
    /// none is known until a frame says, nor while it says `null`, nor
    /// once the socket drops.
    #[test]
    fn the_lag_grows_while_the_engine_says_nothing() {
        let (worker, _outgoing, _shared) = worker();
        assert_eq!(worker.lag(), None);
        inbound(&worker, &json!(["lag", {"ms": null}]).to_string());
        assert_eq!(worker.lag(), None);
        inbound(&worker, &json!(["lag", {"ms": 1500}]).to_string());
        let lag = worker.lag().expect("said");
        assert!(lag >= Duration::from_millis(1500) && lag < Duration::from_millis(1600), "{lag:?}");
        *worker.lag.lock().unwrap() = Some((Duration::from_millis(1500), Instant::now() - Duration::from_secs(1)));
        assert!(worker.lag().unwrap() >= Duration::from_millis(2500), "a second of silence adds a second");
        disconnected(&worker);
        assert_eq!(worker.lag(), None, "a dropped socket says nothing");
    }

    /// Reads stop trusting the cache once the engine is further behind
    /// than the limit, and trust it again when it catches up; a zero
    /// limit always trusts it.
    #[test]
    fn a_lagging_engine_sends_reads_to_postgres() {
        let client = EngineClient::with_config("ws://127.0.0.1:1/never/s2s/v1", Config::default());
        let worker = &client.pool.workers[0];
        worker.connected.store(true, Ordering::Release);
        assert!(!client.behind(), "no frame, no lag");
        inbound(worker, &json!(["lag", {"ms": 50}]).to_string());
        assert!(!client.behind());
        inbound(worker, &json!(["lag", {"ms": 2500}]).to_string());
        assert!(client.behind());
        assert!(client.lag().unwrap() >= Duration::from_millis(2500));
        inbound(worker, &json!(["lag", {"ms": 0}]).to_string());
        assert!(!client.behind(), "caught up");
        let trusting = EngineClient::with_config(
            "ws://127.0.0.1:1/never/s2s/v1",
            Config {
                max_lag: Duration::ZERO,
                ..Config::default()
            },
        );
        trusting.pool.workers[0].connected.store(true, Ordering::Release);
        inbound(&trusting.pool.workers[0], &json!(["lag", {"ms": 60000}]).to_string());
        assert!(!trusting.behind());
    }

    /// The client's position is the least any connected socket has
    /// heard, and unknown while one has heard none: its keys may be
    /// behind the others'. A socket that drops forgets its own, and only
    /// connected sockets count.
    #[test]
    fn the_position_is_the_least_every_connected_socket_has_heard() {
        let client = EngineClient::with_config(
            "ws://127.0.0.1:1/never/s2s/v1",
            Config {
                connections: 2,
                ..Config::default()
            },
        );
        let (first, second) = (&client.pool.workers[0], &client.pool.workers[1]);
        assert_eq!(client.position(), None, "nothing connected");
        first.connected.store(true, Ordering::Release);
        second.connected.store(true, Ordering::Release);
        inbound(first, &json!(["lag", {"ms": 0, "lsn": 900}]).to_string());
        assert_eq!(client.position(), None, "the second has heard none");
        inbound(second, &json!(["lag", {"ms": 0, "lsn": 700}]).to_string());
        assert_eq!(client.position(), Some(700));
        inbound(second, &json!(["lag", {"ms": 0, "lsn": 600}]).to_string());
        assert_eq!(client.position(), Some(700), "a position never moves back");
        inbound(second, &json!(["lag", {"ms": 0, "lsn": null}]).to_string());
        assert_eq!(client.position(), Some(700), "an older frame shape keeps it");
        disconnected(second);
        assert_eq!(client.position(), Some(900), "only connected sockets count");
        second.connected.store(true, Ordering::Release);
        assert_eq!(client.position(), None, "reconnected, it has heard none");
    }

    /// This client's own refusal reasons all pass; the engine's are final.
    #[test]
    fn every_client_side_reason_is_transient() {
        let client = EngineClient::with_config("ws://127.0.0.1:1/never/s2s/v1", Config::default());
        for reason in Shared::TRANSIENT {
            assert!(
                !client.refusal_is_final(&format!("{reason} and more")),
                "{reason}"
            );
        }
        assert!(client.refusal_is_final("the query joins more tables than the engine follows"));
    }

    /// The defaults are the documented ones, and an unset variable
    /// leaves its default.
    #[test]
    fn config_defaults_and_env() {
        let defaults = Config::default();
        assert_eq!(defaults.connections, DEFAULT_CONNECTIONS);
        assert_eq!(defaults.max_subscriptions, DEFAULT_MAX_SUBSCRIPTIONS);
        assert_eq!(defaults.verdict_timeout, DEFAULT_VERDICT_TIMEOUT);
        assert_eq!(env_number("STREAMGRES_S2S_TEST_UNSET_VARIABLE"), None);
    }

    /// A token is in plain text over `ws://` to anything but this
    /// machine, and never over `wss://`.
    #[test]
    fn a_token_over_plain_ws_to_another_host_is_named() {
        assert!(token_in_clear("ws://engine:4848/zero/s2s/v1"));
        assert!(token_in_clear("ws://10.0.0.7/zero/s2s/v1"));
        assert!(!token_in_clear("ws://127.0.0.1:4848/zero/s2s/v1"));
        assert!(!token_in_clear("ws://localhost/zero/s2s/v1"));
        assert!(!token_in_clear("ws://[::1]:4848/zero/s2s/v1"));
        assert!(!token_in_clear("wss://engine:4848/zero/s2s/v1"));
        assert!(tls().is_some(), "the TLS config builds");
    }

    /// The token goes out as a bearer, and only when there is one; it
    /// never shows in the configuration's `Debug`.
    #[test]
    fn the_token_goes_out_as_a_bearer() {
        let url = "ws://127.0.0.1:4848/zero/s2s/v1";
        let plain = upgrade_request(url, None).expect("a request");
        assert!(plain.headers().get(AUTHORIZATION).is_none());
        let with = upgrade_request(url, Some("s3cret")).expect("a request");
        assert_eq!(with.headers()[AUTHORIZATION], "Bearer s3cret");
        let bad = upgrade_request(url, Some("bad\nline")).expect_err("no header");
        assert!(bad.contains("STREAMGRES_S2S_TOKEN"), "{bad}");
        assert!(upgrade_request("not a url", None).is_err());
        let config = Config {
            token: Some("s3cret".into()),
            ..Config::default()
        };
        assert!(!format!("{config:?}").contains("s3cret"));
    }

    /// A pool over `workers`, as `with_config` would build it.
    fn pool_of(workers: Vec<Arc<Worker>>, shared: Arc<Shared>) -> EngineClient {
        EngineClient {
            pool: Arc::new(Pool {
                workers,
                shared,
                config: Config::default(),
            }),
        }
    }

    /// The engine's `hello` names its per-connection limit: a full socket
    /// is passed over, and with every socket full the subscribe is
    /// refused here, transiently, without asking the engine.
    #[test]
    fn a_socket_at_the_engine_limit_is_passed_over() {
        let (worker, mut outgoing, shared) = worker();
        assert!(worker.has_room(), "no limit until the engine names one");
        inbound(
            &worker,
            r#"["hello", {"protocol": 1, "max_subscriptions": 1}]"#,
        );
        assert_eq!(worker.room.load(Ordering::Relaxed), 1);
        let client = pool_of(vec![Arc::clone(&worker)], Arc::clone(&shared));
        assert!(client.least_loaded().is_ok());

        worker.held.store(1, Ordering::Relaxed);
        let reason = client.least_loaded().err().expect("full");
        assert!(
            reason.starts_with("the subscription limit of 1 per connection"),
            "{reason}"
        );
        assert!(!client.refusal_is_final(&reason));
        match Subscriptions::subscribe(&client, &key()) {
            Outcome::Verdict(Verdict::Refused(refused)) => assert_eq!(refused, reason),
            _ => panic!("expected a refusal"),
        }
        assert!(outgoing.try_recv().is_err(), "the engine was not asked");

        inbound(
            &worker,
            r#"["hello", {"protocol": 1, "max_subscriptions": null}]"#,
        );
        assert!(worker.has_room(), "a later hello with no limit lifts it");
    }

    /// While a socket is down, its refusals say why it last failed to
    /// connect, and the client lists it; a success clears it.
    #[test]
    fn a_failed_connect_is_named_in_the_refusals() {
        let (worker, _outgoing, shared) = worker();
        worker.connected.store(false, Ordering::Release);
        let client = pool_of(vec![Arc::clone(&worker)], Arc::clone(&shared));
        assert_eq!(
            client.least_loaded().err().as_deref(),
            Some("the engine is not connected")
        );

        let response = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(401)
            .body(None)
            .unwrap();
        let error = tokio_tungstenite::tungstenite::Error::Http(Box::new(response));
        worker.connect_failed(Some(connect_failure(&error)));
        let reason = client.least_loaded().err().expect("down");
        assert_eq!(
            reason,
            "the engine is not connected: the engine refused the token (check STREAMGRES_S2S_TOKEN)"
        );
        assert!(!client.refusal_is_final(&reason));
        assert_eq!(client.connect_errors().len(), 1);

        worker.connect_failed(None);
        assert!(client.connect_errors().is_empty());
    }

    /// A pool asked for no sockets opens one.
    #[test]
    fn zero_connections_become_one() {
        let client = EngineClient::with_config(
            "ws://127.0.0.1:1/never/s2s/v1",
            Config {
                connections: 0,
                ..Config::default()
            },
        );
        assert_eq!(client.config().connections, 1);
        assert_eq!(client.per_worker(), vec![0]);
        assert!(!client.is_connected());
    }
}
