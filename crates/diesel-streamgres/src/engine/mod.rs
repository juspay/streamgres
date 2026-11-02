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
//! exactly, the rows are kept as a `Materialized` store that the cache
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;


use crate::cache::{Cache, Live};
use crate::key::QueryKey;
use crate::listen::Listening;
use crate::materialized::Snapshot;
use crate::subscriptions::{Outcome, Subscriptions, Verdict};
use crate::postgres::TableTypes;

mod transport;
mod worker;

use self::transport::token_in_clear;
use self::worker::{Greeting, LiveStore, Outgoing, Shared, Target, Worker};

/// How long a subscribe waits for the engine's verdict before the read is
/// served live instead.
pub const DEFAULT_VERDICT_TIMEOUT: Duration = Duration::from_secs(5);

/// Sockets to the engine per process when the environment says nothing.
pub const DEFAULT_CONNECTIONS: usize = 2;

/// Subscriptions a process may hold across its sockets when the
/// environment says nothing.
pub const DEFAULT_MAX_SUBSCRIPTIONS: usize = 10_000;

/// How long a socket may stay silent, pings unanswered, before it is
/// taken for dead and its keys go stale, when the environment says
/// nothing. A quiet socket is pinged three times in that time.
pub const DEFAULT_SILENCE: Duration = Duration::from_secs(10);

/// How far behind the database the engine may be, by its own heartbeats,
/// before reads stop trusting the cache, when the environment says
/// nothing.
pub const DEFAULT_MAX_LAG: Duration = Duration::from_secs(2);

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
/// | `STREAMGRES_S2S_MAX_JOIN_WORK`      | steps one build of a joined result may take | 1000000 |
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
    /// How many steps one build of a joined result may take. A cached
    /// read over it goes to Postgres; a callback over it ends.
    pub max_join_work: u64,
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
            .field("max_join_work", &self.max_join_work)
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
            max_join_work: crate::materialized::DEFAULT_JOIN_WORK,
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
            max_join_work: env_number("STREAMGRES_S2S_MAX_JOIN_WORK")
                .and_then(|steps| u64::try_from(steps).ok())
                .unwrap_or(defaults.max_join_work),
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
            max_join_work: config.max_join_work,
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

#[cfg(test)]
mod tests;
