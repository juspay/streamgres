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
//! exactly, the result is kept as a [`Materialized`] store and the
//! engine's `delta` frames replace the cache entry in place: Postgres is
//! never read for that query. When a column cannot be encoded (a
//! `numeric`, an interval, a range) the subscription falls back to
//! touches: the cache reads Postgres and the engine's frames drop the
//! entry. Which mode a key is in is the client's business; the cache sees
//! `replace` or `touched`.
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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value as Json, json};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

use crate::cache::Cache;
use crate::key::QueryKey;
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

/// How long to wait before trying the engine again after a lost socket.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// How long one frame may take to go out before the socket is given up
/// on: an engine that holds the socket open but stops reading would
/// otherwise block the worker, and its thread, for good.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// The pool's shape. [`Config::from_env`] reads it from the environment:
///
/// | variable                            | meaning                            | default |
/// |-------------------------------------|------------------------------------|---------|
/// | `STREAMGRES_S2S_CONNECTIONS`        | sockets to the engine              | 2       |
/// | `STREAMGRES_S2S_MAX_SUBSCRIPTIONS`  | subscriptions held, all sockets    | 10000   |
/// | `STREAMGRES_S2S_VERDICT_TIMEOUT_MS` | wait for `accepted` or `refused`   | 5000    |
///
/// A value that is missing, not a number, or zero leaves the default.
#[derive(Clone, Debug)]
pub struct Config {
    pub connections: usize,
    pub max_subscriptions: usize,
    pub verdict_timeout: Duration,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            connections: DEFAULT_CONNECTIONS,
            max_subscriptions: DEFAULT_MAX_SUBSCRIPTIONS,
            verdict_timeout: DEFAULT_VERDICT_TIMEOUT,
        }
    }
}

impl Config {
    pub fn from_env() -> Config {
        let defaults = Config::default();
        Config {
            connections: env_number("STREAMGRES_S2S_CONNECTIONS").unwrap_or(defaults.connections),
            max_subscriptions: env_number("STREAMGRES_S2S_MAX_SUBSCRIPTIONS")
                .unwrap_or(defaults.max_subscriptions),
            verdict_timeout: env_number("STREAMGRES_S2S_VERDICT_TIMEOUT_MS")
                .map(|millis| Duration::from_millis(millis as u64))
                .unwrap_or(defaults.verdict_timeout),
        }
    }
}

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
    cache: Mutex<Option<Weak<Cache>>>,
    /// Set when the pool is dropped; the worker threads then end.
    stopped: AtomicBool,
}

impl Shared {
    fn cache(&self) -> Option<Arc<Cache>> {
        self.cache.lock().expect("cache").as_ref()?.upgrade()
    }

    fn fell_back(&self, reason: String) {
        Self::remember(&self.fallbacks, reason);
    }

    fn refused(&self, reason: String) -> Verdict {
        Self::remember(&self.refusals, reason.clone());
        Verdict::Refused(reason)
    }

    /// The reasons this client refuses on its own, all of which pass.
    const TRANSIENT: [&'static str; 8] = [
        crate::subscriptions::STALE_TYPES,
        "the engine is not connected",
        "the engine client has stopped",
        "the engine did not answer in time",
        "the engine connection dropped",
        "the subscription limit of ",
        "the subscription is pending",
        "the subscription ended before its rows were built",
    ];

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

struct WorkerState {
    next_id: u64,
    by_key: HashMap<QueryKey, u64>,
    by_id: HashMap<u64, QueryKey>,
    /// Subscriptions the engine has not accepted yet: a subscribe
    /// waiting for its verdict, with where to deliver it, or `None` for
    /// a key resubscribed after a reconnect, which nobody waits for. A
    /// read of a pending key goes live: until the engine accepts, a
    /// commit can land that no frame reports.
    pending: HashMap<u64, Option<SyncSender<Reply>>>,
    /// Subscriptions served from the engine's rows, each behind its own
    /// lock so a delta to one is applied without holding up the rest.
    materialized: HashMap<u64, Arc<Mutex<Store>>>,
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

/// One socket to the engine and the subscriptions it carries.
struct Worker {
    index: usize,
    url: String,
    verdict_timeout: Duration,
    outgoing: mpsc::UnboundedSender<Outgoing>,
    connected: AtomicBool,
    /// How many keys this worker holds, kept beside the state so the
    /// pool can pick the least loaded worker without locking each one.
    held: AtomicUsize,
    state: Mutex<WorkerState>,
    shared: Arc<Shared>,
}

struct Pool {
    workers: Vec<Arc<Worker>>,
    shared: Arc<Shared>,
    config: Config,
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.shared.stopped.store(true, Ordering::Release);
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

    pub fn with_config(url: impl Into<String>, config: Config) -> EngineClient {
        let url = url.into();
        let config = Config {
            connections: config.connections.max(1),
            ..config
        };
        let shared = Arc::new(Shared {
            by_key: Mutex::new(HashMap::new()),
            max_subscriptions: config.max_subscriptions,
            types: Mutex::new(HashMap::new()),
            fallbacks: Mutex::new(Vec::new()),
            refusals: Mutex::new(Vec::new()),
            cache: Mutex::new(None),
            stopped: AtomicBool::new(false),
        });
        let workers = (0..config.connections)
            .map(|index| {
                Worker::start(
                    index,
                    url.clone(),
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

    pub fn config(&self) -> &Config {
        &self.pool.config
    }

    /// Whether any socket to the engine is up.
    pub fn is_connected(&self) -> bool {
        self.connections() > 0
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
                let stores: Vec<Arc<Mutex<Store>>> = worker
                    .state
                    .lock()
                    .expect("state")
                    .materialized
                    .values()
                    .cloned()
                    .collect();
                stores
                    .iter()
                    .map(|store| match &*store.lock().expect("store") {
                        Store::Ready(materialized) => materialized.byte_len(),
                        Store::Building(_) => 0,
                    })
                    .sum::<usize>()
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

    /// The connected worker carrying the fewest subscriptions.
    fn least_loaded(&self) -> Option<&Arc<Worker>> {
        self.pool
            .workers
            .iter()
            .filter(|worker| worker.connected.load(Ordering::Acquire))
            .min_by_key(|worker| worker.held.load(Ordering::Relaxed))
    }
}

impl Subscriptions for EngineClient {
    fn subscribe(&self, key: &QueryKey) -> Outcome {
        let shared = &self.pool.shared;
        // Route: the worker already holding the key, else the least
        // loaded one, unless the cap is reached. The key is claimed in
        // the routing map before the worker is asked, so concurrent
        // subscribes count against the cap and land on one worker.
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
                    let Some(worker) = self.least_loaded() else {
                        return Outcome::Verdict(
                            shared.refused("the engine is not connected".to_string()),
                        );
                    };
                    by_key.insert(key.clone(), worker.index);
                    Arc::clone(worker)
                }
            }
        };
        worker.subscribe(key)
    }

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

    fn refusal_is_final(&self, reason: &str) -> bool {
        !Shared::TRANSIENT
            .iter()
            .any(|transient| reason.starts_with(transient))
    }

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
    fn new(
        index: usize,
        url: String,
        verdict_timeout: Duration,
        shared: Arc<Shared>,
    ) -> (Arc<Worker>, mpsc::UnboundedReceiver<Outgoing>) {
        let (outgoing, outgoing_rx) = mpsc::unbounded_channel();
        let worker = Arc::new(Worker {
            index,
            url,
            verdict_timeout,
            outgoing,
            connected: AtomicBool::new(false),
            held: AtomicUsize::new(0),
            state: Mutex::new(WorkerState {
                next_id: 1,
                by_key: HashMap::new(),
                by_id: HashMap::new(),
                pending: HashMap::new(),
                materialized: HashMap::new(),
            }),
            shared,
        });
        (worker, outgoing_rx)
    }

    fn start(
        index: usize,
        url: String,
        verdict_timeout: Duration,
        shared: Arc<Shared>,
    ) -> Arc<Worker> {
        let (worker, outgoing_rx) = Worker::new(index, url, verdict_timeout, shared);
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

    fn subscribe(&self, key: &QueryKey) -> Outcome {
        let (reply, verdict) = sync_channel(1);
        let id = {
            let mut state = self.state.lock().expect("state");
            let held = state.by_key.get(key).copied();
            if !self.connected.load(Ordering::Acquire) {
                // A key this worker holds stays routed to it, to be
                // resubscribed when the socket is back; a new one is
                // released for another worker or another time.
                drop(state);
                if held.is_none() {
                    self.shared.release(key, self.index);
                }
                return Outcome::Verdict(
                    self.shared
                        .refused("the engine is not connected".to_string()),
                );
            }
            if let Some(id) = held {
                if state.pending.contains_key(&id) {
                    // Another read is waiting for the verdict. Accepting
                    // now would fill an entry that a refusal or a timeout
                    // then leaves without a subscription; this read goes
                    // live and the next one finds the answer.
                    return Outcome::Verdict(
                        self.shared
                            .refused("the subscription is pending".to_string()),
                    );
                }
                // Already subscribed (a refill after a touch, or a
                // resubscribe the reader could not materialise without
                // a connection): the rows if the store has them.
                let store = state.materialized.get(&id).cloned();
                drop(state);
                return Outcome::Verdict(match store {
                    Some(store) => match &*store.lock().expect("store") {
                        Store::Ready(materialized) => match materialized.rows() {
                            Ok(rows) => Verdict::Rows {
                                rows,
                                store_bytes: materialized.byte_len(),
                            },
                            Err(_) => Verdict::Accepted,
                        },
                        Store::Building(_) => Verdict::Accepted,
                    },
                    None => Verdict::Accepted,
                });
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
                let missing: Vec<String> = {
                    let types = self.shared.types.lock().expect("types");
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
                };
                // The reader thread put a `Building` store in place
                // before it handed the snapshot over, so every delta
                // from that moment on queues there.
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
        self.note_held(state);
        key
    }

    /// Builds the store for an accepted subscription from its snapshot
    /// and the types already known, applies the deltas that landed
    /// while the types were looked up, and installs it. Falls back to
    /// touches when a table's types are not known or the rows cannot be
    /// encoded. A subscription that ended meanwhile gets nothing.
    fn materialize(&self, id: u64, snapshot: Snapshot) -> Verdict {
        let shared = &self.shared;
        let fallback = |reason: String| {
            self.state.lock().expect("state").materialized.remove(&id);
            shared.fell_back(reason);
            Verdict::Accepted
        };
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
        // The store is transitioned in place, under the state lock and
        // its own: the one `Arc` lives for the subscription, so a delta
        // handler that took a clone of it before this point finds it
        // `Ready` and applies to the live store, and no delta lands
        // between taking the queue and installing the rows.
        let mut state = self.state.lock().expect("state");
        if !state.by_id.contains_key(&id) {
            state.materialized.remove(&id);
            return Verdict::Refused(
                "the subscription ended before its rows were built".to_string(),
            );
        }
        let store = Arc::clone(
            state
                .materialized
                .entry(id)
                .or_insert_with(|| Arc::new(Mutex::new(Store::Building(Vec::new())))),
        );
        let mut store = store.lock().expect("store");
        let queued = match &mut *store {
            Store::Building(queued) => std::mem::take(queued),
            Store::Ready(_) => Vec::new(),
        };
        for body in &queued {
            apply_delta(&mut materialized, body);
        }
        match materialized.rows() {
            Ok(rows) => {
                let store_bytes = materialized.byte_len();
                *store = Store::Ready(materialized);
                Verdict::Rows { rows, store_bytes }
            }
            Err(error) => {
                drop(store);
                state.materialized.remove(&id);
                drop(state);
                shared.fell_back(error.0);
                Verdict::Accepted
            }
        }
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

    fn note_held(&self, state: &WorkerState) {
        self.held.store(state.by_key.len(), Ordering::Relaxed);
    }
}

/// Applies one `delta` body to a store: every table's departures, then
/// its arrivals. The engine coalesces a frame per key, so no key is in
/// both lists and the order between them does not matter.
fn apply_delta(materialized: &mut Materialized, body: &Json) {
    for change in body
        .get("changes")
        .and_then(Json::as_array)
        .into_iter()
        .flatten()
    {
        let Some(table) = change.get("table").and_then(Json::as_str) else {
            continue;
        };
        for row in change
            .get("del")
            .and_then(Json::as_array)
            .into_iter()
            .flatten()
        {
            materialized.del(table, row);
        }
        for row in change
            .get("put")
            .and_then(Json::as_array)
            .into_iter()
            .flatten()
        {
            materialized.put(table, row.clone());
        }
    }
}

fn bind_json(bind: &crate::key::Bind) -> Json {
    json!({
        "oid": bind.type_oid,
        "bytes": bind.bytes.as_ref().map(|bytes| BASE64.encode(bytes)),
    })
}

/// Connects, serves, reconnects, forever: one worker's socket.
async fn connection_loop(worker: Arc<Worker>, mut outgoing: mpsc::UnboundedReceiver<Outgoing>) {
    let mut first = true;
    loop {
        if !first {
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
        first = false;
        if worker.shared.stopped.load(Ordering::Acquire) {
            return;
        }
        let socket = match tokio_tungstenite::connect_async(&worker.url).await {
            Ok((socket, _)) => socket,
            Err(_) => continue,
        };
        let (mut sink, mut stream) = socket.split();

        // Up: a reconnect resubscribes what this worker still holds,
        // under the same ids. Their cache entries went when the socket
        // did, and each is pending again until the engine accepts it,
        // so nothing stale is served meanwhile.
        let resubscribe: Vec<(u64, QueryKey)> = {
            let mut state = worker.state.lock().expect("state");
            worker.connected.store(true, Ordering::Release);
            let held: Vec<(u64, QueryKey)> = state
                .by_id
                .iter()
                .map(|(id, key)| (*id, key.clone()))
                .collect();
            for (id, _) in &held {
                state.pending.insert(*id, None);
            }
            held
        };
        let mut up = true;
        for (id, key) in resubscribe {
            let frame = subscribe_frame(
                id,
                &key.sql,
                &key.binds.iter().map(bind_json).collect::<Vec<_>>(),
            );
            if !send_frame(&mut sink, frame).await {
                up = false;
                break;
            }
        }

        // Serve until the socket breaks either way.
        while up {
            up = tokio::select! {
                frame = stream.next() => match frame {
                    Some(Ok(Message::Text(text))) => {
                        inbound(&worker, &text);
                        true
                    }
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) | Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => true,
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => false,
                },
                message = outgoing.recv() => match message {
                    Some(Outgoing::Subscribe { id, sql, binds }) => {
                        send_frame(&mut sink, subscribe_frame(id, &sql, &binds)).await
                    }
                    Some(Outgoing::Unsubscribe { id }) => {
                        send_frame(&mut sink, json!(["unsubscribe", {"id": id}]).to_string()).await
                    }
                    Some(Outgoing::Stop) | None => {
                        let _ = sink.close().await;
                        return;
                    }
                },
            };
        }
        disconnected(&worker);
    }
}

/// Sends one frame, giving the socket up if it does not take it in time.
async fn send_frame<S>(sink: &mut S, frame: String) -> bool
where
    S: futures_util::Sink<Message> + Unpin,
{
    matches!(
        timeout(SEND_TIMEOUT, sink.send(Message::Text(frame.into()))).await,
        Ok(Ok(()))
    )
}

/// The socket is down. Every wait ends refused, and what it waited for
/// is forgotten: the engine never accepted it, so there is nothing to
/// resubscribe, and a frame for it still queued would otherwise
/// register it twice. The stores go, the keys the engine had accepted
/// stay held, pending again, and leave the cache until the engine is
/// back. The other workers' keys are untouched. The cache's memory of
/// refusals goes too: the engine coming back may be a restarted one.
fn disconnected(worker: &Worker) {
    let (waiting, released, keys) = {
        let mut state = worker.state.lock().expect("state");
        worker.connected.store(false, Ordering::Release);
        state.materialized.clear();
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
}

fn subscribe_frame(id: u64, sql: &str, binds: &[Json]) -> String {
    json!(["subscribe", {"id": id, "sql": sql, "binds": binds, "rows": true}]).to_string()
}

/// One frame from the engine, on one worker's socket.
fn inbound(worker: &Worker, text: &str) {
    let shared = &worker.shared;
    let Ok(frame) = serde_json::from_str::<Json>(text) else {
        return;
    };
    let Some(array) = frame.as_array() else {
        return;
    };
    let tag = array.first().and_then(Json::as_str).unwrap_or("");
    let body = array.get(1).cloned().unwrap_or(Json::Null);
    match tag {
        "accepted" => {
            let Some(id) = body.get("id").and_then(Json::as_u64) else {
                return;
            };
            let snapshot = Snapshot::from_json(&body);
            if snapshot.is_none() && body.get("rows").is_some() {
                shared.fell_back("the accepted frame was not understood".to_string());
            }
            // Under one lock: the wait ends, and for a rows-mode
            // subscription the `Building` store is in place before
            // anyone can see the snapshot, so a delta right behind this
            // frame queues there rather than being lost.
            let (waiter, key) = {
                let mut state = worker.state.lock().expect("state");
                let waiter = state.pending.remove(&id);
                let key = state.by_id.get(&id).cloned();
                if key.is_some() && snapshot.is_some() {
                    state
                        .materialized
                        .entry(id)
                        .or_insert_with(|| Arc::new(Mutex::new(Store::Building(Vec::new()))));
                }
                (waiter, key)
            };
            let Some(key) = key else {
                // Nobody holds the id (the wait timed out, and the
                // frame for it was still queued): the engine must not
                // keep following it.
                let _ = worker.outgoing.send(Outgoing::Unsubscribe { id });
                return;
            };
            match (waiter, snapshot) {
                (Some(Some(reply)), Some(snapshot)) => {
                    let _ = reply.try_send(Reply::Snapshot(snapshot));
                }
                (Some(Some(reply)), None) => {
                    let _ = reply.try_send(Reply::Verdict(Verdict::Accepted));
                }
                (Some(None) | None, Some(snapshot)) => {
                    // A resubscribe after a reconnect: no caller waits, so
                    // the store is built from the types already known and
                    // the cache refilled without a read. The key is
                    // reserved before the rows are built, as a Postgres
                    // read reserves it.
                    let reservation = shared.cache().map(|cache| cache.reserve(&key));
                    if let Verdict::Rows { rows, store_bytes } = worker.materialize(id, snapshot)
                        && let (Some(cache), Some(reservation)) = (shared.cache(), reservation)
                    {
                        cache.replace(reservation, rows, store_bytes);
                    }
                }
                // A resubscribe in touch mode: accepted, and the next
                // read refills it.
                (Some(None) | None, None) => {}
            }
        }
        "refused" => {
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
                // A refusal after acceptance (the engine stopped following
                // the query), or of a resubscribe: the cache must not
                // serve it.
                Some(None) | None => {
                    if let (Some(key), Some(cache)) = (key, shared.cache()) {
                        cache.touched(&key);
                    }
                }
            }
        }
        "delta" => {
            let Some(id) = body.get("id").and_then(Json::as_u64) else {
                return;
            };
            // The worker's lock is held only to find the store; the
            // delta is applied under the store's own. The key is
            // reserved before the store is read, and the store looked
            // up again after, so a subscription ended in between (which
            // bumps the generation as it goes) fails the replace.
            let key = worker.state.lock().expect("state").by_id.get(&id).cloned();
            let Some(key) = key else {
                return;
            };
            let reservation = shared.cache().map(|cache| cache.reserve(&key));
            let store = {
                let state = worker.state.lock().expect("state");
                if !state.by_id.contains_key(&id) {
                    return;
                }
                state.materialized.get(&id).cloned()
            };
            let rows = match store {
                Some(store) => {
                    let mut store = store.lock().expect("store");
                    match &mut *store {
                        Store::Building(queued) => {
                            queued.push(body);
                            return;
                        }
                        Store::Ready(materialized) => {
                            apply_delta(materialized, &body);
                            match materialized.rows() {
                                Ok(rows) => Some((rows, materialized.byte_len())),
                                Err(error) => {
                                    // A value the client cannot encode
                                    // arrived: from here on the query is
                                    // refilled from Postgres.
                                    drop(store);
                                    worker.state.lock().expect("state").materialized.remove(&id);
                                    shared.fell_back(error.0);
                                    None
                                }
                            }
                        }
                    }
                }
                None => None,
            };
            if let (Some(cache), Some(reservation)) = (shared.cache(), reservation) {
                match rows {
                    Some((rows, store_bytes)) => cache.replace(reservation, rows, store_bytes),
                    None => cache.touched(&key),
                }
            }
        }
        "touched" => {
            let Some(ids) = body.get("ids").and_then(Json::as_array) else {
                return;
            };
            let keys: Vec<QueryKey> = {
                let state = worker.state.lock().expect("state");
                ids.iter()
                    .filter_map(Json::as_u64)
                    .filter_map(|id| state.by_id.get(&id).cloned())
                    .collect()
            };
            if let Some(cache) = shared.cache() {
                for key in &keys {
                    cache.touched(key);
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::row::OwnedRow;
    use crate::types::{ColumnType, TypeKind};
    use diesel::deserialize::FromSqlRow;
    use diesel::pg::Pg;
    use diesel::sql_types::{Integer, Text};

    /// A worker with no socket: `connected` is set by hand and frames are
    /// fed to `inbound` directly, so the orderings the reader thread and
    /// the subscribing thread can take are forced rather than hoped for.
    fn worker() -> (Arc<Worker>, mpsc::UnboundedReceiver<Outgoing>, Arc<Shared>) {
        let shared = Arc::new(Shared {
            by_key: Mutex::new(HashMap::new()),
            max_subscriptions: 100,
            types: Mutex::new(HashMap::new()),
            fallbacks: Mutex::new(Vec::new()),
            refusals: Mutex::new(Vec::new()),
            cache: Mutex::new(None),
            stopped: AtomicBool::new(false),
        });
        let (worker, outgoing) = Worker::new(
            0,
            "ws://127.0.0.1:1/never".to_string(),
            Duration::from_secs(5),
            Arc::clone(&shared),
        );
        worker.connected.store(true, Ordering::Release);
        (worker, outgoing, shared)
    }

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

    fn key() -> QueryKey {
        QueryKey {
            sql: "SELECT id, name FROM t".to_string(),
            binds: Vec::new(),
        }
    }

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

    fn delta(id: u64) -> String {
        json!(["delta", {"id": id, "changes": [{"table": "t", "put": [{"id": 2, "name": "b"}], "del": []}]}]).to_string()
    }

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

    #[test]
    fn a_delta_right_behind_accepted_is_in_the_store() {
        let (worker, _outgoing, shared) = worker();
        shared
            .types
            .lock()
            .unwrap()
            .insert("t".to_string(), t_types());
        let handle = subscribing(&worker);
        // The reader thread handles both frames back to back; whether
        // the subscribing thread has woken up in between must not
        // matter.
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

    #[test]
    fn deltas_during_the_type_lookup_queue_and_apply_once() {
        let (worker, _outgoing, shared) = worker();
        let handle = subscribing(&worker);
        inbound(&worker, &accepted_with_rows(1));
        let Outcome::NeedsTypes { snapshot, missing } = handle.join().unwrap() else {
            panic!("the types of t are not known yet");
        };
        assert_eq!(missing, vec!["t".to_string()]);
        // The caller is off looking the types up; a delta lands.
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
        // And a delta after the store is built reaches the same store.
        inbound(&worker, &json!(["delta", {"id": 1, "changes": [{"table": "t", "put": [], "del": [{"id": 1}]}]}]).to_string());
        let Outcome::Verdict(Verdict::Rows { rows, .. }) = worker.subscribe(&key()) else {
            panic!("held in rows mode");
        };
        assert_eq!(decoded(&rows), vec![(2, "b".into())]);
    }

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

        // The frame for it was still queued and went out on the next
        // socket; the engine's acceptance names an id nobody holds.
        worker.connected.store(true, Ordering::Release);
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

    #[test]
    fn a_resubscribed_key_is_pending_until_the_engine_accepts_it() {
        let (worker, _outgoing, _shared) = worker();
        let handle = subscribing(&worker);
        inbound(&worker, &json!(["accepted", {"id": 1}]).to_string());
        assert!(matches!(
            handle.join().unwrap(),
            Outcome::Verdict(Verdict::Accepted)
        ));

        // The socket drops and comes back: the key is held, resubscribed,
        // and not to be trusted until the engine says so.
        disconnected(&worker);
        assert!(
            worker.state.lock().unwrap().by_id.contains_key(&1),
            "an accepted key stays held"
        );
        worker.connected.store(true, Ordering::Release);
        let Outcome::Verdict(Verdict::Refused(reason)) = worker.subscribe(&key()) else {
            panic!("a read in the window goes live");
        };
        assert_eq!(reason, "the subscription is pending");
        inbound(&worker, &json!(["accepted", {"id": 1}]).to_string());
        assert!(matches!(
            worker.subscribe(&key()),
            Outcome::Verdict(Verdict::Accepted)
        ));
    }

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

        // The engine that comes back may know the type: ask it again.
        disconnected(&worker);
        assert!(
            cache.refused_recently(&key()).is_none(),
            "forgotten with the socket"
        );
    }

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

    #[test]
    fn config_defaults_and_env() {
        let defaults = Config::default();
        assert_eq!(defaults.connections, DEFAULT_CONNECTIONS);
        assert_eq!(defaults.max_subscriptions, DEFAULT_MAX_SUBSCRIPTIONS);
        assert_eq!(defaults.verdict_timeout, DEFAULT_VERDICT_TIMEOUT);
        assert_eq!(env_number("STREAMGRES_S2S_TEST_UNSET_VARIABLE"), None);
    }

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
