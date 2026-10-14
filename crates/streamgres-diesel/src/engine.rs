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
//! `numeric`, a microsecond timestamp) the subscription falls back to
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value as Json, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::cache::Cache;
use crate::key::QueryKey;
use crate::materialized::{Materialized, Snapshot};
use crate::subscriptions::{Subscriptions, Verdict};
use crate::types::{TableTypes, TypeLookup};

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
            max_subscriptions: env_number("STREAMGRES_S2S_MAX_SUBSCRIPTIONS").unwrap_or(defaults.max_subscriptions),
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
    /// a table and reused for every delta and resubscribe after.
    types: Mutex<HashMap<String, Arc<TableTypes>>>,
    /// Why the latest subscriptions fell back from rows to refills, most
    /// recent last, at most a few.
    fallbacks: Mutex<Vec<String>>,
    cache: Mutex<Option<Weak<Cache>>>,
}

impl Shared {
    fn cache(&self) -> Option<Arc<Cache>> {
        self.cache.lock().expect("cache").as_ref()?.upgrade()
    }

    fn fell_back(&self, reason: String) {
        let mut fallbacks = self.fallbacks.lock().expect("fallbacks");
        if fallbacks.len() >= 8 {
            fallbacks.remove(0);
        }
        fallbacks.push(reason);
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
    /// Subscribes waiting for a verdict, with where to deliver it.
    pending: HashMap<u64, std::sync::mpsc::SyncSender<Reply>>,
    /// Subscriptions served from the engine's rows.
    materialized: HashMap<u64, Materialized>,
}

/// One socket to the engine and the subscriptions it carries.
struct Worker {
    index: usize,
    url: String,
    verdict_timeout: Duration,
    outgoing: mpsc::UnboundedSender<Outgoing>,
    connected: AtomicBool,
    state: Mutex<WorkerState>,
    shared: Arc<Shared>,
}

struct Pool {
    workers: Vec<Arc<Worker>>,
    shared: Arc<Shared>,
    config: Config,
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
            cache: Mutex::new(None),
        });
        let workers = (0..config.connections)
            .map(|index| Worker::start(index, url.clone(), config.verdict_timeout, Arc::clone(&shared)))
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
            .map(|worker| worker.state.lock().expect("state").by_key.len())
            .collect()
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
        self.pool.shared.fallbacks.lock().expect("fallbacks").clone()
    }

    /// The connected worker carrying the fewest subscriptions.
    fn least_loaded(&self) -> Option<&Arc<Worker>> {
        self.pool
            .workers
            .iter()
            .filter(|worker| worker.connected.load(Ordering::Acquire))
            .min_by_key(|worker| worker.state.lock().expect("state").by_key.len())
    }
}

impl Subscriptions for EngineClient {
    fn subscribe(&self, key: &QueryKey, lookup: &mut dyn TypeLookup) -> Verdict {
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
                        return Verdict::Refused(format!(
                            "the subscription limit of {} is reached",
                            shared.max_subscriptions
                        ));
                    }
                    let Some(worker) = self.least_loaded() else {
                        return Verdict::Refused("the engine is not connected".to_string());
                    };
                    by_key.insert(key.clone(), worker.index);
                    Arc::clone(worker)
                }
            }
        };
        worker.subscribe(key, lookup)
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
    fn start(index: usize, url: String, verdict_timeout: Duration, shared: Arc<Shared>) -> Arc<Worker> {
        let (outgoing, outgoing_rx) = mpsc::unbounded_channel();
        let worker = Arc::new(Worker {
            index,
            url,
            verdict_timeout,
            outgoing,
            connected: AtomicBool::new(false),
            state: Mutex::new(WorkerState {
                next_id: 1,
                by_key: HashMap::new(),
                by_id: HashMap::new(),
                pending: HashMap::new(),
                materialized: HashMap::new(),
            }),
            shared,
        });
        let task = Arc::clone(&worker);
        std::thread::Builder::new()
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

    fn subscribe(&self, key: &QueryKey, lookup: &mut dyn TypeLookup) -> Verdict {
        let (reply, verdict) = std::sync::mpsc::sync_channel(1);
        let id = {
            let mut state = self.state.lock().expect("state");
            if let Some(id) = state.by_key.get(key).copied() {
                // Already subscribed (a refill after a touch, or a
                // resubscribe the reader could not materialise without
                // a connection): the rows if the store has them.
                return match state.materialized.get(&id).map(Materialized::rows) {
                    Some(Ok(rows)) => Verdict::Rows(rows),
                    _ => Verdict::Accepted,
                };
            }
            if !self.connected.load(Ordering::Acquire) {
                drop(state);
                self.shared.release(key, self.index);
                return Verdict::Refused("the engine is not connected".to_string());
            }
            let id = state.next_id;
            state.next_id += 1;
            state.by_key.insert(key.clone(), id);
            state.by_id.insert(id, key.clone());
            state.pending.insert(id, reply);
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
            return Verdict::Refused("the engine client has stopped".to_string());
        }
        match verdict.recv_timeout(self.verdict_timeout) {
            Ok(Reply::Verdict(verdict)) => verdict,
            Ok(Reply::Snapshot(snapshot)) => self.materialize(id, snapshot, Some(lookup)),
            Err(_) => {
                self.forget(id);
                let _ = self.outgoing.send(Outgoing::Unsubscribe { id });
                Verdict::Refused("the engine did not answer in time".to_string())
            }
        }
    }

    fn unsubscribe(&self, key: &QueryKey) {
        let id = {
            let mut state = self.state.lock().expect("state");
            let Some(id) = state.by_key.remove(key) else {
                return;
            };
            state.by_id.remove(&id);
            state.pending.remove(&id);
            state.materialized.remove(&id);
            id
        };
        let _ = self.outgoing.send(Outgoing::Unsubscribe { id });
    }

    /// Builds the store for an accepted subscription from its snapshot.
    /// The tables' types come from `lookup`, or from what earlier
    /// subscribes learnt when there is no connection at hand (a
    /// resubscribe). Falls back to touches when the rows cannot be
    /// encoded or a table's types are not known.
    fn materialize(&self, id: u64, snapshot: Snapshot, mut lookup: Option<&mut dyn TypeLookup>) -> Verdict {
        let shared = &self.shared;
        let mut types: HashMap<String, Arc<TableTypes>> = HashMap::new();
        for table in snapshot.tables() {
            let known = shared.types.lock().expect("types").get(table).cloned();
            let table_types = match (known, lookup.as_deref_mut()) {
                (Some(types), _) => types,
                (None, Some(lookup)) => match lookup.table_types(table) {
                    Ok(found) => {
                        let found = Arc::new(found);
                        shared
                            .types
                            .lock()
                            .expect("types")
                            .insert(table.to_string(), Arc::clone(&found));
                        found
                    }
                    Err(error) => {
                        shared.fell_back(format!("types of `{table}` unavailable: {error}"));
                        return Verdict::Accepted;
                    }
                },
                (None, None) => return Verdict::Accepted,
            };
            types.insert(table.to_string(), table_types);
        }
        match Materialized::new(snapshot, &types) {
            Ok(materialized) => match materialized.rows() {
                Ok(rows) => {
                    self.state
                        .lock()
                        .expect("state")
                        .materialized
                        .insert(id, materialized);
                    Verdict::Rows(rows)
                }
                Err(error) => {
                    shared.fell_back(error.0);
                    Verdict::Accepted
                }
            },
            Err(error) => {
                shared.fell_back(error.0);
                Verdict::Accepted
            }
        }
    }

    /// Drops a subscription from this worker and from the routing map.
    fn forget(&self, id: u64) {
        let key = {
            let mut state = self.state.lock().expect("state");
            let key = state.by_id.remove(&id);
            if let Some(key) = &key {
                state.by_key.remove(key);
            }
            state.pending.remove(&id);
            state.materialized.remove(&id);
            key
        };
        if let Some(key) = key {
            self.shared.release(&key, self.index);
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
        let socket = match tokio_tungstenite::connect_async(&worker.url).await {
            Ok((socket, _)) => socket,
            Err(_) => continue,
        };
        let (mut sink, mut stream) = socket.split();

        // Up: a reconnect resubscribes what this worker still holds,
        // under the same ids. Their cache entries went when the socket
        // did, so nothing stale is served meanwhile.
        let resubscribe: Vec<(u64, QueryKey)> = {
            let state = worker.state.lock().expect("state");
            worker.connected.store(true, Ordering::Release);
            state
                .by_id
                .iter()
                .map(|(id, key)| (*id, key.clone()))
                .collect()
        };
        for (id, key) in resubscribe {
            let frame = subscribe_frame(id, &key.sql, &key.binds.iter().map(bind_json).collect::<Vec<_>>());
            if sink.send(Message::Text(frame.into())).await.is_err() {
                break;
            }
        }

        loop {
            tokio::select! {
                frame = stream.next() => match frame {
                    Some(Ok(Message::Text(text))) => inbound(&worker, &text),
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) | Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => {}
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                },
                message = outgoing.recv() => match message {
                    Some(Outgoing::Subscribe { id, sql, binds }) => {
                        if sink.send(Message::Text(subscribe_frame(id, &sql, &binds).into())).await.is_err() {
                            break;
                        }
                    }
                    Some(Outgoing::Unsubscribe { id }) => {
                        let frame = json!(["unsubscribe", {"id": id}]).to_string();
                        if sink.send(Message::Text(frame.into())).await.is_err() {
                            break;
                        }
                    }
                    None => return,
                },
            }
        }

        // Down: every wait ends refused, the stores go, and this
        // worker's keys leave the cache until the engine is back. The
        // other workers' keys are untouched.
        let (pending, keys): (Vec<std::sync::mpsc::SyncSender<Reply>>, Vec<QueryKey>) = {
            let mut state = worker.state.lock().expect("state");
            worker.connected.store(false, Ordering::Release);
            state.materialized.clear();
            let pending = state.pending.drain().map(|(_, reply)| reply).collect();
            let keys = state.by_key.keys().cloned().collect();
            (pending, keys)
        };
        for reply in pending {
            let _ = reply.try_send(Reply::Verdict(Verdict::Refused(
                "the engine connection dropped".to_string(),
            )));
        }
        if let Some(cache) = worker.shared.cache() {
            for key in &keys {
                cache.touched(key);
            }
        }
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
            let reply = worker.state.lock().expect("state").pending.remove(&id);
            match (reply, snapshot) {
                (Some(reply), Some(snapshot)) => {
                    let _ = reply.try_send(Reply::Snapshot(snapshot));
                }
                (Some(reply), None) => {
                    let _ = reply.try_send(Reply::Verdict(Verdict::Accepted));
                }
                (None, Some(snapshot)) => {
                    // A resubscribe after a reconnect: no caller waits, so
                    // the store is built from the types already known and
                    // the cache refilled without a read.
                    if let Verdict::Rows(rows) = worker.materialize(id, snapshot, None) {
                        let key = worker.state.lock().expect("state").by_id.get(&id).cloned();
                        if let (Some(key), Some(cache)) = (key, shared.cache()) {
                            cache.replace(&key, rows);
                        }
                    }
                }
                (None, None) => {}
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
                state.materialized.remove(&id);
                let key = state.by_id.remove(&id);
                if let Some(key) = &key {
                    state.by_key.remove(key);
                }
                (reply, key)
            };
            if let Some(key) = &key {
                shared.release(key, worker.index);
            }
            match reply {
                Some(reply) => {
                    let _ = reply.try_send(Reply::Verdict(Verdict::Refused(reason)));
                }
                // A refusal after acceptance (the engine stopped following
                // the query): the cache must not serve it.
                None => {
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
            let outcome = {
                let mut state = worker.state.lock().expect("state");
                let key = state.by_id.get(&id).cloned();
                match (key, state.materialized.get_mut(&id)) {
                    (Some(key), Some(materialized)) => {
                        for change in body.get("changes").and_then(Json::as_array).into_iter().flatten() {
                            let Some(table) = change.get("table").and_then(Json::as_str) else {
                                continue;
                            };
                            for row in change.get("del").and_then(Json::as_array).into_iter().flatten() {
                                materialized.del(table, row);
                            }
                            for row in change.get("put").and_then(Json::as_array).into_iter().flatten() {
                                materialized.put(table, row.clone());
                            }
                        }
                        Some((key, materialized.rows().ok()))
                    }
                    (Some(key), None) => Some((key, None)),
                    (None, _) => None,
                }
            };
            if let (Some((key, rows)), Some(cache)) = (outcome, shared.cache()) {
                match rows {
                    Some(rows) => cache.replace(&key, rows),
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
    use super::*;

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
