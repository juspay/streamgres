//! One socket's worker: what it holds, under one lock, and the frames
//! the engine sends it, handled beside that state. A store goes in as
//! `Building` under the lock the waiting subscribe holds, before its
//! snapshot is handed over, so a delta that lands meanwhile waits in it;
//! that ordering is why the handlers and the state live together.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Value as Json, json};
use tokio::sync::mpsc;

use crate::cache::{Cache, Database, Live};
use crate::key::QueryKey;
use crate::listen::{Ended, Listening};
use crate::postgres::OwnedRow;
use crate::materialized::{BuildError, Materialized, Snapshot};
use crate::subscriptions::{Outcome, Verdict};
use crate::postgres::TableTypes;

use super::transport::connection_loop;


/// How far a store's weight may drift from what its cache entry was
/// weighed at, as a share of it (a quarter) and at least this many
/// bytes, before the entry is weighed again.
const REWEIGH_BYTES: usize = 64 * 1024;

/// What a connection task is asked to send.
pub(super) enum Outgoing {
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
pub(super) enum Reply {
    Verdict(Verdict),
    /// Accepted with rows: the caller, which has a connection, builds the
    /// store.
    Snapshot(Snapshot),
}

/// What every worker shares: the cache, the types learnt, the fallback
/// log and the one map saying which worker holds which key.
pub(super) struct Shared {
    /// Which worker holds each key. Also the count against the cap.
    pub(super) by_key: Mutex<HashMap<QueryKey, usize>>,
    pub(super) max_subscriptions: usize,
    /// The bearer token each socket opens with, if the engine asks for one.
    pub(super) token: Option<String>,
    /// How long a socket may stay silent before it is taken for dead.
    pub(super) silence: Duration,
    /// How far behind the engine may be before reads go to Postgres.
    pub(super) max_lag: Duration,
    /// How many steps one build of a joined result may take.
    pub(super) max_join_work: u64,
    /// Column types by table, learnt on the first rows-mode subscribe of
    /// a table and reused for every delta and resubscribe after; looked
    /// up again when a snapshot names a column they lack, which is how a
    /// column a migration added becomes known.
    pub(super) types: Mutex<HashMap<String, Arc<TableTypes>>>,
    /// Why the latest subscriptions fell back from rows to refills, most
    /// recent last, at most a few.
    pub(super) fallbacks: Mutex<Vec<String>>,
    /// Why the latest subscriptions were refused, by the engine or by
    /// this client, most recent last, at most a few.
    pub(super) refusals: Mutex<Vec<String>>,
    /// The cache this client feeds, once one is attached.
    pub(super) cache: Mutex<Option<Weak<Cache>>>,
    /// The keys followed for a callback ([`EngineClient::subscribe`]),
    /// not for the cache: their stores never reach it, and their deltas
    /// are handed to the subscription as changes.
    pub(super) listeners: Mutex<HashMap<QueryKey, Arc<Listening>>>,
    /// Set when the pool is dropped; the worker threads then end.
    pub(super) stopped: AtomicBool,
    /// Wakes retry waits when the pool is dropped.
    pub(super) stop: tokio::sync::Notify,
    /// The shared plain-SQL subscriptions ([`EngineClient::watch_sql`]).
    pub(super) watches: crate::listen::Watches,
    /// Whether the engine was last seen past the lag reads may trust, to
    /// log the crossing each way once.
    pub(super) was_behind: AtomicBool,
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
    pub(super) fn refused(&self, reason: String) -> Verdict {
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
    pub(super) const TRANSIENT: [&'static str; 10] = [
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
pub(super) struct WorkerState {
    /// The id the next subscription is sent under; never reused.
    next_id: u64,
    /// Each held key's subscription id.
    pub(super) by_key: HashMap<QueryKey, u64>,
    /// Each subscription id's key.
    pub(super) by_id: HashMap<u64, QueryKey>,
    /// Subscriptions the engine has not accepted yet: a subscribe
    /// waiting for its verdict, with where to deliver it, or `None` for
    /// a key resubscribed after a reconnect, which nobody waits for. A
    /// read of a pending key goes live: until the engine accepts, a
    /// commit can land that no frame reports.
    pub(super) pending: HashMap<u64, Option<SyncSender<Reply>>>,
    /// Subscriptions served from the engine's rows, each behind its own
    /// lock so a delta to one is applied without holding up the rest.
    pub(super) materialized: HashMap<u64, Arc<LiveStore>>,
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
pub(super) struct LiveStore {
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
                    worker.fall_back(self.id, &self.key, error.to_string());
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
pub(super) enum Greeting {
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
pub(super) enum Target {
    /// An engine's S2S endpoint, at this URL.
    Remote(String),
    /// An engine in this process.
    #[cfg(feature = "embedded")]
    Local(streamgres::embedded::Engine),
}

impl Target {
    /// The remote engine's URL; `None` for one in this process.
    pub(super) fn url(&self) -> Option<&str> {
        match self {
            Target::Remote(url) => Some(url),
            #[cfg(feature = "embedded")]
            Target::Local(_) => None,
        }
    }
}

/// One connection to the engine and the subscriptions it carries.
pub(super) struct Worker {
    pub(super) index: usize,
    /// The worker itself, for the stores it hands the cache.
    this: Weak<Worker>,
    pub(super) target: Target,
    verdict_timeout: Duration,
    pub(super) outgoing: mpsc::UnboundedSender<Outgoing>,
    pub(super) connected: AtomicBool,
    /// How many keys this worker holds, kept beside the state so the
    /// pool can pick the least loaded worker without locking each one.
    pub(super) held: AtomicUsize,
    /// The most subscriptions the engine takes on one connection, from
    /// its `hello`; 0 when it named no limit.
    pub(super) room: AtomicUsize,
    /// Why the latest attempt to connect failed, until one succeeds.
    pub(super) connect_error: Mutex<Option<String>>,
    /// What the engine on the socket said it streams, from its `hello`.
    pub(super) greeting: Mutex<Greeting>,
    /// The engine's lag as its latest `lag` frame said, and when that
    /// came; `None` until one has, and while the engine measures none.
    pub(super) lag: Mutex<Option<(Duration, Instant)>>,
    /// The position up to which every change has reached this worker, as
    /// the engine's latest `lag` frame said (frames arrive in order, so
    /// every change before it has been applied here); 0 until one has,
    /// and while the socket is down.
    pub(super) position: AtomicU64,
    pub(super) state: Mutex<WorkerState>,
    pub(super) shared: Arc<Shared>,
}

impl Worker {
    /// A worker and the receiving end of its outgoing frames, not yet
    /// running.
    pub(super) fn new(
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
    pub(super) fn start(
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
    pub(super) fn wrong_database(&self, key: &QueryKey) -> Option<String> {
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
    pub(super) fn subscribe(&self, key: &QueryKey) -> Outcome {
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
    pub(super) fn materialize_key(&self, key: &QueryKey, snapshot: Snapshot) -> Verdict {
        let id = self.state.lock().expect("state").by_key.get(key).copied();
        match id {
            Some(id) => self.materialize(id, snapshot),
            None => {
                Verdict::Refused("the subscription ended before its rows were built".to_string())
            }
        }
    }

    /// Ends the key's subscription on this worker and with the engine.
    pub(super) fn unsubscribe(&self, key: &QueryKey) {
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
                listening.end(Ended::Unserved(format!(
                    "the engine no longer serves the query's rows: {reason}"
                )));
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
        materialized.set_work_budget(shared.max_join_work);
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
            if let Err(error) = listening.built(&materialized) {
                drop(store);
                drop(state);
                if let BuildError::OverBudget(_) = error {
                    listening.end(Ended::WorkBudget(error.to_string()));
                }
                return fallback(error.to_string());
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
                shared.fell_back(error.to_string());
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
    pub(super) fn has_room(&self) -> bool {
        let room = self.room.load(Ordering::Relaxed);
        room == 0 || self.held.load(Ordering::Relaxed) < room
    }

    /// The refusal while this socket is down, with why it last failed to
    /// connect when it has.
    pub(super) fn not_connected(&self) -> String {
        match &*self.connect_error.lock().expect("connect_error") {
            Some(error) => format!("the engine is not connected: {error}"),
            None => "the engine is not connected".to_string(),
        }
    }

    /// The socket is up: the worker counts as connected, and returns what
    /// it still holds, to resubscribe under the same ids. Their cache
    /// entries went when the socket did, and each is pending again until
    /// the engine accepts it, so nothing stale is served meanwhile.
    pub(super) fn up(&self) -> Vec<(u64, QueryKey)> {
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
    pub(super) fn lag(&self) -> Option<Duration> {
        if !self.connected.load(Ordering::Acquire) {
            return None;
        }
        let (lag, heard) = (*self.lag.lock().expect("lag"))?;
        Some(lag + heard.elapsed())
    }

    /// Records why the latest connect failed, or that one succeeded.
    pub(super) fn connect_failed(&self, error: Option<String>) {
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
pub(super) fn bind_json(bind: &crate::key::Bind) -> Json {
    json!({
        "oid": bind.type_oid,
        "bytes": bind.bytes.as_ref().map(|bytes| BASE64.encode(bytes)),
    })
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
pub(super) fn disconnected(worker: &Worker) {
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
pub(super) fn subscribe_frame(id: u64, sql: &str, binds: &[Json]) -> String {
    json!(["subscribe", {"id": id, "sql": sql, "binds": binds, "rows": true}]).to_string()
}

/// One frame from the engine, on one worker's socket.
pub(super) fn inbound(worker: &Worker, text: &str) {
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
                    listening.end(Ended::Unserved(reason));
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
        listening.end(Ended::Stopped("the engine stopped following the query".to_string()));
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
pub(super) fn on_hello(worker: &Worker, body: &Json) {
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
                listening.end(Ended::Stopped(format!(
                    "the engine stopped following the query: {reason}"
                )));
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
