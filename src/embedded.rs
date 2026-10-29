//! The engine inside a backend's own process: what the server runs for
//! the S2S endpoint, without the server. [`Engine::start`] returns at
//! once; a supervisor thread brings an engine up in the background and,
//! whenever it fails (its feed lost, a panic, a startup that could not
//! finish), tears it down and starts a fresh one, waiting longer each
//! time it fails again soon. A backend talks to it through
//! [`Engine::connect`]: a [`Session`] of the same text frames the S2S
//! endpoint speaks, over channels, served by the same
//! [`crate::live::session`].
//!
//! The engine owns its threads: one for the engine itself (its service
//! runs on a single-threaded runtime), one for the change feed, and a
//! small runtime of its own for storage reads, the live task and the
//! sessions. It never runs on the host's runtime, so a host with none, or
//! a blocking Diesel connection, can use it as well as an async one.
//!
//! By default the feed streams from a temporary slot, which PostgreSQL
//! drops when the feed's connection ends, however it ends: a process
//! killed outright leaves no slot holding WAL, and dropping an [`Engine`]
//! needs no cleanup. The price is that a lost feed cannot be resumed, so
//! the engine starts over on a fresh slot and snapshot; sessions end with
//! it, and their backends reconnect. A permanent slot ([`Slot`]) outlives
//! a dropped connection instead, so the engine serves on through it, at
//! the price of a slot a dead process leaves behind until it is swept.
//!
//! An embedded engine runs against the primary, the database its backend
//! writes to: it is for an application of one or a few processes, each
//! running its own engine with its own slot (a fleet shares a few S2S
//! servers instead, whose cost on the database does not grow with it).
//! Before each start the supervisor refuses a standby, sets the database
//! up when it is not ([`Setup`]), and checks it has room for
//! [`SLOTS_PER_ENGINE`] more replication slots and connections, refusing
//! to take the last ones another consumer may need. Log lines go to the
//! `log` facade ([`crate::log::to_facade`]).

use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use futures_util::stream;
use tokio::sync::{mpsc, oneshot, watch};

use crate::env::Env;
use crate::live::{self, Drained, Limits, Outgoing};
use crate::log::{log_info, log_warn};
use crate::model::{Lsn, TableName};
use crate::plan::{PlanCache, Policy, Side};
use crate::shutdown::Shutdown;
use crate::stats::Stats;
use crate::sync::pg::sql::quote_ident;
use crate::sync::pg::{
    DEFAULT_READ_ROW_LIMIT, Keepalive, Settings, Transport, ddl, estimate_cache, threads,
};
use crate::sync::{CatalogHandle, Event};

/// Replication slots, and replication connections, one engine holds at
/// its peak: its feed's, and up to two read snapshots' while they rotate.
pub const SLOTS_PER_ENGINE: i64 = 3;

/// How many frames a session's backend may have queued for the engine
/// before its sends wait.
const INBOUND_QUEUE: usize = 1024;

/// How long a stopping engine's threads and runtime get to finish.
const STOP_GRACE: Duration = Duration::from_secs(5);

/// How long an engine must have served before its failure counts as a
/// new one, and the next start waits the shortest time again.
const STEADY: Duration = Duration::from_secs(60);

/// How long connecting to check the database may take, unless the DSN
/// says (`connect_timeout`).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How often an engine on a permanent slot drops the slots ended
/// processes left behind ([`Slot::Permanent`]).
const SWEEP_EVERY: Duration = Duration::from_secs(60);

/// The advisory lock the engines of one database take to set it up, so
/// processes starting together install the setup once.
const SETUP_LOCK: i64 = 0x5354_5245_414d_5350; // "STREAMSP"

/// How an engine gets the database set up for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Setup {
    /// Creates the publication (`FOR ALL TABLES`) and installs the
    /// schema-change trigger stack ([`setup_sql`]) when either is
    /// missing, once, under an advisory lock; what is there is left
    /// alone. Needs a role allowed to create both: a superuser, or a
    /// managed database's admin role.
    Install,
    /// Requires both, as the S2S server does, for a database a migration
    /// sets up.
    Require,
}

/// The kind of replication slot an engine's feed streams from. Each
/// start makes a fresh one either way: the engine holds its rows in
/// memory, so a start always begins from a fresh snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Slot {
    /// Dropped by PostgreSQL when the feed's connection ends, however it
    /// ends, so nothing is ever left holding WAL; but a dropped
    /// connection costs a fresh start: a new snapshot, the sessions
    /// ended, their caches refilled.
    Temporary,
    /// Kept through a dropped connection: the feed reopens it and the
    /// engine serves on. The engine drops it when it stops or starts
    /// over. A process that dies without stopping leaves it holding WAL
    /// until an engine on the same database drops it, at its start and
    /// every minute after, once it has been inactive for `cleanup_age`
    /// (PostgreSQL 17 or later; zero never drops it, leaving the WAL to
    /// `max_slot_wal_keep_size`).
    Permanent { cleanup_age: Duration },
}

/// An embedded engine's configuration. [`Config::new`] gives the
/// defaults for a database; [`Config::from_env`] reads the same
/// variables the server does, with defaults sized for a backend's
/// process (a few read connections, one read thread) rather than a
/// server's.
#[derive(Debug, Clone)]
pub struct Config {
    /// The primary the feed, the snapshots and the heartbeats need.
    pub dsn: String,
    /// Where ordinary reads go; may be a PgBouncer transaction pool.
    pub read_dsn: String,
    /// The app and shard the schema-change trigger stack is installed
    /// for ([`setup_sql`]).
    pub app: String,
    pub shard: u32,
    /// How the database gets set up.
    pub setup: Setup,
    /// The kind of slot the feed streams from.
    pub slot: Slot,
    /// The publication the feed streams.
    pub publication: String,
    /// The schemas whose tables are followed.
    pub schemas: Vec<String>,
    /// The schema-change event trigger the engine requires
    /// ([`setup_sql`] installs it), and its messages' prefix.
    pub ddl_trigger: String,
    pub ddl_prefix: String,
    /// How often the engine measures its lag (zero for never).
    pub heartbeat: Duration,
    /// The most rows one read may return.
    pub row_limit: usize,
    /// Tables mirrored in memory.
    pub memory_tables: Vec<TableName>,
    /// How many storage reads may hold a connection at once, and how
    /// many connections stay open while idle.
    pub read_connections: usize,
    pub read_idle_connections: usize,
    /// Threads of the runtime that serves reads and sessions.
    pub read_threads: usize,
    /// How long one storage read may take.
    pub read_timeout: Duration,
    /// How often a fresh snapshot is minted for reads.
    pub snapshot_rotation: Duration,
    /// TCP keepalive on the engine's connections.
    pub keepalive: Keepalive,
    /// What one session may ask the engine to follow.
    pub limits: Limits,
    /// Which side of a join the planner reads first.
    pub join_preferred: Side,
    /// How long, and how many, query plans are kept.
    pub plan_ttl: Duration,
    pub plan_cache: usize,
    /// How long, and how many, row-count estimates are kept.
    pub estimate_ttl: Duration,
    pub estimate_capacity: usize,
    /// The wait before the first restart; it doubles while starts keep
    /// failing, up to `restart_max`.
    pub restart_after: Duration,
    pub restart_max: Duration,
}

impl Config {
    /// The defaults for the database at `dsn`, app `zero`, shard 0.
    pub fn new(dsn: impl Into<String>) -> Config {
        let dsn = dsn.into();
        Config {
            read_dsn: dsn.clone(),
            dsn,
            app: "zero".to_owned(),
            shard: 0,
            setup: Setup::Install,
            slot: Slot::Temporary,
            publication: "xyne_sync_pub".to_owned(),
            schemas: vec!["public".to_owned(), "zero_0".to_owned()],
            ddl_trigger: "zero_ddl_end_0".to_owned(),
            ddl_prefix: "zero/0/ddl".to_owned(),
            heartbeat: Duration::from_millis(500),
            row_limit: DEFAULT_READ_ROW_LIMIT,
            memory_tables: Vec::new(),
            read_connections: 4,
            read_idle_connections: 4,
            read_threads: 1,
            read_timeout: Duration::from_secs(10),
            snapshot_rotation: Duration::from_secs(1),
            keepalive: Keepalive {
                idle: Duration::from_secs(30),
                interval: Duration::from_secs(10),
                retries: 3,
            },
            limits: Limits {
                per_connection: 10_000,
                max_disjuncts: 1024,
                max_list_values: 10_000,
            },
            join_preferred: Side::Parent,
            plan_ttl: Duration::from_secs(600),
            plan_cache: 10_000,
            estimate_ttl: estimate_cache::DEFAULT_TTL,
            estimate_capacity: estimate_cache::DEFAULT_CAPACITY,
            restart_after: Duration::from_secs(1),
            restart_max: Duration::from_secs(30),
        }
    }

    /// The configuration the process environment describes.
    pub fn from_env() -> Result<Config, String> {
        Config::from_lookup(&crate::env::process())
    }

    /// The configuration `env` describes: the server's variable names
    /// (`STREAMGRES_PG_DSN`, `STREAMGRES_APP_ID`, `STREAMGRES_SHARD`,
    /// `STREAMGRES_PUBLICATION`, `STREAMGRES_SCHEMAS`,
    /// `STREAMGRES_READ_CONNECTIONS` and the rest), legacy names
    /// included, an embedded engine's defaults otherwise; and its own,
    /// `STREAMGRES_EMBEDDED_SETUP` (`install`, the default, or
    /// `require`) and `STREAMGRES_EMBEDDED_SLOT` (`temporary`, the
    /// default, or `permanent`, swept after
    /// `STREAMGRES_SLOT_CLEANUP_AGE_MS`, 5 minutes by default).
    pub fn from_lookup<F: Fn(&str) -> Option<String>>(env: &Env<F>) -> Result<Config, String> {
        let dsn = env
            .first(&["STREAMGRES_PG_DSN", "ZERO_UPSTREAM_DB"])
            .ok_or("STREAMGRES_PG_DSN (or ZERO_UPSTREAM_DB) must name the database")?;
        let defaults = Config::new(dsn.clone());
        let app_id = env
            .first(&["STREAMGRES_APP_ID", "ZERO_APP_ID"])
            .unwrap_or_else(|| "zero".to_owned());
        let shard: u32 = match env.first(&["STREAMGRES_SHARD", "ZERO_SHARD_NUM"]) {
            Some(text) => text
                .parse()
                .map_err(|_| format!("STREAMGRES_SHARD must be a number, got `{text}`"))?,
            None => 0,
        };
        let (estimate_ttl, estimate_capacity) = env.estimates();
        let setup = match env.value("STREAMGRES_EMBEDDED_SETUP").as_deref() {
            None | Some("install") => Setup::Install,
            Some("require") => Setup::Require,
            Some(other) => {
                return Err(format!(
                    "STREAMGRES_EMBEDDED_SETUP must be `install` or `require`, got `{other}`"
                ));
            }
        };
        let slot = match env.value("STREAMGRES_EMBEDDED_SLOT").as_deref() {
            None | Some("temporary") => Slot::Temporary,
            Some("permanent") => Slot::Permanent {
                cleanup_age: env.millis("STREAMGRES_SLOT_CLEANUP_AGE_MS", 300_000)?,
            },
            Some(other) => {
                return Err(format!(
                    "STREAMGRES_EMBEDDED_SLOT must be `temporary` or `permanent`, got `{other}`"
                ));
            }
        };
        Ok(Config {
            read_dsn: env.value("STREAMGRES_READ_PG_DSN").unwrap_or_else(|| dsn.clone()),
            setup,
            slot,
            publication: env
                .value("STREAMGRES_PUBLICATION")
                .unwrap_or(defaults.publication.clone()),
            schemas: env.schemas(&app_id, shard),
            ddl_trigger: env
                .value("STREAMGRES_DDL_TRIGGER")
                .unwrap_or_else(|| format!("{app_id}_ddl_end_{shard}")),
            ddl_prefix: env
                .value("STREAMGRES_DDL_PREFIX")
                .unwrap_or_else(|| format!("{app_id}/{shard}/ddl")),
            heartbeat: env.millis("STREAMGRES_HEARTBEAT_MS", 500)?,
            row_limit: env.row_limit()?,
            memory_tables: env.memory_tables(),
            read_connections: env.number("STREAMGRES_READ_CONNECTIONS", defaults.read_connections)?,
            read_idle_connections: env.number(
                "STREAMGRES_READ_IDLE_CONNECTIONS",
                defaults.read_idle_connections,
            )?,
            read_threads: env.positive("STREAMGRES_READ_THREADS", defaults.read_threads)?,
            read_timeout: env.millis("STREAMGRES_READ_TIMEOUT_MS", 10_000)?,
            snapshot_rotation: env.millis("STREAMGRES_SNAPSHOT_ROTATION_MS", 1_000)?,
            keepalive: Keepalive {
                idle: env.millis("STREAMGRES_PG_KEEPALIVE_IDLE_MS", 30_000)?,
                interval: env.millis("STREAMGRES_PG_KEEPALIVE_INTERVAL_MS", 10_000)?,
                retries: env.number("STREAMGRES_PG_KEEPALIVE_RETRIES", 3)?,
            },
            limits: Limits {
                per_connection: env.number(
                    "STREAMGRES_S2S_CONNECTION_SUBSCRIPTIONS",
                    defaults.limits.per_connection,
                )?,
                max_disjuncts: env
                    .number("STREAMGRES_S2S_MAX_DISJUNCTS", defaults.limits.max_disjuncts)?,
                max_list_values: env.number(
                    "STREAMGRES_S2S_MAX_LIST_VALUES",
                    defaults.limits.max_list_values,
                )?,
            },
            join_preferred: env.join_preferred()?,
            plan_ttl: env.millis("STREAMGRES_PLAN_TTL_MS", 600_000)?,
            plan_cache: env.number("STREAMGRES_PLAN_CACHE", defaults.plan_cache)?,
            estimate_ttl,
            estimate_capacity,
            dsn,
            app: app_id,
            shard,
            ..defaults
        })
    }

    /// The engine's settings for one start: a fresh slot name, the feed
    /// on a slot of the configured kind, no table watched.
    fn settings(&self) -> Settings {
        let (temporary_slot, slot_cleanup_age) = match self.slot {
            Slot::Temporary => (true, Duration::ZERO),
            Slot::Permanent { cleanup_age } => (false, cleanup_age),
        };
        Settings {
            dsn: self.dsn.clone(),
            read_dsn: self.read_dsn.clone(),
            slot: threads::slot_name(),
            slot_cleanup_age,
            publication: self.publication.clone(),
            schemas: self.schemas.clone(),
            snapshot_rotation: self.snapshot_rotation,
            read_connections: self.read_connections,
            read_idle_connections: self.read_idle_connections,
            read_timeout: self.read_timeout,
            read_threads: self.read_threads,
            keepalive: self.keepalive,
            watched: Vec::new(),
            ddl_trigger: self.ddl_trigger.clone(),
            ddl_prefix: self.ddl_prefix.clone(),
            heartbeat: self.heartbeat,
            row_limit: self.row_limit,
            memory_tables: self.memory_tables.clone(),
            estimate_ttl: self.estimate_ttl,
            estimate_capacity: self.estimate_capacity,
            temporary_slot,
        }
    }
}

/// The SQL that sets a database up for an engine of app `app`, shard
/// `shard`, streaming `publication`: the schema-change event trigger
/// stack the engine requires, for a migration to run when the engine
/// does not install it itself ([`Setup::Require`]). The publication is
/// the deployment's, created beside it.
pub fn setup_sql(app: &str, shard: u32, publication: &str) -> String {
    crate::sync::pg::ddl_triggers::trigger_stack_sql(app, shard, &[publication])
}

/// What an engine is doing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    /// Coming up: sessions are refused until it serves.
    Starting,
    /// Following queries.
    Serving,
    /// Down for the reason given; a fresh start is on its way.
    Down(String),
    /// Stopped for good.
    Stopped,
}

/// Why an embedded engine could not start or become ready.
#[derive(Debug)]
#[non_exhaustive]
pub enum StartError {
    Spawn(std::io::Error),
    Failed(String),
    Stopped,
    Timeout,
}

impl std::fmt::Display for StartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spawn(error) => write!(f, "starting the embedded engine thread: {error}"),
            Self::Failed(reason) => write!(f, "embedded engine startup failed: {reason}"),
            Self::Stopped => f.write_str("embedded engine stopped before becoming ready"),
            Self::Timeout => f.write_str("timed out waiting for the embedded engine"),
        }
    }
}

impl std::error::Error for StartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Spawn(error) => Some(error),
            _ => None,
        }
    }
}

/// An embedded engine, cheap to clone. The last clone dropped stops it
/// without waiting; [`Engine::stop`] waits.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
}

/// What the clones share.
struct Inner {
    states: watch::Receiver<State>,
    current: Arc<Mutex<Option<Arc<Instance>>>>,
    stop: watch::Sender<bool>,
    supervisor: Mutex<Option<JoinHandle<()>>>,
    stats: Arc<Stats>,
}

/// A running engine, as sessions reach it.
struct Instance {
    context: Arc<live::Context>,
    slot: String,
    io: tokio::runtime::Handle,
    ready: watch::Receiver<bool>,
}

impl Engine {
    /// Starts an engine in the background and returns at once; it serves
    /// once its feed has passed its first snapshot ([`Engine::state`]).
    /// The process's log lines go to the `log` facade from here on.
    pub fn start(config: Config) -> Engine {
        Self::try_start(config).expect("starting the embedded engine thread")
    }

    /// Start in the background, reporting thread creation failures. Use
    /// [`Engine::ready`] to check database and replication setup as well.
    pub fn try_start(config: Config) -> Result<Engine, StartError> {
        crate::log::to_facade();
        let stats = Stats::shared_with_metric_prefix("streamgres".to_owned());
        Stats::install(&stats);
        let (state_tx, states) = watch::channel(State::Starting);
        let (stop, stop_rx) = watch::channel(false);
        let current = Arc::new(Mutex::new(None));
        let supervised = Supervisor {
            config,
            stats: Arc::clone(&stats),
            state: state_tx,
            current: Arc::clone(&current),
            stop: stop_rx,
        };
        let supervisor = std::thread::Builder::new()
            .name("streamgres-embedded".to_owned())
            .spawn(move || supervised.run())
            .map_err(StartError::Spawn)?;
        Ok(Engine {
            inner: Arc::new(Inner {
                states,
                current,
                stop,
                supervisor: Mutex::new(Some(supervisor)),
                stats,
            }),
        })
    }

    /// Wait for the first ready engine, bounded by `timeout`. A failed
    /// attempt is returned immediately; the supervisor continues retrying.
    /// Cancelling this wait does not stop the engine.
    pub async fn ready(&self, timeout: Duration) -> Result<(), StartError> {
        let mut states = self.states();
        tokio::time::timeout(timeout, async move {
            loop {
                let state = states.borrow_and_update().clone();
                match state {
                    State::Serving => return Ok(()),
                    State::Down(reason) => return Err(StartError::Failed(reason)),
                    State::Stopped => return Err(StartError::Stopped),
                    State::Starting => {}
                }
                states.changed().await.map_err(|_| StartError::Stopped)?;
            }
        })
        .await
        .map_err(|_| StartError::Timeout)?
    }

    /// What the engine is doing now.
    pub fn state(&self) -> State {
        self.inner.states.borrow().clone()
    }

    /// Every change of [`Engine::state`].
    pub fn states(&self) -> watch::Receiver<State> {
        self.inner.states.clone()
    }

    /// The replication slot the running engine's feed streams from (a
    /// fresh one each start, gone with the engine), while one runs.
    pub fn slot(&self) -> Option<String> {
        let current = self.inner.current.lock().expect("current");
        current.as_ref().map(|instance| instance.slot.clone())
    }

    /// The engine's measurements, across its restarts.
    pub fn stats(&self) -> &Arc<Stats> {
        &self.inner.stats
    }

    /// A session with the engine serving now; why not, while it is not.
    /// The session ends when the engine does.
    pub fn connect(&self) -> Result<Session, String> {
        let instance = self
            .inner
            .current
            .lock()
            .expect("current")
            .clone()
            .filter(|instance| *instance.ready.borrow())
            .ok_or_else(|| format!("the embedded engine is not serving: {:?}", self.state()))?;
        let (to_engine, inbound) = mpsc::channel::<String>(INBOUND_QUEUE);
        let (out, frames) = Outgoing::new();
        let drained = out.drained();
        let inbound = stream::unfold(inbound, |mut inbound| async move {
            inbound.recv().await.map(|frame| (frame, inbound))
        });
        let context = Arc::clone(&instance.context);
        instance.io.spawn(async move {
            live::session(&context, Box::pin(inbound), out).await;
        });
        Ok(Session {
            to_engine,
            frames,
            drained,
        })
    }

    /// Stops the engine and waits for its threads, its feed's connection
    /// (and with it the slot) included.
    pub fn stop(&self) {
        self.inner.stop.send_replace(true);
        if let Some(supervisor) = self.inner.supervisor.lock().expect("supervisor").take() {
            let _ = supervisor.join();
        }
    }
}

impl Drop for Inner {
    /// Tells the supervisor to stop, without waiting for it.
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

/// One backend's session with an embedded engine: the S2S frames over
/// channels. Frames sent go to the engine; frames received come from it,
/// and each received is released from the session's byte bound.
pub struct Session {
    to_engine: mpsc::Sender<String>,
    frames: mpsc::Receiver<String>,
    drained: Drained,
}

impl Session {
    /// Where the backend's frames go; closed once the session ends.
    pub fn sender(&self) -> mpsc::Sender<String> {
        self.to_engine.clone()
    }

    /// The engine's next frame; `None` once the session has ended.
    pub async fn recv(&mut self) -> Option<String> {
        let frame = self.frames.recv().await?;
        self.drained.sent(frame.len());
        Some(frame)
    }
}

/// The supervisor thread's state.
struct Supervisor {
    config: Config,
    stats: Arc<Stats>,
    state: watch::Sender<State>,
    current: Arc<Mutex<Option<Arc<Instance>>>>,
    stop: watch::Receiver<bool>,
}

impl Supervisor {
    /// Starts engines until stopped: each failure tears its engine down
    /// and, after a wait that doubles while starts keep failing soon,
    /// starts a fresh one.
    fn run(mut self) {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                self.state.send_replace(State::Down(format!("tokio: {error}")));
                return;
            }
        };
        let mut wait = self.config.restart_after;
        while !*self.stop.borrow() {
            self.state.send_replace(State::Starting);
            let started = Instant::now();
            let shutdown = Shutdown::new();
            let settings = self.config.settings();
            let launched = runtime.block_on(async {
                let launch = launch(&self.config, settings.clone(), &self.stats, shutdown.clone());
                tokio::pin!(launch);
                let mut stop = self.stop.clone();
                tokio::select! {
                    launched = &mut launch => launched,
                    _ = stop.wait_for(|stopped| *stopped) => {
                        shutdown.request();
                        launch.await
                    }
                }
            });
            let reason = match launched {
                Ok(running) => {
                    *self.current.lock().expect("current") = Some(Arc::clone(&running.instance));
                    let reason = runtime.block_on(running.watch(&mut self.stop, &self.state));
                    *self.current.lock().expect("current") = None;
                    if let Some(reason) = &reason {
                        self.state.send_replace(State::Down(reason.clone()));
                    }
                    running.tear_down(&runtime);
                    reason
                }
                Err(_) if *self.stop.borrow() => None,
                Err(reason) => {
                    self.state.send_replace(State::Down(reason.clone()));
                    Some(reason)
                }
            };
            self.release(&runtime, &settings.slot);
            let Some(reason) = reason else {
                break;
            };
            log_warn!("the embedded engine is down: {reason}; starting a fresh one in {wait:?}");
            wait = if started.elapsed() >= STEADY {
                self.config.restart_after
            } else {
                wait
            };
            let mut stop = self.stop.clone();
            runtime.block_on(async {
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    _ = stop.wait_for(|stopped| *stopped) => {}
                }
            });
            wait = (wait * 2).min(self.config.restart_max);
        }
        self.state.send_replace(State::Stopped);
        log_info!("the embedded engine stopped");
    }

    /// Drops `slot`, an ended start's permanent slot, if it made one
    /// (ending a backend still making it); the next start makes a fresh
    /// one. One that cannot be dropped now is left to a later sweep.
    fn release(&self, runtime: &tokio::runtime::Runtime, slot: &str) {
        if self.config.slot == Slot::Temporary {
            return;
        }
        let dropped = runtime.block_on(async {
            tokio::time::timeout(STOP_GRACE, Transport::drop_slot_only(&self.config.dsn, slot)).await
        });
        match dropped {
            Ok(Ok(())) => {}
            Ok(Err(error)) => log_warn!(
                "could not drop the embedded engine's slot {slot}, if the start got as far as making it: {error}; a later engine's sweep drops it"
            ),
            Err(_) => log_warn!(
                "dropping the embedded engine's slot {slot}, if the start got as far as making it, took over {STOP_GRACE:?}; a later engine's sweep drops it"
            ),
        }
    }
}

/// A started engine: what sessions reach, and what stopping it takes.
struct Running {
    instance: Arc<Instance>,
    io: tokio::runtime::Runtime,
    engine: JoinHandle<()>,
    feed: threads::FeedHandle,
    shutdown: Shutdown,
}

impl Running {
    /// Waits for the engine to serve, then for it to fail or the stop:
    /// why it failed, or `None` for the stop.
    async fn watch(&self, stop: &mut watch::Receiver<bool>, state: &watch::Sender<State>) -> Option<String> {
        let mut ready = self.instance.ready.clone();
        let mut serving = false;
        loop {
            tokio::select! {
                _ = stop.wait_for(|stopped| *stopped) => return None,
                _ = self.shutdown.wait() => {
                    return Some(self.shutdown.failure().unwrap_or_else(|| "the engine stopped".to_owned()));
                }
                changed = ready.wait_for(|ready| *ready), if !serving => {
                    if changed.is_err() {
                        return Some("the engine's readiness was lost".to_owned());
                    }
                    serving = true;
                    log_info!("the embedded engine is serving");
                    state.send_replace(State::Serving);
                }
            }
        }
    }

    /// Stops the feed (a temporary slot goes with its connection; the
    /// supervisor drops a permanent one), then the engine thread and the
    /// runtime its sessions run on, each given [`STOP_GRACE`].
    fn tear_down(self, runtime: &tokio::runtime::Runtime) {
        self.shutdown.request();
        if let Err(error) = runtime.block_on(async {
            tokio::time::timeout(STOP_GRACE, threads::stop_feed(self.feed))
                .await
                .unwrap_or_else(|_| Err(format!("the feed did not stop within {STOP_GRACE:?}")))
        }) {
            log_warn!("stopping the embedded engine's feed: {error}");
        }
        join_within(self.engine, "engine");
        self.io.shutdown_timeout(STOP_GRACE);
    }
}

/// Joins `thread`, giving up after [`STOP_GRACE`]: a thread that does not
/// stop is left to the process.
fn join_within(thread: JoinHandle<()>, name: &str) {
    let deadline = Instant::now() + STOP_GRACE;
    while !thread.is_finished() {
        if Instant::now() >= deadline {
            log_warn!("the embedded engine's {name} thread did not stop within {STOP_GRACE:?}");
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if thread.join().is_err() {
        log_warn!("the embedded engine's {name} thread panicked while stopping");
    }
}

/// Brings one engine up: the database checked and set up, the catalog
/// loaded, the schema-change trigger required, the engine thread started,
/// and the live task serving sessions once the feed has passed the first
/// snapshot; on a permanent slot, the sweep of ended processes' slots
/// started. A `shutdown` requested meanwhile ends a wait for the slot.
async fn launch(
    config: &Config,
    settings: Settings,
    stats: &Arc<Stats>,
    shutdown: Shutdown,
) -> Result<Running, String> {
    prepare(config).await?;
    let catalog = Arc::new(CatalogHandle::new(
        threads::load_catalog_at(&settings.dsn, &settings.schemas).await?,
    ));
    threads::require_ddl_trigger(&settings.dsn, &settings.ddl_trigger).await?;
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(settings.read_threads.max(1))
        .enable_all()
        .thread_name("streamgres-embedded-io")
        .build()
        .map_err(|error| format!("tokio: {error}"))?;
    let (started_tx, started_rx) = oneshot::channel();
    let engine = {
        let catalog = Arc::clone(&catalog);
        let stats = Arc::clone(stats);
        let reads = io.handle().clone();
        let shutdown = shutdown.clone();
        let settings = settings.clone();
        std::thread::Builder::new()
            .name("streamgres-engine".to_owned())
            .spawn(move || run_engine(settings, catalog, reads, stats, shutdown, started_tx))
            .map_err(|error| format!("engine thread: {error}"))?
    };
    let started = match started_rx.await {
        Ok(Ok(started)) => started,
        Ok(Err(error)) => {
            join_within(engine, "engine");
            io.shutdown_background();
            return Err(format!("starting the engine: {error}"));
        }
        Err(_) => {
            join_within(engine, "engine");
            io.shutdown_background();
            return Err("the engine thread ended before it was up".to_owned());
        }
    };
    let threads::Started {
        commands,
        mut events,
        storage,
        feed,
    } = started;
    let Some(events) = events.pop() else {
        shutdown.request();
        let _ = threads::stop_feed(feed).await;
        join_within(engine, "engine");
        io.shutdown_background();
        return Err("the engine started without its sink".to_owned());
    };
    if !settings.temporary_slot && !settings.slot_cleanup_age.is_zero() {
        io.spawn(sweep(settings.dsn.clone(), settings.slot_cleanup_age));
    }
    let (readiness, ready) = watch::channel(false);
    let (gated, gated_events) = mpsc::unbounded_channel();
    io.spawn(gate(events, gated, readiness));
    let (hub, requests) = live::Hub::new();
    let delivered = hub.delivered();
    let context = Arc::new(live::Context {
        hub,
        catalog: Arc::clone(&catalog),
        storage,
        plans: Arc::new(PlanCache::new(config.plan_ttl, config.plan_cache)),
        policy: Policy {
            limit: u64::try_from(config.row_limit).unwrap_or(u64::MAX),
            preferred: config.join_preferred,
        },
        limits: config.limits,
        stats: Arc::clone(stats),
    });
    io.spawn(live::run(
        catalog,
        0,
        commands,
        gated_events,
        requests,
        ready.clone(),
        config.limits.per_connection,
        Arc::clone(stats),
        delivered,
    ));
    Ok(Running {
        instance: Arc::new(Instance {
            context,
            slot: settings.slot.clone(),
            io: io.handle().clone(),
            ready,
        }),
        io,
        engine,
        feed,
        shutdown,
    })
}

/// The engine thread: the service on a single-threaded runtime, as the
/// server runs it, its start reported on `started` and its end, for any
/// reason but a stop, failing `shutdown`.
fn run_engine(
    settings: Settings,
    catalog: Arc<CatalogHandle>,
    reads: tokio::runtime::Handle,
    stats: Arc<Stats>,
    shutdown: Shutdown,
    started: oneshot::Sender<Result<threads::Started, String>>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = started.send(Err(format!("tokio: {error}")));
            return;
        }
    };
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async move {
        match threads::start(&settings, catalog, reads, 1, &[0], stats, shutdown.clone()).await {
            Ok((engine, service)) => {
                let _ = started.send(Ok(engine));
                match service.await {
                    Ok(_) if shutdown.requested() => {}
                    Ok(_) => shutdown.fail("the engine's service stopped"),
                    Err(error) => shutdown.fail(format!("the engine's service panicked: {error}")),
                }
            }
            Err(error) => {
                shutdown.fail(format!("starting the engine: {error}"));
                let _ = started.send(Err(error));
            }
        }
    });
}

/// Passes the engine's events on to the live task, and says the engine
/// is ready once its position has covered the storage's first snapshot
/// (`floor`), the rule the server's group threads apply.
async fn gate(
    mut events: mpsc::UnboundedReceiver<Event>,
    out: mpsc::UnboundedSender<Event>,
    readiness: watch::Sender<bool>,
) {
    while let Some(event) = events.recv().await {
        if let Event::Committed {
            position, floor, ..
        } = &event
            && !*readiness.borrow()
            && covered(*position, *floor)
        {
            readiness.send_replace(true);
        }
        if out.send(event).is_err() {
            return;
        }
    }
}

/// Every [`SWEEP_EVERY`], drops the permanent slots of ended processes on
/// the database `dsn` once they have been inactive for `age`, as the
/// engine's start did: a process that died leaves its slot holding WAL.
async fn sweep(dsn: String, age: Duration) {
    loop {
        tokio::time::sleep(SWEEP_EVERY).await;
        if let Err(error) = Transport::cleanup_inactive_slots(&dsn, age).await {
            log_warn!("dropping the inactive slots of ended processes: {error}");
        }
    }
}

/// Whether an engine at `position` has covered the storage's first
/// snapshot at `floor` (zero until one is taken).
fn covered(position: Lsn, floor: Lsn) -> bool {
    floor.0 != 0 && position >= floor
}

/// Readies the database for an engine, on one connection: an error on a
/// standby, an embedded engine being for the primary its backend writes
/// to; with [`Setup::Install`], the publication and the trigger stack
/// installed when either is missing (the checks after it require both);
/// and room for the engine's slots.
async fn prepare(config: &Config) -> Result<(), String> {
    let mut connect: tokio_postgres::Config = config
        .dsn
        .parse()
        .map_err(|error| format!("STREAMGRES_PG_DSN: {error}"))?;
    if connect.get_connect_timeout().is_none() {
        connect.connect_timeout(CONNECT_TIMEOUT);
    }
    let (client, connection) = connect
        .connect(tokio_postgres::NoTls)
        .await
        .map_err(|error| format!("connecting to the database: {error}"))?;
    let connection = tokio::spawn(async move {
        let _ = connection.await;
    });
    let prepared = match prepare_on(&client, config).await {
        Ok(()) => room_for_slots(&client).await,
        failed => failed,
    };
    drop(client);
    let _ = connection.await;
    prepared
}

/// [`prepare`] over `client`.
async fn prepare_on(client: &tokio_postgres::Client, config: &Config) -> Result<(), String> {
    let standby: bool = client
        .query_one("SELECT pg_is_in_recovery()", &[])
        .await
        .map_err(|error| format!("asking whether the database is a standby: {error}"))?
        .get(0);
    if standby {
        return Err("the database is a standby; an embedded engine runs against the primary its backend writes to (an engine on a standby is an S2S server's choice)".to_owned());
    }
    if config.setup == Setup::Require || is_set_up(client, config).await? {
        return Ok(());
    }
    if config.ddl_trigger != format!("{}_ddl_end_{}", config.app, config.shard)
        || config.ddl_prefix != format!("{}/{}/ddl", config.app, config.shard)
    {
        return Err(format!(
            "the event trigger `{}` and prefix `{}` are not the ones the setup installs for app `{}`, shard {}; install them yourself and set STREAMGRES_EMBEDDED_SETUP=require",
            config.ddl_trigger, config.ddl_prefix, config.app, config.shard
        ));
    }
    client
        .batch_execute("BEGIN")
        .await
        .map_err(|error| format!("setting the database up: {error}"))?;
    let installed = install(client, config).await;
    let ended = if installed.is_ok() { "COMMIT" } else { "ROLLBACK" };
    let ended = client.batch_execute(ended).await;
    let installed = installed?;
    ended.map_err(|error| format!("setting the database up: {error}"))?;
    if !installed.is_empty() {
        log_info!(
            "set the database up for the embedded engine: created {}",
            installed.join(" and ")
        );
    }
    Ok(())
}

/// Whether the database has the publication and an enabled trigger.
async fn is_set_up(client: &tokio_postgres::Client, config: &Config) -> Result<bool, String> {
    let published = publication_exists(client, &config.publication)
        .await
        .map_err(|error| format!("looking for the publication: {error}"))?;
    let triggered = ddl::trigger_present(client, &config.ddl_trigger)
        .await
        .map_err(|error| format!("looking for the event trigger: {error}"))?;
    Ok(published && triggered && !outdated(client, config).await?)
}

/// Whether the installed trigger stack predates what the engine reads
/// in its messages ([`ddl::stack_outdated`]).
async fn outdated(client: &tokio_postgres::Client, config: &Config) -> Result<bool, String> {
    ddl::stack_outdated(client, &config.ddl_trigger)
        .await
        .map_err(|error| format!("looking at the trigger stack: {error}"))
}

/// Whether the publication `name` exists.
async fn publication_exists(
    client: &tokio_postgres::Client,
    name: &str,
) -> Result<bool, tokio_postgres::Error> {
    Ok(client
        .query_opt("SELECT 1 FROM pg_publication WHERE pubname = $1", &[&name])
        .await?
        .is_some())
}

/// Inside the open transaction, under [`SETUP_LOCK`]: creates what is
/// still missing once the lock is held (another process may have set the
/// database up while this one waited). Returns what it created.
async fn install(client: &tokio_postgres::Client, config: &Config) -> Result<Vec<&'static str>, String> {
    let failed = |error: tokio_postgres::Error| {
        format!(
            "setting the database up: {error} (this needs a role that may create publications and event triggers; run `setup_sql` and create the publication as one, and set STREAMGRES_EMBEDDED_SETUP=require)"
        )
    };
    client
        .execute("SELECT pg_advisory_xact_lock($1)", &[&SETUP_LOCK])
        .await
        .map_err(failed)?;
    let mut installed = Vec::new();
    if !publication_exists(client, &config.publication)
        .await
        .map_err(failed)?
    {
        client
            .batch_execute(&format!(
                "CREATE PUBLICATION {} FOR ALL TABLES",
                quote_ident(&config.publication)
            ))
            .await
            .map_err(failed)?;
        installed.push("the publication");
    }
    let triggered = ddl::trigger_present(client, &config.ddl_trigger)
        .await
        .map_err(|error| format!("looking for the event trigger: {error}"))?;
    if !triggered || outdated(client, config).await? {
        client
            .batch_execute(&setup_sql(&config.app, config.shard, &config.publication))
            .await
            .map_err(failed)?;
        installed.push(if triggered {
            "the schema-change trigger stack, updated"
        } else {
            "the schema-change trigger stack"
        });
    }
    Ok(installed)
}

/// An error unless the database has room for [`SLOTS_PER_ENGINE`] more
/// replication slots and replication connections: an engine that took
/// the last ones would starve another consumer, a replica or another
/// engine, of them.
async fn room_for_slots(client: &tokio_postgres::Client) -> Result<(), String> {
    let row = client
        .query_one(
            "SELECT current_setting('max_replication_slots')::int8 - (SELECT count(*) FROM pg_replication_slots), \
                    current_setting('max_wal_senders')::int8 - (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'walsender')",
            &[],
        )
        .await
        .map_err(|error| format!("reading the replication limits: {error}"))?;
    let (slots, senders): (i64, i64) = (row.get(0), row.get(1));
    if slots < SLOTS_PER_ENGINE || senders < SLOTS_PER_ENGINE {
        return Err(format!(
            "the database has room for {slots} more replication slots and {senders} more replication connections; an engine needs {SLOTS_PER_ENGINE} of each (raise max_replication_slots and max_wal_senders, or run fewer engines)"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    fn waiting_engine(state: State) -> (Engine, watch::Sender<State>) {
        let (state_tx, states) = watch::channel(state);
        let (stop, _) = watch::channel(false);
        (
            Engine {
                inner: Arc::new(Inner {
                    states,
                    current: Arc::new(Mutex::new(None)),
                    stop,
                    supervisor: Mutex::new(None),
                    stats: Stats::shared_with_metric_prefix("test".into()),
                }),
            },
            state_tx,
        )
    }

    #[tokio::test]
    async fn startup_wait_reports_failure_stop_timeout_and_readiness() {
        let (engine, states) = waiting_engine(State::Starting);
        assert!(matches!(
            engine.ready(Duration::from_millis(1)).await,
            Err(StartError::Timeout)
        ));
        states.send_replace(State::Down("permission denied".into()));
        assert!(
            matches!(engine.ready(Duration::from_secs(1)).await, Err(StartError::Failed(reason)) if reason == "permission denied")
        );
        states.send_replace(State::Starting);
        let ready = engine.ready(Duration::from_secs(1));
        let change = async {
            tokio::task::yield_now().await;
            states.send_replace(State::Serving);
        };
        let (result, ()) = tokio::join!(ready, change);
        assert!(result.is_ok());
        states.send_replace(State::Stopped);
        assert!(matches!(
            engine.ready(Duration::from_secs(1)).await,
            Err(StartError::Stopped)
        ));
        drop(states);
        assert!(matches!(
            engine.ready(Duration::from_secs(1)).await,
            Err(StartError::Stopped)
        ));
    }

    use super::*;

    /// The server's names configure an embedded engine too, legacy ones
    /// included; what is not set takes the embedded defaults, sized for a
    /// backend's process.
    #[test]
    fn the_servers_variables_configure_it() {
        let env = Env::new(|name: &str| match name {
            "STREAMGRES_PG_DSN" => Some("postgres://primary/app".to_owned()),
            "XYNE_SYNC_APP_ID" => Some("demo".to_owned()),
            "STREAMGRES_PUBLICATION" => Some("demo_pub".to_owned()),
            "STREAMGRES_READ_THREADS" => Some("3".to_owned()),
            "STREAMGRES_S2S_MAX_DISJUNCTS" => Some("64".to_owned()),
            _ => None,
        });
        let config = Config::from_lookup(&env).unwrap();
        assert_eq!(config.dsn, "postgres://primary/app");
        assert_eq!(config.read_dsn, config.dsn);
        assert_eq!(config.schemas, vec!["public".to_owned(), "demo_0".to_owned()]);
        assert_eq!(config.ddl_trigger, "demo_ddl_end_0");
        assert_eq!(config.ddl_prefix, "demo/0/ddl");
        assert_eq!(config.publication, "demo_pub");
        assert_eq!(config.read_connections, 4);
        assert_eq!(config.read_threads, 3);
        assert_eq!(config.limits.max_disjuncts, 64);
        let settings = config.settings();
        assert!(settings.temporary_slot);
        assert!(settings.slot.starts_with(threads::SLOT_PREFIX));
        assert_ne!(settings.slot, config.settings().slot, "a fresh slot per start");
        assert!(Config::from_lookup(&Env::new(|_: &str| None)).is_err(), "a database is required");
        assert_eq!((config.app.as_str(), config.shard), ("demo", 0));
        assert_eq!(config.setup, Setup::Install, "set up by default");
        let setup = |value: &'static str| {
            Config::from_lookup(&Env::new(move |name: &str| match name {
                "STREAMGRES_PG_DSN" => Some("postgres://primary/app".to_owned()),
                "STREAMGRES_EMBEDDED_SETUP" => Some(value.to_owned()),
                _ => None,
            }))
            .map(|config| config.setup)
        };
        let zero_threads = Config::from_lookup(&Env::new(|name: &str| match name {
            "STREAMGRES_PG_DSN" => Some("postgres://primary/app".to_owned()),
            "STREAMGRES_READ_THREADS" => Some("0".to_owned()),
            _ => None,
        }));
        assert!(zero_threads.is_err(), "no threads is refused, as the server refuses it");
        assert_eq!(config.slot, Slot::Temporary, "a temporary slot by default");
        let slot = |pairs: &'static [(&'static str, &'static str)]| {
            Config::from_lookup(&Env::new(move |name: &str| match name {
                "STREAMGRES_PG_DSN" => Some("postgres://primary/app".to_owned()),
                _ => pairs
                    .iter()
                    .find(|(key, _)| *key == name)
                    .map(|(_, value)| (*value).to_owned()),
            }))
            .map(|config| config.slot)
        };
        assert_eq!(
            slot(&[("STREAMGRES_EMBEDDED_SLOT", "permanent")]),
            Ok(Slot::Permanent { cleanup_age: Duration::from_secs(300) })
        );
        assert_eq!(
            slot(&[("STREAMGRES_EMBEDDED_SLOT", "permanent"), ("STREAMGRES_SLOT_CLEANUP_AGE_MS", "0")]),
            Ok(Slot::Permanent { cleanup_age: Duration::ZERO })
        );
        assert_eq!(slot(&[("STREAMGRES_EMBEDDED_SLOT", "temporary")]), Ok(Slot::Temporary));
        assert!(slot(&[("STREAMGRES_EMBEDDED_SLOT", "durable")]).is_err());
        let permanent = Config {
            slot: Slot::Permanent { cleanup_age: Duration::from_secs(60) },
            ..config.clone()
        }
        .settings();
        assert!(!permanent.temporary_slot);
        assert_eq!(permanent.slot_cleanup_age, Duration::from_secs(60));
        assert_eq!(settings.slot_cleanup_age, Duration::ZERO, "nothing to sweep on a temporary slot");
        assert_eq!(setup("require"), Ok(Setup::Require));
        assert_eq!(setup("install"), Ok(Setup::Install));
        assert!(setup("maybe").is_err());
    }

    /// The setup an operator runs by hand is the stack checked in for the
    /// CI smoke (`scripts/sql/ddl-triggers.sql`), rendered for the same
    /// app, shard and publication: a stack older than the engine misses
    /// what the engine reads.
    #[test]
    fn the_setup_is_the_checked_in_stack() {
        let checked_in = include_str!("../scripts/sql/ddl-triggers.sql");
        assert_eq!(
            setup_sql("xyne", 0, "xyne_sync_pub"),
            checked_in,
            "scripts/sql/ddl-triggers.sql is stale: render it again with setup_sql(\"xyne\", 0, \"xyne_sync_pub\")"
        );
    }

    /// Ready once the position covers a snapshot that has been taken.
    #[test]
    fn readiness_follows_the_first_snapshot() {
        assert!(!covered(Lsn(100), Lsn(0)), "no snapshot yet");
        assert!(!covered(Lsn(99), Lsn(100)));
        assert!(covered(Lsn(100), Lsn(100)));
        assert!(covered(Lsn(101), Lsn(100)));
    }
}
