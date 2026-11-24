//! The engine side wired over PostgreSQL: a feed thread that holds the
//! replication connection, decodes the `pgoutput` events into rows and
//! hands the engine one transaction per commit (reopening the slot when
//! the connection drops), and, on the caller's engine thread, the storage
//! (its reads on the pool) and the [`Service`]. Whoever serves clients
//! talks to this side over the service's channels, commands in and events
//! out, and never touches a database connection.
//!
//! The order at start is what makes the first snapshot and the feed
//! agree: the slot is made to exist, then the storage mints its first
//! snapshot, then the feed opens with the slot moved up to that
//! snapshot's point, so it streams exactly what the snapshot does not
//! hold. Each process streams from a slot of its own, named
//! [`SLOT_PREFIX`] and a fresh UUID: a restart needs nothing of the
//! slot before it (the slot is moved up to the first snapshot anyway).
//! Schema changes reach the feed as the messages of the schema-change
//! event trigger ([`super::ddl`]); the server refuses to serve
//! without that trigger ([`require_ddl_trigger`]).
//!
//! A consumer that must read some rows for itself (an application's
//! mutation-id table, say) names those tables in [`Settings::watched`].
//! Their writes travel inside the transaction that carried them and reach
//! the consumer in its [`Event::Committed`], so a mutation's rows and its
//! id are one poke and no later transaction's id rides out early.

use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::spawn_local;

use super::ddl::{self, DdlSource};
use super::{Feed, Keepalive, PgStorage, Transport, load_catalog};
use crate::ivm::MultiTableIVM;
use crate::log::{log_error, log_info, log_warn};
use crate::model::{Catalog, Lsn, MultiTableReadQuery, TableName};
use crate::shutdown::Shutdown;
use crate::stats::Stats;
use crate::sync::{CatalogHandle, Command, Event, Runtime, Service, Sources, Transaction};

/// How the engine side reaches its database.
///
/// - `dsn`: the database, with the user and password in the URL.
/// - `slot`: the replication slot of the change feed, this process's own
///   ([`slot_name`]).
/// - `publication`: the publication the feed streams.
/// - `schemas`: the schemas whose tables the catalog carries.
/// - `snapshot_rotation`: how often a fresh exported snapshot is minted
///   for reads.
/// - `read_connections`: how many storage reads may hold a connection at
///   once.
/// - `read_idle_connections`: how many read connections to keep open for
///   reuse after their reads finish.
/// - `read_timeout`: how long one storage read may take before it is
///   given up and refused; zero for no limit.
/// - `read_threads`: how many threads the reads pool runs on.
/// - `keepalive`: how the kernel probes a silent connection to the
///   database so that nothing between drops it ([`Keepalive`]).
/// - `watched`: tables whose writes are copied to the consumer as they
///   are decoded, besides being routed like every other write.
/// - `ddl_trigger`: the event trigger on `ddl_command_end` through which
///   schema changes are heard; the server will not serve without it.
/// - `ddl_prefix`: the prefix of that trigger's logical messages.
#[derive(Debug, Clone)]
pub struct Settings {
    pub dsn: String,
    /// Endpoint for ordinary SQL reads; may be a PgBouncer transaction pool.
    /// Snapshot minting and replication always use `dsn` above.
    pub read_dsn: String,
    pub slot: String,
    pub slot_cleanup_enabled: bool,
    pub slot_sweep_lock_key: String,
    pub slot_cleanup_age: Duration,
    pub slot_sweep_interval: Duration,
    pub slot_cleanup_startup_timeout: Duration,
    pub publication: String,
    pub schemas: Vec<String>,
    pub snapshot_rotation: Duration,
    pub read_connections: usize,
    pub read_idle_connections: usize,
    pub read_timeout: Duration,
    pub read_threads: usize,
    pub keepalive: Keepalive,
    pub watched: Vec<TableName>,
    pub ddl_trigger: String,
    pub ddl_prefix: String,
}

/// What every slot a server names for itself starts with.
pub const SLOT_PREFIX: &str = "xyne_sync_slot_";

pub const DEFAULT_SWEEP_LOCK_KEY: &str = "streamgres.slot_cleanup";

/// A slot name of this process's own: [`SLOT_PREFIX`] and a fresh UUID
/// (lowercase hex, no dashes, as slot names allow).
pub fn slot_name() -> String {
    format!("{SLOT_PREFIX}{}", uuid::Uuid::new_v4().simple())
}

/// How often the feed thread tells the engine where the feed is without a
/// transaction to carry the news: the most a snapshot minted while
/// nothing is being written waits before reads move onto it, and a server
/// on a quiet database before it says it is ready.
const POSITION_EVERY: Duration = Duration::from_millis(200);

/// The feed has four seconds of the process's 25-second application shutdown
/// budget; Kubernetes retains five seconds of a 30-second grace period.
const FEED_STOP_GRACE: Duration = Duration::from_secs(4);

async fn periodic_sweep(
    dsn: String,
    slot: String,
    mut stopped: watch::Receiver<bool>,
    every: Duration,
    age: Duration,
    lock_key: String,
) {
    loop {
        if *stopped.borrow() {
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(every) => {}
            _ = stopped.changed() => return,
        }
        tokio::select! {
            result = Transport::cleanup_inactive_slots(&dsn, &slot, age, &lock_key) => {
                if let Err(error) = result {
                    log_warn!("periodic slot cleanup failed: {error}");
                }
            }
            _ = stopped.changed() => return,
        }
    }
}

struct SweepTask {
    stop: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl SweepTask {
    fn spawn(dsn: String, slot: String, every: Duration, age: Duration, lock_key: String) -> Self {
        let (stop, stopped) = watch::channel(false);
        let task = spawn_local(periodic_sweep(dsn, slot, stopped, every, age, lock_key));
        Self {
            stop,
            task: Some(task),
        }
    }

    async fn stop(mut self) {
        self.stop.send_replace(true);
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for SweepTask {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// The running engine side: commands in, one event stream per consumer
/// (the consumer of a client being the one at its id modulo their count),
/// and the storage handle for the reads that are not the engine's (a
/// planner's counts, a connect-time query).
pub struct Started {
    pub commands: mpsc::Sender<Command<MultiTableReadQuery>>,
    pub events: Vec<mpsc::UnboundedReceiver<Event>>,
    pub storage: Arc<PgStorage>,
    pub feed: FeedHandle,
}

/// The feed's stop signal and its OS thread. Shutdown sets the signal, waits
/// for the replication connection to close, and only then may remove the
/// feed's slot.
pub struct FeedHandle {
    stop: watch::Sender<bool>,
    thread: Option<std::thread::JoinHandle<()>>,
    sweeper: Option<SweepTask>,
}

impl Drop for FeedHandle {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        self.sweeper.take();
    }
}

/// The service's task, kept on the engine thread: it resolves only when
/// the service stops or panics, and a server with no engine must not
/// keep listening.
pub type ServiceTask = tokio::task::JoinHandle<Runtime<MultiTableIVM>>;

/// Load the catalog of `schemas` over a connection of its own.
pub async fn load_catalog_at(dsn: &str, schemas: &[String]) -> Result<Catalog, String> {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| format!("connecting to the database: {error}"))?;
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let (catalog, notes) = load_catalog(&client, schemas)
        .await
        .map_err(|error| format!("reading the catalog: {error}"))?;
    drop(client);
    let _ = handle.await;
    for note in &notes {
        log_info!("catalog: {note}");
    }
    log_info!(
        "catalog loaded from {}: {} tables",
        schemas.join(", "),
        catalog.tables().count()
    );
    let mut tables: Vec<_> = catalog.tables().collect();
    tables.sort_by(|a, b| a.name.as_str().cmp(b.name.as_str()));
    for table in tables {
        log_info!("schema: {table}");
    }
    Ok(catalog)
}

/// Refuse to serve without the event trigger `name` on `ddl_command_end`
/// at `dsn`: it is how the server hears of schema changes, and a server
/// that would not hear of them could go on serving rows in a shape the
/// clients no longer expect.
pub async fn require_ddl_trigger(dsn: &str, name: &str) -> Result<(), String> {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| format!("connecting to the database: {error}"))?;
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let present = ddl::trigger_present(&client, name)
        .await
        .map_err(|error| format!("looking for the DDL event trigger `{name}`: {error}"))?;
    drop(client);
    let _ = handle.await;
    if present {
        log_info!("schema changes are heard through the event trigger {name}");
        Ok(())
    } else {
        Err(format!(
            "the event trigger `{name}` is missing or disabled; the server hears of schema changes through it and will not serve without it"
        ))
    }
}

/// Start the feed thread: it decodes every replication event, starting
/// from `catalog` and following the schema changes the DDL trigger
/// announces, sends one transaction per commit into the returned channel,
/// reopens the slot with a backoff when the connection drops (the catalog
/// as the feed has grown it survives the connection), and ends the
/// process when the receiver is gone or decoding fails. The first time
/// the slot opens it is moved up to `start`, the first read snapshot's
/// point, when it is behind it.
pub fn spawn_feed(
    settings: Settings,
    catalog: Arc<Catalog>,
    start: Lsn,
    shutdown: Shutdown,
) -> Result<(mpsc::Receiver<Transaction>, FeedHandle), String> {
    let (out, transactions) = mpsc::channel(1024);
    let (stop, mut stop_rx) = watch::channel(false);
    let thread = std::thread::Builder::new()
        .name("streamgres-feed".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("feed runtime");
            let running_shutdown = shutdown.clone();
            let stopped = runtime.block_on(async move {
                let ddl = DdlSource {
                    prefix: settings.ddl_prefix.clone(),
                    schemas: settings.schemas.clone(),
                };
                let mut feed = Feed::with_ddl(catalog, ddl);
                let mut start = Some(start);
                let mut failures = 0u32;
                loop {
                    let opened = match start {
                        Some(from) => Transport::open_from(
                            &settings.dsn,
                            &settings.slot,
                            &settings.publication,
                            from,
                        )
                        .await,
                        None => Transport::open(
                            &settings.dsn,
                            &settings.slot,
                            &settings.publication,
                        )
                        .await,
                    };
                    match opened {
                        Ok(transport) => {
                            log_info!("change feed open on slot {}", settings.slot);
                            start = None;
                            failures = 0;
                            let outcome = transport
                                .stream_with_shutdown(
                                    POSITION_EVERY,
                                    &mut feed,
                                    &settings.watched,
                                    out.clone(),
                                    stop_rx.clone(),
                                )
                                .await;
                            if *stop_rx.borrow() {
                                return true;
                            }
                            if out.is_closed() {
                                return false;
                            }
                            match outcome {
                                Ok(()) => log_warn!("change feed dropped; reopening the slot"),
                                Err(error) => {
                                    log_error!("change feed failed: {error}");
                                    running_shutdown.fail(format!("change feed failed: {error}"));
                                    return true;
                                }
                            }
                        }
                        Err(error) => {
                            failures += 1;
                            log_error!("opening the change feed (attempt {failures}): {error}");
                            if let Ok(Some(failure)) =
                                Transport::slot_failure(&settings.dsn, &settings.slot).await
                            {
                                log_error!(
                                    "{failure}; stopping so a restart begins on a fresh slot and snapshot"
                                );
                                running_shutdown.fail(failure);
                                return true;
                            }
                        }
                    }
                    let backoff = Duration::from_secs(1 << failures.min(4));
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        changed = stop_rx.changed() => {
                            if changed.is_ok() && *stop_rx.borrow() {
                                return true;
                            }
                        }
                    }
                }
            });
            if stopped {
                log_info!("change feed thread stopped gracefully");
            } else {
                log_error!("change feed thread stopped: the engine is gone");
                if !shutdown.requested() {
                    shutdown.fail("change feed thread stopped: the engine is gone");
                }
            }
        })
        .map_err(|error| format!("feed thread: {error}"))?;
    Ok((
        transactions,
        FeedHandle {
            stop,
            thread: Some(thread),
            sweeper: None,
        },
    ))
}

/// Stop the feed thread and wait until its replication connection is closed.
pub async fn stop_feed(mut feed: FeedHandle) -> Result<(), String> {
    feed.stop.send_replace(true);
    if let Some(sweeper) = feed.sweeper.take() {
        sweeper.stop().await;
    }
    let Some(thread) = feed.thread.take() else {
        return Ok(());
    };
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("streamgres-feed-join".to_owned())
        .spawn(move || {
            let outcome = thread
                .join()
                .map_err(|_| "change feed thread panicked while stopping".to_owned());
            let _ = done_tx.send(outcome);
        })
        .map_err(|error| format!("joining change feed thread: {error}"))?;
    match tokio::time::timeout(FEED_STOP_GRACE, done_rx).await {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(_)) => Err("change feed join worker stopped unexpectedly".to_owned()),
        Err(_) => Err(format!(
            "change feed did not stop within {FEED_STOP_GRACE:?}"
        )),
    }
}

/// Bring the engine side up on the current thread's local set: the slot
/// made to exist, the storage over `settings` (its reads running on
/// `reads`) with its first snapshot minted, the feed thread started at
/// that snapshot's point, and the service, keeping `catalog` current,
/// delivering its events to `consumers` streams and its timings to
/// `stats`. Must run inside a `LocalSet`.
pub async fn start(
    settings: &Settings,
    catalog: Arc<CatalogHandle>,
    reads: tokio::runtime::Handle,
    consumers: usize,
    stats: Arc<Stats>,
    shutdown: Shutdown,
) -> Result<(Started, ServiceTask), String> {
    if settings.slot_cleanup_enabled
        && settings.slot_cleanup_age.as_secs_f64() > (i64::MAX / 1_000_000 - 1) as f64
    {
        return Err("slot cleanup age exceeds the PostgreSQL interval range".to_owned());
    }
    if settings.slot_cleanup_enabled
        && (settings.slot_sweep_interval.is_zero()
            || settings.slot_sweep_interval.as_millis() > i64::MAX as u128)
    {
        return Err(
            "slot sweep interval must be positive and fit in signed 64-bit milliseconds".to_owned(),
        );
    }
    if settings.slot_cleanup_enabled {
        if settings.slot_sweep_lock_key.trim().is_empty()
            || settings.slot_sweep_lock_key.contains('\0')
        {
            return Err("sweep lock key must be nonblank and contain no NUL".to_owned());
        }
        if settings.slot_cleanup_startup_timeout.is_zero()
            || settings.slot_cleanup_startup_timeout.as_millis() > i64::MAX as u128
        {
            return Err(
                "startup cleanup timeout must be positive and fit in signed 64-bit milliseconds"
                    .to_owned(),
            );
        }
        tokio::select! {
            result = Transport::cleanup_slots_at_startup(
                &settings.dsn, &settings.slot, settings.slot_cleanup_age,
                settings.slot_cleanup_startup_timeout,
                &settings.slot_sweep_lock_key,
            ) => {
                if let Err(error) = result {
                    log_warn!("startup slot cleanup failed: {error}; continuing initialization");
                }
            }
            _ = shutdown.wait() => return Err("startup cancelled by shutdown".to_owned()),
        }
    }
    let sweeper = settings.slot_cleanup_enabled.then(|| {
        SweepTask::spawn(
            settings.dsn.clone(),
            settings.slot.clone(),
            settings.slot_sweep_interval,
            settings.slot_cleanup_age,
            settings.slot_sweep_lock_key.clone(),
        )
    });
    Transport::prepare(&settings.dsn, &settings.slot, &settings.publication)
        .await
        .map_err(|error| format!("preparing the change feed: {error}"))?;
    let mut config: tokio_postgres::Config = settings
        .dsn
        .parse()
        .map_err(|error| format!("connecting the storage: {error}"))?;
    settings.keepalive.apply(&mut config);
    let mut read_config: tokio_postgres::Config = settings
        .read_dsn
        .parse()
        .map_err(|error| format!("connecting the read pool: {error}"))?;
    settings.keepalive.apply(&mut read_config);
    let pg =
        PgStorage::connect_configured_with_read_config(config, read_config, catalog.load(), reads)
            .await
            .map_err(|error| format!("connecting the storage: {error}"))?
            .with_rotation(settings.snapshot_rotation)
            .with_read_connections(settings.read_connections)
            .with_read_idle_connections(settings.read_idle_connections)
            .with_read_timeout(settings.read_timeout);
    let pg = Arc::new(pg);
    let (feed, mut feed_handle) = spawn_feed(
        settings.clone(),
        catalog.load(),
        pg.first_position(),
        shutdown,
    )?;
    feed_handle.sweeper = sweeper;
    let cached = Sources::cached_from_env();
    let storage = Rc::new(Sources::new(pg.clone(), catalog.load(), cached.clone()));
    if !cached.is_empty() {
        let rows = storage
            .warm()
            .await
            .map_err(|error| format!("warming the memory tables: {error}"))?;
        log_info!("memory tables warmed with {rows} rows");
    }
    let (sinks, events): (Vec<_>, Vec<_>) = (0..consumers.max(1))
        .map(|_| mpsc::unbounded_channel())
        .unzip();
    let first = sinks[0].clone();
    let engine = MultiTableIVM::new().with_row_limit(super::read_row_limit());
    let (service, commands) = Service::new(engine, storage, first);
    let service = spawn_local(
        service
            .with_feed(feed)
            .with_catalog(catalog)
            .with_sinks(sinks)
            .with_stats(stats)
            .run(),
    );
    Ok((
        Started {
            commands,
            events,
            storage: pg,
            feed: feed_handle,
        },
        service,
    ))
}

#[cfg(test)]
mod slot_cleanup_tests {
    use super::*;

    #[tokio::test]
    async fn periodic_cleanup_guard_drop_cancels_inflight_connection() {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (accepted, connected) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _ = accepted.send(());
            let mut bytes = Vec::new();
            socket.read_to_end(&mut bytes).await.unwrap();
        });
        tokio::task::LocalSet::new()
            .run_until(async {
                let cleanup = SweepTask::spawn(
                    format!("host=127.0.0.1 port={port} user=test dbname=test"),
                    slot_name(),
                    Duration::from_millis(10),
                    Duration::ZERO,
                    DEFAULT_SWEEP_LOCK_KEY.to_owned(),
                );
                tokio::time::timeout(Duration::from_secs(2), connected)
                    .await
                    .unwrap()
                    .unwrap();
                drop(cleanup);
                tokio::time::timeout(Duration::from_secs(2), server)
                    .await
                    .unwrap()
                    .unwrap();
            })
            .await;
    }

    #[tokio::test]
    async fn periodic_slot_cleanup_runs_and_stops() {
        let Ok(dsn) = std::env::var("STREAMGRES_SLOT_TEST_DSN") else {
            return;
        };
        let configured = dsn.clone();
        let (admin, driver) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        let driver = tokio::spawn(driver);
        let initial = slot_name();
        admin
            .query_one(
                "SELECT * FROM pg_create_logical_replication_slot($1, 'test_decoding')",
                &[&initial],
            )
            .await
            .unwrap();
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(periodic_sweep(
            configured,
            String::new(),
            stopped,
            Duration::from_millis(25),
            Duration::ZERO,
            DEFAULT_SWEEP_LOCK_KEY.to_owned(),
        ));
        for slot in [initial, slot_name()] {
            if !Transport::slot_exists(&dsn, &slot).await.unwrap() {
                admin
                    .query_one(
                        "SELECT * FROM pg_create_logical_replication_slot($1, 'test_decoding')",
                        &[&slot],
                    )
                    .await
                    .unwrap();
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                while Transport::slot_exists(&dsn, &slot).await.unwrap() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("initial/periodic cleanup did not delete an orphan");
        }
        stop.send_replace(true);
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        let retained = slot_name();
        admin
            .query_one(
                "SELECT * FROM pg_create_logical_replication_slot($1, 'test_decoding')",
                &[&retained],
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(Transport::slot_exists(&dsn, &retained).await.unwrap());
        admin
            .query_one("SELECT pg_drop_replication_slot($1)", &[&retained])
            .await
            .unwrap();
        driver.abort();
    }
}
