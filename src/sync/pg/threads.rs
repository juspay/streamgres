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
//! The process owns its slot through an advisory lock, and at startup and
//! every minute drops the inactive slots whose owner is gone
//! ([`super::slots`]).
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
use super::ddl_triggers;
use super::slots::{self, SlotOwner};
use super::{Feed, Keepalive, PgStorage, Transport, load_catalog};
use crate::ivm::MultiTableIVM;
use crate::log::{log_error, log_info, log_warn};
use crate::model::{Catalog, MultiTableReadQuery, Position, TableName};
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
    /// Tell the consumer when a subscription's rows become incomplete and
    /// complete again ([`Service::with_settle_reports`]).
    pub settle_reports: bool,
    /// The most rows one storage read, and one subscription's part, may
    /// hold; a query past it is refused.
    pub row_limit: usize,
    /// Maximum wire payload buffered by one snapshot read.
    pub read_byte_limit: usize,
    /// Tables held whole in memory and read from there.
    pub memory_tables: Vec<TableName>,
}

/// What every slot a server names for itself starts with.
pub const SLOT_PREFIX: &str = "xyne_sync_slot_";

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
}

// A cancelled startup or a panic must not detach a live feed forever.
// Explicit shutdown additionally joins it before dropping the slot.
impl Drop for FeedHandle {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

/// The service's task, kept on the engine thread: it resolves only when
/// the service stops or panics, and a server with no engine must not
/// keep listening.
pub type ServiceTask = tokio::task::JoinHandle<Runtime<MultiTableIVM>>;

/// Load the catalog of `schemas` over a connection of its own, the tables
/// `publication` streams in full.
pub async fn load_catalog_at(
    dsn: &str,
    schemas: &[String],
    publication: &str,
) -> Result<Catalog, String> {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| format!("connecting to the database: {error}"))?;
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let (catalog, mut notes) = load_catalog(&client, schemas)
        .await
        .map_err(|error| format!("reading the catalog: {error}"))?;
    let (catalog, published) =
        super::catalog::restrict_to_publication(&client, catalog, publication)
            .await
            .map_err(|error| format!("reading the publication: {error}"))?;
    notes.extend(published);
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

/// What an embedded engine learns of its database before it starts.
#[derive(Debug, Clone)]
pub struct Source {
    /// The cluster's system identifier: the same on a primary and its
    /// physical standbys, different on every other cluster.
    pub cluster: String,
}

/// Looks the database at `dsn` over for an engine that relies on the
/// current trigger stack: the event trigger `trigger` must be there, and
/// the stack in `schema` at [`ddl_triggers::STACK_VERSION`], since an
/// older one leaves out what the engine needs to hear (row-level security
/// turned on, a column's collation).
pub async fn inspect_source(dsn: &str, trigger: &str, schema: &str) -> Result<Source, String> {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| format!("connecting to the database: {error}"))?;
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let inspected = async {
        if !ddl::trigger_present(&client, trigger)
            .await
            .map_err(|error| format!("looking for the DDL event trigger `{trigger}`: {error}"))?
        {
            return Err(format!(
                "the event trigger `{trigger}` is missing or disabled; the engine hears of schema changes through it"
            ));
        }
        let version = ddl::stack_version(&client, schema)
            .await
            .map_err(|error| format!("reading the trigger stack's version: {error}"))?;
        if version < ddl_triggers::STACK_VERSION {
            return Err(format!(
                "the trigger stack in `{schema}` is version {version}, and version {} is needed; install it again (CacheConfig::setup_sql)",
                ddl_triggers::STACK_VERSION
            ));
        }
        let cluster: String = client
            .query_one("SELECT system_identifier::text FROM pg_control_system()", &[])
            .await
            .map_err(|error| format!("reading the cluster's identifier: {error}"))?
            .get(0);
        Ok(Source { cluster })
    }
    .await;
    drop(client);
    let _ = handle.await;
    inspected
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
    start: Position,
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
            let mut cancelled = stop_rx.clone();
            let running = async move {
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
                            if let Ok(false) =
                                Transport::slot_exists(&settings.dsn, &settings.slot).await
                            {
                                log_error!(
                                    "slot {} is gone, and a slot made again would skip the changes since; stopping so a restart begins on a fresh slot and snapshot",
                                    settings.slot
                                );
                                running_shutdown.fail(format!(
                                    "change-feed slot {} disappeared",
                                    settings.slot
                                ));
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
            };
            let stopped = runtime.block_on(async move {
                tokio::select! {
                    outcome = running => outcome,
                    () = async {
                        loop {
                            if *cancelled.borrow() { return; }
                            if cancelled.changed().await.is_err() { return; }
                        }
                    } => true,
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
        },
    ))
}

/// Keep this process's slot owned and sweep the slots whose owner is gone,
/// every [`slots::SWEEP_EVERY`], until the feed stops; the ownership ends
/// with it.
async fn keep_and_sweep(
    mut owner: SlotOwner,
    config: tokio_postgres::Config,
    mut stopped: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(slots::SWEEP_EVERY) => {}
            changed = stopped.changed() => {
                if changed.is_err() || *stopped.borrow() {
                    return;
                }
                continue;
            }
        }
        if let Err(error) = owner.keep().await {
            log_warn!("keeping slot {} owned: {error}", owner.slot());
            continue;
        }
        if let Err(error) = slots::sweep(&config, SLOT_PREFIX, owner.slot()).await {
            log_warn!("sweeping replication slots whose owner is gone: {error}");
        }
    }
}

/// Stop the feed thread and wait until its replication connection is closed.
pub async fn stop_feed(mut feed: FeedHandle) -> Result<(), String> {
    feed.stop.send_replace(true);
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
    let direct: tokio_postgres::Config = settings
        .dsn
        .parse()
        .map_err(|error| format!("reading the database address: {error}"))?;
    let owner = SlotOwner::claim(&direct, &settings.slot)
        .await
        .map_err(|error| format!("taking ownership of slot {}: {error}", settings.slot))?;
    if let Err(error) = slots::sweep(&direct, SLOT_PREFIX, &settings.slot).await {
        log_warn!("sweeping replication slots whose owner is gone: {error}");
    }
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
    let pg = PgStorage::connect_with_row_limit(
        config,
        read_config,
        catalog.load(),
        reads,
        settings.row_limit,
    )
    .await
    .map_err(|error| format!("connecting the storage: {error}"))?
    .with_read_byte_limit(settings.read_byte_limit)
    .with_rotation(settings.snapshot_rotation)
    .with_read_connections(settings.read_connections)
    .with_read_idle_connections(settings.read_idle_connections)
    .with_read_timeout(settings.read_timeout);
    let pg = Arc::new(pg);
    let (feed, feed_handle) = spawn_feed(
        settings.clone(),
        catalog.load(),
        pg.first_position(),
        shutdown,
    )?;
    spawn_local(keep_and_sweep(owner, direct, feed_handle.stop.subscribe()));
    let cached = settings.memory_tables.clone();
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
    let engine = MultiTableIVM::new().with_row_limit(settings.row_limit);
    let (service, commands) = Service::new(engine, storage, first);
    let service = if settings.settle_reports {
        service.with_settle_reports()
    } else {
        service
    };
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
mod cancellation_tests {
    use super::*;
    #[test]
    fn abandoning_a_feed_handle_signals_its_thread() {
        let (stop, mut stopped) = watch::channel(false);
        drop(FeedHandle { stop, thread: None });
        assert!(*stopped.borrow_and_update());
    }
    #[test]
    fn stopping_a_feed_cancels_a_stalled_connection_attempt() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let dsn = format!(
            "postgresql://postgres@{}/test",
            listener.local_addr().unwrap()
        );
        let settings = Settings {
            dsn: dsn.clone(),
            read_dsn: dsn,
            slot: "test".into(),
            publication: "p".into(),
            schemas: vec![],
            snapshot_rotation: Duration::from_secs(1),
            read_connections: 1,
            read_idle_connections: 1,
            read_timeout: Duration::from_secs(1),
            read_threads: 1,
            keepalive: Keepalive {
                idle: Duration::from_secs(1),
                interval: Duration::from_secs(1),
                retries: 1,
            },
            watched: vec![],
            ddl_trigger: "test".into(),
            ddl_prefix: "test".into(),
            settle_reports: true,
            row_limit: 10,
            read_byte_limit: 1 << 20,
            memory_tables: vec![],
        };
        let (_transactions, mut feed) = spawn_feed(
            settings,
            Arc::new(Catalog::new(vec![])),
            Position(0),
            Shutdown::new(),
        )
        .unwrap();
        let (_peer, _) = listener.accept().unwrap();
        let thread = feed.thread.take().unwrap();
        drop(feed);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !thread.is_finished() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            thread.is_finished(),
            "dropping a feed must cancel connection establishment too"
        );
        thread.join().unwrap();
    }
}
