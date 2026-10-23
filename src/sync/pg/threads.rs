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
//! At startup it can drop this server's inactive slots once they have
//! exceeded an explicitly configured age; the PostgreSQL 17
//! `inactive_since` timestamp makes that cleanup safe across restarts.
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
/// - `slot_cleanup_age`: how long an inactive slot of an ended process is
///   retained before startup removes it (zero disables cleanup).
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
/// - `temporary_slot`: the feed's slot is a temporary one its own
///   replication connection makes, which PostgreSQL drops when that
///   connection ends ([`spawn_temporary_feed`]); a feed that drops
///   stops the engine, to start over on a fresh slot and snapshot.
/// - `heartbeat`: how often the engine writes a heartbeat into the WAL
///   to measure how far behind it is ([`crate::sync::heartbeat`]); zero
///   for none.
/// - `row_limit`: the most rows one read may return, and the planner's
///   bound on a side it reads whole ([`super::DEFAULT_READ_ROW_LIMIT`]).
/// - `memory_tables`: tables mirrored in memory and answered from there.
/// - `estimate_ttl`, `estimate_capacity`: how long the planner's
///   estimates are kept, and how many.
#[derive(Debug, Clone)]
pub struct Settings {
    pub dsn: String,
    /// Endpoint for ordinary SQL reads; may be a PgBouncer transaction pool.
    /// Snapshot minting and replication always use `dsn` above.
    pub read_dsn: String,
    pub slot: String,
    pub slot_cleanup_age: Duration,
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
    pub heartbeat: Duration,
    pub row_limit: usize,
    pub memory_tables: Vec<TableName>,
    pub estimate_ttl: Duration,
    pub estimate_capacity: usize,
    pub temporary_slot: bool,
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
            "the event trigger `{name}` (STREAMGRES_DDL_TRIGGER) is not on this database or is disabled; the server hears of schema changes through it and will not serve without it"
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
                let mut feed = Feed::with_ddl(catalog, ddl)
                    .with_heartbeat(crate::sync::heartbeat::prefix(&settings.slot));
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

/// The feed thread on a temporary slot ([`Transport::open_temporary`]):
/// `created` resolves once the slot exists, or with why it could not be
/// made; the first read snapshot must be taken after that, and its point
/// sent on `first`, through which the feed then drops what the snapshot
/// already holds. A stream that ends for any reason but a stop cannot be
/// resumed, so it fails `shutdown`, for the engine to start over.
#[allow(clippy::type_complexity)]
pub fn spawn_temporary_feed(
    settings: Settings,
    catalog: Arc<Catalog>,
    shutdown: Shutdown,
) -> Result<
    (
        mpsc::Receiver<Transaction>,
        FeedHandle,
        tokio::sync::oneshot::Receiver<Result<(), String>>,
        tokio::sync::oneshot::Sender<Lsn>,
    ),
    String,
> {
    let (out, transactions) = mpsc::channel(1024);
    let (stop, stop_rx) = watch::channel(false);
    let (created_tx, created) = tokio::sync::oneshot::channel();
    let (first, first_rx) = tokio::sync::oneshot::channel::<Lsn>();
    let thread = std::thread::Builder::new()
        .name("streamgres-feed".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("feed runtime");
            let slot = settings.slot.clone();
            let starting = shutdown.clone();
            let ended = runtime.block_on(async move {
                let transport = match Transport::open_temporary(
                    &settings.dsn,
                    &settings.slot,
                    &settings.publication,
                    &starting,
                )
                .await
                {
                    Ok(transport) => {
                        let _ = created_tx.send(Ok(()));
                        transport
                    }
                    Err(error) => {
                        let _ = created_tx.send(Err(error.to_string()));
                        return None;
                    }
                };
                let point = first_rx.await.ok()?;
                log_info!(
                    "change feed open on temporary slot {}, skipping through {point}",
                    settings.slot
                );
                let ddl = DdlSource {
                    prefix: settings.ddl_prefix.clone(),
                    schemas: settings.schemas.clone(),
                };
                let mut feed = Feed::with_ddl(catalog, ddl)
                    .with_heartbeat(crate::sync::heartbeat::prefix(&settings.slot))
                    .skipping_through(point);
                let outcome = transport
                    .stream_with_shutdown(
                        POSITION_EVERY,
                        &mut feed,
                        &settings.watched,
                        out.clone(),
                        stop_rx.clone(),
                    )
                    .await;
                if *stop_rx.borrow() || out.is_closed() {
                    return None;
                }
                Some(match outcome {
                    Ok(()) => "the change feed's connection ended".to_owned(),
                    Err(error) => format!("the change feed failed: {error}"),
                })
            });
            if let Some(why) = ended {
                log_error!(
                    "{why}; its temporary slot {slot} went with it and cannot be resumed, so the engine stops to start over"
                );
                if !shutdown.requested() {
                    shutdown.fail(format!("{why} on a temporary slot"));
                }
            } else {
                log_info!("change feed thread stopped");
            }
        })
        .map_err(|error| format!("feed thread: {error}"))?;
    Ok((
        transactions,
        FeedHandle {
            stop,
            thread: Some(thread),
        },
        created,
        first,
    ))
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

/// Writes this engine's heartbeats into the WAL of the database `config`
/// names, for as long as the process runs, reconnecting a second after a
/// failure. It runs off the engine thread (on the reads runtime), so a
/// stalled engine thread shows as a growing lag rather than as heartbeats
/// that stop coming. Once heartbeats have been written, a stretch in
/// which they cannot be makes the lag grow, so backends read Postgres
/// until they are written again. Until the first is written the lag is
/// unknown and backends trust the engine, as they would one that sends
/// no lag: a standby cannot write them, and its engine keeps serving.
/// A failure is logged once until heartbeats are written again.
async fn beat(config: tokio_postgres::Config, slot: String, every: Duration, stats: Arc<Stats>) {
    let prefix = crate::sync::heartbeat::prefix(&slot);
    let mut warned = false;
    let mut written = 0;
    loop {
        let outcome = match super::open(&config, &tokio::runtime::Handle::current()).await {
            Ok(client) => crate::sync::heartbeat::emit(&client, &prefix, every, &stats.freshness)
                .await
                .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        if stats.freshness.written() != written {
            written = stats.freshness.written();
            warned = false;
        }
        if let Err(error) = outcome
            && !warned
        {
            log_warn!("heartbeats stopped: {error}; retrying every second");
            warned = true;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
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
    Transport::cleanup_inactive_slots(&settings.dsn, settings.slot_cleanup_age)
        .await
        .map_err(|error| format!("cleaning up inactive replication slots: {error}"))?;
    let temporary = if settings.temporary_slot {
        Transport::require_publication(&settings.dsn, &settings.publication)
            .await
            .map_err(|error| format!("preparing the change feed: {error}"))?;
        let (feed, handle, created, first) =
            spawn_temporary_feed(settings.clone(), catalog.load(), shutdown.clone())?;
        created
            .await
            .map_err(|_| "the change feed thread ended before its slot was made".to_owned())?
            .map_err(|error| format!("opening the change feed: {error}"))?;
        Some((feed, handle, first))
    } else {
        Transport::prepare(&settings.dsn, &settings.slot, &settings.publication)
            .await
            .map_err(|error| format!("preparing the change feed: {error}"))?;
        None
    };
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
    // Minting the first snapshot waits for every transaction open at the
    // time, so a long one holds it back; a shutdown meanwhile gives up.
    let connected = PgStorage::connect_configured_with_read_config(
        config.clone(),
        read_config,
        catalog.load(),
        reads.clone(),
    );
    let pg = tokio::select! {
        connected = connected => {
            connected.map_err(|error| format!("connecting the storage: {error}"))?
        }
        () = shutdown.wait() => {
            return Err("stopped before the first snapshot was taken".to_owned());
        }
    };
    let pg = Arc::new(
        pg.with_rotation(settings.snapshot_rotation)
            .with_read_connections(settings.read_connections)
            .with_read_idle_connections(settings.read_idle_connections)
            .with_read_timeout(settings.read_timeout)
            .with_read_row_limit(settings.row_limit)
            .with_estimates(settings.estimate_ttl, settings.estimate_capacity),
    );
    let (feed, feed_handle) = match temporary {
        Some((feed, handle, first)) => {
            let _ = first.send(pg.first_position());
            (feed, handle)
        }
        None => spawn_feed(
            settings.clone(),
            catalog.load(),
            pg.first_position(),
            shutdown,
        )?,
    };
    let cached = settings.memory_tables.clone();
    let storage = Rc::new(Sources::new(pg.clone(), catalog.load(), cached.clone()));
    if !cached.is_empty() {
        let rows = storage
            .warm()
            .await
            .map_err(|error| format!("warming the memory tables: {error}"))?;
        log_info!("memory tables warmed with {rows} rows");
    }
    if !settings.heartbeat.is_zero() {
        reads.spawn(beat(config.clone(), settings.slot.clone(), settings.heartbeat, Arc::clone(&stats)));
    }
    let (sinks, events): (Vec<_>, Vec<_>) = (0..consumers.max(1))
        .map(|_| mpsc::unbounded_channel())
        .unzip();
    let first = sinks[0].clone();
    let engine = MultiTableIVM::new().with_row_limit(settings.row_limit);
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
