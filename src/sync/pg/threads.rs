//! The engine side wired over PostgreSQL: a feed thread that holds the
//! replication connection, decodes the `pgoutput` events into rows and
//! hands the engine one transaction per commit (reopening the slot when
//! the connection drops), and, on the caller's engine thread, the storage
//! (its reads on the pool) and the [`Service`]. Whoever serves clients
//! talks to this side over the service's channels, commands in and events
//! out, and never touches a database connection.
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
use tokio::task::spawn_local;

use super::{Feed, Keepalive, PgStorage, Transport, load_catalog};
use crate::ivm::MultiTableIVM;
use crate::log::{log_error, log_info, log_warn};
use crate::model::{Catalog, MultiTableReadQuery, TableName};
use crate::stats::Stats;
use crate::sync::{Command, Event, Runtime, Service, Sources, Transaction};

/// How the engine side reaches its database.
///
/// - `dsn`: the database, with the user and password in the URL.
/// - `slot`: the permanent replication slot of the change feed.
/// - `schemas`: the schemas whose tables the catalog carries.
/// - `snapshot_rotation`: how often a fresh exported snapshot is minted
///   for reads.
/// - `read_connections`: how many storage reads may hold a connection at
///   once.
/// - `read_timeout`: how long one storage read may take before it is
///   given up and refused; zero for no limit.
/// - `read_threads`: how many threads the reads pool runs on.
/// - `keepalive`: how the kernel probes a silent connection to the
///   database so that nothing between drops it ([`Keepalive`]).
/// - `watched`: tables whose writes are copied to the consumer as they
///   are decoded, besides being routed like every other write.
#[derive(Debug, Clone)]
pub struct Settings {
    pub dsn: String,
    pub slot: String,
    pub schemas: Vec<String>,
    pub snapshot_rotation: Duration,
    pub read_connections: usize,
    pub read_timeout: Duration,
    pub read_threads: usize,
    pub keepalive: Keepalive,
    pub watched: Vec<TableName>,
}

/// How often the feed thread tells the engine where the feed is without a
/// transaction to carry the news: the most a snapshot minted while
/// nothing is being written waits before reads move onto it, and a server
/// on a quiet database before it says it is ready.
const POSITION_EVERY: Duration = Duration::from_millis(200);

/// The running engine side: commands in, one event stream per consumer
/// (the consumer of a client being the one at its id modulo their count),
/// and the storage handle for the reads that are not the engine's (a
/// planner's counts, a connect-time query).
pub struct Started {
    pub commands: mpsc::Sender<Command<MultiTableReadQuery>>,
    pub events: Vec<mpsc::UnboundedReceiver<Event>>,
    pub storage: Arc<PgStorage>,
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
    Ok(catalog)
}

/// Start the feed thread: it decodes every replication event with
/// `catalog` and sends one transaction per commit into the returned
/// channel, reopens the slot with a backoff when the connection drops,
/// and ends the process when the receiver is gone or decoding fails.
pub fn spawn_feed(
    settings: Settings,
    catalog: Arc<Catalog>,
) -> Result<mpsc::Receiver<Transaction>, String> {
    let (out, transactions) = mpsc::channel(1024);
    std::thread::Builder::new()
        .name("xyne-sync-feed".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("feed runtime");
            runtime.block_on(async move {
                let mut failures = 0u32;
                loop {
                    match Transport::open(&settings.dsn, &settings.slot).await {
                        Ok(transport) => {
                            log_info!("change feed open on slot {}", settings.slot);
                            failures = 0;
                            let outcome = transport
                                .stream(
                                    POSITION_EVERY,
                                    Feed::new(catalog.clone()),
                                    &settings.watched,
                                    out.clone(),
                                )
                                .await;
                            if out.is_closed() {
                                return;
                            }
                            match outcome {
                                Ok(()) => log_warn!("change feed dropped; reopening the slot"),
                                Err(error) => {
                                    log_error!("change feed failed: {error}");
                                    crate::log::exit(1);
                                }
                            }
                        }
                        Err(error) => {
                            failures += 1;
                            log_error!("opening the change feed (attempt {failures}): {error}");
                        }
                    }
                    let backoff = Duration::from_secs(1 << failures.min(4));
                    tokio::time::sleep(backoff).await;
                }
            });
            log_error!("change feed thread stopped: the engine is gone");
            crate::log::exit(1);
        })
        .map_err(|error| format!("feed thread: {error}"))?;
    Ok(transactions)
}

/// Bring the engine side up on the current thread's local set: the
/// storage over `settings` (its reads running on `reads`) and the
/// service, taking the feed thread's transactions from `feed`, delivering
/// its events to `consumers` streams and its timings to `stats`. Must run
/// inside a `LocalSet`.
pub async fn start(
    settings: &Settings,
    catalog: Arc<Catalog>,
    feed: mpsc::Receiver<Transaction>,
    reads: tokio::runtime::Handle,
    consumers: usize,
    stats: Arc<Stats>,
) -> Result<(Started, ServiceTask), String> {
    let mut config: tokio_postgres::Config = settings
        .dsn
        .parse()
        .map_err(|error| format!("connecting the storage: {error}"))?;
    settings.keepalive.apply(&mut config);
    let pg = PgStorage::connect_configured(config, catalog.clone(), reads)
        .await
        .map_err(|error| format!("connecting the storage: {error}"))?
        .with_rotation(settings.snapshot_rotation)
        .with_read_connections(settings.read_connections)
        .with_read_timeout(settings.read_timeout);
    let pg = Arc::new(pg);
    let cached = Sources::cached_from_env();
    let storage = Rc::new(Sources::new(pg.clone(), catalog.clone(), cached.clone()));
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
    let (service, commands) = Service::new(MultiTableIVM::new(), storage, first);
    let service = spawn_local(
        service
            .with_feed(feed)
            .with_sinks(sinks)
            .with_stats(stats)
            .run(),
    );
    Ok((
        Started {
            commands,
            events,
            storage: pg,
        },
        service,
    ))
}
