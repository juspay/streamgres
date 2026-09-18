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

use super::{Feed, PgStorage, Transport, load_catalog};
use crate::ivm::MultiTableIVM;
use crate::log::{log_error, log_info, log_warn};
use crate::model::{Catalog, MultiTableReadQuery, TableName};
use crate::stats::Stats;
use crate::sync::{Command, Event, Service, Sources, Transaction};

/// How the engine side reaches its database.
///
/// - `dsn`: the database, with the user and password in the URL.
/// - `slot`: the permanent replication slot of the change feed.
/// - `schemas`: the schemas whose tables the catalog carries.
/// - `heartbeat`: how often the feed emits a heartbeat so the engine's
///   position moves while nothing is written.
/// - `snapshot_rotation`: how often a fresh exported snapshot is minted
///   for reads.
/// - `read_connections`: how many storage reads may hold a connection at
///   once.
/// - `read_threads`: how many threads the reads pool runs on.
/// - `watched`: tables whose writes are copied to the consumer as they
///   are decoded, besides being routed like every other write.
#[derive(Debug, Clone)]
pub struct Settings {
    pub dsn: String,
    pub slot: String,
    pub schemas: Vec<String>,
    pub heartbeat: Duration,
    pub snapshot_rotation: Duration,
    pub read_connections: usize,
    pub read_threads: usize,
    pub watched: Vec<TableName>,
}

/// The running engine side: commands in, one event stream per consumer
/// (the consumer of a client being the one at its id modulo their count),
/// and the storage handle for the reads that are not the engine's (a
/// planner's counts, a connect-time query).
pub struct Started {
    pub commands: mpsc::Sender<Command<MultiTableReadQuery>>,
    pub events: Vec<mpsc::UnboundedReceiver<Event>>,
    pub storage: Arc<PgStorage>,
}

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
                                    settings.heartbeat,
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
                                    std::process::exit(1);
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
            std::process::exit(1);
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
) -> Result<Started, String> {
    let pg = PgStorage::connect_on(&settings.dsn, catalog.clone(), reads)
        .await
        .map_err(|error| format!("connecting the storage: {error}"))?
        .with_rotation(settings.snapshot_rotation)
        .with_read_connections(settings.read_connections);
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
    spawn_local(
        service
            .with_feed(feed)
            .with_sinks(sinks)
            .with_stats(stats)
            .run(),
    );
    Ok(Started {
        commands,
        events,
        storage: pg,
    })
}
