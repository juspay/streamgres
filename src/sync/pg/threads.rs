//! The engine side wired over PostgreSQL: a feed thread that holds the
//! replication connection and forwards raw `pgoutput` events (reopening
//! the slot when the connection drops), and, on the caller's engine
//! thread, the storage, the [`Service`] and the decoder that turns the
//! events into the service's writes and progress marks. Whoever serves
//! clients talks to this side over the service's channels, commands in
//! and events out, and never touches a database connection.
//!
//! A consumer that must read some rows for itself (an application's
//! mutation-id table, say) names those tables in [`Settings::watched`].
//! Their writes arrive on a third channel as one batch per transaction,
//! sent before that transaction's progress mark, so the consumer takes
//! exactly one batch per [`Event::Moved`] and never mixes a later
//! transaction's rows into an earlier poke.

use std::rc::Rc;
use std::time::Duration;

use pgwire_replication::ReplicationEvent;
use tokio::sync::mpsc;
use tokio::task::spawn_local;

use super::{Feed, PgStorage, Transport, load_catalog};
use crate::ivm::MultiTableIVM;
use crate::log::{log_error, log_info, log_warn};
use crate::model::{Catalog, MultiTableReadQuery, TableName, WriteQuery};
use crate::sync::{Command, Event, Service, Sources};

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
    pub watched: Vec<TableName>,
}

/// The running engine side: commands in, events out, and one batch of
/// watched writes per transaction, in step with [`Event::Moved`].
pub struct Started {
    pub commands: mpsc::Sender<Command<MultiTableReadQuery>>,
    pub events: mpsc::UnboundedReceiver<Event>,
    pub watched: mpsc::UnboundedReceiver<Vec<WriteQuery>>,
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

/// Start the feed thread: it forwards every replication event into the
/// returned channel, reopens the slot with a backoff when the connection
/// drops, and ends the process when the receiver is gone.
pub fn spawn_feed(settings: Settings) -> Result<mpsc::Receiver<ReplicationEvent>, String> {
    let (events_tx, events_rx) = mpsc::channel(4096);
    std::thread::Builder::new()
        .name("xyne-sync-feed".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("feed runtime");
            let local = tokio::task::LocalSet::new();
            local.block_on(&runtime, async move {
                let mut failures = 0u32;
                loop {
                    match Transport::open(&settings.dsn, &settings.slot).await {
                        Ok(transport) => {
                            log_info!("change feed open on slot {}", settings.slot);
                            failures = 0;
                            transport.run(settings.heartbeat, events_tx.clone()).await;
                            if events_tx.is_closed() {
                                return;
                            }
                            log_warn!("change feed dropped; reopening the slot");
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
    Ok(events_rx)
}

/// Bring the engine side up on the current thread's local set: the
/// storage over `settings`, the service, and the decoder pumping `feed`
/// into it. Must run inside a `LocalSet`.
pub async fn start(
    settings: &Settings,
    catalog: Rc<Catalog>,
    feed: mpsc::Receiver<ReplicationEvent>,
) -> Result<Started, String> {
    let pg = PgStorage::connect(&settings.dsn, catalog.clone())
        .await
        .map_err(|error| format!("connecting the storage: {error}"))?
        .with_rotation(settings.snapshot_rotation)
        .with_read_connections(settings.read_connections);
    let cached = Sources::cached_from_env();
    let storage = Rc::new(Sources::new(Rc::new(pg), catalog.clone(), cached.clone()));
    if !cached.is_empty() {
        let rows = storage
            .warm()
            .await
            .map_err(|error| format!("warming the memory tables: {error}"))?;
        log_info!("memory tables warmed with {rows} rows");
    }
    let (events_tx, events) = mpsc::unbounded_channel();
    let (watched_tx, watched) = mpsc::unbounded_channel();
    let (service, commands) = Service::new(MultiTableIVM::new(), storage, events_tx);
    spawn_local(service.run());
    spawn_local(pump(
        Feed::new(catalog),
        feed,
        commands.clone(),
        settings.watched.clone(),
        watched_tx,
    ));
    Ok(Started {
        commands,
        events,
        watched,
    })
}

/// Decode every replication event and hand the service each transaction
/// as one commit followed by its progress mark, with the transaction's
/// watched writes going to the consumer first, as one batch; a decoding
/// failure or the end of the feed ends the process.
async fn pump(
    mut feed: Feed,
    mut events: mpsc::Receiver<ReplicationEvent>,
    commands: mpsc::Sender<Command<MultiTableReadQuery>>,
    watched_tables: Vec<TableName>,
    watched: mpsc::UnboundedSender<Vec<WriteQuery>>,
) {
    while let Some(event) = events.recv().await {
        match feed.absorb(event) {
            Ok(Some(transaction)) => {
                let at = transaction.at;
                let batch: Vec<WriteQuery> = transaction
                    .writes
                    .iter()
                    .filter(|write| watched_tables.contains(write.table()))
                    .cloned()
                    .collect();
                if watched.send(batch).is_err() {
                    return;
                }
                if commands
                    .send(Command::Commit {
                        writes: transaction.writes,
                        at,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
                if commands
                    .send(Command::Progress(feed.progress()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Ok(None) => {}
            Err(error) => {
                log_error!("change feed decoding failed: {error}");
                std::process::exit(1);
            }
        }
    }
    log_error!("the change feed ended");
    std::process::exit(1);
}
