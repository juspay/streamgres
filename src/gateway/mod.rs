//! The sync gateway: the server a Zero client (`@rocicorp/zero` 1.9, sync
//! protocol 51) connects to in place of zero-cache, with the IVM engine
//! behind it.
//!
//! # Threads
//!
//! - **Feed thread.** The replication connection: it forwards raw
//!   `pgoutput` events to the engine thread and emits a heartbeat at an
//!   interval so the engine's position moves while nothing is written.
//! - **Engine thread.** The runtime, the decoder, every client group's
//!   view, and the storage reads, which run as tasks on its local set and
//!   land when they return ([`core`]). Every value type the engine holds
//!   is thread-bound, so this is the only thread that touches writes,
//!   queries or deltas; it hands connections finished frames.
//! - **Server threads.** A multi-threaded runtime for the WebSocket
//!   connections ([`connection`]): the handshake, the message loop, the
//!   liveness rules, and the HTTP calls to the application server
//!   ([`backend`]) for query ASTs and mutations.
//!
//! # What a connection gets
//!
//! `connected`, then pokes: each a versioned batch of row `put`s and
//! `del`s, `desiredQueriesPatches` echoing what the client asked for,
//! `gotQueriesPatch` once a query's first rows have all arrived, and
//! `lastMutationIDChanges` as the application server records mutations
//! (read off the `<app>_<shard>.clients` table in the same transaction
//! as the mutation's rows). A `pong` answers every `ping`, and one goes
//! out on the server's own account when the downstream is quiet.
//!
//! # Not yet
//!
//! A client group's history is not kept: a client reconnecting with a
//! cookie the gateway does not hold (after a restart, or after changes it
//! missed) is told to start over. Query TTLs are not honored (a query
//! nobody desires is released at once). Inspector messages are ignored.

pub mod ast;
pub mod backend;
pub mod config;
pub mod connection;
pub mod core;
pub mod log;
pub mod protocol;
pub mod wire;

use std::sync::Arc;

use tokio::sync::mpsc;

pub use config::Config;

use self::log::{gw_error, gw_info, gw_warn};
use crate::sync::pg::{Transport, load_catalog};

/// Run the gateway with `config` until ctrl-c; returns the reason it could
/// not start.
pub fn serve(config: Config) -> Result<(), String> {
    log::set_level(config.log);
    let config = Arc::new(config);
    let server = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("xyne-sync-server")
        .build()
        .map_err(|error| format!("tokio: {error}"))?;
    let catalog = server.block_on(catalog(&config))?;
    let (requests_tx, requests_rx) = mpsc::channel::<core::Request>(1024);
    let (events_tx, events_rx) = mpsc::channel(4096);

    let engine_config = config.clone();
    let engine_catalog = catalog.clone();
    let engine_requests = requests_tx.clone();
    std::thread::Builder::new()
        .name("xyne-sync-engine".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("engine runtime");
            let local = tokio::task::LocalSet::new();
            let outcome = local.block_on(
                &runtime,
                core::run(
                    engine_config,
                    engine_catalog,
                    requests_rx,
                    engine_requests,
                    events_rx,
                ),
            );
            if let Err(error) = outcome {
                gw_error!("engine thread stopped: {error}");
            } else {
                gw_info!("engine thread stopped");
            }
            std::process::exit(1);
        })
        .map_err(|error| format!("engine thread: {error}"))?;

    let feed_config = config.clone();
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
                    match Transport::open(&feed_config.dsn, &feed_config.slot).await {
                        Ok(transport) => {
                            gw_info!("change feed open on slot {}", feed_config.slot);
                            failures = 0;
                            transport
                                .run(feed_config.heartbeat, events_tx.clone())
                                .await;
                            if events_tx.is_closed() {
                                return;
                            }
                            gw_warn!("change feed dropped; reopening the slot");
                        }
                        Err(error) => {
                            failures += 1;
                            gw_error!("opening the change feed (attempt {failures}): {error}");
                        }
                    }
                    let backoff = std::time::Duration::from_secs(1 << failures.min(4));
                    tokio::time::sleep(backoff).await;
                }
            });
            gw_error!("change feed thread stopped: the engine is gone");
            std::process::exit(1);
        })
        .map_err(|error| format!("feed thread: {error}"))?;

    let backend = Arc::new(backend::Backend::new(&config)?);
    let lmids = Arc::new(connection::LmidReader::new(
        &config.dsn,
        &config.upstream_schema(),
    ));
    let state = Arc::new(connection::AppState {
        config,
        requests: requests_tx,
        backend,
        lmids,
    });
    server.block_on(connection::serve(state))
}

/// Load the catalog of the configured schemas over a connection of its
/// own.
async fn catalog(config: &Config) -> Result<crate::model::Catalog, String> {
    let (client, connection) = tokio_postgres::connect(&config.dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| format!("connecting to the database: {error}"))?;
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let (catalog, notes) = load_catalog(&client, &config.schemas)
        .await
        .map_err(|error| format!("reading the catalog: {error}"))?;
    drop(client);
    let _ = handle.await;
    for note in &notes {
        gw_info!("catalog: {note}");
    }
    let tables = config
        .schemas
        .iter()
        .map(|schema| schema.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    gw_info!(
        "catalog loaded from {tables}: {} tables",
        catalog.tables().count()
    );
    Ok(catalog)
}
