//! The client side: the server a Zero client (`@rocicorp/zero` 1.9, sync
//! protocol 51) connects to in place of zero-cache, with the IVM engine
//! behind it.
//!
//! Everything here is about clients. It speaks their protocol
//! ([`protocol`]), translates the ASTs their queries arrive as into the
//! engine's trees ([`ast`]), calls the application server for those ASTs
//! and for their mutations ([`backend`]), decides which side of a join
//! is read whole before registering it ([`plan`]), keeps each client
//! group's view and builds its pokes ([`groups`]). The engine, its storage and the
//! change feed are none of its business: it reaches them only through
//! [`crate::sync::Service`]'s channels, which [`crate::sync::pg`] wires
//! up ([`crate::sync::pg::threads`]).
//!
//! # Threads
//!
//! - **Feed thread.** The replication connection, forwarding raw
//!   `pgoutput` events and a heartbeat so the engine's position moves
//!   while nothing is written. Started by the sync side.
//! - **Engine thread.** The service (the runtime, the storage reads as
//!   tasks on its local set) and, on the same thread because every value
//!   the engine holds is thread-bound, the decoder and this module's
//!   client-group views. The two halves speak only over channels, so the
//!   split is a thread boundary away.
//! - **Server threads.** A multi-threaded runtime for the WebSocket
//!   connections ([`connection`]): the handshake, the message loop, the
//!   liveness rules, and the HTTP calls to the application server.
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
//! cookie this server does not hold (after a restart, or after changes it
//! missed) is told to start over. Inspector messages are ignored.

pub mod ast;
pub mod backend;
pub mod config;
pub mod connection;
pub mod groups;
pub mod plan;
pub mod protocol;
pub mod wire;

use std::rc::Rc;
use std::sync::Arc;

use tokio::sync::mpsc;

pub use config::Config;

use crate::log::{self, log_error, log_info};
use crate::sync::pg::threads;

/// Run the server with `config` until ctrl-c; returns the reason it could
/// not start.
pub fn serve(config: Config) -> Result<(), String> {
    log::set_level(config.log);
    let config = Arc::new(config);
    let settings = config.engine_settings();
    let server = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("xyne-sync-server")
        .build()
        .map_err(|error| format!("tokio: {error}"))?;
    let catalog = server.block_on(threads::load_catalog_at(&settings.dsn, &settings.schemas))?;
    let feed = threads::spawn_feed(settings.clone())?;
    let (requests_tx, requests_rx) = mpsc::channel::<groups::Request>(1024);

    let engine_config = config.clone();
    let engine_requests = requests_tx.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel::<Result<(), String>>();
    std::thread::Builder::new()
        .name("xyne-sync-engine".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("engine runtime");
            let local = tokio::task::LocalSet::new();
            let served = local.block_on(&runtime, async move {
                let catalog = Rc::new(catalog);
                let engine = match threads::start(&settings, catalog.clone(), feed).await {
                    Ok(engine) => {
                        let _ = started_tx.send(Ok(()));
                        engine
                    }
                    Err(error) => {
                        let _ = started_tx.send(Err(error));
                        return false;
                    }
                };
                groups::run(engine_config, catalog, engine, requests_rx, engine_requests).await;
                true
            });
            if served {
                log_error!("engine thread stopped");
                std::process::exit(1);
            }
        })
        .map_err(|error| format!("engine thread: {error}"))?;
    started_rx
        .recv()
        .map_err(|_| "the engine thread ended before it was up".to_owned())??;

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
    log_info!("client side up");
    server.block_on(connection::serve(state))
}
