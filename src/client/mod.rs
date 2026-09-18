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
//! - **Feed thread.** The replication connection, decoding the `pgoutput`
//!   events into rows and handing the engine one transaction per commit,
//!   and a heartbeat so the engine's position moves while nothing is
//!   written. Started by the sync side.
//! - **Engine thread.** The service: the runtime and the engine, and
//!   nothing else; the storage reads it asks for run on the reads pool.
//! - **Group threads.** `XYNE_SYNC_GROUP_THREADS` of them, each keeping
//!   the views of the client groups that hash to it and building their
//!   pokes ([`groups`]).
//! - **Reads pool.** A small runtime for the storage: the SQL, the
//!   connections, the row decoding; the planner's counts and the
//!   connect-time reads go there too.
//! - **Server threads.** A multi-threaded runtime for the WebSocket
//!   connections ([`connection`]): the handshake, the message loop, the
//!   liveness rules, the HTTP calls to the application server, and the
//!   translation and planning of every query.
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
    let catalog =
        Arc::new(server.block_on(threads::load_catalog_at(&settings.dsn, &settings.schemas))?);
    let reads = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(settings.read_threads)
        .enable_all()
        .thread_name("xyne-sync-reads")
        .build()
        .map_err(|error| format!("tokio: {error}"))?;
    let feed = threads::spawn_feed(settings.clone(), catalog.clone())?;
    let shards = config.group_threads.max(1);
    let stats = crate::stats::Stats::shared();

    let engine_catalog = catalog.clone();
    let engine_stats = stats.clone();
    let reads_handle = reads.handle().clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel::<Result<threads::Started, String>>();
    std::thread::Builder::new()
        .name("xyne-sync-engine".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("engine runtime");
            let local = tokio::task::LocalSet::new();
            local.block_on(&runtime, async move {
                match threads::start(
                    &settings,
                    engine_catalog,
                    feed,
                    reads_handle,
                    shards,
                    engine_stats,
                )
                .await
                {
                    Ok((engine, service)) => {
                        let _ = started_tx.send(Ok(engine));
                        match service.await {
                            Ok(_) => log_error!("the engine's service stopped"),
                            Err(error) => log_error!("the engine's service panicked: {error}"),
                        }
                        std::process::exit(1);
                    }
                    Err(error) => {
                        let _ = started_tx.send(Err(error));
                    }
                }
            });
        })
        .map_err(|error| format!("engine thread: {error}"))?;
    let threads::Started {
        commands,
        events,
        storage,
    } = started_rx
        .recv()
        .map_err(|_| "the engine thread ended before it was up".to_owned())??;

    let (readiness, ready) = tokio::sync::watch::channel(false);
    let mut requests = Vec::with_capacity(shards);
    for (shard, events) in events.into_iter().enumerate() {
        let (requests_tx, requests_rx) = mpsc::channel::<groups::Request>(1024);
        requests.push(requests_tx.clone());
        let config = config.clone();
        let catalog = catalog.clone();
        let commands = commands.clone();
        let stats = stats.clone();
        let readiness = readiness.clone();
        std::thread::Builder::new()
            .name(format!("xyne-sync-groups-{shard}"))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("groups runtime");
                let local = tokio::task::LocalSet::new();
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    local.block_on(
                        &runtime,
                        groups::run(
                            config,
                            catalog,
                            shard,
                            shards,
                            commands,
                            events,
                            requests_rx,
                            requests_tx,
                            stats,
                            readiness,
                        ),
                    )
                }));
                match outcome {
                    Ok(()) => log_error!("group thread {shard} stopped"),
                    Err(_) => log_error!("group thread {shard} panicked"),
                }
                std::process::exit(1);
            })
            .map_err(|error| format!("group thread {shard}: {error}"))?;
    }

    let backend = Arc::new(backend::Backend::new(&config)?);
    let lmids = Arc::new(connection::LmidReader::new(
        storage.clone(),
        &config.upstream_schema(),
    ));
    let plans = Arc::new(plan::PlanCache::new(config.plan_ttl, config.plan_cache));
    let state = Arc::new(connection::AppState {
        config,
        requests,
        backend,
        storage,
        catalog,
        plans,
        lmids,
        stats,
        ready,
    });
    drop(readiness);
    log_info!("client side up");
    let served = server.block_on(connection::serve(state));
    drop(reads);
    served
}
