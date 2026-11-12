//! The supervisor: a thread that runs one engine after another. Each runs
//! on a single-threaded runtime of its own (the engine's service is
//! single-threaded), with a small runtime of its own for storage reads,
//! so the host needs no runtime at all. When an engine fails, panics or
//! does not start in time it is torn down, its slot dropped, and a fresh
//! one started on a fresh slot after a wait that doubles while engines
//! keep failing soon. Stopping the cache ends whatever the supervisor is
//! doing: a start in progress, a running engine, a wait.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::LocalSet;

use super::state::{Phase, Running, Shared};
use crate::log::{log_info, log_warn};
use crate::model::{MultiTableReadQuery, SubId};
use crate::shutdown::Shutdown;
use crate::stats::Stats;
use crate::sync::pg::Transport;
use crate::sync::pg::threads::{self, FeedHandle, ServiceTask, Started};
use crate::sync::{CatalogHandle, Command, Event};

/// The first wait after a failure.
const FIRST_BACKOFF: Duration = Duration::from_secs(1);
/// The longest wait between engines.
const LAST_BACKOFF: Duration = Duration::from_secs(30);
/// How long an engine must have run for its failure to start the waits
/// over.
const STEADY: Duration = Duration::from_secs(60);
/// How often the kept queries are swept ([`super::state::State::sweep`]).
const SWEEP_EVERY: Duration = Duration::from_secs(1);
/// How long dropping a stopped engine's slot may take.
const SLOT_DROP_GRACE: Duration = Duration::from_secs(5);
/// How long the storage reads' runtime may take to end.
const READS_STOP_GRACE: Duration = Duration::from_secs(5);

/// Starts the supervisor thread; `None` when the thread could not start,
/// which leaves the cache down with the reason.
pub(super) fn spawn(shared: Arc<Shared>) -> Option<std::thread::JoinHandle<()>> {
    let supervised = shared.clone();
    let spawned = std::thread::Builder::new()
        .name("streamgres-supervisor".to_owned())
        .spawn(move || supervise(&supervised));
    match spawned {
        Ok(handle) => Some(handle),
        Err(error) => {
            shared.write().phase = Phase::Down(format!("the engine thread did not start: {error}"));
            shared.notify();
            None
        }
    }
}

/// Runs engines until the cache is stopped.
fn supervise(shared: &Shared) {
    supervise_with(shared, run_engine);
}

fn supervise_with(shared: &Shared, mut run: impl FnMut(&Shared) -> Result<(), String>) {
    let mut backoff = FIRST_BACKOFF;
    let mut engines = 0u64;
    while !shared.stopping.requested() {
        if engines > 0 {
            shared.counters.restarts.fetch_add(1, Ordering::Relaxed);
        }
        engines += 1;
        let started = Instant::now();
        let outcome =
            std::panic::catch_unwind(AssertUnwindSafe(|| run(shared))).unwrap_or_else(|panic| {
                Err(format!(
                    "the engine panicked: {}",
                    panic_text(panic.as_ref())
                ))
            });
        {
            let mut state = shared.write();
            if matches!(state.phase, Phase::Stopped) {
                return;
            }
            let reason = match outcome {
                Ok(()) => "the engine stopped".to_owned(),
                Err(reason) => reason,
            };
            log_warn!("embedded engine down: {reason}; starting another in {backoff:?}");
            state.phase = Phase::Down(reason);
        }
        shared.notify();
        if started.elapsed() >= STEADY {
            backoff = FIRST_BACKOFF;
        }
        let deadline = Instant::now() + backoff;
        while Instant::now() < deadline {
            if shared.stopping.requested() {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        backoff = (backoff * 2).min(LAST_BACKOFF);
    }
}

/// What a panic carried, when it was text.
fn panic_text(panic: &(dyn std::any::Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("no message")
}

/// An engine that has started.
struct Engine {
    commands: mpsc::Sender<Command<MultiTableReadQuery>>,
    events: mpsc::UnboundedReceiver<Event>,
    unsubscribe: mpsc::UnboundedReceiver<SubId>,
    feed: FeedHandle,
    service: ServiceTask,
}

/// One engine's life, from start to failure or stop.
fn run_engine(shared: &Shared) -> Result<(), String> {
    let engine = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("the engine runtime: {error}"))?;
    let reads = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("streamgres-reads")
        .enable_all()
        .build()
        .map_err(|error| format!("the read runtime: {error}"))?;
    let failing = Shutdown::new();
    let local = LocalSet::new();
    let slot = threads::slot_name();
    let cleanup = std::cell::Cell::new(false);
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| local.block_on(&engine, async {
        let start_timeout = shared.config.start_timeout;
        let started = tokio::select! {
            started = start(shared, reads.handle().clone(), failing.clone(), &slot, &cleanup) => started?,
            () = tokio::time::sleep(start_timeout) => {
                return Err(format!("the engine did not start within {start_timeout:?}"));
            }
            () = shared.stopping.wait() => return Ok(()),
        };
        let (outcome, feed) = serve(shared, started, &failing).await;
        failing.request();
        if let Err(error) = threads::stop_feed(feed).await {
            log_warn!("stopping the embedded engine's feed: {error}");
        }
        outcome
    }))).unwrap_or_else(|panic| Err(format!("the engine panicked: {}", panic_text(panic.as_ref()))));
    failing.request();
    // Cancels service tasks even when their engine future unwound.
    drop(local);
    if cleanup.get() {
        engine.block_on(async {
            match tokio::time::timeout(
                SLOT_DROP_GRACE,
                Transport::drop_slot_only(&shared.config.dsn, &slot),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => log_warn!("dropping slot {slot}: {error}"),
                Err(_) => log_warn!("dropping slot {slot} timed out"),
            }
        });
    }
    reads.shutdown_timeout(READS_STOP_GRACE);
    outcome
}

/// Brings an engine up and marks the cache running.
async fn start(
    shared: &Shared,
    reads: tokio::runtime::Handle,
    failing: Shutdown,
    slot: &str,
    cleanup: &std::cell::Cell<bool>,
) -> Result<Engine, String> {
    let config = &shared.config;
    let settings = config.settings(slot.to_owned());
    let source =
        threads::inspect_source(&config.dsn, &config.ddl_trigger(), &config.stack_schema()).await?;
    let catalog =
        threads::load_catalog_at(&config.dsn, &config.schemas, &config.publication).await?;
    let catalog = Arc::new(CatalogHandle::new(catalog));
    cleanup.set(true);
    let (started, service) = threads::start(
        &settings,
        catalog.clone(),
        reads,
        1,
        Stats::shared(),
        failing,
    )
    .await?;
    let Started {
        commands,
        mut events,
        feed,
        ..
    } = started;
    let events = events
        .pop()
        .ok_or_else(|| "the engine gave no event stream".to_owned())?;
    let (unsubscribe, unsubscribed) = mpsc::unbounded_channel();
    {
        let mut state = shared.write();
        if !matches!(state.phase, Phase::Stopped) {
            state.started(Running {
                commands: commands.clone(),
                unsubscribe,
                catalog,
                slot: settings.slot.clone(),
                cluster: source.cluster,
            });
        }
    }
    shared.notify();
    log_info!("embedded engine running on slot {}", settings.slot);
    Ok(Engine {
        commands,
        events,
        unsubscribe: unsubscribed,
        feed,
        service,
    })
}

/// Applies the engine's events to the cache until it fails or the cache
/// is stopped; why it ended, and its feed to stop.
async fn serve(
    shared: &Shared,
    engine: Engine,
    failing: &Shutdown,
) -> (Result<(), String>, FeedHandle) {
    let Engine {
        commands,
        mut events,
        mut unsubscribe,
        feed,
        mut service,
        ..
    } = engine;
    let config = &shared.config;
    let source_watch = watch_source(shared);
    tokio::pin!(source_watch);
    let mut sweep = tokio::time::interval(SWEEP_EVERY);
    let outcome = loop {
        let ended: Vec<SubId> = tokio::select! {
            source = &mut source_watch => break source,
            event = events.recv() => {
                let Some(event) = event else {
                    break Err("the engine's events ended".to_owned());
                };
                let ended = shared.write().absorb(event, config, &shared.counters);
                shared.notify();
                ended
            }
            sub = unsubscribe.recv() => sub.into_iter().collect(),
            _ = sweep.tick() => shared.write().sweep(shared.now(), config),
            () = shared.stopping.wait() => break Ok(()),
            () = failing.wait() => {
                break Err(failing.failure().unwrap_or_else(|| "the engine stopped".to_owned()));
            }
            ended = &mut service => {
                break Err(match ended {
                    Ok(_) => "the engine's service stopped".to_owned(),
                    Err(error) => format!("the engine's service panicked: {error}"),
                });
            }
        };
        // An unsubscribe waits for room: one lost would leave the engine
        // keeping a query nobody reads.
        for sub in ended {
            match unregister(&commands, sub, &shared.stopping, failing).await {
                Ok(true) => {}
                Ok(false) => return (Ok(()), feed),
                Err(reason) => return (Err(reason), feed),
            }
        }
    };
    (outcome, feed)
}

/// A full command queue delays an unsubscribe, never discards it. Stop
/// and failure remain selectable while waiting for the service to drain it.
async fn unregister(
    commands: &mpsc::Sender<Command<MultiTableReadQuery>>,
    sub: SubId,
    stopping: &Shutdown,
    failing: &Shutdown,
) -> Result<bool, String> {
    tokio::select! {
        sent = commands.send(Command::Unregister(sub)) => {
            sent.map_err(|_| "the service command channel closed".to_owned())?;
            Ok(true)
        }
        () = stopping.wait() => Ok(false),
        () = failing.wait() => Err("the engine stopped while unsubscribing".to_owned()),
    }
}

/// Polling SQL observes the source even while the feed is busy with old WAL.
/// Use the instant before the round trip so network delay never looks fresh.
async fn watch_source(shared: &Shared) -> Result<(), String> {
    let (client, connection) = tokio_postgres::connect(&shared.config.dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| format!("source freshness connection: {error}"))?;
    let connection = tokio::spawn(connection);
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    loop {
        tick.tick().await;
        let sampled = Instant::now();
        let row = client.query_one(
            "SELECT (CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END)::text", &[]
        ).await.map_err(|error| format!("source freshness query: {error}"))?;
        let text: String = row
            .try_get(0)
            .map_err(|error| format!("source freshness WAL position: {error}"))?;
        let position = crate::sync::pg::Lsn::parse(&text)
            .map(crate::model::Position::from)
            .ok_or_else(|| "source freshness query returned an invalid WAL position".to_owned())?;
        shared.write().observe_source(position, sampled);
        shared.notify();
        if connection.is_finished() {
            return Err("source freshness connection ended".to_owned());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_panicking_engine_is_marked_down_and_restarted() {
        let shared = Shared::new(super::super::CacheConfig::new(
            "postgresql://localhost/test",
            "p",
        ));
        let mut attempts = 0;
        let mut recovered = false;
        supervise_with(&shared, |shared| {
            attempts += 1;
            if attempts == 1 {
                let (commands, _) = mpsc::channel(1);
                let (unsubscribe, _) = mpsc::unbounded_channel();
                shared.write().started(Running {
                    commands,
                    unsubscribe,
                    catalog: Arc::new(CatalogHandle::new(crate::model::Catalog::new(vec![]))),
                    slot: "injected".into(),
                    cluster: "1".into(),
                });
                panic!("injected engine panic");
            }
            recovered = matches!(&shared.read().phase, Phase::Down(reason) if reason.contains("injected engine panic"));
            shared.stop();
            Ok(())
        });
        assert_eq!(attempts, 2);
        assert!(recovered);
        assert_eq!(shared.counters.restarts.load(Ordering::Relaxed), 1);
        assert!(matches!(shared.read().phase, Phase::Stopped));
    }
    #[test]
    fn a_full_command_queue_preserves_unsubscribes_and_allows_stop() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (commands, mut received) = mpsc::channel(1);
            commands.send(Command::Unregister(SubId(1))).await.unwrap();
            let stopping = Shutdown::new();
            let failing = Shutdown::new();
            let (sent, drained) = tokio::join!(
                unregister(&commands, SubId(2), &stopping, &failing),
                async {
                    assert!(matches!(
                        received.recv().await,
                        Some(Command::Unregister(SubId(1)))
                    ));
                    received.recv().await
                }
            );
            assert_eq!(sent, Ok(true));
            assert!(matches!(drained, Some(Command::Unregister(SubId(2)))));
            commands.send(Command::Unregister(SubId(3))).await.unwrap();
            let waiting = unregister(&commands, SubId(4), &stopping, &failing);
            tokio::pin!(waiting);
            tokio::select! {
                _ = &mut waiting => panic!("a full queue should wait"),
                () = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
            stopping.request();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), waiting)
                    .await
                    .unwrap(),
                Ok(false)
            );
        });
    }
}
