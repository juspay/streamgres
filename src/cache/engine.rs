//! The supervisor: a thread that runs one engine after another. Each runs
//! on a single-threaded runtime of its own (the engine's service is
//! single-threaded), with a small runtime of its own for storage reads,
//! so the host needs no runtime at all. When an engine fails it is torn
//! down, its slot dropped, and a fresh one started on a fresh slot after
//! a wait that doubles while engines keep failing soon.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::LocalSet;

use super::state::{Phase, Running, Shared};
use crate::log::{log_info, log_warn};
use crate::shutdown::Shutdown;
use crate::stats::Stats;
use crate::sync::pg::Transport;
use crate::sync::pg::threads::{self, Started};
use crate::sync::{CatalogHandle, Command};

/// The first wait after a failure.
const FIRST_BACKOFF: Duration = Duration::from_secs(1);
/// The longest wait between engines.
const LAST_BACKOFF: Duration = Duration::from_secs(30);
/// How long an engine must have run for its failure to start the waits
/// over.
const STEADY: Duration = Duration::from_secs(60);
/// How often idle queries are let go.
const EVICT_EVERY: Duration = Duration::from_secs(10);
/// How long dropping a stopped engine's slot may take.
const SLOT_DROP_GRACE: Duration = Duration::from_secs(5);

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
            shared.lock().phase = Phase::Down(format!("the engine thread did not start: {error}"));
            shared.notify();
            None
        }
    }
}

/// Runs engines until the cache is stopped.
fn supervise(shared: &Shared) {
    let mut backoff = FIRST_BACKOFF;
    loop {
        if matches!(shared.lock().phase, Phase::Stopped) {
            return;
        }
        let started = Instant::now();
        let outcome = run_engine(shared);
        {
            let mut state = shared.lock();
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
            if matches!(shared.lock().phase, Phase::Stopped) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        backoff = (backoff * 2).min(LAST_BACKOFF);
    }
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
    let config = shared.config.clone();
    let settings = config.settings(threads::slot_name());
    let shutdown = Shutdown::new();
    let local = LocalSet::new();
    let outcome = local.block_on(&engine, async {
        let catalog =
            threads::load_catalog_at(&config.dsn, &config.schemas, &config.publication).await?;
        threads::require_ddl_trigger(&config.dsn, &config.ddl_trigger()).await?;
        let catalog = Arc::new(CatalogHandle::new(catalog));
        let (started, mut service) = threads::start(
            &settings,
            catalog.clone(),
            reads.handle().clone(),
            1,
            Stats::shared(),
            shutdown.clone(),
        )
        .await?;
        let Started {
            commands,
            mut events,
            feed,
            ..
        } = started;
        let mut events = events
            .pop()
            .ok_or_else(|| "the engine gave no event stream".to_owned())?;
        {
            let mut state = shared.lock();
            if matches!(state.phase, Phase::Stopped) {
                shutdown.request();
            } else {
                state.running(Running {
                    commands: commands.clone(),
                    catalog,
                    shutdown: shutdown.clone(),
                    slot: settings.slot.clone(),
                });
            }
        }
        shared.notify();
        log_info!("embedded engine running on slot {}", settings.slot);

        let mut evict = tokio::time::interval(EVICT_EVERY);
        let outcome = loop {
            tokio::select! {
                event = events.recv() => {
                    let Some(event) = event else {
                        break Err("the engine's events ended".to_owned());
                    };
                    let orphans = shared.lock().absorb(event);
                    shared.notify();
                    for sub in orphans {
                        let _ = commands.try_send(Command::Unregister(sub));
                    }
                }
                _ = evict.tick() => {
                    let idle = shared.lock().evict_idle(config.idle_ttl);
                    for sub in idle {
                        let _ = commands.try_send(Command::Unregister(sub));
                    }
                }
                () = shutdown.wait() => {
                    break shutdown.failure().map_or(Ok(()), Err);
                }
                ended = &mut service => {
                    break Err(match ended {
                        Ok(_) => "the engine's service stopped".to_owned(),
                        Err(error) => format!("the engine's service panicked: {error}"),
                    });
                }
            }
        };
        shutdown.request();
        if let Err(error) = threads::stop_feed(feed).await {
            log_warn!("stopping the embedded engine's feed: {error}");
        }
        match tokio::time::timeout(
            SLOT_DROP_GRACE,
            Transport::drop_slot_only(&settings.dsn, &settings.slot),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => log_warn!("dropping slot {}: {error}", settings.slot),
            Err(_) => log_warn!("dropping slot {} timed out", settings.slot),
        }
        outcome
    });
    reads.shutdown_timeout(Duration::from_secs(5));
    outcome
}
