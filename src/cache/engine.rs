//! The seam between the cache and the engine it keeps its queries in.
//!
//! The cache talks to an engine through [`Engine`] and nothing else: it
//! asks it to run one instance at a time, and drives that instance through
//! the [`Instance`] it is handed: commands in, events out, the catalog the
//! engine reads by, and samples of where the source is. Which database the
//! rows come from, how the change feed reaches the engine, and what an
//! instance leaves behind to clean up are the engine's own
//! (`crate::sync::embed` for Postgres). The trait is private to the crate.
//!
//! The supervisor here runs one instance after another: when one fails,
//! panics or does not start in time it is torn down and a fresh one
//! started after a wait that doubles while instances keep failing soon.
//! Stopping the cache ends whatever the supervisor is doing.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use super::rows::Rows;
use super::state::{Phase, Running, Shared};
use crate::log::{log_info, log_warn};
use crate::model::{MultiTableReadQuery, Position, SubId};
use crate::parser::Bind;
use crate::shutdown::Shutdown;
use crate::sync::{CatalogHandle, Command, Event};

/// A future the supervisor's thread polls; it need not be `Send`.
pub(crate) type LocalFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// A future any runtime may poll.
pub(crate) type SendFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What the cache drives a running instance by.
pub(crate) struct Instance {
    /// Where the cache sends subscriptions and their ends.
    pub commands: mpsc::Sender<Command<MultiTableReadQuery>>,
    /// What the engine reports: rows, positions, refusals.
    pub events: mpsc::UnboundedReceiver<Event>,
    /// The catalog the instance reads by, as it follows schema changes.
    pub catalog: Arc<CatalogHandle>,
    /// Where the source is, sampled apart from the change feed, with the
    /// instant just before each sample was asked for.
    pub source: mpsc::UnboundedReceiver<(Position, Instant)>,
    /// Ends when the instance fails, with why.
    pub failed: LocalFuture<'static, String>,
    /// What logs and metrics call this instance (a replication slot).
    pub name: String,
}

/// The cache's part of an instance's life: given the running instance, it
/// serves until the instance fails or the cache stops.
pub(crate) type Serve<'a> = Box<dyn FnOnce(Instance) -> LocalFuture<'a, Result<(), String>> + 'a>;

/// An engine the cache can run and read the origin of.
pub(crate) trait Engine: Send + Sync + 'static {
    /// Runs one instance on the calling thread: starts it within
    /// `start_timeout` (unless `stopping` is asked first), hands it to
    /// `serve`, and tears it down, whatever it left behind, once `serve`
    /// returns. Why it ended; `Ok` when the cache stopped it.
    fn run(
        &self,
        stopping: &Shutdown,
        start_timeout: Duration,
        serve: Serve<'_>,
    ) -> Result<(), String>;

    /// Reads `sql` with `binds` from the origin itself, as the fallback
    /// for a read the engine cannot answer, within `timeout`.
    fn read_origin<'a>(
        &'a self,
        sql: &'a str,
        binds: &'a [Bind],
        timeout: Duration,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> SendFuture<'a, Result<Rows, String>>;

    /// `position` as the source writes its own positions (a WAL location
    /// as `X/Y` for Postgres), for logs and inspection.
    fn position_text(&self, position: Position) -> String;

    /// Catalog admission must not depend on an engine instance being ready.
    fn origin_catalog(&self) -> SendFuture<'_, Result<Arc<crate::model::Catalog>, String>> {
        Box::pin(async { Err("origin catalog discovery is not available".to_owned()) })
    }

    /// Every type of the origin's own schemas (enums, domains, composites)
    /// with its OID and its array type's OID: what an adapter names a
    /// custom type's parameters by.
    fn origin_types(&self) -> SendFuture<'_, Result<Vec<TypeOid>, String>>;

    /// Runs `sql` with three text-array parameters on the origin and
    /// returns its one row's text columns: how the cache checks which
    /// tables its own connection reads otherwise than the engine.
    fn check_origin<'a>(
        &'a self,
        sql: &'a str,
        params: [Vec<String>; 3],
    ) -> SendFuture<'a, Result<Vec<String>, String>>;
}

/// A type of the origin: where it is, what it is called, its OID and its
/// array type's OID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TypeOid {
    pub schema: String,
    pub name: String,
    pub oid: u32,
    pub array_oid: u32,
    pub visible: bool,
}

/// The first wait after a failure.
const FIRST_BACKOFF: Duration = Duration::from_secs(1);
/// The longest wait between instances.
const LAST_BACKOFF: Duration = Duration::from_secs(30);
/// How long an instance must have run for its failure to start the waits
/// over.
const STEADY: Duration = Duration::from_secs(60);
/// How often the kept queries are swept ([`super::state::State::sweep`]).
const SWEEP_EVERY: Duration = Duration::from_secs(1);

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

/// Runs instances until the cache is stopped.
fn supervise(shared: &Shared) {
    supervise_with(shared, |shared| {
        let serve: Serve<'_> = Box::new(move |instance| Box::pin(serve(shared, instance)));
        shared
            .engine
            .run(&shared.stopping, shared.config.start_timeout, serve)
    });
}

fn supervise_with(shared: &Shared, mut run: impl FnMut(&Shared) -> Result<(), String>) {
    let mut backoff = FIRST_BACKOFF;
    let mut instances = 0u64;
    while !shared.stopping.requested() {
        if instances > 0 {
            shared.counters.restarts.fetch_add(1, Ordering::Relaxed);
        }
        instances += 1;
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

/// Marks the cache running on `instance`, then applies its events to the
/// cache until it fails or the cache is stopped; why it ended.
async fn serve(shared: &Shared, instance: Instance) -> Result<(), String> {
    let Instance {
        commands,
        mut events,
        catalog,
        mut source,
        mut failed,
        name,
    } = instance;
    let (unsubscribe, mut unsubscribed) = mpsc::unbounded_channel();
    {
        let mut state = shared.write();
        if matches!(state.phase, Phase::Stopped) {
            return Ok(());
        }
        state.started(Running {
            commands: commands.clone(),
            unsubscribe,
            catalog,
            name: name.clone(),
        });
    }
    shared.notify();
    log_info!("embedded engine running on {name}");
    let config = &shared.config;
    let mut sweep = tokio::time::interval(SWEEP_EVERY);
    loop {
        let ended: Vec<SubId> = tokio::select! {
            event = events.recv() => {
                let Some(event) = event else {
                    return Err("the engine's events ended".to_owned());
                };
                let ended = shared.write().absorb(event, config, &shared.counters);
                shared.notify();
                ended
            }
            sample = source.recv() => {
                let Some((position, sampled)) = sample else {
                    return Err("the source's samples ended".to_owned());
                };
                shared.write().observe_source(position, sampled);
                shared.notify();
                Vec::new()
            }
            sub = unsubscribed.recv() => sub.into_iter().collect(),
            _ = sweep.tick() => shared.write().sweep(shared.now(), config),
            () = shared.stopping.wait() => return Ok(()),
            reason = &mut failed => return Err(reason),
        };
        // An unsubscribe waits for room: one lost would leave the engine
        // keeping a query nobody reads.
        for sub in ended {
            tokio::select! {
                sent = commands.send(Command::Unregister(sub)) => {
                    sent.map_err(|_| "the engine's command channel closed".to_owned())?;
                }
                () = shared.stopping.wait() => return Ok(()),
                reason = &mut failed => return Err(reason),
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::cache::CacheConfig;

    /// An engine that runs nothing: each instance is handed to the test.
    pub(crate) struct Scripted<F: Fn(&Shutdown) -> Result<Instance, String> + Send + Sync + 'static>(
        pub F,
    );

    impl<F: Fn(&Shutdown) -> Result<Instance, String> + Send + Sync + 'static> Engine for Scripted<F> {
        fn run(
            &self,
            stopping: &Shutdown,
            _start_timeout: Duration,
            serve: Serve<'_>,
        ) -> Result<(), String> {
            let instance = (self.0)(stopping)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            runtime.block_on(serve(instance))
        }

        fn read_origin<'a>(
            &'a self,
            _sql: &'a str,
            _binds: &'a [Bind],
            _timeout: Duration,
            _permit: tokio::sync::OwnedSemaphorePermit,
        ) -> SendFuture<'a, Result<Rows, String>> {
            Box::pin(async { Err("no origin".to_owned()) })
        }

        fn check_origin<'a>(
            &'a self,
            _sql: &'a str,
            _params: [Vec<String>; 3],
        ) -> SendFuture<'a, Result<Vec<String>, String>> {
            Box::pin(async { Err("no origin".to_owned()) })
        }

        fn origin_types(&self) -> SendFuture<'_, Result<Vec<TypeOid>, String>> {
            Box::pin(async { Err("no origin".to_owned()) })
        }

        fn position_text(&self, position: Position) -> String {
            position.to_string()
        }
    }

    fn idle(stopping: &Shutdown) -> Result<Instance, String> {
        let _ = stopping;
        Err("no instance".to_owned())
    }

    #[test]
    fn a_panicking_engine_is_marked_down_and_restarted() {
        let shared = Shared::new(CacheConfig::default(), Arc::new(Scripted(idle)));
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
                    name: "injected".into(),
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
}
