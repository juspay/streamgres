//! The runtime: owns the threads and drives the data source.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use streamlib_datasource::{Feed, Source, SourceError};
use tokio::sync::{OnceCell, mpsc};

use crate::config::Config;
use crate::engine::{self, Command, Position, Status};
use crate::error::{Error, Result};

/// A running streamlib instance for one data source. Cheap to clone; all clones share it.
pub struct Runtime<S: Source> {
    inner: Arc<Inner<S>>,
}

impl<S: Source> Clone for Runtime<S> {
    fn clone(&self) -> Self {
        Runtime {
            inner: self.inner.clone(),
        }
    }
}

struct Inner<S: Source> {
    src: Arc<S>,
    status: Arc<Status<S>>,
    io: Mutex<Option<Io>>,
    engine: Mutex<Option<EngineThread>>,
    /// Every `shutdown` caller waits on this one run.
    stopped: OnceCell<()>,
    drop_grace: Duration,
}

struct EngineThread {
    commands: mpsc::UnboundedSender<Command>,
    thread: JoinHandle<()>,
}

/// The io runtime; it runs every source call, so the source's background tasks live here.
struct Io(Option<tokio::runtime::Runtime>);

impl Io {
    fn new(threads: usize) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads.max(1))
            .thread_name("streamlib-io")
            .enable_all()
            .build()
            .map_err(|e| Error::Start(format!("io runtime: {e}")))?;
        Ok(Io(Some(rt)))
    }

    fn handle(&self) -> &tokio::runtime::Handle {
        self.0
            .as_ref()
            .expect("io runtime is live until dropped")
            .handle()
    }

    /// Run a source call here and wait for it.
    async fn run<T: Send + 'static>(
        &self,
        task: impl Future<Output = std::result::Result<T, SourceError>> + Send + 'static,
    ) -> Result<T> {
        self.handle()
            .spawn(task)
            .await
            .map_err(|e| Error::Start(format!("io task: {e}")))?
            .map_err(Error::Source)
    }
}

impl Drop for Io {
    fn drop(&mut self) {
        // Never blocks, so it's safe even inside async code.
        if let Some(rt) = self.0.take() {
            rt.shutdown_background();
        }
    }
}

/// Undoes a half-finished `start` if its future is dropped (cancelled).
struct Starting<S: Source> {
    src: Arc<S>,
    io: Option<Io>,
    grace: Duration,
}

impl<S: Source> Drop for Starting<S> {
    fn drop(&mut self) {
        if let Some(io) = self.io.take() {
            cleanup_in_background(self.src.clone(), Some(io), None, self.grace);
        }
    }
}

impl<S: Source> Runtime<S> {
    /// Open the source, start its stream and the engine thread.
    pub async fn start(cfg: Config, source: S) -> Result<Self> {
        let mut starting = Starting {
            src: Arc::new(source),
            io: Some(Io::new(cfg.io_threads)?),
            grace: cfg.drop_grace,
        };
        match launch(&cfg, &starting).await {
            Ok((status, engine)) => Ok(Runtime {
                inner: Arc::new(Inner {
                    src: starting.src.clone(),
                    status,
                    io: Mutex::new(starting.io.take()),
                    engine: Mutex::new(Some(engine)),
                    stopped: OnceCell::new(),
                    drop_grace: cfg.drop_grace,
                }),
            }),
            Err(e) => {
                // Release anything `open` created before failing.
                if let Some(io) = starting.io.take() {
                    let _ = io.run(close(starting.src.clone())).await;
                }
                Err(e)
            }
        }
    }

    /// Stop the engine within `grace`, close the source, stop the threads; every concurrent caller waits for it.
    pub async fn shutdown(&self, grace: Duration) {
        self.inner
            .stopped
            .get_or_init(|| stop(&self.inner, grace))
            .await;
    }

    /// True until shutdown or a failure the engine can't recover from.
    pub fn is_running(&self) -> bool {
        let stopped = self.inner.status.progress.borrow().stopped;
        !stopped && self.failure().is_none() && self.inner.engine.lock().unwrap().is_some()
    }

    /// Why the engine stopped on its own, if it did.
    pub fn failure(&self) -> Option<Error> {
        self.inner.status.failure.lock().unwrap().clone()
    }

    /// How many times the stream was resumed after a transient failure.
    pub fn reconnects(&self) -> u64 {
        self.inner
            .status
            .reconnects
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The last position the engine applied.
    pub fn applied(&self) -> Position<S> {
        self.inner.status.progress.borrow().applied.clone()
    }

    /// Wait until the engine has applied `position`; fails fast if the engine stops first.
    pub async fn wait_applied(&self, position: &Position<S>, timeout: Duration) -> Result<()> {
        let mut progress = self.inner.status.progress.subscribe();
        let settled = progress.wait_for(|p| p.applied >= *position || p.stopped);
        let reached = match tokio::time::timeout(timeout, settled).await {
            Ok(Ok(p)) => p.applied >= *position,
            Ok(Err(_)) => false,
            Err(_) => {
                return Err(Error::Source(SourceError::Transient(format!(
                    "not applied within {timeout:?}"
                ))));
            }
        };
        if reached {
            Ok(())
        } else {
            Err(self.failure().unwrap_or(Error::Closed))
        }
    }

    /// The data source, for operations outside the runtime (tests, admin).
    pub fn source(&self) -> &S {
        &self.inner.src
    }
}

impl<S: Source> Drop for Inner<S> {
    fn drop(&mut self) {
        let engine = self.engine.get_mut().unwrap().take();
        let io = self.io.get_mut().unwrap().take();
        if engine.is_some() || io.is_some() {
            tracing::warn!("runtime dropped without shutdown; cleaning up in the background");
            cleanup_in_background(self.src.clone(), io, engine, self.drop_grace);
        }
    }
}

/// Open the source on the io runtime and start the engine thread.
async fn launch<S: Source>(
    cfg: &Config,
    starting: &Starting<S>,
) -> Result<(Arc<Status<S>>, EngineThread)> {
    let io = starting.io.as_ref().expect("io present while starting");
    let src = starting.src.clone();
    let setup = cfg.setup;
    let feed = io
        .run(async move {
            if setup {
                src.setup().await?;
            }
            src.open().await?;
            src.stream(None).await
        })
        .await?;
    starting.src.advance(&feed.start);
    let status = Arc::new(Status::new(feed.start.clone()));
    let engine = spawn_engine(
        starting.src.clone(),
        io.handle().clone(),
        feed,
        status.clone(),
        cfg.max_backoff,
    )?;
    Ok((status, engine))
}

fn spawn_engine<S: Source>(
    src: Arc<S>,
    io: tokio::runtime::Handle,
    feed: Feed<Position<S>>,
    status: Arc<Status<S>>,
    max_backoff: Duration,
) -> Result<EngineThread> {
    let (commands, receiver) = mpsc::unbounded_channel();
    let engine = engine::Engine {
        src,
        io,
        feed,
        commands: receiver,
        status,
        max_backoff,
        backoff: engine::FIRST_BACKOFF,
    };
    let thread = std::thread::Builder::new()
        .name("streamlib-engine".into())
        .spawn(move || engine::run_thread(engine))
        .map_err(|e| Error::Start(format!("engine thread: {e}")))?;
    Ok(EngineThread { commands, thread })
}

/// The one shutdown run behind `Runtime::shutdown`.
async fn stop<S: Source>(inner: &Inner<S>, grace: Duration) {
    let engine = inner.engine.lock().unwrap().take();
    if let Some(engine) = engine {
        let _ = engine.commands.send(Command::Shutdown);
        let deadline = Instant::now() + grace;
        while !engine.thread.is_finished() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if engine.thread.is_finished() {
            if engine.thread.join().is_err() {
                tracing::warn!("engine thread panicked");
            }
        } else {
            tracing::warn!(
                ?grace,
                "engine didn't stop in time; closing the source anyway"
            );
        }
    }
    let io = inner.io.lock().unwrap().take();
    if let Some(io) = io {
        let _ = io.run(close(inner.src.clone())).await;
    }
    inner.status.progress.send_modify(|p| p.stopped = true);
}

async fn close<S: Source>(src: Arc<S>) -> std::result::Result<(), SourceError> {
    src.close().await;
    Ok(())
}

/// Stop the engine and close the source on a separate thread, for `Drop` paths that can't await.
fn cleanup_in_background<S: Source>(
    src: Arc<S>,
    io: Option<Io>,
    engine: Option<EngineThread>,
    grace: Duration,
) {
    let spawned = std::thread::Builder::new()
        .name("streamlib-cleanup".into())
        .spawn(move || {
            if let Some(engine) = engine {
                let _ = engine.commands.send(Command::Shutdown);
                let deadline = Instant::now() + grace;
                while !engine.thread.is_finished() && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
            match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => {
                    // The timeout must be created inside the runtime it runs on.
                    let closed =
                        rt.block_on(async { tokio::time::timeout(grace, src.close()).await });
                    if closed.is_err() {
                        tracing::warn!(?grace, "source close timed out during cleanup");
                    }
                }
                Err(e) => tracing::error!(error = %e, "cleanup runtime failed; source not closed"),
            }
            drop(io);
        });
    if let Err(e) = spawned {
        tracing::error!(error = %e, "cleanup thread failed to start; source not closed");
    }
}
