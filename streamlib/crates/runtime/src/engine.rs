//! The engine thread: the single owner that applies the change stream in commit order.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use streamlib_datasource::{Feed, Snapshots, Source, SourceError};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, watch};

use crate::error::Error;

pub(crate) type Position<S> = <S as Snapshots>::Position;

pub(crate) enum Command {
    Shutdown,
}

/// What waiters see: how far the engine got, and whether it has stopped.
#[derive(Clone, Debug)]
pub(crate) struct Progress<P> {
    pub applied: P,
    pub stopped: bool,
}

/// Shared between the engine thread and the runtime handles.
pub(crate) struct Status<S: Source> {
    pub progress: watch::Sender<Progress<Position<S>>>,
    /// Why the engine stopped on its own, if it did.
    pub failure: Mutex<Option<Error>>,
    /// How many times the stream was resumed after a transient failure.
    pub reconnects: AtomicU64,
}

impl<S: Source> Status<S> {
    pub fn new(start: Position<S>) -> Self {
        Status {
            progress: watch::channel(Progress {
                applied: start,
                stopped: false,
            })
            .0,
            failure: Mutex::default(),
            reconnects: AtomicU64::new(0),
        }
    }

    fn fail(&self, error: Error) {
        tracing::error!(%error, "engine stopped");
        self.failure.lock().unwrap().get_or_insert(error);
    }

    fn stop(&self) {
        self.progress.send_modify(|p| p.stopped = true);
    }
}

/// Everything the engine thread owns.
pub(crate) struct Engine<S: Source> {
    pub src: Arc<S>,
    /// The io runtime, where stream calls run so the source's tasks live there.
    pub io: Handle,
    pub feed: Feed<Position<S>>,
    pub commands: mpsc::UnboundedReceiver<Command>,
    pub status: Arc<Status<S>>,
    pub max_backoff: Duration,
    /// Next wait before resuming; resets only once a txn arrives, so a flapping stream backs off.
    pub backoff: Duration,
}

/// First wait before resuming a lost stream.
pub(crate) const FIRST_BACKOFF: Duration = Duration::from_millis(100);

/// Run the engine on this thread until shutdown or failure; a panic is reported, not lost.
pub(crate) fn run_thread<S: Source>(engine: Engine<S>) {
    let status = engine.status.clone();
    let ran = std::panic::catch_unwind(AssertUnwindSafe(|| {
        match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt.block_on(run(engine)),
            Err(e) => status.fail(Error::Start(format!("engine runtime: {e}"))),
        }
    }));
    if let Err(panic) = ran {
        let message = panic
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic".into());
        status.fail(Error::Engine(format!("engine panicked: {message}")));
    }
    status.stop();
}

/// How the apply loop ended.
enum Exit {
    Shutdown,
    StreamLost(SourceError),
}

async fn run<S: Source>(mut engine: Engine<S>) {
    loop {
        match apply(&mut engine).await {
            Exit::Shutdown => return,
            Exit::StreamLost(SourceError::Transient(why)) => {
                tracing::warn!(%why, "change stream lost; resuming");
                match resume(&mut engine).await {
                    Some(feed) => {
                        engine.feed = feed;
                        engine.status.reconnects.fetch_add(1, Ordering::Relaxed);
                    }
                    None => return,
                }
            }
            Exit::StreamLost(e) => return engine.status.fail(Error::Source(e)),
        }
    }
}

/// Apply txns in order until shutdown or the stream fails.
async fn apply<S: Source>(engine: &mut Engine<S>) -> Exit {
    loop {
        tokio::select! {
            biased;
            command = engine.commands.recv() => match command {
                Some(Command::Shutdown) | None => return Exit::Shutdown,
            },
            txn = engine.feed.next() => match txn {
                Ok(txn) => {
                    engine.backoff = FIRST_BACKOFF;
                    // Queries come in step 2; for now every txn is applied by just moving forward.
                    engine.src.advance(&txn.position);
                    engine.src.ack(&txn.position);
                    engine.status.progress.send_modify(|p| p.applied = txn.position);
                }
                Err(e) => return Exit::StreamLost(e),
            },
        }
    }
}

/// Reopen the stream after the last applied position, with backoff; `None` on shutdown or failure.
async fn resume<S: Source>(engine: &mut Engine<S>) -> Option<Feed<Position<S>>> {
    loop {
        // Wait first: the old connection may still hold the stream for a moment.
        tokio::select! {
            biased;
            _ = engine.commands.recv() => return None,
            _ = tokio::time::sleep(engine.backoff) => {}
        }
        engine.backoff = (engine.backoff * 2).min(engine.max_backoff);
        let from = engine.status.progress.borrow().applied.clone();
        let src = engine.src.clone();
        let opened = engine
            .io
            .spawn(async move { src.stream(Some(&from)).await });
        let result = tokio::select! {
            biased;
            _ = engine.commands.recv() => return None,
            r = opened => r.unwrap_or_else(|e| Err(SourceError::Transient(format!("io task: {e}")))),
        };
        match result {
            Ok(feed) => return Some(feed),
            Err(SourceError::Transient(why)) => tracing::warn!(%why, "resume failed; retrying"),
            Err(e) => {
                engine.status.fail(Error::Source(e));
                return None;
            }
        }
    }
}
