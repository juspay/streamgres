//! The asynchronous driver: one task owns the runtime, takes commands
//! (subscribe, unsubscribe, a positioned write, a progress mark) from a
//! channel, hands every delta to an output channel, and runs each storage
//! read the runtime asks for as its own task, reporting the result back
//! into the loop. The runtime is touched only between awaits, never
//! across one, so any number of reads can be out while writes keep
//! flowing. Single-threaded by design: run it on a
//! [`tokio::task::LocalSet`].

use std::rc::Rc;

use tokio::sync::{mpsc, oneshot};
use tokio::task::spawn_local;

use crate::model::{Lsn, Snapshot};
use super::runtime::{Runtime, Step};
use super::storage::{Storage, StorageError};
use crate::ivm::{Engine, FetchId};
use crate::model::{SubId, WriteQuery};

/// What a client of the service can ask.
///
/// - `Register`: subscribe; the id comes back on `reply`, the snapshot
///   through the delta channel like every other update.
/// - `Unregister`: unsubscribe.
/// - `Write`: one write the change feed delivered, with its commit
///   location.
/// - `Progress`: the feed has delivered everything up to `lsn`.
pub enum Command<Q> {
    Register {
        query: Q,
        reply: oneshot::Sender<SubId>,
    },
    Unregister(SubId),
    Write {
        write: WriteQuery,
        at: Lsn,
    },
    Progress(Lsn),
}

/// The loop's handles: the command inlet and the delta outlet.
pub struct Service<E: Engine, S: Storage> {
    runtime: Runtime<E>,
    storage: Rc<S>,
    commands: mpsc::Receiver<Command<E::Query>>,
    updates: mpsc::UnboundedSender<Vec<E::Update>>,
    results: mpsc::UnboundedReceiver<(FetchId, Result<Snapshot, StorageError>)>,
    report: mpsc::UnboundedSender<(FetchId, Result<Snapshot, StorageError>)>,
}

impl<E, S> Service<E, S>
where
    E: Engine + 'static,
    E::Query: 'static,
    E::Update: 'static,
    S: Storage + 'static,
{
    /// A service over `engine` and `storage`, delivering deltas to
    /// `updates`; returns it with the command sender to drive it by.
    pub fn new(
        engine: E,
        storage: Rc<S>,
        updates: mpsc::UnboundedSender<Vec<E::Update>>,
    ) -> (Self, mpsc::Sender<Command<E::Query>>) {
        let (commands_tx, commands) = mpsc::channel(1024);
        let (report, results) = mpsc::unbounded_channel();
        let service = Service {
            runtime: Runtime::new(engine),
            storage,
            commands,
            updates,
            results,
            report,
        };
        (service, commands_tx)
    }

    /// Run until every command sender is dropped; returns the runtime for
    /// inspection.
    pub async fn run(mut self) -> Runtime<E> {
        loop {
            let step = tokio::select! {
                command = self.commands.recv() => match command {
                    Some(command) => self.handle(command),
                    None => break,
                },
                result = self.results.recv() => match result {
                    Some((id, Ok(snapshot))) => self.runtime.fetched(id, snapshot),
                    Some((id, Err(error))) => {
                        eprintln!("storage read {} failed, retrying: {error}", id.0);
                        self.runtime.failed(id)
                    }
                    None => break,
                },
            };
            self.dispatch(step);
        }
        self.runtime
    }

    /// Apply one command to the runtime.
    fn handle(&mut self, command: Command<E::Query>) -> Step<E::Update> {
        match command {
            Command::Register { query, reply } => {
                let (sub, step) = self.runtime.register(query);
                let _ = reply.send(sub);
                step
            }
            Command::Unregister(sub) => {
                self.runtime.unregister(sub);
                Step::default()
            }
            Command::Write { write, at } => self.runtime.write(&write, at),
            Command::Progress(lsn) => self.runtime.progress(lsn),
        }
    }

    /// Deliver a step's deltas and start a task per read it asked for,
    /// each told the stream position the read must at least reflect.
    fn dispatch(&mut self, step: Step<E::Update>) {
        if !step.updates.is_empty() {
            let _ = self.updates.send(step.updates);
        }
        let at_least = self.runtime.stream_position();
        for fetch in step.selects {
            let storage = self.storage.clone();
            let report = self.report.clone();
            spawn_local(async move {
                let result = storage.select(&fetch.query, at_least).await;
                let _ = report.send((fetch.id, result));
            });
        }
    }
}
