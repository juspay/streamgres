//! The asynchronous driver: one task owns the runtime, takes commands
//! (subscribe, unsubscribe, a positioned write, a progress mark) from a
//! channel, hands every delta to an output channel, and runs the storage
//! reads the runtime asks for. Every read, a registration's snapshot as
//! much as a join fetch or a window refill, runs as its own task while the
//! loop keeps routing; the loop never waits on storage, and each result is
//! brought up to the engine's position when it lands. The runtime is
//! touched only between awaits. Single-threaded by design: run it on a
//! [`tokio::task::LocalSet`].

use std::rc::Rc;

use tokio::sync::{mpsc, oneshot};
use tokio::task::spawn_local;

use super::runtime::{Runtime, Step};
use super::storage::{Storage, StorageError};
use crate::ivm::{ClientUpdate, Engine, Fetch, FetchId};
use crate::model::{ClientId, Lsn, Snapshot, SubId, WriteQuery};

/// What a client of the service can ask.
///
/// - `Register`: subscribe for `client`; the id comes back on `reply`,
///   the snapshot through the delta channel like every other update.
/// - `Unregister`: unsubscribe one subscription.
/// - `UnregisterClient`: a client went away; every subscription of it goes.
/// - `Write`: one write the change feed delivered, with its commit
///   location.
/// - `Progress`: the feed has delivered everything up to `lsn`.
pub enum Command<Q> {
    Register {
        client: ClientId,
        query: Q,
        reply: oneshot::Sender<SubId>,
    },
    Unregister(SubId),
    UnregisterClient(ClientId),
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
    updates: mpsc::UnboundedSender<Vec<ClientUpdate>>,
    results: mpsc::UnboundedReceiver<(FetchId, Result<Snapshot, StorageError>)>,
    report: mpsc::UnboundedSender<(FetchId, Result<Snapshot, StorageError>)>,
}

impl<E, S> Service<E, S>
where
    E: Engine + 'static,
    E::Query: 'static,
    S: Storage + 'static,
{
    /// A service over `engine` and `storage`, delivering deltas to
    /// `updates`; returns it with the command sender to drive it by.
    pub fn new(
        engine: E,
        storage: Rc<S>,
        updates: mpsc::UnboundedSender<Vec<ClientUpdate>>,
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
                        eprintln!("storage read {} failed, parked: {error}", id.0);
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
    fn handle(&mut self, command: Command<E::Query>) -> Step {
        match command {
            Command::Register {
                client,
                query,
                reply,
            } => {
                let (sub, step) = self.runtime.register(client, query);
                let _ = reply.send(sub);
                step
            }
            Command::Unregister(sub) => {
                self.runtime.unregister(sub);
                Step::default()
            }
            Command::UnregisterClient(client) => {
                self.runtime.unregister_client(client);
                Step::default()
            }
            Command::Write { write, at } => {
                self.storage.absorb(&write, at);
                let step = self.runtime.write(&write, at);
                self.moved();
                step
            }
            Command::Progress(lsn) => {
                let step = self.runtime.progress(lsn);
                self.moved();
                step
            }
        }
    }

    /// The stream moved: tell the storage, and learn its floor.
    fn moved(&mut self) {
        self.storage.advance(self.runtime.position());
        self.runtime.set_floor(self.storage.floor());
    }

    /// Deliver a step's deltas and start each of its reads as a task.
    fn dispatch(&self, step: Step) {
        if !step.updates.is_empty() {
            let _ = self.updates.send(step.updates);
        }
        for fetch in step.selects {
            self.spawn(fetch);
        }
    }

    /// Run one read as its own task, reporting the result into the loop.
    fn spawn(&self, fetch: Fetch) {
        let storage = self.storage.clone();
        let report = self.report.clone();
        spawn_local(async move {
            let result = storage.select(&fetch.query).await;
            let _ = report.send((fetch.id, result));
        });
    }
}
