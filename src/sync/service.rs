//! The asynchronous driver: one task owns the runtime, takes commands
//! (subscribe, unsubscribe, a positioned write, a progress mark) from a
//! channel, hands every delta to an output channel, and runs the storage
//! reads the runtime asks for. A registration's snapshot read runs as its
//! own task while writes keep flowing; a read asked for in the middle of
//! maintaining a subscription (a join crossing, a window refill) is run
//! **before anything else**, the loop awaiting it and taking no command
//! meanwhile, so it lands at exactly the position the engine is at. The
//! runtime is touched only between awaits. Single-threaded by design: run
//! it on a [`tokio::task::LocalSet`].

use std::collections::VecDeque;
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
            self.dispatch(step).await;
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

    /// Deliver a step's deltas and run its reads: a snapshot read as its
    /// own task, a blocking read right here, landed before returning
    /// (and whatever that landing asks for, the same way).
    async fn dispatch(&mut self, step: Step) {
        let mut queue: VecDeque<Fetch> = VecDeque::new();
        self.emit(step, &mut queue);
        while let Some(fetch) = queue.pop_front() {
            if !fetch.kind.is_blocking() {
                self.spawn(fetch);
                continue;
            }
            let step = match self.storage.select(&fetch.query).await {
                Ok(snapshot) => self.runtime.fetched(fetch.id, snapshot),
                Err(error) => {
                    eprintln!("storage read {} failed, parked: {error}", fetch.id.0);
                    self.runtime.failed(fetch.id)
                }
            };
            self.emit(step, &mut queue);
        }
    }

    /// Send a step's deltas and queue its reads.
    fn emit(&self, step: Step, queue: &mut VecDeque<Fetch>) {
        if !step.updates.is_empty() {
            let _ = self.updates.send(step.updates);
        }
        queue.extend(step.selects);
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
