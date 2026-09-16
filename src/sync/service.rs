//! The asynchronous driver: one task owns the runtime, takes commands
//! (subscribe, unsubscribe, a committed transaction, a progress mark)
//! from a channel, hands every delta to an event channel, and runs the storage
//! reads the runtime asks for. Every read, a registration's snapshot as
//! much as a join fetch or a window refill, runs as its own task while the
//! loop keeps routing; the loop never waits on storage, and each result is
//! brought up to the engine's position when it lands. The runtime is
//! touched only between awaits. Single-threaded by design: run it on a
//! [`tokio::task::LocalSet`].
//!
//! Besides the deltas, the events tell a consumer what it needs to batch
//! and to acknowledge without touching the runtime: which subscription a
//! registration became, when a subscription's first rows have all
//! arrived, when a read landed, and where the stream is after each
//! progress mark. Everything a consumer learns arrives on that one
//! stream, so a subscription is always named before anything about it
//! is: no consumer ever meets a [`SubId`] it has not been told about.

use std::collections::HashMap;
use std::rc::Rc;

use tokio::sync::mpsc;
use tokio::task::spawn_local;

use super::runtime::{Runtime, Step};
use super::storage::{Storage, StorageError};
use crate::ivm::{ClientUpdate, Engine, Fetch, FetchId};
use crate::log::log_warn;
use crate::model::{ClientId, Lsn, SingleTableReadQuery, Snapshot, SubId, WriteQuery};

/// What a client of the service can ask.
///
/// - `Register`: subscribe for `client`; the subscription's id comes back
///   as [`Event::Registered`] carrying `token` unchanged, before any
///   event about that subscription, and its first rows arrive as updates
///   like every other change.
/// - `Unregister`: unsubscribe one subscription.
/// - `UnregisterClient`: a client went away; every subscription of it goes.
/// - `Commit`: every write of one committed transaction, with the
///   location of its commit record. A transaction arrives as one command
///   so nothing can be interleaved inside it: a read that lands while it
///   is being routed is seen only once the whole commit has been, and a
///   consumer never meets half a transaction.
/// - `Progress`: the feed has delivered everything up to `lsn`.
/// - `Count`: how many rows match `query`, no further than `cap`,
///   answered as [`Event::Counted`] with the same `token`. What a planner
///   asks before registering a join, to learn which side to read whole.
pub enum Command<Q> {
    Register {
        client: ClientId,
        query: Q,
        token: u64,
    },
    Unregister(SubId),
    UnregisterClient(ClientId),
    Commit {
        writes: Vec<WriteQuery>,
        at: Lsn,
    },
    Progress(Lsn),
    Count {
        query: SingleTableReadQuery,
        cap: u64,
        token: u64,
    },
}

/// What the service tells its consumer.
///
/// - `Registered`: the subscription a [`Command::Register`] became, with
///   that command's `token`. It precedes every other event about the
///   subscription.
/// - `Updates`: one step's deltas, folded per client and row.
/// - `Hydrated`: subscriptions whose first rows have all arrived (every
///   part of their tree is live), each named once.
/// - `Landed`: a storage read landed; its deltas came just before. A
///   consumer that batches per landing flushes here.
/// - `Moved`: the stream passed a progress mark: the engine's position
///   and the storage floor. A consumer that batches per committed
///   transaction flushes here, and serves once the position covers the
///   floor.
#[derive(Debug)]
pub enum Event {
    Registered {
        token: u64,
        sub: SubId,
    },
    Updates(Vec<ClientUpdate>),
    Hydrated(Vec<SubId>),
    Landed,
    Moved {
        position: Lsn,
        floor: Lsn,
    },
    Counted {
        token: u64,
        count: Result<u64, String>,
    },
}

/// The loop's handles: the command inlet and the event outlet.
pub struct Service<E: Engine, S: Storage> {
    runtime: Runtime<E>,
    storage: Rc<S>,
    commands: mpsc::Receiver<Command<E::Query>>,
    events: mpsc::UnboundedSender<Event>,
    /// Subscriptions registered but not yet reported hydrated, and the
    /// client each belongs to, so a client going away takes its own with
    /// it instead of leaving them to be probed forever.
    awaiting: HashMap<SubId, ClientId>,
    results: mpsc::UnboundedReceiver<(FetchId, Result<Snapshot, StorageError>)>,
    report: mpsc::UnboundedSender<(FetchId, Result<Snapshot, StorageError>)>,
}

impl<E, S> Service<E, S>
where
    E: Engine + 'static,
    E::Query: 'static,
    S: Storage + 'static,
{
    /// A service over `engine` and `storage`, delivering events to
    /// `events`; returns it with the command sender to drive it by.
    pub fn new(
        engine: E,
        storage: Rc<S>,
        events: mpsc::UnboundedSender<Event>,
    ) -> (Self, mpsc::Sender<Command<E::Query>>) {
        let (commands_tx, commands) = mpsc::channel(1024);
        let (report, results) = mpsc::unbounded_channel();
        let service = Service {
            runtime: Runtime::new(engine),
            storage,
            commands,
            events,
            awaiting: HashMap::new(),
            results,
            report,
        };
        (service, commands_tx)
    }

    /// Run until every command sender is dropped; returns the runtime for
    /// inspection.
    pub async fn run(mut self) -> Runtime<E> {
        loop {
            tokio::select! {
                command = self.commands.recv() => match command {
                    Some(command) => self.handle(command),
                    None => break,
                },
                result = self.results.recv() => match result {
                    Some((id, Ok(snapshot))) => {
                        let step = self.runtime.fetched(id, snapshot);
                        self.dispatch(step, true);
                    }
                    Some((id, Err(error))) => {
                        log_warn!("storage read {} failed, parked: {error}", id.0);
                        let step = self.runtime.failed(id);
                        self.dispatch(step, false);
                    }
                    None => break,
                },
            }
        }
        self.runtime
    }

    /// Apply one command to the runtime and dispatch what it produced.
    fn handle(&mut self, command: Command<E::Query>) {
        match command {
            Command::Register {
                client,
                query,
                token,
            } => {
                let (sub, step) = self.runtime.register(client, query);
                let _ = self.events.send(Event::Registered { token, sub });
                self.awaiting.insert(sub, client);
                self.dispatch(step, false);
            }
            Command::Unregister(sub) => {
                self.runtime.unregister(sub);
                self.awaiting.remove(&sub);
            }
            Command::UnregisterClient(client) => {
                self.runtime.unregister_client(client);
                self.awaiting.retain(|_, owner| *owner != client);
            }
            Command::Commit { writes, at } => {
                let mut commit = Step::default();
                for write in writes {
                    self.storage.absorb(&write, at);
                    let step = self.runtime.write(&write, at);
                    commit.updates.extend(step.updates);
                    commit.selects.extend(step.selects);
                }
                self.moved();
                self.dispatch(commit, false);
            }
            Command::Progress(lsn) => {
                let step = self.runtime.progress(lsn);
                self.moved();
                self.dispatch(step, false);
                let _ = self.events.send(Event::Moved {
                    position: self.runtime.position(),
                    floor: self.runtime.floor(),
                });
            }
            Command::Count { query, cap, token } => {
                let storage = self.storage.clone();
                let events = self.events.clone();
                spawn_local(async move {
                    let count = storage
                        .count(&query, cap)
                        .await
                        .map_err(|error| error.to_string());
                    let _ = events.send(Event::Counted { token, count });
                });
            }
        }
    }

    /// The stream moved: tell the storage, and learn its floor.
    fn moved(&mut self) {
        self.storage.advance(self.runtime.position());
        self.runtime.set_floor(self.storage.floor());
    }

    /// Deliver a step's deltas, start each of its reads as a task, name
    /// the subscriptions that became hydrated, and mark a landing.
    fn dispatch(&mut self, step: Step, landed: bool) {
        if !step.updates.is_empty() {
            let _ = self.events.send(Event::Updates(step.updates));
        }
        for fetch in step.selects {
            self.spawn(fetch);
        }
        self.settle_awaiting();
        if landed {
            let _ = self.events.send(Event::Landed);
        }
    }

    /// Name the awaited subscriptions whose first rows have all arrived,
    /// and forget them.
    fn settle_awaiting(&mut self) {
        let hydrated: Vec<SubId> = self
            .awaiting
            .keys()
            .copied()
            .filter(|sub| self.runtime.engine().hydrated(*sub))
            .collect();
        if hydrated.is_empty() {
            return;
        }
        for sub in &hydrated {
            self.awaiting.remove(sub);
        }
        let _ = self.events.send(Event::Hydrated(hydrated));
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
