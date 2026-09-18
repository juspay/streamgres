//! The asynchronous driver: one task owns the runtime, takes commands
//! (subscribe, unsubscribe) from a channel and committed transactions
//! from the feed's, hands every delta to an event channel, and runs the
//! storage reads the runtime asks for. Every read, a registration's
//! snapshot as much as a join fetch or a window refill, runs as its own
//! task while the loop keeps routing; the loop never waits on storage, and
//! each result is brought up to the engine's position when it lands. The
//! runtime is touched only between awaits. Single-threaded by design: run
//! it on a [`tokio::task::LocalSet`].
//!
//! Besides the deltas, the events tell a consumer what it needs to batch
//! and to acknowledge without touching the runtime: which subscription a
//! registration became, when a subscription's first rows have all
//! arrived, when a read landed, and where the stream is after each
//! transaction, with the writes on the tables the consumer watches for
//! itself. Everything a consumer learns arrives on that one stream, so a
//! subscription is always named before anything about it is: no consumer
//! ever meets a [`SubId`] it has not been told about. The consumer may be
//! several ([`Service::with_sinks`]), each owning the clients whose ids
//! are its own modulo the count: a client's events go to its sink alone,
//! and what concerns everyone (a landing, a commit) goes to every sink.

use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::spawn_local;

use super::runtime::{Runtime, Step};
use super::storage::{Storage, StorageError};
use crate::ivm::{ClientUpdate, Engine, Fetch, FetchId};
use crate::log::log_warn;
use crate::model::{ClientId, IdMap, Lsn, Snapshot, SubId, WriteQuery};
use crate::stats::Stats;

/// What a client of the service can ask.
///
/// - `Register`: subscribe for `client`; the subscription's id comes back
///   as [`Event::Registered`] carrying `token` unchanged, before any
///   event about that subscription, and its first rows arrive as updates
///   like every other change.
/// - `Unregister`: unsubscribe one subscription.
/// - `UnregisterClient`: a client went away; every subscription of it goes.
/// - `Transaction`: one committed transaction, which the feed normally
///   delivers on its own channel ([`Service::with_feed`]) and a caller
///   without a feed hands in here.
pub enum Command<Q> {
    Register {
        client: ClientId,
        query: Q,
        token: u64,
    },
    Unregister(SubId),
    UnregisterClient(ClientId),
    Transaction(Transaction),
}

/// One committed transaction as the feed delivers it: every write of it,
/// the location of its commit record, the position the feed has delivered
/// everything up to once it is applied, the writes among them on the
/// tables the consumer watches for itself, and the instant the feed
/// decoded it (where the server's own clock on it starts). A transaction
/// is one step of the engine, so nothing can be interleaved inside it: a
/// read that lands while it is being routed is seen only once the whole
/// transaction has been, and a consumer never meets half of one.
#[derive(Debug)]
pub struct Transaction {
    pub writes: Vec<WriteQuery>,
    pub at: Lsn,
    pub progress: Lsn,
    pub watched: Vec<WriteQuery>,
    pub received: Instant,
}

impl Transaction {
    /// A transaction of `writes` committed at `at`, the feed's progress
    /// mark being that same location, with nothing watched, received now.
    pub fn new(writes: Vec<WriteQuery>, at: Lsn) -> Self {
        Transaction {
            writes,
            at,
            progress: at,
            watched: Vec::new(),
            received: Instant::now(),
        }
    }
}

/// What the service tells its consumer. Every step's deltas (folded per
/// client and row) travel inside the event that ends the step, so a
/// consumer never sees half a step, and a transaction's rows and its
/// watched writes are one event.
///
/// - `Registered`: the subscription a [`Command::Register`] became, with
///   that command's `token`, the rows it was served at once (a twin's) and
///   `reads`, how many storage reads the registration issued (zero when
///   the frames already held answered it). It precedes every other event
///   about the subscription.
/// - `Landed`: a storage read landed, with the deltas it produced.
/// - `Refused`: a storage read the subscription depended on was refused
///   (it returned more rows than a read may), so the subscription is gone;
///   `reason` is what the client can be told.
/// - `Committed`: a transaction was applied: its deltas for this
///   consumer, the engine's position, the storage floor, the transaction's
///   writes on the watched tables, and two instants for the consumer's
///   clock, when the feed decoded the transaction and when the engine
///   finished with it. A consumer serves once the position covers the
///   floor.
/// - `Hydrated`: subscriptions whose first rows have all arrived (every
///   part of their tree is live), each named once.
#[derive(Debug)]
pub enum Event {
    Registered {
        token: u64,
        sub: SubId,
        updates: Vec<ClientUpdate>,
        reads: usize,
    },
    Landed {
        updates: Vec<ClientUpdate>,
    },
    Refused {
        sub: SubId,
        reason: String,
    },
    Committed {
        updates: Vec<ClientUpdate>,
        position: Lsn,
        floor: Lsn,
        watched: Vec<WriteQuery>,
        received: Instant,
        routed: Instant,
    },
    Hydrated(Vec<SubId>),
}

/// How a step ended: what its deltas travel inside of.
enum Outcome {
    Registered {
        client: ClientId,
        token: u64,
        sub: SubId,
        reads: usize,
    },
    Landed,
    Committed {
        position: Lsn,
        floor: Lsn,
        watched: Vec<WriteQuery>,
        received: Instant,
        routed: Instant,
    },
    Nothing,
}

/// The loop's handles: the command inlet, the feed's transaction inlet
/// when there is one, and the event outlet.
pub struct Service<E: Engine, S: Storage> {
    runtime: Runtime<E>,
    storage: Rc<S>,
    commands: mpsc::Receiver<Command<E::Query>>,
    feed: Option<mpsc::Receiver<Transaction>>,
    sinks: Vec<mpsc::UnboundedSender<Event>>,
    stats: Option<Arc<Stats>>,
    /// Subscriptions registered but not yet reported hydrated, and the
    /// client each belongs to, so a client going away takes its own with
    /// it instead of leaving them to be probed forever.
    awaiting: IdMap<SubId, ClientId>,
    /// When each read in flight was handed to the driver, for `read_io`.
    issued: IdMap<FetchId, Instant>,
    /// When every awaited subscription was last checked for completion;
    /// the checks otherwise touch only what a step could have completed.
    swept: Instant,
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
            feed: None,
            sinks: vec![events],
            stats: None,
            awaiting: IdMap::default(),
            issued: IdMap::default(),
            swept: Instant::now(),
            results,
            report,
        };
        (service, commands_tx)
    }

    /// Take the committed transactions from `feed` as well.
    pub fn with_feed(mut self, feed: mpsc::Receiver<Transaction>) -> Self {
        self.feed = Some(feed);
        self
    }

    /// Deliver the events to `sinks` instead, the sink of a client being
    /// the one at its id modulo their count (at least one sink).
    pub fn with_sinks(mut self, sinks: Vec<mpsc::UnboundedSender<Event>>) -> Self {
        if !sinks.is_empty() {
            self.sinks = sinks;
        }
        self
    }

    /// Record how long each transaction waits for the engine and takes in
    /// it, and publish the engine's counters, into `stats`.
    pub fn with_stats(mut self, stats: Arc<Stats>) -> Self {
        self.stats = Some(stats);
        self
    }

    /// Run until every command sender is dropped (or the feed ends);
    /// returns the runtime for inspection.
    pub async fn run(mut self) -> Runtime<E> {
        let mut commands = Vec::with_capacity(64);
        let mut transactions = Vec::with_capacity(64);
        loop {
            tokio::select! {
                taken = self.commands.recv_many(&mut commands, 64) => {
                    if taken == 0 {
                        break;
                    }
                    for command in commands.drain(..) {
                        self.handle(command);
                    }
                }
                taken = recv_transactions(&mut self.feed, &mut transactions) => {
                    if taken == 0 {
                        break;
                    }
                    for transaction in transactions.drain(..) {
                        self.commit(transaction);
                    }
                }
                result = self.results.recv() => match result {
                    Some((id, Ok(snapshot))) => {
                        let started = Instant::now();
                        if let Some(stats) = &self.stats
                            && let Some(issued) = self.issued.remove(&id)
                        {
                            stats.read_io.record(started.duration_since(issued));
                        }
                        let waiting = self.runtime.waiting_on(id);
                        let step = self.runtime.fetched(id, snapshot);
                        if let Some(stats) = &self.stats {
                            stats.land_step.record(started.elapsed());
                        }
                        self.dispatch(step, Outcome::Landed);
                        self.settle(&waiting);
                    }
                    Some((id, Err(error))) => {
                        self.issued.remove(&id);
                        if let Some(reason) = error.refusal() {
                            log_warn!("storage read {} refused: {reason}", id.0);
                            for (sub, client) in self.runtime.refused(id) {
                                self.awaiting.remove(&sub);
                                self.send_to(
                                    client,
                                    Event::Refused {
                                        sub,
                                        reason: reason.to_owned(),
                                    },
                                );
                            }
                        } else {
                            log_warn!("storage read {} failed, parked: {error}", id.0);
                            let step = self.runtime.failed(id);
                            self.dispatch(step, Outcome::Nothing);
                        }
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
                let started = Instant::now();
                let (sub, step) = self.runtime.register(client, query);
                let reads = step.selects.len();
                if let Some(stats) = &self.stats {
                    stats.register_step.record(started.elapsed());
                }
                self.awaiting.insert(sub, client);
                self.dispatch(
                    step,
                    Outcome::Registered {
                        client,
                        token,
                        sub,
                        reads,
                    },
                );
                self.settle(&[sub]);
            }
            Command::Unregister(sub) => {
                self.runtime.unregister(sub);
                self.awaiting.remove(&sub);
            }
            Command::UnregisterClient(client) => {
                self.runtime.unregister_client(client);
                self.awaiting.retain(|_, owner| *owner != client);
            }
            Command::Transaction(transaction) => self.commit(transaction),
        }
    }

    /// Apply one committed transaction as one step: every write routed,
    /// the progress mark taken, the storage told, the deltas delivered
    /// together, and the consumer told where the engine now is.
    fn commit(&mut self, transaction: Transaction) {
        let Transaction {
            writes,
            at,
            progress,
            watched,
            received,
        } = transaction;
        let started = Instant::now();
        let mut step = Step::default();
        for write in &writes {
            self.storage.absorb(write, at);
            let routed = self.runtime.write(write, at);
            step.updates.extend(routed.updates);
            step.selects.extend(routed.selects);
        }
        let moved = self.runtime.progress(progress);
        step.updates.extend(moved.updates);
        step.selects.extend(moved.selects);
        self.moved();
        let position = self.runtime.position();
        let floor = self.runtime.floor();
        let routed = Instant::now();
        if let Some(stats) = &self.stats {
            stats
                .feed_to_engine
                .record(started.duration_since(received));
            stats.engine_step.record(routed.duration_since(started));
            stats.transactions.fetch_add(1, Ordering::Relaxed);
            stats
                .writes
                .fetch_add(writes.len() as u64, Ordering::Relaxed);
            stats.publish_engine(self.runtime.engine_stats(), self.runtime.stats());
        }
        self.dispatch(
            step,
            Outcome::Committed {
                position,
                floor,
                watched,
                received,
                routed,
            },
        );
    }

    /// The sink that owns `client`.
    fn sink_of(&self, client: ClientId) -> &mpsc::UnboundedSender<Event> {
        &self.sinks[client.0 as usize % self.sinks.len()]
    }

    /// Send `event` to the sink that owns `client`.
    fn send_to(&self, client: ClientId, event: Event) {
        let _ = self.sink_of(client).send(event);
    }

    /// One step's deltas split by sink, each client's to its own.
    fn partition(&self, updates: Vec<ClientUpdate>) -> Vec<Vec<ClientUpdate>> {
        if self.sinks.len() == 1 {
            return vec![updates];
        }
        let mut batches: Vec<Vec<ClientUpdate>> =
            (0..self.sinks.len()).map(|_| Vec::new()).collect();
        for update in updates {
            batches[update.client.0 as usize % self.sinks.len()].push(update);
        }
        batches
    }

    /// The stream moved: tell the storage, and learn its floor.
    fn moved(&mut self) {
        self.storage.advance(self.runtime.position());
        self.runtime.set_floor(self.storage.floor());
    }

    /// Deliver a step's deltas inside the event its `outcome` calls for
    /// (a registration's to its client's sink; a landing's to the sinks
    /// with any; a commit's to every sink, empty or not), start each of
    /// its reads as a task, and name the subscriptions that became
    /// hydrated.
    fn dispatch(&mut self, step: Step, outcome: Outcome) {
        let Step { updates, selects } = step;
        match outcome {
            Outcome::Registered {
                client,
                token,
                sub,
                reads,
            } => {
                self.send_to(
                    client,
                    Event::Registered {
                        token,
                        sub,
                        updates,
                        reads,
                    },
                );
            }
            Outcome::Landed => {
                for (sink, batch) in self.sinks.iter().zip(self.partition(updates)) {
                    if !batch.is_empty() {
                        let _ = sink.send(Event::Landed { updates: batch });
                    }
                }
            }
            Outcome::Committed {
                position,
                floor,
                watched,
                received,
                routed,
            } => {
                for (sink, batch) in self.sinks.iter().zip(self.partition(updates)) {
                    let _ = sink.send(Event::Committed {
                        updates: batch,
                        position,
                        floor,
                        watched: watched.clone(),
                        received,
                        routed,
                    });
                }
            }
            Outcome::Nothing => {
                debug_assert!(updates.is_empty(), "a parked read has no deltas");
            }
        }
        for fetch in selects {
            self.spawn(fetch);
        }
        if self.swept.elapsed() >= Duration::from_secs(1) {
            self.swept = Instant::now();
            let all: Vec<SubId> = self.awaiting.keys().copied().collect();
            self.settle(&all);
        }
    }

    /// Name those of `candidates` that are awaited and whose first rows
    /// have all arrived, each to the sink of its client, and forget them.
    /// A step names the subscriptions it could have completed (the one it
    /// registered, the ones waiting on the read it landed); every awaited
    /// subscription is checked at most once a second besides, so the check
    /// never grows with the number still hydrating.
    fn settle(&mut self, candidates: &[SubId]) {
        let hydrated: Vec<(SubId, ClientId)> = candidates
            .iter()
            .filter_map(|sub| {
                let client = *self.awaiting.get(sub)?;
                self.runtime
                    .engine()
                    .hydrated(*sub)
                    .then_some((*sub, client))
            })
            .collect();
        if hydrated.is_empty() {
            return;
        }
        let mut per_sink: Vec<Vec<SubId>> = (0..self.sinks.len()).map(|_| Vec::new()).collect();
        for (sub, client) in hydrated {
            self.awaiting.remove(&sub);
            per_sink[client.0 as usize % self.sinks.len()].push(sub);
        }
        for (sink, subs) in self.sinks.iter().zip(per_sink) {
            if !subs.is_empty() {
                let _ = sink.send(Event::Hydrated(subs));
            }
        }
    }

    /// Run one read as its own task, reporting the result into the loop.
    fn spawn(&mut self, fetch: Fetch) {
        self.issued.insert(fetch.id, Instant::now());
        let storage = self.storage.clone();
        let report = self.report.clone();
        spawn_local(async move {
            let result = storage.select_shared(fetch.query.clone()).await;
            let _ = report.send((fetch.id, result));
        });
    }
}

/// Take up to a batch of transactions from the feed, if there is one; a
/// service without a feed waits here forever, and a feed that ended
/// yields nothing.
async fn recv_transactions(
    feed: &mut Option<mpsc::Receiver<Transaction>>,
    buffer: &mut Vec<Transaction>,
) -> usize {
    match feed {
        Some(feed) => feed.recv_many(buffer, 64).await,
        None => std::future::pending().await,
    }
}
