//! Every client group's view, and the threads that keep them. A group
//! thread owns no engine, no storage and no database connection: it sends
//! the engine side subscribe and unsubscribe commands and reads back that
//! side's events (deltas, hydrated subscriptions, landings, commits),
//! turning both into the pokes a Zero client expects. Connections hand it
//! requests over a channel (connect, disconnect, planned queries, pulls)
//! and receive finished frames.
//!
//! # Threads, batches, bytes
//!
//! There are `XYNE_SYNC_GROUP_THREADS` of these threads, each owning the
//! groups whose id hashes to it; the engine side knows which thread
//! registered each subscription and sends it that subscription's share of
//! every delta, so a thread hears only of its own groups. A thread
//! wakes, takes everything the connections and the engine side have sent
//! since, applies all of it, and only then pokes each group that has
//! something to hear, once: idle, that is one poke per transaction; under
//! load one poke carries every transaction that arrived meanwhile, which
//! is what keeps the frame count per connection bounded as the write rate
//! climbs. Within a flush every row image is serialized to its `rowsPatch`
//! bytes once and shared by every group's frame that carries it; a group's
//! frame is a prefix, those fragments, and a suffix, so a poke costs the
//! bookkeeping and a copy, never a JSON tree per group.
//!
//! # A client group's view
//!
//! Zero's client keeps one row store per client group, shared by its
//! queries, and learns of changes as pokes: a versioned batch of row puts
//! and dels, query-state patches and mutation ids. This module keeps, per
//! group, the queries each of its clients desires (by hash), the
//! subscription behind each query, and for every row shipped the
//! subscription parts holding it, so a row is `del`ed only when its last
//! holder lets go. A poke goes out per
//! committed transaction (so a mutation's rows and its `lastMutationID`
//! travel together), per landed read, and per query change, advancing the
//! group's version; the version is the cookie the client hands back when
//! it reconnects.
//!
//! # A client that comes back, or comes late
//!
//! What a client is owed is everything after its cookie, and a group
//! answers that in one of four ways ([`Groups::connect`]):
//!
//! - **The cookie is the group's version.** Nothing is owed. This is the
//!   usual reconnect, because a group with no connection open stands
//!   still: its deltas are still accounted against its row ledger as they
//!   arrive, but they wait as one coalesced list of row operations and no
//!   poke is built, so the version stays where the last client left it and
//!   the first poke after it returns carries all it missed.
//! - **No cookie, and the group is under way** (a tab opened before the
//!   store it shares held a cookie). The newcomer is sent the group's
//!   whole state from the ledger, as one poke from nothing to the group's
//!   version; the other connections of the group are not disturbed.
//! - **An older cookie the group's log still reaches** (a tab that was
//!   away while another kept the group moving). The group keeps its most
//!   recent pokes, as the frames it already built, up to
//!   `XYNE_SYNC_GROUP_LOG_BYTES`: every poke sent is kept and the oldest
//!   dropped until the rest fit, whether or not anyone was really
//!   listening (a socket that died unnoticed is sent to, and logged for,
//!   until the server learns of it). The returning connection is sent the
//!   ones after its cookie, as they were — but for the mutation ids: each
//!   replayed poke closes with a part carrying the group's mutation ids
//!   as they are *now*, so the first poke the client processes already
//!   confirms every mutation the application has processed since the
//!   poke was built. A logged poke tells the ids of its time; a client
//!   rebases its still-pending mutations against every poke that does not
//!   confirm them, and one of those, replayed stale, can make a mutator
//!   fail on a row the poke has since removed and drop the connection —
//!   on every reconnect, since the replay is the same. The client merges
//!   a poke's parts in order and refuses an id that goes backwards, so
//!   every replayed poke gets the same current ids, in its last part.
//! - **Anything else** (a cookie older than the log, or from a server
//!   that is gone): the client is told to start over, and syncs afresh.
//!
//! The application's mutation ids arrive as writes to its clients table,
//! which the engine side carries inside the transaction's own
//! [`Event::Committed`], so a mutation's rows and its id go out in the
//! same poke and no later transaction's id rides out early. At every
//! connect they are also read from the table itself, merged into what the
//! group knows, and sent with the connection's first poke: the state
//! poke, the replayed pokes, or a poke of their own at the next flush.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use serde_json::{Value as Json, json};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::spawn_local;

use super::ast::Translated;
use super::config::Config;
use super::protocol;
use super::wire;
use crate::ivm::{Delta, QueryPart};
use crate::log::{Level, log_debug, log_event, log_info, log_warn};
use crate::model::{
    Catalog, DataFrameKey, DataFrameOperation, DataFrameRow, Lsn, MultiTableReadQuery, RowData,
    SubId, TableName, Value, WriteQuery,
};
use crate::stats::Stats;
use crate::sync::{CatalogHandle, Command, Event};

/// What goes out on one connection: a text frame, one poke (its frames
/// written together, one flush; `since` is when the feed decoded the
/// oldest transaction it carries, `sent` when it was handed over, both
/// for the server's own clock), a WebSocket ping (the liveness probe a
/// browser answers on its own), or the close.
#[derive(Debug, Clone)]
pub enum Outbound {
    Text(Arc<str>),
    Poke {
        frames: Arc<[Bytes]>,
        since: Option<Instant>,
        sent: Instant,
    },
    Ping,
    Close,
}

/// What a client group holds, by table and row key: looked up by
/// reference, so a delta the group already has the image of costs two
/// hash probes and no copy of its key.
type Ledger = HashMap<TableName, HashMap<DataFrameKey, Held>>;

/// One delta waiting for a group's next poke, shared with every other
/// group it concerns, and the subscriptions and parts of this group it
/// applies to.
struct Pending {
    delta: Rc<Delta>,
    holders: Vec<(SubId, QueryPart)>,
}

/// A connection's handle: the client it speaks for and where its frames
/// go.
#[derive(Debug, Clone)]
pub struct Socket {
    pub client: String,
    pub sink: mpsc::UnboundedSender<Outbound>,
}

/// One change to a client's desired queries, ready for this thread: a
/// query already translated and planned (`Some(Ok)`), one that could not
/// be and whose client has been told (`Some(Err)`), or a hash the client
/// re-desires without an AST (`None`).
#[derive(Debug)]
pub enum DesiredOp {
    Put {
        hash: String,
        name: String,
        ttl: Option<f64>,
        planned: Option<Result<Box<Translated>, String>>,
    },
    Del {
        hash: String,
    },
    Clear,
}

/// What a connection asks of this thread.
#[derive(Debug)]
pub enum Request {
    Connect {
        group: String,
        wsid: String,
        socket: Socket,
        base_cookie: Option<String>,
        lmids: Vec<(String, i64)>,
        reply: oneshot::Sender<ConnectReply>,
    },
    Disconnect {
        group: String,
        wsid: String,
    },
    Desired {
        group: String,
        client: String,
        ops: Vec<DesiredOp>,
    },
    DeleteClients {
        group: String,
        clients: Vec<String>,
    },
    Pull {
        group: String,
        reply: oneshot::Sender<(String, HashMap<String, i64>)>,
    },
    Expire {
        group: String,
        generation: u64,
    },
    ExpireQuery {
        group: String,
        hash: String,
        generation: u64,
    },
}

/// How long a query outlives the last client desiring it when the client
/// gave no `ttl`.
const DEFAULT_QUERY_TTL: Duration = Duration::from_secs(300);
/// The longest a query outlives the last client desiring it.
const MAX_QUERY_TTL: Duration = Duration::from_secs(600);

/// A client's `ttl` (milliseconds) as the lifetime honored for it.
fn lifetime_of(ttl: Option<f64>) -> Duration {
    match ttl {
        Some(ms) if ms.is_finite() && ms >= 0.0 => {
            Duration::from_millis(ms.min(MAX_QUERY_TTL.as_millis() as f64) as u64)
        }
        _ => DEFAULT_QUERY_TTL,
    }
}

/// This thread's answer to a connection.
#[derive(Debug)]
pub enum ConnectReply {
    Accepted,
    /// The client's cookie is not one this server holds; the client must
    /// start over.
    Reset {
        reason: String,
    },
}

/// One row operation bound for a group.
enum RowOp {
    Put(TableName, DataFrameKey, DataFrameRow),
    Del(TableName, DataFrameKey),
}

/// One desired query of a group.
///
/// - `sub`: the subscription, once the engine side has answered.
/// - `awaiting`: the generation of the registration in flight, zero when
///   none is.
/// - `hidden`: the parts of its tree whose rows are not shipped.
/// - `got`: whether the client has been told the query is complete.
/// - `ttl`: how long it outlives the last client desiring it.
/// - `inactive`: the generation of the release in flight, zero while a
///   client still desires it.
struct QueryState {
    sub: Option<SubId>,
    awaiting: u64,
    hidden: HashSet<QueryPart>,
    got: bool,
    ttl: Duration,
    inactive: u64,
    /// When the query was last sent to register, for the hydration clock.
    since: Instant,
    /// Whether the registration had to read storage.
    cold: bool,
    /// The query's name, for the error a refusal carries.
    name: String,
    /// The reason and moment of a refusal, while a re-ask is answered
    /// with the same error instead of a registration.
    refused: Option<(String, Instant)>,
}

/// How long a refused query stays refused: a client asking again within
/// this hears the reason again; after it the query is registered anew
/// (the data or the budget may have changed).
const REFUSAL_COOLDOWN: Duration = Duration::from_secs(60);

/// What a client group holds of one row: who shows it (subscription and
/// part) and the image last sent, so a second holder of the same image
/// sends nothing and a released holder deletes nothing another still
/// shows.
struct Held {
    holders: HashSet<(SubId, QueryPart)>,
    image: Arc<RowData>,
}

/// Account one delta against the group's row ledger and say what the
/// client hears: a put when the row is new to it or its image changed, a
/// del when its last holder let go, nothing when another holder already
/// showed the same image or still shows the row. This is the one place a
/// row is decided to travel or not, whatever brought it: a query's first
/// rows, a write, or a read a write set off.
fn account(
    rows: &mut Ledger,
    table: &TableName,
    key: &DataFrameKey,
    image: Option<&DataFrameRow>,
    holders: HashSet<(SubId, QueryPart)>,
) -> Option<RowOp> {
    match image {
        Some(image) => {
            if holders.is_empty() {
                return None;
            }
            if !rows.contains_key(table) {
                rows.insert(table.clone(), HashMap::new());
            }
            let of_table = rows.get_mut(table)?;
            match of_table.get_mut(key) {
                Some(held) => {
                    held.holders.extend(holders);
                    if Arc::ptr_eq(&held.image, &image.data) || *held.image == *image.data {
                        return None;
                    }
                    held.image = image.data.clone();
                }
                None => {
                    of_table.insert(
                        key.clone(),
                        Held {
                            holders,
                            image: image.data.clone(),
                        },
                    );
                }
            }
            Some(RowOp::Put(table.clone(), key.clone(), image.clone()))
        }
        None => {
            let of_table = rows.get_mut(table)?;
            let held = of_table.get_mut(key)?;
            for holder in &holders {
                held.holders.remove(holder);
            }
            if !held.holders.is_empty() {
                return None;
            }
            of_table.remove(key);
            if of_table.is_empty() {
                rows.remove(table);
            }
            Some(RowOp::Del(table.clone(), key.clone()))
        }
    }
}

/// Every row the group shows only through `sub`, taken out of the ledger:
/// the deletes a released subscription owes the client.
fn release_rows(rows: &mut Ledger, sub: SubId) -> Vec<(TableName, DataFrameKey)> {
    let mut emptied = Vec::new();
    for (table, of_table) in rows.iter_mut() {
        of_table.retain(|key, held| {
            held.holders.retain(|(holder, _)| *holder != sub);
            if held.holders.is_empty() {
                emptied.push((table.clone(), key.clone()));
            }
            !held.holders.is_empty()
        });
    }
    rows.retain(|_, of_table| !of_table.is_empty());
    emptied
}

/// One client group's view.
///
/// `generation` stamps the group's last connect or disconnect from the
/// one monotonic counter, so an expiry timer scheduled for an earlier
/// incarnation of the same group id can never match this one.
struct Group {
    version: u64,
    generation: u64,
    sockets: HashMap<String, Socket>,
    desired: HashMap<String, HashSet<String>>,
    queries: HashMap<String, QueryState>,
    subs: HashSet<SubId>,
    /// The hidden parts of each subscription, for the per-row check.
    hidden: HashMap<SubId, HashSet<QueryPart>>,
    rows: Ledger,
    lmids: HashMap<String, i64>,
    queued_desired: HashMap<String, Vec<Json>>,
    queued_got: Vec<Json>,
    queued_lmids: HashMap<String, i64>,
    queued_rows: Vec<RowOp>,
    /// How long `queued_rows` was when it was last coalesced: a group
    /// with no connection coalesces its waiting operations again only once
    /// they have doubled, so holding them costs each delta once.
    queued_floor: usize,
    log: PokeLog,
}

/// A group's most recent pokes as the frames that were sent, oldest
/// first and without a gap up to the group's version, within a budget of
/// bytes: every poke sent is pushed, and the oldest are dropped until the
/// rest fit. It is what a connection that returns behind the group is
/// sent: a tab that reconnects before it has taken up what another tab
/// stored, or a client whose socket died unnoticed while the server went
/// on sending to it. A poke larger than the whole budget (a first
/// hydration) empties the log, since nothing older can be continued from
/// without it.
#[derive(Default)]
struct PokeLog {
    pokes: VecDeque<Logged>,
    bytes: usize,
}

/// One poke as the log keeps it: the version it took the group to, its
/// id (what a part added at replay names), its frames and the bytes it
/// is counted as.
struct Logged {
    version: u64,
    poke_id: String,
    frames: Arc<[Bytes]>,
    size: usize,
}

/// What a logged frame is counted as on top of its own bytes, for the
/// allocations that hold it, so a log of many small pokes stays near its
/// budget in memory too.
const FRAME_OVERHEAD: usize = 64;

impl PokeLog {
    /// Remember the poke `poke_id` that took the group to `version`.
    fn push(&mut self, version: u64, poke_id: &str, frames: &Arc<[Bytes]>, budget: usize) {
        let size: usize = frames
            .iter()
            .map(|frame| frame.len() + FRAME_OVERHEAD)
            .sum();
        if size > budget {
            self.pokes.clear();
            self.bytes = 0;
            return;
        }
        self.pokes.push_back(Logged {
            version,
            poke_id: poke_id.to_owned(),
            frames: frames.clone(),
            size,
        });
        self.bytes += size;
        while self.bytes > budget {
            match self.pokes.pop_front() {
                Some(dropped) => self.bytes -= dropped.size,
                None => break,
            }
        }
    }

    /// The pokes after `version`, in order, as their ids and frames, when
    /// the log reaches back that far.
    fn after(&self, version: u64) -> Option<Vec<(String, Arc<[Bytes]>)>> {
        let oldest = self.pokes.front()?.version;
        (oldest <= version + 1).then(|| {
            self.pokes
                .iter()
                .filter(|logged| logged.version > version)
                .map(|logged| (logged.poke_id.clone(), logged.frames.clone()))
                .collect()
        })
    }
}

impl Group {
    /// An empty group.
    fn new() -> Self {
        Group {
            version: 0,
            generation: 0,
            sockets: HashMap::new(),
            desired: HashMap::new(),
            queries: HashMap::new(),
            subs: HashSet::new(),
            hidden: HashMap::new(),
            rows: HashMap::new(),
            lmids: HashMap::new(),
            queued_desired: HashMap::new(),
            queued_got: Vec::new(),
            queued_lmids: HashMap::new(),
            queued_rows: Vec::new(),
            queued_floor: 0,
            log: PokeLog::default(),
        }
    }

    /// Send one poke's frames to every connection of the group.
    fn broadcast(&self, frames: &Arc<[Bytes]>, since: Option<Instant>) {
        let sent = Instant::now();
        for socket in self.sockets.values() {
            let _ = socket.sink.send(Outbound::Poke {
                frames: frames.clone(),
                since,
                sent,
            });
        }
    }
}

/// One group thread's state.
///
/// - `shard`: this thread's index among the group threads; every
///   registration names it as the sink its subscription belongs to.
/// - `by_sub`: the group and query hash of every subscription this thread
///   owns: the engine side names subscriptions, and this is where a
///   subscription finds its client group.
/// - `pending`: the deltas waiting for each group's next poke.
/// - `stats`: where the flushes and the pokes are timed; `oldest` is when
///   the feed decoded the oldest transaction applied since the last flush,
///   the start of the clock every poke of the next flush carries.
pub struct Groups {
    config: Arc<Config>,
    catalog: Arc<CatalogHandle>,
    shard: usize,
    stats: Arc<Stats>,
    oldest: Option<Instant>,
    /// Commands to the engine side, in the order they were made.
    commands: mpsc::UnboundedSender<Command<MultiTableReadQuery>>,
    requests: mpsc::Sender<Request>,
    ready: bool,
    /// Flipped once, when this thread starts serving; the connections and
    /// the health check watch it.
    readiness: watch::Sender<bool>,
    backlog: Vec<Request>,
    groups: HashMap<String, Group>,
    by_sub: HashMap<SubId, (String, String)>,
    /// Registrations in flight, by the token they were sent with.
    awaiting: HashMap<u64, (String, String)>,
    pending: HashMap<String, Vec<Pending>>,
    lmid_changes: HashMap<String, HashMap<String, i64>>,
    /// Groups with something to hear at the next flush.
    dirty: HashSet<String>,
    clients_table: TableName,
    next_poke: u64,
    next_generation: u64,
}

/// Run group thread `shard` of `shards` until every request sender is
/// gone: take what the connections and the engine side sent, in batches,
/// apply it, and flush once per batch. Must run inside a `LocalSet`.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    config: Arc<Config>,
    catalog: Arc<CatalogHandle>,
    shard: usize,
    shards: usize,
    commands: mpsc::Sender<Command<MultiTableReadQuery>>,
    mut events: mpsc::UnboundedReceiver<Event>,
    mut requests_rx: mpsc::Receiver<Request>,
    requests: mpsc::Sender<Request>,
    stats: Arc<Stats>,
    readiness: watch::Sender<bool>,
) {
    let (outbox, outbox_rx) = mpsc::unbounded_channel();
    spawn_local(forward(outbox_rx, commands));
    let mut core = Groups {
        clients_table: TableName::from(config.clients_table().as_str()),
        config,
        catalog,
        shard,
        stats,
        oldest: None,
        commands: outbox,
        requests,
        ready: false,
        readiness,
        backlog: Vec::new(),
        groups: HashMap::new(),
        by_sub: HashMap::new(),
        awaiting: HashMap::new(),
        pending: HashMap::new(),
        lmid_changes: HashMap::new(),
        dirty: HashSet::new(),
        next_poke: 1,
        next_generation: 0,
    };
    log_info!(
        "client groups {}/{} up; waiting for the change feed to pass the first snapshot before serving queries",
        shard + 1,
        shards.max(1)
    );
    let mut taken_requests = Vec::with_capacity(256);
    let mut taken_events = Vec::with_capacity(1024);
    loop {
        tokio::select! {
            taken = requests_rx.recv_many(&mut taken_requests, 256) => {
                core.stats.set_groups_inbox(shard, requests_rx.len() as u64);
                if taken == 0 {
                    break;
                }
                for request in taken_requests.drain(..) {
                    core.handle(request);
                }
            }
            taken = events.recv_many(&mut taken_events, 1024) => {
                if taken == 0 {
                    break;
                }
                for event in taken_events.drain(..) {
                    core.event(event);
                }
            }
        }
        core.flush();
    }
}

/// Hand the engine side one command at a time, in order, so a full
/// command channel never blocks the group thread's loop.
async fn forward(
    mut outbox: mpsc::UnboundedReceiver<Command<MultiTableReadQuery>>,
    commands: mpsc::Sender<Command<MultiTableReadQuery>>,
) {
    while let Some(command) = outbox.recv().await {
        if commands.send(command).await.is_err() {
            return;
        }
    }
}

impl Groups {
    /// Apply one request.
    fn handle(&mut self, request: Request) {
        match request {
            Request::Connect {
                group,
                wsid,
                socket,
                base_cookie,
                lmids,
                reply,
            } => {
                let outcome = self.connect(&group, wsid, socket, base_cookie, lmids);
                let _ = reply.send(outcome);
            }
            Request::Disconnect { group, wsid } => self.disconnect(&group, &wsid),
            Request::Desired { .. } if !self.ready => self.backlog.push(request),
            Request::Desired { group, client, ops } => self.desired(&group, &client, ops),
            Request::DeleteClients { group, clients } => {
                for client in clients {
                    self.clear_client(&group, &client);
                }
            }
            Request::Pull { group, reply } => {
                let answer = self
                    .groups
                    .get(&group)
                    .map(|group| (protocol::cookie(group.version), group.lmids.clone()))
                    .unwrap_or_else(|| (protocol::cookie(0), HashMap::new()));
                let _ = reply.send(answer);
            }
            Request::Expire { group, generation } => self.expire(&group, generation),
            Request::ExpireQuery {
                group,
                hash,
                generation,
            } => self.expire_query(&group, &hash, generation),
        }
    }

    /// One event from the engine side.
    fn event(&mut self, event: Event) {
        match event {
            Event::Registered {
                token,
                sub,
                updates,
                reads,
            } => {
                self.registered(token, sub, reads);
                self.absorb(updates);
            }
            Event::Landed { updates } => self.absorb(updates),
            Event::Refused { sub, reason } => self.refused(sub, &reason),
            Event::Capped { sub } => self.capped(sub),
            Event::Heavy { sub, table, rows } => self.heavy(sub, table.as_str(), rows),
            Event::Hydrated(subs) => {
                for sub in subs {
                    let Some((group_id, hash)) = self.by_sub.get(&sub).cloned() else {
                        continue;
                    };
                    let Some(group) = self.groups.get_mut(&group_id) else {
                        continue;
                    };
                    if let Some(state) = group.queries.get_mut(&hash)
                        && !state.got
                    {
                        state.got = true;
                        let hydrate = if state.cold {
                            &self.stats.hydrate_cold
                        } else {
                            &self.stats.hydrate_warm
                        };
                        let elapsed = state.since.elapsed();
                        hydrate.record(elapsed);
                        let slow = elapsed >= self.config.slow_query;
                        log_event!(
                            if slow { Level::Warn } else { Level::Debug },
                            if slow { "slow query" } else { "query hydrated" },
                            group = group_id,
                            name = state.name,
                            hash = hash,
                            kind = if state.cold { "cold" } else { "warm" },
                            ms = format!("{:.1}", elapsed.as_secs_f64() * 1000.0)
                        );
                        group.queued_got.push(json!({"op": "put", "hash": hash}));
                        self.dirty.insert(group_id);
                    }
                }
            }
            Event::Committed {
                updates,
                position,
                floor,
                watched,
                received,
                routed,
            } => {
                self.absorb(updates);
                self.stats.engine_to_groups.record(routed.elapsed());
                self.oldest = Some(match self.oldest {
                    Some(oldest) => oldest.min(received),
                    None => received,
                });
                for write in &watched {
                    if write.table() == &self.clients_table {
                        self.note_lmid(write);
                    }
                }
                self.serve_when_covered(position, floor);
            }
        }
    }

    /// Keep one step's deltas for their groups' next pokes: each delta is
    /// resolved to the groups owning its subscriptions and shared between
    /// them, with the subscriptions and parts of each group it applies to.
    fn absorb(&mut self, updates: Vec<Delta>) {
        for delta in updates {
            let delta = Rc::new(delta);
            for audience in &delta.audiences {
                for sub in audience.subs.iter() {
                    let Some((group_id, _)) = self.by_sub.get(&sub) else {
                        continue;
                    };
                    if !self.pending.contains_key(group_id.as_str()) {
                        self.pending.insert(group_id.clone(), Vec::new());
                    }
                    let Some(queue) = self.pending.get_mut(group_id.as_str()) else {
                        continue;
                    };
                    match queue.last_mut() {
                        Some(last) if Rc::ptr_eq(&last.delta, &delta) => {
                            last.holders.push((sub, audience.part));
                        }
                        _ => queue.push(Pending {
                            delta: delta.clone(),
                            holders: vec![(sub, audience.part)],
                        }),
                    }
                    if !self.dirty.contains(group_id.as_str()) {
                        self.dirty.insert(group_id.clone());
                    }
                }
            }
        }
    }

    /// Start serving once the engine's position covers the storage's
    /// snapshots; the requests that arrived meanwhile run then.
    fn serve_when_covered(&mut self, position: Lsn, floor: Lsn) {
        if self.ready || floor.0 == 0 || position < floor {
            return;
        }
        self.ready = true;
        self.readiness.send_replace(true);
        log_info!("serving: engine at {position}, storage snapshot at {floor}");
        let backlog = std::mem::take(&mut self.backlog);
        for request in backlog {
            self.handle(request);
        }
    }

    /// Send one command to the engine side.
    fn command(&self, command: Command<MultiTableReadQuery>) {
        let _ = self.commands.send(command);
    }

    /// Mark a group as having something to hear at the next flush.
    fn mark(&mut self, group_id: &str) {
        self.dirty.insert(group_id.to_owned());
    }

    /// Attach a connection to its group and send it what it is owed since
    /// its cookie (the module's "A client that comes back, or comes
    /// late"): nothing when the cookie is the group's version, the group's
    /// whole state when it has none and the group is under way, the logged
    /// pokes after an older cookie. A client that cannot be caught up is
    /// told to start over; no other connection of the group is touched.
    fn connect(
        &mut self,
        group_id: &str,
        wsid: String,
        socket: Socket,
        base_cookie: Option<String>,
        lmids: Vec<(String, i64)>,
    ) -> ConnectReply {
        let offered = match base_cookie.as_deref().map(protocol::version_of) {
            None => 0,
            Some(Some(version)) => version,
            Some(None) => {
                self.stats.connects_reset.fetch_add(1, Ordering::Relaxed);
                return ConnectReply::Reset {
                    reason: "the cookie is not one this server wrote".to_owned(),
                };
            }
        };
        let owed = match self.groups.get(group_id) {
            None if base_cookie.is_some() => {
                Err("the server holds no state for this client group".to_owned())
            }
            None => Ok(Owed::Nothing),
            Some(group) if offered == group.version => Ok(Owed::Nothing),
            Some(_) if base_cookie.is_none() => Ok(Owed::Everything),
            Some(group) => match group.log.after(offered) {
                Some(pokes) if offered < group.version => Ok(Owed::Pokes(pokes)),
                _ => Err(format!(
                    "the server is at {}, the client at {}, and the pokes between are not kept",
                    protocol::cookie(group.version),
                    protocol::cookie(offered)
                )),
            },
        };
        let owed = match owed {
            Ok(owed) => owed,
            Err(reason) => {
                self.stats.connects_reset.fetch_add(1, Ordering::Relaxed);
                return ConnectReply::Reset { reason };
            }
        };
        if !self.groups.contains_key(group_id) {
            self.groups.insert(group_id.to_owned(), Group::new());
            self.stats.client_groups.fetch_add(1, Ordering::Relaxed);
            log_info!("client group {group_id} opened");
        }
        self.next_generation += 1;
        let generation = self.next_generation;
        let group = self.groups.get_mut(group_id).expect("just ensured");
        for (client, lmid) in lmids {
            let known = group.lmids.entry(client.clone()).or_insert(0);
            if lmid > *known {
                *known = lmid;
            }
        }
        for (client, lmid) in &group.lmids {
            group.queued_lmids.insert(client.clone(), *lmid);
        }
        if !group.queued_lmids.is_empty() {
            self.dirty.insert(group_id.to_owned());
        }
        group.generation = generation;
        log_info!(
            "connection {wsid} joined client group {group_id} as client {}",
            socket.client
        );
        let sent = Instant::now();
        match owed {
            Owed::Nothing => {
                self.stats.connects_current.fetch_add(1, Ordering::Relaxed);
            }
            Owed::Pokes(pokes) => {
                self.stats.connects_from_log.fetch_add(1, Ordering::Relaxed);
                log_event!(
                    Level::Info,
                    "connection caught up from the group's log",
                    wsid = wsid,
                    group = group_id,
                    from = protocol::cookie(offered),
                    to = protocol::cookie(group.version),
                    pokes = pokes.len()
                );
                for (poke_id, frames) in pokes {
                    let _ = socket.sink.send(Outbound::Poke {
                        frames: with_lmids(&poke_id, &frames, &group.lmids),
                        since: None,
                        sent,
                    });
                }
            }
            Owed::Everything => {
                self.stats
                    .connects_from_state
                    .fetch_add(1, Ordering::Relaxed);
                let got: Vec<Json> = group
                    .queries
                    .iter()
                    .filter(|(_, state)| state.got)
                    .map(|(hash, _)| json!({"op": "put", "hash": hash}))
                    .collect();
                let images: Vec<(&TableName, DataFrameRow)> = group
                    .rows
                    .iter()
                    .flat_map(|(table, of_table)| {
                        of_table.values().map(move |held| {
                            (
                                table,
                                DataFrameRow {
                                    data: held.image.clone(),
                                },
                            )
                        })
                    })
                    .collect();
                let patches: Vec<Patch<'_>> = images
                    .iter()
                    .map(|(table, row)| Patch::Put(table, row))
                    .collect();
                let poke_id = self.next_poke.to_string();
                self.next_poke += 1;
                let (frames, puts, _) = build_poke(
                    &self.catalog.load(),
                    &self.stats,
                    self.config.rows_per_part.max(1),
                    &poke_id,
                    None,
                    &protocol::cookie(group.version),
                    head_of(&HashMap::new(), &got, &group.lmids),
                    &patches,
                    &mut HashMap::new(),
                );
                log_event!(
                    Level::Info,
                    "connection caught up from the group's state",
                    wsid = wsid,
                    group = group_id,
                    to = protocol::cookie(group.version),
                    rows = puts,
                    queries = got.len()
                );
                self.stats.pokes.fetch_add(1, Ordering::Relaxed);
                let _ = socket.sink.send(Outbound::Poke {
                    frames,
                    since: None,
                    sent,
                });
            }
        }
        if group.sockets.insert(wsid, socket).is_none() {
            self.stats.clients.fetch_add(1, Ordering::Relaxed);
        }
        self.mark(group_id);
        ConnectReply::Accepted
    }

    /// A connection closed; when it was the group's last, the group's
    /// subscriptions stay for the configured grace period.
    fn disconnect(&mut self, group_id: &str, wsid: &str) {
        let Some(group) = self.groups.get_mut(group_id) else {
            return;
        };
        if group.sockets.remove(wsid).is_some() {
            self.stats.clients.fetch_sub(1, Ordering::Relaxed);
        }
        log_info!("connection {wsid} left client group {group_id}");
        if group.sockets.is_empty() {
            self.next_generation += 1;
            let generation = self.next_generation;
            let group = self.groups.get_mut(group_id).expect("just had it");
            group.generation = generation;
            let requests = self.requests.clone();
            let ttl = self.config.group_ttl;
            let group_id = group_id.to_owned();
            spawn_local(async move {
                tokio::time::sleep(ttl).await;
                let _ = requests
                    .send(Request::Expire {
                        group: group_id,
                        generation,
                    })
                    .await;
            });
        }
    }

    /// The grace period of a group with no connections ran out.
    fn expire(&mut self, group_id: &str, generation: u64) {
        let expired = self
            .groups
            .get(group_id)
            .is_some_and(|group| group.sockets.is_empty() && group.generation == generation);
        if expired {
            log_info!("client group {group_id} expired; its subscriptions are released");
            self.drop_group(group_id);
        }
    }

    /// Forget a group and every subscription it had.
    fn drop_group(&mut self, group_id: &str) {
        if let Some(group) = self.groups.remove(group_id) {
            self.stats.client_groups.fetch_sub(1, Ordering::Relaxed);
            self.stats
                .clients
                .fetch_sub(group.sockets.len() as u64, Ordering::Relaxed);
            if !group.subs.is_empty() {
                self.command(Command::UnregisterAll(group.subs.iter().copied().collect()));
            }
            for sub in &group.subs {
                self.by_sub.remove(sub);
            }
            self.pending.remove(group_id);
            self.lmid_changes.remove(group_id);
            self.dirty.remove(group_id);
        }
    }

    /// Apply a client's desired-query changes to its group.
    fn desired(&mut self, group_id: &str, client: &str, ops: Vec<DesiredOp>) {
        if !self.groups.contains_key(group_id) {
            log_warn!("desired queries for an unknown client group {group_id}");
            return;
        }
        for op in ops {
            match op {
                DesiredOp::Put {
                    hash,
                    name,
                    ttl,
                    planned,
                } => self.put(group_id, client, hash, &name, ttl, planned),
                DesiredOp::Del { hash } => self.del(group_id, client, &hash),
                DesiredOp::Clear => self.clear_client(group_id, client),
            }
        }
    }

    /// One client now desires `hash`; register it for the group when it
    /// is new and it arrived planned (a query that could not be planned or
    /// translated is known to the group with no subscription behind it;
    /// its client was told by the connection).
    fn put(
        &mut self,
        group_id: &str,
        client: &str,
        hash: String,
        name: &str,
        ttl: Option<f64>,
        planned: Option<Result<Box<Translated>, String>>,
    ) {
        let lifetime = lifetime_of(ttl);
        self.mark(group_id);
        let group = self.groups.get_mut(group_id).expect("checked");
        group
            .desired
            .entry(client.to_owned())
            .or_default()
            .insert(hash.clone());
        let mut echo = json!({"op": "put", "hash": hash});
        if let Some(ttl) = ttl {
            echo["ttl"] = if ttl.fract() == 0.0 && ttl.abs() < 9.0e15 {
                json!(ttl as i64)
            } else {
                json!(ttl)
            };
        }
        group
            .queued_desired
            .entry(client.to_owned())
            .or_default()
            .push(echo);
        if let Some(state) = group.queries.get_mut(&hash) {
            state.inactive = 0;
            state.ttl = lifetime;
            match &state.refused {
                Some((reason, when)) if when.elapsed() < REFUSAL_COOLDOWN => {
                    let error = protocol::transform_error(vec![protocol::errored_query(
                        &hash,
                        &state.name,
                        reason,
                    )]);
                    if let Some(socket) = group.sockets.get(client) {
                        let _ = socket.sink.send(Outbound::Text(Arc::from(error)));
                    }
                    return;
                }
                Some(_) => {
                    group.queries.remove(&hash);
                }
                None => return,
            }
        }
        let Some(Ok(translated)) = planned else {
            group.queries.insert(
                hash,
                QueryState {
                    sub: None,
                    awaiting: 0,
                    hidden: HashSet::new(),
                    got: false,
                    ttl: lifetime,
                    inactive: 0,
                    since: Instant::now(),
                    cold: false,
                    name: name.to_owned(),
                    refused: None,
                },
            );
            return;
        };
        self.next_generation += 1;
        let token = self.next_generation;
        group.queries.insert(
            hash.clone(),
            QueryState {
                sub: None,
                awaiting: token,
                hidden: translated.hidden,
                got: false,
                ttl: lifetime,
                inactive: 0,
                since: Instant::now(),
                cold: false,
                name: name.to_owned(),
                refused: None,
            },
        );
        self.awaiting
            .insert(token, (group_id.to_owned(), hash.clone()));
        self.command(Command::Register {
            sink: self.shard,
            query: translated.query,
            token,
        });
        log_debug!("group {group_id}: query {name} ({hash}) registering");
    }

    /// The engine side registered a query (`reads` storage reads issued):
    /// adopt the subscription, unless the query was released while the
    /// registration was in flight, in which case it is let go at once.
    fn registered(&mut self, token: u64, sub: SubId, reads: usize) {
        let Some((group_id, hash)) = self.awaiting.remove(&token) else {
            self.command(Command::Unregister(sub));
            return;
        };
        let adopted = self
            .groups
            .get_mut(&group_id)
            .and_then(|group| {
                let state = group.queries.get_mut(&hash)?;
                (state.awaiting == token).then(|| {
                    state.awaiting = 0;
                    state.cold = reads > 0;
                    state.sub = Some(sub);
                    group.subs.insert(sub);
                    group.hidden.insert(sub, state.hidden.clone());
                })
            })
            .is_some();
        if adopted {
            log_debug!("group {group_id}: query {hash} registered as {}", sub.0);
            self.by_sub.insert(sub, (group_id, hash));
        } else {
            self.command(Command::Unregister(sub));
        }
    }

    /// A page of the query stopped reaching past the rows its join gate
    /// rejects: the query is served, its page short, and it is reported by
    /// name as one to rewrite.
    fn capped(&mut self, sub: SubId) {
        let Some((group_id, hash)) = self.by_sub.get(&sub).cloned() else {
            return;
        };
        let name = self
            .groups
            .get(&group_id)
            .and_then(|group| group.queries.get(&hash))
            .map(|state| state.name.clone())
            .unwrap_or_default();
        self.stats.note_short_page(&name);
        log_event!(
            Level::Warn,
            "page capped",
            name = name,
            hash = hash,
            group = group_id,
            reason = crate::stats::SHORT_PAGE
        );
    }

    /// A read the query waited on took at least half the row limit: the
    /// query is served, and it is reported by name, with the table and the
    /// share of the limit, as one to narrow before it is refused.
    fn heavy(&mut self, sub: SubId, table: &str, rows: u64) {
        let Some((group_id, hash)) = self.by_sub.get(&sub).cloned() else {
            return;
        };
        let name = self
            .groups
            .get(&group_id)
            .and_then(|group| group.queries.get(&hash))
            .map(|state| state.name.clone())
            .unwrap_or_default();
        let percent = self.stats.note_heavy_read(&name, table, rows);
        log_event!(
            if percent >= 80 {
                Level::Warn
            } else {
                Level::Info
            },
            "heavy read",
            name = name,
            hash = hash,
            group = group_id,
            table = table,
            rows = rows,
            limit = self.stats.read_row_limit.load(Ordering::Relaxed),
            percent = percent
        );
    }

    /// The engine side refused a read a query depended on: the query's
    /// subscription is gone and its rows with it, the query stays desired
    /// but errored, and every socket of the group hears the reason.
    fn refused(&mut self, sub: SubId, reason: &str) {
        let Some((group_id, hash)) = self.by_sub.remove(&sub) else {
            return;
        };
        self.mark(&group_id);
        let Some(group) = self.groups.get_mut(&group_id) else {
            return;
        };
        group.subs.remove(&sub);
        group.hidden.remove(&sub);
        for (table, key) in release_rows(&mut group.rows, sub) {
            group.queued_rows.push(RowOp::Del(table, key));
        }
        let Some(state) = group.queries.get_mut(&hash) else {
            return;
        };
        if state.got {
            group.queued_got.push(json!({"op": "del", "hash": hash}));
        }
        state.sub = None;
        state.got = false;
        state.awaiting = 0;
        state.refused = Some((reason.to_owned(), Instant::now()));
        let error =
            protocol::transform_error(vec![protocol::errored_query(&hash, &state.name, reason)]);
        let text: Arc<str> = Arc::from(error);
        for socket in group.sockets.values() {
            let _ = socket.sink.send(Outbound::Text(text.clone()));
        }
        let kind = self.stats.note_refusal(&state.name, reason);
        log_event!(
            Level::Warn,
            "query refused",
            name = state.name,
            hash = hash,
            kind = kind,
            at = "read",
            group = group_id,
            reason = reason
        );
    }

    /// One client no longer desires `hash`; unregister it when nobody in
    /// the group does.
    fn del(&mut self, group_id: &str, client: &str, hash: &str) {
        self.mark(group_id);
        let group = self.groups.get_mut(group_id).expect("checked");
        if let Some(desired) = group.desired.get_mut(client) {
            desired.remove(hash);
        }
        group
            .queued_desired
            .entry(client.to_owned())
            .or_default()
            .push(json!({"op": "del", "hash": hash}));
        let wanted = group.desired.values().any(|desired| desired.contains(hash));
        if !wanted {
            self.deactivate(group_id, hash);
        }
    }

    /// Nobody desires `hash` any more: keep it registered for its
    /// lifetime, so a client coming back to it within that time finds its
    /// rows in place, then release it.
    fn deactivate(&mut self, group_id: &str, hash: &str) {
        self.next_generation += 1;
        let generation = self.next_generation;
        let Some(state) = self
            .groups
            .get_mut(group_id)
            .and_then(|group| group.queries.get_mut(hash))
        else {
            return;
        };
        state.inactive = generation;
        let ttl = state.ttl;
        let requests = self.requests.clone();
        let group = group_id.to_owned();
        let hash = hash.to_owned();
        spawn_local(async move {
            tokio::time::sleep(ttl).await;
            let _ = requests
                .send(Request::ExpireQuery {
                    group,
                    hash,
                    generation,
                })
                .await;
        });
    }

    /// A query's lifetime after its last client ran out; released unless a
    /// client came back to it meanwhile.
    fn expire_query(&mut self, group_id: &str, hash: &str, generation: u64) {
        let expired = self
            .groups
            .get(group_id)
            .and_then(|group| group.queries.get(hash))
            .is_some_and(|state| state.inactive == generation);
        if expired {
            self.unsubscribe(group_id, hash);
        }
    }

    /// A client is gone (or cleared its queries): drop what only it
    /// desired.
    fn clear_client(&mut self, group_id: &str, client: &str) {
        let Some(group) = self.groups.get_mut(group_id) else {
            return;
        };
        let hashes: Vec<String> = group
            .desired
            .remove(client)
            .map(|desired| desired.into_iter().collect())
            .unwrap_or_default();
        self.mark(group_id);
        for hash in hashes {
            let group = self.groups.get_mut(group_id).expect("checked");
            group
                .queued_desired
                .entry(client.to_owned())
                .or_default()
                .push(json!({"op": "del", "hash": hash}));
            let wanted = group
                .desired
                .values()
                .any(|desired| desired.contains(&hash));
            if !wanted {
                self.deactivate(group_id, &hash);
            }
        }
    }

    /// Remove a query from its group: its subscription goes, the rows only
    /// it held are `del`ed, and its `got` is withdrawn.
    fn unsubscribe(&mut self, group_id: &str, hash: &str) {
        self.mark(group_id);
        let group = self.groups.get_mut(group_id).expect("checked");
        let Some(state) = group.queries.remove(hash) else {
            return;
        };
        if state.got {
            group.queued_got.push(json!({"op": "del", "hash": hash}));
        }
        let Some(sub) = state.sub else {
            return;
        };
        group.subs.remove(&sub);
        group.hidden.remove(&sub);
        for (table, key) in release_rows(&mut group.rows, sub) {
            group.queued_rows.push(RowOp::Del(table, key));
        }
        self.by_sub.remove(&sub);
        self.command(Command::Unregister(sub));
        log_debug!("group {group_id}: query {hash} unregistered ({})", sub.0);
    }

    /// A write to the mutation-id table: remember the change for the
    /// group's next poke.
    fn note_lmid(&mut self, write: &WriteQuery) {
        let Some(image) = write.new_row_image() else {
            return;
        };
        let text = |column: &str| match image.data.get(column) {
            Some(Value::String(text)) => Some(text.clone()),
            _ => None,
        };
        let (Some(group), Some(client)) = (text("clientGroupID"), text("clientID")) else {
            return;
        };
        let lmid = match image.data.get("lastMutationID") {
            Some(Value::Int(lmid)) => *lmid,
            Some(Value::Float(lmid)) => *lmid as i64,
            _ => return,
        };
        self.stats.lmid_seen(&client, lmid);
        self.dirty.insert(group.clone());
        self.lmid_changes
            .entry(group)
            .or_default()
            .insert(client, lmid);
    }

    /// Turn everything accumulated into one poke per group that has
    /// anything to hear, every row serialized once for all of them.
    fn flush(&mut self) {
        let groups = &self.groups;
        self.pending.retain(|group, _| groups.contains_key(group));
        if self.dirty.is_empty() {
            self.oldest = None;
            return;
        }
        let started = Instant::now();
        let since = self.oldest.take();
        let mut touched: Vec<String> = self.dirty.drain().collect();
        touched.sort();
        let mut fragments: HashMap<usize, Bytes> = HashMap::new();
        for group_id in touched {
            self.poke(&group_id, &mut fragments, since);
        }
        self.stats.groups_flush.record(started.elapsed());
    }

    /// Account what has arrived for one group and, when a connection is
    /// there to hear it, assemble and send its poke; `fragments` are the
    /// row entries already serialized in this flush, by image, and `since`
    /// when the oldest transaction of the flush was decoded. A group with
    /// no connection keeps its ledger current and its row operations
    /// waiting, coalesced, and stands still: no frames, no new version,
    /// so the client that returns with the group's cookie is owed exactly
    /// what waits here.
    fn poke(
        &mut self,
        group_id: &str,
        fragments: &mut HashMap<usize, Bytes>,
        since: Option<Instant>,
    ) {
        let lmid_changes = self.lmid_changes.remove(group_id).unwrap_or_default();
        let Some(group) = self.groups.get_mut(group_id) else {
            return;
        };
        let updates = self.pending.remove(group_id).unwrap_or_default();
        let mut rows: Vec<RowOp> = std::mem::take(&mut group.queued_rows);
        for Pending { delta, holders } in updates {
            let (key, image) = match &delta.op {
                DataFrameOperation::Add(key, row) => (key, Some(row)),
                DataFrameOperation::Delete(key, _) => (key, None),
            };
            let holders: HashSet<(SubId, QueryPart)> = holders
                .into_iter()
                .filter(|(sub, _)| group.subs.contains(sub))
                .filter(|(sub, part)| {
                    !group
                        .hidden
                        .get(sub)
                        .is_some_and(|hidden| hidden.contains(part))
                })
                .collect();
            if let Some(op) = account(&mut group.rows, &delta.table, key, image, holders) {
                rows.push(op);
            }
        }
        for (client, lmid) in lmid_changes {
            let known = group.lmids.entry(client.clone()).or_insert(0);
            if lmid >= *known {
                *known = lmid;
                group.queued_lmids.insert(client, lmid);
            }
        }
        if group.sockets.is_empty() {
            if rows.len() > group.queued_floor * 2 + 64 {
                rows = coalesce(rows);
                group.queued_floor = rows.len();
            }
            group.queued_rows = rows;
            return;
        }
        group.queued_floor = 0;
        if rows.is_empty()
            && group.queued_got.is_empty()
            && group.queued_desired.is_empty()
            && group.queued_lmids.is_empty()
        {
            return;
        }
        let got = std::mem::take(&mut group.queued_got);
        let desired = std::mem::take(&mut group.queued_desired);
        let lmids = std::mem::take(&mut group.queued_lmids);
        let rows = coalesce(rows);
        let patches: Vec<Patch<'_>> = rows
            .iter()
            .map(|op| match op {
                RowOp::Put(table, _, row) => Patch::Put(table, row),
                RowOp::Del(table, key) => Patch::Del(table, key),
            })
            .collect();
        let poke_id = self.next_poke.to_string();
        self.next_poke += 1;
        let base = (group.version > 0).then(|| protocol::cookie(group.version));
        group.version += 1;
        let cookie = protocol::cookie(group.version);
        let (frames, puts, dels) = build_poke(
            &self.catalog.load(),
            &self.stats,
            self.config.rows_per_part.max(1),
            &poke_id,
            base.as_deref(),
            &cookie,
            head_of(&desired, &got, &lmids),
            &patches,
            fragments,
        );
        log_debug!(
            "group {group_id}: poke {poke_id} {} -> {cookie}: {puts} puts, {dels} dels, {} got, {} lmids",
            base.as_deref().unwrap_or("null"),
            got.len(),
            lmids.len()
        );
        self.stats.pokes.fetch_add(1, Ordering::Relaxed);
        group.log.push(
            group.version,
            &poke_id,
            &frames,
            self.config.group_log_bytes,
        );
        group.broadcast(&frames, since);
    }
}

/// What a connecting client is owed since its cookie.
enum Owed {
    Nothing,
    /// The group's whole state, as one poke from nothing.
    Everything,
    /// The logged pokes after its cookie, as their ids and frames, to be
    /// sent as they were with the group's current mutation ids added.
    Pokes(Vec<(String, Arc<[Bytes]>)>),
}

/// One row operation of a poke, borrowed from wherever it is kept: a
/// flush's operations, or the ledger's images.
enum Patch<'a> {
    Put(&'a TableName, &'a DataFrameRow),
    Del(&'a TableName, &'a DataFrameKey),
}

/// The query-state and mutation-id fields of a poke's first part, as JSON
/// fields ready to follow the poke id.
fn head_of(
    desired: &HashMap<String, Vec<Json>>,
    got: &[Json],
    lmids: &HashMap<String, i64>,
) -> Vec<u8> {
    let mut head: Vec<u8> = Vec::new();
    if !desired.is_empty() {
        head.extend_from_slice(b",\"desiredQueriesPatches\":");
        let _ = serde_json::to_writer(&mut head, desired);
    }
    if !got.is_empty() {
        head.extend_from_slice(b",\"gotQueriesPatch\":");
        let _ = serde_json::to_writer(&mut head, got);
    }
    if !lmids.is_empty() {
        head.extend_from_slice(b",\"lastMutationIDChanges\":");
        let _ = serde_json::to_writer(&mut head, lmids);
    }
    head
}

/// A logged poke `poke_id` as it is replayed: its frames as they were,
/// with one more part before the end carrying `lmids`, the group's
/// mutation ids as they are now. The client merges a poke's parts in
/// order, so the last part's ids stand; they are never below the ones the
/// poke told at its time, which is what the client requires. With no ids
/// to tell, the frames are sent as they are.
fn with_lmids(poke_id: &str, frames: &Arc<[Bytes]>, lmids: &HashMap<String, i64>) -> Arc<[Bytes]> {
    let Some((end, parts)) = frames.split_last().filter(|_| !lmids.is_empty()) else {
        return frames.clone();
    };
    let mut replayed: Vec<Bytes> = Vec::with_capacity(frames.len() + 1);
    replayed.extend(parts.iter().cloned());
    replayed.push(part_frame(
        poke_id,
        Some(head_of(&HashMap::new(), &[], lmids)),
        &[],
    ));
    replayed.push(end.clone());
    replayed.into()
}

/// One poke as its frames, from `base` to `cookie`: the start, the parts
/// (`head` in the first, `per_part` row operations each, every put taken
/// from `fragments` when the image was already serialized in this flush),
/// the end. Also how many puts and dels it carries.
#[allow(clippy::too_many_arguments)]
fn build_poke(
    catalog: &Catalog,
    stats: &Stats,
    per_part: usize,
    poke_id: &str,
    base: Option<&str>,
    cookie: &str,
    head: Vec<u8>,
    rows: &[Patch<'_>],
    fragments: &mut HashMap<usize, Bytes>,
) -> (Arc<[Bytes]>, usize, usize) {
    let mut frames: Vec<Bytes> = Vec::with_capacity(rows.len().div_ceil(per_part) + 3);
    frames.push(Bytes::from(protocol::poke_start(poke_id, base)));
    let mut head = Some(head);
    if rows.is_empty() {
        frames.push(part_frame(poke_id, head.take(), &[]));
    }
    let (mut puts, mut dels) = (0usize, 0usize);
    for chunk in rows.chunks(per_part) {
        let mut entries: Vec<Bytes> = Vec::with_capacity(chunk.len());
        for op in chunk {
            match op {
                Patch::Put(table, row) => {
                    let Some(declared) = catalog.table(table.as_str()) else {
                        continue;
                    };
                    puts += 1;
                    let fragment = match fragments.entry(Arc::as_ptr(&row.data) as usize) {
                        std::collections::hash_map::Entry::Occupied(entry) => {
                            stats.rows_shared.fetch_add(1, Ordering::Relaxed);
                            entry.into_mut()
                        }
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            stats.rows_serialized.fetch_add(1, Ordering::Relaxed);
                            let mut bytes = Vec::with_capacity(256);
                            wire::write_put(&mut bytes, declared, row);
                            entry.insert(Bytes::from(bytes))
                        }
                    };
                    entries.push(fragment.clone());
                }
                Patch::Del(table, key) => {
                    dels += 1;
                    let mut bytes = Vec::with_capacity(64);
                    wire::write_del(&mut bytes, table.as_str(), key);
                    entries.push(Bytes::from(bytes));
                }
            }
        }
        frames.push(part_frame(poke_id, head.take(), &entries));
    }
    frames.push(Bytes::from(protocol::poke_end(poke_id, cookie)));
    (frames.into(), puts, dels)
}

/// One `pokePart` frame: the poke id, the query-state and mutation-id
/// fields of the poke's first part (`head`, already as JSON fields), and
/// the row entries, already serialized, as its `rowsPatch`.
fn part_frame(poke_id: &str, head: Option<Vec<u8>>, entries: &[Bytes]) -> Bytes {
    let size = 64
        + head.as_ref().map_or(0, Vec::len)
        + entries.iter().map(Bytes::len).sum::<usize>()
        + entries.len();
    let mut frame: Vec<u8> = Vec::with_capacity(size);
    frame.extend_from_slice(b"[\"pokePart\",{\"pokeID\":");
    let _ = serde_json::to_writer(&mut frame, poke_id);
    if let Some(head) = head {
        frame.extend_from_slice(&head);
    }
    frame.extend_from_slice(b",\"rowsPatch\":[");
    for (index, entry) in entries.iter().enumerate() {
        if index > 0 {
            frame.push(b',');
        }
        let _ = frame.write_all(entry);
    }
    frame.extend_from_slice(b"]}]");
    Bytes::from(frame)
}

/// Keep one operation per row, the last one, in first-seen order.
fn coalesce(rows: Vec<RowOp>) -> Vec<RowOp> {
    let mut index: HashMap<(TableName, DataFrameKey), usize> = HashMap::new();
    let mut out: Vec<Option<RowOp>> = Vec::with_capacity(rows.len());
    for op in rows {
        let slot = match &op {
            RowOp::Put(table, key, _) | RowOp::Del(table, key) => (table.clone(), key.clone()),
        };
        match index.get(&slot) {
            Some(&position) => out[position] = Some(op),
            None => {
                index.insert(slot, out.len());
                out.push(Some(op));
            }
        }
    }
    out.into_iter().flatten().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ColumnName;

    /// A holder of a row: subscription `sub`, main part.
    fn holder(sub: u64) -> HashSet<(SubId, QueryPart)> {
        HashSet::from([(SubId(sub), QueryPart::main())])
    }

    /// A row keyed by `id` with one `name` column.
    fn row(id: i64, name: &str) -> (DataFrameKey, DataFrameRow) {
        (
            DataFrameKey::from(HashMap::from([(ColumnName::from("id"), Value::Int(id))])),
            DataFrameRow::from(HashMap::from([
                (ColumnName::from("id"), Value::Int(id)),
                (ColumnName::from("name"), Value::from(name)),
            ])),
        )
    }

    /// A row two queries show stays on the client while either still
    /// holds it, and the second holder's arrival sends nothing when the
    /// image is the one already sent.
    #[test]
    fn a_row_stays_on_the_client_while_any_query_still_holds_it() {
        let table = TableName::from("t");
        let mut rows = HashMap::new();
        let (key, image) = row(1, "a");
        let first = account(&mut rows, &table, &key, Some(&image), holder(1));
        assert!(
            matches!(first, Some(RowOp::Put(..))),
            "the first holder puts the row"
        );
        let second = account(&mut rows, &table, &key, Some(&image), holder(2));
        assert!(
            second.is_none(),
            "the same image from a second holder sends nothing"
        );
        let released = account(&mut rows, &table, &key, None, holder(1));
        assert!(
            released.is_none(),
            "the first holder letting go deletes nothing the second shows"
        );
        let gone = account(&mut rows, &table, &key, None, holder(2));
        assert!(
            matches!(gone, Some(RowOp::Del(..))),
            "the last holder letting go deletes"
        );
        assert!(rows.is_empty());
    }

    /// A changed image is sent whoever holds the row, and a released
    /// subscription owes exactly the rows only it showed.
    #[test]
    fn a_changed_image_is_sent_whoever_holds_the_row() {
        let table = TableName::from("t");
        let mut rows = HashMap::new();
        let (key, image) = row(1, "a");
        account(&mut rows, &table, &key, Some(&image), holder(1));
        let (_, changed) = row(1, "b");
        let again = account(&mut rows, &table, &key, Some(&changed), holder(2));
        assert!(
            matches!(again, Some(RowOp::Put(..))),
            "a changed image is sent"
        );
        let (key2, image2) = row(2, "only two");
        account(&mut rows, &table, &key2, Some(&image2), holder(2));
        let owed = release_rows(&mut rows, SubId(2));
        assert_eq!(
            owed,
            vec![(table.clone(), key2)],
            "only the row nobody else shows is deleted"
        );
        assert!(
            rows.get(&table)
                .is_some_and(|of_table| of_table.contains_key(&key)),
            "the shared row stays for the first holder"
        );
    }

    /// A group thread on its own: the engine side is this test, which
    /// reads the commands the thread sends and hands it events.
    struct Bench {
        core: Groups,
        commands: mpsc::UnboundedReceiver<Command<MultiTableReadQuery>>,
    }

    /// One fake connection: what its socket was sent.
    #[derive(Debug)]
    struct Tab {
        wsid: String,
        frames: mpsc::UnboundedReceiver<Outbound>,
    }

    /// One poke as a test reads it: the cookie it starts from and ends at,
    /// the rows put (by id, with the name sent) and deleted, the queries
    /// reported complete, and the mutation ids as the client would merge
    /// them (part by part, the last part standing).
    #[derive(Debug, PartialEq)]
    struct Seen {
        base: Option<String>,
        cookie: String,
        puts: Vec<(i64, String)>,
        dels: Vec<i64>,
        got: Vec<String>,
        lmids: Vec<(String, i64)>,
    }

    impl Bench {
        /// A thread over one table `notes(id, name)`, the log's budget as
        /// given.
        fn new(log_bytes: &str) -> Self {
            let vars: HashMap<&str, &str> = HashMap::from([
                ("XYNE_SYNC_PG_DSN", "postgresql://none/none"),
                ("XYNE_SYNC_QUERY_URL", "http://none/query"),
                ("XYNE_SYNC_MUTATE_URL", "http://none/push"),
                ("XYNE_SYNC_GROUP_LOG_BYTES", log_bytes),
            ]);
            let config = Config::from_lookup(|name| vars.get(name).map(|v| (*v).to_owned()))
                .expect("a configuration");
            let catalog = Catalog::new([crate::model::DbTable::new(
                "notes",
                ["id"],
                vec![
                    crate::model::DbColumn::new("id", crate::model::ValueType::Int),
                    crate::model::DbColumn::new("name", crate::model::ValueType::String),
                ],
            )]);
            let (outbox, commands) = mpsc::unbounded_channel();
            let (requests, _requests_rx) = mpsc::channel(16);
            let core = Groups {
                clients_table: TableName::from(config.clients_table().as_str()),
                config: Arc::new(config),
                catalog: Arc::new(CatalogHandle::new(catalog)),
                shard: 0,
                stats: Stats::shared(),
                oldest: None,
                commands: outbox,
                requests,
                ready: true,
                readiness: watch::channel(true).0,
                backlog: Vec::new(),
                groups: HashMap::new(),
                by_sub: HashMap::new(),
                awaiting: HashMap::new(),
                pending: HashMap::new(),
                lmid_changes: HashMap::new(),
                dirty: HashSet::new(),
                next_poke: 1,
                next_generation: 0,
            };
            Bench { core, commands }
        }

        /// Connect `wsid` to group `g` with `cookie`; the tab when accepted,
        /// the reason when told to start over.
        fn connect(&mut self, wsid: &str, cookie: Option<&str>) -> Result<Tab, String> {
            self.connect_with(wsid, cookie, Vec::new())
        }

        /// [`Bench::connect`] with `lmids` as what the clients table says
        /// at the moment of connecting.
        fn connect_with(
            &mut self,
            wsid: &str,
            cookie: Option<&str>,
            lmids: Vec<(String, i64)>,
        ) -> Result<Tab, String> {
            let (sink, frames) = mpsc::unbounded_channel();
            let socket = Socket {
                client: format!("client-{wsid}"),
                sink,
            };
            match self.core.connect(
                "g",
                wsid.to_owned(),
                socket,
                cookie.map(str::to_owned),
                lmids,
            ) {
                ConnectReply::Accepted => Ok(Tab {
                    wsid: wsid.to_owned(),
                    frames,
                }),
                ConnectReply::Reset { reason } => Err(reason),
            }
        }

        /// The application recorded mutation `lmid` of client `client` in
        /// group `g`: the write to the clients table, as the feed carries
        /// it.
        fn mutation_recorded(&mut self, client: &str, lmid: i64) {
            let table = self.core.clients_table.clone();
            let key = DataFrameKey::from(HashMap::from([
                (ColumnName::from("clientGroupID"), Value::from("g")),
                (ColumnName::from("clientID"), Value::from(client)),
            ]));
            let record = DataFrameRow::from(HashMap::from([
                (ColumnName::from("clientGroupID"), Value::from("g")),
                (ColumnName::from("clientID"), Value::from(client)),
                (ColumnName::from("lastMutationID"), Value::Int(lmid)),
            ]));
            self.core
                .note_lmid(&WriteQuery::INSERT(crate::model::InsertQuery {
                    table,
                    pkey_value: key,
                    record,
                }));
        }

        /// The tab desires the query `hash`; the engine side registers it
        /// as `sub`, delivers `rows` and reports it complete.
        fn hydrate(&mut self, tab: &Tab, hash: &str, sub: u64, rows: &[(i64, &str)]) {
            let query = MultiTableReadQuery::single(crate::model::SingleTableReadQuery::new(
                "notes",
                crate::model::Where::AND(Vec::new()),
                crate::model::OrderBy::new("id", crate::model::Order::ASC),
                u32::MAX,
            ));
            self.core.desired(
                "g",
                &format!("client-{}", tab.wsid),
                vec![DesiredOp::Put {
                    hash: hash.to_owned(),
                    name: hash.to_owned(),
                    ttl: None,
                    planned: Some(Ok(Box::new(Translated {
                        query,
                        hidden: HashSet::new(),
                    }))),
                }],
            );
            let token = loop {
                match self.commands.try_recv().expect("a registration is sent") {
                    Command::Register { token, .. } => break token,
                    _ => continue,
                }
            };
            self.core.event(Event::Registered {
                token,
                sub: SubId(sub),
                updates: rows.iter().map(|(id, name)| put(sub, *id, name)).collect(),
                reads: 1,
            });
            self.core.event(Event::Hydrated(vec![SubId(sub)]));
            self.core.flush();
        }

        /// Deltas arrive from the engine side and the thread flushes.
        fn deliver(&mut self, updates: Vec<Delta>) {
            self.core.event(Event::Landed { updates });
            self.core.flush();
        }

        /// The group's version, as its cookie.
        fn cookie(&self) -> String {
            protocol::cookie(self.core.groups["g"].version)
        }
    }

    impl Tab {
        /// Every poke the tab's socket was sent since the last look.
        fn pokes(&mut self) -> Vec<Seen> {
            let mut out = Vec::new();
            while let Ok(outbound) = self.frames.try_recv() {
                let Outbound::Poke { frames, .. } = outbound else {
                    continue;
                };
                let mut seen = Seen {
                    base: None,
                    cookie: String::new(),
                    puts: Vec::new(),
                    dels: Vec::new(),
                    got: Vec::new(),
                    lmids: Vec::new(),
                };
                let mut merged_lmids: HashMap<String, i64> = HashMap::new();
                for frame in frames.iter() {
                    let parsed: Json = serde_json::from_slice(frame).expect("a JSON frame");
                    let (tag, body) = (parsed[0].as_str().unwrap_or(""), &parsed[1]);
                    match tag {
                        "pokeStart" => {
                            seen.base = body["baseCookie"].as_str().map(str::to_owned);
                        }
                        "pokeEnd" => seen.cookie = body["cookie"].as_str().unwrap_or("").to_owned(),
                        _ => {
                            for (client, lmid) in body["lastMutationIDChanges"]
                                .as_object()
                                .into_iter()
                                .flatten()
                            {
                                merged_lmids.insert(client.clone(), lmid.as_i64().unwrap_or(-1));
                            }
                            for op in body["rowsPatch"].as_array().into_iter().flatten() {
                                if op["op"] == "put" {
                                    seen.puts.push((
                                        op["value"]["id"].as_i64().unwrap_or(-1),
                                        op["value"]["name"].as_str().unwrap_or("").to_owned(),
                                    ));
                                } else {
                                    seen.dels.push(op["id"]["id"].as_i64().unwrap_or(-1));
                                }
                            }
                            for op in body["gotQueriesPatch"].as_array().into_iter().flatten() {
                                seen.got.push(op["hash"].as_str().unwrap_or("").to_owned());
                            }
                        }
                    }
                }
                seen.puts.sort();
                seen.dels.sort_unstable();
                seen.lmids = merged_lmids.into_iter().collect();
                seen.lmids.sort();
                out.push(seen);
            }
            out
        }
    }

    /// Subscription `sub` shows the note `id` named `name`.
    fn put(sub: u64, id: i64, name: &str) -> Delta {
        let (key, image) = row(id, name);
        Delta {
            table: TableName::from("notes"),
            op: DataFrameOperation::Add(key, image),
            audiences: vec![crate::ivm::Audience {
                part: QueryPart::main(),
                subs: crate::ivm::Subs::One(SubId(sub)),
            }],
        }
    }

    /// Subscription `sub` no longer shows the note `id`.
    fn gone(sub: u64, id: i64) -> Delta {
        let (key, image) = row(id, "");
        Delta {
            table: TableName::from("notes"),
            op: DataFrameOperation::Delete(key, image),
            audiences: vec![crate::ivm::Audience {
                part: QueryPart::main(),
                subs: crate::ivm::Subs::One(SubId(sub)),
            }],
        }
    }

    /// Run `body` where the thread's timers can be spawned.
    fn on_local<T>(body: impl FnOnce() -> T) -> T {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        let local = tokio::task::LocalSet::new();
        local.block_on(&runtime, async { body() })
    }

    /// A group with no connection stands still: what changes while its
    /// client is away is accounted and waits, no poke is built and the
    /// version does not move, so the client that returns with its cookie
    /// is accepted and its next poke carries exactly the net of what it
    /// missed, once.
    #[test]
    fn a_client_that_returns_is_sent_what_it_missed() {
        on_local(|| {
            let mut bench = Bench::new("262144");
            let mut tab = bench.connect("a1", None).expect("accepted");
            bench.hydrate(&tab, "h1", 1, &[(1, "one"), (2, "two")]);
            let first = tab.pokes();
            let last = first.last().expect("a poke");
            assert_eq!(last.cookie, bench.cookie());
            assert_eq!(last.got, vec!["h1"]);
            let left_at = bench.cookie();
            bench.core.disconnect("g", "a1");

            bench.deliver(vec![put(1, 3, "three"), gone(1, 1)]);
            bench.deliver(vec![put(1, 3, "three, renamed")]);
            bench.deliver(vec![put(1, 4, "four"), gone(1, 4)]);
            assert_eq!(
                bench.cookie(),
                left_at,
                "the group did not move while nobody listened"
            );
            assert!(bench.core.groups["g"].log.pokes.len() <= first.len());

            let mut back = bench
                .connect("a2", Some(&left_at))
                .expect("the cookie still fits");
            bench.core.flush();
            let pokes = back.pokes();
            assert_eq!(pokes.len(), 1, "{pokes:?}");
            assert_eq!(pokes[0].base.as_deref(), Some(left_at.as_str()));
            assert_eq!(pokes[0].puts, vec![(3, "three, renamed".to_owned())]);
            assert_eq!(pokes[0].dels, vec![1, 4]);
            assert_eq!(pokes[0].cookie, bench.cookie());
            let stats = &bench.core.stats;
            assert_eq!(stats.connects_reset.load(Ordering::Relaxed), 0);
        });
    }

    /// A tab that joins a group under way without a cookie is sent the
    /// group's whole state as one poke from nothing, and the tab already
    /// there keeps its subscriptions and goes on hearing: nothing is
    /// unregistered, and the next change reaches both from the same
    /// cookie.
    #[test]
    fn a_tab_without_a_cookie_joins_a_group_under_way() {
        on_local(|| {
            let mut bench = Bench::new("262144");
            let mut first = bench.connect("a", None).expect("accepted");
            bench.hydrate(&first, "h1", 1, &[(1, "one"), (2, "two")]);
            bench.deliver(vec![put(1, 3, "three"), gone(1, 2)]);
            first.pokes();
            let at = bench.cookie();

            let mut second = bench.connect("b", None).expect("accepted, not reset");
            let caught_up = second.pokes();
            assert_eq!(caught_up.len(), 1, "{caught_up:?}");
            assert_eq!(caught_up[0].base, None);
            assert_eq!(caught_up[0].cookie, at);
            assert_eq!(
                caught_up[0].puts,
                vec![(1, "one".to_owned()), (3, "three".to_owned())]
            );
            assert_eq!(caught_up[0].got, vec!["h1"]);
            assert!(
                first.pokes().is_empty(),
                "the first tab hears nothing of it"
            );
            assert!(bench.core.groups["g"].subs.contains(&SubId(1)));
            while let Ok(command) = bench.commands.try_recv() {
                assert!(
                    !matches!(command, Command::Unregister(_) | Command::UnregisterAll(_)),
                    "the group's subscriptions were let go"
                );
            }

            bench.deliver(vec![put(1, 5, "five")]);
            let (to_first, to_second) = (first.pokes(), second.pokes());
            assert_eq!(to_first, to_second);
            let change = to_first.last().expect("a poke");
            assert_eq!(change.base.as_deref(), Some(at.as_str()));
            assert_eq!(change.puts, vec![(5, "five".to_owned())]);
        });
    }

    /// A tab that was away while another kept the group moving returns
    /// with an older cookie and is sent the pokes after it, as they were;
    /// with nothing logged, or a cookie the log no longer reaches, it is
    /// told to start over, as is a cookie this server never wrote or one
    /// for a group it does not hold.
    #[test]
    fn a_tab_behind_the_group_is_sent_the_logged_pokes() {
        on_local(|| {
            let mut bench = Bench::new("262144");
            let mut stays = bench.connect("a", None).expect("accepted");
            bench.hydrate(&stays, "h1", 1, &[(1, "one")]);
            let leaves = bench.connect("b", Some(&bench.cookie())).expect("accepted");
            let left_at = bench.cookie();
            bench.core.disconnect("g", &leaves.wsid);
            stays.pokes();

            bench.deliver(vec![put(1, 2, "two")]);
            bench.deliver(vec![gone(1, 1)]);
            let heard = stays.pokes();
            assert_eq!(heard.len(), 2);

            let mut back = bench
                .connect("b2", Some(&left_at))
                .expect("caught up from the log");
            assert_eq!(back.pokes(), heard, "the same pokes, in order");
            bench.deliver(vec![put(1, 3, "three")]);
            assert_eq!(back.pokes(), stays.pokes());
            assert_eq!(
                bench.core.stats.connects_from_log.load(Ordering::Relaxed),
                1
            );

            assert!(
                bench.connect("c", Some("zz")).is_err(),
                "not a cookie of ours"
            );
            let ahead = protocol::cookie(bench.core.groups["g"].version + 5);
            assert!(
                bench.connect("d", Some(&ahead)).is_err(),
                "ahead of the group"
            );

            let mut unlogged = Bench::new("0");
            let mut tab = unlogged.connect("a", None).expect("accepted");
            unlogged.hydrate(&tab, "h1", 1, &[(1, "one")]);
            let old = unlogged.cookie();
            unlogged.deliver(vec![put(1, 2, "two")]);
            tab.pokes();
            let reason = unlogged
                .connect("b", Some(&old))
                .expect_err("nothing is kept");
            assert!(reason.contains("not kept"), "{reason}");
            let mut empty = Bench::new("262144");
            assert!(empty.connect("x", Some("01")).is_err(), "no such group");
        });
    }

    /// A client that returns behind the log is told the mutation ids as
    /// they are now, with the first poke it is sent: every replayed poke
    /// closes with the ids read at connect merged into the group's (the
    /// client merges a poke's parts in order and refuses an id that goes
    /// back, so none may tell less), its rows and cookies as they were;
    /// the group then hears the ids once, in a poke of their own, and a
    /// tab joining without a cookie gets them in its state poke. Without
    /// this, a poke replayed from before the application processed a
    /// mutation made the client rebase it against rows the poke had
    /// removed, fail, and reconnect into the same replay.
    #[test]
    fn a_client_behind_the_log_is_told_the_mutation_ids_as_they_are_now() {
        on_local(|| {
            let mut bench = Bench::new("262144");
            let mut stays = bench.connect("a", None).expect("accepted");
            bench.hydrate(&stays, "h1", 1, &[(1, "one")]);
            let leaves = bench.connect("b", Some(&bench.cookie())).expect("accepted");
            let left_at = bench.cookie();
            bench.core.disconnect("g", &leaves.wsid);
            stays.pokes();

            bench.deliver(vec![put(1, 2, "two")]);
            bench.mutation_recorded("x", 3);
            bench.deliver(vec![gone(1, 1)]);
            bench.deliver(vec![put(1, 3, "three")]);
            let heard = stays.pokes();
            assert_eq!(heard.len(), 3);
            assert!(heard[0].lmids.is_empty());
            assert_eq!(heard[1].lmids, vec![("x".to_owned(), 3)]);
            assert!(heard[2].lmids.is_empty());

            let now = vec![("x".to_owned(), 5), ("y".to_owned(), 1)];
            let mut back = bench
                .connect_with("b2", Some(&left_at), now.clone())
                .expect("caught up from the log");
            let replayed = back.pokes();
            assert_eq!(replayed.len(), heard.len(), "{replayed:?}");
            for (poke, original) in replayed.iter().zip(&heard) {
                assert_eq!(
                    poke.lmids, now,
                    "the ids as they are now, in every replayed poke"
                );
                assert_eq!(
                    (&poke.base, &poke.cookie, &poke.puts, &poke.dels, &poke.got),
                    (
                        &original.base,
                        &original.cookie,
                        &original.puts,
                        &original.dels,
                        &original.got
                    ),
                    "the rest as it was"
                );
            }

            bench.core.flush();
            let told = back.pokes();
            assert_eq!(told.len(), 1, "the ids go out to the group once: {told:?}");
            assert_eq!(told[0].lmids, now);
            assert!(told[0].puts.is_empty() && told[0].dels.is_empty());
            assert_eq!(told[0].base.as_deref(), Some(heard[2].cookie.as_str()));
            assert_eq!(stays.pokes(), told, "the tab that stayed hears the same");

            let mut fresh = bench.connect("c", None).expect("accepted");
            let state = fresh.pokes();
            assert_eq!(state.len(), 1);
            assert_eq!(state[0].base, None);
            assert_eq!(state[0].lmids, now);
            assert_eq!(
                state[0].puts,
                vec![(2, "two".to_owned()), (3, "three".to_owned())]
            );
        });
    }

    /// A socket that died unnoticed is still sent to, and what it is sent
    /// is logged like any poke: the client that returns with the cookie it
    /// last received, while its dead socket is still attached, is sent
    /// every poke after it, in order, many more than a handful, and hears
    /// what follows; the dead socket's later departure changes nothing.
    #[test]
    fn a_client_whose_socket_died_unnoticed_is_caught_up() {
        on_local(|| {
            let mut bench = Bench::new("262144");
            let mut dead = bench.connect("a", None).expect("accepted");
            bench.hydrate(&dead, "h1", 1, &[(1, "one")]);
            let last_received = bench.cookie();
            dead.pokes();
            for id in 2..=201 {
                bench.deliver(vec![put(1, id, "written while nobody heard")]);
            }
            let unheard = dead.pokes();
            assert_eq!(unheard.len(), 200);

            let mut back = bench
                .connect("a-again", Some(&last_received))
                .expect("caught up");
            let replayed = back.pokes();
            assert_eq!(
                replayed, unheard,
                "every poke after its cookie, as it was sent"
            );
            assert_eq!(replayed[0].base.as_deref(), Some(last_received.as_str()));
            assert_eq!(
                replayed.last().map(|poke| poke.cookie.clone()),
                Some(bench.cookie())
            );

            bench.core.disconnect("g", "a");
            bench.deliver(vec![put(1, 500, "after")]);
            let after = back.pokes();
            assert_eq!(after.len(), 1);
            assert_eq!(after[0].puts, vec![(500, "after".to_owned())]);
        });
    }

    /// The two ways of keeping what a client missed meet without a gap or
    /// an overlap. A socket dies unnoticed at cookie `c`: the pokes sent to
    /// it meanwhile are in the log. The server then learns of it and the
    /// group stands still: what changes from there on waits as one
    /// operation per row. The client that returns much later with `c` is
    /// sent the logged pokes as they were, which bring it to where the
    /// group stopped, and then one poke from there with the net of the
    /// rest; had it received some of the logged pokes before dying, its
    /// cookie says so and it is sent only the ones after it.
    #[test]
    fn a_long_absence_is_the_logged_pokes_and_then_the_waiting_rows() {
        on_local(|| {
            let mut bench = Bench::new("262144");
            let mut dead = bench.connect("a", None).expect("accepted");
            bench.hydrate(&dead, "h1", 1, &[(1, "one")]);
            let last_received = bench.cookie();
            dead.pokes();
            for id in 2..=6 {
                bench.deliver(vec![put(1, id, "sent to the dead socket")]);
            }
            let unheard = dead.pokes();
            let partly = unheard[1].cookie.clone();
            bench.core.disconnect("g", "a");
            let stopped_at = bench.cookie();

            bench.deliver(vec![put(1, 7, "seven")]);
            bench.deliver(vec![put(1, 7, "seven, renamed"), gone(1, 2)]);
            bench.deliver(vec![put(1, 8, "eight"), gone(1, 8)]);
            assert_eq!(bench.cookie(), stopped_at);

            let mut back = bench
                .connect("a2", Some(&last_received))
                .expect("caught up");
            bench.core.flush();
            let pokes = back.pokes();
            assert_eq!(pokes.len(), unheard.len() + 1, "{pokes:?}");
            assert_eq!(pokes[..unheard.len()], unheard[..]);
            let rest = pokes.last().expect("the waiting rows");
            assert_eq!(rest.base.as_deref(), Some(stopped_at.as_str()));
            assert_eq!(rest.puts, vec![(7, "seven, renamed".to_owned())]);
            assert_eq!(rest.dels, vec![2, 8]);
            assert_eq!(rest.cookie, bench.cookie());

            let mut other = Bench::new("262144");
            let mut tab = other.connect("a", None).expect("accepted");
            other.hydrate(&tab, "h1", 1, &[(1, "one")]);
            tab.pokes();
            for id in 2..=6 {
                other.deliver(vec![put(1, id, "sent to the dead socket")]);
            }
            let sent = tab.pokes();
            assert_eq!(sent[1].cookie, partly);
            other.core.disconnect("g", "a");
            let mut half = other.connect("a2", Some(&partly)).expect("caught up");
            assert_eq!(
                half.pokes(),
                sent[2..],
                "only the pokes after the one it last received"
            );
        });
    }

    /// The log keeps the newest pokes within its budget without a gap:
    /// every poke is pushed and the oldest are dropped until the rest fit,
    /// however many that is, and a poke larger than the budget empties it,
    /// since nothing older can be continued from without it.
    #[test]
    fn the_poke_log_is_bounded_and_gapless() {
        let frames = |bytes: usize| -> Arc<[Bytes]> {
            vec![Bytes::from(vec![b'x'; bytes - FRAME_OVERHEAD])].into()
        };
        let mut log = PokeLog::default();
        for version in 1..=5 {
            log.push(version, "p", &frames(400), 1_000);
        }
        assert_eq!(log.pokes.len(), 2);
        assert_eq!(log.bytes, 800);
        assert!(log.after(2).is_none(), "version 3 is gone");
        assert_eq!(log.after(3).map(|pokes| pokes.len()), Some(2));
        assert_eq!(log.after(4).map(|pokes| pokes.len()), Some(1));
        log.push(6, "p", &frames(5_000), 1_000);
        assert!(log.pokes.is_empty() && log.bytes == 0);
        assert!(log.after(5).is_none());
        for version in 7..=1_006 {
            log.push(version, "p", &frames(100), 256 * 1024);
        }
        assert_eq!(log.pokes.len(), 1_000, "only the bytes bound it");
        assert_eq!(log.after(6).map(|pokes| pokes.len()), Some(1_000));
    }
}
