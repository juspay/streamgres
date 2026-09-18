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
//! groups whose id hashes to it; a group's engine client id carries the
//! thread's index in its low digits, so the engine side routes every
//! event to the thread that owns the client with no table. A thread
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
//! group, the engine client its subscriptions belong to, the queries each
//! of its clients desires (by hash), the subscription behind each query,
//! and for every row shipped the subscription parts holding it, so a row
//! is `del`ed only when its last holder lets go. A poke goes out per
//! committed transaction (so a mutation's rows and its `lastMutationID`
//! travel together), per landed read, and per query change, advancing the
//! group's version; the version is the cookie the client hands back when
//! it reconnects, and a cookie this server does not hold (it keeps no
//! history) resets the client to a fresh sync.
//!
//! The application's mutation ids arrive as writes to its clients table,
//! which the engine side carries inside the transaction's own
//! [`Event::Committed`], so a mutation's rows and its id go out in the
//! same poke and no later transaction's id rides out early.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use serde_json::{Value as Json, json};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::spawn_local;

use super::ast::Translated;
use super::config::Config;
use super::protocol::{self, ClientSchema};
use super::wire;
use crate::ivm::{ClientUpdate, QueryPart};
use crate::log::{log_debug, log_info, log_warn};
use crate::model::{
    Catalog, ClientId, DataFrameKey, DataFrameOperation, DataFrameRow, Lsn, MultiTableReadQuery,
    RowData, SubId, TableName, Value, WriteQuery,
};
use crate::stats::Stats;
use crate::sync::{Command, Event};

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

/// The columns a client's schema declares per table: what a row is
/// shipped with when the schema is known. Interned by content, so groups
/// with the same schema share one and their serialized rows.
type Columns = HashMap<String, HashSet<String>>;

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
        schema: Option<ClientSchema>,
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
        schema: Option<ClientSchema>,
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
/// showed the same image or still shows the row.
fn account(
    rows: &mut HashMap<(TableName, DataFrameKey), Held>,
    table: TableName,
    key: DataFrameKey,
    image: Option<DataFrameRow>,
    holders: HashSet<(SubId, QueryPart)>,
) -> Option<RowOp> {
    let slot = (table.clone(), key.clone());
    match image {
        Some(image) => {
            if holders.is_empty() {
                return None;
            }
            match rows.get_mut(&slot) {
                Some(held) => {
                    held.holders.extend(holders);
                    if Arc::ptr_eq(&held.image, &image.data) || *held.image == *image.data {
                        return None;
                    }
                    held.image = image.data.clone();
                }
                None => {
                    rows.insert(
                        slot,
                        Held {
                            holders,
                            image: image.data.clone(),
                        },
                    );
                }
            }
            Some(RowOp::Put(table, key, image))
        }
        None => {
            let held = rows.get_mut(&slot)?;
            for holder in &holders {
                held.holders.remove(holder);
            }
            if held.holders.is_empty() {
                rows.remove(&slot);
                Some(RowOp::Del(table, key))
            } else {
                None
            }
        }
    }
}

/// Every row the group shows only through `sub`, taken out of the ledger:
/// the deletes a released subscription owes the client.
fn release_rows(
    rows: &mut HashMap<(TableName, DataFrameKey), Held>,
    sub: SubId,
) -> Vec<(TableName, DataFrameKey)> {
    let mut emptied = Vec::new();
    for (slot, held) in rows.iter_mut() {
        held.holders.retain(|(holder, _)| *holder != sub);
        if held.holders.is_empty() {
            emptied.push(slot.clone());
        }
    }
    for slot in &emptied {
        rows.remove(slot);
    }
    emptied
}

/// One client group's view.
///
/// `generation` stamps the group's last connect or disconnect from the
/// one monotonic counter, so an expiry timer scheduled for an earlier
/// incarnation of the same group id can never match this one.
struct Group {
    client: ClientId,
    version: u64,
    generation: u64,
    sockets: HashMap<String, Socket>,
    desired: HashMap<String, HashSet<String>>,
    queries: HashMap<String, QueryState>,
    subs: HashSet<SubId>,
    /// The hidden parts of each subscription, for the per-row check.
    hidden: HashMap<SubId, HashSet<QueryPart>>,
    rows: HashMap<(TableName, DataFrameKey), Held>,
    lmids: HashMap<String, i64>,
    columns: Option<Arc<Columns>>,
    queued_desired: HashMap<String, Vec<Json>>,
    queued_got: Vec<Json>,
    queued_lmids: HashMap<String, i64>,
    queued_rows: Vec<RowOp>,
}

impl Group {
    /// An empty group over engine client `client`.
    fn new(client: ClientId) -> Self {
        Group {
            client,
            version: 0,
            generation: 0,
            sockets: HashMap::new(),
            desired: HashMap::new(),
            queries: HashMap::new(),
            subs: HashSet::new(),
            hidden: HashMap::new(),
            rows: HashMap::new(),
            lmids: HashMap::new(),
            columns: None,
            queued_desired: HashMap::new(),
            queued_got: Vec::new(),
            queued_lmids: HashMap::new(),
            queued_rows: Vec::new(),
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

    /// The address of the group's column set, the key its serialized rows
    /// are shared under; zero when no schema is known.
    fn schema_key(&self) -> usize {
        self.columns
            .as_ref()
            .map_or(0, |columns| Arc::as_ptr(columns) as usize)
    }
}

/// One group thread's state.
///
/// - `shard` / `shards`: this thread's index and how many there are; the
///   engine client ids it hands out are `shard + shards × n`.
/// - `schemas`: the client column sets seen, interned by content.
/// - `stats`: where the flushes and the pokes are timed; `oldest` is when
///   the feed decoded the oldest transaction applied since the last flush,
///   the start of the clock every poke of the next flush carries.
pub struct Groups {
    config: Arc<Config>,
    catalog: Arc<Catalog>,
    shard: usize,
    shards: usize,
    schemas: HashMap<Vec<(String, Vec<String>)>, Arc<Columns>>,
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
    by_client: HashMap<ClientId, String>,
    by_sub: HashMap<SubId, (String, String)>,
    /// Registrations in flight, by the token they were sent with.
    awaiting: HashMap<u64, (String, String)>,
    next_client: u64,
    pending: HashMap<ClientId, Vec<ClientUpdate>>,
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
    catalog: Arc<Catalog>,
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
        shards: shards.max(1),
        schemas: HashMap::new(),
        stats,
        oldest: None,
        commands: outbox,
        requests,
        ready: false,
        readiness,
        backlog: Vec::new(),
        groups: HashMap::new(),
        by_client: HashMap::new(),
        by_sub: HashMap::new(),
        awaiting: HashMap::new(),
        next_client: 1,
        pending: HashMap::new(),
        lmid_changes: HashMap::new(),
        dirty: HashSet::new(),
        next_poke: 1,
        next_generation: 0,
    };
    log_info!(
        "client groups {}/{} up; waiting for the first heartbeat before serving queries",
        shard + 1,
        shards.max(1)
    );
    let mut taken_requests = Vec::with_capacity(256);
    let mut taken_events = Vec::with_capacity(1024);
    loop {
        tokio::select! {
            taken = requests_rx.recv_many(&mut taken_requests, 256) => {
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
                schema,
                lmids,
                reply,
            } => {
                let outcome = self.connect(&group, wsid, socket, base_cookie, schema, lmids);
                let _ = reply.send(outcome);
            }
            Request::Disconnect { group, wsid } => self.disconnect(&group, &wsid),
            Request::Desired { .. } if !self.ready => self.backlog.push(request),
            Request::Desired {
                group,
                client,
                schema,
                ops,
            } => self.desired(&group, &client, schema, ops),
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
                        hydrate.record(state.since.elapsed());
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

    /// Keep one step's deltas for their groups' next pokes.
    fn absorb(&mut self, updates: Vec<ClientUpdate>) {
        for update in updates {
            if let Some(group) = self.by_client.get(&update.client) {
                self.dirty.insert(group.clone());
            }
            self.pending.entry(update.client).or_default().push(update);
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

    /// Attach a connection to its group, creating or resetting the group
    /// as its cookie requires.
    fn connect(
        &mut self,
        group_id: &str,
        wsid: String,
        socket: Socket,
        base_cookie: Option<String>,
        schema: Option<ClientSchema>,
        lmids: Vec<(String, i64)>,
    ) -> ConnectReply {
        let known: Option<Option<String>> = self
            .groups
            .get(group_id)
            .map(|group| (group.version > 0).then(|| protocol::cookie(group.version)));
        match (&known, &base_cookie) {
            (None, Some(_)) => {
                return ConnectReply::Reset {
                    reason: "the server holds no state for this client group".to_owned(),
                };
            }
            (Some(current), offered) if current != offered => {
                if offered.is_none() {
                    self.drop_group(group_id);
                } else {
                    return ConnectReply::Reset {
                        reason: format!(
                            "the server is at {}, the client at {}",
                            current.as_deref().unwrap_or("the start"),
                            offered.as_deref().unwrap_or("the start")
                        ),
                    };
                }
            }
            _ => {}
        }
        if !self.groups.contains_key(group_id) {
            let client = ClientId(self.shard as u64 + self.shards as u64 * self.next_client);
            self.next_client += 1;
            self.groups.insert(group_id.to_owned(), Group::new(client));
            self.by_client.insert(client, group_id.to_owned());
            log_info!(
                "client group {group_id} opened as engine client {}",
                client.0
            );
        }
        self.next_generation += 1;
        let generation = self.next_generation;
        let columns = schema.as_ref().map(|schema| self.intern_schema(schema));
        let group = self.groups.get_mut(group_id).expect("just ensured");
        if let Some(columns) = columns {
            group.columns = Some(columns);
        }
        for (client, lmid) in lmids {
            let known = group.lmids.entry(client.clone()).or_insert(0);
            if lmid > *known {
                *known = lmid;
            }
        }
        for (client, lmid) in &group.lmids {
            group.queued_lmids.insert(client.clone(), *lmid);
        }
        group.generation = generation;
        log_info!(
            "connection {wsid} joined client group {group_id} as client {}",
            socket.client
        );
        group.sockets.insert(wsid, socket);
        self.mark(group_id);
        ConnectReply::Accepted
    }

    /// A connection closed; when it was the group's last, the group's
    /// subscriptions stay for the configured grace period.
    fn disconnect(&mut self, group_id: &str, wsid: &str) {
        let Some(group) = self.groups.get_mut(group_id) else {
            return;
        };
        group.sockets.remove(wsid);
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
            self.command(Command::UnregisterClient(group.client));
            self.by_client.remove(&group.client);
            self.by_sub.retain(|_, (owner, _)| owner != group_id);
            self.pending.remove(&group.client);
            self.lmid_changes.remove(group_id);
            self.dirty.remove(group_id);
        }
    }

    /// Apply a client's desired-query changes to its group.
    fn desired(
        &mut self,
        group_id: &str,
        client: &str,
        schema: Option<ClientSchema>,
        ops: Vec<DesiredOp>,
    ) {
        if !self.groups.contains_key(group_id) {
            log_warn!("desired queries for an unknown client group {group_id}");
            return;
        }
        if let Some(schema) = &schema {
            let columns = self.intern_schema(schema);
            self.groups.get_mut(group_id).expect("checked").columns = Some(columns);
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
        let engine_client = group.client;
        self.awaiting
            .insert(token, (group_id.to_owned(), hash.clone()));
        self.command(Command::Register {
            client: engine_client,
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
        log_warn!(
            "group {group_id}: query {} ({hash}) refused: {reason}",
            state.name
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
        self.dirty.insert(group.clone());
        self.lmid_changes
            .entry(group)
            .or_default()
            .insert(client, lmid);
    }

    /// The client's schema as the columns to ship per table, shared with
    /// every group that declared the same.
    fn intern_schema(&mut self, schema: &ClientSchema) -> Arc<Columns> {
        let mut key: Vec<(String, Vec<String>)> = schema
            .tables
            .iter()
            .map(|(table, spec)| {
                let mut columns: Vec<String> = spec.columns.keys().cloned().collect();
                columns.sort();
                (table.clone(), columns)
            })
            .collect();
        key.sort();
        self.schemas
            .entry(key)
            .or_insert_with(|| {
                Arc::new(
                    schema
                        .tables
                        .iter()
                        .map(|(table, spec)| {
                            (table.clone(), spec.columns.keys().cloned().collect())
                        })
                        .collect(),
                )
            })
            .clone()
    }

    /// Turn everything accumulated into one poke per group that has
    /// anything to hear, every row serialized once for all of them.
    fn flush(&mut self) {
        let stray: Vec<ClientId> = self
            .pending
            .keys()
            .filter(|client| !self.by_client.contains_key(client))
            .copied()
            .collect();
        for client in stray {
            self.pending.remove(&client);
        }
        if self.dirty.is_empty() {
            self.oldest = None;
            return;
        }
        let started = Instant::now();
        let since = self.oldest.take();
        let mut touched: Vec<String> = self.dirty.drain().collect();
        touched.sort();
        let mut fragments: HashMap<(usize, usize), Bytes> = HashMap::new();
        for group_id in touched {
            self.poke(&group_id, &mut fragments, since);
        }
        self.stats.groups_flush.record(started.elapsed());
    }

    /// Assemble and send one group's poke, if there is anything in it;
    /// `fragments` are the row entries already serialized in this flush,
    /// by image and column set, and `since` when the oldest transaction
    /// of the flush was decoded.
    fn poke(
        &mut self,
        group_id: &str,
        fragments: &mut HashMap<(usize, usize), Bytes>,
        since: Option<Instant>,
    ) {
        let lmid_changes = self.lmid_changes.remove(group_id).unwrap_or_default();
        let Some(group) = self.groups.get_mut(group_id) else {
            return;
        };
        let updates = self.pending.remove(&group.client).unwrap_or_default();
        let mut rows: Vec<RowOp> = std::mem::take(&mut group.queued_rows);
        for update in updates {
            let table = update.table;
            let (key, image, adds) = match update.op {
                DataFrameOperation::Add(key, row) => (key, Some(row), true),
                DataFrameOperation::Delete(key, _) => (key, None, false),
            };
            let holders: HashSet<(SubId, QueryPart)> = update
                .targets
                .into_iter()
                .filter(|target| group.subs.contains(&target.sub))
                .filter(|target| {
                    !group
                        .hidden
                        .get(&target.sub)
                        .is_some_and(|hidden| hidden.contains(&target.part))
                })
                .map(|target| (target.sub, target.part))
                .collect();
            debug_assert!(adds == image.is_some());
            if let Some(op) = account(&mut group.rows, table, key, image, holders) {
                rows.push(op);
            }
        }
        let got = std::mem::take(&mut group.queued_got);
        let desired = std::mem::take(&mut group.queued_desired);
        let mut lmids = std::mem::take(&mut group.queued_lmids);
        for (client, lmid) in lmid_changes {
            let known = group.lmids.entry(client.clone()).or_insert(0);
            if lmid >= *known {
                *known = lmid;
                lmids.insert(client, lmid);
            }
        }
        if rows.is_empty() && got.is_empty() && desired.is_empty() && lmids.is_empty() {
            return;
        }
        let rows = coalesce(rows);
        let poke_id = self.next_poke.to_string();
        self.next_poke += 1;
        let base = (group.version > 0).then(|| protocol::cookie(group.version));
        group.version += 1;
        let cookie = protocol::cookie(group.version);
        let per_part = self.config.rows_per_part.max(1);
        let mut frames: Vec<Bytes> = Vec::with_capacity(rows.len().div_ceil(per_part) + 3);
        frames.push(Bytes::from(protocol::poke_start(&poke_id, base.as_deref())));
        let got_count = got.len();
        let mut head: Vec<u8> = Vec::new();
        if !desired.is_empty() {
            head.extend_from_slice(b",\"desiredQueriesPatches\":");
            let _ = serde_json::to_writer(&mut head, &desired);
        }
        if !got.is_empty() {
            head.extend_from_slice(b",\"gotQueriesPatch\":");
            let _ = serde_json::to_writer(&mut head, &got);
        }
        if !lmids.is_empty() {
            head.extend_from_slice(b",\"lastMutationIDChanges\":");
            let _ = serde_json::to_writer(&mut head, &lmids);
        }
        let mut head = Some(head);
        if rows.is_empty() {
            frames.push(part_frame(&poke_id, head.take(), &[]));
        }
        let schema = group.schema_key();
        let mut puts = 0usize;
        let mut dels = 0usize;
        for chunk in rows.chunks(per_part) {
            let mut entries: Vec<Bytes> = Vec::with_capacity(chunk.len());
            for op in chunk {
                match op {
                    RowOp::Put(table, _, row) => {
                        let Some(declared) = self.catalog.table(table.as_str()) else {
                            continue;
                        };
                        puts += 1;
                        let (shared, serialized) =
                            (&self.stats.rows_shared, &self.stats.rows_serialized);
                        let fragment =
                            match fragments.entry((Arc::as_ptr(&row.data) as usize, schema)) {
                                std::collections::hash_map::Entry::Occupied(entry) => {
                                    shared.fetch_add(1, Ordering::Relaxed);
                                    entry.into_mut()
                                }
                                std::collections::hash_map::Entry::Vacant(entry) => {
                                    serialized.fetch_add(1, Ordering::Relaxed);
                                    let allowed = group
                                        .columns
                                        .as_ref()
                                        .and_then(|columns| columns.get(table.as_str()));
                                    let mut bytes = Vec::with_capacity(256);
                                    wire::write_put(&mut bytes, declared, row, allowed);
                                    entry.insert(Bytes::from(bytes))
                                }
                            };
                        entries.push(fragment.clone());
                    }
                    RowOp::Del(table, key) => {
                        dels += 1;
                        let mut bytes = Vec::with_capacity(64);
                        wire::write_del(&mut bytes, table.as_str(), key);
                        entries.push(Bytes::from(bytes));
                    }
                }
            }
            frames.push(part_frame(&poke_id, head.take(), &entries));
        }
        frames.push(Bytes::from(protocol::poke_end(&poke_id, &cookie)));
        log_debug!(
            "group {group_id}: poke {poke_id} {} -> {cookie}: {puts} puts, {dels} dels, {got_count} got, {} lmids",
            base.as_deref().unwrap_or("null"),
            lmids.len()
        );
        self.stats.pokes.fetch_add(1, Ordering::Relaxed);
        let frames: Arc<[Bytes]> = frames.into();
        group.broadcast(&frames, since);
    }
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
        let first = account(
            &mut rows,
            table.clone(),
            key.clone(),
            Some(image.clone()),
            holder(1),
        );
        assert!(
            matches!(first, Some(RowOp::Put(..))),
            "the first holder puts the row"
        );
        let second = account(
            &mut rows,
            table.clone(),
            key.clone(),
            Some(image.clone()),
            holder(2),
        );
        assert!(
            second.is_none(),
            "the same image from a second holder sends nothing"
        );
        let released = account(&mut rows, table.clone(), key.clone(), None, holder(1));
        assert!(
            released.is_none(),
            "the first holder letting go deletes nothing the second shows"
        );
        let gone = account(&mut rows, table.clone(), key.clone(), None, holder(2));
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
        account(
            &mut rows,
            table.clone(),
            key.clone(),
            Some(image),
            holder(1),
        );
        let (_, changed) = row(1, "b");
        let again = account(
            &mut rows,
            table.clone(),
            key.clone(),
            Some(changed),
            holder(2),
        );
        assert!(
            matches!(again, Some(RowOp::Put(..))),
            "a changed image is sent"
        );
        let (key2, image2) = row(2, "only two");
        account(
            &mut rows,
            table.clone(),
            key2.clone(),
            Some(image2),
            holder(2),
        );
        let owed = release_rows(&mut rows, SubId(2));
        assert_eq!(
            owed,
            vec![(table.clone(), key2)],
            "only the row nobody else shows is deleted"
        );
        assert!(
            rows.contains_key(&(table, key)),
            "the shared row stays for the first holder"
        );
    }
}
