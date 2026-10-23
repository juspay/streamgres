//! Following a backend's SQL: a backend subscribes the SQL its ORM
//! renders and is kept current on it, over whatever carries the frames.
//! The S2S endpoint ([`crate::client::s2s`]) carries them over a
//! WebSocket; a backend that embeds the engine carries them over
//! channels in its own process. A [`session`] reads the backend's frames
//! from any stream of text and queues the engine's on an [`Outgoing`],
//! so both run the same code.
//!
//! A backend keeps its own copy of what it reads (a Diesel wrapper serves
//! repeated reads from a local cache). In rows mode the engine ships the
//! query's rows on `accepted` and each committed transaction's changes
//! to them as a `delta`, so the backend keeps the result without reading
//! Postgres. Otherwise it is sent a `touched` naming every subscription a
//! committed transaction changed the rows of, and reads Postgres again.
//! A query outside what the engine follows is `refused`, and the backend
//! serves it live. A session carries no user: it serves whatever the SQL
//! names, so it belongs to trusted backends.
//!
//! One task ([`run`]) owns every S2S subscription: it is one more
//! consumer of the engine's events beside the group threads, with its own
//! sink index, and maps the engine's subscription ids back to the
//! connection and the id the backend chose. Nothing of the Zero protocol
//! (client groups, pokes, mutations, the application server) is involved.
//!
//! # Protocol
//!
//! JSON text frames, each a two-element array of a tag and a body. A
//! connection holds at most `STREAMGRES_S2S_CONNECTION_SUBSCRIPTIONS`
//! subscriptions, those still registering included; a subscribe past
//! that is refused.
//!
//! Upstream:
//! - `["subscribe", {"id": n, "sql": "...", "binds": [{"oid": 25, "bytes": "<base64>"}, {"oid": 20, "bytes": null}], "rows": bool}]`
//!   `sql` is what the ORM rendered, with `$1..` placeholders; each bind
//!   is the parameter's Postgres type OID and its binary bytes, `null`
//!   for SQL `NULL`. `id` is the backend's own, unique per connection.
//!   `rows` asks for the rows themselves (below) rather than touches.
//!   The SQL may join tables (`FROM ("a" INNER JOIN "b" ON (...))`, as
//!   Diesel renders it); the engine follows each table and the backend
//!   composes the join from the rows it is sent.
//! - `["unsubscribe", {"id": n}]`
//! - `["ping"]`
//!
//! Downstream:
//! - `["hello", {"protocol": 1, "max_subscriptions": n, "database":
//!   {"system": "...", "name": "..."}}]` first; `max_subscriptions` is the
//!   per-connection limit, `null` for none, so a client can stop short of
//!   it rather than be refused. `database` names what the engine streams
//!   (the cluster's `system_identifier` and the database's name), so a
//!   backend reading another database does not take these rows for its
//!   own. An engine that cannot read them closes the socket instead of
//!   greeting it.
//! - `["accepted", {"id": n}]` once the engine follows the query; from
//!   that moment every change to its rows is reported, so the backend
//!   reads Postgres only after it. In rows mode the body also carries
//!   the hydrated result: `"nodes"`, one per table of the query, root
//!   first, each with `"table"`, `"key"` (its primary-key columns),
//!   `"parent"` (the table it joins to, `null` for the root),
//!   `"parent_column"` and `"child_column"` (the join equality),
//!   `"inner"` (`false` for a LEFT join) and `"rows"` (JSON objects keyed
//!   by column, every column of the row); then `"columns"` (the projected
//!   `[table, column]` pairs, `null` for `*`), `"names"` (what each comes
//!   out as: its alias or its own name; for a `COUNT(*)` the one output
//!   name), `"count"` (a `COUNT(*)`), `"order"` (`[[column, "asc"|"desc"],
//!   ...]`, on the root table) and `"limit"` (`null` for none).
//! - `["accepted", ...]` also carries `"offset"`: the engine keeps
//!   `limit + offset` rows and the client skips `offset` of them. Under
//!   a `COUNT(*)` the engine keeps every row, and `limit` and `offset`
//!   apply to the one row the count is. A
//!   `bytea` column travels as Postgres's hex text; a bind of an enum
//!   type is read as its label; a time travels as microseconds since
//!   the Unix epoch, Postgres's own precision (the Zero protocol sees
//!   milliseconds), and a timestamp bind is read the same way.
//! - `["refused", {"id": n, "reason": "..."}]` when the engine cannot
//!   follow the query (outside the SQL subset, an unknown table, too many
//!   rows). The backend serves that query live.
//! - `["touched", {"ids": [n, ...]}]` per transaction that changed the
//!   rows of those subscriptions, for subscriptions not in rows mode.
//! - `["delta", {"id": n, "changes": [{"table": t, "put": [row, ...], "del": [key, ...]}, ...]}]`
//!   per transaction, for one subscription in rows mode: per table, the
//!   rows that entered or changed and the keys of rows that left. One
//!   entry per key, its last state in the transaction: a changed row is
//!   a `put` of its new image, a row that came and went is a `del`, and
//!   no key is in both lists. A backend that stops reading its frames
//!   is closed once its queue is full rather than buffered for: more
//!   than 4096 frames, or more than 64 MiB of them, waiting.
//! - `["pong"]`
//! - `["lag", {"ms": n}]` right after `hello`, then every 500 ms: how far
//!   at most the engine's applied changes trail the database, the age of
//!   the newest heartbeat the replication stream has brought back and
//!   the engine applied (`null` before the first is sent), so a backend
//!   can stop trusting its copy while the engine is behind.
//! - `["error", {"reason": "..."}]` for a frame that is not one of the
//!   above (not JSON, an unknown tag, no usable `id`). A `subscribe`
//!   whose `id` reads but whose body does not is answered `refused` with
//!   that id instead, so no subscription is left waiting.
//!
//! An `id` is never reused on a connection: frames already queued for an
//! unsubscribed id may still arrive after the `unsubscribe`, and would
//! otherwise be taken for the new subscription's.

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::{FutureExt, Stream, StreamExt};
use serde::Serialize;
use serde_json::value::RawValue;
use serde_json::{Value as Json, json};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};

use crate::plan::{self, PlanCache, Policy, Translated};
use crate::sync::pg::PgStorage;
use crate::wire;
use crate::ivm::Delta;
use crate::log::{log_debug, log_info, log_warn};
use crate::model::{
    ComparisonOperator, DataFrameKey, DataFrameOperation, DataFrameRow, MultiTableReadQuery, Order,
    OrderBy, SubId, TableName, Value, Where,
};
use crate::parser::binds::decodes;
use crate::parser::{BindValue, Projection, parse_select};
use crate::stats::Stats;
use crate::sync::catalog::with_enums;
use crate::sync::{CatalogHandle, Command, Event};

/// The S2S protocol version the endpoint's `hello` announces.
pub const PROTOCOL_VERSION: u32 = 1;

/// How many requests may wait for the task before a connection's reader
/// waits too.
const REQUEST_QUEUE: usize = 1024;

/// How many frames may wait for a connection's writer. A backend that
/// stops reading would otherwise have every result and delta buffered
/// for it without limit; past this the connection is closed instead.
const OUTGOING_QUEUE: usize = 4096;

/// How many bytes of frames may wait for a connection's writer: a
/// rows-mode `accepted` can be megabytes, so a count of frames alone
/// does not bound what a backend that stops reading pins. A frame is
/// always let onto an empty queue, so one large result still goes out.
const OUTGOING_BYTES: usize = 64 * 1024 * 1024;

/// A connection's outgoing frames, bounded in frames and in bytes, with
/// the bytes still waiting for its writer.
#[derive(Debug, Clone)]
pub struct Outgoing {
    frames: mpsc::Sender<String>,
    queued: Arc<AtomicUsize>,
}

impl Outgoing {
    /// A queue and the receiving end its writer drains.
    pub fn new() -> (Outgoing, mpsc::Receiver<String>) {
        let (frames, receiver) = mpsc::channel(OUTGOING_QUEUE);
        let queued = Arc::new(AtomicUsize::new(0));
        (Outgoing { frames, queued }, receiver)
    }

    /// Queues `text` for the writer. `false` when the backend is not
    /// keeping up (the queue is full in frames or in bytes) and the
    /// connection must be closed; a writer already gone is not that.
    fn push(&self, text: String) -> bool {
        let len = text.len();
        let before = self.queued.fetch_add(len, Ordering::AcqRel);
        if before > 0 && before + len > OUTGOING_BYTES {
            self.queued.fetch_sub(len, Ordering::AcqRel);
            return false;
        }
        match self.frames.try_send(text) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                self.queued.fetch_sub(len, Ordering::AcqRel);
                false
            }
            Err(TrySendError::Closed(_)) => {
                self.queued.fetch_sub(len, Ordering::AcqRel);
                true
            }
        }
    }

    /// The writer's side of the byte count. It holds no sender, so the
    /// queue still closes once every [`Outgoing`] is dropped.
    pub fn drained(&self) -> Drained {
        Drained(Arc::clone(&self.queued))
    }
}

/// The bytes still waiting in a connection's [`Outgoing`], as its
/// writer releases them.
pub struct Drained(Arc<AtomicUsize>);

impl Drained {
    /// The writer sent a frame of `len` bytes.
    pub fn sent(&self, len: usize) {
        self.0.fetch_sub(len, Ordering::AcqRel);
    }
}

/// What a connection asks of the task.
#[derive(Debug)]
pub enum Request {
    Connected {
        conn: u64,
        out: Outgoing,
        /// Fired by the task to close the connection, with the reason.
        close: oneshot::Sender<&'static str>,
        /// How many subscriptions the connection holds, registering ones
        /// included, kept by the task for the connection's handler.
        held: Arc<AtomicUsize>,
    },
    /// A query parsed and planned by the connection's handler.
    Subscribe(Box<Subscribe>),
    Unsubscribe {
        conn: u64,
        id: u64,
    },
    Disconnected {
        conn: u64,
    },
}

/// A subscription ready to register.
#[derive(Debug)]
pub struct Subscribe {
    conn: u64,
    id: u64,
    query: MultiTableReadQuery,
    /// Present when the backend asked for rows.
    shape: Option<Shape>,
}

/// Per table, what one transaction did to each key: its new image (an
/// index into the transaction's serialized rows), or `None` once
/// deleted. One entry per key, so a row admitted and evicted in one
/// transaction is a delete, not a put the client would keep.
type Changes = Vec<(TableName, HashMap<DataFrameKey, Option<usize>>)>;

/// A `delta` frame's body, its rows spliced in as they were serialized.
#[derive(Serialize)]
struct DeltaBody<'a> {
    id: u64,
    changes: Vec<TableChanges<'a>>,
}

/// One table's part of a `delta`.
#[derive(Serialize)]
struct TableChanges<'a> {
    table: &'a str,
    put: Vec<&'a RawValue>,
    del: Vec<Json>,
}

/// How often every connection is told the engine's lag.
const LAG_EVERY: Duration = Duration::from_millis(500);

/// How often a refused upgrade of one kind is logged at most; the ones
/// in between are counted into the next line.
pub(crate) const REFUSAL_LOG_EVERY: Duration = Duration::from_secs(60);

/// What sessions share: the requests they send the task, the ids they
/// are given, and the database the engine streams once it is read.
#[derive(Debug)]
pub struct Hub {
    requests: mpsc::Sender<Request>,
    next_conn: AtomicU64,
    unidentified: Throttle,
    /// The database the engine streams, as `hello` names it, once read:
    /// it cannot change while the process runs.
    database: tokio::sync::OnceCell<Json>,
}

impl Hub {
    /// A hub, and the requests its sessions send the task ([`run`]).
    pub fn new() -> (Hub, mpsc::Receiver<Request>) {
        let (requests, requests_rx) = mpsc::channel(REQUEST_QUEUE);
        (
            Hub {
                requests,
                next_conn: AtomicU64::new(1),
                unidentified: Throttle::new(REFUSAL_LOG_EVERY),
                database: tokio::sync::OnceCell::new(),
            },
            requests_rx,
        )
    }
}

/// How large a query, and how many of them, one connection may ask the
/// engine to follow; zero is no limit.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Subscriptions one connection may hold, registering ones included.
    pub per_connection: usize,
    /// Disjuncts a query's filters may normalize to ([`within_limits`]).
    pub max_disjuncts: u64,
    /// Values an `IN` or `NOT IN` list may hold.
    pub max_list_values: usize,
}

/// Everything a session needs of the engine side: the hub, the catalog
/// queries are parsed against, the storage that plans and identifies
/// them, the plans remembered across connections, the join policy, the
/// limits, and the stats the lag is read from.
pub struct Context {
    pub hub: Hub,
    pub catalog: Arc<CatalogHandle>,
    pub storage: Arc<PgStorage>,
    pub plans: Arc<PlanCache>,
    pub policy: Policy,
    pub limits: Limits,
    pub stats: Arc<Stats>,
}

/// Lets a repeated warning through at most once per period. A backend
/// with the wrong token retries every second, and so can anyone who
/// reaches the port, so each refusal cannot be a log line.
#[derive(Debug)]
pub(crate) struct Throttle {
    every: Duration,
    next: Mutex<Option<Instant>>,
    quiet: AtomicU64,
}

impl Throttle {
    /// A throttle logging at most once per `every`.
    pub(crate) fn new(every: Duration) -> Throttle {
        Throttle {
            every,
            next: Mutex::new(None),
            quiet: AtomicU64::new(0),
        }
    }

    /// At `now`: how many were held back since the last one let through,
    /// when this one may be logged; `None` when it is counted instead.
    fn pass(&self, now: Instant) -> Option<u64> {
        let mut next = self.next.lock().expect("throttle");
        if next.is_some_and(|next| now < next) {
            self.quiet.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        *next = Some(now + self.every);
        Some(self.quiet.swap(0, Ordering::Relaxed))
    }
}

/// Logs a refused upgrade, throttled per kind.
pub(crate) fn refusal(throttle: &Throttle, what: &str) {
    if let Some(quiet) = throttle.pass(Instant::now()) {
        if quiet == 0 {
            log_warn!("s2s connection refused: {what}");
        } else {
            log_warn!("s2s connection refused: {what} ({quiet} more since the last warning)");
        }
    }
}

/// One backend connection as the task sees it.
struct Conn {
    out: Outgoing,
    /// Closes the connection; taken once used.
    close: Option<oneshot::Sender<&'static str>>,
    /// The backend's ids and the engine's subscriptions they became.
    subs: HashMap<u64, SubId>,
    /// The backend's ids still registering, and their tokens.
    registering: HashMap<u64, u64>,
    /// `subs` and `registering` together, for the connection's handler.
    held: Arc<AtomicUsize>,
}

impl Conn {
    /// A connection holding nothing yet.
    fn new(out: Outgoing, close: oneshot::Sender<&'static str>, held: Arc<AtomicUsize>) -> Conn {
        Conn {
            out,
            close: Some(close),
            subs: HashMap::new(),
            registering: HashMap::new(),
            held,
        }
    }

    /// Queues a frame for the writer.
    fn send(&mut self, message: Json) {
        self.send_text(message.to_string());
    }

    /// Queues frame text for the writer. A full queue means the backend
    /// is not reading: the connection is closed rather than buffered for.
    fn send_text(&mut self, text: String) {
        if !self.out.push(text)
            && let Some(close) = self.close.take()
        {
            let _ = close.send("the backend is not reading its frames");
        }
    }

    /// How many subscriptions the connection holds, registering ones
    /// included.
    fn holds(&self) -> usize {
        self.subs.len() + self.registering.len()
    }

    /// Publishes [`Conn::holds`] to the connection's handler.
    fn note_held(&self) {
        self.held.store(self.holds(), Ordering::Release);
    }
}

/// What a rows-mode subscription needs beyond its ids: the query's shape
/// for the backend to compose, order and project by, and the rows
/// gathered per table while the engine hydrates.
#[derive(Debug)]
pub struct Shape {
    nodes: Vec<NodeShape>,
    projection: Projection,
    order: Vec<OrderBy>,
    /// The statement's own limit and offset; the tree holds `limit +
    /// offset` rows and the client skips and cuts (under a `COUNT(*)`,
    /// the one counted row).
    limit: u32,
    offset: u32,
}

/// One table of the query: how it joins its parent, and its hydrating
/// rows (a key's latest image, `None` once deleted).
#[derive(Debug)]
struct NodeShape {
    table: TableName,
    parent: Option<usize>,
    parent_column: Option<String>,
    child_column: Option<String>,
    inner: bool,
    snapshot: HashMap<DataFrameKey, Option<DataFrameRow>>,
}

impl Shape {
    /// The shape of `query` for a backend that asked for rows, the
    /// statement's own `limit` and `offset` beside it.
    pub fn new(
        query: &MultiTableReadQuery,
        projection: Projection,
        limit: u32,
        offset: u32,
    ) -> Shape {
        let mut nodes = Vec::new();
        /// Notes `query`'s node and its joined nodes below it, depth first.
        fn walk(
            query: &MultiTableReadQuery,
            parent: Option<usize>,
            edge: Option<(&str, &str, bool)>,
            nodes: &mut Vec<NodeShape>,
        ) {
            nodes.push(NodeShape {
                table: query.main_table.table.clone(),
                parent,
                parent_column: edge.map(|(p, _, _)| p.to_string()),
                child_column: edge.map(|(_, c, _)| c.to_string()),
                inner: edge.is_none_or(|(_, _, inner)| inner),
                snapshot: HashMap::new(),
            });
            let index = nodes.len() - 1;
            for join in &query.joins {
                walk(
                    &join.sub,
                    Some(index),
                    Some((
                        join.main_table_column.as_str(),
                        join.sub_table_column.as_str(),
                        join.is_inner,
                    )),
                    nodes,
                );
            }
        }
        walk(query, None, None, &mut nodes);
        Shape {
            nodes,
            projection,
            order: query.main_table.order_by.clone(),
            limit,
            offset,
        }
    }

    /// The node of `table`.
    fn node_of(&mut self, table: &TableName) -> Option<&mut NodeShape> {
        self.nodes.iter_mut().find(|node| &node.table == table)
    }

    /// Folds a hydrating change into its node's snapshot: the key's
    /// latest image, or `None` once deleted.
    fn absorb(&mut self, table: &TableName, op: &DataFrameOperation) {
        let Some(node) = self.node_of(table) else {
            return;
        };
        let (key, row) = match op {
            DataFrameOperation::Add(key, row) => (key, Some(row.clone())),
            DataFrameOperation::Delete(key, _) => (key, None),
        };
        node.snapshot.insert(key.clone(), row);
    }
}

/// The S2S task's state: every connection, registration and
/// subscription, and the engine's side of them.
struct Task {
    catalog: Arc<CatalogHandle>,
    sink: usize,
    commands: mpsc::Sender<Command<MultiTableReadQuery>>,
    conns: HashMap<u64, Conn>,
    /// Registrations in flight, by the token the command carried: the
    /// connection, its id, and the shape when rows were asked for.
    by_token: HashMap<u64, (u64, u64, Option<Shape>)>,
    /// Every live subscription, back to its connection and id.
    by_sub: HashMap<SubId, (u64, u64)>,
    /// Rows-mode subscriptions, by the engine's id.
    shapes: HashMap<SubId, Shape>,
    /// Registered but not yet hydrated: `accepted` waits for the rows,
    /// so the backend's first read lands after the engine's.
    hydrating: HashSet<SubId>,
    /// The token the next registration is sent under.
    next_token: u64,
    /// How many subscriptions one connection may hold, registering ones
    /// included (0: no limit).
    per_connection: usize,
    /// Where the engine's lag is measured ([`crate::sync::heartbeat`]).
    stats: Arc<Stats>,
}

/// Runs the task: the engine's events on one side, the connections'
/// requests on the other. Returns when both channels are closed.
/// Queries are registered only once the engine's position covers the
/// storage; until then a registration would read a snapshot the feed
/// has not caught up with.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    catalog: Arc<CatalogHandle>,
    sink: usize,
    commands: mpsc::Sender<Command<MultiTableReadQuery>>,
    mut events: mpsc::UnboundedReceiver<Event>,
    mut requests: mpsc::Receiver<Request>,
    mut ready: watch::Receiver<bool>,
    per_connection: usize,
    stats: Arc<Stats>,
) {
    while !*ready.borrow() {
        if ready.changed().await.is_err() {
            return;
        }
    }
    log_info!("s2s endpoint serving");
    let mut task = Task {
        catalog,
        sink,
        commands,
        conns: HashMap::new(),
        by_token: HashMap::new(),
        by_sub: HashMap::new(),
        shapes: HashMap::new(),
        hydrating: HashSet::new(),
        next_token: 1,
        per_connection,
        stats,
    };
    let mut lag = tokio::time::interval(LAG_EVERY);
    lag.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = lag.tick() => task.report_lag(),
            event = events.recv() => match event {
                Some(event) => task.event(event).await,
                None => return,
            },
            request = requests.recv() => match request {
                Some(request) => task.request(request).await,
                None => return,
            },
        }
    }
}

impl Task {
    /// Tells every connection how far behind the engine is, from its
    /// heartbeats: `null` while it sends none.
    fn report_lag(&mut self) {
        let frame = lag_frame(&self.stats).to_string();
        for connection in self.conns.values_mut() {
            connection.send_text(frame.clone());
        }
    }

    /// Handles one connection's request.
    async fn request(&mut self, request: Request) {
        match request {
            Request::Connected {
                conn,
                out,
                close,
                held,
            } => {
                self.conns.insert(conn, Conn::new(out, close, held));
            }
            Request::Subscribe(subscribe) => {
                let Subscribe {
                    conn,
                    id,
                    query,
                    shape,
                } = *subscribe;
                self.subscribe(conn, id, query, shape).await
            }
            Request::Unsubscribe { conn, id } => self.unsubscribe(conn, id).await,
            Request::Disconnected { conn } => self.disconnected(conn).await,
        }
    }

    /// Ends the connection's subscription `id`. A registration still in
    /// flight for the id is not wanted either: `Registered` then finds no
    /// owner and unregisters it, and the id is free to subscribe again.
    async fn unsubscribe(&mut self, conn: u64, id: u64) {
        let Some(connection) = self.conns.get_mut(&conn) else {
            return;
        };
        let sub = connection.subs.remove(&id);
        let registering = connection.registering.remove(&id);
        connection.note_held();
        if let Some(sub) = sub {
            self.forget(sub);
            let _ = self.commands.send(Command::Unregister(sub)).await;
        }
        if let Some(token) = registering {
            self.by_token.remove(&token);
        }
    }

    /// Ends every subscription of a connection that went away, and lets
    /// go of its registrations in flight.
    async fn disconnected(&mut self, conn: u64) {
        let Some(gone) = self.conns.remove(&conn) else {
            return;
        };
        let subs: Vec<SubId> = gone.subs.values().copied().collect();
        for sub in &subs {
            self.forget(*sub);
        }
        for token in gone.registering.values() {
            self.by_token.remove(token);
        }
        if !subs.is_empty() {
            let _ = self.commands.send(Command::UnregisterAll(subs)).await;
        }
    }

    /// Registers a connection's query with the engine, unless the id is
    /// taken or the connection holds its limit.
    async fn subscribe(
        &mut self,
        conn: u64,
        id: u64,
        query: MultiTableReadQuery,
        shape: Option<Shape>,
    ) {
        let Some(connection) = self.conns.get_mut(&conn) else {
            return;
        };
        if connection.subs.contains_key(&id) || connection.registering.contains_key(&id) {
            connection.send(refused(id, "that id is already subscribed"));
            return;
        }
        if self.per_connection > 0 && connection.holds() >= self.per_connection {
            let reason = format!(
                "the subscription limit of {} per connection is reached",
                self.per_connection
            );
            connection.send(refused(id, &reason));
            return;
        }
        let token = self.next_token;
        self.next_token += 1;
        connection.registering.insert(id, token);
        connection.note_held();
        self.by_token.insert(token, (conn, id, shape));
        let command = Command::Register {
            sink: self.sink,
            query,
            token,
        };
        if self.commands.send(command).await.is_err() {
            self.by_token.remove(&token);
            if let Some(connection) = self.conns.get_mut(&conn) {
                connection.registering.remove(&id);
                connection.note_held();
                connection.send(refused(id, "the engine is shutting down"));
            }
        }
    }

    /// Handles one engine event.
    async fn event(&mut self, event: Event) {
        match event {
            Event::Registered {
                token,
                sub,
                reads,
                updates,
            } => self.registered(token, sub, reads, &updates).await,
            Event::Hydrated(subs) => {
                for sub in subs {
                    if self.hydrating.remove(&sub) {
                        self.accept(sub).await;
                    }
                }
            }
            Event::Refused { sub, reason } => self.drop_sub(sub, &reason).await,
            Event::Capped { sub } => {
                self.drop_sub(sub, "the query has more rows than the engine follows")
                    .await;
            }
            Event::Committed { updates, .. } | Event::Landed { updates } => self.changed(&updates),
            Event::Heavy { sub, table, rows } => {
                if let Some((conn, id)) = self.by_sub.get(&sub) {
                    log_warn!("s2s connection {conn}: query {id} holds {rows} rows of {table}");
                }
            }
            Event::Heartbeat(n) => self.stats.freshness.applied(n),
        }
    }

    /// The engine registered the query of `token` as `sub`. When the
    /// connection went away, or unsubscribed the id, while the
    /// registration was in flight, the subscription is not wanted and is
    /// unregistered. A twin's rows come with the registration, in
    /// `updates`, and seed a rows-mode shape's snapshot.
    async fn registered(&mut self, token: u64, sub: SubId, reads: usize, updates: &[Delta]) {
        let Some((conn, id, shape)) = self.by_token.remove(&token) else {
            let _ = self.commands.send(Command::Unregister(sub)).await;
            return;
        };
        let Some(connection) = self.conns.get_mut(&conn) else {
            let _ = self.commands.send(Command::Unregister(sub)).await;
            return;
        };
        connection.registering.remove(&id);
        connection.subs.insert(id, sub);
        connection.note_held();
        self.by_sub.insert(sub, (conn, id));
        if let Some(mut shape) = shape {
            for delta in updates {
                if delta.targets().any(|target| target.sub == sub) {
                    shape.absorb(&delta.table, &delta.op);
                }
            }
            self.shapes.insert(sub, shape);
        }
        if reads == 0 {
            self.accept(sub).await;
        } else {
            self.hydrating.insert(sub);
        }
    }

    /// `accepted`, with the hydrated rows for a rows-mode subscription.
    /// A table that left the catalog meanwhile cannot be served: the
    /// subscription is refused, and ended on the engine side too.
    async fn accept(&mut self, sub: SubId) {
        let Some((conn, id)) = self.by_sub.get(&sub).copied() else {
            return;
        };
        let Some(connection) = self.conns.get_mut(&conn) else {
            return;
        };
        let Some(shape) = self.shapes.get_mut(&sub) else {
            connection.send(accepted(id));
            return;
        };
        let catalog = self.catalog.load();
        let table_names: Vec<String> = shape
            .nodes
            .iter()
            .map(|node| node.table.as_str().to_owned())
            .collect();
        let mut nodes = Vec::with_capacity(shape.nodes.len());
        for node in shape.nodes.iter_mut() {
            let Some(table) = catalog.table(node.table.as_str()) else {
                self.drop_sub(sub, "a table left the catalog").await;
                return;
            };
            let rows: Vec<Json> = node
                .snapshot
                .drain()
                .filter_map(|(_, row)| row)
                .map(|row| wire::row_json(&row, table))
                .collect();
            nodes.push(json!({
                "table": table.name.as_str(),
                "key": table.pkey.iter().map(|c| c.as_str()).collect::<Vec<_>>(),
                "parent": node.parent.map(|p| table_names[p].clone()),
                "parent_column": node.parent_column,
                "child_column": node.child_column,
                "inner": node.inner,
                "rows": rows,
            }));
        }
        let (columns, names) = match &shape.projection {
            Projection::All => (Json::Null, Json::Null),
            Projection::Count { name } => (Json::Null, json!([name])),
            Projection::Columns(columns) => (
                Json::Array(
                    columns
                        .iter()
                        .map(|p| json!([p.table.as_str(), p.column.as_str()]))
                        .collect(),
                ),
                Json::Array(
                    columns
                        .iter()
                        .map(|p| Json::from(p.name.as_str()))
                        .collect(),
                ),
            ),
        };
        let order: Vec<Json> = shape
            .order
            .iter()
            .map(|clause| {
                json!([
                    clause.column.as_str(),
                    match clause.direction {
                        Order::ASC => "asc",
                        Order::DESC => "desc",
                    }
                ])
            })
            .collect();
        let limit = (shape.limit != u32::MAX).then_some(shape.limit);
        connection.send(json!(["accepted", {
            "id": id,
            "nodes": nodes,
            "columns": columns,
            "names": names,
            "count": matches!(shape.projection, Projection::Count { .. }),
            "order": order,
            "limit": limit,
            "offset": shape.offset,
        }]));
    }

    /// Tells each connection what the deltas did to its subscriptions:
    /// rows-mode ones get the rows per table, the rest a touch.
    /// Every row is serialized once, for the first rows-mode
    /// subscription it reaches, and spliced into every frame that
    /// carries it.
    ///
    /// A subscription still hydrating is not told: the backend reads
    /// only after `accepted`, which carries the change (rows mode) or
    /// lands after it (touch mode). For a rows-mode subscription, per
    /// table in order of appearance, the last operation on a key wins: a
    /// row that left and came back is a put, one that came and went is
    /// a delete, and no key is in both lists. A row of a table that left
    /// the catalog mid-flight cannot be kept by the backend, so it goes
    /// out as a delete.
    fn changed(&mut self, updates: &[Delta]) {
        let mut touched: HashMap<u64, HashSet<u64>> = HashMap::new();
        let mut deltas: HashMap<SubId, Changes> = HashMap::new();
        let mut images: Vec<Box<RawValue>> = Vec::new();
        let catalog = self.catalog.load();
        for delta in updates {
            let table = catalog.table(delta.table.as_str());
            let mut image: Option<Option<usize>> = None;
            for target in delta.targets() {
                let Some((conn, id)) = self.by_sub.get(&target.sub) else {
                    continue;
                };
                if self.hydrating.contains(&target.sub) {
                    if let Some(shape) = self.shapes.get_mut(&target.sub) {
                        shape.absorb(&delta.table, &delta.op);
                    }
                    continue;
                }
                if !self.shapes.contains_key(&target.sub) {
                    touched.entry(*conn).or_default().insert(*id);
                    continue;
                }
                let keys = table_keys(deltas.entry(target.sub).or_default(), &delta.table);
                match (&delta.op, table) {
                    (DataFrameOperation::Add(key, row), Some(table)) => {
                        let image = *image.get_or_insert_with(|| {
                            serialized(&wire::row_json(row, table)).map(|raw| {
                                images.push(raw);
                                images.len() - 1
                            })
                        });
                        keys.insert(key.clone(), image);
                    }
                    (DataFrameOperation::Delete(key, _) | DataFrameOperation::Add(key, _), _) => {
                        keys.insert(key.clone(), None);
                    }
                }
            }
        }
        for (sub, changes) in deltas {
            if let Some((conn, id)) = self.by_sub.get(&sub)
                && let Some(connection) = self.conns.get_mut(conn)
                && let Some(text) = delta_frame(*id, &changes, &images)
            {
                connection.send_text(text);
            }
        }
        for (conn, ids) in touched {
            if let Some(connection) = self.conns.get_mut(&conn) {
                let ids: Vec<u64> = ids.into_iter().collect();
                connection.send(json!(["touched", {"ids": ids}]));
            }
        }
    }

    /// The engine stopped following a subscription, or this task cannot
    /// serve it: the backend must stop trusting its cache for it, and
    /// the engine must stop routing to it. The engine's own `Refused`
    /// has already unregistered the subscription, and the extra
    /// unregister is a no-op (ids are never reused); `Capped` and this
    /// task's own refusals leave it registered, and need it.
    async fn drop_sub(&mut self, sub: SubId, reason: &str) {
        if let Some((conn, id)) = self.by_sub.get(&sub).copied()
            && let Some(connection) = self.conns.get_mut(&conn)
        {
            connection.subs.remove(&id);
            connection.note_held();
            connection.send(refused(id, reason));
        }
        self.forget(sub);
        let _ = self.commands.send(Command::Unregister(sub)).await;
    }

    /// Drops the task's own records of `sub`.
    fn forget(&mut self, sub: SubId) {
        self.by_sub.remove(&sub);
        self.shapes.remove(&sub);
        self.hydrating.remove(&sub);
    }
}

/// One table's keys in a subscription's changes, added in order of
/// appearance the first time the table is named.
fn table_keys<'c>(
    changes: &'c mut Changes,
    table: &TableName,
) -> &'c mut HashMap<DataFrameKey, Option<usize>> {
    let index = match changes.iter().position(|(named, _)| named == table) {
        Some(index) => index,
        None => {
            changes.push((table.clone(), HashMap::new()));
            changes.len() - 1
        }
    };
    &mut changes[index].1
}

/// A `delta` frame for subscription `id`, its put rows spliced in from
/// `images`, the transaction's serialized rows.
fn delta_frame(id: u64, changes: &Changes, images: &[Box<RawValue>]) -> Option<String> {
    let changes: Vec<TableChanges> = changes
        .iter()
        .map(|(table, keys)| {
            let mut put = Vec::new();
            let mut del = Vec::new();
            for (key, row) in keys {
                match row {
                    Some(index) => put.push(&*images[*index]),
                    None => del.push(wire::key_json(key)),
                }
            }
            TableChanges {
                table: table.as_str(),
                put,
                del,
            }
        })
        .collect();
    serde_json::to_string(&("delta", DeltaBody { id, changes })).ok()
}

/// A row image as JSON text, written once and spliced into every frame
/// that carries it. A JSON value always serializes; a row that somehow
/// did not would be sent as a delete, which the backend refills.
fn serialized(image: &Json) -> Option<Box<RawValue>> {
    serde_json::value::to_raw_value(image).ok()
}

/// An `accepted` with no rows.
fn accepted(id: u64) -> Json {
    json!(["accepted", {"id": id}])
}

/// A `refused` for the backend's `id`.
fn refused(id: u64, reason: &str) -> Json {
    json!(["refused", {"id": id, "reason": reason}])
}

/// A frame from the connection's own handler. `false` when the queue is
/// full, which means the backend is not reading: the handler then ends
/// the connection, as the task would, rather than drop the frame and
/// leave a subscription unanswered.
fn send(out: &Outgoing, message: Json) -> bool {
    out.push(message.to_string())
}

/// One connection, from `hello` to close, over any transport: `inbound`
/// is the backend's text frames, and `out` the queue the transport's
/// writer drains. Before it greets, the session reads which database
/// the engine streams; when that cannot be read it ends at once, and
/// the transport closes the connection without a `hello`. Whatever ends
/// the session (the inbound frames ending, the task asking to close, a
/// panic in a frame's handling) the task hears the connection is gone,
/// or its subscriptions would outlive it. The transport flushes and
/// closes once this returns.
pub async fn session<S>(context: &Context, inbound: S, out: Outgoing)
where
    S: Stream<Item = String> + Unpin,
{
    let hub = &context.hub;
    let database = match database(context).await {
        Ok(database) => database,
        Err(error) => {
            refusal(
                &hub.unidentified,
                &format!("the database it streams cannot be read: {error}"),
            );
            return;
        }
    };
    let conn = hub.next_conn.fetch_add(1, Ordering::Relaxed);
    let (close, closed) = oneshot::channel::<&'static str>();
    let held = Arc::new(AtomicUsize::new(0));
    let limit = context.limits.per_connection;
    send(
        &out,
        json!(["hello", {
            "protocol": PROTOCOL_VERSION,
            "max_subscriptions": (limit > 0).then_some(limit),
            "database": database,
        }]),
    );
    send(&out, lag_frame(&context.stats));
    if hub
        .requests
        .send(Request::Connected {
            conn,
            out: out.clone(),
            close,
            held: Arc::clone(&held),
        })
        .await
        .is_err()
    {
        return;
    }
    log_info!("s2s connection {conn} open");
    let serving = serve(conn, inbound, closed, &out, &held, context);
    if AssertUnwindSafe(serving).catch_unwind().await.is_err() {
        log_warn!("s2s connection {conn}: a frame's handling panicked");
    }
    let _ = hub.requests.send(Request::Disconnected { conn }).await;
    log_info!("s2s connection {conn} closed");
}

/// The `lag` frame: how far the engine's applied changes trail the
/// database, `null` before the first heartbeat is sent.
fn lag_frame(stats: &Stats) -> Json {
    let ms = stats
        .freshness
        .lag()
        .map(|lag| u64::try_from(lag.as_millis()).unwrap_or(u64::MAX));
    json!(["lag", {"ms": ms}])
}

/// Reads frames until the backend closes, the task asks to close, or
/// the task is gone. The task enforces the subscription limit; checking
/// it here too keeps a connection at its limit from having more queries
/// planned, which reads Postgres, only to be refused.
async fn serve<S>(
    conn: u64,
    mut inbound: S,
    mut closed: oneshot::Receiver<&'static str>,
    out: &Outgoing,
    held: &AtomicUsize,
    context: &Context,
) where
    S: Stream<Item = String> + Unpin,
{
    let hub = &context.hub;
    let limit = context.limits.per_connection;
    let unread = || log_warn!("s2s connection {conn} closed: the backend is not reading its frames");
    loop {
        let frame = tokio::select! {
            frame = inbound.next() => frame,
            reason = &mut closed => {
                if let Ok(reason) = reason {
                    log_warn!("s2s connection {conn} closed: {reason}");
                }
                return;
            }
        };
        let Some(text) = frame else {
            return;
        };
        match upstream(&text) {
            Ok(Frame::Ping) => {
                if !send(out, json!(["pong"])) {
                    unread();
                    return;
                }
            }
            Ok(Frame::Unsubscribe { id }) => {
                if hub
                    .requests
                    .send(Request::Unsubscribe { conn, id })
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Ok(Frame::Subscribe {
                id,
                sql,
                binds,
                rows,
            }) => {
                let prepared = if limit > 0 && held.load(Ordering::Acquire) >= limit {
                    Err(format!(
                        "the subscription limit of {limit} per connection is reached"
                    ))
                } else {
                    prepare(context, conn, id, &sql, &binds, rows).await
                };
                let request = match prepared {
                    Ok(request) => request,
                    Err(reason) => {
                        log_debug!("s2s connection {conn}: query {id} refused: {reason}");
                        if !send(out, refused(id, &reason)) {
                            unread();
                            return;
                        }
                        continue;
                    }
                };
                if hub.requests.send(request).await.is_err() {
                    return;
                }
            }
            Err(Rejected { id: Some(id), reason }) => {
                log_warn!("s2s connection {conn}: query {id} refused: {reason}");
                if !send(out, refused(id, &reason)) {
                    unread();
                    return;
                }
            }
            Err(Rejected { id: None, reason }) => {
                log_warn!("s2s connection {conn}: {reason}");
                if !send(out, json!(["error", {"reason": reason}])) {
                    unread();
                    return;
                }
            }
        }
    }
}

/// Parses the SQL against the catalog and plans the query the way a
/// Zero query is planned (which side of each join is read whole, how a
/// page is read), so the engine registers it ready to run. A bind of a
/// type the catalog has not seen may be an enum created since it was
/// loaded: those are looked up first ([`learn_enums`]). A query larger
/// than the engine should take on its own thread is refused before it
/// is planned ([`within_limits`]).
async fn prepare(
    context: &Context,
    conn: u64,
    id: u64,
    sql: &str,
    binds: &[BindValue],
    rows: bool,
) -> Result<Request, String> {
    let mut catalog = context.catalog.load();
    let unknown: Vec<u32> = binds
        .iter()
        .map(|bind| bind.type_oid)
        .filter(|oid| !decodes(*oid) && !catalog.is_enum_oid(*oid))
        .collect();
    if !unknown.is_empty() && learn_enums(context, &unknown).await {
        catalog = context.catalog.load();
    }
    let select = parse_select(sql, binds, &catalog).map_err(|error| error.message)?;
    within_limits(
        &select.query,
        context.limits.max_disjuncts,
        context.limits.max_list_values,
    )?;
    let shape = rows.then(|| Shape::new(&select.query, select.projection, select.limit, select.offset));
    let translated = Translated {
        query: select.query,
        hidden: HashSet::new(),
        uncursored: None,
    };
    let planned = plan::plan(
        sql,
        translated,
        context.policy,
        &context.plans,
        &*context.storage,
    )
    .await?;
    Ok(Request::Subscribe(Box::new(Subscribe {
        conn,
        id,
        query: planned.query,
        shape,
    })))
}

/// Which database the engine streams, for `hello`: the cluster's
/// `system_identifier` and the database's name, as a backend reads them
/// through its own connection. A backend whose reads go to another
/// database must not take this engine's rows for its own, and checks.
/// Read once and kept, since it cannot change while the process runs.
/// A read that fails is an error, and the connection is closed without
/// a `hello`: a `null` would have the backend take the engine on trust,
/// so it reconnects instead, reading Postgres meanwhile.
async fn database(context: &Context) -> Result<Json, String> {
    context
        .hub
        .database
        .get_or_try_init(|| async {
            let rows = context
                .storage
                .simple_query(DATABASE_SQL)
                .await
                .map_err(|error| error.to_string())?;
            match rows.first().map(|row| (row.get(0), row.get(1))) {
                Some((Some(system), Some(name))) => Ok(json!({"system": system, "name": name})),
                _ => Err("the identity query returned no row".to_owned()),
            }
        })
        .await
        .cloned()
}

/// The identity [`database`] announces.
const DATABASE_SQL: &str =
    "SELECT system_identifier::text, current_database()::text FROM pg_control_system()";

/// Refuses a query whose filters would normalize to more than
/// `max_disjuncts` disjuncts, or whose `IN` and `NOT IN` lists hold more
/// than `max_list_values` values; a limit of 0 is none. The engine does
/// both on the thread every subscriber's changes go through, so one such
/// query would hold them all up, and enough `OR`s would exhaust memory.
fn within_limits(
    query: &MultiTableReadQuery,
    max_disjuncts: u64,
    max_list_values: usize,
) -> Result<(), String> {
    let (disjuncts, values) = query_size(query);
    if max_disjuncts > 0 && disjuncts > max_disjuncts {
        return Err(format!(
            "the filters expand to {disjuncts} alternatives, more than the {max_disjuncts} the engine follows"
        ));
    }
    if max_list_values > 0 && values > max_list_values {
        return Err(format!(
            "the IN lists hold {values} values, more than the {max_list_values} the engine follows"
        ));
    }
    Ok(())
}

/// How large a query is to the engine: the disjuncts its tables'
/// filters normalize to, summed, and the values its `IN` and `NOT IN`
/// lists hold. Counted from the filter trees without expanding them, so
/// a filter whose expansion would not fit in memory is measured cheaply;
/// the disjuncts saturate.
fn query_size(query: &MultiTableReadQuery) -> (u64, usize) {
    let (mut disjuncts, mut values) = filter_size(&query.main_table.filter);
    for join in &query.joins {
        let (more_disjuncts, more_values) = query_size(&join.sub);
        disjuncts = disjuncts.saturating_add(more_disjuncts);
        values = values.saturating_add(more_values);
    }
    (disjuncts, values)
}

/// [`query_size`] for one filter: an `AND` multiplies its parts'
/// disjuncts, an `OR` adds them, as normalizing to an `OR` of `AND`s
/// does.
fn filter_size(filter: &Where) -> (u64, usize) {
    match filter {
        Where::Condition(condition) => {
            let values = match (&condition.comparison_operator, &condition.value) {
                (ComparisonOperator::IN | ComparisonOperator::NOT_IN, Value::List(items)) => {
                    items.len()
                }
                _ => 0,
            };
            (1, values)
        }
        Where::AND(parts) => parts.iter().map(filter_size).fold((1, 0), |(d, v), (pd, pv)| {
            (d.saturating_mul(pd), v.saturating_add(pv))
        }),
        Where::OR(parts) => parts.iter().map(filter_size).fold((0, 0), |(d, v), (pd, pv)| {
            (d.saturating_add(pd), v.saturating_add(pv))
        }),
    }
}

/// Looks `oids` up among the database's enum types and teaches the
/// catalog the ones that are (with their array types), so a bind of an
/// enum created after the catalog was loaded reads as its label. Whether
/// any was learned; a failed lookup learns nothing, and the bind is then
/// refused as a type the engine does not carry.
async fn learn_enums(context: &Context, oids: &[u32]) -> bool {
    let list: Vec<String> = oids.iter().map(u32::to_string).collect();
    let list = list.join(", ");
    let sql = format!(
        "SELECT oid::text, typarray::text FROM pg_type WHERE typtype = 'e' AND (oid IN ({list}) OR typarray IN ({list}))"
    );
    let Ok(rows) = context.storage.simple_query(&sql).await else {
        return false;
    };
    let learned: Vec<u32> = rows
        .iter()
        .flat_map(|row| [row.get(0), row.get(1)])
        .flatten()
        .filter_map(|text| text.parse::<u32>().ok())
        .filter(|oid| *oid != 0)
        .collect();
    if learned.is_empty() {
        return false;
    }
    context
        .catalog
        .update(|catalog| with_enums(catalog, learned.iter().copied()));
    true
}

/// One upstream frame.
#[derive(Debug)]
enum Frame {
    Ping,
    Subscribe {
        id: u64,
        sql: String,
        binds: Vec<BindValue>,
        rows: bool,
    },
    Unsubscribe {
        id: u64,
    },
}

/// An upstream frame that could not be read: the `id` it carried, when
/// one could be read, and why.
#[derive(Debug)]
struct Rejected {
    id: Option<u64>,
    reason: String,
}

impl Rejected {
    /// A rejection that names no subscription.
    fn frame(reason: impl Into<String>) -> Rejected {
        Rejected {
            id: None,
            reason: reason.into(),
        }
    }
}

/// Reads one upstream frame.
fn upstream(text: &str) -> Result<Frame, Rejected> {
    let frame: Json = serde_json::from_str(text)
        .map_err(|error| Rejected::frame(format!("not JSON: {error}")))?;
    let (tag, body) = match frame.as_array().map(Vec::as_slice) {
        Some([Json::String(tag), body]) => (tag.as_str(), body),
        Some([Json::String(tag)]) => (tag.as_str(), &Json::Null),
        _ => return Err(Rejected::frame("a frame is a [tag, body] array")),
    };
    let id = |body: &Json| {
        body.get("id")
            .and_then(Json::as_u64)
            .ok_or_else(|| Rejected::frame("`id` must be a non-negative integer"))
    };
    match tag {
        "ping" => Ok(Frame::Ping),
        "subscribe" => {
            let id = id(body)?;
            subscription(body)
                .map(|(sql, binds, rows)| Frame::Subscribe {
                    id,
                    sql,
                    binds,
                    rows,
                })
                .map_err(|reason| Rejected {
                    id: Some(id),
                    reason,
                })
        }
        "unsubscribe" => Ok(Frame::Unsubscribe { id: id(body)? }),
        other => Err(Rejected::frame(format!("unknown tag `{other}`"))),
    }
}

/// A `subscribe` body past its `id`: the SQL, the binds, and whether
/// rows were asked for.
fn subscription(body: &Json) -> Result<(String, Vec<BindValue>, bool), String> {
    let sql = body
        .get("sql")
        .and_then(Json::as_str)
        .ok_or("`sql` must be a string")?
        .to_string();
    let binds = body
        .get("binds")
        .map(|binds| binds.as_array().ok_or("`binds` must be an array"))
        .transpose()?
        .map(|binds| binds.iter().map(bind).collect::<Result<Vec<_>, _>>())
        .transpose()?
        .unwrap_or_default();
    let rows = body.get("rows").and_then(Json::as_bool).unwrap_or(false);
    Ok((sql, binds, rows))
}

/// One bind: its type OID and its base64 bytes, `null` for `NULL`.
fn bind(value: &Json) -> Result<BindValue, String> {
    let type_oid = value
        .get("oid")
        .and_then(Json::as_u64)
        .and_then(|oid| u32::try_from(oid).ok())
        .ok_or("a bind's `oid` must be a 32-bit integer")?;
    let bytes = match value.get("bytes") {
        None | Some(Json::Null) => None,
        Some(Json::String(text)) => Some(
            BASE64
                .decode(text)
                .map_err(|_| "a bind's `bytes` must be base64")?,
        ),
        Some(_) => return Err("a bind's `bytes` must be a base64 string or null".to_string()),
    };
    Ok(BindValue { type_oid, bytes })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::{Audience, QueryPart, Subs};
    use crate::model::{Catalog, ColumnName, DbColumn, DbTable, Value, ValueType};

    /// One table, `notes (id, name)`.
    fn test_catalog() -> Catalog {
        Catalog::new([DbTable::new(
            "notes",
            ["id"],
            vec![
                DbColumn::new("id", ValueType::Int),
                DbColumn::new("name", ValueType::String),
            ],
        )])
    }

    /// A task over one table, `notes (id, name)`, with connection 1 open:
    /// the task, the engine's commands, and connection 1's frames.
    fn task(
        per_connection: usize,
    ) -> (
        Task,
        mpsc::Receiver<Command<MultiTableReadQuery>>,
        mpsc::Receiver<String>,
    ) {
        let catalog = test_catalog();
        let (commands, commands_rx) = mpsc::channel(64);
        let mut task = Task {
            catalog: Arc::new(CatalogHandle::new(catalog)),
            sink: 0,
            commands,
            conns: HashMap::new(),
            by_token: HashMap::new(),
            by_sub: HashMap::new(),
            shapes: HashMap::new(),
            hydrating: HashSet::new(),
            next_token: 1,
            per_connection,
            stats: Stats::shared_with_metric_prefix("test".to_owned()),
        };
        let frames = connect(&mut task, 1);
        (task, commands_rx, frames)
    }

    /// Opens connection `conn` on the task: its frames.
    fn connect(task: &mut Task, conn: u64) -> mpsc::Receiver<String> {
        let (close, _closed) = oneshot::channel();
        let (out, frames) = Outgoing::new();
        task.conns
            .insert(conn, Conn::new(out, close, Arc::new(AtomicUsize::new(0))));
        frames
    }

    /// Connection `conn` subscribes `SELECT * FROM notes` under `id`.
    async fn subscribe(task: &mut Task, conn: u64, id: u64, rows: bool) {
        let select =
            parse_select("SELECT * FROM notes", &[], &task.catalog.load()).expect("parses");
        let shape = rows.then(|| Shape::new(&select.query, select.projection, select.limit, select.offset));
        task.subscribe(conn, id, select.query, shape).await;
    }

    /// The tokens of the registrations sent to the engine so far.
    fn registered(commands: &mut mpsc::Receiver<Command<MultiTableReadQuery>>) -> Vec<u64> {
        let mut tokens = Vec::new();
        while let Ok(command) = commands.try_recv() {
            if let Command::Register { token, .. } = command {
                tokens.push(token);
            }
        }
        tokens
    }

    /// The frames sent to a connection so far.
    fn frames(out: &mut mpsc::Receiver<String>) -> Vec<Json> {
        let mut frames = Vec::new();
        while let Ok(text) = out.try_recv() {
            frames.push(serde_json::from_str(&text).expect("json"));
        }
        frames
    }

    /// The engine registered the subscription of `token` as `sub`, with
    /// nothing to read.
    async fn register(task: &mut Task, token: u64, sub: u64) {
        task.event(Event::Registered {
            token,
            sub: SubId(sub),
            updates: Vec::new(),
            reads: 0,
        })
        .await;
    }

    /// A delta adding note `id` named `name`, aimed at `subs`.
    fn put(subs: &[u64], id: i64, name: &str) -> Delta {
        let key = DataFrameKey::from(HashMap::from([(ColumnName::from("id"), Value::Int(id))]));
        let row = DataFrameRow::from(HashMap::from([
            (ColumnName::from("id"), Value::Int(id)),
            (ColumnName::from("name"), Value::from(name)),
        ]));
        Delta {
            table: TableName::from("notes"),
            op: DataFrameOperation::Add(key, row),
            audiences: vec![Audience {
                part: QueryPart::main(),
                subs: Subs::Many(subs.iter().map(|sub| SubId(*sub)).collect()),
            }],
        }
    }

    /// A connection holds as many subscriptions as it is allowed, those
    /// the engine is still registering counted, and one more is refused
    /// until one goes; other connections have their own allowance.
    #[tokio::test]
    async fn a_connection_holds_at_most_its_subscription_limit() {
        let (mut task, mut commands, mut out) = task(2);
        subscribe(&mut task, 1, 1, false).await;
        subscribe(&mut task, 1, 2, false).await;
        subscribe(&mut task, 1, 3, false).await;
        assert_eq!(registered(&mut commands), vec![1, 2]);
        assert_eq!(
            frames(&mut out),
            vec![
                json!(["refused", {"id": 3, "reason": "the subscription limit of 2 per connection is reached"}])
            ]
        );

        register(&mut task, 1, 10).await;
        subscribe(&mut task, 1, 4, false).await;
        assert!(
            registered(&mut commands).is_empty(),
            "a registered one still counts"
        );

        task.request(Request::Unsubscribe { conn: 1, id: 1 }).await;
        subscribe(&mut task, 1, 5, false).await;
        assert_eq!(registered(&mut commands), vec![3]);

        let mut other_out = connect(&mut task, 2);
        subscribe(&mut task, 2, 1, false).await;
        assert_eq!(registered(&mut commands), vec![4]);
        assert!(frames(&mut other_out).is_empty());
    }

    /// With the limit at zero a connection subscribes without one.
    #[tokio::test]
    async fn a_zero_limit_lifts_it() {
        let (mut task, mut commands, mut out) = task(0);
        for id in 1..=5 {
            subscribe(&mut task, 1, id, false).await;
        }
        assert_eq!(registered(&mut commands).len(), 5);
        assert!(frames(&mut out).is_empty());
    }

    /// A row two rows-mode subscriptions share reaches both, and a row
    /// of one transaction never stands in for another's.
    #[tokio::test]
    async fn each_subscription_gets_the_rows_of_its_own_deltas() {
        let (mut task, _commands, mut out) = task(0);
        subscribe(&mut task, 1, 1, true).await;
        subscribe(&mut task, 1, 2, true).await;
        register(&mut task, 1, 10).await;
        register(&mut task, 2, 20).await;
        frames(&mut out);

        task.changed(&[put(&[10, 20], 1, "one"), put(&[10], 2, "two")]);
        let mut deltas: Vec<Json> = frames(&mut out);
        deltas.sort_by_key(|frame| frame[1]["id"].as_u64());
        let puts = |frame: &Json| -> Vec<(i64, String)> {
            let mut rows: Vec<(i64, String)> = frame[1]["changes"][0]["put"]
                .as_array()
                .expect("puts")
                .iter()
                .map(|row| {
                    (
                        row["id"].as_i64().unwrap(),
                        row["name"].as_str().unwrap().to_owned(),
                    )
                })
                .collect();
            rows.sort();
            rows
        };
        assert_eq!(deltas.len(), 2, "{deltas:?}");
        assert_eq!(
            puts(&deltas[0]),
            vec![(1, "one".to_owned()), (2, "two".to_owned())]
        );
        assert_eq!(puts(&deltas[1]), vec![(1, "one".to_owned())]);
    }

    /// Every connection hears the engine's lag from its heartbeats:
    /// `null` before the first, then the age of the first while none is
    /// applied, then the age of the newest one the engine's events said
    /// was applied.
    #[tokio::test]
    async fn every_connection_hears_the_lag() {
        let (mut task, _commands, mut out) = task(0);
        let mut other = connect(&mut task, 2);
        task.report_lag();
        assert_eq!(frames(&mut out), vec![json!(["lag", {"ms": null}])]);
        task.stats
            .freshness
            .sent(1, std::time::Instant::now() - Duration::from_millis(1500));
        task.report_lag();
        for frames in [frames(&mut out), frames(&mut other)] {
            let ms = frames[frames.len() - 1][1]["ms"].as_u64().expect("a lag");
            assert!((1500..2500).contains(&ms), "{ms}");
        }
        task.stats.freshness.sent(2, std::time::Instant::now());
        task.event(Event::Heartbeat(2)).await;
        task.report_lag();
        let ms = frames(&mut out)[0][1]["ms"].as_u64().expect("a lag");
        assert!(ms < 500, "caught up: {ms}");
    }

    /// A queue takes frames until either bound is reached, always takes
    /// one frame when empty however large, and frees a frame's bytes
    /// once the writer has sent it.
    #[test]
    fn the_outgoing_queue_is_bounded_in_bytes() {
        let (out, mut frames) = Outgoing::new();
        let big = "x".repeat(OUTGOING_BYTES + 1);
        assert!(out.push(big.clone()), "an empty queue takes any frame");
        assert!(!out.push("y".to_owned()), "past the byte bound");
        let sent = frames.try_recv().expect("the big frame");
        out.drained().sent(sent.len());
        assert!(out.push("y".to_owned()), "room again once it went out");
        assert_eq!(out.queued.load(Ordering::Acquire), 1);
    }

    /// The writer's byte count keeps no sender alive: once every
    /// `Outgoing` is gone the writer's queue ends, so a closing
    /// connection flushes and sends its close frame without waiting.
    #[test]
    fn the_writer_queue_closes_with_its_senders() {
        let (out, mut frames) = Outgoing::new();
        let drained = out.drained();
        assert!(out.push("x".to_owned()));
        drop(out);
        assert_eq!(frames.try_recv().as_deref(), Ok("x"));
        drained.sent(1);
        assert!(matches!(
            frames.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }

    /// A subscription touched by several deltas of one transaction is
    /// named once, and the connection's handler sees what it holds.
    #[tokio::test]
    async fn a_touch_names_each_subscription_once() {
        let (mut task, _commands, mut out) = task(0);
        subscribe(&mut task, 1, 1, false).await;
        assert_eq!(task.conns[&1].held.load(Ordering::Acquire), 1);
        register(&mut task, 1, 10).await;
        assert_eq!(task.conns[&1].held.load(Ordering::Acquire), 1);
        frames(&mut out);
        task.changed(&[put(&[10], 1, "one"), put(&[10], 2, "two"), put(&[10], 3, "three")]);
        assert_eq!(frames(&mut out), vec![json!(["touched", {"ids": [1]}])]);
        task.request(Request::Unsubscribe { conn: 1, id: 1 }).await;
        assert_eq!(task.conns[&1].held.load(Ordering::Acquire), 0);
    }

    /// An id unsubscribed while still registering frees its place at
    /// once, and its late registration is unregistered.
    #[tokio::test]
    async fn an_id_unsubscribed_while_registering_is_let_go() {
        let (mut task, mut commands, _out) = task(1);
        subscribe(&mut task, 1, 1, false).await;
        assert_eq!(registered(&mut commands), vec![1]);
        task.request(Request::Unsubscribe { conn: 1, id: 1 }).await;
        subscribe(&mut task, 1, 2, false).await;
        assert_eq!(registered(&mut commands), vec![2], "the place came back");
        register(&mut task, 1, 10).await;
        assert!(matches!(
            commands.try_recv(),
            Ok(Command::Unregister(SubId(10)))
        ));
        assert!(task.by_sub.is_empty());
    }

    /// The first warning passes, the next ones within the period are
    /// counted, and the first after it says how many were held back.
    #[test]
    fn a_throttled_warning_passes_once_per_period() {
        let throttle = Throttle::new(Duration::from_secs(60));
        let start = Instant::now();
        assert_eq!(throttle.pass(start), Some(0));
        assert_eq!(throttle.pass(start + Duration::from_secs(1)), None);
        assert_eq!(throttle.pass(start + Duration::from_secs(59)), None);
        assert_eq!(throttle.pass(start + Duration::from_secs(60)), Some(2));
        assert_eq!(throttle.pass(start + Duration::from_secs(61)), None);
        assert_eq!(throttle.pass(start + Duration::from_secs(200)), Some(1));
    }

    /// A query's size is counted from its filter trees as their expansion
    /// would be: thirty `AND`ed `OR` pairs are 2^30 disjuncts, measured
    /// without building one, and refused; an `IN` list counts its values.
    #[test]
    fn an_oversized_query_is_refused_before_the_engine_sees_it() {
        let catalog = test_catalog();
        let pairs = |k: usize| {
            let filter = (0..k)
                .map(|i| format!(r#"("notes"."id" = {i} OR "notes"."name" = 'n{i}')"#))
                .collect::<Vec<_>>()
                .join(" AND ");
            parse_select(&format!(r#"SELECT * FROM "notes" WHERE {filter}"#), &[], &catalog)
                .expect("parses")
                .query
        };
        assert_eq!(query_size(&pairs(3)), (8, 0));
        assert_eq!(query_size(&pairs(30)).0, 1 << 30);
        assert!(within_limits(&pairs(10), 1024, 0).is_ok());
        let refused = within_limits(&pairs(11), 1024, 0).expect_err("2048 disjuncts");
        assert!(refused.contains("2048 alternatives"), "{refused}");
        assert!(within_limits(&pairs(30), 0, 0).is_ok(), "0 lifts the limit");

        let list = (0..5).map(|i| i.to_string()).collect::<Vec<_>>().join(", ");
        let listed = parse_select(
            &format!(r#"SELECT * FROM "notes" WHERE "notes"."id" IN ({list}) OR "notes"."id" NOT IN ({list})"#),
            &[],
            &catalog,
        )
        .expect("parses")
        .query;
        assert_eq!(query_size(&listed), (2, 10));
        assert!(within_limits(&listed, 0, 10).is_ok());
        assert!(within_limits(&listed, 0, 9).is_err());
    }

    /// Frames read into requests, and a bad one says which id it was for
    /// when it can.
    #[test]
    fn frames_parse_into_requests() {
        let frame = upstream(
            r#"["subscribe", {"id": 7, "sql": "SELECT * FROM t WHERE a = $1", "binds": [{"oid": 25, "bytes": "T1BFTg=="}, {"oid": 20, "bytes": null}], "rows": true}]"#,
        )
        .expect("parses");
        match frame {
            Frame::Subscribe {
                id,
                sql,
                binds,
                rows,
            } => {
                assert_eq!(id, 7);
                assert_eq!(sql, "SELECT * FROM t WHERE a = $1");
                assert!(rows);
                assert_eq!(
                    binds,
                    vec![
                        BindValue {
                            type_oid: 25,
                            bytes: Some(b"OPEN".to_vec())
                        },
                        BindValue {
                            type_oid: 20,
                            bytes: None
                        }
                    ]
                );
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            upstream(r#"["unsubscribe", {"id": 2}]"#),
            Ok(Frame::Unsubscribe { id: 2 })
        ));
        assert!(matches!(upstream(r#"["ping"]"#), Ok(Frame::Ping)));
        assert!(upstream(r#"["subscribe", {"id": -1}]"#).is_err());
        assert!(upstream(r#"["nope", {}]"#).is_err());
        assert!(upstream("{}").is_err());
        assert!(matches!(
            upstream(
                r#"["subscribe", {"id": 1, "sql": "x", "binds": [{"oid": 25, "bytes": "%%"}]}]"#
            ),
            Err(Rejected { id: Some(1), .. })
        ));
        assert!(matches!(
            upstream(r#"["subscribe", {"id": 7, "sql": 5}]"#),
            Err(Rejected { id: Some(7), .. })
        ));
        assert!(matches!(
            upstream(r#"["subscribe", {"sql": "x"}]"#),
            Err(Rejected { id: None, .. })
        ));
    }
}
