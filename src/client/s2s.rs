//! The server-to-server endpoint: a backend subscribes the SQL its ORM
//! renders and is told when a transaction touched a subscribed query.
//!
//! A backend keeps its own copy of what it reads (a Diesel wrapper serves
//! repeated reads from a local cache). It does not want the rows from
//! here, Postgres has those; it wants to know *when* a cached result is
//! no longer current. So the contract is small: `subscribe` a query, get
//! `accepted` or `refused`, then a `touched` naming every subscription a
//! committed transaction changed the rows of, and `unsubscribe` when the
//! backend drops it.
//!
//! One task ([`run`]) owns every S2S subscription: it is one more
//! consumer of the engine's events beside the group threads, with its own
//! sink index, and maps the engine's subscription ids back to the
//! connection and the id the backend chose. Nothing of the Zero protocol
//! (client groups, pokes, mutations, the application server) is involved.
//!
//! # Protocol
//!
//! JSON text frames, each a two-element array of a tag and a body, on a
//! WebSocket at `{base}/s2s/v1`.
//!
//! Admission:
//! - With `STREAMGRES_S2S_TOKEN` set, the upgrade must carry
//!   `Authorization: Bearer <token>` and is answered 401 otherwise.
//!   Unset, any connection is taken, so the port belongs on a trusted
//!   network.
//! - At most `STREAMGRES_S2S_MAX_CONNECTIONS` connections are open at
//!   once; past that the upgrade is answered 503.
//! - A connection holds at most `STREAMGRES_S2S_CONNECTION_SUBSCRIPTIONS`
//!   subscriptions, those still registering included; a subscribe past
//!   that is refused. The two limits together bound what one engine
//!   follows for its backends.
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
//! - `["hello", {"protocol": 1, "max_subscriptions": n}]` first;
//!   `max_subscriptions` is the per-connection limit, `null` for none, so
//!   a client can stop short of it rather than be refused.
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
//!   `limit + offset` rows and the client skips `offset` of them. A
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
//!   is closed once its queue is full rather than buffered for.
//! - `["pong"]`

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::stream::SplitStream;
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde_json::{Value as Json, json};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};

use super::ast::Translated;
use super::connection::AppState;
use super::{plan, wire};
use crate::ivm::Delta;
use crate::log::{log_debug, log_info, log_warn};
use crate::model::{
    DataFrameKey, DataFrameOperation, DataFrameRow, MultiTableReadQuery, Order, OrderBy, SubId,
    TableName,
};
use crate::parser::{BindValue, Projection, parse_select};
use crate::sync::{CatalogHandle, Command, Event};

pub const PROTOCOL_VERSION: u32 = 1;

/// How many requests may wait for the task before a connection's reader
/// waits too.
const REQUEST_QUEUE: usize = 1024;

/// How many frames may wait for a connection's writer. A backend that
/// stops reading would otherwise have every result and delta buffered
/// for it without limit; past this the connection is closed instead.
const OUTGOING_QUEUE: usize = 4096;

/// What a connection asks of the task.
#[derive(Debug)]
pub enum Request {
    Connected {
        conn: u64,
        out: mpsc::Sender<String>,
        /// Fired by the task to close the connection, with the reason.
        close: oneshot::Sender<&'static str>,
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

/// Per table, what one transaction did to each key: its new image, or
/// `None` once deleted. One entry per key, so a row admitted and evicted
/// in one transaction is a delete, not a put the client would keep.
type Changes = Vec<(TableName, HashMap<DataFrameKey, Option<Json>>)>;

/// How often a refused upgrade of one kind is logged at most; the ones
/// in between are counted into the next line.
const REFUSAL_LOG_EVERY: Duration = Duration::from_secs(60);

/// The handle connections talk to the task through.
#[derive(Debug)]
pub struct Handle {
    requests: mpsc::Sender<Request>,
    next_conn: AtomicU64,
    /// Connections open now, against `max_connections` (0: no limit).
    open: Arc<AtomicUsize>,
    max_connections: usize,
    unauthorized: Throttle,
    full: Throttle,
}

impl Handle {
    pub fn new(max_connections: usize) -> (Handle, mpsc::Receiver<Request>) {
        let (requests, requests_rx) = mpsc::channel(REQUEST_QUEUE);
        (
            Handle {
                requests,
                next_conn: AtomicU64::new(1),
                open: Arc::new(AtomicUsize::new(0)),
                max_connections,
                unauthorized: Throttle::new(REFUSAL_LOG_EVERY),
                full: Throttle::new(REFUSAL_LOG_EVERY),
            },
            requests_rx,
        )
    }

    /// A place among the open connections, if one is free; it is given
    /// back when the [`Admitted`] drops.
    fn admit(&self) -> Option<Admitted> {
        let limit = self.max_connections;
        self.open
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |open| {
                (limit == 0 || open < limit).then_some(open + 1)
            })
            .ok()?;
        Some(Admitted(Arc::clone(&self.open)))
    }
}

/// One open connection's place, held for as long as the connection is.
struct Admitted(Arc<AtomicUsize>);

impl Drop for Admitted {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Lets a repeated warning through at most once per period. A backend
/// with the wrong token retries every second, and so can anyone who
/// reaches the port, so each refusal cannot be a log line.
#[derive(Debug)]
struct Throttle {
    every: Duration,
    next: Mutex<Option<Instant>>,
    quiet: AtomicU64,
}

impl Throttle {
    fn new(every: Duration) -> Throttle {
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
fn refusal(throttle: &Throttle, what: &str) {
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
    out: mpsc::Sender<String>,
    /// Closes the connection; taken once used.
    close: Option<oneshot::Sender<&'static str>>,
    /// The backend's ids and the engine's subscriptions they became.
    subs: HashMap<u64, SubId>,
}

impl Conn {
    /// Queues a frame for the writer. A full queue means the backend is
    /// not reading: the connection is closed rather than buffered for.
    fn send(&mut self, message: Json) {
        match self.out.try_send(message.to_string()) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                if let Some(close) = self.close.take() {
                    let _ = close.send("the backend is not reading its frames");
                }
            }
            Err(TrySendError::Closed(_)) => {}
        }
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
    /// offset` rows and the client skips and cuts.
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
    pub fn new(query: &MultiTableReadQuery, projection: Projection, offset: u32) -> Shape {
        let mut nodes = Vec::new();
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
            // The tree's limit is `limit + offset`, so it is never below
            // the offset: `parse_select` refuses a sum that would pass
            // `u32::MAX`, which also keeps it clear of the no-limit value.
            limit: if query.main_table.limit == u32::MAX {
                u32::MAX
            } else {
                query.main_table.limit - offset
            },
            offset,
        }
    }

    fn node_of(&mut self, table: &TableName) -> Option<&mut NodeShape> {
        self.nodes.iter_mut().find(|node| &node.table == table)
    }

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
    next_token: u64,
    /// How many subscriptions one connection may hold, registering ones
    /// included (0: no limit).
    per_connection: usize,
}

/// Runs the task: the engine's events on one side, the connections'
/// requests on the other. Returns when both channels are closed.
pub async fn run(
    catalog: Arc<CatalogHandle>,
    sink: usize,
    commands: mpsc::Sender<Command<MultiTableReadQuery>>,
    mut events: mpsc::UnboundedReceiver<Event>,
    mut requests: mpsc::Receiver<Request>,
    mut ready: watch::Receiver<bool>,
    per_connection: usize,
) {
    // Queries are registered only once the engine's position covers the
    // storage; until then a registration would read a snapshot the feed
    // has not caught up with.
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
    };
    loop {
        tokio::select! {
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
    async fn request(&mut self, request: Request) {
        match request {
            Request::Connected { conn, out, close } => {
                self.conns.insert(
                    conn,
                    Conn {
                        out,
                        close: Some(close),
                        subs: HashMap::new(),
                    },
                );
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
            Request::Unsubscribe { conn, id } => {
                let sub = self
                    .conns
                    .get_mut(&conn)
                    .and_then(|conn| conn.subs.remove(&id));
                if let Some(sub) = sub {
                    self.forget(sub);
                    let _ = self.commands.send(Command::Unregister(sub)).await;
                }
                // A registration still in flight for the id is not wanted
                // either: `Registered` then finds no owner and unregisters
                // it, and the id is free to subscribe again.
                self.by_token
                    .retain(|_, (owner, own_id, _)| !(*owner == conn && *own_id == id));
            }
            Request::Disconnected { conn } => {
                if let Some(gone) = self.conns.remove(&conn) {
                    let subs: Vec<SubId> = gone.subs.values().copied().collect();
                    for sub in &subs {
                        self.forget(*sub);
                    }
                    self.by_token.retain(|_, (owner, _, _)| *owner != conn);
                    if !subs.is_empty() {
                        let _ = self.commands.send(Command::UnregisterAll(subs)).await;
                    }
                }
            }
        }
    }

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
        let mut taken = connection.subs.contains_key(&id);
        let mut registering = 0;
        for (owner, own_id, _) in self.by_token.values() {
            if *owner == conn {
                registering += 1;
                taken |= *own_id == id;
            }
        }
        if taken {
            connection.send(refused(id, "that id is already subscribed"));
            return;
        }
        if self.per_connection > 0 && connection.subs.len() + registering >= self.per_connection {
            let reason = format!(
                "the subscription limit of {} per connection is reached",
                self.per_connection
            );
            connection.send(refused(id, &reason));
            return;
        }
        let token = self.next_token;
        self.next_token += 1;
        self.by_token.insert(token, (conn, id, shape));
        let command = Command::Register {
            sink: self.sink,
            query,
            token,
        };
        if self.commands.send(command).await.is_err() {
            self.by_token.remove(&token);
            if let Some(connection) = self.conns.get_mut(&conn) {
                connection.send(refused(id, "the engine is shutting down"));
            }
        }
    }

    async fn event(&mut self, event: Event) {
        match event {
            Event::Registered {
                token,
                sub,
                reads,
                updates,
            } => {
                let Some((conn, id, shape)) = self.by_token.remove(&token) else {
                    // The connection went away, or unsubscribed the id,
                    // while the registration was in flight: the
                    // subscription is not wanted.
                    let _ = self.commands.send(Command::Unregister(sub)).await;
                    return;
                };
                let Some(connection) = self.conns.get_mut(&conn) else {
                    let _ = self.commands.send(Command::Unregister(sub)).await;
                    return;
                };
                connection.subs.insert(id, sub);
                self.by_sub.insert(sub, (conn, id));
                if let Some(mut shape) = shape {
                    // A twin's rows come with the registration.
                    for delta in &updates {
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
        }
    }

    /// `accepted`, with the hydrated rows for a rows-mode subscription.
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
                // The subscription cannot be served: refused, and ended
                // on the engine side too.
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
    fn changed(&mut self, updates: &[Delta]) {
        let mut touched: HashMap<u64, Vec<u64>> = HashMap::new();
        // Per rows-mode subscription, per table in order of appearance,
        // each key's last state in the transaction.
        let mut deltas: HashMap<SubId, Changes> = HashMap::new();
        let catalog = self.catalog.load();
        for delta in updates {
            let table = catalog.table(delta.table.as_str());
            // The row as JSON, built for the first rows-mode subscription
            // it reaches and cloned for the rest.
            let mut image: Option<Json> = None;
            for target in delta.targets() {
                let Some((conn, id)) = self.by_sub.get(&target.sub) else {
                    continue;
                };
                if self.hydrating.contains(&target.sub) {
                    // Not accepted yet: the backend reads only after
                    // `accepted`, which carries this change (rows mode)
                    // or lands after it (touch mode).
                    if let Some(shape) = self.shapes.get_mut(&target.sub) {
                        shape.absorb(&delta.table, &delta.op);
                    }
                    continue;
                }
                if self.shapes.contains_key(&target.sub) {
                    let changes = deltas.entry(target.sub).or_default();
                    let index = match changes.iter().position(|(t, _)| *t == delta.table) {
                        Some(index) => index,
                        None => {
                            changes.push((delta.table.clone(), HashMap::new()));
                            changes.len() - 1
                        }
                    };
                    let (_, keys) = &mut changes[index];
                    // The last operation on a key wins: a row that left
                    // and came back is a put, one that came and went is
                    // a delete, and no key is in both lists.
                    match (&delta.op, table) {
                        (DataFrameOperation::Add(key, row), Some(table)) => {
                            let image = image.get_or_insert_with(|| wire::row_json(row, table));
                            keys.insert(key.clone(), Some(image.clone()));
                        }
                        (DataFrameOperation::Delete(key, _), _) => {
                            keys.insert(key.clone(), None);
                        }
                        (DataFrameOperation::Add(key, _), None) => {
                            // The table left the catalog mid-flight: the
                            // backend cannot keep the row, so drop it.
                            keys.insert(key.clone(), None);
                        }
                    }
                } else {
                    let ids = touched.entry(*conn).or_default();
                    if !ids.contains(id) {
                        ids.push(*id);
                    }
                }
            }
        }
        for (sub, changes) in deltas {
            if let Some((conn, id)) = self.by_sub.get(&sub)
                && let Some(connection) = self.conns.get_mut(conn)
            {
                let changes: Vec<Json> = changes
                    .into_iter()
                    .map(|(table, keys)| {
                        let mut put = Vec::new();
                        let mut del = Vec::new();
                        for (key, row) in keys {
                            match row {
                                Some(row) => put.push(row),
                                None => del.push(wire::key_json(&key)),
                            }
                        }
                        json!({"table": table.as_str(), "put": put, "del": del})
                    })
                    .collect();
                connection.send(json!(["delta", {"id": id, "changes": changes}]));
            }
        }
        for (conn, ids) in touched {
            if let Some(connection) = self.conns.get_mut(&conn) {
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
            connection.send(refused(id, reason));
        }
        self.forget(sub);
        let _ = self.commands.send(Command::Unregister(sub)).await;
    }

    fn forget(&mut self, sub: SubId) {
        self.by_sub.remove(&sub);
        self.shapes.remove(&sub);
        self.hydrating.remove(&sub);
    }
}

fn accepted(id: u64) -> Json {
    json!(["accepted", {"id": id}])
}

fn refused(id: u64, reason: &str) -> Json {
    json!(["refused", {"id": id, "reason": reason}])
}

/// A frame from the connection's own handler, which never floods its
/// own writer: a full queue only means the backend is not reading, and
/// the task closes the connection for that.
fn send(out: &mpsc::Sender<String>, message: Json) {
    let _ = out.try_send(message.to_string());
}

/// The route handler: upgrades the socket once the engine is serving.
pub async fn connect(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    if let Some(token) = &state.config.s2s_token
        && !authorized(&headers, &token.0)
    {
        refusal(&state.s2s.unauthorized, "no valid bearer token");
        return (StatusCode::UNAUTHORIZED, "a bearer token is required").into_response();
    }
    if !*state.ready.borrow() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "the engine is not serving yet",
        )
            .into_response();
    }
    let Some(admitted) = state.s2s.admit() else {
        let what = format!(
            "{} connections are open, the most allowed",
            state.s2s.max_connections
        );
        refusal(&state.s2s.full, &what);
        return (StatusCode::SERVICE_UNAVAILABLE, "too many S2S connections").into_response();
    };
    let upgrade = upgrade.max_message_size(state.config.max_message_bytes);
    upgrade.on_upgrade(move |socket| async move {
        handle(socket, state).await;
        drop(admitted);
    })
}

/// Whether the request carries `Authorization: Bearer <token>`. The
/// comparison takes as long for a near miss as for a far one, so the
/// token cannot be guessed a byte at a time.
fn authorized(headers: &HeaderMap, token: &str) -> bool {
    let Some(presented) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
        .map(|(_, presented)| presented.trim_start_matches(' '))
    else {
        return false;
    };
    let (presented, token) = (presented.as_bytes(), token.as_bytes());
    presented.len() == token.len()
        && presented
            .iter()
            .zip(token)
            .fold(0u8, |differ, (a, b)| differ | (a ^ b))
            == 0
}

async fn handle(socket: WebSocket, state: Arc<AppState>) {
    let handle = &state.s2s;
    let conn = handle.next_conn.fetch_add(1, Ordering::Relaxed);
    let (mut sink, stream) = socket.split();
    let (out, mut out_rx) = mpsc::channel::<String>(OUTGOING_QUEUE);
    let (close, closed) = oneshot::channel::<&'static str>();
    let writer = tokio::spawn(async move {
        while let Some(text) = out_rx.recv().await {
            if sink.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
        let _ = sink.send(Message::Close(None)).await;
    });
    if handle
        .requests
        .send(Request::Connected {
            conn,
            out: out.clone(),
            close,
        })
        .await
        .is_err()
    {
        return;
    }
    let limit = state.config.s2s_connection_subscriptions;
    send(
        &out,
        json!(["hello", {
            "protocol": PROTOCOL_VERSION,
            "max_subscriptions": (limit > 0).then_some(limit),
        }]),
    );
    log_info!("s2s connection {conn} open");

    // Whatever ends the session, a panic in a frame's handling included,
    // the task hears the connection is gone, or its subscriptions would
    // outlive it.
    let session = serve(conn, stream, closed, &out, &state);
    if AssertUnwindSafe(session).catch_unwind().await.is_err() {
        log_warn!("s2s connection {conn}: a frame's handling panicked");
    }
    let _ = handle.requests.send(Request::Disconnected { conn }).await;
    drop(out);
    let _ = writer.await;
    log_info!("s2s connection {conn} closed");
}

/// Reads frames until the backend closes, the task asks to close, or
/// the task is gone.
async fn serve(
    conn: u64,
    mut stream: SplitStream<WebSocket>,
    mut closed: oneshot::Receiver<&'static str>,
    out: &mpsc::Sender<String>,
    state: &Arc<AppState>,
) {
    let handle = &state.s2s;
    loop {
        let frame = tokio::select! {
            frame = stream.next() => frame,
            reason = &mut closed => {
                if let Ok(reason) = reason {
                    log_warn!("s2s connection {conn} closed: {reason}");
                }
                return;
            }
        };
        let text = match frame {
            Some(Ok(Message::Text(text))) => text,
            Some(Ok(Message::Ping(_)))
            | Some(Ok(Message::Pong(_)))
            | Some(Ok(Message::Binary(_))) => continue,
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return,
        };
        match upstream(&text) {
            Ok(Frame::Ping) => send(out, json!(["pong"])),
            Ok(Frame::Unsubscribe { id }) => {
                if handle
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
                let request = match prepare(state, conn, id, &sql, &binds, rows).await {
                    Ok(request) => request,
                    Err(reason) => {
                        log_debug!("s2s connection {conn}: query {id} refused: {reason}");
                        send(out, refused(id, &reason));
                        continue;
                    }
                };
                if handle.requests.send(request).await.is_err() {
                    return;
                }
            }
            Err(reason) => {
                log_warn!("s2s connection {conn}: {reason}");
                send(out, json!(["error", {"reason": reason}]));
            }
        }
    }
}

/// Parses the SQL against the catalog and plans the query the way a
/// Zero query is planned (which side of each join is read whole, how a
/// page is read), so the engine registers it ready to run.
async fn prepare(
    state: &AppState,
    conn: u64,
    id: u64,
    sql: &str,
    binds: &[BindValue],
    rows: bool,
) -> Result<Request, String> {
    let catalog = state.catalog.load();
    let select = parse_select(sql, binds, &catalog).map_err(|error| error.message)?;
    let shape = rows.then(|| Shape::new(&select.query, select.projection, select.offset));
    let translated = Translated {
        query: select.query,
        hidden: HashSet::new(),
    };
    let planned = plan::plan(
        sql,
        translated,
        state.config.policy(),
        &state.plans,
        &*state.storage,
    )
    .await?;
    Ok(Request::Subscribe(Box::new(Subscribe {
        conn,
        id,
        query: planned.query,
        shape,
    })))
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

fn upstream(text: &str) -> Result<Frame, String> {
    let frame: Json = serde_json::from_str(text).map_err(|error| format!("not JSON: {error}"))?;
    let (tag, body) = match frame.as_array().map(Vec::as_slice) {
        Some([Json::String(tag), body]) => (tag.as_str(), body),
        Some([Json::String(tag)]) => (tag.as_str(), &Json::Null),
        _ => return Err("a frame is a [tag, body] array".to_string()),
    };
    let id = |body: &Json| {
        body.get("id")
            .and_then(Json::as_u64)
            .ok_or_else(|| "`id` must be a non-negative integer".to_string())
    };
    match tag {
        "ping" => Ok(Frame::Ping),
        "subscribe" => {
            let id = id(body)?;
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
            Ok(Frame::Subscribe {
                id,
                sql,
                binds,
                rows,
            })
        }
        "unsubscribe" => Ok(Frame::Unsubscribe { id: id(body)? }),
        other => Err(format!("unknown tag `{other}`")),
    }
}

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

    /// A task over one table, `notes (id, name)`, with connection 1 open:
    /// the task, the engine's commands, and connection 1's frames.
    fn task(
        per_connection: usize,
    ) -> (
        Task,
        mpsc::Receiver<Command<MultiTableReadQuery>>,
        mpsc::Receiver<String>,
    ) {
        let catalog = Catalog::new([DbTable::new(
            "notes",
            ["id"],
            vec![
                DbColumn::new("id", ValueType::Int),
                DbColumn::new("name", ValueType::String),
            ],
        )]);
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
        };
        let (out, frames) = mpsc::channel(64);
        connect(&mut task, 1, out);
        (task, commands_rx, frames)
    }

    fn connect(task: &mut Task, conn: u64, out: mpsc::Sender<String>) {
        let (close, _closed) = oneshot::channel();
        task.conns.insert(
            conn,
            Conn {
                out,
                close: Some(close),
                subs: HashMap::new(),
            },
        );
    }

    /// Connection `conn` subscribes `SELECT * FROM notes` under `id`.
    async fn subscribe(task: &mut Task, conn: u64, id: u64, rows: bool) {
        let select =
            parse_select("SELECT * FROM notes", &[], &task.catalog.load()).expect("parses");
        let shape = rows.then(|| Shape::new(&select.query, select.projection, select.offset));
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

        let (other, mut other_out) = mpsc::channel(64);
        connect(&mut task, 2, other);
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

    /// Places are handed out up to the limit and come back when their
    /// connections close; a zero limit hands them out without one.
    #[test]
    fn connections_are_admitted_up_to_the_limit() {
        let (handle, _requests) = Handle::new(2);
        let first = handle.admit().expect("a place");
        let second = handle.admit().expect("a place");
        assert!(handle.admit().is_none());
        drop(first);
        let third = handle.admit().expect("the freed place");
        assert!(handle.admit().is_none());
        drop((second, third));
        assert_eq!(handle.open.load(Ordering::Acquire), 0);

        let (open, _requests) = Handle::new(0);
        let places: Vec<Admitted> = (0..100).map(|_| open.admit().expect("a place")).collect();
        assert_eq!(open.open.load(Ordering::Acquire), places.len());
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

    /// Only the configured token, as a bearer, opens the endpoint.
    #[test]
    fn only_the_bearer_token_authorizes() {
        let with = |value: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(AUTHORIZATION, value.parse().unwrap());
            headers
        };
        assert!(authorized(&with("Bearer s3cret"), "s3cret"));
        assert!(authorized(&with("bearer s3cret"), "s3cret"));
        assert!(authorized(&with("BEARER  s3cret"), "s3cret"));
        assert!(!authorized(&with("Bearer s3cres"), "s3cret"));
        assert!(!authorized(&with("Bearer s3cret2"), "s3cret"));
        assert!(!authorized(&with("Bearer "), "s3cret"));
        assert!(!authorized(&with("Basic s3cret"), "s3cret"));
        assert!(!authorized(&with("s3cret"), "s3cret"));
        assert!(!authorized(&HeaderMap::new(), "s3cret"));
    }

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
        assert!(
            upstream(
                r#"["subscribe", {"id": 1, "sql": "x", "binds": [{"oid": 25, "bytes": "%%"}]}]"#
            )
            .is_err()
        );
    }
}
