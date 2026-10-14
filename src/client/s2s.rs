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
//! - `["hello", {"protocol": 1}]` first.
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
//!   rows that entered or changed and the keys of rows that left, in the
//!   order applied. A changed row appears as a `del` of its key and a
//!   `put` of its new image in the same frame.
//! - `["pong"]`

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value as Json, json};
use tokio::sync::{mpsc, watch};

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

/// What a connection asks of the task.
#[derive(Debug)]
pub enum Request {
    Connected {
        conn: u64,
        out: mpsc::UnboundedSender<String>,
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

/// Per table, the puts and deletes of one transaction.
type Changes = Vec<(TableName, Vec<Json>, Vec<Json>)>;

/// The handle connections talk to the task through.
#[derive(Debug)]
pub struct Handle {
    requests: mpsc::Sender<Request>,
    next_conn: AtomicU64,
}

impl Handle {
    pub fn new() -> (Handle, mpsc::Receiver<Request>) {
        let (requests, requests_rx) = mpsc::channel(REQUEST_QUEUE);
        (
            Handle {
                requests,
                next_conn: AtomicU64::new(1),
            },
            requests_rx,
        )
    }
}

/// One backend connection as the task sees it.
struct Conn {
    out: mpsc::UnboundedSender<String>,
    /// The backend's ids and the engine's subscriptions they became.
    subs: HashMap<u64, SubId>,
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
    snapshot: Vec<(DataFrameKey, Option<DataFrameRow>)>,
}

impl Shape {
    pub fn new(query: &MultiTableReadQuery, projection: Projection, offset: u32) -> Shape {
        let mut nodes = Vec::new();
        fn walk(query: &MultiTableReadQuery, parent: Option<usize>, edge: Option<(&str, &str, bool)>, nodes: &mut Vec<NodeShape>) {
            nodes.push(NodeShape {
                table: query.main_table.table.clone(),
                parent,
                parent_column: edge.map(|(p, _, _)| p.to_string()),
                child_column: edge.map(|(_, c, _)| c.to_string()),
                inner: edge.is_none_or(|(_, _, inner)| inner),
                snapshot: Vec::new(),
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
        match node.snapshot.iter_mut().find(|(k, _)| k == key) {
            Some(slot) => slot.1 = row,
            None => node.snapshot.push((key.clone(), row)),
        }
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
    };
    loop {
        tokio::select! {
            event = events.recv() => match event {
                Some(event) => task.event(event),
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
            Request::Connected { conn, out } => {
                self.conns.insert(
                    conn,
                    Conn {
                        out,
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
        let Some(connection) = self.conns.get(&conn) else {
            return;
        };
        if connection.subs.contains_key(&id)
            || self.by_token.values().any(|(c, i, _)| *c == conn && *i == id)
        {
            send(&connection.out, refused(id, "that id is already subscribed"));
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
            send(&connection.out, refused(id, "the engine is shutting down"));
        }
    }

    fn event(&mut self, event: Event) {
        match event {
            Event::Registered {
                token,
                sub,
                reads,
                updates,
            } => {
                let Some((conn, id, shape)) = self.by_token.remove(&token) else {
                    // The connection went away while the registration
                    // was in flight: the subscription is not wanted.
                    let _ = self.commands.try_send(Command::Unregister(sub));
                    return;
                };
                let Some(connection) = self.conns.get_mut(&conn) else {
                    let _ = self.commands.try_send(Command::Unregister(sub));
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
                    self.accept(sub);
                } else {
                    self.hydrating.insert(sub);
                }
            }
            Event::Hydrated(subs) => {
                for sub in subs {
                    if self.hydrating.remove(&sub) {
                        self.accept(sub);
                    }
                }
            }
            Event::Refused { sub, reason } => self.drop_sub(sub, &reason),
            Event::Capped { sub } => {
                self.drop_sub(sub, "the query has more rows than the engine follows");
            }
            Event::Committed { updates, .. } | Event::Landed { updates } => self.changed(&updates),
            Event::Heavy { sub, table, rows } => {
                if let Some((conn, id)) = self.by_sub.get(&sub) {
                    log_warn!(
                        "s2s connection {conn}: query {id} holds {rows} rows of {table}"
                    );
                }
            }
        }
    }

    /// `accepted`, with the hydrated rows for a rows-mode subscription.
    fn accept(&mut self, sub: SubId) {
        let Some((conn, id)) = self.by_sub.get(&sub).copied() else {
            return;
        };
        let Some(connection) = self.conns.get(&conn) else {
            return;
        };
        let Some(shape) = self.shapes.get_mut(&sub) else {
            send(&connection.out, accepted(id));
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
                send(&connection.out, refused(id, "a table left the catalog"));
                return;
            };
            let rows: Vec<Json> = node
                .snapshot
                .drain(..)
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
                Json::Array(columns.iter().map(|p| Json::from(p.name.as_str())).collect()),
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
        send(
            &connection.out,
            json!(["accepted", {
                "id": id,
                "nodes": nodes,
                "columns": columns,
                "names": names,
                "count": matches!(shape.projection, Projection::Count { .. }),
                "order": order,
                "limit": limit,
                "offset": shape.offset,
            }]),
        );
    }

    /// Tells each connection what the deltas did to its subscriptions:
    /// rows-mode ones get the rows per table, the rest a touch.
    fn changed(&mut self, updates: &[Delta]) {
        let mut touched: HashMap<u64, Vec<u64>> = HashMap::new();
        // Per rows-mode subscription, per table in order of appearance,
        // the puts and deletes in order.
        let mut deltas: HashMap<SubId, Changes> = HashMap::new();
        let catalog = self.catalog.load();
        for delta in updates {
            let table = catalog.table(delta.table.as_str());
            for target in delta.targets() {
                let Some((conn, id)) = self.by_sub.get(&target.sub) else {
                    continue;
                };
                if let Some(shape) = self.shapes.get_mut(&target.sub) {
                    if self.hydrating.contains(&target.sub) {
                        shape.absorb(&delta.table, &delta.op);
                        continue;
                    }
                    let changes = deltas.entry(target.sub).or_default();
                    let index = match changes.iter().position(|(t, _, _)| *t == delta.table) {
                        Some(index) => index,
                        None => {
                            changes.push((delta.table.clone(), Vec::new(), Vec::new()));
                            changes.len() - 1
                        }
                    };
                    let (_, puts, dels) = &mut changes[index];
                    match (&delta.op, table) {
                        (DataFrameOperation::Add(_, row), Some(table)) => {
                            puts.push(wire::row_json(row, table));
                        }
                        (DataFrameOperation::Delete(key, _), _) => dels.push(wire::key_json(key)),
                        (DataFrameOperation::Add(key, _), None) => {
                            // The table left the catalog mid-flight: the
                            // backend cannot keep the row, so drop it.
                            dels.push(wire::key_json(key));
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
                && let Some(connection) = self.conns.get(conn)
            {
                let changes: Vec<Json> = changes
                    .into_iter()
                    .map(|(table, put, del)| json!({"table": table.as_str(), "put": put, "del": del}))
                    .collect();
                send(&connection.out, json!(["delta", {"id": id, "changes": changes}]));
            }
        }
        for (conn, ids) in touched {
            if let Some(connection) = self.conns.get(&conn) {
                send(&connection.out, json!(["touched", {"ids": ids}]));
            }
        }
    }

    /// The engine stopped following a subscription: the backend must stop
    /// trusting its cache for it.
    fn drop_sub(&mut self, sub: SubId, reason: &str) {
        if let Some((conn, id)) = self.by_sub.get(&sub).copied()
            && let Some(connection) = self.conns.get_mut(&conn)
        {
            connection.subs.remove(&id);
            send(&connection.out, refused(id, reason));
        }
        self.forget(sub);
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

fn send(out: &mpsc::UnboundedSender<String>, message: Json) {
    let _ = out.send(message.to_string());
}

/// The route handler: upgrades the socket once the engine is serving.
pub async fn connect(State(state): State<Arc<AppState>>, upgrade: WebSocketUpgrade) -> Response {
    if !*state.ready.borrow() {
        return (StatusCode::SERVICE_UNAVAILABLE, "the engine is not serving yet").into_response();
    }
    let upgrade = upgrade.max_message_size(state.config.max_message_bytes);
    upgrade.on_upgrade(move |socket| handle(socket, state))
}

async fn handle(socket: WebSocket, state: Arc<AppState>) {
    let handle = &state.s2s;
    let conn = handle.next_conn.fetch_add(1, Ordering::Relaxed);
    let (mut sink, mut stream) = socket.split();
    let (out, mut out_rx) = mpsc::unbounded_channel::<String>();
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
        })
        .await
        .is_err()
    {
        return;
    }
    send(&out, json!(["hello", {"protocol": PROTOCOL_VERSION}]));
    log_info!("s2s connection {conn} open");

    while let Some(frame) = stream.next().await {
        let text = match frame {
            Ok(Message::Text(text)) => text,
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) | Ok(Message::Binary(_)) => continue,
            Ok(Message::Close(_)) | Err(_) => break,
        };
        match upstream(&text) {
            Ok(Frame::Ping) => send(&out, json!(["pong"])),
            Ok(Frame::Unsubscribe { id }) => {
                if handle
                    .requests
                    .send(Request::Unsubscribe { conn, id })
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Ok(Frame::Subscribe {
                id,
                sql,
                binds,
                rows,
            }) => {
                let request = match prepare(&state, conn, id, &sql, &binds, rows).await {
                    Ok(request) => request,
                    Err(reason) => {
                        log_debug!("s2s connection {conn}: query {id} refused: {reason}");
                        send(&out, refused(id, &reason));
                        continue;
                    }
                };
                if handle.requests.send(request).await.is_err() {
                    break;
                }
            }
            Err(reason) => {
                log_warn!("s2s connection {conn}: {reason}");
                send(&out, json!(["error", {"reason": reason}]));
            }
        }
    }
    let _ = handle.requests.send(Request::Disconnected { conn }).await;
    drop(out);
    let _ = writer.await;
    log_info!("s2s connection {conn} closed");
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
        assert!(upstream(r#"["subscribe", {"id": 1, "sql": "x", "binds": [{"oid": 25, "bytes": "%%"}]}]"#).is_err());
    }
}
