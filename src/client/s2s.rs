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
//! - `["unsubscribe", {"id": n}]`
//! - `["ping"]`
//!
//! Downstream:
//! - `["hello", {"protocol": 1}]` first.
//! - `["accepted", {"id": n}]` once the engine follows the query; from
//!   that moment every change to its rows is reported, so the backend
//!   reads Postgres only after it. In rows mode the body also carries
//!   the hydrated result: `"table"`, `"key"` (the primary-key columns),
//!   `"columns"` (the projection, `null` for `*`), `"count"` (a
//!   `COUNT(*)`), `"order"` (`[[column, "asc"|"desc"], ...]`), `"limit"`
//!   (`null` for none) and `"rows"` (JSON objects keyed by column, every
//!   column of the row).
//! - `["refused", {"id": n, "reason": "..."}]` when the engine cannot
//!   follow the query (outside the SQL subset, an unknown table, too many
//!   rows). The backend serves that query live.
//! - `["touched", {"ids": [n, ...]}]` per transaction that changed the
//!   rows of those subscriptions, for subscriptions not in rows mode.
//! - `["delta", {"id": n, "put": [row, ...], "del": [key, ...]}]` per
//!   transaction, for one subscription in rows mode: rows that entered
//!   or changed, and the keys of rows that left, in the order applied.
//!   A changed row appears as a `del` of its key and a `put` of its new
//!   image in the same frame.
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

use super::connection::AppState;
use super::wire;
use crate::ivm::Delta;
use crate::log::{log_debug, log_info, log_warn};
use crate::model::{
    DataFrameKey, DataFrameOperation, DataFrameRow, MultiTableReadQuery, Order, OrderBy,
    SingleTableReadQuery, SubId, TableName,
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
    Subscribe {
        conn: u64,
        id: u64,
        sql: String,
        binds: Vec<BindValue>,
        rows: bool,
    },
    Unsubscribe {
        conn: u64,
        id: u64,
    },
    Disconnected {
        conn: u64,
    },
}

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
/// for the backend to order and project by, and the rows gathered while
/// the engine hydrates it.
struct Shape {
    table: TableName,
    projection: Projection,
    order: Vec<OrderBy>,
    limit: u32,
    /// The hydrating result: a key's latest image, `None` once deleted.
    snapshot: Vec<(DataFrameKey, Option<DataFrameRow>)>,
}

impl Shape {
    fn new(query: &SingleTableReadQuery, projection: Projection) -> Shape {
        Shape {
            table: query.table.clone(),
            projection,
            order: query.order_by.clone(),
            limit: query.limit,
            snapshot: Vec::new(),
        }
    }

    fn absorb(&mut self, op: &DataFrameOperation) {
        let (key, row) = match op {
            DataFrameOperation::Add(key, row) => (key, Some(row.clone())),
            DataFrameOperation::Delete(key, _) => (key, None),
        };
        match self.snapshot.iter_mut().find(|(k, _)| k == key) {
            Some(slot) => slot.1 = row,
            None => self.snapshot.push((key.clone(), row)),
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
            Request::Subscribe {
                conn,
                id,
                sql,
                binds,
                rows,
            } => self.subscribe(conn, id, &sql, &binds, rows).await,
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

    async fn subscribe(&mut self, conn: u64, id: u64, sql: &str, binds: &[BindValue], rows: bool) {
        let Some(connection) = self.conns.get(&conn) else {
            return;
        };
        if connection.subs.contains_key(&id)
            || self.by_token.values().any(|(c, i, _)| *c == conn && *i == id)
        {
            send(&connection.out, refused(id, "that id is already subscribed"));
            return;
        }
        let catalog = self.catalog.load();
        let (query, projection) = match parse_select(sql, binds, &catalog) {
            Ok(parsed) => parsed,
            Err(error) => {
                log_debug!("s2s connection {conn}: query {id} refused: {}", error.message);
                send(&connection.out, refused(id, &error.message));
                return;
            }
        };
        let shape = rows.then(|| Shape::new(&query, projection));
        let token = self.next_token;
        self.next_token += 1;
        self.by_token.insert(token, (conn, id, shape));
        let command = Command::Register {
            sink: self.sink,
            query: MultiTableReadQuery::single(query),
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
                            shape.absorb(&delta.op);
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
        let Some(table) = catalog.table(shape.table.as_str()) else {
            send(&connection.out, refused(id, "the table left the catalog"));
            return;
        };
        let rows: Vec<Json> = shape
            .snapshot
            .drain(..)
            .filter_map(|(_, row)| row)
            .map(|row| wire::row_json(&row, table))
            .collect();
        let columns = match &shape.projection {
            Projection::All => Json::Null,
            Projection::Count => Json::Null,
            Projection::Columns(columns) => {
                Json::Array(columns.iter().map(|c| Json::from(c.as_str())).collect())
            }
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
                "table": table.name.as_str(),
                "key": table.pkey.iter().map(|c| c.as_str()).collect::<Vec<_>>(),
                "columns": columns,
                "count": matches!(shape.projection, Projection::Count),
                "order": order,
                "limit": limit,
                "rows": rows,
            }]),
        );
    }

    /// Tells each connection what the deltas did to its subscriptions:
    /// rows-mode ones get the rows, the rest a touch.
    fn changed(&mut self, updates: &[Delta]) {
        let mut touched: HashMap<u64, Vec<u64>> = HashMap::new();
        // Per rows-mode subscription, the puts and deletes in order.
        let mut deltas: HashMap<SubId, (Vec<Json>, Vec<Json>)> = HashMap::new();
        let catalog = self.catalog.load();
        for delta in updates {
            let table = catalog.table(delta.table.as_str());
            for target in delta.targets() {
                let Some((conn, id)) = self.by_sub.get(&target.sub) else {
                    continue;
                };
                if let Some(shape) = self.shapes.get_mut(&target.sub) {
                    if self.hydrating.contains(&target.sub) {
                        shape.absorb(&delta.op);
                        continue;
                    }
                    let (puts, dels) = deltas.entry(target.sub).or_default();
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
        for (sub, (put, del)) in deltas {
            if let Some((conn, id)) = self.by_sub.get(&sub)
                && let Some(connection) = self.conns.get(conn)
            {
                send(&connection.out, json!(["delta", {"id": id, "put": put, "del": del}]));
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
        match upstream(conn, &text) {
            Ok(Some(request)) => {
                if handle.requests.send(request).await.is_err() {
                    break;
                }
            }
            Ok(None) => send(&out, json!(["pong"])),
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

/// One upstream frame as a request; `None` for a ping.
fn upstream(conn: u64, text: &str) -> Result<Option<Request>, String> {
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
        "ping" => Ok(None),
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
            Ok(Some(Request::Subscribe {
                conn,
                id,
                sql,
                binds,
                rows,
            }))
        }
        "unsubscribe" => Ok(Some(Request::Unsubscribe { conn, id: id(body)? })),
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
        let request = upstream(
            3,
            r#"["subscribe", {"id": 7, "sql": "SELECT * FROM t WHERE a = $1", "binds": [{"oid": 25, "bytes": "T1BFTg=="}, {"oid": 20, "bytes": null}]}]"#,
        )
        .expect("parses")
        .expect("a request");
        match request {
            Request::Subscribe {
                conn,
                id,
                sql,
                binds,
                rows,
            } => {
                assert_eq!((conn, id), (3, 7));
                assert!(!rows);
                assert_eq!(sql, "SELECT * FROM t WHERE a = $1");
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
            upstream(1, r#"["unsubscribe", {"id": 2}]"#),
            Ok(Some(Request::Unsubscribe { conn: 1, id: 2 }))
        ));
        assert!(matches!(upstream(1, r#"["ping"]"#), Ok(None)));
        assert!(upstream(1, r#"["subscribe", {"id": -1}]"#).is_err());
        assert!(upstream(1, r#"["nope", {}]"#).is_err());
        assert!(upstream(1, "{}").is_err());
        assert!(upstream(1, r#"["subscribe", {"id": 1, "sql": "x", "binds": [{"oid": 25, "bytes": "%%"}]}]"#).is_err());
    }
}
