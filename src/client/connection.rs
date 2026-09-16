//! One WebSocket connection of a Zero client, from the upgrade to the
//! close: the handshake header or first message, the `connected`
//! acknowledgement, the message loop, the liveness rules on both sides
//! (a `pong` for every `ping` and one of the server's own whenever the
//! downstream goes quiet, a WebSocket ping frame at an interval and a
//! close when nothing at all came back), and the cleanup that hands the
//! connection's client group back to the group thread. Query names go to
//! the application server for their ASTs and mutations are forwarded to it
//! from here, on the server's threads; the group thread only ever sees
//! finished ASTs.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::get;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value as Json, json};
use tokio::sync::{Mutex, mpsc, oneshot};

use super::backend::{Backend, Identity, PushOutcome, TransformOutcome};
use super::config::Config;
use super::groups::{ConnectReply, DesiredOp, Outbound, Request, Socket};
use super::protocol::{
    self, DeleteClients, InitConnection, PROTOCOL_VERSION, QueryPatchOp, Upstream,
};
use crate::log::{log_debug, log_info, log_warn};

/// What every connection shares.
pub struct AppState {
    pub config: Arc<Config>,
    pub requests: mpsc::Sender<Request>,
    pub backend: Arc<Backend>,
    pub lmids: Arc<LmidReader>,
}

/// The connect URL's parameters.
#[derive(Debug, Clone)]
struct ConnectParams {
    client_id: String,
    group_id: String,
    base_cookie: Option<String>,
    wsid: String,
}

/// The routes: the connect endpoint under the base path, and a health
/// check.
pub fn router(state: Arc<AppState>) -> Router {
    let base = state.config.base_path.clone();
    Router::new()
        .route(&format!("{base}/sync/v{{version}}/connect"), get(connect))
        .route(&format!("{base}/health"), get(health))
        .route("/health", get(health))
        .with_state(state)
}

/// Bind and serve until ctrl-c.
pub async fn serve(state: Arc<AppState>) -> Result<(), String> {
    let address = state.config.bind.clone();
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .map_err(|error| format!("binding {address}: {error}"))?;
    log_info!(
        "listening on http://{address}{}/sync/v{PROTOCOL_VERSION}/connect",
        state.config.base_path
    );
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            log_info!("shutting down");
        })
        .await
        .map_err(|error| format!("serving: {error}"))
}

async fn health() -> &'static str {
    "ok"
}

/// The upgrade: keep the handshake header (it must be echoed as the
/// selected subprotocol), the cookies and the origin, then hand the socket
/// to [`handle`].
async fn connect(
    Path(version): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    upgrade: WebSocketUpgrade,
) -> impl IntoResponse {
    let header = headers
        .get("sec-websocket-protocol")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let cookie = headers
        .get("cookie")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let origin = headers
        .get("origin")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let params = ConnectParams {
        client_id: query.get("clientID").cloned().unwrap_or_default(),
        group_id: query.get("clientGroupID").cloned().unwrap_or_default(),
        base_cookie: query
            .get("baseCookie")
            .cloned()
            .filter(|cookie| !cookie.is_empty()),
        wsid: query.get("wsid").cloned().unwrap_or_default(),
    };
    let version: u32 = version.parse().unwrap_or(0);
    let upgrade = upgrade.max_message_size(state.config.max_message_bytes);
    let upgrade = match &header {
        Some(header) => upgrade.protocols([header.clone()]),
        None => upgrade,
    };
    upgrade.on_upgrade(move |socket| handle(socket, state, params, version, header, cookie, origin))
}

/// One connection's mutable state on the server side.
struct Conn {
    state: Arc<AppState>,
    params: ConnectParams,
    identity: Identity,
    out: mpsc::UnboundedSender<Outbound>,
    joined: bool,
}

/// One connection, start to finish.
async fn handle(
    socket: WebSocket,
    state: Arc<AppState>,
    params: ConnectParams,
    version: u32,
    header: Option<String>,
    cookie: Option<String>,
    origin: Option<String>,
) {
    let (sink, stream) = socket.split();
    let (out, out_rx) = mpsc::unbounded_channel::<Outbound>();
    let pong_interval = state.config.pong_interval;
    let writer = tokio::spawn(write_loop(sink, out_rx, pong_interval));
    let wsid = params.wsid.clone();
    let group_id = params.group_id.clone();
    if version != PROTOCOL_VERSION {
        log_warn!("connection {wsid}: protocol v{version} is not v{PROTOCOL_VERSION}");
        send(
            &out,
            protocol::error(
                "VersionNotSupported",
                &format!(
                    "server is at sync protocol v{PROTOCOL_VERSION} and does not support v{version}"
                ),
            ),
        );
        let _ = out.send(Outbound::Close);
        let _ = writer.await;
        return;
    }
    if params.client_id.is_empty() || params.group_id.is_empty() {
        send(
            &out,
            protocol::error(
                "InvalidConnectionRequest",
                "clientID and clientGroupID are required",
            ),
        );
        let _ = out.send(Outbound::Close);
        let _ = writer.await;
        return;
    }
    send(&out, protocol::connected(&wsid));
    let handshake = match header.as_deref().map(protocol::decode_handshake) {
        Some(Ok(handshake)) => handshake,
        Some(Err(error)) => {
            log_warn!("connection {wsid}: {error}");
            protocol::Handshake::default()
        }
        None => protocol::Handshake::default(),
    };
    let mut conn = Conn {
        identity: Identity {
            cookie,
            origin,
            token: handshake.auth_token.clone(),
            ..Identity::default()
        },
        state: state.clone(),
        params,
        out: out.clone(),
        joined: false,
    };
    let schema = handshake
        .init
        .as_ref()
        .and_then(|init| init.client_schema.clone());
    let lmids = state.lmids.lmids(&group_id).await;
    let (reply_tx, reply_rx) = oneshot::channel();
    let request = Request::Connect {
        group: group_id.clone(),
        wsid: wsid.clone(),
        socket: Socket {
            client: conn.params.client_id.clone(),
            sink: out.clone(),
        },
        base_cookie: conn.params.base_cookie.clone(),
        schema,
        lmids,
        reply: reply_tx,
    };
    let accepted = match state.requests.send(request).await {
        Ok(()) => matches!(reply_rx.await, Ok(ConnectReply::Accepted)),
        Err(_) => false,
    };
    if !accepted {
        log_info!("connection {wsid}: client group {group_id} must start over");
        send(
            &out,
            protocol::error(
                "InvalidConnectionRequestBaseCookie",
                "the server holds no sync state for this client group; start a fresh sync",
            ),
        );
        let _ = out.send(Outbound::Close);
        let _ = writer.await;
        return;
    }
    conn.joined = true;
    let mut keep_going = true;
    if let Some(init) = handshake.init {
        keep_going = conn.init(init, true).await;
    }
    if keep_going {
        conn.read_loop(stream).await;
    }
    let _ = state
        .requests
        .send(Request::Disconnect {
            group: group_id,
            wsid,
        })
        .await;
    let _ = out.send(Outbound::Close);
    let _ = writer.await;
}

/// Queue one text frame.
fn send(out: &mpsc::UnboundedSender<Outbound>, frame: String) {
    let _ = out.send(Outbound::Text(frame.into()));
}

/// The writer: frames in order, a `pong` of the server's own whenever the
/// downstream has been quiet for `pong_interval`, a ping frame on request.
async fn write_loop(
    mut sink: SplitSink<WebSocket, Message>,
    mut frames: mpsc::UnboundedReceiver<Outbound>,
    pong_interval: Duration,
) {
    let mut last_sent = Instant::now();
    let mut idle = tokio::time::interval(pong_interval.max(Duration::from_millis(100)));
    idle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            frame = frames.recv() => match frame {
                Some(Outbound::Text(text)) => {
                    if sink.send(Message::Text(text.to_string().into())).await.is_err() {
                        break;
                    }
                    last_sent = Instant::now();
                }
                Some(Outbound::Ping) => {
                    if sink.send(Message::Ping(Default::default())).await.is_err() {
                        break;
                    }
                }
                Some(Outbound::Close) | None => {
                    let _ = sink.send(Message::Close(None)).await;
                    break;
                }
            },
            _ = idle.tick() => {
                if last_sent.elapsed() >= pong_interval {
                    if sink.send(Message::Text(protocol::pong().into())).await.is_err() {
                        break;
                    }
                    last_sent = Instant::now();
                }
            }
        }
    }
}

impl Conn {
    /// The reader: every frame resets the liveness clock; a ping frame
    /// goes out at the configured interval, and a connection that has
    /// answered nothing for the client timeout is closed.
    async fn read_loop(&mut self, mut stream: SplitStream<WebSocket>) {
        let ping_interval = self.state.config.ping_interval.max(Duration::from_secs(1));
        let timeout = self.state.config.client_timeout;
        let mut last_inbound = Instant::now();
        let mut ticker = tokio::time::interval(ping_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            tokio::select! {
                frame = stream.next() => match frame {
                    Some(Ok(Message::Text(text))) => {
                        last_inbound = Instant::now();
                        if !self.dispatch(text.as_str()).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) | Some(Ok(Message::Ping(_))) | Some(Ok(Message::Binary(_))) => {
                        last_inbound = Instant::now();
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        log_debug!("connection {}: closed by the client", self.params.wsid);
                        break;
                    }
                    Some(Err(error)) => {
                        log_debug!("connection {}: {error}", self.params.wsid);
                        break;
                    }
                },
                _ = ticker.tick() => {
                    if last_inbound.elapsed() > timeout {
                        log_warn!(
                            "connection {}: nothing heard for {:?}; closing",
                            self.params.wsid,
                            last_inbound.elapsed()
                        );
                        break;
                    }
                    let _ = self.out.send(Outbound::Ping);
                }
            }
        }
    }

    /// One upstream message; false when the connection should end.
    async fn dispatch(&mut self, text: &str) -> bool {
        let message = match protocol::parse_upstream(text) {
            Ok(message) => message,
            Err(error) => {
                log_warn!("connection {}: {error}", self.params.wsid);
                send(&self.out, protocol::error("InvalidMessage", &error));
                return false;
            }
        };
        match message {
            Upstream::Ping => {
                send(&self.out, protocol::pong());
                true
            }
            Upstream::InitConnection(init) => self.init(*init, false).await,
            Upstream::ChangeDesiredQueries(ops) => self.desired(ops, None).await,
            Upstream::DeleteClients(deleted) => {
                self.delete_clients(&deleted).await;
                true
            }
            Upstream::Push(push) => {
                self.push(push).await;
                true
            }
            Upstream::Pull(pull) => {
                let (reply_tx, reply_rx) = oneshot::channel();
                let request = Request::Pull {
                    group: self.params.group_id.clone(),
                    reply: reply_tx,
                };
                if self.state.requests.send(request).await.is_ok()
                    && let Ok((cookie, lmids)) = reply_rx.await
                {
                    send(
                        &self.out,
                        protocol::pull_response(&cookie, &pull.request_id, &lmids),
                    );
                }
                true
            }
            Upstream::CloseConnection => {
                log_debug!("connection {}: closeConnection", self.params.wsid);
                false
            }
            Upstream::Other(tag) => {
                log_debug!("connection {}: ignoring `{tag}`", self.params.wsid);
                true
            }
        }
    }

    /// The first message: endpoint overrides, deleted clients, the desired
    /// queries (and the schema, unless it already went with the connect).
    async fn init(&mut self, init: InitConnection, schema_sent: bool) -> bool {
        if let Some(url) = init.user_query_url {
            self.identity.query_url = Some(url);
        }
        if let Some(headers) = init.user_query_headers {
            self.identity.query_headers = headers;
        }
        if let Some(url) = init.user_push_url {
            self.identity.mutate_url = Some(url);
        }
        if let Some(headers) = init.user_push_headers {
            self.identity.mutate_headers = headers;
        }
        if let Some(deleted) = &init.deleted {
            self.delete_clients(deleted).await;
        }
        let schema = if schema_sent {
            None
        } else {
            init.client_schema
        };
        self.desired(init.desired_queries_patch, schema).await
    }

    /// Clients the client says are gone: their queries go, and the client
    /// hears back which were dropped.
    async fn delete_clients(&mut self, deleted: &DeleteClients) {
        if !deleted.client_ids.is_empty() {
            let _ = self
                .state
                .requests
                .send(Request::DeleteClients {
                    group: self.params.group_id.clone(),
                    clients: deleted.client_ids.clone(),
                })
                .await;
        }
        send(&self.out, protocol::delete_clients(deleted));
    }

    /// Desired-query changes: custom queries go to the application server
    /// for their ASTs, then everything goes to the engine thread in the
    /// order the client sent it.
    async fn desired(
        &mut self,
        ops: Vec<QueryPatchOp>,
        schema: Option<protocol::ClientSchema>,
    ) -> bool {
        let mut prepared: Vec<DesiredOp> = Vec::with_capacity(ops.len());
        let mut requests: Vec<Json> = Vec::new();
        let mut positions: HashMap<String, usize> = HashMap::new();
        for op in ops {
            match op {
                QueryPatchOp::Put {
                    hash,
                    ttl,
                    name,
                    args,
                    ast,
                } => {
                    let name = name.unwrap_or_else(|| "query".to_owned());
                    if ast.is_none() {
                        positions.insert(hash.clone(), prepared.len());
                        requests.push(
                            json!({"id": hash, "name": name, "args": args.unwrap_or_default()}),
                        );
                    }
                    prepared.push(DesiredOp::Put {
                        hash,
                        name,
                        ttl,
                        ast,
                    });
                }
                QueryPatchOp::Del { hash } => prepared.push(DesiredOp::Del { hash }),
                QueryPatchOp::Clear => prepared.push(DesiredOp::Clear),
            }
        }
        if !requests.is_empty() {
            let ids: Vec<String> = positions.keys().cloned().collect();
            match self.state.backend.transform(&self.identity, requests).await {
                TransformOutcome::Queries(results) => {
                    let mut errored = Vec::new();
                    for result in results {
                        let Some(id) = result.get("id").and_then(Json::as_str) else {
                            continue;
                        };
                        let Some(&position) = positions.get(id) else {
                            continue;
                        };
                        if let Some(ast) = result.get("ast") {
                            if let DesiredOp::Put { ast: slot, .. } = &mut prepared[position] {
                                *slot = Some(ast.clone());
                            }
                        } else {
                            log_warn!(
                                "connection {}: query {id} errored at the application server: {}",
                                self.params.wsid,
                                result.get("message").and_then(Json::as_str).unwrap_or("")
                            );
                            errored.push(result);
                        }
                    }
                    if !errored.is_empty() {
                        send(&self.out, protocol::transform_error(errored));
                    }
                }
                TransformOutcome::Failed { status, message } => {
                    log_warn!(
                        "connection {}: transform failed: {message}",
                        self.params.wsid
                    );
                    send(
                        &self.out,
                        protocol::transform_failed(&ids, status, &message),
                    );
                    return false;
                }
            }
        }
        let request = Request::Desired {
            group: self.params.group_id.clone(),
            client: self.params.client_id.clone(),
            schema,
            ops: prepared,
        };
        self.state.requests.send(request).await.is_ok()
    }

    /// A push: forwarded as is; the application server's answer comes
    /// back as a `pushResponse`, or as the error that stood in its way.
    async fn push(&mut self, push: protocol::Push) {
        if push.client_group_id != self.params.group_id {
            log_warn!(
                "connection {}: a push for client group {} on the connection of {}",
                self.params.wsid,
                push.client_group_id,
                self.params.group_id
            );
        }
        match self.state.backend.push(&self.identity, &push.body).await {
            PushOutcome::Response(json) => {
                if let Some(mutations) = json.get("mutations") {
                    send(
                        &self.out,
                        protocol::push_response(json!({"mutations": mutations})),
                    );
                } else if json.get("error").is_some() {
                    send(&self.out, protocol::push_response(json));
                } else if json.get("kind").and_then(Json::as_str) == Some("PushFailed") {
                    send(
                        &self.out,
                        serde_json::to_string(&json!(["error", json])).unwrap_or_default(),
                    );
                } else {
                    log_warn!(
                        "connection {}: unexpected mutate response: {json}",
                        self.params.wsid
                    );
                    send(
                        &self.out,
                        protocol::push_failed(
                            &push.mutation_ids,
                            None,
                            None,
                            "unexpected response from the mutate endpoint",
                        ),
                    );
                }
            }
            PushOutcome::Failed {
                status,
                preview,
                message,
            } => {
                send(
                    &self.out,
                    protocol::push_failed(&push.mutation_ids, status, preview.as_deref(), &message),
                );
            }
        }
    }
}

/// Reads a client group's last mutation ids from the database at connect
/// time, on the server's threads, over one lazily opened connection.
pub struct LmidReader {
    dsn: String,
    table: String,
    client: Mutex<Option<tokio_postgres::Client>>,
}

impl LmidReader {
    /// A reader of `<schema>.clients` at `dsn`.
    pub fn new(dsn: &str, schema: &str) -> Self {
        LmidReader {
            dsn: dsn.to_owned(),
            table: format!("\"{}\".\"clients\"", schema.replace('"', "\"\"")),
            client: Mutex::new(None),
        }
    }

    /// The last mutation id of every client of `group`; empty when the
    /// table cannot be read (the group then starts from the feed's word).
    pub async fn lmids(&self, group: &str) -> Vec<(String, i64)> {
        let mut guard = self.client.lock().await;
        if guard.is_none() {
            match tokio_postgres::connect(&self.dsn, tokio_postgres::NoTls).await {
                Ok((client, connection)) => {
                    tokio::spawn(async move {
                        let _ = connection.await;
                    });
                    *guard = Some(client);
                }
                Err(error) => {
                    log_warn!("cannot read last mutation ids: {error}");
                    return Vec::new();
                }
            }
        }
        let sql = format!(
            "SELECT \"clientID\", \"lastMutationID\" FROM {} WHERE \"clientGroupID\" = $1",
            self.table
        );
        let result = guard
            .as_ref()
            .expect("just opened")
            .query(&sql, &[&group])
            .await;
        match result {
            Ok(rows) => rows
                .iter()
                .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1)))
                .collect(),
            Err(error) => {
                log_warn!("reading last mutation ids of {group}: {error}");
                *guard = None;
                Vec::new()
            }
        }
    }
}
