//! One WebSocket connection of a Zero client, from the upgrade to the
//! close: the handshake header or first message, the `connected`
//! acknowledgement, the message loop, the liveness rules on both sides
//! (a `pong` for every `ping` and one of the server's own whenever the
//! downstream goes quiet, a WebSocket ping frame at an interval and a
//! close when nothing at all came back), and the cleanup that hands the
//! connection's client group back to the group thread. Query names go to
//! the application server for their ASTs, the ASTs are translated into the
//! engine's trees and planned (which side of each join is read whole,
//! from the plan cache or by counting on the reads pool) and mutations are
//! forwarded to the application server, all from here, on the server's
//! threads; the group thread only ever sees planned trees. Nothing is
//! planned before the change feed has passed the first read snapshot: a
//! connection that arrives earlier waits for it (the writer keeps
//! answering the client's pings meanwhile), and `/health` says the same
//! thing to whoever starts the process.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade, close_code};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::serve::ListenerExt;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value as Json, json};
use tokio::sync::{mpsc, oneshot, watch};

use super::ast::{self, Ast, Translated};
use super::backend::{Backend, Identity, PushOutcome, TransformOutcome};
use super::config::Config;
use super::groups::{ConnectReply, DesiredOp, Outbound, Request, Socket};
use super::plan::{self, PlanCache};
use super::protocol::{
    self, DeleteClients, InitConnection, PROTOCOL_VERSION, QueryPatchOp, Upstream,
};
use super::schema;
use super::transform::TransformCache;
use super::warm::WarmStart;
use crate::log::{Level, log_debug, log_event, log_info, log_warn};
use crate::stats::Stats;
use crate::sync::CatalogHandle;
use crate::sync::pg::PgStorage;
use crate::sync::pg::sql::{quote_ident, quote_literal};

/// What every connection shares.
///
/// - `requests`: the group threads' inlets, one per thread; a connection
///   speaks to the thread its client group hashes to.
/// - `storage`: the engine side's storage handle, for the planner's
///   counts and the connect-time mutation ids.
/// - `catalog`: the tables, for translating ASTs; the engine side keeps
///   it current with the schema.
/// - `plans`: the join plans remembered across connections.
/// - `stats`: the server's measurements, served at `/stats`.
/// - `ready`: whether the engine's position has covered the storage's
///   snapshots, flipped once by the group threads; queries wait for it.
pub struct AppState {
    pub config: Arc<Config>,
    pub requests: Vec<mpsc::Sender<Request>>,
    pub backend: Arc<Backend>,
    pub storage: Arc<PgStorage>,
    pub catalog: Arc<CatalogHandle>,
    pub plans: Arc<PlanCache>,
    pub lmids: Arc<LmidReader>,
    pub stats: Arc<Stats>,
    pub ready: watch::Receiver<bool>,
    pub warm: Arc<WarmStart>,
    pub warmed: watch::Receiver<bool>,
    pub transforms: Arc<TransformCache>,
}

/// One desired-query change between the client's message and the group
/// thread: a put still carries its AST (the application server's, or the
/// client's own) until it is translated and planned.
enum Pending {
    Put {
        hash: String,
        name: String,
        ttl: Option<f64>,
        ast: Option<Json>,
    },
    Del {
        hash: String,
    },
    Clear,
}

/// The connect URL's parameters.
#[derive(Debug, Clone)]
struct ConnectParams {
    client_id: String,
    group_id: String,
    base_cookie: Option<String>,
    wsid: String,
}

/// The routes: the connect endpoint under the base path, a health check
/// (`503` until the change feed has passed the first read snapshot, `200`
/// once queries are served), and
/// the server's measurements (`/stats`; `?reset=1` also zeroes the
/// histograms after reading them, so a load run measures itself alone).
pub fn router(state: Arc<AppState>) -> Router {
    let base = state.config.base_path.clone();
    let mut router =
        Router::new().route(&format!("{base}/sync/v{{version}}/connect"), get(connect));
    for probe in ["health", "healthz", "readyz"] {
        router = router
            .route(&format!("{base}/{probe}"), get(health))
            .route(&format!("/{probe}"), get(health));
    }
    router
        .route("/stats", get(stats))
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// The server's measurements in Prometheus exposition format.
async fn metrics(
    State(state): State<Arc<AppState>>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.stats.prometheus(),
    )
}

/// How long the listener waits for the sockets to close after a
/// shutdown signal before returning anyway.
const DRAIN_GRACE: Duration = Duration::from_secs(5);

/// The window over which the goodbyes are spread, so a thousand clients
/// do not reconnect in the same instant.
const DRAIN_STAGGER_MS: u64 = 3_000;

/// The one flag every writer watches: flipped by the first shutdown
/// signal, never unflipped.
fn shutdown() -> &'static watch::Sender<bool> {
    static FLAG: std::sync::OnceLock<watch::Sender<bool>> = std::sync::OnceLock::new();
    FLAG.get_or_init(|| watch::channel(false).0)
}

/// Resolves on ctrl-c or, on unix, `SIGTERM` (what `docker stop` sends).
async fn signalled() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(terminate) => terminate,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Where in the stagger window this connection says goodbye: a hash of
/// its id, so the spread is even and needs no coordination.
fn goodbye_after(wsid: &str) -> Duration {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    wsid.hash(&mut hasher);
    Duration::from_millis(hasher.finish() % DRAIN_STAGGER_MS)
}

/// Bind and serve until ctrl-c.
pub async fn serve(state: Arc<AppState>) -> Result<(), String> {
    let address = state.config.bind.clone();
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .map_err(|error| format!("binding {address}: {error}"))?
        .tap_io(|socket| {
            if let Err(error) = socket.set_nodelay(true) {
                log_warn!("TCP_NODELAY could not be set on a client socket: {error}");
            }
        });
    log_info!(
        "listening on http://{address}{}/sync/v{PROTOCOL_VERSION}/connect",
        state.config.base_path
    );
    let warm = state.warm.clone();
    let serving = axum::serve(listener, router(state)).with_graceful_shutdown(async move {
        signalled().await;
        log_info!("shutting down: closing the clients, {DRAIN_GRACE:?} at most");
        warm.save();
        shutdown().send_replace(true);
    });
    let grace = async {
        let mut flag = shutdown().subscribe();
        while !*flag.borrow() {
            if flag.changed().await.is_err() {
                return;
            }
        }
        tokio::time::sleep(DRAIN_GRACE).await;
    };
    tokio::select! {
        result = serving => result.map_err(|error| format!("serving: {error}")),
        _ = grace => {
            log_warn!("drain grace elapsed with sockets still open; exiting");
            Ok(())
        }
    }
}

/// Ready, or not yet: the status the health check answers with.
async fn health(State(state): State<Arc<AppState>>) -> (StatusCode, &'static str) {
    readiness(*state.ready.borrow() && *state.warmed.borrow())
}

/// What `/health` says for a readiness.
fn readiness(ready: bool) -> (StatusCode, &'static str) {
    if ready {
        (StatusCode::OK, "ok")
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "waiting for the change feed to pass the first read snapshot",
        )
    }
}

/// The server's measurements as JSON.
async fn stats(
    Query(query): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> axum::Json<Json> {
    let mut body = state.stats.json();
    body["plans_cached"] = json!(state.plans.len());
    body["group_threads"] = json!(state.requests.len());
    if query
        .get("reset")
        .is_some_and(|value| value != "0" && value != "false")
    {
        state.stats.reset();
    }
    axum::Json(body)
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

/// One connection's mutable state on the server side; `requests` is the
/// inlet of the group thread owning its client group.
struct Conn {
    state: Arc<AppState>,
    params: ConnectParams,
    identity: Identity,
    out: mpsc::UnboundedSender<Outbound>,
    requests: mpsc::Sender<Request>,
    joined: bool,
}

/// The group thread that owns `group_id`, among `shards`: a stable hash
/// of the id, so every connection of a group meets the same thread.
fn shard_of(group_id: &str, shards: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    group_id.hash(&mut hasher);
    (hasher.finish() % shards.max(1) as u64) as usize
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
    let writer = tokio::spawn(write_loop(
        sink,
        out_rx,
        pong_interval,
        state.stats.clone(),
        params.wsid.clone(),
    ));
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
    let requests = state.requests[shard_of(&group_id, state.requests.len())].clone();
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
        requests: requests.clone(),
        joined: false,
    };
    let declared = handshake
        .init
        .as_ref()
        .and_then(|init| init.client_schema.as_ref());
    if let Some(declared) = declared
        && !admits(&state, &conn.params, &out, declared)
    {
        let _ = out.send(Outbound::Close);
        let _ = writer.await;
        return;
    }
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
        lmids,
        reply: reply_tx,
    };
    let refusal = match requests.send(request).await {
        Ok(()) => match reply_rx.await {
            Ok(ConnectReply::Accepted) => None,
            Ok(ConnectReply::Reset { reason }) => Some(reason),
            Err(_) => Some("the group thread is gone".to_owned()),
        },
        Err(_) => Some("the group thread is gone".to_owned()),
    };
    if let Some(reason) = refusal {
        log_event!(
            Level::Info,
            "client told to start over",
            wsid = wsid,
            group = group_id,
            cookie = conn.params.base_cookie.as_deref().unwrap_or(""),
            reason = reason
        );
        send(
            &out,
            protocol::error(
                "InvalidConnectionRequestBaseCookie",
                &format!("{reason}; start a fresh sync"),
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
        let opened = Instant::now();
        conn.state
            .stats
            .connections_opened
            .fetch_add(1, Ordering::Relaxed);
        conn.state
            .stats
            .connections_open
            .fetch_add(1, Ordering::Relaxed);
        log_event!(
            Level::Info,
            "connection opened",
            wsid = conn.params.wsid,
            group = conn.params.group_id,
            client = conn.params.client_id,
            authenticated = conn.identity.token.is_some() || conn.identity.cookie.is_some(),
            origin = conn.identity.origin.as_deref().unwrap_or("")
        );
        let reason = conn.read_loop(stream).await;
        conn.state
            .stats
            .connections_open
            .fetch_sub(1, Ordering::Relaxed);
        match reason {
            "client" => &conn.state.stats.connections_closed_by_client,
            "error" => &conn.state.stats.connections_closed_by_error,
            _ => &conn.state.stats.connections_closed_by_server,
        }
        .fetch_add(1, Ordering::Relaxed);
        log_event!(
            Level::Info,
            "connection closed",
            wsid = conn.params.wsid,
            group = conn.params.group_id,
            client = conn.params.client_id,
            reason = reason,
            seconds = format!("{:.1}", opened.elapsed().as_secs_f64())
        );
    }
    let _ = requests
        .send(Request::Disconnect {
            group: group_id,
            wsid,
        })
        .await;
    let _ = out.send(Outbound::Close);
    let _ = writer.await;
}

/// Whether the server can serve a client that declares `declared`. When
/// it cannot, the client is told everything that does not fit
/// (`SchemaVersionNotSupported`, on which a Zero client reloads for a build
/// that fits, as it does with zero-cache), and the refusal is counted and
/// logged with its first findings; the caller closes the connection.
fn admits(
    state: &AppState,
    params: &ConnectParams,
    out: &mpsc::UnboundedSender<Outbound>,
    declared: &protocol::ClientSchema,
) -> bool {
    let found = schema::mismatches(&state.catalog.load(), declared);
    if found.is_empty() {
        return true;
    }
    state
        .stats
        .connections_refused_schema
        .fetch_add(1, Ordering::Relaxed);
    log_event!(
        Level::Warn,
        "connection refused",
        wsid = params.wsid,
        group = params.group_id,
        client = params.client_id,
        kind = schema::KIND,
        mismatches = found.len(),
        first = found.iter().take(3).cloned().collect::<Vec<_>>().join(" ")
    );
    send(out, protocol::error(schema::KIND, &found.join("\n")));
    false
}

/// Queue one text frame.
fn send(out: &mpsc::UnboundedSender<Outbound>, frame: String) {
    let _ = out.send(Outbound::Text(frame.into()));
}

/// The writer: frames in order, a poke's frames fed together and flushed
/// once, a `pong` of the server's own whenever the downstream has been
/// quiet for `pong_interval`, a ping frame on request.
async fn write_loop(
    mut sink: SplitSink<WebSocket, Message>,
    mut frames: mpsc::UnboundedReceiver<Outbound>,
    pong_interval: Duration,
    stats: Arc<Stats>,
    wsid: String,
) {
    let mut last_sent = Instant::now();
    let mut idle = tokio::time::interval(pong_interval.max(Duration::from_millis(100)));
    idle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut shutting_down = shutdown().subscribe();
    loop {
        tokio::select! {
            changed = shutting_down.changed(), if !*shutting_down.borrow() => {
                if changed.is_ok() && *shutting_down.borrow() {
                    tokio::time::sleep(goodbye_after(&wsid)).await;
                    let _ = sink
                        .send(Message::Close(Some(CloseFrame {
                            code: close_code::AWAY,
                            reason: Utf8Bytes::from_static("server shutting down"),
                        })))
                        .await;
                    break;
                }
            }
            frame = frames.recv() => match frame {
                Some(Outbound::Text(text)) => {
                    if sink.send(Message::Text(Utf8Bytes::from(&*text))).await.is_err() {
                        break;
                    }
                    last_sent = Instant::now();
                }
                Some(Outbound::Poke { frames: poke, since, sent }) => {
                    let mut failed = false;
                    for frame in poke.iter() {
                        let Ok(text) = Utf8Bytes::try_from(frame.clone()) else {
                            continue;
                        };
                        if sink.feed(Message::Text(text)).await.is_err() {
                            failed = true;
                            break;
                        }
                    }
                    if failed || sink.flush().await.is_err() {
                        break;
                    }
                    last_sent = Instant::now();
                    stats.groups_to_socket.record(last_sent.duration_since(sent));
                    if let Some(since) = since {
                        stats.end_to_end.record(last_sent.duration_since(since));
                    }
                    stats.frames.fetch_add(poke.len() as u64, Ordering::Relaxed);
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
    async fn read_loop(&mut self, mut stream: SplitStream<WebSocket>) -> &'static str {
        let ping_interval = self.state.config.ping_interval.max(Duration::from_secs(1));
        let timeout = self.state.config.client_timeout;
        let mut last_inbound = Instant::now();
        let mut ticker = tokio::time::interval(ping_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        let mut reason = "server";
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
                        reason = "client";
                        break;
                    }
                    Some(Err(error)) => {
                        log_debug!("connection {}: {error}", self.params.wsid);
                        reason = "error";
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
        reason
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
            Upstream::ChangeDesiredQueries(ops) => self.desired(ops).await,
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
                if self.requests.send(request).await.is_ok()
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

    /// The first message: the client's schema (judged here unless it came
    /// with the connect and already was), endpoint overrides, deleted
    /// clients, the desired queries. False when the schema is one the
    /// server cannot serve, which ends the connection.
    async fn init(&mut self, init: InitConnection, judged: bool) -> bool {
        if let Some(declared) = &init.client_schema
            && !judged
            && !admits(&self.state, &self.params, &self.out, declared)
        {
            return false;
        }
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
        self.desired(init.desired_queries_patch).await
    }

    /// Clients the client says are gone: their queries go, and the client
    /// hears back which were dropped.
    async fn delete_clients(&mut self, deleted: &DeleteClients) {
        if !deleted.client_ids.is_empty() {
            let _ = self
                .requests
                .send(Request::DeleteClients {
                    group: self.params.group_id.clone(),
                    clients: deleted.client_ids.clone(),
                })
                .await;
        }
        send(&self.out, protocol::delete_clients(deleted));
    }

    /// Wait until the server serves queries; false only when every group
    /// thread is gone, which ends the connection.
    async fn await_ready(&self) -> bool {
        if *self.state.ready.borrow() {
            return true;
        }
        let mut ready = self.state.ready.clone();
        ready.wait_for(|ready| *ready).await.is_ok()
    }

    /// Desired-query changes: custom queries go to the application server
    /// for their ASTs, every AST is translated and planned here, and
    /// everything goes to the group thread in the order the client sent
    /// it. A query that cannot be translated or planned is refused to the
    /// client from here and passed on as such, so the group still knows
    /// the hash.
    async fn desired(&mut self, ops: Vec<QueryPatchOp>) -> bool {
        if !self.await_ready().await {
            return false;
        }
        let mut pending: Vec<Pending> = Vec::with_capacity(ops.len());
        let mut requests: Vec<Json> = Vec::new();
        let mut positions: HashMap<String, usize> = HashMap::new();
        let mut asked: HashMap<String, (String, Json)> = HashMap::new();
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
                    let mut ast = ast;
                    if ast.is_none() {
                        let args = Json::Array(args.unwrap_or_default());
                        match self.state.transforms.lookup(&self.identity, &name, &args) {
                            Some(cached) => {
                                self.state
                                    .stats
                                    .transform_hits
                                    .fetch_add(1, Ordering::Relaxed);
                                ast = Some(cached);
                            }
                            None => {
                                self.state
                                    .stats
                                    .transform_misses
                                    .fetch_add(1, Ordering::Relaxed);
                                positions.insert(hash.clone(), pending.len());
                                requests.push(json!({"id": hash, "name": name, "args": args}));
                                asked.insert(hash.clone(), (name.clone(), args));
                            }
                        }
                    }
                    pending.push(Pending::Put {
                        hash,
                        name,
                        ttl,
                        ast,
                    });
                }
                QueryPatchOp::Del { hash } => pending.push(Pending::Del { hash }),
                QueryPatchOp::Clear => pending.push(Pending::Clear),
            }
        }
        let mut errored = Vec::new();
        if !requests.is_empty() {
            let ids: Vec<String> = positions.keys().cloned().collect();
            let started = Instant::now();
            let outcome = self.state.backend.transform(&self.identity, requests).await;
            self.state.stats.transform.record(started.elapsed());
            match outcome {
                TransformOutcome::Queries(results) => {
                    for result in results {
                        let Some(id) = result.get("id").and_then(Json::as_str) else {
                            continue;
                        };
                        let Some(&position) = positions.get(id) else {
                            continue;
                        };
                        if let Some(ast) = result.get("ast") {
                            if let Some((name, args)) = asked.get(id) {
                                self.state.transforms.store(
                                    &self.identity,
                                    name,
                                    args,
                                    ast.clone(),
                                );
                            }
                            if let Pending::Put { ast: slot, .. } = &mut pending[position] {
                                *slot = Some(ast.clone());
                            }
                        } else {
                            log_warn!(
                                "connection {}: query {id} errored at the application server: {}",
                                self.params.wsid,
                                result.get("message").and_then(Json::as_str).unwrap_or("")
                            );
                            self.state
                                .stats
                                .transform_errors
                                .fetch_add(1, Ordering::Relaxed);
                            errored.push(result);
                        }
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
        let state = &*self.state;
        let planned = futures_util::future::join_all(pending.iter().map(|op| async move {
            match op {
                Pending::Put {
                    ast: Some(ast),
                    name,
                    ..
                } => Some(plan_ast(state, name, ast.clone()).await),
                _ => None,
            }
        }))
        .await;
        let mut prepared: Vec<DesiredOp> = Vec::with_capacity(pending.len());
        for (op, planned) in pending.into_iter().zip(planned) {
            match op {
                Pending::Put {
                    hash,
                    name,
                    ttl,
                    ast,
                } => {
                    let planned = match (ast, planned) {
                        (None, _) => None,
                        (Some(_), Some(Ok(translated))) => Some(Ok(Box::new(translated))),
                        (Some(_), failed) => {
                            let reason = planned_failure(failed);
                            let kind = self.state.stats.note_refusal(&name, &reason);
                            log_event!(
                                Level::Warn,
                                "query refused",
                                name = name,
                                hash = hash,
                                kind = kind,
                                at = "plan",
                                group = self.params.group_id,
                                connection = self.params.wsid,
                                reason = reason
                            );
                            errored.push(protocol::errored_query(&hash, &name, &reason));
                            Some(Err(reason))
                        }
                    };
                    prepared.push(DesiredOp::Put {
                        hash,
                        name,
                        ttl,
                        planned,
                    });
                }
                Pending::Del { hash } => prepared.push(DesiredOp::Del { hash }),
                Pending::Clear => prepared.push(DesiredOp::Clear),
            }
        }
        if !errored.is_empty() {
            send(&self.out, protocol::transform_error(errored));
        }
        let request = Request::Desired {
            group: self.params.group_id.clone(),
            client: self.params.client_id.clone(),
            ops: prepared,
        };
        self.requests.send(request).await.is_ok()
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
        let started = Instant::now();
        for mutation in &push.mutation_ids {
            self.state.stats.push_sent(&mutation.client_id, mutation.id);
        }
        let outcome = self.state.backend.push(&self.identity, &push.body).await;
        let elapsed = started.elapsed();
        self.state.stats.push.record(elapsed);
        let failed = matches!(outcome, PushOutcome::Failed { .. });
        if failed {
            self.state
                .stats
                .pushes_failed
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.state.stats.pushes_ok.fetch_add(1, Ordering::Relaxed);
        }
        log_event!(
            if failed || elapsed >= self.state.config.slow_query {
                Level::Warn
            } else {
                Level::Debug
            },
            "push forwarded",
            wsid = self.params.wsid,
            group = self.params.group_id,
            mutations = push.mutation_ids.len(),
            ms = format!("{:.1}", elapsed.as_secs_f64() * 1000.0),
            failed = failed
        );
        match outcome {
            PushOutcome::Response(json) => {
                if let Some(mutations) = json.get("mutations") {
                    send(
                        &self.out,
                        protocol::push_response(
                            json!({"mutations": mutations}),
                            &self.params.client_id,
                        ),
                    );
                } else if json.get("error").is_some() {
                    send(
                        &self.out,
                        protocol::push_response(json, &self.params.client_id),
                    );
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

/// One AST into the tree the engine registers: translated against the
/// catalog, then planned (the cache first, the counts otherwise); a shape
/// that translates is kept for the next process's warm start.
async fn plan_ast(state: &AppState, name: &str, ast: Json) -> Result<Translated, String> {
    let parsed: Ast = Ast::deserialize(&ast).map_err(|error| format!("malformed AST: {error}"))?;
    let started = Instant::now();
    let translated = ast::translate(&parsed, &state.catalog.load())?;
    state.warm.record(name, &ast);
    let planned = plan::plan(
        translated,
        state.config.policy(),
        &state.plans,
        &*state.storage,
    )
    .await;
    let elapsed = started.elapsed();
    state.stats.plan.record(elapsed);
    if let Ok(translated) = &planned {
        let page_drives = plan::page_drives(&translated.query);
        if page_drives {
            state
                .stats
                .plans_page_driven
                .fetch_add(1, Ordering::Relaxed);
        }
        log_event!(
            Level::Debug,
            "query planned",
            name = name,
            table = translated.query.main_table.table,
            page_drives = page_drives,
            ms = format!("{:.2}", elapsed.as_secs_f64() * 1000.0)
        );
    }
    planned
}

/// The reason a planned put failed, or the absence of a plan spelled out.
fn planned_failure(planned: Option<Result<Translated, String>>) -> String {
    match planned {
        Some(Err(reason)) => reason,
        _ => "the query was not planned".to_owned(),
    }
}

/// Reads a client group's last mutation ids at connect time, on the reads
/// pool, over the engine side's storage handle.
pub struct LmidReader {
    storage: Arc<PgStorage>,
    table: String,
}

impl LmidReader {
    /// A reader of `<schema>.clients` through `storage`.
    pub fn new(storage: Arc<PgStorage>, schema: &str) -> Self {
        LmidReader {
            storage,
            table: format!("{}.{}", quote_ident(schema), quote_ident("clients")),
        }
    }

    /// The last mutation id of every client of `group`; empty when the
    /// table cannot be read (the group then starts from the feed's word).
    pub async fn lmids(&self, group: &str) -> Vec<(String, i64)> {
        let sql = format!(
            "SELECT \"clientID\", \"lastMutationID\" FROM {} WHERE \"clientGroupID\" = {}",
            self.table,
            quote_literal(group)
        );
        match self.storage.simple_query(&sql).await {
            Ok(rows) => rows
                .iter()
                .filter_map(|row| {
                    let client = row.get(0)?.to_owned();
                    let lmid = row.get(1)?.parse::<i64>().ok()?;
                    Some((client, lmid))
                })
                .collect(),
            Err(error) => {
                log_warn!("reading last mutation ids of {group}: {error}");
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The health check refuses until the feed is past the first snapshot
    /// and answers `ok` after it, so a process manager waits for the feed,
    /// not the socket.
    #[test]
    fn health_follows_readiness() {
        assert_eq!(readiness(false).0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(readiness(true), (StatusCode::OK, "ok"));
    }
}
