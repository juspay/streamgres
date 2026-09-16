//! Every client group's view, and the one thread that keeps it. It owns
//! no engine, no storage and no database connection: it sends the engine
//! side subscribe and unsubscribe commands and reads back that side's
//! events (deltas, hydrated subscriptions, landings, stream progress),
//! turning both into the pokes a Zero client expects. Connections hand it
//! requests over a channel (connect, disconnect, desired queries, pulls)
//! and receive finished frames.
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
//! which the engine side copies here as one batch per transaction; the
//! batch is taken when that transaction reports its progress, so a
//! mutation's rows and its id go out in the same poke and no later
//! transaction's id rides out early.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value as Json, json};
use tokio::sync::{mpsc, oneshot};
use tokio::task::spawn_local;

use super::ast::{self, Ast, Translated};
use super::config::Config;
use super::plan::{Planner, Policy, Step};
use super::protocol::{self, ClientSchema};
use super::wire;
use crate::ivm::{ClientUpdate, QueryPart};
use crate::log::{log_debug, log_info, log_warn};
use crate::model::{
    Catalog, ClientId, DataFrameKey, DataFrameOperation, DataFrameRow, Lsn, MultiTableReadQuery,
    SubId, TableName, Value, WriteQuery,
};
use crate::sync::{Command, Event};

/// One frame for one connection: a text frame, a WebSocket ping (the
/// liveness probe a browser answers on its own), or the close.
#[derive(Debug, Clone)]
pub enum Outbound {
    Text(Arc<str>),
    Ping,
    Close,
}

/// A connection's handle: the client it speaks for and where its frames
/// go.
#[derive(Debug, Clone)]
pub struct Socket {
    pub client: String,
    pub sink: mpsc::UnboundedSender<Outbound>,
}

/// One change to a client's desired queries, ready for this thread: a
/// custom query already transformed into its AST, or the reason it could
/// not be.
#[derive(Debug)]
pub enum DesiredOp {
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

/// One query between its translation and its registration: the planner
/// deciding which side of its joins is read whole, and where its answer
/// goes.
struct Planning {
    group: String,
    client: String,
    hash: String,
    name: String,
    planner: Planner,
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
    rows: HashMap<(TableName, DataFrameKey), HashSet<(SubId, QueryPart)>>,
    lmids: HashMap<String, i64>,
    columns: Option<HashMap<String, HashSet<String>>>,
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

    /// Send one frame to every connection of the group.
    fn broadcast(&self, frame: &Arc<str>) {
        for socket in self.sockets.values() {
            let _ = socket.sink.send(Outbound::Text(frame.clone()));
        }
    }

    /// Send one frame to the connections of one client.
    fn send_to_client(&self, client: &str, frame: &Arc<str>) {
        for socket in self
            .sockets
            .values()
            .filter(|socket| socket.client == client)
        {
            let _ = socket.sink.send(Outbound::Text(frame.clone()));
        }
    }

    /// Remember the client's schema as the columns to ship per table.
    fn adopt_schema(&mut self, schema: &ClientSchema) {
        self.columns = Some(
            schema
                .tables
                .iter()
                .map(|(table, spec)| (table.clone(), spec.columns.keys().cloned().collect()))
                .collect(),
        );
    }
}

/// The client-group thread's state.
pub struct Groups {
    config: Arc<Config>,
    catalog: Rc<Catalog>,
    /// Commands to the engine side, in the order they were made.
    commands: mpsc::UnboundedSender<Command<MultiTableReadQuery>>,
    requests: mpsc::Sender<Request>,
    ready: bool,
    backlog: Vec<Request>,
    groups: HashMap<String, Group>,
    by_client: HashMap<ClientId, String>,
    by_sub: HashMap<SubId, (String, String)>,
    /// Registrations in flight, by the token they were sent with.
    awaiting: HashMap<u64, (String, String)>,
    /// Plans waiting on a count, by the token the count was asked with.
    plans: HashMap<u64, Planning>,
    next_client: u64,
    pending: HashMap<ClientId, Vec<ClientUpdate>>,
    lmid_changes: HashMap<String, HashMap<String, i64>>,
    /// Groups with something to hear at the next flush.
    dirty: HashSet<String>,
    clients_table: TableName,
    next_poke: u64,
    next_generation: u64,
}

/// Run the client-group thread until every request sender is gone. Must
/// run inside a `LocalSet`.
pub async fn run(
    config: Arc<Config>,
    catalog: Rc<Catalog>,
    engine: crate::sync::pg::Started,
    mut requests_rx: mpsc::Receiver<Request>,
    requests: mpsc::Sender<Request>,
) {
    let crate::sync::pg::Started {
        commands,
        mut events,
        mut watched,
    } = engine;
    let (outbox, outbox_rx) = mpsc::unbounded_channel();
    spawn_local(forward(outbox_rx, commands));
    let mut core = Groups {
        clients_table: TableName::from(config.clients_table().as_str()),
        config,
        catalog,
        commands: outbox,
        requests,
        ready: false,
        backlog: Vec::new(),
        groups: HashMap::new(),
        by_client: HashMap::new(),
        by_sub: HashMap::new(),
        awaiting: HashMap::new(),
        plans: HashMap::new(),
        next_client: 1,
        pending: HashMap::new(),
        lmid_changes: HashMap::new(),
        dirty: HashSet::new(),
        next_poke: 1,
        next_generation: 0,
    };
    log_info!("client groups up; waiting for the first heartbeat before serving queries");
    loop {
        tokio::select! {
            request = requests_rx.recv() => match request {
                Some(request) => core.handle(request),
                None => break,
            },
            event = events.recv() => match event {
                Some(event) => core.event(event, &mut watched),
                None => break,
            },
        }
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
                self.flush();
            }
            Request::Disconnect { group, wsid } => self.disconnect(&group, &wsid),
            Request::Desired { .. } if !self.ready => self.backlog.push(request),
            Request::Desired {
                group,
                client,
                schema,
                ops,
            } => {
                self.desired(&group, &client, schema, ops);
                self.flush();
            }
            Request::DeleteClients { group, clients } => {
                for client in clients {
                    self.clear_client(&group, &client);
                }
                self.flush();
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
            } => {
                self.expire_query(&group, &hash, generation);
                self.flush();
            }
        }
    }

    /// One event from the engine side.
    fn event(&mut self, event: Event, watched: &mut mpsc::UnboundedReceiver<Vec<WriteQuery>>) {
        match event {
            Event::Registered { token, sub } => self.registered(token, sub),
            Event::Updates(updates) => {
                for update in updates {
                    if let Some(group) = self.by_client.get(&update.client) {
                        self.dirty.insert(group.clone());
                    }
                    self.pending.entry(update.client).or_default().push(update);
                }
            }
            // A query completes either because a read landed (an
            // `Event::Landed` follows) or because an existing tree already
            // held its rows, in which case nothing else would poke the
            // group until the next write or heartbeat: flush here.
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
                        group.queued_got.push(json!({"op": "put", "hash": hash}));
                        self.dirty.insert(group_id);
                    }
                }
                self.flush();
            }
            Event::Landed => self.flush(),
            Event::Counted { token, count } => self.counted(token, count),
            Event::Moved { position, floor } => {
                if let Ok(batch) = watched.try_recv() {
                    for write in batch {
                        if write.table() == &self.clients_table {
                            self.note_lmid(&write);
                        }
                    }
                }
                self.flush();
                self.serve_when_covered(position, floor);
            }
        }
    }

    /// The join policy the configuration sets.
    fn policy(&self) -> Policy {
        Policy {
            limit: self.config.join_limit,
            preferred: self.config.join_preferred_side,
        }
    }

    /// Drive one query's plan: ask the engine side for the count it needs
    /// next, or act on its decision.
    fn plan(&mut self, token: u64, mut planning: Planning) {
        match planning.planner.step() {
            Step::Count(query, cap) => {
                log_debug!(
                    "group {}: query {} ({}) counts {} up to {cap}",
                    planning.group,
                    planning.name,
                    planning.hash,
                    query.table
                );
                self.plans.insert(token, planning);
                self.command(Command::Count { query, cap, token });
            }
            Step::Done(Ok(translated)) => self.register_planned(token, planning, translated),
            Step::Done(Err(reason)) => self.refuse(token, planning, &reason),
        }
    }

    /// A count the engine side answered, for a plan in flight.
    fn counted(&mut self, token: u64, count: Result<u64, String>) {
        let Some(mut planning) = self.plans.remove(&token) else {
            return;
        };
        match count {
            Ok(count) => {
                planning.planner.answer(count);
                self.plan(token, planning);
            }
            Err(error) => self.refuse(
                token,
                planning,
                &format!("counting its rows failed: {error}"),
            ),
        }
    }

    /// The plan is in: register the query in the shape the planner chose,
    /// unless the query was released while the plan was being made.
    fn register_planned(&mut self, token: u64, planning: Planning, translated: Translated) {
        let Some(group) = self.groups.get_mut(&planning.group) else {
            return;
        };
        let Some(state) = group.queries.get_mut(&planning.hash) else {
            return;
        };
        if state.awaiting != token {
            return;
        }
        state.hidden = translated.hidden;
        let engine_client = group.client;
        self.awaiting
            .insert(token, (planning.group.clone(), planning.hash.clone()));
        self.command(Command::Register {
            client: engine_client,
            query: translated.query,
            token,
        });
        log_debug!(
            "group {}: query {} ({}) registering",
            planning.group,
            planning.name,
            planning.hash
        );
    }

    /// The plan refused the query: the client is told why, and the query
    /// stays known to the group with no subscription behind it.
    fn refuse(&mut self, token: u64, planning: Planning, reason: &str) {
        log_warn!(
            "group {}: query {} ({}) refused: {reason}",
            planning.group,
            planning.name,
            planning.hash
        );
        let Some(group) = self.groups.get_mut(&planning.group) else {
            return;
        };
        if let Some(state) = group.queries.get_mut(&planning.hash)
            && state.awaiting == token
        {
            state.awaiting = 0;
        }
        let frame: Arc<str> = protocol::transform_error(vec![protocol::errored_query(
            &planning.hash,
            &planning.name,
            reason,
        )])
        .into();
        group.send_to_client(&planning.client, &frame);
    }

    /// Start serving once the engine's position covers the storage's
    /// snapshots; the requests that arrived meanwhile run then.
    fn serve_when_covered(&mut self, position: Lsn, floor: Lsn) {
        if self.ready || floor.0 == 0 || position < floor {
            return;
        }
        self.ready = true;
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
            let client = ClientId(self.next_client);
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
        let group = self.groups.get_mut(group_id).expect("just ensured");
        if let Some(schema) = &schema {
            group.adopt_schema(schema);
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
            self.plans.retain(|_, planning| planning.group != group_id);
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
            self.groups
                .get_mut(group_id)
                .expect("checked")
                .adopt_schema(schema);
        }
        for op in ops {
            match op {
                DesiredOp::Put {
                    hash,
                    name,
                    ttl,
                    ast,
                } => self.put(group_id, client, hash, &name, ttl, ast),
                DesiredOp::Del { hash } => self.del(group_id, client, &hash),
                DesiredOp::Clear => self.clear_client(group_id, client),
            }
        }
    }

    /// One client now desires `hash`; register it for the group when it
    /// is new.
    fn put(
        &mut self,
        group_id: &str,
        client: &str,
        hash: String,
        name: &str,
        ttl: Option<f64>,
        ast: Option<Json>,
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
            return;
        }
        let Some(ast) = ast else {
            group.queries.insert(
                hash,
                QueryState {
                    sub: None,
                    awaiting: 0,
                    hidden: HashSet::new(),
                    got: false,
                    ttl: lifetime,
                    inactive: 0,
                },
            );
            return;
        };
        let translated: Result<Translated, String> = serde_json::from_value::<Ast>(ast)
            .map_err(|error| format!("malformed AST: {error}"))
            .and_then(|ast| ast::translate(&ast, &self.catalog));
        match translated {
            Ok(translated) => {
                self.next_generation += 1;
                let generation = self.next_generation;
                group.queries.insert(
                    hash.clone(),
                    QueryState {
                        sub: None,
                        awaiting: generation,
                        hidden: translated.hidden.clone(),
                        got: false,
                        ttl: lifetime,
                        inactive: 0,
                    },
                );
                let planning = Planning {
                    group: group_id.to_owned(),
                    client: client.to_owned(),
                    hash: hash.clone(),
                    name: name.to_owned(),
                    planner: Planner::new(translated, self.policy()),
                };
                self.plan(generation, planning);
            }
            Err(message) => {
                log_warn!("group {group_id}: query {name} ({hash}) cannot run here: {message}");
                group.queries.insert(
                    hash.clone(),
                    QueryState {
                        sub: None,
                        awaiting: 0,
                        hidden: HashSet::new(),
                        got: false,
                        ttl: lifetime,
                        inactive: 0,
                    },
                );
                let frame: Arc<str> =
                    protocol::transform_error(vec![protocol::errored_query(&hash, name, &message)])
                        .into();
                group.send_to_client(client, &frame);
            }
        }
    }

    /// The engine side registered a query: adopt the subscription, unless
    /// the query was released while the registration was in flight, in
    /// which case it is let go at once.
    fn registered(&mut self, token: u64, sub: SubId) {
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
        let mut emptied = Vec::new();
        for (key, holders) in group.rows.iter_mut() {
            holders.retain(|(holder, _)| *holder != sub);
            if holders.is_empty() {
                emptied.push(key.clone());
            }
        }
        for (table, key) in emptied {
            group.rows.remove(&(table.clone(), key.clone()));
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

    /// Turn everything accumulated into one poke per group that has
    /// anything to hear.
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
        let mut touched: Vec<String> = self.dirty.drain().collect();
        touched.sort();
        for group_id in touched {
            self.poke(&group_id);
        }
    }

    /// Assemble and send one group's poke, if there is anything in it.
    fn poke(&mut self, group_id: &str) {
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
            let slot = (table.clone(), key.clone());
            if adds {
                if holders.is_empty() {
                    continue;
                }
                group.rows.insert(slot, holders);
                if let Some(image) = image {
                    rows.push(RowOp::Put(table, key, image));
                }
            } else {
                let Some(current) = group.rows.get_mut(&slot) else {
                    continue;
                };
                for holder in &holders {
                    current.remove(holder);
                }
                if current.is_empty() {
                    group.rows.remove(&slot);
                    rows.push(RowOp::Del(table, key));
                }
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
        let mut frames: Vec<Arc<str>> = Vec::new();
        frames.push(protocol::poke_start(&poke_id, base.as_deref()).into());
        let got_count = got.len();
        let mut first = Map::new();
        if !desired.is_empty() {
            first.insert("desiredQueriesPatches".to_owned(), json!(desired));
        }
        if !got.is_empty() {
            first.insert("gotQueriesPatch".to_owned(), Json::Array(got));
        }
        if !lmids.is_empty() {
            first.insert("lastMutationIDChanges".to_owned(), json!(lmids));
        }
        let mut first = Some(first);
        if rows.is_empty() {
            frames.push(protocol::poke_part(&poke_id, first.take().unwrap_or_default()).into());
        }
        let mut puts = 0usize;
        let mut dels = 0usize;
        for chunk in rows.chunks(self.config.rows_per_part.max(1)) {
            let mut body = first.take().unwrap_or_default();
            let mut patch = Vec::with_capacity(chunk.len());
            for op in chunk {
                match op {
                    RowOp::Put(table, _, row) => {
                        puts += 1;
                        let allowed = group
                            .columns
                            .as_ref()
                            .and_then(|columns| columns.get(table.as_str()));
                        let value = match self.catalog.table(table.as_str()) {
                            Some(declared) => wire::row_json(row, declared, allowed),
                            None => continue,
                        };
                        patch.push(
                            json!({"op": "put", "tableName": table.as_str(), "value": value}),
                        );
                    }
                    RowOp::Del(table, key) => {
                        dels += 1;
                        patch.push(json!({"op": "del", "tableName": table.as_str(), "id": wire::key_json(key)}));
                    }
                }
            }
            body.insert("rowsPatch".to_owned(), Json::Array(patch));
            frames.push(protocol::poke_part(&poke_id, body).into());
        }
        frames.push(protocol::poke_end(&poke_id, &cookie).into());
        log_debug!(
            "group {group_id}: poke {poke_id} {} -> {cookie}: {puts} puts, {dels} dels, {got_count} got, {} lmids",
            base.as_deref().unwrap_or("null"),
            lmids.len()
        );
        for frame in &frames {
            group.broadcast(frame);
        }
    }
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
