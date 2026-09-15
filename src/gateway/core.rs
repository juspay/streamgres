//! The engine thread of the gateway: the one owner of the runtime, the
//! decoder of the change feed, the storage reads, and every client
//! group's view. Connections hand it requests over a channel (connect,
//! disconnect, desired queries, pulls); the feed thread hands it raw
//! replication events; it hands each group's connections finished poke
//! frames. Nothing here blocks: reads run as tasks on this thread's local
//! set and land when they return.
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
//! it reconnects, and a cookie the gateway does not hold (it keeps no
//! history) resets the client to a fresh sync.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

use pgwire_replication::ReplicationEvent;
use serde_json::{Map, Value as Json, json};
use tokio::sync::{mpsc, oneshot};
use tokio::task::spawn_local;

use super::ast::{self, Ast, Translated};
use super::config::Config;
use super::log::{gw_debug, gw_error, gw_info, gw_warn};
use super::protocol::{self, ClientSchema};
use super::wire;
use crate::ivm::{ClientUpdate, Engine, Fetch, FetchId, MultiTableIVM, QueryPart};
use crate::model::{
    Catalog, ClientId, DataFrameKey, DataFrameOperation, DataFrameRow, Snapshot, SubId, TableName,
    Value, WriteQuery,
};
use crate::sync::pg::{Feed, PgStorage};
use crate::sync::{Runtime, Sources, Storage, StorageError};

/// One frame for one connection.
#[derive(Debug, Clone)]
pub enum Outbound {
    Text(Arc<str>),
    Close,
}

/// A connection's handle: the client it speaks for and where its frames
/// go.
#[derive(Debug, Clone)]
pub struct Socket {
    pub client: String,
    pub sink: mpsc::UnboundedSender<Outbound>,
}

/// One change to a client's desired queries, ready for the engine: a
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

/// What a connection asks of the engine thread.
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
}

/// The engine thread's answer to a connection.
#[derive(Debug)]
pub enum ConnectReply {
    Accepted,
    /// The client's cookie is not one the gateway holds; the client must
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
struct QueryState {
    sub: Option<SubId>,
    hidden: HashSet<QueryPart>,
    got: bool,
}

/// One client group's view.
struct Group {
    client: ClientId,
    version: u64,
    generation: u64,
    sockets: HashMap<String, Socket>,
    desired: HashMap<String, HashSet<String>>,
    queries: HashMap<String, QueryState>,
    subs: HashSet<SubId>,
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

/// The engine thread's state.
pub struct Core {
    config: Arc<Config>,
    catalog: Rc<Catalog>,
    runtime: Runtime<MultiTableIVM>,
    storage: Rc<Sources>,
    feed: Feed,
    ready: bool,
    backlog: Vec<Request>,
    groups: HashMap<String, Group>,
    by_client: HashMap<ClientId, String>,
    next_client: u64,
    pending: HashMap<ClientId, Vec<ClientUpdate>>,
    lmid_changes: HashMap<String, HashMap<String, i64>>,
    clients_table: TableName,
    requests: mpsc::Sender<Request>,
    report: mpsc::UnboundedSender<(FetchId, Result<Snapshot, StorageError>)>,
    next_poke: u64,
}

/// Run the engine thread until every request sender and the feed are
/// gone. Must run inside a `LocalSet`.
pub async fn run(
    config: Arc<Config>,
    catalog: Catalog,
    mut requests_rx: mpsc::Receiver<Request>,
    requests: mpsc::Sender<Request>,
    mut events: mpsc::Receiver<ReplicationEvent>,
) -> Result<(), String> {
    let catalog = Rc::new(catalog);
    let pg = PgStorage::connect(&config.dsn, catalog.clone())
        .await
        .map_err(|error| format!("connecting the storage: {error}"))?
        .with_rotation(config.snapshot_rotation);
    let cached = Sources::cached_from_env();
    let storage = Rc::new(Sources::new(Rc::new(pg), catalog.clone(), cached.clone()));
    if !cached.is_empty() {
        let rows = storage
            .warm()
            .await
            .map_err(|error| format!("warming the memory tables: {error}"))?;
        gw_info!("memory tables warmed with {rows} rows");
    }
    let (report, mut results) = mpsc::unbounded_channel();
    let mut core = Core {
        clients_table: TableName::from(config.clients_table().as_str()),
        config,
        feed: Feed::new(catalog.clone()),
        catalog,
        runtime: Runtime::new(MultiTableIVM::new()),
        storage,
        ready: false,
        backlog: Vec::new(),
        groups: HashMap::new(),
        by_client: HashMap::new(),
        next_client: 1,
        pending: HashMap::new(),
        lmid_changes: HashMap::new(),
        requests,
        report,
        next_poke: 1,
    };
    gw_info!("engine thread up; waiting for the first heartbeat before serving queries");
    loop {
        tokio::select! {
            request = requests_rx.recv() => match request {
                Some(request) => core.handle(request),
                None => break,
            },
            event = events.recv() => match event {
                Some(event) => core.event(event),
                None => return Err("the change feed ended".to_owned()),
            },
            result = results.recv() => match result {
                Some((id, result)) => core.landed(id, result),
                None => break,
            },
        }
    }
    Ok(())
}

impl Core {
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
        }
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
        let known = self
            .groups
            .get(group_id)
            .map(|group| protocol::cookie(group.version));
        match (&known, &base_cookie) {
            (None, Some(_)) => {
                return ConnectReply::Reset {
                    reason: "the server holds no state for this client group".to_owned(),
                };
            }
            (Some(current), Some(offered)) if current != offered => {
                return ConnectReply::Reset {
                    reason: format!("the server is at {current}, the client at {offered}"),
                };
            }
            (Some(_), None) => self.drop_group(group_id),
            _ => {}
        }
        if !self.groups.contains_key(group_id) {
            let client = ClientId(self.next_client);
            self.next_client += 1;
            self.groups.insert(group_id.to_owned(), Group::new(client));
            self.by_client.insert(client, group_id.to_owned());
            gw_info!(
                "client group {group_id} opened as engine client {}",
                client.0
            );
        }
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
        group.generation += 1;
        gw_info!(
            "connection {wsid} joined client group {group_id} as client {}",
            socket.client
        );
        group.sockets.insert(wsid, socket);
        ConnectReply::Accepted
    }

    /// A connection closed; when it was the group's last, the group's
    /// subscriptions stay for the configured grace period.
    fn disconnect(&mut self, group_id: &str, wsid: &str) {
        let Some(group) = self.groups.get_mut(group_id) else {
            return;
        };
        group.sockets.remove(wsid);
        gw_info!("connection {wsid} left client group {group_id}");
        if group.sockets.is_empty() {
            group.generation += 1;
            let generation = group.generation;
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
            gw_info!("client group {group_id} expired; its subscriptions are released");
            self.drop_group(group_id);
        }
    }

    /// Forget a group and every subscription it had.
    fn drop_group(&mut self, group_id: &str) {
        if let Some(group) = self.groups.remove(group_id) {
            self.runtime.unregister_client(group.client);
            self.by_client.remove(&group.client);
            self.pending.remove(&group.client);
            self.lmid_changes.remove(group_id);
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
            gw_warn!("desired queries for an unknown client group {group_id}");
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
        let group = self.groups.get_mut(group_id).expect("checked");
        group
            .desired
            .entry(client.to_owned())
            .or_default()
            .insert(hash.clone());
        let mut echo = json!({"op": "put", "hash": hash});
        if let Some(ttl) = ttl {
            echo["ttl"] = json!(ttl);
        }
        group
            .queued_desired
            .entry(client.to_owned())
            .or_default()
            .push(echo);
        if group.queries.contains_key(&hash) {
            return;
        }
        let Some(ast) = ast else {
            group.queries.insert(
                hash,
                QueryState {
                    sub: None,
                    hidden: HashSet::new(),
                    got: false,
                },
            );
            return;
        };
        let translated: Result<Translated, String> = serde_json::from_value::<Ast>(ast)
            .map_err(|error| format!("malformed AST: {error}"))
            .and_then(|ast| ast::translate(&ast, &self.catalog));
        match translated {
            Ok(translated) => {
                let engine_client = group.client;
                let (sub, step) = self.runtime.register(engine_client, translated.query);
                let group = self.groups.get_mut(group_id).expect("checked");
                group.subs.insert(sub);
                group.queries.insert(
                    hash.clone(),
                    QueryState {
                        sub: Some(sub),
                        hidden: translated.hidden,
                        got: false,
                    },
                );
                gw_debug!(
                    "group {group_id}: query {name} ({hash}) registered as {}",
                    sub.0
                );
                self.absorb_step(step, false);
            }
            Err(message) => {
                gw_warn!("group {group_id}: query {name} ({hash}) cannot run here: {message}");
                group.queries.insert(
                    hash.clone(),
                    QueryState {
                        sub: None,
                        hidden: HashSet::new(),
                        got: false,
                    },
                );
                let frame: Arc<str> =
                    protocol::transform_error(vec![protocol::errored_query(&hash, name, &message)])
                        .into();
                group.send_to_client(client, &frame);
            }
        }
    }

    /// One client no longer desires `hash`; unregister it when nobody in
    /// the group does.
    fn del(&mut self, group_id: &str, client: &str, hash: &str) {
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
                self.unsubscribe(group_id, &hash);
            }
        }
    }

    /// Remove a query from its group: its subscription goes, the rows only
    /// it held are `del`ed, and its `got` is withdrawn.
    fn unsubscribe(&mut self, group_id: &str, hash: &str) {
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
        self.runtime.unregister(sub);
        gw_debug!("group {group_id}: query {hash} unregistered ({})", sub.0);
    }

    /// One raw replication event: a commit's writes route through the
    /// engine and the commit's position closes the step with a flush.
    fn event(&mut self, event: ReplicationEvent) {
        match self.feed.absorb(event) {
            Ok(Some(transaction)) => {
                let at = transaction.at;
                for write in transaction.writes {
                    self.write(write, at);
                }
                self.progress();
            }
            Ok(None) => {}
            Err(error) => {
                gw_error!("change feed decoding failed: {error}");
                std::process::exit(1);
            }
        }
    }

    /// One write of a transaction: recorded for the mutation-id table,
    /// mirrored into storage, routed.
    fn write(&mut self, write: WriteQuery, at: crate::model::Lsn) {
        if write.table() == &self.clients_table {
            self.note_lmid(&write);
        }
        self.storage.absorb(&write, at);
        let step = self.runtime.write(&write, at);
        self.moved();
        self.absorb_step(step, false);
    }

    /// The feed's position moved: tell the runtime, flush the transaction,
    /// and start serving once the storage's snapshots are covered.
    fn progress(&mut self) {
        let step = self.runtime.progress(self.feed.progress());
        self.moved();
        self.absorb_step(step, true);
        let floor = self.storage.floor();
        if !self.ready && floor.0 > 0 && self.runtime.position() >= floor {
            self.ready = true;
            gw_info!(
                "serving: engine at {}, storage snapshot at {}",
                self.runtime.position(),
                self.storage.floor()
            );
            let backlog = std::mem::take(&mut self.backlog);
            for request in backlog {
                self.handle(request);
            }
        }
    }

    /// The stream moved: tell the storage, and learn its floor.
    fn moved(&mut self) {
        self.storage.advance(self.runtime.position());
        self.runtime.set_floor(self.storage.floor());
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
        self.lmid_changes
            .entry(group)
            .or_default()
            .insert(client, lmid);
    }

    /// A storage read returned.
    fn landed(&mut self, id: FetchId, result: Result<Snapshot, StorageError>) {
        let step = match result {
            Ok(snapshot) => self.runtime.fetched(id, snapshot),
            Err(error) => {
                gw_warn!("storage read {} failed, parked: {error}", id.0);
                self.runtime.failed(id)
            }
        };
        self.absorb_step(step, true);
    }

    /// Take a step's reads and updates; flush the pokes when asked.
    fn absorb_step(&mut self, step: crate::sync::Step, flush: bool) {
        for fetch in step.selects {
            self.spawn_read(fetch);
        }
        for update in step.updates {
            self.pending.entry(update.client).or_default().push(update);
        }
        if flush {
            self.flush();
        }
    }

    /// Run one read as its own task, reporting the result into the loop.
    fn spawn_read(&self, fetch: Fetch) {
        let storage = self.storage.clone();
        let report = self.report.clone();
        spawn_local(async move {
            let result = storage.select(&fetch.query).await;
            let _ = report.send((fetch.id, result));
        });
    }

    /// Turn everything accumulated into one poke per group that has
    /// anything to hear.
    fn flush(&mut self) {
        let mut touched: HashSet<String> = HashSet::new();
        for client in self.pending.keys() {
            if let Some(group) = self.by_client.get(client) {
                touched.insert(group.clone());
            }
        }
        touched.extend(self.lmid_changes.keys().cloned());
        for (id, group) in &self.groups {
            let waiting = !group.queued_desired.is_empty()
                || !group.queued_got.is_empty()
                || !group.queued_lmids.is_empty()
                || !group.queued_rows.is_empty()
                || group
                    .queries
                    .values()
                    .any(|query| !query.got && query.sub.is_some());
            if waiting {
                touched.insert(id.clone());
            }
        }
        let stray: Vec<ClientId> = self
            .pending
            .keys()
            .filter(|client| !self.by_client.contains_key(client))
            .copied()
            .collect();
        for client in stray {
            self.pending.remove(&client);
        }
        let mut touched: Vec<String> = touched.into_iter().collect();
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
                    !group.queries.values().any(|query| {
                        query.sub == Some(target.sub) && query.hidden.contains(&target.part)
                    })
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
        let mut got = std::mem::take(&mut group.queued_got);
        for (hash, query) in group.queries.iter_mut() {
            if !query.got
                && query
                    .sub
                    .is_some_and(|sub| self.runtime.engine().hydrated(sub))
            {
                query.got = true;
                got.push(json!({"op": "put", "hash": hash}));
            }
        }
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
        let base = protocol::cookie(group.version);
        group.version += 1;
        let cookie = protocol::cookie(group.version);
        let mut frames: Vec<Arc<str>> = Vec::new();
        frames.push(protocol::poke_start(&poke_id, Some(&base)).into());
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
        gw_debug!(
            "group {group_id}: poke {poke_id} {base} -> {cookie}: {puts} puts, {dels} dels, {got_count} got, {} lmids",
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
