//! Callbacks on a query's changes: a webhook is a subscription.
//!
//! [`EngineClient::subscribe`] follows a Diesel query through the engine
//! and calls back with each change of its result, whoever wrote it (a
//! service, a migration, `psql`):
//!
//! ```ignore
//! let subscription = engine
//!     .subscribe(&database_url, payments::table.filter(payments::status.eq("SUCCESS")),
//!         |change: Change<Payment>| async move {
//!             if let Change::Added(payment) = change {
//!                 notify_merchant(&payment).await;
//!             }
//!         })
//!     .await?;
//! // The subscription lasts as long as `subscription` does.
//! ```
//!
//! What the callback is given:
//! - First, [`Change::Present`] for every row the result holds when the
//!   subscription starts: every commit before the call, once the engine
//!   has received it (it is waited for, up to the verdict timeout; one
//!   received later arrives as added). Nothing falls between a read of
//!   the table and the subscription.
//! - Then, per committed transaction and in commit order, [`Change::Added`]
//!   for a row that entered the result, [`Change::Updated`] for one that
//!   changed and stayed, [`Change::Removed`] for one that left. A row is
//!   one row of the result: one table row, or one composition of a join.
//! - One change at a time: the next waits for the callback's future.
//! - When the engine connection drops and comes back (the engine
//!   restarted, the network failed), the result as the engine has it then
//!   is compared with the last one known, and the difference arrives as
//!   changes. Nothing is lost, but changes made while the connection was
//!   down arrive as their net effect: a row that entered and left
//!   meanwhile is not reported.
//! - The same holds when the callback falls more than [`PENDING_LIMIT`]
//!   changes behind: from then on the pending changes are kept as their
//!   net effect per row until it catches up, and [`Subscription::lagged`]
//!   counts the times.
//! - Not across processes: a new subscription starts with
//!   [`Change::Present`] rows, to reconcile from.
//!
//! [`EngineClient::subscribe_sql`] does the same for a query given as
//! plain SQL, and hands back each row as a [`SqlRow`]: an application
//! whose own Diesel is another version, or a fork, or none, subscribes
//! without naming a Diesel type.
//!
//! ```ignore
//! let subscription = engine
//!     .subscribe_sql(&database_url, "SELECT id, status FROM payments", |change| async move {
//!         if let Change::Updated { new, .. } = change {
//!             println!("payment {:?} is now {:?}", new.text("id"), new.text("status"));
//!         }
//!     })
//!     .await?;
//! ```
//!
//! The rows are the engine's, read with its own role: the subscription
//! is the backend's own code, as an S2S connection is, and a table with
//! row-level security is refused. Only a query the engine serves with its
//! rows can be subscribed: one it follows only by refills (a column type
//! it cannot encode) is refused with the reason. The engine must stream
//! the database `database_url` names, and the query's tables must be
//! ones it follows, bare in `public` or as `schema.table` in another
//! followed schema; otherwise subscribing is refused.
//!
//! A single-table query without `LIMIT` is followed change by change from
//! each delta the engine sends, whatever the size of its result. A join,
//! a `LIMIT` or a `COUNT` is compared whole after each delta: keep those
//! results small.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use diesel::PgConnection;
use diesel::deserialize::{self, FromSql, FromSqlRow};
use diesel::pg::Pg;
use diesel::row::{Field, Row};
use diesel::sql_types::Untyped;
use diesel::query_builder::{Query, QueryFragment, QueryId};
use diesel::query_dsl::CompatibleType;
use diesel::{Connection, QueryResult};
use serde_json::Value as Json;
use tokio::sync::Notify;

use crate::engine::EngineClient;
use crate::key::QueryKey;
use crate::materialized::Materialized;
use crate::encode::oid;
use crate::row::OwnedRow;
use crate::subscriptions::{Subscriptions, Verdict};

/// How many changes may wait for a callback before they are kept as
/// their net effect per row.
pub const PENDING_LIMIT: usize = 10_000;

/// How long a subscribe waits for an engine that names no position to
/// name one ([`EngineClient::catch_up`]).
const UNNAMED_POSITION_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

/// The first context id of a subscription's query: above every id a
/// cache gives its readers' contexts ([`crate::cache::Cache::context_id`]),
/// so a subscription shares no key, and so no rows, with a cache.
pub(crate) const LISTENING_BASE: u32 = 1 << 31;

/// One change of a subscribed query's result, as its Diesel type.
#[derive(Debug, Clone, PartialEq)]
pub enum Change<T> {
    /// A row the result held when the subscription started.
    Present(T),
    /// A row that entered the result.
    Added(T),
    /// A row that changed and is still in the result.
    Updated { old: T, new: T },
    /// A row that left the result, as it was last.
    Removed(T),
}

/// One change as the engine side computes it: a result row's identity,
/// and the row before and after (`None` where it was or is not in the
/// result). Both `None`, or both equal, is no change.
#[derive(Debug, Clone)]
pub(crate) struct RowChange {
    identity: String,
    old: Option<OwnedRow>,
    new: Option<OwnedRow>,
    /// Part of the result the subscription started with.
    present: bool,
}

/// The changes waiting for the callback: in order while it keeps up;
/// past [`PENDING_LIMIT`], the net effect per row, in the order rows first
/// changed, until it has caught up.
enum Pending {
    Exact(VecDeque<RowChange>),
    Net {
        order: VecDeque<String>,
        net: HashMap<String, RowChange>,
    },
}

impl Pending {
    /// Takes `change` in: in order, or folded into the row's net effect.
    fn push(&mut self, change: RowChange) -> bool {
        let mut lagged = false;
        if let Pending::Exact(queue) = self
            && queue.len() >= PENDING_LIMIT
        {
            let mut order = VecDeque::new();
            let mut net: HashMap<String, RowChange> = HashMap::new();
            for change in queue.drain(..) {
                fold(&mut order, &mut net, change);
            }
            *self = Pending::Net { order, net };
            lagged = true;
        }
        match self {
            Pending::Exact(queue) => queue.push_back(change),
            Pending::Net { order, net } => fold(order, net, change),
        }
        lagged
    }

    /// The next change to deliver, if any: back in order once the net
    /// effects are all out.
    fn pop(&mut self) -> Option<RowChange> {
        match self {
            Pending::Exact(queue) => queue.pop_front(),
            Pending::Net { order, net } => {
                let next = order.pop_front().and_then(|identity| net.remove(&identity));
                if order.is_empty() {
                    *self = Pending::Exact(VecDeque::new());
                }
                next
            }
        }
    }
}

/// `change` folded into the net effect of its row: the row as it was
/// before the first pending change, and as it is after the last.
fn fold(order: &mut VecDeque<String>, net: &mut HashMap<String, RowChange>, change: RowChange) {
    match net.get_mut(&change.identity) {
        Some(held) => {
            held.new = change.new;
            held.present &= change.present;
        }
        None => {
            order.push_back(change.identity.clone());
            net.insert(change.identity.clone(), change);
        }
    }
}

/// A subscription as the engine side and its delivery task share it.
pub(crate) struct Listening {
    pending: Mutex<Pending>,
    notify: Notify,
    /// The result as last known, kept while the engine connection is
    /// down, to compare the engine's result with once it is back.
    previous: Mutex<Option<Materialized>>,
    /// Whether the result the subscription started with has been
    /// delivered (as [`Change::Present`]).
    started: AtomicBool,
    stopped: AtomicBool,
    lagged: AtomicU64,
    errors: Mutex<Vec<String>>,
    /// The database the engine streams, to look column types up on when
    /// the engine connection comes back without them.
    database_url: String,
    /// That database as Postgres identifies it, for an engine streaming
    /// another to be told apart.
    pub(crate) database: crate::cache::Database,
}

impl Listening {
    /// A subscription not yet started.
    pub(crate) fn new(database_url: String, database: crate::cache::Database) -> Arc<Listening> {
        Arc::new(Listening {
            pending: Mutex::new(Pending::Exact(VecDeque::new())),
            notify: Notify::new(),
            previous: Mutex::new(None),
            started: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            lagged: AtomicU64::new(0),
            errors: Mutex::new(Vec::new()),
            database_url,
            database,
        })
    }

    /// Hands `changes` to the delivery task.
    pub(crate) fn push(&self, changes: Vec<RowChange>) {
        if changes.is_empty() {
            return;
        }
        let mut pending = self.pending.lock().expect("pending");
        for change in changes {
            if pending.push(change) {
                self.lagged.fetch_add(1, Ordering::Relaxed);
            }
        }
        drop(pending);
        self.notify.notify_one();
    }

    /// Why changes could not be followed or delivered, oldest first.
    pub(crate) fn errors(&self) -> Vec<String> {
        self.errors.lock().expect("errors").clone()
    }

    /// Records why something could not be followed or delivered.
    pub(crate) fn failed(&self, reason: String) {
        let mut errors = self.errors.lock().expect("errors");
        if errors.len() >= 64 {
            errors.remove(0);
        }
        errors.push(reason);
    }

    /// A store the engine side has just built for the subscription, from
    /// the engine's snapshot. The first one starts it: its rows are
    /// [`Change::Present`]. One built after the connection came back is
    /// compared with the result last known, and the difference is the
    /// changes. A result that cannot be encoded is the reason the engine
    /// side stops serving the query's rows.
    pub(crate) fn built(&self, store: &Materialized) -> Result<(), String> {
        let previous = self.previous.lock().expect("previous").take();
        let changes = if self.started.swap(true, Ordering::AcqRel) {
            match previous {
                Some(previous) => compare(&previous, store),
                None => Ok(Vec::new()),
            }
        } else {
            store.keyed().map(|rows| {
                rows.into_iter()
                    .map(|(identity, row)| RowChange {
                        identity,
                        old: None,
                        new: Some(row),
                        present: true,
                    })
                    .collect()
            })
        };
        let changes = changes.map_err(|error| error.0)?;
        self.push(changes);
        Ok(())
    }

    /// The column types of `tables`, looked up on a connection of the
    /// subscription's own, closed after.
    pub(crate) fn lookup_types(
        &self,
        tables: &[String],
    ) -> Result<HashMap<String, Arc<crate::types::TableTypes>>, String> {
        use crate::types::TypeLookup;
        let mut conn = PgConnection::establish(&self.database_url)
            .map_err(|error| format!("connecting to look the column types up: {error}"))?;
        tables
            .iter()
            .map(|table| {
                conn.table_types(table)
                    .map(|types| (table.clone(), Arc::new(types)))
                    .map_err(|error| format!("looking the column types of `{table}` up: {error}"))
            })
            .collect()
    }

    /// Keeps the store as the result last known, the engine connection
    /// having dropped.
    pub(crate) fn keep(&self, store: Materialized) {
        let mut previous = self.previous.lock().expect("previous");
        if previous.is_none() {
            *previous = Some(store);
        }
    }

    /// Applies the engine's `delta` to `store` and hands on what it
    /// changed in the result: read off the delta for a plain result
    /// ([`Materialized::is_plain`]), else by comparing the result before
    /// and after.
    pub(crate) fn apply(
        &self,
        store: &mut Materialized,
        body: Json,
        apply: impl FnOnce(&mut Materialized, Json),
    ) {
        let changes = if store.is_plain() {
            plain_changes(store, &body).map(|changes| {
                apply(store, body);
                changes
                    .into_iter()
                    .map(|(identity, old)| {
                        let new = store
                            .root_row(&identity)
                            .map(|row| store.project_root(row))
                            .transpose()?;
                        Ok(RowChange {
                            identity,
                            old,
                            new,
                            present: false,
                        })
                    })
                    .collect::<Result<Vec<_>, crate::encode::Unsupported>>()
            })
        } else {
            let before = store.keyed();
            apply(store, body);
            Ok(before.and_then(|before| {
                let after = store.keyed()?;
                Ok(difference(before, after))
            }))
        };
        match changes.and_then(|changes| changes) {
            Ok(changes) => self.push(changes),
            Err(error) => self.failed(error.0),
        }
    }
}

/// The rows of a plain result a delta names, by identity, each with the
/// row as it is before the delta: what to compare with after it.
fn plain_changes(
    store: &Materialized,
    body: &Json,
) -> Result<Vec<(String, Option<OwnedRow>)>, crate::encode::Unsupported> {
    let mut seen: Vec<(String, Option<OwnedRow>)> = Vec::new();
    let Some(Json::Array(changes)) = body.get("changes") else {
        return Ok(seen);
    };
    for change in changes {
        let Some(table) = change.get("table").and_then(Json::as_str) else {
            continue;
        };
        let named = ["del", "put"]
            .iter()
            .filter_map(|list| change.get(*list).and_then(Json::as_array))
            .flatten();
        for object in named {
            let Some(identity) = store.root_identity(table, object) else {
                continue;
            };
            if seen.iter().any(|(held, _)| *held == identity) {
                continue;
            }
            let old = store
                .root_row(&identity)
                .map(|row| store.project_root(row))
                .transpose()?;
            seen.push((identity, old));
        }
    }
    Ok(seen)
}

/// The changes from `before` to `after`, results with their rows'
/// identities: what left, in `before`'s order, then what entered or
/// changed, in `after`'s.
fn difference(before: Vec<(String, OwnedRow)>, after: Vec<(String, OwnedRow)>) -> Vec<RowChange> {
    let mut old: HashMap<String, OwnedRow> = before.iter().cloned().collect();
    let staying: std::collections::HashSet<&String> =
        after.iter().map(|(identity, _)| identity).collect();
    let mut changes: Vec<RowChange> = before
        .iter()
        .filter(|(identity, _)| !staying.contains(identity))
        .map(|(identity, row)| RowChange {
            identity: identity.clone(),
            old: Some(row.clone()),
            new: None,
            present: false,
        })
        .collect();
    for (identity, row) in after {
        let was = old.remove(&identity);
        if was.as_ref() != Some(&row) {
            changes.push(RowChange {
                identity,
                old: was,
                new: Some(row),
                present: false,
            });
        }
    }
    changes
}

/// The changes from the result of `before` to that of `after`.
fn compare(
    before: &Materialized,
    after: &Materialized,
) -> Result<Vec<RowChange>, crate::encode::Unsupported> {
    Ok(difference(before.keyed()?, after.keyed()?))
}

/// A running subscription. Dropping it, or [`Subscription::stop`], ends
/// it: no callback starts after, and the engine stops following the
/// query.
pub struct Subscription {
    engine: EngineClient,
    key: QueryKey,
    listening: Arc<Listening>,
    task: tokio::task::JoinHandle<()>,
}

impl Subscription {
    /// How many times the callback fell more than [`PENDING_LIMIT`]
    /// changes behind, and changes were delivered as their net effect.
    pub fn lagged(&self) -> u64 {
        self.listening.lagged.load(Ordering::Relaxed)
    }

    /// The latest reasons a change could not be followed or delivered
    /// (a row its type would not decode, a value the engine side cannot
    /// encode), oldest first.
    pub fn errors(&self) -> Vec<String> {
        self.listening.errors()
    }

    /// Ends the subscription now.
    pub fn stop(self) {}
}

impl Drop for Subscription {
    /// No callback starts from here on, and the engine lets the query go.
    fn drop(&mut self) {
        self.listening.stopped.store(true, Ordering::Release);
        self.task.abort();
        self.engine.stop_listening(&self.key);
    }
}

/// Why a subscribe failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeError(pub String);

impl std::fmt::Display for SubscribeError {
    /// The reason.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SubscribeError {}

impl Listening {
    /// The pending changes, taken and decoded as rows of SQL type `ST`.
    #[cfg(test)]
    pub(crate) fn take<ST, T: FromSqlRow<ST, Pg>>(&self) -> Vec<Change<T>> {
        let mut pending = self.pending.lock().expect("pending");
        std::iter::from_fn(|| pending.pop())
            .filter_map(|change| decode::<ST, T>(change).expect("decodes"))
            .collect()
    }
}

impl EngineClient {
    /// Waits, up to the verdict timeout, for every socket to have
    /// received each change committed before now, so the rows a
    /// subscription starts with hold them as present. A standby's
    /// connection is not waited for, and an engine that names no
    /// position (one from before positions, or one that has sent nothing
    /// yet) for a second at most, two of its `lag` frames; past the wait,
    /// a commit not yet received arrives as added.
    fn catch_up(&self, conn: &mut PgConnection) {
        use diesel::RunQueryDsl;
        let Ok(position) = diesel::sql_query(crate::cache::WAL_POSITION_SQL)
            .get_result::<crate::cache::WalPosition>(conn)
        else {
            return;
        };
        let Some(crate::cache::Read::At(position)) = position.read() else {
            return;
        };
        let started = std::time::Instant::now();
        loop {
            let waited = started.elapsed();
            match Subscriptions::position(self) {
                Some(reached) if reached >= position => return,
                Some(_) if waited < self.config().verdict_timeout => {}
                None if waited < UNNAMED_POSITION_WAIT => {}
                _ => return,
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

/// The database `conn` reads, for the engine's to be checked against:
/// the engine streams one database, and a query rendered and typed on
/// another would be served the engine's rows as that one's. Refused
/// when a relation earlier on the search path than `public` shadows a
/// table there: the engine resolves the query's bare names in `public`,
/// and the types would be looked up for another relation.
fn reads_as(conn: &mut PgConnection) -> Result<crate::cache::Database, SubscribeError> {
    use diesel::RunQueryDsl;
    let context = diesel::sql_query(crate::cache::CONTEXT_SQL)
        .get_result::<crate::cache::Context>(conn)
        .map_err(|error| SubscribeError(format!("reading what the connection reads: {error}")))?;
    if context.shadowed {
        return Err(SubscribeError(
            "a relation earlier on the search path than `public` shadows a table there, \
             which the engine would read instead"
                .to_string(),
        ));
    }
    Ok(context.database())
}

/// Counts the subscriptions made, for their keys' context ids.
static NEXT_LISTENER: AtomicU64 = AtomicU64::new(0);

impl EngineClient {
    /// Follows `query` and calls `callback` with each change of its
    /// result, until the returned [`Subscription`] is dropped (see the
    /// [module](self) for what is delivered, and when). `database_url`
    /// is the database the engine streams: a connection there renders
    /// the query and reads its tables' column types, and is closed
    /// before this returns. The callbacks run on the runtime this is
    /// called on.
    pub async fn subscribe<Q, T, ST, F, Fut>(
        &self,
        database_url: &str,
        query: Q,
        callback: F,
    ) -> Result<Subscription, SubscribeError>
    where
        Q: Query + QueryFragment<Pg> + QueryId + Send + 'static,
        Q::SqlType: CompatibleType<T, Pg, SqlType = ST>,
        T: FromSqlRow<ST, Pg> + Send + 'static,
        ST: Send + 'static,
        F: Fn(Change<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let listener = NEXT_LISTENER.fetch_add(1, Ordering::Relaxed);
        let context = u32::try_from(listener)
            .ok()
            .and_then(|listener| LISTENING_BASE.checked_add(listener))
            .ok_or_else(|| SubscribeError("too many subscriptions in this process".to_string()))?;
        let engine = self.clone();
        let url = database_url.to_string();
        let started = tokio::task::spawn_blocking(move || {
            let mut conn = PgConnection::establish(&url)
                .map_err(|error| SubscribeError(format!("connecting to the database: {error}")))?;
            let database = reads_as(&mut conn)?;
            let mut key = QueryKey::of(&query, &mut conn)
                .map_err(|error| SubscribeError(format!("rendering the query: {error}")))?;
            key.context = context;
            let listening = Listening::new(url, database);
            engine.catch_up(&mut conn);
            engine.start_listening(&key, Arc::clone(&listening));
            match engine.subscribe_with(&key, &mut conn) {
                Verdict::Rows { .. } => Ok((key, listening)),
                Verdict::Accepted { .. } => {
                    engine.stop_listening(&key);
                    Err(SubscribeError(format!(
                        "the engine follows this query only by refills, not with its rows: {}",
                        engine.fallbacks().last().cloned().unwrap_or_default()
                    )))
                }
                Verdict::Refused(reason) => {
                    engine.stop_listening(&key);
                    Err(SubscribeError(reason))
                }
            }
        })
        .await
        .map_err(|error| SubscribeError(format!("subscribing: {error}")))?;
        let (key, listening) = started?;
        let delivered = Arc::clone(&listening);
        let task = tokio::spawn(deliver(delivered, callback, PhantomData::<fn() -> (ST, T)>));
        Ok(Subscription {
            engine: self.clone(),
            key,
            listening,
            task,
        })
    }
}

impl EngineClient {
    /// [`subscribe`](Self::subscribe) for a query given as plain SQL: each
    /// change's rows as [`SqlRow`]s, read by column name, so the caller
    /// needs no Diesel type of this crate's Diesel. Identifiers outside
    /// `public` are written `"schema"."table"`; values are written into
    /// the SQL (it takes no binds), so quote what is not the caller's own.
    pub async fn subscribe_sql<F, Fut>(
        &self,
        database_url: &str,
        sql: impl Into<String>,
        callback: F,
    ) -> Result<Subscription, SubscribeError>
    where
        F: Fn(Change<SqlRow>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.subscribe(database_url, diesel::sql_query(sql.into()), callback)
            .await
    }
}

/// One row of a plain-SQL subscription ([`EngineClient::subscribe_sql`]):
/// its values by column name, as the engine has them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlRow(OwnedRow);

impl SqlRow {
    /// The row's columns, in the query's order.
    pub fn columns(&self) -> &[String] {
        self.0.columns()
    }

    /// Whether `column` is SQL `NULL` (or not a column of the row).
    pub fn is_null(&self, column: &str) -> bool {
        self.position(column)
            .and_then(|index| self.0.value(index))
            .is_none()
    }

    /// `column` as text, for the types whose text form needs no
    /// conversion table: booleans, integers, text of every kind, `json`,
    /// `jsonb` and `uuid`. `None` for `NULL`, another type, or no such
    /// column; read those with [`get`](Self::get).
    pub fn text(&self, column: &str) -> Option<String> {
        let value = self.0.value(self.position(column)?)?;
        let bytes = value.bytes();
        let int = |width: usize| -> Option<String> {
            let mut buffer = [0u8; 8];
            if bytes.len() != width {
                return None;
            }
            buffer[8 - width..].copy_from_slice(bytes);
            let shift = 64 - 8 * width as u32;
            Some(((i64::from_be_bytes(buffer) << shift) >> shift).to_string())
        };
        match value.type_oid().get() {
            oid::BOOL => Some((bytes.first()? != &0).to_string()),
            oid::INT2 => int(2),
            oid::INT4 => int(4),
            oid::INT8 => int(8),
            oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::JSON => {
                String::from_utf8(bytes.to_vec()).ok()
            }
            oid::JSONB => String::from_utf8(bytes.get(1..)?.to_vec()).ok(),
            oid::UUID if bytes.len() == 16 => {
                let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
                Some(format!(
                    "{}-{}-{}-{}-{}",
                    &hex[0..8],
                    &hex[8..12],
                    &hex[12..16],
                    &hex[16..20],
                    &hex[20..32]
                ))
            }
            _ => None,
        }
    }

    /// `column` decoded as `T` from SQL type `ST`, as Diesel decodes it
    /// (this crate's Diesel), for the types [`text`](Self::text) leaves out.
    pub fn get<ST, T>(&self, column: &str) -> deserialize::Result<T>
    where
        T: FromSql<ST, Pg>,
    {
        let index = self
            .position(column)
            .ok_or_else(|| format!("no column `{column}`"))?;
        let field = self.0.get(index).ok_or_else(|| format!("no column `{column}`"))?;
        T::from_nullable_sql(field.value())
    }

    /// The row as the engine holds it.
    pub fn owned(&self) -> &OwnedRow {
        &self.0
    }

    fn position(&self, column: &str) -> Option<usize> {
        self.0.columns().iter().position(|name| name == column)
    }
}

impl FromSqlRow<Untyped, Pg> for SqlRow {
    /// Copies the row: names and values as they are.
    fn build_from_row<'a>(row: &impl Row<'a, Pg>) -> deserialize::Result<Self> {
        OwnedRow::capture(row, None)
            .map(SqlRow)
            .ok_or_else(|| "a column of the row has no name".into())
    }
}

/// The delivery task: each pending change, decoded as `T` from rows of
/// SQL type `ST`, to `callback`, one at a time, until the subscription
/// stops.
async fn deliver<ST, T, F, Fut>(
    listening: Arc<Listening>,
    callback: F,
    _types: PhantomData<fn() -> (ST, T)>,
) where
    T: FromSqlRow<ST, Pg>,
    F: Fn(Change<T>) -> Fut,
    Fut: Future<Output = ()>,
{
    while !listening.stopped.load(Ordering::Acquire) {
        let next = listening.pending.lock().expect("pending").pop();
        let Some(change) = next else {
            listening.notify.notified().await;
            continue;
        };
        match decode::<ST, T>(change) {
            Ok(Some(change)) => callback(change).await,
            Ok(None) => {}
            Err(error) => listening.failed(format!("a row would not decode: {error}")),
        }
    }
}

/// A change as `T`, a row of SQL type `ST`; `None` for no change (a row
/// that entered and left while the callback was behind, or one that
/// changed back).
fn decode<ST, T>(change: RowChange) -> QueryResult<Option<Change<T>>>
where
    T: FromSqlRow<ST, Pg>,
{
    let row = |row: &OwnedRow| {
        <T as FromSqlRow<ST, Pg>>::build_from_row(row)
            .map_err(diesel::result::Error::DeserializationError)
    };
    Ok(match (&change.old, &change.new) {
        (None, Some(new)) if change.present => Some(Change::Present(row(new)?)),
        (None, Some(new)) => Some(Change::Added(row(new)?)),
        (Some(old), Some(new)) if old != new => Some(Change::Updated {
            old: row(old)?,
            new: row(new)?,
        }),
        (Some(old), None) => Some(Change::Removed(row(old)?)),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::OwnedValue;

    /// A one-column row holding `value` as an int4.
    fn row(value: i32) -> OwnedRow {
        OwnedRow::new(
            Arc::from(vec!["n".to_string()]),
            vec![Some(OwnedValue::new(
                std::num::NonZeroU32::new(23).expect("int4"),
                value.to_be_bytes().to_vec(),
            ))],
        )
    }

    /// A change of `identity` from `old` to `new`.
    fn change(identity: &str, old: Option<i32>, new: Option<i32>) -> RowChange {
        RowChange {
            identity: identity.to_string(),
            old: old.map(row),
            new: new.map(row),
            present: false,
        }
    }

    /// What a change decodes to, as an `i32` query's.
    fn decoded(change: RowChange) -> Option<Change<i32>> {
        decode::<diesel::sql_types::Integer, i32>(change).expect("decodes")
    }

    /// A row of `(name, type oid, bytes)` columns, `None` bytes for NULL.
    fn sql_row(columns: &[(&str, u32, Option<Vec<u8>>)]) -> OwnedRow {
        OwnedRow::new(
            Arc::from(columns.iter().map(|(name, _, _)| name.to_string()).collect::<Vec<_>>()),
            columns
                .iter()
                .map(|(_, type_oid, bytes)| {
                    bytes.clone().map(|bytes| {
                        OwnedValue::new(std::num::NonZeroU32::new(*type_oid).expect("oid"), bytes)
                    })
                })
                .collect(),
        )
    }

    /// A plain-SQL subscription's change decodes to a `SqlRow` of the same
    /// columns, whatever their types.
    #[test]
    fn sql_rows_decode_whole() {
        let row = sql_row(&[("id", 20, Some(7i64.to_be_bytes().to_vec())), ("note", 25, None)]);
        let change = RowChange {
            identity: "7".to_string(),
            old: None,
            new: Some(row.clone()),
            present: false,
        };
        let decoded = decode::<Untyped, SqlRow>(change).expect("decodes");
        assert_eq!(decoded, Some(Change::Added(SqlRow(row))));
    }

    /// `text` renders the types it knows from their binary form, and
    /// leaves the rest, NULL and unknown columns to `None`.
    #[test]
    fn sql_row_text_reads_known_types() {
        let row = SqlRow(sql_row(&[
            ("flag", oid::BOOL, Some(vec![1])),
            ("small", oid::INT2, Some((-3i16).to_be_bytes().to_vec())),
            ("count", oid::INT4, Some(42i32.to_be_bytes().to_vec())),
            ("big", oid::INT8, Some((-9_000_000_000i64).to_be_bytes().to_vec())),
            ("name", oid::VARCHAR, Some(b"alpha".to_vec())),
            ("doc", oid::JSON, Some(br#"{"a":1}"#.to_vec())),
            ("docb", oid::JSONB, Some([&[1u8][..], br#"{"b":2}"#].concat())),
            ("key", oid::UUID, Some((0u8..16).collect())),
            ("at", oid::TIMESTAMP, Some(0i64.to_be_bytes().to_vec())),
            ("gone", oid::TEXT, None),
        ]));
        assert_eq!(row.text("flag").as_deref(), Some("true"));
        assert_eq!(row.text("small").as_deref(), Some("-3"));
        assert_eq!(row.text("count").as_deref(), Some("42"));
        assert_eq!(row.text("big").as_deref(), Some("-9000000000"));
        assert_eq!(row.text("name").as_deref(), Some("alpha"));
        assert_eq!(row.text("doc").as_deref(), Some(r#"{"a":1}"#));
        assert_eq!(row.text("docb").as_deref(), Some(r#"{"b":2}"#));
        assert_eq!(
            row.text("key").as_deref(),
            Some("00010203-0405-0607-0809-0a0b0c0d0e0f")
        );
        assert_eq!(row.text("at"), None);
        assert_eq!(row.text("gone"), None);
        assert!(row.is_null("gone"));
        assert!(!row.is_null("count"));
        assert_eq!(row.text("missing"), None);
        assert_eq!(row.columns()[0], "flag");
    }

    /// `get` decodes through Diesel what `text` does not.
    #[test]
    fn sql_row_get_decodes_through_diesel() {
        let row = SqlRow(sql_row(&[
            ("count", oid::INT4, Some(42i32.to_be_bytes().to_vec())),
            ("gone", oid::INT4, None),
        ]));
        assert_eq!(row.get::<diesel::sql_types::Integer, i32>("count").expect("int"), 42);
        assert_eq!(
            row.get::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, Option<i32>>("gone")
                .expect("null"),
            None
        );
        assert!(row.get::<diesel::sql_types::Integer, i32>("missing").is_err());
    }

    /// Each kind of change decodes to its variant; no change to nothing.
    #[test]
    fn changes_decode_to_their_kind() {
        assert_eq!(decoded(change("a", None, Some(1))), Some(Change::Added(1)));
        let mut present = change("a", None, Some(1));
        present.present = true;
        assert_eq!(decoded(present), Some(Change::Present(1)));
        assert_eq!(
            decoded(change("a", Some(1), Some(2))),
            Some(Change::Updated { old: 1, new: 2 })
        );
        assert_eq!(
            decoded(change("a", Some(1), None)),
            Some(Change::Removed(1))
        );
        assert_eq!(decoded(change("a", Some(1), Some(1))), None, "changed back");
        assert_eq!(decoded(change("a", None, None)), None, "entered and left");
    }

    /// Pending changes come out in order; past the limit, as their net
    /// effect per row, in the order rows first changed, and in order
    /// again once those are out.
    #[test]
    fn pending_changes_keep_order_then_fold_when_behind() {
        let mut pending = Pending::Exact(VecDeque::new());
        assert!(!pending.push(change("a", None, Some(1))));
        assert!(!pending.push(change("a", Some(1), Some(2))));
        let out = pending.pop().expect("first");
        assert_eq!(
            (out.old, out.new),
            (None, Some(row(1))),
            "each change, in order"
        );

        let mut pending = Pending::Exact(VecDeque::new());
        for at in 0..PENDING_LIMIT {
            pending.push(change(&format!("r{}", at % 10), None, Some(1)));
        }
        assert!(
            pending.push(change("r0", Some(1), None)),
            "the limit folds them"
        );
        let first = pending.pop().expect("net of r0");
        assert_eq!(first.identity, "r0");
        assert_eq!(
            (first.old, first.new),
            (None, None),
            "entered and left: nothing"
        );
        let mut rest = Vec::new();
        while let Some(change) = pending.pop() {
            rest.push(change.identity);
        }
        assert_eq!(rest.len(), 9, "one net change per row");
        assert!(matches!(pending, Pending::Exact(_)), "in order again");
    }

    /// The difference of two results: what left, then what entered or
    /// changed; a row unchanged is no change.
    #[test]
    fn a_difference_names_what_left_entered_and_changed() {
        let before = vec![
            ("a".into(), row(1)),
            ("b".into(), row(2)),
            ("c".into(), row(3)),
        ];
        let after = vec![
            ("b".into(), row(2)),
            ("c".into(), row(30)),
            ("d".into(), row(4)),
        ];
        let changes = difference(before, after);
        let named: Vec<(&str, Option<OwnedRow>, Option<OwnedRow>)> = changes
            .iter()
            .map(|change| {
                (
                    change.identity.as_str(),
                    change.old.clone(),
                    change.new.clone(),
                )
            })
            .collect();
        assert_eq!(
            named,
            vec![
                ("a", Some(row(1)), None),
                ("c", Some(row(3)), Some(row(30))),
                ("d", None, Some(row(4))),
            ]
        );
    }
}
