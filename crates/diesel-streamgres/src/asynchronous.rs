//! The same wrapper over diesel-async's `AsyncPgConnection`.
//!
//! `AsyncSyncedConnection` implements `AsyncConnection`, so an
//! application on diesel-async swaps its connection type and nothing
//! else changes: the DSL, `sql_query`, transactions and the bb8 or
//! deadpool pools go through unchanged. The routing, the cache, the
//! read-your-writes window and the engine side are the ones the blocking
//! wrapper uses; only how a read reaches them differs.
//!
//! # Why the futures are built up front
//!
//! `AsyncConnection::load` returns a future that may not borrow the
//! connection (the trait's lifetimes do not relate `'conn` to `'query`)
//! and may not carry the query (it need not be `Send`). diesel-async
//! handles this by rendering the query to SQL and bind bytes before the
//! future exists; its own `load` future is `'query` and owns nothing of
//! the connection. So this wrapper does the same: everything that needs
//! the query or the connection happens synchronously in `load`, and the
//! future it returns owns the pieces it may need: the inner connection's
//! own load future (polled only when the read goes to Postgres), a
//! lookup of every table's column types (polled only when the engine
//! ships rows for a table not yet known), and the cache. A subscribe
//! blocks on the engine, so it runs on a blocking thread.
//!
//! One consequence: diesel-async records a query as started when its
//! future is built, and as finished when the future completes, so a
//! miss the engine then serves from its own rows leaves one started
//! query in the instrumentation with no finish. A hit builds nothing.
//!
//! # Pipelining
//!
//! diesel-async lets several futures from one connection run at once.
//! Its transaction state is shared with every such future, and asking
//! for it while one is pending is a panic in diesel-async itself. The
//! wrapper counts its own futures in flight and, while any is, sends a
//! read to Postgres without asking: a read pipelined behind a write
//! must see that write, which only Postgres can promise.
//!
//! A bind of a user-defined type needs its OID, which the blocking
//! wrapper resolves on the spot through the connection. Here the first
//! read with such a bind goes to Postgres while the OID is resolved and
//! remembered in the cache, for every connection sharing it; from the
//! next read on the query is cached like any other.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use diesel::QueryableByName;
use diesel::connection::Instrumentation;
use diesel::deserialize::FromSqlRow;
use diesel::pg::{
    FailedToLookupTypeError, Pg, PgMetadataCache, PgMetadataCacheKey, PgMetadataLookup,
    PgTypeMetadata,
};
use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
use diesel::result::Error;
use diesel::sql_query;
use diesel::sql_types::{Oid, Text, Untyped};
use diesel::{ConnectionResult, QueryResult};
use diesel_async::pooled_connection::PoolableConnection;
use diesel::connection::CacheSize;
use diesel_async::{
    AnsiTransactionManager, AsyncConnection, AsyncConnectionCore, AsyncPgConnection,
    SimpleAsyncConnection, TransactionManager,
};
use futures_util::future::{BoxFuture, FutureExt, TryFutureExt};
use futures_util::stream::{self, BoxStream, StreamExt, TryStreamExt};
use tokio::task::spawn_blocking;

use crate::cache::{
    AfterWrites, CONTEXT_SQL, Cache, Context, REPLAY_POSITION_SQL, Read, WAL_POSITION_SQL,
    WalPosition,
};
use crate::cursor::SyncedRow;
use crate::key::QueryKey;
use crate::route::{Route, Served};
use crate::session::{Effect, RECHECK_EVERY, Scope, SessionChanges};
use crate::row::{OwnedCursor, OwnedRow, capture_all};
use crate::subscriptions::STALE_TYPES;
use crate::materialized::Snapshot;
use crate::subscriptions::{Outcome, Subscriptions, Verdict};
use crate::types::{ALL_TABLES_SQL, TableAttribute, TableTypes, all_table_types};
use crate::{Attachment, SETTINGS, commits, may_write, read_your_writes_from_env, rendered};

/// diesel-async's Postgres row, which it does not export by name.
pub type PgRow = <AsyncPgConnection as AsyncConnectionCore>::Row<'static, 'static>;

/// The row an `AsyncSyncedConnection` yields: diesel-async's own, or one
/// the cache holds.
pub type AsyncSyncedRow = SyncedRow<PgRow>;

/// A diesel-async Postgres connection whose reads can be served from a
/// synced local copy. See [`crate::SyncedConnection`] for what is
/// cached and when; the rules are the same.
pub struct AsyncSyncedConnection {
    inner: AsyncPgConnection,
    cache: Option<Arc<Cache>>,
    route: Route,
    scoped: Option<Route>,
    read_your_writes: Duration,
    /// OIDs of user-defined types seen in binds, for the cache key:
    /// the cache's, shared by every connection through it, or this
    /// connection's own when it is a passthrough.
    metadata: Arc<Mutex<PgMetadataCache>>,
    /// Whether this connection has loaded every table's types for the
    /// engine side; false again when the engine named a table that was
    /// not among them.
    catalog_loaded: Arc<AtomicBool>,
    /// Cloned into every future the wrapper hands out: while the count
    /// is above one, a future is pending on this connection.
    inflight: Arc<()>,
    /// Where the last read was served from, written by the read's
    /// future once it knows: 0 before any read, else [`Served`] + 1.
    served: Arc<AtomicU8>,
    /// Whether the connection is known to be to a primary, whose reads
    /// are current; learned on the first refill, and never asked again.
    primary: Arc<AtomicBool>,
    /// What its statements have changed in its session.
    session: SessionChanges,
    /// The context it reads in ([`Scope::bits`]), and when that was read
    /// (milliseconds after `born`), written by the read that asked.
    scope: Arc<AtomicU64>,
    checked: Arc<AtomicU64>,
    born: Instant,
}

impl AsyncSyncedConnection {
    /// Wraps an established connection as a passthrough.
    pub fn new(inner: AsyncPgConnection) -> Self {
        AsyncSyncedConnection {
            inner,
            cache: None,
            route: Route::Postgres,
            scoped: None,
            read_your_writes: Duration::ZERO,
            metadata: Arc::new(Mutex::new(PgMetadataCache::new())),
            catalog_loaded: Arc::new(AtomicBool::new(false)),
            inflight: Arc::new(()),
            served: Arc::new(AtomicU8::new(0)),
            primary: Arc::new(AtomicBool::new(false)),
            session: SessionChanges::default(),
            scope: Arc::new(AtomicU64::new(Scope::Unchecked.bits())),
            checked: Arc::new(AtomicU64::new(0)),
            born: Instant::now(),
        }
    }

    /// Takes in what `sql` does to the connection's session
    /// ([`SessionChanges`]): once it is back as established, or may have
    /// changed in a way the statement does not say, what the connection
    /// reads as is read again before the cache serves it.
    fn observe(&mut self, sql: &str) {
        if self.cache.is_some()
            && matches!(self.session.apply(sql), Effect::Restored | Effect::Recheck)
        {
            self.scope.store(Scope::Unchecked.bits(), Ordering::Release);
        }
    }

    /// The context this connection reads in, as last read; unchecked
    /// once [`RECHECK_EVERY`] has passed since.
    fn scope(&self) -> Scope {
        let since = self.born.elapsed().saturating_sub(Duration::from_millis(
            self.checked.load(Ordering::Acquire),
        ));
        match Scope::from_bits(self.scope.load(Ordering::Acquire)) {
            _ if since >= RECHECK_EVERY => Scope::Unchecked,
            scope => scope,
        }
    }

    /// Where this connection's last read was served from: the cache,
    /// the engine's rows, or Postgres. `None` before any read. A read
    /// still in flight has not written it yet.
    pub fn last_served(&self) -> Option<Served> {
        match self.served.load(Ordering::Acquire) {
            1 => Some(Served::Cache),
            2 => Some(Served::Engine),
            3 => Some(Served::Postgres),
            _ => None,
        }
    }

    /// Wraps a connection with a cache and the route its reads take
    /// unless a query or a scope says otherwise.
    pub fn with_cache(inner: AsyncPgConnection, cache: Arc<Cache>, route: Route) -> Self {
        AsyncSyncedConnection {
            metadata: Arc::clone(cache.type_metadata()),
            cache: Some(cache),
            route,
            read_your_writes: read_your_writes_from_env(),
            ..Self::new(inner)
        }
    }

    /// Changes the route this connection's reads take by default.
    pub fn set_route(&mut self, route: Route) {
        self.route = route;
    }

    /// The route this connection's reads take by default.
    pub fn route(&self) -> Route {
        self.route
    }

    /// How long after a write reads through this connection's cache go
    /// to Postgres; see [`crate::SyncedConnection::set_read_your_writes`].
    pub fn set_read_your_writes(&mut self, window: Duration) {
        self.read_your_writes = window;
    }

    /// How long after a write reads go to Postgres.
    pub fn read_your_writes(&self) -> Duration {
        self.read_your_writes
    }

    /// The cache this connection reads through, if any.
    pub fn cache(&self) -> Option<&Arc<Cache>> {
        self.cache.as_ref()
    }

    /// The connection that talks to Postgres.
    pub fn inner(&mut self) -> &mut AsyncPgConnection {
        &mut self.inner
    }

    /// Gives the Postgres connection back.
    pub fn into_inner(self) -> AsyncPgConnection {
        self.inner
    }

    /// Runs `f` with every read inside it routed `route`; a flag on the
    /// query itself still wins. The closure gets the connection back.
    /// The outer route is restored however the future ends: completed,
    /// dropped before completion, or unwound.
    pub async fn with_route<R, F>(&mut self, route: Route, f: F) -> R
    where
        F: for<'a> FnOnce(&'a mut Self) -> BoxFuture<'a, R>,
    {
        /// Puts the outer route back when dropped.
        struct Restore<'a> {
            conn: &'a mut AsyncSyncedConnection,
            outer: Option<Route>,
        }
        impl Drop for Restore<'_> {
            /// Restores the outer route.
            fn drop(&mut self) {
                self.conn.scoped = self.outer;
            }
        }
        let outer = self.scoped.replace(route);
        let restore = Restore { conn: self, outer };
        f(&mut *restore.conn).await
    }

    /// Whether a future this wrapper handed out is still pending.
    fn pipelined(&self) -> bool {
        Arc::strong_count(&self.inflight) > 1
    }

    /// Whether a write about to be issued should read its WAL position
    /// once done: when reads through the cache go to standbys, which
    /// cannot say where the primary's WAL is, and the write commits on
    /// its own (outside a transaction). Asked before the write's future
    /// exists, and not while another is pending: diesel-async refuses
    /// its transaction state then. A write it skips leaves the reads to
    /// the window.
    fn resolves_writes(&mut self) -> bool {
        self.cache.as_ref().is_some_and(|cache| cache.reads_standbys())
            && !self.pipelined()
            && !self.in_transaction()
    }

    /// Marks the write now, and again when `future` completes or is
    /// dropped: a read issued after the write's future resolves is what
    /// the window is for, and the clock should start as late as the write
    /// did; a write whose future is dropped may still commit, and its
    /// second mark sends the next read to look up the position again.
    /// With `resolve` ([`Self::resolves_writes`]), the write reads its
    /// WAL position once done, pipelined behind it
    /// ([`crate::SyncedConnection`] does the same).
    fn written<'a, T: Send + 'a>(
        &mut self,
        future: BoxFuture<'a, QueryResult<T>>,
        resolve: bool,
    ) -> BoxFuture<'a, QueryResult<T>> {
        /// Marks the write again when the write's future goes, however
        /// it goes.
        struct Ended(Option<Arc<Cache>>);
        impl Drop for Ended {
            /// Marks the write's end.
            fn drop(&mut self) {
                if let Some(cache) = &self.0 {
                    cache.mark_write();
                }
            }
        }
        let cache = self.cache.clone();
        if let Some(cache) = &cache {
            cache.mark_write();
        }
        let probe = resolve.then(|| self.inner.load(sql_query(WAL_POSITION_SQL)));
        let token = Arc::clone(&self.inflight);
        async move {
            let _pending = token;
            let ended = Ended(cache.clone());
            let result = future.await;
            drop(ended);
            if let (Some(cache), Some(probe)) = (cache, probe)
                && result.is_ok()
            {
                let writes = cache.write_count();
                if let Some(Read::At(position)) = read_position(probe).await {
                    cache.resolve_writes(writes, position);
                }
            }
            result
        }
        .boxed()
    }

    /// A read future with the in-flight token attached.
    fn tracked<'a, T: Send + 'a>(
        &self,
        future: BoxFuture<'a, QueryResult<T>>,
    ) -> BoxFuture<'a, QueryResult<T>> {
        let token = Arc::clone(&self.inflight);
        async move {
            let _pending = token;
            future.await
        }
        .boxed()
    }


    /// Whether a transaction is open; an unreadable state counts as one.
    fn in_transaction(&mut self) -> bool {
        AnsiTransactionManager::transaction_manager_status_mut(self)
            .transaction_depth()
            .map(|depth| depth.is_some())
            .unwrap_or(true)
    }

    /// The key of a query, and the user-defined types its binds name
    /// whose OIDs are not known yet. A query that cannot be rendered
    /// until those are known (an array of an enum, whose element OID
    /// goes into the bind) has no key yet: `Ok((None, unknown))`.
    fn key_of<T: QueryFragment<Pg>>(
        &mut self,
        query: &T,
    ) -> QueryResult<(Option<QueryKey>, Vec<TypeName>)> {
        let mut lookup = Lookup {
            cache: Arc::clone(&self.metadata),
            unknown: Vec::new(),
        };
        let key = QueryKey::of(query, &mut lookup);
        match key {
            Ok(key) => Ok((Some(key), lookup.unknown)),
            Err(error) if !lookup.unknown.is_empty() => {
                let _ = error;
                Ok((None, lookup.unknown))
            }
            Err(error) => Err(error),
        }
    }

    /// A Postgres read's rows as the wrapper's.
    fn live_stream<'a>(
        stream: BoxStream<'a, QueryResult<PgRow>>,
    ) -> BoxStream<'a, QueryResult<AsyncSyncedRow>> {
        stream.map_ok(SyncedRow::Live).boxed()
    }

    /// Cached rows as the wrapper's.
    fn cached_stream(rows: Arc<[OwnedRow]>) -> BoxStream<'static, QueryResult<AsyncSyncedRow>> {
        stream::iter(OwnedCursor::new(rows).map(|row| row.map(SyncedRow::Cached))).boxed()
    }

    /// The live read, with every unknown type resolved first and
    /// remembered for the next read.
    fn resolving<'query>(
        &mut self,
        unknown: Vec<TypeName>,
        live: LoadFuture<'query>,
    ) -> LoadFuture<'query> {
        let resolvers: Vec<LoadFuture<'query>> = unknown
            .iter()
            .map(|name| self.inner.load(type_oids_query(name)))
            .collect();
        let metadata = Arc::clone(&self.metadata);
        async move {
            for (name, resolver) in unknown.into_iter().zip(resolvers) {
                let rows: Vec<PgRow> = resolver.await?.try_collect().await?;
                if let Some(row) = rows.first() {
                    let oids = <TypeOids as FromSqlRow<Untyped, Pg>>::build_from_row(row)
                        .map_err(Error::DeserializationError)?;
                    metadata
                        .lock()
                        .expect("metadata")
                        .store_type(name.key(), (oids.oid, oids.typarray));
                }
            }
            live.await
        }
        .boxed()
    }
}

/// A user-defined type a bind named, as Diesel names it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TypeName {
    schema: Option<String>,
    name: String,
}

impl TypeName {
    /// The name as Diesel's metadata cache keys it.
    fn key(&self) -> PgMetadataCacheKey<'static> {
        PgMetadataCacheKey::new(
            self.schema.clone().map(Cow::Owned),
            Cow::Owned(self.name.clone()),
        )
    }
}

/// A type lookup over the OIDs already known, remembering what it did
/// not know rather than asking the database (it cannot, synchronously).
/// The cache is shared by every connection through one `Cache`, so it
/// is locked per lookup, never taken out: a connection that took it out
/// would leave the others an empty one and lose their writes.
struct Lookup {
    cache: Arc<Mutex<PgMetadataCache>>,
    unknown: Vec<TypeName>,
}

impl PgMetadataLookup for Lookup {
    /// The type's OIDs when known; otherwise noted as unknown, and a
    /// failed lookup for Diesel.
    fn lookup_type(&mut self, type_name: &str, schema: Option<&str>) -> PgTypeMetadata {
        let key = PgMetadataCacheKey::new(schema.map(Cow::Borrowed), Cow::Borrowed(type_name));
        if let Some(metadata) = self.cache.lock().expect("metadata").lookup_type(&key) {
            return metadata;
        }
        let name = TypeName {
            schema: schema.map(str::to_string),
            name: type_name.to_string(),
        };
        if !self.unknown.contains(&name) {
            self.unknown.push(name);
        }
        PgTypeMetadata::from_result(Err(FailedToLookupTypeError::new(key.into_owned())))
    }

    /// The lookup itself.
    fn as_any<'a>(&mut self) -> &mut (dyn std::any::Any + 'a)
    where
        Self: 'a,
    {
        self
    }
}

/// A type's OID and its array type's.
#[derive(QueryableByName)]
struct TypeOids {
    #[diesel(sql_type = Oid)]
    oid: u32,
    #[diesel(sql_type = Oid)]
    typarray: u32,
}

/// Diesel's own resolution of a type name, as a query.
fn type_oids_query(
    name: &TypeName,
) -> diesel::query_builder::BoxedSqlQuery<'static, Pg, diesel::query_builder::SqlQuery> {
    match &name.schema {
        Some(schema) => sql_query(
            "SELECT t.oid, t.typarray FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace \
             WHERE t.typname = $1 AND n.nspname = $2",
        )
        .into_boxed()
        .bind::<Text, _>(name.name.clone())
        .bind::<Text, _>(schema.clone()),
        None => {
            sql_query("SELECT oid, typarray FROM pg_type WHERE oid = quote_ident($1)::regtype::oid")
                .into_boxed()
                .bind::<Text, _>(name.name.clone())
        }
    }
}

/// A Postgres read in flight.
type LoadFuture<'query> = BoxFuture<'query, QueryResult<BoxStream<'static, QueryResult<PgRow>>>>;

impl SimpleAsyncConnection for AsyncSyncedConnection {
    /// Runs SQL text, marking a possible write now and again when it
    /// completes. diesel-async's future borrows the connection, so it
    /// cannot be pipelined and needs no in-flight token. When reads
    /// through the cache go to standbys, a write that has committed (a
    /// `COMMIT`, or any write outside a transaction) reads its WAL
    /// position after it on the same connection.
    fn batch_execute(
        &mut self,
        query: &str,
    ) -> impl std::future::Future<Output = QueryResult<()>> + Send {
        self.observe(query);
        let cache = if may_write(query) {
            self.cache.clone()
        } else {
            None
        };
        if let Some(cache) = &cache {
            cache.mark_write();
        }
        let resolve = cache.as_ref().is_some_and(|cache| cache.reads_standbys())
            && (commits(query) || self.resolves_writes());
        let inner = &mut self.inner;
        async move {
            let result = inner.batch_execute(query).await;
            if let Some(cache) = cache {
                cache.mark_write();
                if resolve && result.is_ok() {
                    let writes = cache.write_count();
                    let probe = inner.load(sql_query(WAL_POSITION_SQL));
                    if let Some(Read::At(position)) = read_position(probe).await {
                        cache.resolve_writes(writes, position);
                    }
                }
            }
            result
        }
    }
}

impl AsyncConnectionCore for AsyncSyncedConnection {
    type ExecuteFuture<'conn, 'query> = BoxFuture<'query, QueryResult<usize>>;
    type LoadFuture<'conn, 'query> = BoxFuture<'query, QueryResult<Self::Stream<'conn, 'query>>>;
    type Stream<'conn, 'query> = BoxStream<'static, QueryResult<AsyncSyncedRow>>;
    type Row<'conn, 'query> = AsyncSyncedRow;
    type Backend = Pg;

    /// A read, from the cache when it can be. A query whose binds name
    /// types not known yet is read live this time, the types resolved
    /// for the next (one that cannot even be keyed until then too).
    ///
    /// A read is keyed in the context the connection reads in
    /// ([`Context`]) and shares answers only with readers in the same one
    /// ([`Cache::context_id`]). When that context is to be read (the
    /// connection's first read through the cache, or once it may have
    /// changed) the read goes to Postgres, pipelined with reading it.
    /// While its session differs from how it was established
    /// (the statements it has run), or its context cannot be served, it reads
    /// Postgres.
    ///
    /// On a miss, the futures the read may need are built at once; which
    /// are polled depends on the engine's answer. The read's future owns
    /// the subscription from the moment the subscribe may have made it
    /// until the fill; dropped before that, it ends it ([`Unfilled`]).
    /// The subscribe runs on a blocking thread and cannot be cancelled,
    /// so it checks the guard's flag once it returns: a subscription
    /// made for a read that has gone is ended there, by the same path.
    fn load<'conn, 'query, T>(&'conn mut self, source: T) -> Self::LoadFuture<'conn, 'query>
    where
        T: AsQuery + 'query,
        T::Query: QueryFragment<Pg> + QueryId + 'query,
    {
        let query = source.as_query();
        self.served.store(3, Ordering::Release);
        let Some(cache) = self.cache.clone() else {
            let live = self.inner.load(query).map_ok(Self::live_stream).boxed();
            return self.tracked(live);
        };
        let (key, unknown) = match self.key_of(&query) {
            Ok(found) => found,
            Err(error) => return std::future::ready(Err(error)).boxed(),
        };
        let Some(mut key) = key else {
            self.observe(&rendered(&query));
            let live = self.inner.load(query);
            let live = self
                .resolving(unknown, live)
                .map_ok(Self::live_stream)
                .boxed();
            return self.tracked(live);
        };
        self.observe(&key.sql);
        let named = key.take_route();
        let route = named.or(self.scoped).unwrap_or(self.route);
        if !key.is_select() {
            let resolve = self.resolves_writes();
            let live = self.inner.load(query).map_ok(Self::live_stream).boxed();
            return self.written(live, resolve);
        }
        if route == Route::Postgres || self.pipelined() || self.in_transaction() {
            let live = self.inner.load(query).map_ok(Self::live_stream).boxed();
            return self.tracked(live);
        }
        // From here on the read is one the cache could serve: each way out
        // reports where it was served.
        if named.is_none() && !crate::rollout::through_cache() {
            let live = self.inner.load(query).map_ok(Self::live_stream).boxed();
            return self.tracked(live);
        }
        if key.is_locking() {
            cache.stats.locking.fetch_add(1, Ordering::Relaxed);
            let live = self.inner.load(query).map_ok(Self::live_stream).boxed();
            crate::metrics::report(|metrics| metrics.served(Served::Postgres));
            return self.tracked(live);
        }
        match cache.after_writes(self.read_your_writes) {
            AfterWrites::Serve => {}
            AfterWrites::Postgres => {
                cache.stats.after_write.fetch_add(1, Ordering::Relaxed);
                let live = self.inner.load(query).map_ok(Self::live_stream).boxed();
                crate::metrics::report(|metrics| metrics.served(Served::Postgres));
                return self.tracked(live);
            }
            AfterWrites::Resolve(writes) => {
                cache.stats.after_write.fetch_add(1, Ordering::Relaxed);
                let probe = self.inner.load(sql_query(WAL_POSITION_SQL));
                let live = self.inner.load(query);
                let live = async move {
                    let (position, live) = futures_util::join!(read_position(probe), live);
                    match position {
                        Some(Read::At(position)) => cache.resolve_writes(writes, position),
                        Some(Read::Null) => cache.saw_standby(),
                        None => {}
                    }
                    live.map(Self::live_stream)
                }
                .boxed();
                crate::metrics::report(|metrics| metrics.served(Served::Postgres));
                return self.tracked(live);
            }
        }
        if cache.subscriptions().behind() {
            cache.stats.lagging.fetch_add(1, Ordering::Relaxed);
            let live = self.inner.load(query).map_ok(Self::live_stream).boxed();
            crate::metrics::report(|metrics| metrics.served(Served::Postgres));
            return self.tracked(live);
        }
        if !unknown.is_empty() {
            let live = self.inner.load(query);
            let live = self
                .resolving(unknown, live)
                .map_ok(Self::live_stream)
                .boxed();
            crate::metrics::report(|metrics| metrics.served(Served::Postgres));
            return self.tracked(live);
        }
        match self.scope() {
            _ if self.session.changed() => {
                cache.stats.other_context.fetch_add(1, Ordering::Relaxed);
                let live = self.inner.load(query).map_ok(Self::live_stream).boxed();
                crate::metrics::report(|metrics| metrics.served(Served::Postgres));
                return self.tracked(live);
            }
            Scope::In(context) => key.context = context,
            Scope::Own => {
                cache.stats.other_context.fetch_add(1, Ordering::Relaxed);
                let live = self.inner.load(query).map_ok(Self::live_stream).boxed();
                crate::metrics::report(|metrics| metrics.served(Served::Postgres));
                return self.tracked(live);
            }
            Scope::Unchecked => {
                let probe = self.inner.load(sql_query(CONTEXT_SQL));
                let live = self.inner.load(query);
                let (scope, checked, born) = (Arc::clone(&self.scope), Arc::clone(&self.checked), self.born);
                let live = async move {
                    let (context, live) = futures_util::join!(read_context(probe), live);
                    if let Some(context) = context {
                        let read = cache.context_id(&context).map_or(Scope::Own, Scope::In);
                        let at = u64::try_from(born.elapsed().as_millis()).unwrap_or(u64::MAX);
                        checked.store(at, Ordering::Release);
                        scope.store(read.bits(), Ordering::Release);
                    }
                    live.map(Self::live_stream)
                }
                .boxed();
                crate::metrics::report(|metrics| metrics.served(Served::Postgres));
                return self.tracked(live);
            }
        }
        if let Some(rows) = cache.get(&key) {
            self.served.store(1, Ordering::Release);
            crate::metrics::report(|metrics| metrics.served(Served::Cache));
            return std::future::ready(Ok(Self::cached_stream(rows))).boxed();
        }
        if cache.refused_recently(&key).is_some() {
            let live = self.inner.load(query).map_ok(Self::live_stream).boxed();
            crate::metrics::report(|metrics| metrics.served(Served::Postgres));
            return self.tracked(live);
        }
        let replay: Option<LoadFuture<'query>> = (!self.primary.load(Ordering::Acquire))
            .then(|| self.inner.load(sql_query(REPLAY_POSITION_SQL)));
        let live = self.inner.load(query);
        let catalog: Option<LoadFuture<'query>> = if self.catalog_loaded.load(Ordering::Acquire) {
            None
        } else {
            Some(self.inner.load(sql_query(ALL_TABLES_SQL)))
        };
        let catalog_loaded = Arc::clone(&self.catalog_loaded);
        let subscriptions = cache.shared_subscriptions();
        let token = Arc::clone(&self.inflight);
        let served = Arc::clone(&self.served);
        let primary = Arc::clone(&self.primary);
        async move {
            let _pending = token;
            let reservation = cache.reserve(&key);
            let guard = Unfilled {
                cache: Arc::clone(&cache),
                key: key.clone(),
                gone: Arc::new(AtomicBool::new(false)),
                armed: true,
            };
            let outcome = {
                let subscriptions = Arc::clone(&subscriptions);
                let cache = Arc::clone(&cache);
                let key = key.clone();
                let gone = Arc::clone(&guard.gone);
                spawn_blocking(move || {
                    let outcome = subscriptions.subscribe(&key);
                    if gone.load(Ordering::Acquire) {
                        cache.abandon(&key);
                    }
                    outcome
                })
                .await
                .map_err(|error| Error::QueryBuilderError(Box::new(error)))?
            };
            let verdict = match outcome {
                Outcome::Verdict(verdict) => verdict,
                Outcome::NeedsTypes { snapshot, missing } => {
                    with_types(&*subscriptions, &key, snapshot, &missing, catalog, &catalog_loaded)
                        .await?
                }
            };
            match verdict {
                Verdict::Refused(reason) => {
                    guard.disarm();
                    cache.refused(&key, &reason);
                    crate::metrics::report(|metrics| metrics.served(Served::Postgres));
                    live.await.map(Self::live_stream)
                }
                Verdict::Rows { rows, live: store } => {
                    guard.disarm();
                    cache.fill_live(reservation, store, &rows);
                    if subscriptions.behind() {
                        cache.stats.lagging.fetch_add(1, Ordering::Relaxed);
                        crate::metrics::report(|metrics| metrics.served(Served::Postgres));
                        return live.await.map(Self::live_stream);
                    }
                    served.store(2, Ordering::Release);
                    crate::metrics::report(|metrics| metrics.served(Served::Engine));
                    Ok(Self::cached_stream(rows))
                }
                Verdict::Accepted { since } => {
                    crate::metrics::report(|metrics| metrics.served(Served::Postgres));
                    let keep = match replay {
                        None => true,
                        Some(replay) => match read_position(replay).await {
                            Some(Read::Null) => {
                                primary.store(true, Ordering::Release);
                                true
                            }
                            Some(Read::At(replayed)) => {
                                cache.saw_standby();
                                since.is_some_and(|since| replayed >= since)
                            }
                            None => false,
                        },
                    };
                    let pg_rows: Vec<PgRow> = live.await?.try_collect().await?;
                    let rows: Arc<[OwnedRow]> =
                        Arc::from(capture_all(pg_rows.into_iter().map(Ok))?);
                    guard.disarm();
                    if keep {
                        cache.fill(reservation, Arc::clone(&rows));
                    } else {
                        cache.stats.unreplayed.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Self::cached_stream(rows))
                }
            }
        }
        .boxed()
    }

    /// Runs a statement, marking a write when it may be one.
    fn execute_returning_count<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> Self::ExecuteFuture<'conn, 'query>
    where
        T: QueryFragment<Pg> + QueryId + 'query,
    {
        let sql = rendered(&source);
        self.observe(&sql);
        let writes = may_write(&sql);
        let resolve = writes && self.resolves_writes();
        let future = self.inner.execute_returning_count(source);
        if writes {
            self.written(future, resolve)
        } else {
            self.tracked(future)
        }
    }

}

impl AsyncConnection for AsyncSyncedConnection {
    type TransactionManager = AnsiTransactionManager;

    /// Connects, attached to the cache [`crate::install`] set up for
    /// this URL, if any.
    async fn establish(database_url: &str) -> ConnectionResult<Self> {
        let inner = AsyncPgConnection::establish(database_url).await?;
        Ok(
            match SETTINGS
                .get()
                .and_then(|settings| settings.attachment_for(database_url))
            {
                Some(Attachment { cache, route }) => Self::with_cache(inner, cache, route),
                None => Self::new(inner),
            },
        )
    }

    /// The inner connection's transaction state.
    fn transaction_state(&mut self) -> &mut AnsiTransactionManager {
        self.inner.transaction_state()
    }

    /// The inner connection's instrumentation.
    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        self.inner.instrumentation()
    }

    /// Sets the inner connection's instrumentation.
    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        self.inner.set_instrumentation(instrumentation)
    }

    /// Sizes the inner connection's statement cache.
    fn set_prepared_statement_cache_size(&mut self, size: CacheSize) {
        self.inner.set_prepared_statement_cache_size(size)
    }
}

/// What `probe` ([`CONTEXT_SQL`]) reads; `None` when it fails, and the
/// next read asks again.
async fn read_context(probe: LoadFuture<'_>) -> Option<Context> {
    let rows: Vec<PgRow> = probe.await.ok()?.try_collect().await.ok()?;
    rows.first()
        .and_then(|row| <Context as FromSqlRow<Untyped, Pg>>::build_from_row(row).ok())
}

/// What `probe` ([`WAL_POSITION_SQL`], [`REPLAY_POSITION_SQL`]) reads;
/// `None` when it fails.
async fn read_position(probe: LoadFuture<'_>) -> Option<Read> {
    let rows: Vec<PgRow> = probe.await.ok()?.try_collect().await.ok()?;
    <WalPosition as FromSqlRow<Untyped, Pg>>::build_from_row(rows.first()?)
        .ok()?
        .read()
}

/// The engine's rows for a key whose tables' types the engine side does
/// not know yet: every table's types are read from the catalog, not only
/// the missing ones, since the engine side keeps them and the next query
/// on another table then needs no lookup at all. A table created since
/// this load is not among them, so the next miss loads the catalog
/// again. A connection that already loaded the catalog has no lookup to
/// make: the types it lacks are stale, and the read goes live.
async fn with_types(
    subscriptions: &dyn Subscriptions,
    key: &QueryKey,
    snapshot: Snapshot,
    missing: &[String],
    catalog: Option<LoadFuture<'_>>,
    catalog_loaded: &AtomicBool,
) -> QueryResult<Verdict> {
    let Some(catalog) = catalog else {
        catalog_loaded.store(false, Ordering::Release);
        subscriptions.unsubscribe(key);
        return Ok(Verdict::Refused(STALE_TYPES.to_string()));
    };
    let rows: Vec<PgRow> = catalog.await?.try_collect().await?;
    let attributes = rows
        .iter()
        .map(|row| {
            <TableAttribute as FromSqlRow<Untyped, Pg>>::build_from_row(row)
                .map_err(Error::DeserializationError)
        })
        .collect::<QueryResult<Vec<_>>>()?;
    let all = all_table_types(attributes);
    catalog_loaded.store(
        missing.iter().all(|table| all.contains_key(table)),
        Ordering::Release,
    );
    let types: HashMap<String, Arc<TableTypes>> = all
        .into_iter()
        .map(|(table, found)| (table, Arc::new(found)))
        .collect();
    Ok(subscriptions.materialize(key, snapshot, types))
}

/// A subscription made for a read whose future may be dropped before
/// the fill (while the subscribe runs on its blocking thread, during
/// the `Accepted` read of Postgres, during the catalog lookup). Armed
/// until the fill; dropped armed, it ends the subscription through the
/// cache, which also fails any other reader's fill in flight for the
/// key, and raises `gone` for the blocking thread. Ending it twice is
/// harmless: the cache side is idempotent, and a subscription another
/// read made since is protected by that reader's own generation.
struct Unfilled {
    cache: Arc<Cache>,
    key: QueryKey,
    gone: Arc<AtomicBool>,
    armed: bool,
}

impl Unfilled {
    /// The read filled: the subscription is the cache's from here.
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for Unfilled {
    /// Ends the subscription if the read never filled.
    fn drop(&mut self) {
        if self.armed {
            self.gone.store(true, Ordering::Release);
            self.cache.abandon(&self.key);
        }
    }
}

/// Lets bb8 and deadpool pool the wrapper. diesel-async's own check
/// also asks the socket whether it closed, which it keeps private; a
/// pool that verifies connections on checkout (the default) catches
/// that on its ping.
impl PoolableConnection for AsyncSyncedConnection {
    /// Whether the transaction manager says the connection is broken.
    fn is_broken(&mut self) -> bool {
        AnsiTransactionManager::is_broken_transaction_manager(self)
    }
}
