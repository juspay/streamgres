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
//! A bind of a user-defined type needs its OID, which the blocking
//! wrapper resolves on the spot through the connection. Here the first
//! read with such a bind goes to Postgres while the OID is resolved and
//! remembered; from the next read on the query is cached like any other.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use diesel::QueryableByName;
use diesel::connection::Instrumentation;
use diesel::deserialize::FromSqlRow;
use diesel::pg::{
    FailedToLookupTypeError, Pg, PgMetadataCache, PgMetadataCacheKey, PgMetadataLookup, PgTypeMetadata,
};
use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
use diesel::sql_query;
use diesel::sql_types::{Oid, Text, Untyped};
use diesel::{ConnectionResult, QueryResult};
use diesel_async::pooled_connection::PoolableConnection;
use diesel_async::{
    AnsiTransactionManager, AsyncConnection, AsyncPgConnection, CacheSize, SimpleAsyncConnection, TransactionManager,
};
use futures_util::future::{BoxFuture, FutureExt, TryFutureExt};
use futures_util::stream::{self, BoxStream, StreamExt, TryStreamExt};

use crate::cache::Cache;
use crate::cursor::SyncedRow;
use crate::key::QueryKey;
use crate::route::Route;
use crate::row::{OwnedCursor, OwnedRow, capture_all};
use crate::subscriptions::{Outcome, Verdict};
use crate::types::{ALL_TABLES_SQL, TableAttribute, TableTypes, all_table_types};
use crate::{Attachment, SETTINGS, read_your_writes_from_env};

/// diesel-async's Postgres row, which it does not export by name.
pub type PgRow = <AsyncPgConnection as AsyncConnection>::Row<'static, 'static>;

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
    wrote_at: Option<Instant>,
    read_your_writes: Duration,
    /// OIDs of user-defined types seen in binds, for the cache key.
    metadata: Arc<Mutex<PgMetadataCache>>,
    /// Whether this connection has loaded every table's types for the
    /// engine side; false again when the engine named a table that was
    /// not among them.
    catalog_loaded: Arc<AtomicBool>,
}

impl AsyncSyncedConnection {
    /// Wraps an established connection as a passthrough.
    pub fn new(inner: AsyncPgConnection) -> Self {
        AsyncSyncedConnection {
            inner,
            cache: None,
            route: Route::Postgres,
            scoped: None,
            wrote_at: None,
            read_your_writes: Duration::ZERO,
            metadata: Arc::new(Mutex::new(PgMetadataCache::new())),
            catalog_loaded: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Wraps a connection with a cache and the route its reads take
    /// unless a query or a scope says otherwise.
    pub fn with_cache(inner: AsyncPgConnection, cache: Arc<Cache>, route: Route) -> Self {
        AsyncSyncedConnection {
            cache: Some(cache),
            route,
            read_your_writes: read_your_writes_from_env(),
            ..Self::new(inner)
        }
    }

    pub fn set_route(&mut self, route: Route) {
        self.route = route;
    }

    pub fn route(&self) -> Route {
        self.route
    }

    pub fn set_read_your_writes(&mut self, window: Duration) {
        self.read_your_writes = window;
    }

    pub fn read_your_writes(&self) -> Duration {
        self.read_your_writes
    }

    pub fn cache(&self) -> Option<&Arc<Cache>> {
        self.cache.as_ref()
    }

    pub fn inner(&mut self) -> &mut AsyncPgConnection {
        &mut self.inner
    }

    pub fn into_inner(self) -> AsyncPgConnection {
        self.inner
    }

    /// Runs `f` with every read inside it routed `route`; a flag on the
    /// query itself still wins. The closure gets the connection back.
    pub async fn with_route<R, F>(&mut self, route: Route, f: F) -> R
    where
        F: for<'a> FnOnce(&'a mut Self) -> BoxFuture<'a, R>,
    {
        let outer = self.scoped.replace(route);
        let result = f(self).await;
        self.scoped = outer;
        result
    }

    fn wrote(&mut self) {
        if !self.read_your_writes.is_zero() {
            self.wrote_at = Some(Instant::now());
        }
    }

    fn within_write_window(&self) -> bool {
        self.wrote_at
            .is_some_and(|wrote_at| wrote_at.elapsed() < self.read_your_writes)
    }

    fn in_transaction(&mut self) -> bool {
        AnsiTransactionManager::transaction_manager_status_mut(self)
            .transaction_depth()
            .map(|depth| depth.is_some())
            .unwrap_or(true)
    }

    /// The key of a query, and the user-defined types its binds name
    /// whose OIDs are not known yet.
    fn key_of<T: QueryFragment<Pg>>(&mut self, query: &T) -> QueryResult<(QueryKey, Vec<PgMetadataCacheKey<'static>>)> {
        let mut lookup = Lookup {
            cache: std::mem::replace(&mut *self.metadata.lock().expect("metadata"), PgMetadataCache::new()),
            unknown: Vec::new(),
        };
        let key = QueryKey::of(query, &mut lookup);
        *self.metadata.lock().expect("metadata") = lookup.cache;
        Ok((key?, lookup.unknown))
    }

    fn live_stream<'a>(stream: BoxStream<'a, QueryResult<PgRow>>) -> BoxStream<'a, QueryResult<AsyncSyncedRow>> {
        stream.map_ok(SyncedRow::Live).boxed()
    }

    fn cached_stream(rows: Arc<[OwnedRow]>) -> BoxStream<'static, QueryResult<AsyncSyncedRow>> {
        stream::iter(OwnedCursor::new(rows).map(|row| row.map(SyncedRow::Cached))).boxed()
    }
}

/// A type lookup over the OIDs already known, remembering what it did
/// not know rather than asking the database (it cannot, synchronously).
struct Lookup {
    cache: PgMetadataCache,
    unknown: Vec<PgMetadataCacheKey<'static>>,
}

impl PgMetadataLookup for Lookup {
    fn lookup_type(&mut self, type_name: &str, schema: Option<&str>) -> PgTypeMetadata {
        let key = PgMetadataCacheKey::new(schema.map(Cow::Borrowed), Cow::Borrowed(type_name));
        if let Some(metadata) = self.cache.lookup_type(&key) {
            return metadata;
        }
        let key = key.into_owned();
        if !self.unknown.contains(&key) {
            self.unknown.push(key.clone());
        }
        PgTypeMetadata::from_result(Err(FailedToLookupTypeError::new(key)))
    }

    fn as_any<'a>(&mut self) -> &mut (dyn std::any::Any + 'a)
    where
        Self: 'a,
    {
        self
    }
}

#[derive(QueryableByName)]
struct TypeOids {
    #[diesel(sql_type = Oid)]
    oid: u32,
    #[diesel(sql_type = Oid)]
    typarray: u32,
}

/// Diesel's own resolution of a type name, as a query.
fn type_oids_query(key: &PgMetadataCacheKey<'static>) -> diesel::query_builder::BoxedSqlQuery<'static, Pg, diesel::query_builder::SqlQuery> {
    let (schema, name) = key_parts(key);
    match schema {
        Some(schema) => sql_query(
            "SELECT t.oid, t.typarray FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace \
             WHERE t.typname = $1 AND n.nspname = $2",
        )
        .into_boxed()
        .bind::<Text, _>(name)
        .bind::<Text, _>(schema),
        None => sql_query("SELECT oid, typarray FROM pg_type WHERE oid = quote_ident($1)::regtype::oid")
            .into_boxed()
            .bind::<Text, _>(name),
    }
}

/// `PgMetadataCacheKey` keeps its parts private; its `Debug` output is
/// the only way out without a Diesel change, and it is stable.
fn key_parts(key: &PgMetadataCacheKey<'static>) -> (Option<String>, String) {
    let text = format!("{key:?}");
    let field = |name: &str| -> Option<String> {
        let start = text.find(&format!("{name}: "))? + name.len() + 2;
        let rest = &text[start..];
        let rest = rest.strip_prefix("Some(")?.strip_prefix('"').or_else(|| rest.strip_prefix('"'))?;
        let end = rest.find('"')?;
        Some(rest[..end].to_string())
    };
    (field("schema"), field("type_name").unwrap_or_default())
}

type LoadFuture<'query> = BoxFuture<'query, QueryResult<BoxStream<'static, QueryResult<PgRow>>>>;

impl SimpleAsyncConnection for AsyncSyncedConnection {
    fn batch_execute(&mut self, query: &str) -> impl std::future::Future<Output = QueryResult<()>> + Send {
        self.wrote();
        self.inner.batch_execute(query)
    }
}

impl AsyncConnection for AsyncSyncedConnection {
    type ExecuteFuture<'conn, 'query> = BoxFuture<'query, QueryResult<usize>>;
    type LoadFuture<'conn, 'query> = BoxFuture<'query, QueryResult<Self::Stream<'conn, 'query>>>;
    type Stream<'conn, 'query> = BoxStream<'static, QueryResult<AsyncSyncedRow>>;
    type Row<'conn, 'query> = AsyncSyncedRow;
    type Backend = Pg;
    type TransactionManager = AnsiTransactionManager;

    async fn establish(database_url: &str) -> ConnectionResult<Self> {
        let inner = AsyncPgConnection::establish(database_url).await?;
        Ok(match SETTINGS.get().and_then(|settings| settings.attachment_for(database_url)) {
            Some(Attachment { cache, route }) => Self::with_cache(inner, cache, route),
            None => Self::new(inner),
        })
    }

    fn load<'conn, 'query, T>(&'conn mut self, source: T) -> Self::LoadFuture<'conn, 'query>
    where
        T: AsQuery + 'query,
        T::Query: QueryFragment<Pg> + QueryId + 'query,
    {
        let query = source.as_query();
        let Some(cache) = self.cache.clone() else {
            return self.inner.load(query).map_ok(Self::live_stream).boxed();
        };
        let (mut key, unknown) = match self.key_of(&query) {
            Ok(found) => found,
            Err(error) => return std::future::ready(Err(error)).boxed(),
        };
        let route = key.take_route().or(self.scoped).unwrap_or(self.route);
        if !key.is_select() {
            self.wrote();
            return self.inner.load(query).map_ok(Self::live_stream).boxed();
        }
        if route == Route::Postgres || self.in_transaction() {
            return self.inner.load(query).map_ok(Self::live_stream).boxed();
        }
        if self.within_write_window() {
            cache.stats.after_write.fetch_add(1, Ordering::Relaxed);
            return self.inner.load(query).map_ok(Self::live_stream).boxed();
        }
        if !unknown.is_empty() {
            // Live this time; the OIDs are resolved for the next time.
            let resolvers: Vec<LoadFuture<'query>> = unknown.iter().map(|key| self.inner.load(type_oids_query(key))).collect();
            let live = self.inner.load(query);
            let metadata = Arc::clone(&self.metadata);
            return async move {
                for (key, resolver) in unknown.into_iter().zip(resolvers) {
                    let rows: Vec<PgRow> = resolver.await?.try_collect().await?;
                    if let Some(row) = rows.first() {
                        let oids = <TypeOids as FromSqlRow<Untyped, Pg>>::build_from_row(row)
                            .map_err(diesel::result::Error::DeserializationError)?;
                        metadata
                            .lock()
                            .expect("metadata")
                            .store_type(key, (oids.oid, oids.typarray));
                    }
                }
                live.await.map(Self::live_stream)
            }
            .boxed();
        }
        if let Some(entry) = cache.get(&key) {
            return std::future::ready(Ok(Self::cached_stream(Arc::clone(&entry.rows)))).boxed();
        }

        // A miss. The futures the read may need are built now; which
        // are polled depends on the engine's answer.
        let live = self.inner.load(query);
        let catalog: Option<LoadFuture<'query>> = if self.catalog_loaded.load(Ordering::Acquire) {
            None
        } else {
            Some(self.inner.load(sql_query(ALL_TABLES_SQL)))
        };
        let catalog_loaded = Arc::clone(&self.catalog_loaded);
        let subscriptions = cache.shared_subscriptions();
        async move {
            let generation = cache.reserve(&key);
            let outcome = {
                let subscriptions = Arc::clone(&subscriptions);
                let key = key.clone();
                tokio::task::spawn_blocking(move || subscriptions.subscribe(&key))
                    .await
                    .map_err(|error| diesel::result::Error::QueryBuilderError(Box::new(error)))?
            };
            let verdict = match outcome {
                Outcome::Verdict(verdict) => verdict,
                Outcome::NeedsTypes { snapshot, missing } => {
                    let mut types: HashMap<String, Arc<TableTypes>> = HashMap::new();
                    match catalog {
                        Some(catalog) => {
                            let rows: Vec<PgRow> = catalog.await?.try_collect().await?;
                            let attributes = rows
                                .iter()
                                .map(|row| {
                                    <TableAttribute as FromSqlRow<Untyped, Pg>>::build_from_row(row)
                                        .map_err(diesel::result::Error::DeserializationError)
                                })
                                .collect::<QueryResult<Vec<_>>>()?;
                            let mut all = all_table_types(attributes);
                            catalog_loaded.store(true, Ordering::Release);
                            for table in missing {
                                if let Some(found) = all.remove(&table) {
                                    types.insert(table, Arc::new(found));
                                }
                            }
                        }
                        // A table that was not there when the catalog was
                        // loaded: this query falls back to refills, and
                        // the next miss loads the catalog again.
                        None => catalog_loaded.store(false, Ordering::Release),
                    }
                    subscriptions.materialize(&key, snapshot, types)
                }
            };
            match verdict {
                Verdict::Refused(_) => {
                    cache.stats.refused.fetch_add(1, Ordering::Relaxed);
                    live.await.map(Self::live_stream)
                }
                Verdict::Rows(rows) => {
                    cache.fill(key, generation, Arc::clone(&rows));
                    Ok(Self::cached_stream(rows))
                }
                Verdict::Accepted => {
                    let pg_rows: Vec<PgRow> = live.await?.try_collect().await?;
                    let rows: Arc<[OwnedRow]> = Arc::from(capture_all(pg_rows.into_iter().map(Ok))?);
                    cache.fill(key, generation, Arc::clone(&rows));
                    Ok(Self::cached_stream(rows))
                }
            }
        }
        .boxed()
    }

    fn execute_returning_count<'conn, 'query, T>(&'conn mut self, source: T) -> Self::ExecuteFuture<'conn, 'query>
    where
        T: QueryFragment<Pg> + QueryId + 'query,
    {
        self.wrote();
        self.inner.execute_returning_count(source)
    }

    fn transaction_state(&mut self) -> &mut AnsiTransactionManager {
        self.inner.transaction_state()
    }

    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        self.inner.instrumentation()
    }

    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        self.inner.set_instrumentation(instrumentation)
    }

    fn set_prepared_statement_cache_size(&mut self, size: CacheSize) {
        self.inner.set_prepared_statement_cache_size(size)
    }
}

/// Lets bb8 and deadpool pool the wrapper. diesel-async's own check
/// also asks the socket whether it closed, which it keeps private; a
/// pool that verifies connections on checkout (the default) catches
/// that on its ping.
impl PoolableConnection for AsyncSyncedConnection {
    fn is_broken(&mut self) -> bool {
        AnsiTransactionManager::is_broken_transaction_manager(self)
    }
}
