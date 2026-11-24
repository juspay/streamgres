//! The library's API: [`Streamgres`], how it starts ([`Builder`]), and
//! its one read ([`Streamgres::load_and_sync`]).

use std::collections::HashMap;
use std::future::{Future, IntoFuture};
use std::panic::Location;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

use super::config::{CacheConfig, Ttl};
use super::inspect::{EngineState, Inspect, KeptQuery, QueryId, Recorder, Served, Trace};
use super::metrics::Metrics;
use super::reader::Reader;
use super::rows::Rows;
use super::{Cache, Refusal};
use crate::log::{Level, enabled, log_debug, log_warn};
use crate::parser::Bind;
use crate::sync::embed::{PgConfig, PgEngine};
use crate::sync::pg::binary::oid;
use crate::sync::pg::connections::{Connections, Io, OnRuntime, OwnPool};

/// What answers a read the engine cannot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fallback {
    /// Postgres answers it, over Streamgres's pool
    /// ([`Builder::pool_size`], or [`Builder::connections`]): at most
    /// `max_concurrent` such reads at once, each waiting at most `wait`
    /// for its turn and taking at most `timeout`.
    Postgres {
        max_concurrent: usize,
        wait: Duration,
        timeout: Duration,
    },
    /// The read fails with [`Error::Refused`].
    Error,
}

impl Default for Fallback {
    fn default() -> Self {
        Fallback::Postgres {
            max_concurrent: 8,
            wait: Duration::from_secs(1),
            timeout: Duration::from_secs(10),
        }
    }
}

/// What a read Streamgres can never answer does.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OnUnsupported {
    /// It fails with [`Error::Unsupported`], so the statement is fixed
    /// (or read on Diesel directly) rather than silently served by
    /// Postgres.
    #[default]
    Error,
    /// The fallback answers it, as it answers runtime refusals.
    Fallback,
}

/// Why a read or a start failed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// Streamgres can never answer this statement (its SQL, a table or
    /// column, or a type it does not hold exactly), and
    /// [`Builder::on_unsupported`] is [`OnUnsupported::Error`].
    Unsupported(String),
    /// The engine could not answer this time, and the fallback is
    /// [`Fallback::Error`].
    Refused(String),
    /// The fallback had no room within its wait.
    Busy,
    /// The fallback's read of Postgres failed.
    Origin(String),
    /// The engine did not become ready within the start timeout, and
    /// [`Builder::allow_degraded_start`] was not set.
    NotReady(String),
    /// The database is not set up for Streamgres: run
    /// [`Builder::setup_sql`] as an administrator.
    Setup(String),
    /// Streamgres could not start, or has stopped.
    Stopped(String),
    /// A blocking operation would prevent its application runtime from making progress.
    BlockingContext,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Unsupported(reason) => write!(f, "unsupported: {reason}"),
            Error::Refused(reason) => write!(f, "refused: {reason}"),
            Error::Busy => write!(f, "the fallback is busy"),
            Error::Origin(reason) => write!(f, "the fallback read failed: {reason}"),
            Error::NotReady(reason) => write!(f, "not ready: {reason}"),
            Error::Setup(reason) => write!(f, "not set up: {reason}"),
            Error::Stopped(reason) => write!(f, "stopped: {reason}"),
            Error::BlockingContext => f.write_str("blocking application-pool operations cannot run in a current-thread Tokio context; await the operation or use a plain thread"),
        }
    }
}

impl std::error::Error for Error {}

/// A statement and its bind values, each sent in Postgres's binary
/// format with its type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sql {
    text: String,
    binds: Vec<Bind>,
}

impl Sql {
    /// `text`, a `SELECT` with `$1`, `$2`, … for its parameters.
    pub fn new(text: impl Into<String>) -> Self {
        Sql {
            text: text.into(),
            binds: Vec::new(),
        }
    }

    /// Binds the next parameter to `value`.
    #[must_use]
    pub fn bind(mut self, value: impl Param) -> Self {
        let (type_oid, bytes) = value.encode();
        self.binds.push(Bind { type_oid, bytes });
        self
    }

    /// Binds the next parameter to `bytes`, already in Postgres's binary
    /// format for the type of OID `type_oid` (`None` for `NULL`): what an
    /// adapter hands on from its driver.
    #[must_use]
    pub fn bind_raw(mut self, type_oid: u32, bytes: Option<Vec<u8>>) -> Self {
        self.binds.push(Bind { type_oid, bytes });
        self
    }

    /// The statement's text.
    pub fn text(&self) -> &str {
        &self.text
    }
}

/// A value [`Sql::bind`] takes: its type's OID and its binary form.
pub trait Param {
    fn encode(self) -> (u32, Option<Vec<u8>>);
}

macro_rules! param {
    ($type:ty, $oid:expr) => {
        impl Param for $type {
            fn encode(self) -> (u32, Option<Vec<u8>>) {
                ($oid, Some(self.to_be_bytes().to_vec()))
            }
        }
    };
}

param!(i16, oid::INT2);
param!(i32, oid::INT4);
param!(i64, oid::INT8);
param!(f32, oid::FLOAT4);
param!(f64, oid::FLOAT8);

impl Param for bool {
    fn encode(self) -> (u32, Option<Vec<u8>>) {
        (oid::BOOL, Some(vec![u8::from(self)]))
    }
}

impl Param for &str {
    fn encode(self) -> (u32, Option<Vec<u8>>) {
        (oid::TEXT, Some(self.as_bytes().to_vec()))
    }
}

impl Param for String {
    fn encode(self) -> (u32, Option<Vec<u8>>) {
        (oid::TEXT, Some(self.into_bytes()))
    }
}

impl<T: Param + Default> Param for Option<T> {
    fn encode(self) -> (u32, Option<Vec<u8>>) {
        match self {
            Some(value) => value.encode(),
            None => (T::default().encode().0, None),
        }
    }
}

/// How often Streamgres checks whether the engine's catalog changed, to
/// read the database's types again.
const TYPES_EVERY: Duration = Duration::from_millis(250);

/// How Streamgres starts: [`Streamgres::connect`], the settings, then
/// [`Builder::start`].
#[derive(Clone)]
#[must_use]
pub struct Builder {
    connections: Option<Arc<dyn Connections>>,
    pg: PgConfig,
    cache: CacheConfig,
    fallback: Fallback,
    on_unsupported: OnUnsupported,
    pool_size: usize,
    debug_history: usize,
    degraded_start: bool,
}

impl std::fmt::Debug for Builder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder")
            .field("pg", &self.pg)
            .field(
                "connections",
                &self.connections.as_ref().map(|_| "application-owned"),
            )
            .finish_non_exhaustive()
    }
}

impl Builder {
    /// How many connections Streamgres's own pool opens at most, for its
    /// loads, fallback reads and catalog queries (default 8). Replication
    /// uses connections of its own besides.
    pub fn pool_size(mut self, size: usize) -> Self {
        self.pool_size = size;
        self
    }

    /// Runs Streamgres's ordinary queries on these connections instead of
    /// a pool of its own: an application pool it shares
    /// (`streamgres_diesel::connections`). Optional.
    pub fn connections(mut self, connections: impl Connections) -> Self {
        self.connections = Some(Arc::new(connections));
        self
    }

    /// The publication the engine streams (default `streamgres`).
    pub fn publication(mut self, name: impl Into<String>) -> Self {
        self.pg.publication = name.into();
        self
    }

    /// The schemas whose tables the engine carries (default `public`).
    pub fn schemas<S: Into<String>>(mut self, schemas: impl IntoIterator<Item = S>) -> Self {
        self.pg.schemas = schemas.into_iter().map(Into::into).collect();
        self
    }

    /// The application id and shard that name the schema-change trigger
    /// stack (default `streamgres`, 0).
    pub fn app(mut self, app: impl Into<String>, shard: u32) -> Self {
        self.pg.app = app.into();
        self.pg.shard = shard;
        self
    }

    /// Replication slots this engine names, left inactive this long, are
    /// dropped when an engine starts (PostgreSQL 17 and later; default:
    /// never).
    pub fn slot_cleanup_age(mut self, age: Duration) -> Self {
        self.pg.slot_cleanup_age = age;
        self
    }

    /// What the kept queries' rows may weigh together (default 256 MiB).
    pub fn cache_budget(mut self, bytes: usize) -> Self {
        self.cache.max_bytes = bytes;
        self
    }

    /// What one query's rows may weigh (default 32 MiB) and how many
    /// rows it may have (default 100 000); a larger query is answered by
    /// the fallback.
    pub fn max_query_size(mut self, bytes: usize, rows: usize) -> Self {
        self.cache.max_entry_bytes = bytes;
        self.cache.max_rows_per_query = rows;
        self
    }

    /// How long a query is kept after its last read, for a read that
    /// names no ttl (default 5 minutes).
    pub fn default_ttl(mut self, ttl: impl Into<Ttl>) -> Self {
        self.cache.default_ttl = ttl.into();
        self
    }

    /// How far behind Postgres the engine may be and still answer
    /// (default 2 seconds).
    pub fn max_lag(mut self, lag: Duration) -> Self {
        self.cache.max_lag = lag;
        self
    }

    /// What answers a read the engine cannot answer this time: it is
    /// starting, restarting or behind, the query is still loading, or a
    /// budget was exceeded (default [`Fallback::Postgres`]).
    pub fn fallback(mut self, fallback: Fallback) -> Self {
        self.fallback = fallback;
        self
    }

    /// What a read Streamgres can never answer does: its SQL, a table or
    /// column, or a type it does not hold exactly (default
    /// [`OnUnsupported::Error`]).
    pub fn on_unsupported(mut self, on_unsupported: OnUnsupported) -> Self {
        self.on_unsupported = on_unsupported;
        self
    }

    /// Lets [`Builder::start`] succeed before the engine is ready: reads go
    /// to the fallback while it comes up. Without it, an engine not ready
    /// within the start timeout is [`Error::NotReady`]. Either way, a setup
    /// that cannot work (the DDL trigger missing) is [`Error::Setup`], and
    /// [`Builder::fallback`] still decides what answers after startup.
    pub fn allow_degraded_start(mut self) -> Self {
        self.degraded_start = true;
        self
    }

    /// How long [`Builder::start`] waits for the engine to be ready, and
    /// how long one engine may take to start (default 30 seconds).
    pub fn start_timeout(mut self, timeout: Duration) -> Self {
        self.cache.start_timeout = timeout;
        self
    }

    /// Records, per query, each place in the application that reads it and
    /// its last `reads` reads, for [`Streamgres::inspect`] (default 0:
    /// nothing recorded). Recording takes a lock per read; meant for
    /// development and debugging.
    pub fn debug_history(mut self, reads: usize) -> Self {
        self.debug_history = reads;
        self
    }

    /// The SQL that installs the schema-change trigger stack the engine
    /// needs, for an administrator to run once on the database.
    pub fn setup_sql(&self) -> String {
        self.pg.setup_sql()
    }

    /// Starts the engine and waits up to the start timeout for it to be
    /// ready: [`Error::Setup`] when the DDL trigger is missing, and
    /// [`Error::NotReady`] when the engine is not ready by then, unless
    /// [`Builder::allow_degraded_start`] lets reads fall back while it comes
    /// up.
    ///
    /// Streamgres runs its connections and background work on a runtime
    /// of its own, so this future, and every read's, may be awaited on any
    /// executor.
    pub async fn start(self) -> Result<Streamgres, Error> {
        let app = tokio::runtime::Handle::try_current().ok();
        let io = Arc::new(Io::new().map_err(Error::Stopped)?);
        let runtime = io.handle().clone();
        runtime
            .spawn(self.start_on(io, app))
            .await
            .unwrap_or_else(|error| Err(Error::Stopped(format!("starting Streamgres: {error}"))))
    }

    /// [`Self::start`] for synchronous code: it blocks this thread until
    /// Streamgres has started. Not for an async context.
    pub fn start_blocking(self) -> Result<Streamgres, Error> {
        if !blocking_allowed(self.connections.is_some()) {
            return Err(Error::BlockingContext);
        }
        let io = Arc::new(Io::new().map_err(Error::Stopped)?);
        let app = tokio::runtime::Handle::try_current().ok();
        let runtime = io.handle().clone();
        wait(&runtime, self.start_on(io, app))
            .unwrap_or_else(|reason| Err(Error::Stopped(format!("starting Streamgres: {reason}"))))
    }

    /// Starts Streamgres; run on `io`, which it then runs on. `app` is
    /// the caller's runtime, if any: an application pool is used there.
    async fn start_on(
        self,
        io: Arc<Io>,
        app: Option<tokio::runtime::Handle>,
    ) -> Result<Streamgres, Error> {
        let reads_on_origin = self.connections.is_some();
        let (connections, runtime): (Arc<dyn Connections>, _) = match self.connections {
            Some(connections) => (
                connections,
                app.ok_or_else(|| {
                    Error::Stopped(
                        "an application pool (`.connections`) is used on the application's \
                         Tokio runtime: start Streamgres inside it"
                            .to_owned(),
                    )
                })?,
            ),
            None => (
                Arc::new(OwnPool::new(&self.pg.dsn, self.pool_size).map_err(Error::Stopped)?),
                io.handle().clone(),
            ),
        };
        let connections = Arc::new(OnRuntime {
            connections,
            runtime,
            io: io.clone(),
        });
        // A missing trigger is a setup mistake no wait will mend: refuse it
        // now, whether or not a degraded start is allowed. A check that
        // cannot run is left to the readiness wait.
        match trigger_present(connections.as_ref(), &self.pg.ddl_trigger()).await {
            Ok(false) => {
                return Err(Error::Setup(
                    "the DDL trigger is missing; install Builder::setup_sql".to_owned(),
                ));
            }
            Ok(true) => {}
            Err(reason) => log_warn!("Streamgres could not check its DDL trigger: {reason}"),
        }
        let engine = PgEngine::new(self.pg, connections, io.handle().clone(), reads_on_origin);
        let start_timeout = self.cache.start_timeout;
        let recorder = Recorder::new(self.debug_history, self.cache.max_entries.saturating_mul(2));
        let cache = Arc::new(Cache::start(self.cache, Arc::new(engine)));
        let waiting = cache.clone();
        let ready = tokio::task::spawn_blocking(move || waiting.wait_ready(start_timeout))
            .await
            .map_err(|error| Error::Stopped(error.to_string()))?;
        if let Err(refusal) = ready {
            if !self.degraded_start {
                stop(io.handle(), cache).await;
                return Err(Error::NotReady(refusal.to_string()));
            }
            log_warn!("Streamgres started before its engine was ready: {refusal}");
        }
        let permits = match &self.fallback {
            Fallback::Postgres { max_concurrent, .. } => {
                Some(Arc::new(Semaphore::new((*max_concurrent).max(1))))
            }
            Fallback::Error => None,
        };
        let lib = Streamgres {
            inner: Arc::new(Inner {
                cache,
                fallback: self.fallback,
                on_unsupported: self.on_unsupported,
                permits,
                reader: Mutex::new(None),
                types: RwLock::new(TypeRegistry::default()),
                type_refresh: AsyncMutex::new(()),
                type_generation: AtomicU64::new(0),
                reader_refresh: AsyncMutex::new(()),
                admission: AsyncMutex::new(None),
                application_pool: reads_on_origin,
                recorder,
                io,
            }),
        };
        if let Err(reason) = lib.load_types().await {
            log_warn!("initial type discovery: {reason}");
        }
        lib.watch_types();
        Ok(lib)
    }
}

/// Streamgres inside an application: an engine keeping the queries it is
/// asked for current from Postgres's change feed, and the reads answered
/// from them. Cheap to clone; every clone shares one engine. Dropping the last handle
/// requests shutdown without waiting. Call [`Self::shutdown`] to await cleanup.
#[derive(Clone)]
pub struct Streamgres {
    inner: Arc<Inner>,
}

struct Inner {
    cache: Arc<Cache>,
    fallback: Fallback,
    on_unsupported: OnUnsupported,
    /// The fallback's turns, for [`Fallback::Postgres`].
    permits: Option<Arc<Semaphore>>,
    /// Streamgres's own connection as checked against the catalog served
    /// by now.
    reader: Mutex<Option<Reader>>,
    /// The origin's own types by schema and name: their OIDs and their
    /// array types' OIDs.
    types: RwLock<TypeRegistry>,
    type_refresh: AsyncMutex<()>,
    type_generation: AtomicU64,
    reader_refresh: AsyncMutex<()>,
    admission: AsyncMutex<Option<Admission>>,
    application_pool: bool,
    /// The reads recorded for inspection.
    recorder: Recorder,
    /// Streamgres's runtime; last, so it outlives what runs on it.
    io: Arc<Io>,
}

struct Admission {
    catalog: Arc<crate::model::Catalog>,
    reader: Reader,
    checked: Instant,
}

#[derive(Default)]
struct TypeRegistry {
    qualified: HashMap<(String, String), (u32, u32)>,
    visible: HashMap<String, (u32, u32)>,
}

impl Streamgres {
    /// Starts configuring Streamgres for the database at `dsn`.
    pub fn connect(dsn: impl Into<String>) -> Builder {
        Builder {
            connections: None,
            pg: PgConfig::new(dsn),
            cache: CacheConfig::default(),
            fallback: Fallback::default(),
            on_unsupported: OnUnsupported::default(),
            pool_size: 8,
            debug_history: 0,
            degraded_start: false,
        }
    }

    /// The rows of `sql`. When the engine keeps its query they come from
    /// memory. When it does not yet, the query is registered and this read
    /// goes to the fallback, as does every read until the query has
    /// loaded; from then on the engine keeps its rows current and reads
    /// come from memory, until the query has gone unread for its ttl
    /// ([`LoadAndSync::ttl`]). A statement the engine cannot answer, or a
    /// read while it is not ready, goes to the fallback.
    #[track_caller]
    pub fn load_and_sync(&self, sql: Sql) -> LoadAndSync<'_> {
        LoadAndSync {
            lib: self,
            sql,
            ttl: None,
            at: Location::caller(),
        }
    }

    /// Where `sql`'s query stands now: kept or not and why, its rows and
    /// their weight, its ttl, when it was last read, the engine's position
    /// and lag and, with [`Builder::debug_history`], each place that reads
    /// it and its last reads. It asks the database nothing and changes
    /// nothing.
    pub fn inspect(&self, sql: &Sql) -> Inspect {
        let cache = &self.inner.cache;
        let query = QueryId::of(&sql.text, &sql.binds);
        let reader = self
            .inner
            .reader
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let described = cache.describe(&sql.text, &sql.binds, reader.as_ref());
        let (callers, recent) = self.inner.recorder.of(query);
        Inspect {
            query,
            sql: sql.text.clone(),
            state: described.state,
            rows: described.rows,
            bytes: described.bytes,
            ttl: described.ttl,
            last_read: described.last_read,
            expires_in: described.expires_in,
            engine: cache.engine_state(),
            callers,
            recent,
        }
    }

    /// The engine as a whole: whether it runs, on which instance, where it
    /// is and how far behind, and what it keeps.
    pub fn engine(&self) -> EngineState {
        self.inner.cache.engine_state()
    }

    /// Every query kept now, the most recently read first.
    pub fn queries(&self) -> Vec<KeptQuery> {
        self.inner.cache.kept()
    }

    /// The OIDs of a type of the database's own schemas (an enum's, a
    /// domain's) and of its array type, as Streamgres last read them (at
    /// start, and after catalog changes observed by the background watcher,
    /// or on [`Streamgres::refresh_types`]): what an
    /// adapter sends a custom type's parameters as. Without `schema`, resolve
    /// the name as the connection's search path does.
    pub fn type_oids(&self, name: &str, schema: Option<&str>) -> Option<(u32, u32)> {
        let types = self
            .inner
            .types
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match schema {
            Some(schema) => types
                .qualified
                .get(&(schema.to_owned(), name.to_owned()))
                .copied(),
            None => types.visible.get(name).copied(),
        }
    }

    /// Reads the database's types again, for [`Streamgres::type_oids`]:
    /// what an adapter does when a type it must name is not known yet (one
    /// created since Streamgres last read them).
    pub async fn refresh_types(&self) -> Result<(), Error> {
        self.load_types().await
    }

    /// [`Self::refresh_types`] for synchronous code: it blocks this
    /// thread until the types are read.
    pub fn refresh_types_blocking(&self) -> Result<(), Error> {
        if !blocking_allowed(self.inner.application_pool) {
            return Err(Error::BlockingContext);
        }
        let lib = self.clone();
        wait(
            self.inner.io.handle(),
            async move { lib.load_types().await },
        )
        .map_err(Error::Stopped)?
    }

    /// Reads the database's types again each time the engine's catalog
    /// changes (a schema change it followed, or a restart), checked every
    /// [`TYPES_EVERY`], until Streamgres is dropped.
    fn watch_types(&self) {
        let inner = Arc::downgrade(&self.inner);
        self.inner.io.handle().spawn(async move {
            let mut seen = None;
            loop {
                tokio::time::sleep(TYPES_EVERY).await;
                let Some(inner) = inner.upgrade() else { break };
                let version = inner.cache.catalog_version();
                if version.is_some() && version != seen {
                    match (Streamgres { inner }).load_types().await {
                        Ok(()) => seen = version,
                        Err(reason) => log_warn!("refreshing types: {reason}"),
                    }
                }
            }
        });
    }

    /// Reads the origin's types again ([`Streamgres::type_oids`]).
    async fn load_types(&self) -> Result<(), Error> {
        let observed = self.inner.type_generation.load(Ordering::Acquire);
        let _refresh = self.inner.type_refresh.lock().await;
        if self.inner.type_generation.load(Ordering::Acquire) != observed {
            return Ok(());
        }
        let found = self
            .inner
            .cache
            .engine()
            .origin_types()
            .await
            .map_err(|reason| Error::Origin(format!("refreshing custom types: {reason}")))?;
        let mut registry = TypeRegistry::default();
        for found in found {
            let oids = (found.oid, found.array_oid);
            if found.visible {
                registry.visible.insert(found.name.clone(), oids);
            }
            registry.qualified.insert((found.schema, found.name), oids);
        }
        *self
            .inner
            .types
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = registry;
        self.inner.type_generation.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// What Streamgres has done since it started and what it holds now.
    pub fn metrics(&self) -> Metrics {
        self.inner.cache.metrics()
    }

    /// Stops the engine and waits for it to clean up (its replication
    /// slot dropped). Reads after this go to the fallback.
    pub async fn shutdown(&self) {
        stop(self.inner.io.handle(), self.inner.cache.clone()).await;
    }

    /// [`Self::shutdown`] for synchronous code: it blocks this thread
    /// until the engine has cleaned up. Not for an async context.
    pub fn shutdown_blocking(&self) -> Result<(), Error> {
        if !blocking_allowed(self.inner.application_pool) {
            return Err(Error::BlockingContext);
        }
        let cache = self.inner.cache.clone();
        let outcome = wait(self.inner.io.handle(), async move {
            tokio::task::spawn_blocking(move || cache.stop()).await
        });
        outcome
            .map_err(Error::Stopped)?
            .map_err(|error| Error::Stopped(error.to_string()))
    }

    /// Streamgres's own connection as checked against the catalog the
    /// engine serves by now, checked again when that catalog changed.
    async fn reader(&self) -> Result<Reader, Refusal> {
        if let Some(reader) = self.current_reader()? {
            return Ok(reader);
        }
        let _refresh = self.inner.reader_refresh.lock().await;
        if let Some(reader) = self.current_reader()? {
            return Ok(reader);
        }
        let cache = &self.inner.cache;
        let check = cache.reader_check()?;
        let row = cache
            .engine()
            .check_origin(check.sql, [check.names, check.as_written, check.qualified])
            .await
            .map_err(Refusal::NotReady)?;
        let excluded = row.first().map(String::as_str).unwrap_or("");
        let reader = Reader::new(excluded, check.version);
        *self
            .inner
            .reader
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(reader.clone());
        Ok(reader)
    }

    /// The reader as checked against the catalog the engine serves by
    /// now, when it was; `None` when it must be checked first.
    fn current_reader(&self) -> Result<Option<Reader>, Refusal> {
        let version = self
            .inner
            .cache
            .catalog_version()
            .ok_or_else(|| Refusal::NotReady("the engine is not running".to_owned()))?;
        Ok(self
            .inner
            .reader
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .filter(|reader| reader.is_current(&version))
            .cloned())
    }

    /// As far as a read gets on the caller's thread, with no I/O: answered
    /// from memory or unsupported, or what is left for Streamgres's
    /// runtime ([`Self::finish`]).
    fn begin(&self, sql: &Sql, ttl: Option<Ttl>) -> Begun {
        let reader = match self.current_reader() {
            Ok(Some(reader)) => reader,
            Ok(None) => return Begun::Unchecked,
            Err(refusal) => return Begun::Refused(refusal),
        };
        let read = self
            .inner
            .cache
            .encoded(&sql.text, &sql.binds, &reader, ttl);
        match read {
            Ok(rows) => Begun::Done((Ok(rows), Served::Memory, None)),
            Err(refusal) => match self.unsupported(refusal) {
                Ok(outcome) => Begun::Done(outcome),
                Err(refusal) => Begun::Refused(refusal),
            },
        }
    }

    /// The outcome of an unsupported statement, when it is an error; the
    /// refusal back otherwise, for the fallback.
    fn unsupported(&self, refusal: Refusal) -> Result<Outcome, Refusal> {
        match &refusal {
            Refusal::Unsupported(_) if self.inner.on_unsupported == OnUnsupported::Error => {
                let reason = refusal.to_string();
                Ok((
                    Err(Error::Unsupported(reason.clone())),
                    Served::Unsupported,
                    Some(reason),
                ))
            }
            _ => Err(refusal),
        }
    }

    /// A degraded engine does not turn unsupported statements into implicit
    /// Postgres reads. Source admission is single-flight and briefly memoized.
    async fn validate_degraded(&self, sql: &Sql) -> Result<(), Refusal> {
        let mut admission = self.inner.admission.lock().await;
        if admission
            .as_ref()
            .is_none_or(|snapshot| snapshot.checked.elapsed() >= Duration::from_millis(250))
        {
            let engine = self.inner.cache.engine();
            let catalog = engine.origin_catalog().await.map_err(Refusal::NotReady)?;
            let check = super::ReaderCheck::of(&catalog);
            let row = engine
                .check_origin(check.sql, [check.names, check.as_written, check.qualified])
                .await
                .map_err(Refusal::NotReady)?;
            let excluded = row
                .first()
                .ok_or_else(|| Refusal::NotReady("admission check returned no row".into()))?;
            let reader = Reader::new(excluded, check.version);
            *admission = Some(Admission {
                catalog,
                reader,
                checked: Instant::now(),
            });
        }
        if let Some(snapshot) = admission.as_ref() {
            self.inner
                .cache
                .validate(&sql.text, &sql.binds, &snapshot.catalog, &snapshot.reader)
        } else {
            Err(Refusal::NotReady("admission is unavailable".into()))
        }
    }

    /// The rest of a read [`Self::begin`] left, on Streamgres's runtime:
    /// the reader checked, and the fallback.
    async fn finish(self, sql: Sql, ttl: Option<Ttl>, begun: Begun) -> Outcome {
        let refusal = match begun {
            Begun::Done(outcome) => return outcome,
            Begun::Refused(refusal) => refusal,
            Begun::Unchecked => match self.read_cache(&sql, ttl).await {
                Ok(rows) => return (Ok(rows), Served::Memory, None),
                Err(refusal) => match self.unsupported(refusal) {
                    Ok(outcome) => return outcome,
                    Err(refusal) => refusal,
                },
            },
        };
        if matches!(refusal, Refusal::NotReady(_))
            && self.inner.on_unsupported == OnUnsupported::Error
            && let Err(admission) = self.validate_degraded(&sql).await
        {
            return match self.unsupported(admission) {
                Ok(outcome) => outcome,
                Err(reason) => (
                    Err(Error::NotReady(reason.to_string())),
                    Served::Failed,
                    Some(reason.to_string()),
                ),
            };
        }
        let reason = refusal.to_string();
        let read = self.fall_back(&sql, refusal).await;
        let served = if read.is_ok() {
            Served::Postgres
        } else {
            Served::Failed
        };
        (read, served, Some(reason))
    }

    /// One read, awaited: a memory hit on the caller's thread, anything
    /// else on Streamgres's runtime.
    async fn traced(
        &self,
        sql: Sql,
        ttl: Option<Ttl>,
        at: &'static Location<'static>,
        traced: bool,
    ) -> (Result<Rows, Error>, Option<Trace>) {
        let started = Instant::now();
        let outcome = match self.begin(&sql, ttl) {
            Begun::Done(outcome) => outcome,
            begun => {
                let (lib, task) = (self.clone(), sql.clone());
                crate::sync::pg::connections::Task::new(
                    self.inner.io.handle().spawn(lib.finish(task, ttl, begun)),
                )
                .join()
                .await
                .unwrap_or_else(|error| ended(&error.to_string()))
            }
        };
        self.record(sql, at, started, outcome, traced)
    }

    /// [`Self::traced`], blocking this thread instead of awaiting.
    fn traced_blocking(
        &self,
        sql: Sql,
        ttl: Option<Ttl>,
        at: &'static Location<'static>,
        traced: bool,
    ) -> (Result<Rows, Error>, Option<Trace>) {
        let started = Instant::now();
        let outcome = match self.begin(&sql, ttl) {
            Begun::Done(outcome) => outcome,
            begun => {
                let (lib, task) = (self.clone(), sql.clone());
                if !blocking_allowed(self.inner.application_pool) {
                    (
                        Err(Error::BlockingContext),
                        Served::Failed,
                        Some(Error::BlockingContext.to_string()),
                    )
                } else {
                    wait(self.inner.io.handle(), lib.finish(task, ttl, begun))
                        .unwrap_or_else(|reason| ended(&reason))
                }
            }
        };
        self.record(sql, at, started, outcome, traced)
    }

    /// Records how a read went, when anyone listens: the rows and, when
    /// `traced`, its trace.
    fn record(
        &self,
        sql: Sql,
        at: &'static Location<'static>,
        started: Instant,
        (read, served, reason): Outcome,
        traced: bool,
    ) -> (Result<Rows, Error>, Option<Trace>) {
        let took = started.elapsed();
        let recording = self.inner.recorder.on();
        if served != Served::Memory && enabled(Level::Debug) {
            log_debug!(
                "query {} at {at}: {served} in {took:?}: {}",
                QueryId::of(&sql.text, &sql.binds),
                reason.as_deref().unwrap_or("")
            );
        }
        if !traced && !recording {
            return (read, None);
        }
        let cache = &self.inner.cache;
        let engine = cache.engine_state();
        let kept = cache.describe(&sql.text, &sql.binds, None);
        let trace = Trace {
            query: QueryId::of(&sql.text, &sql.binds),
            rows: read.as_ref().map_or(0, Rows::len),
            sql: sql.text,
            served,
            reason: reason.or_else(|| read.as_ref().err().map(ToString::to_string)),
            ttl: kept.ttl,
            took,
            position: engine.position,
            lag: engine.lag,
            at,
            when: started,
        };
        self.inner.recorder.record(&trace);
        (read, traced.then_some(trace))
    }

    /// The rows from memory, or why not. A read of a query still loading
    /// does not wait for it: the fallback answers it.
    async fn read_cache(&self, sql: &Sql, ttl: Option<Ttl>) -> Result<Rows, Refusal> {
        let reader = self.reader().await?;
        self.inner
            .cache
            .encoded(&sql.text, &sql.binds, &reader, ttl)
    }

    /// The rows from the fallback, for a read the cache refused.
    async fn fall_back(&self, sql: &Sql, refusal: Refusal) -> Result<Rows, Error> {
        let cache = &self.inner.cache;
        let Fallback::Postgres { wait, timeout, .. } = &self.inner.fallback else {
            cache.count_fallback(false);
            return Err(Error::Refused(refusal.to_string()));
        };
        let Some(permits) = &self.inner.permits else {
            cache.count_fallback(false);
            return Err(Error::Refused(refusal.to_string()));
        };
        let Ok(Ok(permit)) = tokio::time::timeout(*wait, permits.clone().acquire_owned()).await
        else {
            cache.count_fallback(false);
            return Err(Error::Busy);
        };
        let read = cache
            .engine()
            .read_origin(&sql.text, &sql.binds, *timeout, permit)
            .await;
        cache.count_fallback(read.is_ok());
        read.map_err(Error::Origin)
    }
}

/// A read of [`Streamgres::load_and_sync`]: await it for the rows.
#[must_use = "a read does nothing until it is awaited"]
pub struct LoadAndSync<'a> {
    lib: &'a Streamgres,
    sql: Sql,
    ttl: Option<Ttl>,
    at: &'static Location<'static>,
}

impl<'a> LoadAndSync<'a> {
    /// Keeps the query this long after its last read, overriding the
    /// default. Of several reads of one query, the
    /// longest ttl stands.
    pub fn ttl(mut self, ttl: impl Into<Ttl>) -> Self {
        self.ttl = Some(ttl.into());
        self
    }

    /// The place in the application the read is made from, when not where
    /// [`Streamgres::load_and_sync`] was called: what an adapter passes on
    /// from its own caller.
    pub fn called_at(mut self, at: &'static Location<'static>) -> Self {
        self.at = at;
        self
    }

    /// The read with its [`Trace`]: who answered it, why the engine did
    /// not if it did not, how long it took and where it was made. The
    /// trace comes back whether the read succeeded or not.
    pub fn inspect(self) -> Inspecting<'a> {
        Inspecting(self)
    }

    /// The read for synchronous code: the rows, this thread blocked until
    /// they are here. A memory hit returns at once; any other read is
    /// made on Streamgres's runtime. Needs no runtime of the caller's.
    pub fn wait(self) -> Result<Rows, Error> {
        self.lib
            .traced_blocking(self.sql, self.ttl, self.at, false)
            .0
    }
}

impl<'a> IntoFuture for LoadAndSync<'a> {
    type Output = Result<Rows, Error>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.lib.traced(self.sql, self.ttl, self.at, false).await.0 })
    }
}

/// A read with its trace ([`LoadAndSync::inspect`]): await it for both.
#[must_use = "a read does nothing until it is awaited"]
pub struct Inspecting<'a>(LoadAndSync<'a>);

impl Inspecting<'_> {
    /// The read and its trace for synchronous code
    /// ([`LoadAndSync::wait`]).
    pub fn wait(self) -> (Result<Rows, Error>, Trace) {
        let LoadAndSync { lib, sql, ttl, at } = self.0;
        let (read, trace) = lib.traced_blocking(sql, ttl, at, true);
        (read, trace.expect("a traced read has a trace"))
    }
}

impl<'a> IntoFuture for Inspecting<'a> {
    type Output = (Result<Rows, Error>, Trace);
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let LoadAndSync { lib, sql, ttl, at } = self.0;
        Box::pin(async move {
            let (read, trace) = lib.traced(sql, ttl, at, true).await;
            (read, trace.expect("a traced read has a trace"))
        })
    }
}

/// Whether the DDL trigger `name` is installed, checked on one of
/// `connections`.
async fn trigger_present(connections: &dyn Connections, name: &str) -> Result<bool, String> {
    let mut lease = connections.get().await?;
    let present = crate::sync::pg::ddl::trigger_present(&mut lease, name)
        .await
        .map_err(|error| error.to_string())?;
    lease.recycle();
    Ok(present)
}

/// Stops `cache` on `runtime`'s blocking threads, off the caller's.
async fn stop(runtime: &tokio::runtime::Handle, cache: Arc<Cache>) {
    let _ = runtime.spawn_blocking(move || cache.stop()).await;
}

/// The rows, who answered, and why the engine did not when it did not.
type Outcome = (Result<Rows, Error>, Served, Option<String>);

/// A read as far as [`Streamgres::begin`] took it.
enum Begun {
    Done(Outcome),
    /// Refused, for the fallback.
    Refused(Refusal),
    /// The reader must be checked before the engine is asked.
    Unchecked,
}

/// A read whose task on Streamgres's runtime ended without an answer.
fn ended(reason: &str) -> Outcome {
    (
        Err(Error::Origin(format!("the read ended: {reason}"))),
        Served::Failed,
        None,
    )
}

/// Runs `work` on `runtime` and blocks this thread until it is done.
fn wait<T: Send + 'static>(
    runtime: &tokio::runtime::Handle,
    work: impl Future<Output = T> + Send + 'static,
) -> Result<T, String> {
    let run = || {
        let (done, outcome) = std::sync::mpsc::sync_channel(1);
        runtime.spawn(async move {
            let _ = done.send(work.await);
        });
        outcome
            .recv()
            .map_err(|_| "the task ended without finishing".to_owned())
    };
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(run)
        }
        _ => run(),
    }
}

fn blocking_allowed(application_pool: bool) -> bool {
    !application_pool
        || tokio::runtime::Handle::try_current().is_ok_and(|handle| {
            handle.runtime_flavor() != tokio::runtime::RuntimeFlavor::CurrentThread
        })
        || tokio::runtime::Handle::try_current().is_err()
}
