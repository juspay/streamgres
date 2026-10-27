//! A Diesel connection that sits between an application and Postgres and
//! serves reads from a local copy the Streamgres engine keeps fresh.
//!
//! `SyncedConnection<C>` wraps any Diesel Postgres connection `C` and
//! implements the same traits, so an application swaps its connection type
//! and nothing else changes: the query DSL, `sql_query`, transactions, pools
//! and `#[derive(Queryable)]` all go through unchanged.
//!
//! Every read Diesel performs reaches [`LoadConnection::load`]; every write
//! reaches [`Connection::execute_returning_count`]; raw multi-statement SQL
//! reaches [`SimpleConnection::batch_execute`]. Those three methods are the
//! whole interception surface, and the wrapper owns all of them. Writes
//! always go to Postgres. A read is answered from the [`cache::Cache`]
//! when all of these hold:
//!
//! - the read's route is [`Route::Streamgres`]: flagged so on the query
//!   ([`RouteDsl::via`]), or in a [`SyncedConnection::with_route`] scope,
//!   or by the connection's own route, in that order of precedence;
//! - no transaction is open on it (inside one, the application must see
//!   its own writes), and every write through the cache has reached it:
//!   by the engine's position when it reports one, else for the
//!   [`SyncedConnection::read_your_writes`] window after the last write
//!   (after one, a read must see what was written before the engine has
//!   shipped it);
//! - the statement is a `SELECT` (Diesel sends `INSERT ... RETURNING`
//!   through the same path);
//! - the engine accepted a subscription for the query
//!   ([`subscriptions::Subscriptions`]).
//!
//! On a miss the query is subscribed first. When the engine can serve
//! the rows itself (every projected column has a type the client encodes
//! exactly, see [`encode`]), the result comes from the engine, is kept as
//! Diesel rows ([`materialized`]), and every later change replaces it in
//! place; Postgres is never read for that query. Otherwise the rows are
//! read from Postgres once and kept byte for byte ([`row::OwnedRow`]),
//! and when the engine reports the query touched they are dropped and the
//! next read refills them. Every other read goes to Postgres as before.
//!
//! # Why the opt-in feature
//!
//! Diesel seals `Connection` behind the cargo feature
//! `i-implement-a-third-party-backend-and-opt-into-breaking-changes`.
//! Consumers that already wrap their connection enable it, so this crate
//! does too rather than reaching into Diesel's hidden derive re-exports.

#[cfg(feature = "async")]
pub mod asynchronous;
pub mod cache;
pub mod cursor;
pub mod encode;
pub mod engine;
pub mod key;
pub mod materialized;
pub mod route;
pub mod row;
mod session;
pub mod subscriptions;
pub mod types;

use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use diesel::connection::{
    AnsiTransactionManager, ConnectionSealed, DefaultLoadingMode, Instrumentation, LoadConnection,
    SimpleConnection, TransactionManager,
};
use diesel::expression::QueryMetadata;
use diesel::pg::{GetPgMetadataCache, Pg, PgMetadataCache};
use diesel::query_builder::{Query, QueryFragment, QueryId};
use diesel::r2d2::R2D2Connection;
use diesel::{Connection, ConnectionResult, QueryResult};

#[cfg(feature = "async")]
pub use crate::asynchronous::AsyncSyncedConnection;
use crate::cache::{
    AfterWrites, CONTEXT_SQL, Cache, Context, REPLAY_POSITION_SQL, Read, WAL_POSITION_SQL,
    WalPosition,
};
use crate::session::{Effect, Scope, SessionChanges};
use crate::cursor::{SyncedCursor, SyncedRow};
use crate::key::QueryKey;
pub use crate::route::{Route, RouteDsl, Served, Via};
/// The engine inside this process ([`engine::EngineClient::embedded`]):
/// its handle, its configuration and the SQL that sets a database up
/// for it, so an application depends on this crate alone.
#[cfg(feature = "embedded")]
pub use streamgres::embedded;
use crate::row::{OwnedCursor, OwnedRow, capture_all};
use crate::subscriptions::Verdict;

/// [`Route`] under another name: `Consistency::Eventual` is
/// [`Route::Streamgres`], `Consistency::Strong` is [`Route::Postgres`].
pub type Consistency = Route;

/// What a connection established with some URL gets: the cache its
/// reads go through and the route they take by default.
#[derive(Clone)]
pub struct Attachment {
    pub cache: Arc<Cache>,
    pub route: Route,
}

/// What a process shares between its connections, by the URL they are
/// established with.
///
/// One engine streams one database, and a cache is kept fresh by one
/// engine, so a cache is per database: pools against the same database
/// (a master and its replicas) share one, pools against different
/// databases each get their own, attached to the engine streaming that
/// database. The route is
/// per pool: a master pool can go to Postgres while a replica pool goes
/// through Streamgres.
pub struct Settings {
    for_url: Box<AttachmentFor>,
}

/// What [`Settings`] asks of a URL.
type AttachmentFor = dyn Fn(&str) -> Option<Attachment> + Send + Sync;

impl Settings {
    /// One database: every connection shares `cache`, routed by its URL.
    pub fn single(
        cache: Arc<Cache>,
        route_for: impl Fn(&str) -> Route + Send + Sync + 'static,
    ) -> Settings {
        Settings::by_url(move |url| {
            Some(Attachment {
                cache: Arc::clone(&cache),
                route: route_for(url),
            })
        })
    }

    /// A cache and a route per URL. A URL that yields `None` gets a
    /// passthrough connection: every read goes to Postgres.
    pub fn by_url(
        for_url: impl Fn(&str) -> Option<Attachment> + Send + Sync + 'static,
    ) -> Settings {
        Settings {
            for_url: Box::new(for_url),
        }
    }

    /// What a connection established with `url` gets.
    pub fn attachment_for(&self, url: &str) -> Option<Attachment> {
        (self.for_url)(url)
    }
}

/// The settings [`install`] put in place, for every connection
/// established after.
static SETTINGS: OnceLock<Settings> = OnceLock::new();

/// How long after a write a connection's reads go to Postgres when the
/// environment says nothing.
pub const DEFAULT_READ_YOUR_WRITES: Duration = Duration::from_millis(1000);

/// `STREAMGRES_READ_YOUR_WRITES_MS`: how long after a write a
/// connection's reads go to Postgres when the engine reports no
/// position, so a flow that writes and then reads sees its own write
/// before the engine has shipped it. 0 turns read-your-writes off, by
/// position too. Read once per process.
pub fn read_your_writes_from_env() -> Duration {
    static WINDOW: OnceLock<Duration> = OnceLock::new();
    *WINDOW.get_or_init(|| {
        std::env::var("STREAMGRES_READ_YOUR_WRITES_MS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_READ_YOUR_WRITES)
    })
}

/// Installs the process-wide settings. Connections established after this
/// (including by pools, which call [`Connection::establish`]) use them;
/// before it, every connection is a passthrough. Returns the settings back
/// if some were already installed.
pub fn install(settings: Settings) -> Result<(), Settings> {
    SETTINGS.set(settings)
}

/// A Postgres connection whose reads can be served from a synced local copy.
///
/// `C` is the connection that talks to Postgres, normally
/// [`diesel::PgConnection`]. Everything the wrapper does not understand is
/// delegated to it untouched.
pub struct SyncedConnection<C = diesel::PgConnection> {
    inner: C,
    cache: Option<Arc<Cache>>,
    route: Route,
    /// A [`SyncedConnection::with_route`] scope in progress.
    scoped: Option<Route>,
    read_your_writes: Duration,
    /// Where the last read was served from; `None` before the first.
    served: Option<Served>,
    /// Whether the connection is known to be to a primary, whose reads
    /// are current; learned on the first refill, and never asked again.
    primary: bool,
    /// What its statements have changed in its session.
    session: SessionChanges,
    /// Whether it reads as the cache's answers are for.
    scope: Scope,
}

impl<C> SyncedConnection<C> {
    /// Wraps an already established connection as a passthrough.
    pub fn new(inner: C) -> Self {
        SyncedConnection {
            inner,
            cache: None,
            route: Route::Postgres,
            scoped: None,
            read_your_writes: Duration::ZERO,
            served: None,
            primary: false,
            session: SessionChanges::default(),
            scope: Scope::Unchecked,
        }
    }

    /// Wraps a connection with a cache and the route its reads take
    /// unless a query or a scope says otherwise.
    pub fn with_cache(inner: C, cache: Arc<Cache>, route: Route) -> Self {
        SyncedConnection {
            inner,
            cache: Some(cache),
            route,
            scoped: None,
            read_your_writes: read_your_writes_from_env(),
            served: None,
            primary: false,
            session: SessionChanges::default(),
            scope: Scope::Unchecked,
        }
    }

    /// Where this connection's last read was served from: the cache,
    /// the engine's rows, or Postgres. `None` before any read.
    pub fn last_served(&self) -> Option<Served> {
        self.served
    }

    /// Changes the route this connection's reads take by default.
    pub fn set_route(&mut self, route: Route) {
        self.route = route;
    }

    /// The route this connection's reads take by default.
    pub fn route(&self) -> Route {
        self.route
    }

    /// [`SyncedConnection::set_route`] under another name.
    pub fn set_consistency(&mut self, consistency: Consistency) {
        self.set_route(consistency);
    }

    /// [`SyncedConnection::route`] under another name.
    pub fn consistency(&self) -> Consistency {
        self.route()
    }

    /// How long after a write reads through this connection's cache go
    /// to Postgres when the engine reports no position. A write is
    /// followed, in the same flow, by reads that expect to see it, often
    /// on another connection of the same pool; the cache sees the write
    /// only once the engine has shipped the commit. With positions, reads
    /// go to Postgres until the engine has delivered past the write, and
    /// the window only says whether to wait at all (zero turns it off);
    /// without, every connection sharing the cache reads the database for
    /// the window. The clock is the cache's; the window is this
    /// connection's. From `STREAMGRES_READ_YOUR_WRITES_MS` by default.
    pub fn set_read_your_writes(&mut self, window: Duration) {
        self.read_your_writes = window;
    }

    /// How long after a write reads go to Postgres.
    pub fn read_your_writes(&self) -> Duration {
        self.read_your_writes
    }

    /// Marks a write on the cache's clock. A write is marked as it
    /// starts and again as it ends, so the read-your-writes window runs
    /// from its end however long it took: a read after it on any
    /// connection of the cache goes to Postgres for the whole window.
    fn wrote(&mut self) {
        if let Some(cache) = &self.cache {
            cache.mark_write();
        }
    }

    /// Takes in what `sql` does to the connection's session
    /// ([`SessionChanges`]): once it is back as established, what the
    /// connection reads as is read again before the cache serves it.
    fn observe(&mut self, sql: &str) {
        if self.cache.is_some() && self.session.apply(sql) == Effect::Restored {
            self.scope = Scope::Unchecked;
        }
    }


    /// Runs `f` with every read inside it routed `route`, whatever the
    /// connection's own route; a flag on the query itself still wins.
    /// For code that holds the connection anyway, such as a pool's
    /// `interact` closure. The outer route is restored however `f`
    /// ends, a panic included.
    pub fn with_route<R>(&mut self, route: Route, f: impl FnOnce(&mut Self) -> R) -> R {
        /// Puts the outer route back when dropped.
        struct Restore<'a, C> {
            conn: &'a mut SyncedConnection<C>,
            outer: Option<Route>,
        }
        impl<C> Drop for Restore<'_, C> {
            /// Restores the outer route.
            fn drop(&mut self) {
                self.conn.scoped = self.outer;
            }
        }
        let outer = self.scoped.replace(route);
        let restore = Restore { conn: self, outer };
        f(&mut *restore.conn)
    }

    /// The cache this connection reads through, if any.
    pub fn cache(&self) -> Option<&Arc<Cache>> {
        self.cache.as_ref()
    }

    /// The connection that talks to Postgres.
    pub fn inner(&mut self) -> &mut C {
        &mut self.inner
    }

    /// Gives the Postgres connection back.
    pub fn into_inner(self) -> C {
        self.inner
    }
}

impl<C> SyncedConnection<C>
where
    C: LoadConnection<DefaultLoadingMode>
        + Connection<Backend = Pg, TransactionManager = AnsiTransactionManager>,
{
    /// Whether this connection's reads may use `cache`: its session as it
    /// was established, and what it reads as the cache's answers are for
    /// ([`Cache::admits`]), read on its first read through the cache and
    /// again once its session is back as established. When that cannot
    /// be read, this read goes to Postgres and the next asks again.
    fn shares(&mut self, cache: &Cache) -> bool {
        if self.session.changed() {
            return false;
        }
        if self.scope == Scope::Unchecked {
            let Some(context) = context_of(&mut self.inner) else {
                return false;
            };
            self.scope = if cache.admits(&context) {
                Scope::Shared
            } else {
                Scope::Own
            };
        }
        self.scope == Scope::Shared
    }

    /// Once a write has committed, reads its WAL position on this
    /// connection, when reads through the cache go to standbys
    /// ([`Cache::reads_standbys`]): a standby cannot say where the
    /// primary's WAL is, so without this its reads would wait out the
    /// window. Inside a transaction nothing has committed yet; its
    /// `COMMIT` (`committed`) reads it.
    fn resolve_written(&mut self, committed: bool) {
        let Some(cache) = self.cache.clone() else {
            return;
        };
        if !cache.reads_standbys() || (!committed && in_transaction(&mut self.inner)) {
            return;
        }
        let writes = cache.write_count();
        if let Some(Read::At(position)) = read_position(&mut self.inner, WAL_POSITION_SQL) {
            cache.resolve_writes(writes, position);
        }
    }

    /// Whether a read on this connection now sees every change up to
    /// `since`, the last the engine will not report for the query: on a
    /// primary always (learned once), on a standby once it has replayed
    /// past it. Asked before the read, which then sees at least as much.
    /// A standby with no position to compare, or one that cannot say how
    /// far it has replayed, is not trusted.
    fn replayed_past(&mut self, cache: &Cache, since: Option<u64>) -> bool {
        if self.primary {
            return true;
        }
        match read_position(&mut self.inner, REPLAY_POSITION_SQL) {
            Some(Read::Null) => {
                self.primary = true;
                true
            }
            Some(Read::At(replayed)) => {
                cache.saw_standby();
                since.is_some_and(|since| replayed >= since)
            }
            None => false,
        }
    }
}

impl<C> SimpleConnection for SyncedConnection<C>
where
    C: LoadConnection<DefaultLoadingMode>
        + Connection<Backend = Pg, TransactionManager = AnsiTransactionManager>,
{
    /// Raw SQL may write; so does the ANSI manager's `COMMIT`, which is
    /// when a transaction's writes become what the next read expects.
    /// Opening, naming a point in, or abandoning a transaction writes
    /// nothing.
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        self.observe(query);
        let writes = may_write(query);
        if writes {
            self.wrote();
        }
        let done = self.inner.batch_execute(query);
        if writes {
            self.wrote();
            if done.is_ok() {
                self.resolve_written(commits(query));
            }
        }
        done
    }
}

impl<C> ConnectionSealed for SyncedConnection<C> {}

impl<C> Connection for SyncedConnection<C>
where
    C: LoadConnection<DefaultLoadingMode>
        + Connection<Backend = Pg, TransactionManager = AnsiTransactionManager>,
{
    type Backend = Pg;
    /// The ANSI manager is generic over the connection it drives: it
    /// issues `BEGIN`/`COMMIT`/`ROLLBACK` through `batch_execute` and
    /// keeps its depth in whatever `transaction_state` returns, so the
    /// wrapper reuses it and points it at the inner connection's state.
    /// One transaction state, not two that could disagree.
    type TransactionManager = AnsiTransactionManager;

    /// Connects, attached to the cache [`install`] set up for this URL,
    /// if any.
    fn establish(database_url: &str) -> ConnectionResult<Self> {
        let inner = C::establish(database_url)?;
        Ok(
            match SETTINGS
                .get()
                .and_then(|settings| settings.attachment_for(database_url))
            {
                Some(Attachment { cache, route }) => {
                    SyncedConnection::with_cache(inner, cache, route)
                }
                None => SyncedConnection::new(inner),
            },
        )
    }

    /// Begins Diesel's test transaction on the inner connection.
    fn begin_test_transaction(&mut self) -> QueryResult<()> {
        self.inner.begin_test_transaction()
    }

    /// A pool verifies a connection on checkout with `SELECT 1` through
    /// this path; that is not a write.
    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Pg> + QueryId,
    {
        let sql = rendered(source);
        self.observe(&sql);
        let writes = may_write(&sql);
        if writes {
            self.wrote();
        }
        let done = self.inner.execute_returning_count(source);
        if writes {
            self.wrote();
            if done.is_ok() {
                self.resolve_written(false);
            }
        }
        done
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
}

impl<C, B> LoadConnection<B> for SyncedConnection<C>
where
    C: LoadConnection<B>
        + LoadConnection<DefaultLoadingMode>
        + GetPgMetadataCache
        + Connection<Backend = Pg, TransactionManager = AnsiTransactionManager>
        + 'static,
{
    type Cursor<'conn, 'query>
        = SyncedCursor<<C as LoadConnection<B>>::Cursor<'conn, 'query>>
    where
        Self: 'conn;
    type Row<'conn, 'query>
        = SyncedRow<<C as LoadConnection<B>>::Row<'conn, 'query>>
    where
        Self: 'conn;

    /// A read, from the cache when it can be.
    ///
    /// The key is rendered before the route is known: a flag on the
    /// query travels in its SQL, and is stripped from the key. A locking
    /// read goes to Postgres, since the lock is the point: `FOR UPDATE`
    /// outside a transaction still blocks a concurrent writer for the
    /// statement. On a miss the generation is taken before subscribing,
    /// so a touch, or an expiry that ends the subscription, between then
    /// and the fill leaves nothing cached rather than something no one
    /// updates. Once subscribed, the engine reports every change from
    /// there on, so what Postgres returns then is current until the
    /// first touch; a read that fails leaves no entry, and the
    /// subscription ends with it rather than being held for nothing. A
    /// connection's first read through the cache first reads what the
    /// connection reads as ([`Context`]): the cache serves it only if
    /// that is what the cache's answers are for ([`Cache::admits`]), and
    /// only while its session is as it was established
    /// ([`SessionChanges`]). The first connection admitted also tells the
    /// cache which database its reads go to, which the engine side checks
    /// its engine streams. While the engine side is too far behind the
    /// database, reads go to Postgres.
    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> QueryResult<Self::Cursor<'conn, 'query>>
    where
        T: Query + QueryFragment<Pg> + QueryId + 'query,
        Pg: QueryMetadata<T::SqlType>,
    {
        self.served = Some(Served::Postgres);
        let Some(cache) = self.cache.clone() else {
            return LoadConnection::<B>::load(&mut self.inner, source).map(SyncedCursor::Live);
        };
        let mut key = QueryKey::of(&source, &mut self.inner)?;
        self.observe(&key.sql);
        let route = key.take_route().or(self.scoped).unwrap_or(self.route);
        if !key.is_select() {
            self.wrote();
            if cache.reads_standbys() && !in_transaction(&mut self.inner) {
                let rows = LoadConnection::<DefaultLoadingMode>::load(&mut self.inner, source)
                    .and_then(capture_all);
                self.wrote();
                let rows = rows?;
                self.resolve_written(false);
                return Ok(SyncedCursor::Cached(OwnedCursor::new(Arc::from(rows))));
            }
            let done = LoadConnection::<B>::load(&mut self.inner, source).map(SyncedCursor::Live);
            cache.mark_write();
            return done;
        }
        if route == Route::Postgres || in_transaction(&mut self.inner) {
            return LoadConnection::<B>::load(&mut self.inner, source).map(SyncedCursor::Live);
        }
        if key.is_locking() {
            cache.stats.locking.fetch_add(1, Ordering::Relaxed);
            return LoadConnection::<B>::load(&mut self.inner, source).map(SyncedCursor::Live);
        }
        if after_writes(&cache, self.read_your_writes, &mut self.inner) != AfterWrites::Serve {
            cache.stats.after_write.fetch_add(1, Ordering::Relaxed);
            return LoadConnection::<B>::load(&mut self.inner, source).map(SyncedCursor::Live);
        }
        if cache.subscriptions().behind() {
            cache.stats.lagging.fetch_add(1, Ordering::Relaxed);
            return LoadConnection::<B>::load(&mut self.inner, source).map(SyncedCursor::Live);
        }
        if !self.shares(&cache) {
            cache.stats.other_context.fetch_add(1, Ordering::Relaxed);
            return LoadConnection::<B>::load(&mut self.inner, source).map(SyncedCursor::Live);
        }
        if let Some(rows) = cache.get(&key) {
            self.served = Some(Served::Cache);
            return Ok(SyncedCursor::Cached(OwnedCursor::new(rows)));
        }
        if cache.refused_recently(&key).is_some() {
            return LoadConnection::<B>::load(&mut self.inner, source).map(SyncedCursor::Live);
        }
        let reservation = cache.reserve(&key);
        let since = match cache.subscriptions().subscribe_with(&key, &mut self.inner) {
            Verdict::Refused(reason) => {
                cache.refused(&key, &reason);
                return LoadConnection::<B>::load(&mut self.inner, source).map(SyncedCursor::Live);
            }
            Verdict::Rows { rows, live } => {
                cache.fill_live(reservation, live, &rows);
                if cache.subscriptions().behind() {
                    cache.stats.lagging.fetch_add(1, Ordering::Relaxed);
                    return LoadConnection::<B>::load(&mut self.inner, source)
                        .map(SyncedCursor::Live);
                }
                self.served = Some(Served::Engine);
                return Ok(SyncedCursor::Cached(OwnedCursor::new(rows)));
            }
            Verdict::Accepted { since } => since,
        };
        let keep = self.replayed_past(&cache, since);
        let rows = LoadConnection::<DefaultLoadingMode>::load(&mut self.inner, source)
            .and_then(capture_all);
        let rows: Arc<[OwnedRow]> = match rows {
            Ok(rows) => Arc::from(rows),
            Err(error) => {
                cache.abandon(&key);
                return Err(error);
            }
        };
        if keep {
            cache.fill(reservation, Arc::clone(&rows));
        } else {
            cache.stats.unreplayed.fetch_add(1, Ordering::Relaxed);
        }
        Ok(SyncedCursor::Cached(OwnedCursor::new(rows)))
    }
}

/// What `conn` reads as ([`Context`]); `None` when it cannot be read.
fn context_of<C>(conn: &mut C) -> Option<Context>
where
    C: LoadConnection<DefaultLoadingMode> + Connection<Backend = Pg>,
{
    use diesel::RunQueryDsl;
    diesel::sql_query(CONTEXT_SQL)
        .load::<Context>(conn)
        .ok()?
        .into_iter()
        .next()
}

/// Whether a read may be served from `cache` after the writes through
/// it, with `window` the connection's read-your-writes window
/// ([`Cache::after_writes`]). Writes with no position yet get one here:
/// one small query on `conn`, for the first read after a write; when it
/// fails, the window decides.
fn after_writes<C>(cache: &Cache, window: Duration, conn: &mut C) -> AfterWrites
where
    C: LoadConnection<DefaultLoadingMode> + Connection<Backend = Pg>,
{
    match cache.after_writes(window) {
        AfterWrites::Resolve(writes) => match read_position(conn, WAL_POSITION_SQL) {
            Some(Read::At(position)) => {
                cache.resolve_writes(writes, position);
                match cache.after_writes(window) {
                    AfterWrites::Resolve(_) => AfterWrites::Postgres,
                    decided => decided,
                }
            }
            Some(Read::Null) => {
                cache.saw_standby();
                cache.by_window(window)
            }
            None => cache.by_window(window),
        },
        decided => decided,
    }
}

/// What `sql` ([`WAL_POSITION_SQL`], [`REPLAY_POSITION_SQL`]) reads on
/// `conn`; `None` when it cannot be read.
fn read_position<C>(conn: &mut C, sql: &str) -> Option<Read>
where
    C: LoadConnection<DefaultLoadingMode> + Connection<Backend = Pg>,
{
    use diesel::RunQueryDsl;
    diesel::sql_query(sql)
        .load::<WalPosition>(conn)
        .ok()?
        .first()?
        .read()
}

/// Whether `sql` ends a transaction with its writes: a `COMMIT` or an
/// `END`, as the ANSI manager and hand-written SQL send them.
pub(crate) fn commits(sql: &str) -> bool {
    let word = sql.split_whitespace().next().unwrap_or("");
    let word = word.trim_end_matches(';');
    word.eq_ignore_ascii_case("COMMIT") || word.eq_ignore_ascii_case("END")
}

/// The SQL a statement renders to, for [`may_write`]; a statement that
/// cannot be rendered is taken for a write.
pub(crate) fn rendered<T: QueryFragment<Pg>>(query: &T) -> String {
    let mut builder = diesel::pg::PgQueryBuilder::default();
    match query.to_sql(&mut builder, &Pg) {
        Ok(()) => diesel::query_builder::QueryBuilder::finish(builder),
        Err(_) => String::new(),
    }
}

/// Whether raw SQL could change what a read sees: any statement of it
/// but a plain `SELECT` (a pool's `SELECT 1` on every checkout would
/// otherwise keep the read-your-writes window open for good) and the
/// statements that only move a transaction along without committing it.
/// A batch is split on every `;`, one inside a string literal included:
/// a piece that then starts oddly counts as a write, which only sends a
/// few more reads to Postgres.
pub(crate) fn may_write(sql: &str) -> bool {
    sql.split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .any(|statement| !reads_only(statement))
}

/// Whether one statement is a plain read or only moves a transaction
/// along.
fn reads_only(statement: &str) -> bool {
    let head = statement.trim_start_matches('(');
    let word = head
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .next()
        .unwrap_or("");
    [
        "SELECT",
        "BEGIN",
        "START",
        "SAVEPOINT",
        "RELEASE",
        "ROLLBACK",
        "SET",
        "RESET",
        "DISCARD",
        "SHOW",
    ]
    .iter()
    .any(|statement| word.eq_ignore_ascii_case(statement))
}

/// Whether a transaction is open on the connection. Inside one, reads must
/// see the connection's own uncommitted writes, which no cache has. A
/// broken transaction state counts as open: the inner connection reports
/// it on the read.
fn in_transaction<C>(conn: &mut C) -> bool
where
    C: Connection<Backend = Pg, TransactionManager = AnsiTransactionManager>,
{
    AnsiTransactionManager::transaction_manager_status_mut(conn)
        .transaction_depth()
        .map(|depth| depth.is_some())
        .unwrap_or(true)
}

impl<C> R2D2Connection for SyncedConnection<C>
where
    C: R2D2Connection
        + LoadConnection<DefaultLoadingMode>
        + Connection<Backend = Pg, TransactionManager = AnsiTransactionManager>,
{
    /// Pings the inner connection.
    fn ping(&mut self) -> QueryResult<()> {
        self.inner.ping()
    }

    /// Whether the inner connection is broken.
    fn is_broken(&mut self) -> bool {
        self.inner.is_broken()
    }
}

/// Lets the wrapper resolve Postgres type OIDs the way the inner connection
/// does, which makes it a `PgMetadataLookup`. Diesel needs that whenever a
/// query binds a custom type, and applications that collect binds themselves
/// hand the connection in for the same reason.
impl<C> GetPgMetadataCache for SyncedConnection<C>
where
    C: GetPgMetadataCache,
{
    /// The inner connection's type metadata cache.
    fn get_metadata_cache(&mut self) -> &mut PgMetadataCache {
        self.inner.get_metadata_cache()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reads and the statements that only move a transaction along or
    /// change the session are not writes; anything else, or a batch with
    /// anything else in it, is.
    #[test]
    fn what_counts_as_a_write() {
        for sql in [
            "SELECT 1",
            "(SELECT 1) UNION (SELECT 2)",
            "BEGIN",
            "START TRANSACTION",
            "SAVEPOINT a",
            "RELEASE a",
            "ROLLBACK",
            "SET app.tenant = 'a'",
            "RESET ALL",
            "DISCARD ALL",
            "SHOW search_path",
            "SET ROLE r; SELECT 1;",
        ] {
            assert!(!may_write(sql), "{sql}");
        }
        for sql in [
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "COMMIT",
            "SELECT 1; DELETE FROM t",
            "WITH gone AS (DELETE FROM t RETURNING *) SELECT * FROM gone",
            "CALL p()",
        ] {
            assert!(may_write(sql), "{sql}");
        }
    }
}
