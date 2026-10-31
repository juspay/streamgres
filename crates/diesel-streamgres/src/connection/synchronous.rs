//! [`SyncedConnection`]: the wrapper over a blocking Diesel connection.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use diesel::connection::{
    AnsiTransactionManager, ConnectionSealed, DefaultLoadingMode, Instrumentation, LoadConnection,
    SimpleConnection, TransactionManager,
};
use diesel::expression::QueryMetadata;
use diesel::pg::{GetPgMetadataCache, Pg, PgMetadataCache};
use diesel::query_builder::{Query, QueryFragment, QueryId};
use diesel::r2d2::R2D2Connection;
use diesel::{Connection, ConnectionResult, QueryResult};

use crate::cache::{
    AfterWrites, CONTEXT_SQL, Cache, Context, REPLAY_POSITION_SQL, Read, WAL_POSITION_SQL,
    WalPosition,
};
use crate::session::{RawTransaction, Scope, SessionChanges};
use crate::cursor::{SyncedCursor, SyncedRow};
use crate::key::QueryKey;
use crate::Consistency;
use crate::route::{Route, Served};
use crate::row::{OwnedCursor, OwnedRow, capture_all};
use crate::subscriptions::Verdict;

use super::policy::{self, Bypass, Replayed, commits, may_write, rendered};
use super::{Attachment, SETTINGS, read_your_writes_from_env};

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
    /// Whether its raw SQL left a transaction block open.
    raw: RawTransaction,
    /// The context it reads in, and when that was read.
    scope: Scope,
    checked: Instant,
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
            raw: RawTransaction::default(),
            scope: Scope::Unchecked,
            checked: Instant::now(),
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
            raw: RawTransaction::default(),
            scope: Scope::Unchecked,
            checked: Instant::now(),
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
    /// ([`SessionChanges`]): once it is back as established, or may have
    /// changed in a way the statement does not say, what the connection
    /// reads as is read again before the cache serves it. A transaction
    /// block it opens counts as open from here ([`RawTransaction`]).
    fn observe(&mut self, sql: &str) {
        self.raw.before(sql);
        if self.cache.is_some() && policy::rechecks(self.session.apply(sql)) {
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
    /// The context this connection's reads share `cache` answers in
    /// ([`Cache::context_id`]); `None` when they must read Postgres: its
    /// session has changed since it was established, or its context is
    /// one the cache cannot serve. The context is read on the first read
    /// through the cache, again whenever a statement may have changed it
    /// unseen ([`Effect::Recheck`]) or the session is back as established,
    /// and at least every [`RECHECK_EVERY`]. When it cannot be read, this
    /// read goes to Postgres and the next asks again.
    fn context_in(&mut self, cache: &Cache) -> Option<u32> {
        if self.session.changed() {
            return None;
        }
        if policy::scope_now(self.scope, self.checked.elapsed()) == Scope::Unchecked {
            let context = context_of(&mut self.inner)?;
            self.scope = cache.context_id(&context).map_or(Scope::Own, Scope::In);
            self.checked = Instant::now();
        }
        match self.scope {
            Scope::In(id) => Some(id),
            Scope::Own | Scope::Unchecked => None,
        }
    }

    /// Whether a transaction is open on the connection, whether Diesel's
    /// transaction manager or raw SQL ([`RawTransaction`]) opened it.
    fn transaction_open(&mut self) -> bool {
        self.raw.is_open() || in_transaction(&mut self.inner)
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
        if !cache.reads_standbys() || (!committed && self.transaction_open()) {
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
        let replayed = Replayed::of(read_position(&mut self.inner, REPLAY_POSITION_SQL), since);
        match replayed {
            Replayed::Primary => self.primary = true,
            Replayed::Standby(_) => cache.saw_standby(),
            Replayed::Unknown => {}
        }
        replayed.keep()
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
        if done.is_ok() {
            self.raw.after(query);
        }
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

    /// Connects, attached to the cache [`install`](crate::install) set up for this URL,
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
        if done.is_ok() {
            self.raw.after(&sql);
        }
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

    /// Sizes the inner connection's statement cache.
    #[cfg(feature = "diesel-2-3")]
    fn set_prepared_statement_cache_size(&mut self, size: diesel::connection::CacheSize) {
        self.inner.set_prepared_statement_cache_size(size)
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
    /// read is keyed in the context the connection reads in ([`Context`],
    /// read on its first read through the cache and whenever it may have
    /// changed), and shares answers only with readers in the same one
    /// ([`Cache::context_id`]); while its session differs from how it was
    /// established (the statements it has run), or its context cannot be served,
    /// it reads Postgres. While the engine side is too far behind the
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
        let named = key.take_route();
        let route = policy::route(named, self.scoped, self.route);
        if !key.is_select() {
            self.wrote();
            if cache.reads_standbys() && !self.transaction_open() {
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
        if route == Route::Postgres || self.transaction_open() {
            return LoadConnection::<B>::load(&mut self.inner, source).map(SyncedCursor::Live);
        }
        // From here on the read is one the cache could serve: each way out
        // reports where it was served.
        if named.is_none() && !crate::rollout::through_cache() {
            return LoadConnection::<B>::load(&mut self.inner, source).map(SyncedCursor::Live);
        }
        if key.is_locking() {
            return bypassed::<C, B, T>(&mut self.inner, &cache, Bypass::Locking, source);
        }
        if after_writes(&cache, self.read_your_writes, &mut self.inner) != AfterWrites::Serve {
            return bypassed::<C, B, T>(&mut self.inner, &cache, Bypass::AfterWrites, source);
        }
        if cache.subscriptions().behind() {
            return bypassed::<C, B, T>(&mut self.inner, &cache, Bypass::Lagging, source);
        }
        let Some(context) = self.context_in(&cache) else {
            return bypassed::<C, B, T>(&mut self.inner, &cache, Bypass::OtherContext, source);
        };
        key.context = context;
        if let Some(rows) = cache.get(&key) {
            self.served = Some(Served::Cache);
            crate::metrics::report(|metrics| metrics.served(Served::Cache));
            if crate::shadow::sampled() {
                shadow_read(&mut self.inner, &cache, &key, &rows, source);
            }
            return Ok(SyncedCursor::Cached(OwnedCursor::new(rows)));
        }
        if cache.refused_recently(&key).is_some() {
            return bypassed::<C, B, T>(&mut self.inner, &cache, Bypass::Refused, source);
        }
        let reservation = cache.reserve(&key);
        let since = match cache.subscriptions().subscribe_with(&key, &mut self.inner) {
            Verdict::Refused(reason) => {
                cache.refused(&key, &reason);
                return bypassed::<C, B, T>(&mut self.inner, &cache, Bypass::Refused, source);
            }
            Verdict::Rows { rows, live } => {
                cache.fill_live(reservation, live, &rows);
                if cache.subscriptions().behind() {
                    return bypassed::<C, B, T>(&mut self.inner, &cache, Bypass::Lagging, source);
                }
                self.served = Some(Served::Engine);
                crate::metrics::report(|metrics| metrics.served(Served::Engine));
                if crate::shadow::sampled() {
                    shadow_read(&mut self.inner, &cache, &key, &rows, source);
                }
                return Ok(SyncedCursor::Cached(OwnedCursor::new(rows)));
            }
            Verdict::Accepted { since } => since,
        };
        let keep = self.replayed_past(&cache, since);
        crate::metrics::report(|metrics| metrics.served(Served::Postgres));
        let _permit = match cache.fallback_permit() {
            Ok(permit) => permit,
            Err(error) => {
                cache.abandon(&key);
                return Err(error);
            }
        };
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

/// Reads `source` from Postgres for `bypass`: counted and reported
/// ([`Bypass::record`]) and, unless it needs none, behind a fallback
/// permit held for as long as the cursor lives.
fn bypassed<'conn, 'query, C, B, T>(
    inner: &'conn mut C,
    cache: &Cache,
    bypass: Bypass,
    source: T,
) -> QueryResult<SyncedCursor<<C as LoadConnection<B>>::Cursor<'conn, 'query>>>
where
    C: LoadConnection<B> + Connection<Backend = Pg> + 'conn,
    T: Query + QueryFragment<Pg> + QueryId + 'query,
    Pg: QueryMetadata<T::SqlType>,
{
    bypass.record(cache);
    if !bypass.limited() {
        return LoadConnection::<B>::load(inner, source).map(SyncedCursor::Live);
    }
    let permit = cache.fallback_permit()?;
    LoadConnection::<B>::load(inner, source).map(|cursor| SyncedCursor::Limited(cursor, permit))
}

/// A shadow read ([`crate::shadow`]): `source` run on Postgres beside the
/// `cached` answer just served, the two compared, and a difference judged
/// once the engine has passed the position Postgres had reached.
fn shadow_read<C, T>(conn: &mut C, cache: &Cache, key: &QueryKey, cached: &[OwnedRow], source: T)
where
    C: LoadConnection<DefaultLoadingMode> + Connection<Backend = Pg>,
    T: Query + QueryFragment<Pg> + QueryId,
    Pg: QueryMetadata<T::SqlType>,
{
    use crate::shadow::{Outcome, SETTLE, conclude, same};
    let live = LoadConnection::<DefaultLoadingMode>::load(conn, source).and_then(capture_all);
    let reached = read_reached(conn);
    let Ok(live) = live else {
        conclude(Outcome::Unverified, &key.sql, cached, &[]);
        return;
    };
    if same(cached, &live) {
        conclude(Outcome::Agreed, &key.sql, cached, &live);
        return;
    }
    let Some(reached) = reached else {
        conclude(Outcome::Unverified, &key.sql, cached, &live);
        return;
    };
    let deadline = Instant::now() + SETTLE;
    loop {
        match cache.subscriptions().position() {
            Some(position) if position >= reached => {
                let outcome = match cache.get(key) {
                    Some(now) if same(&now, &live) => Outcome::Lagged,
                    Some(now) => {
                        conclude(Outcome::Mismatched, &key.sql, &now, &live);
                        return;
                    }
                    None => Outcome::Unverified,
                };
                conclude(outcome, &key.sql, cached, &live);
                return;
            }
            Some(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(2)),
            _ => {
                conclude(Outcome::Unverified, &key.sql, cached, &live);
                return;
            }
        }
    }
}

/// The WAL position the database `conn` reads has reached: inserted on a
/// primary, replayed on a standby; `None` when it cannot say.
fn read_reached<C>(conn: &mut C) -> Option<u64>
where
    C: LoadConnection<DefaultLoadingMode> + Connection<Backend = Pg>,
{
    match read_position(conn, WAL_POSITION_SQL)? {
        Read::At(position) => Some(position),
        Read::Null => match read_position(conn, REPLAY_POSITION_SQL)? {
            Read::At(position) => Some(position),
            Read::Null => None,
        },
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
