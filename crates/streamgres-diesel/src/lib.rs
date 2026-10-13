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
//! - the connection is [`Consistency::Eventual`];
//! - no transaction is open on it (inside one, the application must see
//!   its own writes);
//! - the statement is a `SELECT` (Diesel sends `INSERT ... RETURNING`
//!   through the same path);
//! - the engine accepted a subscription for the query
//!   ([`subscriptions::Subscriptions`]).
//!
//! On a miss the query is subscribed first, then read from Postgres, and
//! the rows are kept byte for byte ([`row::OwnedRow`]). When the engine
//! reports the query touched, the rows are dropped and the next read
//! refills them. Every other read goes to Postgres as before.
//!
//! # Why the opt-in feature
//!
//! Diesel seals `Connection` behind the cargo feature
//! `i-implement-a-third-party-backend-and-opt-into-breaking-changes`.
//! Consumers that already wrap their connection (Hyperswitch through Deja)
//! enable it, so this crate does too rather than reaching into Diesel's
//! hidden derive re-exports.

pub mod cache;
pub mod cursor;
pub mod engine;
pub mod key;
pub mod row;
pub mod subscriptions;

use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};

use diesel::connection::{
    AnsiTransactionManager, ConnectionSealed, DefaultLoadingMode, Instrumentation,
    LoadConnection, SimpleConnection, TransactionManager,
};
use diesel::expression::QueryMetadata;
use diesel::pg::{GetPgMetadataCache, Pg, PgMetadataCache};
use diesel::query_builder::{Query, QueryFragment, QueryId};
use diesel::r2d2::R2D2Connection;
use diesel::{Connection, ConnectionResult, QueryResult};

use crate::cache::Cache;
use crate::cursor::{SyncedCursor, SyncedRow};
use crate::key::QueryKey;
use crate::row::{OwnedCursor, OwnedRow, capture_all};
use crate::subscriptions::Verdict;

/// How fresh a read on a connection must be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Consistency {
    /// Reads may come from the cache, which lags a commit by the engine's
    /// propagation delay.
    #[default]
    Eventual,
    /// Every read goes to Postgres.
    Strong,
}

/// What a process shares between its connections: the cache and how to
/// treat a connection by the URL it was established with.
pub struct Settings {
    pub cache: Arc<Cache>,
    /// The consistency for a connection established with this URL, so a
    /// master pool can be strong and a replica pool eventual.
    pub consistency_for: Box<dyn Fn(&str) -> Consistency + Send + Sync>,
}

static SETTINGS: OnceLock<Settings> = OnceLock::new();

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
    consistency: Consistency,
}

impl<C> SyncedConnection<C> {
    /// Wraps an already established connection as a passthrough.
    pub fn new(inner: C) -> Self {
        SyncedConnection {
            inner,
            cache: None,
            consistency: Consistency::Strong,
        }
    }

    /// Wraps a connection with a cache and a consistency.
    pub fn with_cache(inner: C, cache: Arc<Cache>, consistency: Consistency) -> Self {
        SyncedConnection {
            inner,
            cache: Some(cache),
            consistency,
        }
    }

    /// Changes how fresh this connection's reads must be.
    pub fn set_consistency(&mut self, consistency: Consistency) {
        self.consistency = consistency;
    }

    pub fn consistency(&self) -> Consistency {
        self.consistency
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

impl<C> SimpleConnection for SyncedConnection<C>
where
    C: SimpleConnection,
{
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        self.inner.batch_execute(query)
    }
}

impl<C> ConnectionSealed for SyncedConnection<C> {}

impl<C> Connection for SyncedConnection<C>
where
    C: Connection<Backend = Pg, TransactionManager = AnsiTransactionManager>,
{
    type Backend = Pg;
    // The ANSI manager is generic over the connection it drives: it issues
    // `BEGIN`/`COMMIT`/`ROLLBACK` through `batch_execute` and keeps its depth
    // in whatever `transaction_state` returns, so the wrapper reuses it and
    // points it at the inner connection's state. One transaction state, not
    // two that could disagree.
    type TransactionManager = AnsiTransactionManager;

    fn establish(database_url: &str) -> ConnectionResult<Self> {
        let inner = C::establish(database_url)?;
        Ok(match SETTINGS.get() {
            Some(settings) => SyncedConnection::with_cache(
                inner,
                Arc::clone(&settings.cache),
                (settings.consistency_for)(database_url),
            ),
            None => SyncedConnection::new(inner),
        })
    }

    fn begin_test_transaction(&mut self) -> QueryResult<()> {
        self.inner.begin_test_transaction()
    }

    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Pg> + QueryId,
    {
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

    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> QueryResult<Self::Cursor<'conn, 'query>>
    where
        T: Query + QueryFragment<Pg> + QueryId + 'query,
        Pg: QueryMetadata<T::SqlType>,
    {
        let cache = match self.cache.as_ref() {
            Some(cache)
                if self.consistency == Consistency::Eventual
                    && !in_transaction(&mut self.inner) =>
            {
                Arc::clone(cache)
            }
            _ => return LoadConnection::<B>::load(&mut self.inner, source).map(SyncedCursor::Live),
        };

        let key = QueryKey::of(&source, &mut self.inner)?;
        if !key.is_select() {
            return LoadConnection::<B>::load(&mut self.inner, source).map(SyncedCursor::Live);
        }
        if let Some(entry) = cache.get(&key) {
            return Ok(SyncedCursor::Cached(OwnedCursor::new(Arc::clone(
                &entry.rows,
            ))));
        }
        if let Verdict::Refused(_) = cache.subscriptions().subscribe(&key) {
            cache.stats.refused.fetch_add(1, Ordering::Relaxed);
            return LoadConnection::<B>::load(&mut self.inner, source).map(SyncedCursor::Live);
        }

        // Subscribed: the engine reports every change from here on, so
        // what Postgres returns now is current until the first touch.
        let generation = cache.reserve(&key);
        let live = LoadConnection::<DefaultLoadingMode>::load(&mut self.inner, source)?;
        let rows: Arc<[OwnedRow]> = Arc::from(capture_all(live)?);
        cache.fill(key, generation, Arc::clone(&rows));
        Ok(SyncedCursor::Cached(OwnedCursor::new(rows)))
    }
}

/// Whether a transaction is open on the connection. Inside one, reads must
/// see the connection's own uncommitted writes, which no cache has.
fn in_transaction<C>(conn: &mut C) -> bool
where
    C: Connection<Backend = Pg, TransactionManager = AnsiTransactionManager>,
{
    AnsiTransactionManager::transaction_manager_status_mut(conn)
        .transaction_depth()
        .map(|depth| depth.is_some())
        // A broken transaction state counts as open: the inner connection
        // reports it on the read.
        .unwrap_or(true)
}

impl<C> R2D2Connection for SyncedConnection<C>
where
    C: R2D2Connection + Connection<Backend = Pg, TransactionManager = AnsiTransactionManager>,
{
    fn ping(&mut self) -> QueryResult<()> {
        self.inner.ping()
    }

    fn is_broken(&mut self) -> bool {
        self.inner.is_broken()
    }
}

/// Lets the wrapper resolve Postgres type OIDs the way the inner connection
/// does, which makes it a `PgMetadataLookup`. Diesel needs that whenever a
/// query binds a custom type, and applications that collect binds themselves
/// (Hyperswitch's KV path does) hand the connection in for the same reason.
impl<C> GetPgMetadataCache for SyncedConnection<C>
where
    C: GetPgMetadataCache,
{
    fn get_metadata_cache(&mut self) -> &mut PgMetadataCache {
        self.inner.get_metadata_cache()
    }
}
