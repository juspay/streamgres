//! A Diesel connection that sits between an application and Postgres.
//!
//! `SyncedConnection<C>` wraps any Diesel Postgres connection `C` and
//! implements the same traits, so an application swaps its connection type
//! and nothing else changes: the query DSL, `sql_query`, transactions, pools
//! and `#[derive(Queryable)]` all go through unchanged.
//!
//! Every read Diesel performs reaches [`LoadConnection::load`]; every write
//! reaches [`Connection::execute_returning_count`]; raw multi-statement SQL
//! reaches [`SimpleConnection::batch_execute`]. Those three methods are the
//! whole interception surface, and the wrapper owns all of them.
//!
//! This first cut is a pure passthrough. It exists to prove that the wrapper
//! is transparent to Diesel and to the pools applications use (`r2d2`, and
//! through it `async-bb8-diesel`) before any read is served from a cache.
//! The cache, the engine client and the consistency rules land on top of
//! this type without changing its public surface.
//!
//! # Why the opt-in feature
//!
//! Diesel seals `Connection` behind the cargo feature
//! `i-implement-a-third-party-backend-and-opt-into-breaking-changes`.
//! Consumers that already wrap their connection (Hyperswitch through Deja)
//! enable it, so this crate does too rather than reaching into Diesel's
//! hidden derive re-exports.

use diesel::connection::{
    AnsiTransactionManager, ConnectionSealed, Instrumentation, LoadConnection, SimpleConnection,
};
use diesel::expression::QueryMetadata;
use diesel::pg::Pg;
use diesel::query_builder::{Query, QueryFragment, QueryId};
use diesel::r2d2::R2D2Connection;
use diesel::{Connection, ConnectionResult, QueryResult};

/// A Postgres connection whose reads can be served from a synced local copy.
///
/// `C` is the connection that talks to Postgres, normally
/// [`diesel::PgConnection`]. Everything the wrapper does not understand is
/// delegated to it untouched.
pub struct SyncedConnection<C = diesel::PgConnection> {
    inner: C,
}

impl<C> SyncedConnection<C> {
    /// Wraps an already established connection.
    pub fn new(inner: C) -> Self {
        SyncedConnection { inner }
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
        C::establish(database_url).map(SyncedConnection::new)
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
    C: LoadConnection<B> + Connection<Backend = Pg, TransactionManager = AnsiTransactionManager>,
{
    type Cursor<'conn, 'query>
        = C::Cursor<'conn, 'query>
    where
        Self: 'conn;
    type Row<'conn, 'query>
        = C::Row<'conn, 'query>
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
        self.inner.load(source)
    }
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
