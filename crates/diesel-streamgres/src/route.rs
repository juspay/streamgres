//! Where a read goes: through the cache the engine keeps fresh, or
//! straight to Postgres.
//!
//! The route is decided at three levels, the most specific winning:
//!
//! 1. **The query.** [`RouteDsl::via`] wraps a Diesel query in a [`Via`]
//!    marker that renders a leading SQL comment, `/*streamgres:postgres*/`
//!    or `/*streamgres:streamgres*/`. The wrapper reads the comment at its
//!    interception point, strips it, and routes on it; Postgres ignores
//!    comments, so a flagged query sent live is the same query.
//!    [`crate::SyncedConnection::with_route`] is the same flag for every
//!    read inside a closure, for code that holds the connection anyway.
//! 2. **The connection pool.** Every connection carries a route, chosen
//!    by the database URL it was established with (see
//!    [`crate::Settings`]), so a master pool can go to Postgres and a
//!    replica pool through Streamgres.
//! 3. **The process default**, [`Route::default`], which is Streamgres.
//!
//! Three things go to Postgres whatever the flags say: a statement that
//! is not a `SELECT`, a read inside an open transaction, and a query the
//! engine refuses.

use diesel::QueryResult;
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::query_dsl::RunQueryDsl;
use diesel::query_dsl::methods::{LimitDsl, OffsetDsl};

/// Where a connection's last read was served from, as
/// [`crate::SyncedConnection::last_served`] reports it. Per connection,
/// so it is exact under concurrency where the cache's shared counters
/// are not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Served {
    /// The cache had the rows: no round trip at all.
    Cache,
    /// The engine shipped the rows when the query was subscribed: no
    /// Postgres read.
    Engine,
    /// Postgres ran the query (a miss the engine only accepted, a
    /// refusal, a write window, a transaction, or the Postgres route).
    Postgres,
}

/// Where a read goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Route {
    /// Through the cache: the result may lag a commit by the engine's
    /// propagation delay.
    #[default]
    Streamgres,
    /// Straight to Postgres, every time.
    Postgres,
}

impl Route {
    /// Another name for [`Route::Streamgres`], under [`crate::Consistency`].
    #[allow(non_upper_case_globals)]
    pub const Eventual: Route = Route::Streamgres;
    /// Another name for [`Route::Postgres`], under [`crate::Consistency`].
    #[allow(non_upper_case_globals)]
    pub const Strong: Route = Route::Postgres;

    const MARKER_STREAMGRES: &'static str = "/*streamgres:streamgres*/ ";
    const MARKER_POSTGRES: &'static str = "/*streamgres:postgres*/ ";

    /// The comment a flagged query starts with.
    pub(crate) fn marker(self) -> &'static str {
        match self {
            Route::Streamgres => Self::MARKER_STREAMGRES,
            Route::Postgres => Self::MARKER_POSTGRES,
        }
    }

    /// Splits a leading marker off rendered SQL.
    pub(crate) fn strip(sql: &str) -> Option<(Route, &str)> {
        if let Some(rest) = sql.strip_prefix(Self::MARKER_POSTGRES) {
            return Some((Route::Postgres, rest));
        }
        if let Some(rest) = sql.strip_prefix(Self::MARKER_STREAMGRES) {
            return Some((Route::Streamgres, rest));
        }
        None
    }
}

/// A query flagged with the route it takes. Built by [`RouteDsl::via`];
/// runs like the query it wraps.
#[derive(Debug, Clone, Copy)]
pub struct Via<Q> {
    query: Q,
    route: Route,
}

impl<Q> Via<Q> {
    pub fn route(&self) -> Route {
        self.route
    }

    pub fn into_inner(self) -> Q {
        self.query
    }
}

/// Flags a query with its route: `query.via(Route::Postgres).first(conn)`.
pub trait RouteDsl: Sized {
    fn via(self, route: Route) -> Via<Self> {
        Via { query: self, route }
    }

    /// This read goes straight to Postgres.
    fn via_postgres(self) -> Via<Self> {
        self.via(Route::Postgres)
    }

    /// This read may come from the cache, whatever its connection says.
    fn via_streamgres(self) -> Via<Self> {
        self.via(Route::Streamgres)
    }
}

impl<T: Query> RouteDsl for T {}

impl<Q: QueryFragment<Pg>> QueryFragment<Pg> for Via<Q> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_sql(self.route.marker());
        self.query.walk_ast(out.reborrow())
    }
}

impl<Q: QueryId> QueryId for Via<Q> {
    type QueryId = Via<Q::QueryId>;
    // The same query flagged both ways renders two SQL texts, so the
    // prepared statement is keyed by the text, not the type.
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<Q: Query> Query for Via<Q> {
    type SqlType = Q::SqlType;
}

impl<Q, Conn> RunQueryDsl<Conn> for Via<Q> {}

impl<Q: LimitDsl> LimitDsl for Via<Q> {
    type Output = Via<Q::Output>;

    fn limit(self, limit: i64) -> Self::Output {
        Via {
            query: self.query.limit(limit),
            route: self.route,
        }
    }
}

impl<Q: OffsetDsl> OffsetDsl for Via<Q> {
    type Output = Via<Q::Output>;

    fn offset(self, offset: i64) -> Self::Output {
        Via {
            query: self.query.offset(offset),
            route: self.route,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markers_round_trip() {
        for route in [Route::Streamgres, Route::Postgres] {
            let sql = format!("{}SELECT 1", route.marker());
            assert_eq!(Route::strip(&sql), Some((route, "SELECT 1")));
        }
        assert_eq!(Route::strip("SELECT 1"), None);
        assert_eq!(Route::strip("/* a comment */ SELECT 1"), None);
    }

    #[test]
    fn old_names_still_mean_the_same() {
        assert_eq!(Route::Eventual, Route::Streamgres);
        assert_eq!(Route::Strong, Route::Postgres);
        assert_eq!(Route::default(), Route::Streamgres);
    }
}
