//! Diesel queries answered by Streamgres from synchronous code: the same
//! `load_and_sync` as [`crate::LoadAndSync`], returning the rows instead of
//! a future, as `diesel::RunQueryDsl::load` does beside
//! `diesel_async::RunQueryDsl::load`. Import one of the two in a module.
//!
//! ```ignore
//! use streamgres_diesel::sync::LoadAndSync;
//!
//! let configs: Vec<MerchantConfig> = merchant_config::table
//!     .filter(merchant_config::merchant_id.eq(&merchant_id))
//!     .select(MerchantConfig::as_select())
//!     .load_and_sync(&lib)?;
//! ```
//!
//! A read answered from memory returns at once on the calling thread. Any
//! other read (the fallback's, a reader check) is made on Streamgres's own
//! runtime while the calling thread waits, as it waits on `libpq`. The
//! caller needs no runtime; start Streamgres with
//! [`streamgres::cache::Builder::start_blocking`].

use std::panic::Location;

use diesel::deserialize::FromSqlRow;
use diesel::pg::Pg;
use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
use diesel::query_dsl::CompatibleType;
use diesel::result::QueryResult;
use streamgres::cache::Sql;

use crate::{Inspect, Streamgres, Trace, Ttl, Types, decode, render, rendered, streamgres_error};

/// Reading a Diesel query through Streamgres, blocking.
pub trait LoadAndSync: AsQuery + Sized {
    /// The rows `.load(conn)` would return ([`crate::LoadAndSync`]), kept
    /// for the default ttl.
    #[track_caller]
    fn load_and_sync<U>(self, lib: &Streamgres) -> QueryResult<Vec<U>>
    where
        Self::Query: QueryFragment<Pg> + QueryId,
        Self::SqlType: CompatibleType<U, Pg>,
        U: FromSqlRow<<Self::SqlType as CompatibleType<U, Pg>>::SqlType, Pg>,
    {
        read(self, lib, None, Location::caller(), false).0
    }

    /// [`Self::load_and_sync`], the query kept this long after its last
    /// read (`.ttl(..)` of the async read).
    #[track_caller]
    fn load_and_sync_ttl<U>(self, lib: &Streamgres, ttl: impl Into<Ttl>) -> QueryResult<Vec<U>>
    where
        Self::Query: QueryFragment<Pg> + QueryId,
        Self::SqlType: CompatibleType<U, Pg>,
        U: FromSqlRow<<Self::SqlType as CompatibleType<U, Pg>>::SqlType, Pg>,
    {
        read(self, lib, Some(ttl.into()), Location::caller(), false).0
    }

    /// The read with its [`Trace`] (`.inspect()` of the async read): who
    /// answered it and why Streamgres did not if it did not. The trace is
    /// `None` only when the query could not be rendered.
    #[track_caller]
    fn load_and_sync_inspect<U>(
        self,
        lib: &Streamgres,
        ttl: Option<Ttl>,
    ) -> (QueryResult<Vec<U>>, Option<Trace>)
    where
        Self::Query: QueryFragment<Pg> + QueryId,
        Self::SqlType: CompatibleType<U, Pg>,
        U: FromSqlRow<<Self::SqlType as CompatibleType<U, Pg>>::SqlType, Pg>,
    {
        read(self, lib, ttl, Location::caller(), true)
    }

    /// Where this query stands in Streamgres now ([`Streamgres::inspect`]).
    /// It asks the database nothing and changes nothing.
    fn inspect(self, lib: &Streamgres) -> QueryResult<Inspect>
    where
        Self::Query: QueryFragment<Pg> + QueryId,
    {
        Ok(lib.inspect(&rendered(&self.as_query(), lib)?))
    }
}

impl<T: AsQuery> LoadAndSync for T {}

/// One read of `query`, and its trace when `traced`.
fn read<Q, U>(
    query: Q,
    lib: &Streamgres,
    ttl: Option<Ttl>,
    at: &'static Location<'static>,
    traced: bool,
) -> (QueryResult<Vec<U>>, Option<Trace>)
where
    Q: AsQuery,
    Q::Query: QueryFragment<Pg> + QueryId,
    Q::SqlType: CompatibleType<U, Pg>,
    U: FromSqlRow<<Q::SqlType as CompatibleType<U, Pg>>::SqlType, Pg>,
{
    let sql = match prepare(&query.as_query(), lib) {
        Ok(sql) => sql,
        Err(error) => return (Err(error), None),
    };
    let core = lib.load_and_sync(sql).called_at(at);
    let core = match ttl {
        Some(ttl) => core.ttl(ttl),
        None => core,
    };
    let (rows, trace) = if traced {
        let (rows, trace) = core.inspect().wait();
        (rows, Some(trace))
    } else {
        (core.wait(), None)
    };
    let rows = rows
        .map_err(streamgres_error)
        .and_then(|rows| decode::<<Q::SqlType as CompatibleType<U, Pg>>::SqlType, U>(&rows));
    (rows, trace)
}

/// The SQL `query` renders to and its binds; a custom type not known yet
/// has the database's types read again, once.
fn prepare<Q>(query: &Q, lib: &Streamgres) -> QueryResult<Sql>
where
    Q: QueryFragment<Pg> + QueryId,
{
    let mut types = Types {
        lib: lib.clone(),
        missing: false,
    };
    let first = render(query, &mut types);
    if !types.missing {
        return first;
    }
    lib.refresh_types_blocking().map_err(streamgres_error)?;
    rendered(query, lib)
}
