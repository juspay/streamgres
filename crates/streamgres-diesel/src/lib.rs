//! Diesel reads answered from an embedded Streamgres [`Cache`].
//!
//! [`LoadCached::load_cached`] takes the place of `.load(&mut conn)` on a
//! query an application wants cached:
//!
//! ```ignore
//! use streamgres_diesel::LoadCached;
//!
//! let orders: Vec<Order> = orders::table
//!     .filter(orders::merchant_id.eq(&merchant_id))
//!     .select(Order::as_select())
//!     .load_cached(&cache, &mut conn)?;
//! ```
//!
//! A connection is answered from the cache only when it is tracked: give
//! every connection a [`Tracker`] as its instrumentation, most simply by
//! calling [`install`] once at startup. The tracker sees what the session
//! does, and the adapter checks once per connection, again after anything
//! that may change it and at least every ten seconds, which cached tables
//! the connection would see otherwise than the engine (another table
//! under its search path, one its role may not read); those, and any read
//! inside a transaction, go to Postgres.
//!
//! The query is rendered and its binds collected exactly as Diesel would
//! send them, and handed to the cache. When the cache answers, each value
//! is written in Postgres's binary format for its column's type and read
//! back through the same `FromSql` a row from Postgres goes through. When
//! it refuses (a query it cannot answer exactly, an engine not caught up),
//! the query runs on `conn` as `.load` would run it. A read inside a
//! transaction Diesel opened always runs on `conn`: the transaction's own
//! writes and snapshot are there, not in the cache. Writes and every other
//! read stay on Diesel as they are.

pub use ::diesel;

mod row;
mod session;

pub use session::{Tracker, install};

use std::num::NonZeroU32;

use crate::diesel::connection::{Connection, LoadConnection};
use crate::diesel::deserialize::FromSqlRow;
use crate::diesel::pg::{Pg, PgMetadataLookup, PgQueryBuilder};
use crate::diesel::query_builder::bind_collector::RawBytesBindCollector;
use crate::diesel::query_builder::{AsQuery, Query, QueryBuilder, QueryFragment, QueryId};
use crate::diesel::query_dsl::CompatibleType;
use crate::diesel::query_dsl::RunQueryDsl;
use crate::diesel::query_dsl::methods::LoadQuery;
use crate::diesel::result::{Error, QueryResult};
use crate::diesel::sql_types::{Array, Text};
use streamgres::cache::{Bind, CachedRows};
pub use streamgres::cache::{Cache, CacheConfig, Lsn, Reader, Refusal};
use streamgres::model::Value;
use streamgres::sync::pg::binary;

use crate::row::CachedRow;

/// Loading a query's rows from the cache, falling back to Postgres.
pub trait LoadCached<'query, C, U>: Sized {
    /// The rows `.load(conn)` would return, from `cache` when it can
    /// answer exactly, from `conn` otherwise.
    fn load_cached(self, cache: &Cache, conn: &mut C) -> QueryResult<Vec<U>>;

    /// The rows from `cache` alone: `Ok(None)` when it refused.
    fn try_load_cached(self, cache: &Cache, conn: &mut C) -> QueryResult<Option<Vec<U>>>;
}

impl<'query, T, C, U> LoadCached<'query, C, U> for T
where
    T: AsQuery,
    T::Query: QueryFragment<Pg> + QueryId + RunQueryDsl<C> + LoadQuery<'query, C, U> + 'query,
    T::SqlType: CompatibleType<U, Pg>,
    U: FromSqlRow<<T::SqlType as CompatibleType<U, Pg>>::SqlType, Pg> + 'static,
    C: Connection<Backend = Pg> + LoadConnection + PgMetadataLookup + 'static,
{
    fn load_cached(self, cache: &Cache, conn: &mut C) -> QueryResult<Vec<U>> {
        let query = self.as_query();
        match cached_rows::<_, U, C>(&query, cache, conn)? {
            Some(rows) => Ok(rows),
            None => RunQueryDsl::load(query, conn),
        }
    }

    fn try_load_cached(self, cache: &Cache, conn: &mut C) -> QueryResult<Option<Vec<U>>> {
        let query = self.as_query();
        cached_rows::<_, U, C>(&query, cache, conn)
    }
}

/// The reader `conn` reads as, checked again when its last check no
/// longer stands; `None` when the connection must read Postgres: it is
/// not tracked ([`Tracker`]), it is inside a transaction, or the check
/// cannot be made.
fn reader_of<C>(conn: &mut C, cache: &Cache) -> QueryResult<Option<Reader>>
where
    C: Connection<Backend = Pg> + LoadConnection + 'static,
{
    if in_transaction(conn) {
        return Ok(None);
    }
    let Some(session) = session::session_of(conn.instrumentation()) else {
        return Ok(None);
    };
    let Some(version) = cache.catalog_version() else {
        return Ok(None);
    };
    {
        let session = session::lock(&session);
        if session.in_transaction {
            return Ok(None);
        }
        if let Some(reader) = session.current_reader()
            && reader.is_current(version)
        {
            return Ok(Some(reader.clone()));
        }
    }
    let Ok(check) = cache.reader_check() else {
        return Ok(None);
    };
    let answer: ReaderAnswer = crate::diesel::sql_query(check.sql)
        .bind::<Array<Text>, _>(&check.names)
        .bind::<Array<Text>, _>(&check.as_written)
        .bind::<Array<Text>, _>(&check.qualified)
        .get_result(conn)?;
    let reader = Reader::new(&answer.database, &answer.excluded, check.version);
    let mut session = session::lock(&session);
    session.reader = Some((reader.clone(), std::time::Instant::now()));
    session.changed = false;
    Ok(Some(reader))
}

/// The row of the reader check.
mod answer {
    #![allow(
        clippy::redundant_field_names,
        reason = "diesel_derives 2.2, which Juspay's fork builds with, writes `field: field`"
    )]

    use crate::diesel::sql_types::Text;

    #[derive(crate::diesel::QueryableByName)]
    pub(crate) struct ReaderAnswer {
        #[diesel(sql_type = Text)]
        pub database: String,
        #[diesel(sql_type = Text)]
        pub excluded: String,
    }
}
use answer::ReaderAnswer;

/// Whether `conn` is inside a transaction Diesel opened, or in a state
/// Diesel cannot tell (a broken transaction), where only Postgres knows
/// what the connection would read.
fn in_transaction<C: Connection>(conn: &mut C) -> bool {
    use crate::diesel::connection::TransactionManager;
    !matches!(
        C::TransactionManager::transaction_manager_status_mut(conn).transaction_depth(),
        Ok(None)
    )
}

/// `query`'s rows from `cache`, `None` when it refused or a value cannot
/// be written for its column.
fn cached_rows<Q, U, C>(query: &Q, cache: &Cache, conn: &mut C) -> QueryResult<Option<Vec<U>>>
where
    Q: QueryFragment<Pg> + Query,
    Q::SqlType: CompatibleType<U, Pg>,
    U: FromSqlRow<<Q::SqlType as CompatibleType<U, Pg>>::SqlType, Pg>,
    C: Connection<Backend = Pg> + LoadConnection + PgMetadataLookup + 'static,
{
    let Some(reader) = reader_of(conn, cache)? else {
        return Ok(None);
    };
    let (sql, binds) = rendered(query, conn)?;
    let Ok(answer) = cache.read(&sql, &binds, &reader) else {
        return Ok(None);
    };
    let Some(cells) = encoded(&answer) else {
        return Ok(None);
    };
    let names: Vec<&str> = answer
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    let mut oids = Vec::with_capacity(answer.columns.len());
    for column in &answer.columns {
        let Some(oid) = NonZeroU32::new(column.type_oid) else {
            return Ok(None);
        };
        oids.push(oid);
    }
    cells
        .iter()
        .map(|cells| {
            let row = CachedRow::new(&names, &oids, cells);
            U::build_from_row(&row).map_err(Error::DeserializationError)
        })
        .collect::<QueryResult<Vec<U>>>()
        .map(Some)
}

/// The SQL `query` renders to and its binds, as Diesel sends them.
pub fn rendered<Q>(
    query: &Q,
    lookup: &mut (dyn PgMetadataLookup + 'static),
) -> QueryResult<(String, Vec<Bind>)>
where
    Q: QueryFragment<Pg>,
{
    let mut builder = PgQueryBuilder::default();
    query.to_sql(&mut builder, &Pg)?;
    let sql = builder.finish();
    let mut collector = RawBytesBindCollector::<Pg>::new();
    query.collect_binds(&mut collector, lookup, &Pg)?;
    let binds = collector
        .metadata
        .iter()
        .zip(collector.binds)
        .map(|(metadata, bytes)| {
            let type_oid = metadata
                .oid()
                .map_err(|error| Error::SerializationError(Box::new(error)))?;
            Ok(Bind { type_oid, bytes })
        })
        .collect::<QueryResult<Vec<Bind>>>()?;
    Ok((sql, binds))
}

/// Every value of `answer` in Postgres's binary format for its column's
/// type; `None` when one cannot be written exactly.
fn encoded(answer: &CachedRows) -> Option<Vec<Vec<Option<Vec<u8>>>>> {
    answer
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .zip(&answer.columns)
                .map(|(value, column)| match value {
                    Value::Null => Some(None),
                    Value::List(items) if column.element_oid != 0 => {
                        binary::encode_array(items, column.element_oid).map(Some)
                    }
                    Value::List(_) => None,
                    value => binary::encode(value, column.type_oid).map(Some),
                })
                .collect()
        })
        .collect()
}
