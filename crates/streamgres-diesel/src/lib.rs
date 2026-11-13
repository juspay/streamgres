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
//! calling [`install`] once at startup. The adapter checks which cached
//! tables the connection would see otherwise than the engine (another
//! table under its search path, one its role may not read), and checks
//! again after every statement the connection sends to the database (or
//! only after those that change its session: [`Recheck`]) and at least
//! every [`RECHECK_EVERY`]. Reads of those tables, and any read inside a
//! transaction, go to Postgres.
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

#[cfg(feature = "async")]
mod asynchronous;
mod reader_query;
mod row;
mod session;
#[cfg(feature = "async")]
pub use asynchronous::{AsyncLoadCached, wait_for_writes_async};

pub use session::{RECHECK_EVERY, Recheck, Tracker, install};

use std::any::TypeId;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::{Arc, OnceLock, RwLock};

use crate::diesel::connection::{Connection, LoadConnection};
use crate::diesel::deserialize::FromSqlRow;
use crate::diesel::pg::{Pg, PgMetadataLookup, PgQueryBuilder};
use crate::diesel::query_builder::bind_collector::RawBytesBindCollector;
use crate::diesel::query_builder::{AsQuery, Query, QueryBuilder, QueryFragment, QueryId};
use crate::diesel::query_dsl::CompatibleType;
use crate::diesel::query_dsl::RunQueryDsl;
use crate::diesel::query_dsl::methods::LoadQuery;
use crate::diesel::result::{Error, QueryResult};
use streamgres::cache::{Bind, CachedRows};
pub use streamgres::cache::{Cache, CacheConfig, Position, Reader, Refusal};
use streamgres::model::Value;
use streamgres::sync::pg::Lsn;
use streamgres::sync::pg::binary;

use crate::row::CachedRow;

/// The outcome of asking only the cache, including a refusal's reason.
#[derive(Debug)]
#[non_exhaustive]
pub enum CacheAttempt<T> {
    Hit(T),
    Refused(Refusal),
}

/// Loading a query's rows from the cache, falling back to Postgres.
pub trait LoadCached<'query, C, U>: Sized {
    /// The rows `.load(conn)` would return, from `cache` when it can
    /// answer exactly, from `conn` otherwise.
    fn load_cached(self, cache: &Cache, conn: &mut C) -> QueryResult<Vec<U>>;

    /// The rows from `cache` alone: `Ok(None)` when it refused.
    fn try_load_cached(self, cache: &Cache, conn: &mut C) -> QueryResult<Option<Vec<U>>>;

    /// The cache-only result, preserving the reason when it refuses.
    fn inspect_cached(self, cache: &Cache, conn: &mut C) -> QueryResult<CacheAttempt<Vec<U>>>;

    /// One row, or Diesel's `NotFound`; use `OptionalExtension::optional`
    /// on the result when absence is expected.
    fn get_result_cached(self, cache: &Cache, conn: &mut C) -> QueryResult<U> {
        self.load_cached(cache, conn)?
            .into_iter()
            .next()
            .ok_or(Error::NotFound)
    }

    /// One optional row, with the same fallback as `load_cached`.
    fn optional_cached(self, cache: &Cache, conn: &mut C) -> QueryResult<Option<U>> {
        match self.get_result_cached(cache, conn) {
            Ok(row) => Ok(Some(row)),
            Err(Error::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

/// Reading a query's first row.
pub trait FirstCached<'query, C, U>: Sized {
    /// The row `.first(conn)` would return: the query with `LIMIT 1`, read
    /// as [`LoadCached::load_cached`] reads it; Diesel's `NotFound` when
    /// there is none.
    fn first_cached(self, cache: &Cache, conn: &mut C) -> QueryResult<U>;
}
impl<'query, T, C, U> FirstCached<'query, C, U> for T
where
    T: crate::diesel::query_dsl::methods::LimitDsl,
    <T as crate::diesel::query_dsl::methods::LimitDsl>::Output: LoadCached<'query, C, U>,
{
    fn first_cached(self, cache: &Cache, conn: &mut C) -> QueryResult<U> {
        crate::diesel::query_dsl::methods::LimitDsl::limit(self, 1).get_result_cached(cache, conn)
    }
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
            Ok(rows) => Ok(rows),
            Err(_) => RunQueryDsl::load(query, conn),
        }
    }

    fn try_load_cached(self, cache: &Cache, conn: &mut C) -> QueryResult<Option<Vec<U>>> {
        let query = self.as_query();
        cached_rows::<_, U, C>(&query, cache, conn).map(Result::ok)
    }

    fn inspect_cached(self, cache: &Cache, conn: &mut C) -> QueryResult<CacheAttempt<Vec<U>>> {
        let query = self.as_query();
        cached_rows::<_, U, C>(&query, cache, conn).map(|answer| match answer {
            Ok(rows) => CacheAttempt::Hit(rows),
            Err(reason) => CacheAttempt::Refused(reason),
        })
    }
}

/// The reader `conn` reads as, checked again when its last check no
/// longer stands; why not, when the connection must read Postgres: it is
/// not tracked ([`Tracker`]), it is inside a transaction, or the check
/// cannot be made.
fn reader_of<C>(conn: &mut C, cache: &Cache) -> Result<Reader, Refusal>
where
    C: Connection<Backend = Pg> + LoadConnection + 'static,
{
    if in_transaction(conn) {
        return Err(Refusal::NotReady(
            "a read inside a transaction must use Postgres".to_owned(),
        ));
    }
    let Some(tracker) = session::tracker_of(conn.instrumentation()) else {
        return Err(Refusal::NotReady(
            "the connection has no outer Tracker instrumentation".to_owned(),
        ));
    };
    let Some(version) = cache.catalog_version() else {
        return Err(Refusal::NotReady(
            "the cache has no running catalog".to_owned(),
        ));
    };
    {
        let session = &tracker.session;
        if session.in_transaction {
            return Err(Refusal::NotReady(
                "a raw SQL transaction is open".to_owned(),
            ));
        }
        if let Some(reader) = session.current_reader()
            && reader.is_current(&version)
        {
            return Ok(reader.clone());
        }
    }
    let Ok(check) = cache.reader_check() else {
        return Err(Refusal::NotReady(
            "the cache cannot check this reader now".to_owned(),
        ));
    };
    let cacheable = !session::tracker_of(conn.instrumentation())
        .is_some_and(|tracker| tracker.session.uncached_reader_check);
    let mut answer: QueryResult<ReaderAnswer> =
        reader_query::ReaderQuery(&check, cacheable).get_result(conn);
    if answer.is_err() && cacheable {
        // DISCARD/DEALLOCATE can remove a prepared statement behind Diesel's
        // cache, including when called inside a function. Retry only this
        // read-only validation, then use unnamed checks for this connection.
        if let Some(tracker) = session::tracker_of(conn.instrumentation()) {
            tracker.session.uncached_reader_check = true;
        }
        answer = reader_query::ReaderQuery(&check, false).get_result(conn);
    }
    let Ok(answer) = answer else {
        return Err(Refusal::NotReady(
            "the reader identity or privileges could not be verified".to_owned(),
        ));
    };
    let reader = Reader::new(
        &answer.cluster,
        &answer.database,
        &answer.excluded,
        check.version,
    );
    let Some(tracker) = session::tracker_of(conn.instrumentation()) else {
        return Err(Refusal::NotReady(
            "the connection no longer has Tracker instrumentation".to_owned(),
        ));
    };
    let session = &mut tracker.session;
    session.reader = Some((reader.clone(), std::time::Instant::now()));
    session.changed = false;
    Ok(reader)
}

/// Waits, for at most `timeout`, until the cache has applied every
/// transaction `conn` can see so far (on a standby, as far as it has
/// replayed), so a read after a write sees it; whether it has. One round
/// trip reads where the connection's WAL is.
pub fn wait_for_writes<C>(
    cache: &Cache,
    conn: &mut C,
    timeout: std::time::Duration,
) -> QueryResult<bool>
where
    C: Connection<Backend = Pg> + LoadConnection + 'static,
{
    let now: WalNow =
        crate::diesel::sql_query(
            "SELECT (CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END)::text AS lsn",
        )
        .get_result(conn)?;
    let position = sync_position(&now.lsn)?;
    Ok(cache.wait_for(position, timeout))
}

fn sync_position(text: &str) -> QueryResult<Position> {
    Lsn::parse(text).map(Position::from).ok_or_else(|| {
        Error::DeserializationError("the database returned an invalid WAL location".into())
    })
}

/// The row of the reader check.
mod answer {
    #![allow(
        clippy::redundant_field_names,
        reason = "diesel_derives 2.2, which Juspay's fork builds with, writes `field: field`"
    )]

    use crate::diesel::sql_types::Text;

    #[derive(crate::diesel::QueryableByName)]
    pub(crate) struct WalNow {
        #[diesel(sql_type = Text)]
        pub lsn: String,
    }

    #[derive(crate::diesel::QueryableByName)]
    pub(crate) struct ReaderAnswer {
        #[diesel(sql_type = Text)]
        pub cluster: String,
        #[diesel(sql_type = Text)]
        pub database: String,
        #[diesel(sql_type = Text)]
        pub excluded: String,
    }
}
use answer::{ReaderAnswer, WalNow};

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

/// `query`'s rows from `cache`, retaining why it refused or why its values
/// could not be delivered to the application's decoder.
fn cached_rows<Q, U, C>(
    query: &Q,
    cache: &Cache,
    conn: &mut C,
) -> QueryResult<Result<Vec<U>, Refusal>>
where
    Q: QueryFragment<Pg> + QueryId + Query,
    Q::SqlType: CompatibleType<U, Pg>,
    U: FromSqlRow<<Q::SqlType as CompatibleType<U, Pg>>::SqlType, Pg>,
    C: Connection<Backend = Pg> + LoadConnection + PgMetadataLookup + 'static,
{
    // What needs no round trip is asked first: a read the cache refuses
    // whoever asks never pays for a check of its reader.
    if in_transaction(conn) {
        let reason = Refusal::NotReady("a read inside a transaction must use Postgres".to_owned());
        cache.report_refusal(&reason);
        return Ok(Err(reason));
    }
    let (sql, binds) = rendered(query, conn)?;
    if let Some(reason) = cache.refuses(&sql, &binds) {
        cache.report_refusal(&reason);
        return Ok(Err(reason));
    }
    let reader = match reader_of(conn, cache) {
        Ok(reader) => reader,
        Err(reason) => {
            cache.report_refusal(&reason);
            return Ok(Err(reason));
        }
    };
    let answer = match cache.read(&sql, &binds, &reader) {
        Ok(answer) => answer,
        Err(reason) => return Ok(Err(reason)),
    };
    let events = session::CacheEvents::new(conn.instrumentation(), &sql);
    let decoded = decode_cached::<Q, U>(&answer);
    events.finish(decoded.as_ref().err());
    match decoded {
        Ok(rows) => Ok(Ok(rows)),
        Err(_) => {
            cache.report_unusable();
            Ok(Err(Refusal::Unsupported(
                "the cached answer could not be encoded or decoded".to_owned(),
            )))
        }
    }
}

fn decode_cached<Q, U>(answer: &CachedRows) -> QueryResult<Vec<U>>
where
    Q: Query,
    Q::SqlType: CompatibleType<U, Pg>,
    U: FromSqlRow<<Q::SqlType as CompatibleType<U, Pg>>::SqlType, Pg>,
{
    let unusable = || {
        Error::DeserializationError(
            "the cached answer cannot be represented in PostgreSQL binary format".into(),
        )
    };
    let Some(Encoded { buffer, cells }) = encoded(answer) else {
        return Err(unusable());
    };
    let names: Vec<&str> = answer
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    let mut oids = Vec::with_capacity(answer.columns.len());
    for column in answer.columns.iter() {
        let Some(oid) = NonZeroU32::new(column.type_oid) else {
            return Err(unusable());
        };
        oids.push(oid);
    }
    let width = names.len();
    if width == 0 {
        return Err(unusable());
    }
    cells
        .chunks(width)
        .map(|cells| {
            let row = CachedRow::new(&names, &oids, &buffer, cells);
            U::build_from_row(&row).map_err(Error::DeserializationError)
        })
        .collect::<QueryResult<Vec<U>>>()
}

/// The SQL of each query type Diesel would cache a prepared statement for:
/// one text for every query of the type, so it is rendered once.
static RENDERED: OnceLock<RwLock<HashMap<TypeId, Arc<str>>>> = OnceLock::new();

/// The SQL `query` renders to and its binds, as Diesel sends them. The SQL
/// of a query type Diesel caches statements for is rendered once.
pub fn rendered<Q>(
    query: &Q,
    lookup: &mut (dyn PgMetadataLookup + 'static),
) -> QueryResult<(Arc<str>, Vec<Bind>)>
where
    Q: QueryFragment<Pg> + QueryId,
{
    let render = || -> QueryResult<Arc<str>> {
        let mut builder = PgQueryBuilder::default();
        query.to_sql(&mut builder, &Pg)?;
        Ok(builder.finish().into())
    };
    let sql = match Q::query_id() {
        Some(id) if query.is_safe_to_cache_prepared(&Pg)? => {
            let rendered = RENDERED.get_or_init(Default::default);
            let known = rendered
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&id)
                .cloned();
            match known {
                Some(sql) => sql,
                None => {
                    let sql = render()?;
                    rendered
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .insert(id, sql.clone());
                    sql
                }
            }
        }
        _ => render()?,
    };
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

/// Every value of an answer in Postgres's binary format for its column's
/// type, in one buffer: each cell, row after row, as its bytes' range in
/// `buffer`, `None` for `NULL`.
struct Encoded {
    buffer: Vec<u8>,
    cells: Vec<Option<std::ops::Range<usize>>>,
}

/// `answer` encoded; `None` when a value cannot be written exactly.
fn encoded(answer: &CachedRows) -> Option<Encoded> {
    let rows = answer.rows();
    let mut buffer = Vec::with_capacity(rows.len() * answer.columns.len() * 16);
    let mut cells = Vec::with_capacity(rows.len() * answer.columns.len());
    for row in rows {
        for (value, column) in row.iter().zip(answer.columns.iter()) {
            let at = buffer.len();
            let written = match value {
                Value::Null => {
                    cells.push(None);
                    continue;
                }
                Value::List(items) => {
                    let element = match column.element_oid {
                        0 => binary::array_element(column.type_oid)?,
                        known => known,
                    };
                    binary::encode_array_into(items, element, &mut buffer)
                }
                value => binary::encode_into(value, column.type_oid, &mut buffer),
            };
            if !written {
                return None;
            }
            cells.push(Some(at..buffer.len()));
        }
    }
    Some(Encoded { buffer, cells })
}
