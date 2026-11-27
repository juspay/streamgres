//! Diesel queries answered by an embedded [`Streamgres`].
//!
//! [`LoadAndSync::load_and_sync`] goes where `.load(&mut conn)` would on a
//! query an application wants kept in memory:
//!
//! ```ignore
//! use streamgres_diesel::LoadAndSync;
//!
//! let configs: Vec<MerchantConfig> = merchant_config::table
//!     .filter(merchant_config::merchant_id.eq(&merchant_id))
//!     .select(MerchantConfig::as_select())
//!     .load_and_sync(&lib)
//!     .ttl(Duration::from_secs(300))
//!     .await?;
//! ```
//!
//! The query is rendered and its binds collected exactly as Diesel would
//! send them, and handed to [`Streamgres::load_and_sync`]. The rows come
//! back in Postgres's binary format, whether from memory or from the
//! fallback's read of Postgres, and are read through the same `FromSql`
//! as a row from Postgres. It needs no connection of the application's:
//! the same call serves Diesel and `diesel-async` code alike.
//!
//! Synchronous Diesel code ends the same read with [`Load::wait`]
//! instead of `.await`: `query.load_and_sync(&lib).wait()`. The thread
//! sleeps while Postgres answers, as it would on `libpq`.
//!
//! What stays on Diesel: every write, every read inside a transaction
//! (the transaction's own writes and snapshot are on its connection, not
//! in Streamgres), and locking reads. A read here does not see a write
//! the application just made until the change feed brings it.
//!
//! Read errors keep their Diesel `QueryResult` shape. Streamgres maps
//! known SQLSTATE values to Diesel's corresponding database error kinds.
//! Other Streamgres failures, including timeouts and cancellations, are
//! boxed in `diesel::result::Error::QueryBuilderError` and can be
//! downcast to [`StreamgresError`].

pub use ::diesel;
pub use streamgres::cache::{
    Builder, Caller, EngineState, Error as StreamgresError, Fallback, Inspect, KeptQuery, Metrics,
    OnUnsupported, Op, QueryState, Served, SessionError, Streamgres, Trace, Ttl,
};

mod row;

use std::borrow::Cow;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::num::NonZeroU32;
use std::panic::Location;
use std::pin::Pin;

use diesel::deserialize::FromSqlRow;
use diesel::pg::{
    FailedToLookupTypeError, Pg, PgMetadataCacheKey, PgMetadataLookup, PgQueryBuilder,
    PgTypeMetadata,
};
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::query_builder::{AsQuery, QueryBuilder, QueryFragment, QueryId};
use diesel::query_dsl::CompatibleType;
use diesel::result::{DatabaseErrorKind, Error, QueryResult};
use streamgres::cache::{Rows, Sql, block_on};

use crate::row::CachedRow;

/// Reading a Diesel query through Streamgres.
pub trait LoadAndSync: AsQuery + Sized {
    /// The rows `.load(conn)` would return. When Streamgres keeps the
    /// query they come from memory; when it does not yet, it loads the
    /// query, answers from its rows and keeps them current from then on,
    /// until the query has gone unread for its ttl ([`Load::ttl`]). A
    /// query Streamgres cannot answer goes to its fallback.
    #[track_caller]
    fn load_and_sync<'a, U>(self, lib: &'a Streamgres) -> Load<'a, Self::SqlType, U>
    where
        Self::Query: QueryFragment<Pg> + QueryId + Send + 'a,
        Self::SqlType: CompatibleType<U, Pg>,
        U: FromSqlRow<<Self::SqlType as CompatibleType<U, Pg>>::SqlType, Pg>,
    {
        let query = self.as_query();
        Load {
            lib,
            sql: prepare(query, lib),
            ttl: None,
            at: Location::caller(),
            types: PhantomData,
        }
    }

    /// Where this query stands in Streamgres now ([`Streamgres::inspect`]):
    /// kept or not and why, its rows, its ttl, and who reads it. It asks
    /// the database nothing and changes nothing.
    fn inspect(self, lib: &Streamgres) -> QueryResult<Inspect>
    where
        Self::Query: QueryFragment<Pg> + QueryId,
    {
        Ok(lib.inspect(&rendered(&self.as_query(), lib)?))
    }
}

impl<T: AsQuery> LoadAndSync for T {}

/// A read of [`LoadAndSync::load_and_sync`]: await it for the rows, or
/// [`Load::wait`] for them in synchronous code.
#[must_use = "a read does nothing until it is awaited"]
pub struct Load<'a, ST, U> {
    lib: &'a Streamgres,
    sql: Pin<Box<dyn Future<Output = QueryResult<Sql>> + Send + 'a>>,
    ttl: Option<Ttl>,
    at: &'static Location<'static>,
    types: PhantomData<fn() -> (ST, U)>,
}

impl<'a, ST, U> Load<'a, ST, U> {
    /// Keeps the query this long after its last read, overriding the
    /// table's ttl and the default.
    pub fn ttl(mut self, ttl: impl Into<Ttl>) -> Self {
        self.ttl = Some(ttl.into());
        self
    }

    /// The read with its [`Trace`]: who answered it, why Streamgres did
    /// not if it did not, how long it took and where it was made. The
    /// trace comes back whether the read succeeded or not.
    pub fn inspect(self) -> LoadInspecting<'a, ST, U> {
        LoadInspecting(self)
    }

    /// The read for synchronous code: the rows, this thread blocked until
    /// they are here. A memory hit returns at once; any other read is
    /// made on Streamgres's runtime. The rows are decoded on this thread,
    /// so `U` need not be `Send`.
    ///
    /// For synchronous code only: in async code, a `LocalSet` included, use
    /// `.await`. Tokio panics when a read that must wait blocks inside a
    /// `LocalSet` on a multi-thread runtime.
    pub fn wait(self) -> QueryResult<Vec<U>>
    where
        ST: CompatibleType<U, Pg>,
        U: FromSqlRow<<ST as CompatibleType<U, Pg>>::SqlType, Pg>,
    {
        let Load {
            lib, sql, ttl, at, ..
        } = self;
        let rows = Self::core(lib, block_on(sql)?, ttl, at)
            .wait()
            .map_err(streamgres_error)?;
        decode::<<ST as CompatibleType<U, Pg>>::SqlType, U>(&rows)
    }

    /// The core read this one is.
    fn core(
        lib: &'a Streamgres,
        sql: Sql,
        ttl: Option<Ttl>,
        at: &'static Location<'static>,
    ) -> streamgres::cache::LoadAndSync<'a> {
        let read = lib.load_and_sync(sql).called_at(at);
        match ttl {
            Some(ttl) => read.ttl(ttl),
            None => read,
        }
    }
}

impl<'a, ST, U> IntoFuture for Load<'a, ST, U>
where
    ST: CompatibleType<U, Pg> + 'a,
    U: FromSqlRow<<ST as CompatibleType<U, Pg>>::SqlType, Pg> + Send + 'a,
{
    type Output = QueryResult<Vec<U>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let Load {
            lib, sql, ttl, at, ..
        } = self;
        Box::pin(async move {
            let rows = Self::core(lib, sql.await?, ttl, at)
                .await
                .map_err(streamgres_error)?;
            decode::<<ST as CompatibleType<U, Pg>>::SqlType, U>(&rows)
        })
    }
}

/// A read with its trace ([`Load::inspect`]): await it, or
/// [`LoadInspecting::wait`], for both.
#[must_use = "a read does nothing until it is awaited"]
pub struct LoadInspecting<'a, ST, U>(Load<'a, ST, U>);

impl<'a, ST, U> LoadInspecting<'a, ST, U>
where
    ST: CompatibleType<U, Pg>,
    U: FromSqlRow<<ST as CompatibleType<U, Pg>>::SqlType, Pg>,
{
    /// The read and its trace for synchronous code ([`Load::wait`]),
    /// decoded on this thread; likewise for synchronous code only.
    pub fn wait(self) -> (QueryResult<Vec<U>>, Option<Trace>) {
        let Load {
            lib, sql, ttl, at, ..
        } = self.0;
        let sql = match block_on(sql) {
            Ok(sql) => sql,
            Err(error) => return (Err(error), None),
        };
        let (rows, trace) = Load::<ST, U>::core(lib, sql, ttl, at).inspect().wait();
        let rows = rows
            .map_err(streamgres_error)
            .and_then(|rows| decode::<<ST as CompatibleType<U, Pg>>::SqlType, U>(&rows));
        (rows, Some(trace))
    }
}

impl<'a, ST, U> IntoFuture for LoadInspecting<'a, ST, U>
where
    ST: CompatibleType<U, Pg> + 'a,
    U: FromSqlRow<<ST as CompatibleType<U, Pg>>::SqlType, Pg> + Send + 'a,
{
    type Output = (QueryResult<Vec<U>>, Option<Trace>);
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        let Load {
            lib, sql, ttl, at, ..
        } = self.0;
        Box::pin(async move {
            let sql = match sql.await {
                Ok(sql) => sql,
                Err(error) => return (Err(error), None),
            };
            let (rows, trace) = Load::<ST, U>::core(lib, sql, ttl, at).inspect().await;
            let rows = rows
                .map_err(streamgres_error)
                .and_then(|rows| decode::<<ST as CompatibleType<U, Pg>>::SqlType, U>(&rows));
            (rows, Some(trace))
        })
    }
}

/// `rows` read as Diesel reads a result: each row through `U`'s
/// `FromSqlRow`.
fn decode<ST, U>(rows: &Rows) -> QueryResult<Vec<U>>
where
    U: FromSqlRow<ST, Pg>,
{
    let names: Vec<&str> = rows.columns().iter().map(|column| column.name()).collect();
    let oids = rows
        .columns()
        .iter()
        .map(|column| {
            // A column with no known type is `NULL` in every row: no value
            // of it is decoded, so any type stands in.
            Ok(NonZeroU32::new(column.type_oid()).unwrap_or(NonZeroU32::MIN))
        })
        .collect::<QueryResult<Vec<_>>>()?;
    let (buffer, _) = rows.raw();
    rows.iter()
        .map(|row| {
            let row = CachedRow::new(&names, &oids, buffer, row.cells());
            U::build_from_row(&row).map_err(Error::DeserializationError)
        })
        .collect()
}

/// The SQL `query` renders to and its binds, as Diesel would send them;
/// a custom type's OID named by what `lib` read of the database's types.
/// Render on await so an unresolved custom type can be refreshed without
/// blocking Diesel's synchronous metadata lookup or a Tokio worker.
fn prepare<'a, Q>(
    query: Q,
    lib: &'a Streamgres,
) -> Pin<Box<dyn Future<Output = QueryResult<Sql>> + Send + 'a>>
where
    Q: QueryFragment<Pg> + QueryId + Send + 'a,
{
    Box::pin(async move {
        let (first, missing) = {
            let mut types = Types {
                lib: lib.clone(),
                missing: false,
            };
            let first = render(&query, &mut types);
            (first, types.missing)
        };
        if !missing {
            return first;
        }
        lib.refresh_types().await.map_err(streamgres_error)?;
        rendered(&query, lib)
    })
}

fn rendered<Q>(query: &Q, lib: &Streamgres) -> QueryResult<Sql>
where
    Q: QueryFragment<Pg> + QueryId,
{
    render(
        query,
        &mut Types {
            lib: lib.clone(),
            missing: false,
        },
    )
}

fn render<Q>(query: &Q, types: &mut Types) -> QueryResult<Sql>
where
    Q: QueryFragment<Pg> + QueryId,
{
    let mut builder = PgQueryBuilder::default();
    query.to_sql(&mut builder, &Pg)?;
    let mut sql = Sql::new(builder.finish());
    let mut collector = RawBytesBindCollector::<Pg>::new();
    query.collect_binds(&mut collector, types, &Pg)?;
    for (metadata, bytes) in collector.metadata.iter().zip(collector.binds) {
        let type_oid = metadata
            .oid()
            .map_err(|error| Error::SerializationError(Box::new(error)))?;
        sql = sql.bind_raw(type_oid, bytes);
    }
    Ok(sql)
}

struct Types {
    lib: Streamgres,
    missing: bool,
}

impl PgMetadataLookup for Types {
    fn lookup_type(&mut self, type_name: &str, schema: Option<&str>) -> PgTypeMetadata {
        let found = self.lib.type_oids(type_name, schema);
        self.missing |= found.is_none();
        PgTypeMetadata::from_result(found.ok_or_else(|| {
            FailedToLookupTypeError::new(
                PgMetadataCacheKey::new(
                    schema.map(|schema| Cow::Owned(schema.to_owned())),
                    Cow::Owned(type_name.to_owned()),
                )
                .into_owned(),
            )
        }))
    }
}

/// A Streamgres error as Diesel reports a failed query.
fn streamgres_error(error: StreamgresError) -> Error {
    use DatabaseErrorKind as Kind;
    if let StreamgresError::Origin(SessionError::Database {
        code: Some(code),
        message,
    }) = &error
    {
        let kind = match code.as_str() {
            "23505" => Kind::UniqueViolation,
            "23503" => Kind::ForeignKeyViolation,
            "23502" => Kind::NotNullViolation,
            "23514" => Kind::CheckViolation,
            "23P01" => Kind::ExclusionViolation,
            "23001" => Kind::RestrictViolation,
            "40001" => Kind::SerializationFailure,
            "25006" => Kind::ReadOnlyTransaction,
            _ => return Error::QueryBuilderError(Box::new(error)),
        };
        return Error::DatabaseError(kind, Box::new(message.clone()));
    }
    Error::QueryBuilderError(Box::new(error))
}

#[cfg(test)]
mod error_tests {
    use super::*;

    #[test]
    fn diesel_keeps_database_kinds_and_streamgres_read_errors() {
        let unique = SessionError::database(Some("23505"), "duplicate key");
        assert!(matches!(
            streamgres_error(StreamgresError::Origin(unique)),
            Error::DatabaseError(DatabaseErrorKind::UniqueViolation, _)
        ));

        for error in [
            SessionError::Timeout("deadline reached".into()),
            SessionError::Cancelled("user request".into()),
            SessionError::database(Some("22012"), "division by zero"),
        ] {
            let expected = StreamgresError::Origin(error);
            let Error::QueryBuilderError(source) = streamgres_error(expected.clone()) else {
                panic!("expected the typed Streamgres error");
            };
            assert_eq!(source.downcast_ref::<StreamgresError>(), Some(&expected));
        }
    }
}
