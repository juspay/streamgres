//! The application's `diesel-async` bb8 pool as Streamgres's connections,
//! through diesel-async's public API alone.
//!
//! A statement runs through `RunQueryDsl::load` as a query of Diesel's
//! own, carrying the original SQL and collected bind bytes and OIDs.
//! Each row is read through Diesel's `Row`
//! as its cells' bytes and type OIDs. Statements that return nothing run
//! through `batch_execute`.

use std::cell::Cell;
use std::io::Write as _;

use diesel::deserialize::{self, FromSqlRow};
use diesel::pg::{Pg, PgTypeMetadata};
use diesel::query_builder::QueryId;
use diesel::row::{Field, Row};
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types::{HasSqlType, SingleValue, SqlType, TypeMetadata, Untyped};
use diesel_async::pooled_connection::bb8::{Pool, PooledConnection};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use streamgres::cache::{
    Bind, Connection, ConnectionFuture, Connections, Lease, RawColumn, RawRows, Session,
    SessionError,
};

struct ApplicationPool {
    pool: Pool<AsyncPgConnection>,
}
struct Checkout(PooledConnection<'static, AsyncPgConnection>);

/// Streamgres's connections from the application's pool: the same pool
/// for Diesel writes, direct reads and Streamgres. The pool must target the
/// replicated database with one role and session policy.
pub fn connections(pool: Pool<AsyncPgConnection>) -> impl Connections {
    ApplicationPool { pool }
}

type Manager = <AsyncPgConnection as AsyncConnection>::TransactionManager;

impl Connections for ApplicationPool {
    fn get(&self) -> ConnectionFuture<'_, Result<Lease, String>> {
        Box::pin(async move {
            let mut connection = self
                .pool
                .get_owned()
                .await
                .map_err(|error| error.to_string())?;
            let status =
                <Manager as diesel_async::TransactionManager<_>>::transaction_manager_status_mut(
                    &mut *connection,
                );
            if !matches!(status.transaction_depth(), Ok(None)) {
                *status = diesel::connection::TransactionManagerStatus::InError;
                return Err(
                    "an application checkout already has an open or broken transaction".to_owned(),
                );
            }
            Ok(Lease::new(Checkout(connection)))
        })
    }
}

impl Session for Checkout {
    fn query<'a>(
        &'a mut self,
        sql: &'a str,
        binds: &'a [Bind],
    ) -> ConnectionFuture<'a, Result<RawRows, SessionError>> {
        Box::pin(async move {
            let mut query = diesel::sql_query(sql).into_boxed::<Pg>();
            for bind in binds {
                query = query.bind::<RuntimeType, _>(RuntimeValue {
                    type_oid: bind.type_oid,
                    bytes: bind.bytes.as_deref(),
                });
            }
            let rows: Vec<RawRow> = query.load(&mut *self.0).await.map_err(session_error)?;
            Ok(raw_rows(rows))
        })
    }

    fn execute<'a>(&'a mut self, sql: &'a str) -> ConnectionFuture<'a, Result<(), SessionError>> {
        Box::pin(async move { self.0.batch_execute(sql).await.map_err(session_error) })
    }
}

/// A Diesel error as a [`SessionError`]. Diesel keeps an error's kind but
/// not its SQLSTATE, so `code` here is inferred from that kind (the code
/// Postgres uses for it, not one it returned), and a cancelled statement is
/// told by its message: best effort, for an untranslated server.
fn session_error(error: diesel::result::Error) -> SessionError {
    use diesel::result::{DatabaseErrorKind as Kind, Error};
    let Error::DatabaseError(kind, info) = &error else {
        return SessionError::Other(error.to_string());
    };
    let message = info.message();
    if message.starts_with("canceling statement due to") {
        return SessionError::database(Some(SessionError::QUERY_CANCELED), message);
    }
    let code = match kind {
        Kind::UniqueViolation => Some("23505"),
        Kind::ForeignKeyViolation => Some("23503"),
        Kind::NotNullViolation => Some("23502"),
        Kind::CheckViolation => Some("23514"),
        Kind::ExclusionViolation => Some("23P01"),
        Kind::RestrictViolation => Some("23001"),
        Kind::SerializationFailure => Some("40001"),
        Kind::ReadOnlyTransaction => Some("25006"),
        Kind::ClosedConnection | Kind::UnableToSendCommand => {
            return SessionError::Other(message.to_owned());
        }
        _ => None,
    };
    SessionError::database(code, message)
}

impl Connection for Checkout {
    fn discard(mut self: Box<Self>) {
        // A connection in error is closed by bb8's broken check instead of
        // being handed, perhaps mid-transaction, to an application request.
        *<Manager as diesel_async::TransactionManager<_>>::transaction_manager_status_mut(
            &mut *self.0,
        ) = diesel::connection::TransactionManagerStatus::InError;
    }
}

/// The SQL type of a bind whose type Streamgres names at run time.
#[derive(Debug, Clone, Copy, Default, QueryId)]
struct RuntimeType;

impl SqlType for RuntimeType {
    type IsNull = diesel::sql_types::is_nullable::NotNull;
}

impl SingleValue for RuntimeType {}

thread_local! {
    /// The type of the bind being collected. Diesel collects a bind by
    /// serializing it and then asking its SQL type for its OID, in one
    /// synchronous call on one thread (`BindCollector::push_bound_value`),
    /// so the value hands its OID to the type here.
    static BIND_OID: Cell<u32> = const { Cell::new(0) };
}

impl HasSqlType<RuntimeType> for Pg {
    fn metadata(_: &mut <Pg as TypeMetadata>::MetadataLookup) -> PgTypeMetadata {
        PgTypeMetadata::new(BIND_OID.with(Cell::get), 0)
    }
}

/// A bind value: its type's OID and its bytes in binary (`None` for
/// `NULL`).
#[derive(Debug, Clone, Copy)]
struct RuntimeValue<'a> {
    type_oid: u32,
    bytes: Option<&'a [u8]>,
}

impl ToSql<RuntimeType, Pg> for RuntimeValue<'_> {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        BIND_OID.with(|oid| oid.set(self.type_oid));
        match self.bytes {
            Some(bytes) => {
                out.write_all(bytes)?;
                Ok(IsNull::No)
            }
            None => Ok(IsNull::Yes),
        }
    }
}

/// One row as Diesel read it: each cell's column name, type OID (`None`
/// for `NULL`, which carries no type) and binary bytes.
struct RawRow(Vec<(String, Option<u32>, Option<Vec<u8>>)>);

impl FromSqlRow<Untyped, Pg> for RawRow {
    fn build_from_row<'a>(row: &impl Row<'a, Pg>) -> deserialize::Result<Self> {
        (0..row.field_count())
            .map(|index| {
                let field = row
                    .get(index)
                    .ok_or_else(|| format!("no field {index} in a row"))?;
                let name = field.field_name().unwrap_or_default().to_owned();
                let value = field.value();
                Ok((
                    name,
                    value.as_ref().map(|value| value.get_oid().get()),
                    value.map(|value| value.as_bytes().to_vec()),
                ))
            })
            .collect::<deserialize::Result<_>>()
            .map(RawRow)
    }
}

/// The rows, their columns named and typed from the cells: a column that
/// is `NULL` in every row has no known type (OID 0).
fn raw_rows(rows: Vec<RawRow>) -> RawRows {
    let mut columns: Vec<RawColumn> = <[RawRow]>::first(&rows).map_or_else(Vec::new, |row| {
        row.0
            .iter()
            .map(|(name, _, _)| RawColumn {
                name: name.clone(),
                type_oid: 0,
            })
            .collect()
    });
    for row in &rows {
        for (column, (_, type_oid, _)) in columns.iter_mut().zip(&row.0) {
            if column.type_oid == 0
                && let Some(type_oid) = type_oid
            {
                column.type_oid = *type_oid;
            }
        }
    }
    RawRows {
        columns,
        rows: rows
            .into_iter()
            .map(|row| row.0.into_iter().map(|(_, _, bytes)| bytes).collect())
            .collect(),
    }
}
