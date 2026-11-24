//! The application's `diesel-async` bb8 pool as Streamgres's connections,
//! through diesel-async's public API alone.
//!
//! A statement runs through `RunQueryDsl::load` as a query of Diesel's
//! own: its text pushed as SQL, each `$n` pushed as a bind of the type
//! Streamgres names at run time, and every row read through Diesel's `Row`
//! as its cells' bytes and type OIDs. Statements that return nothing run
//! through `batch_execute`.

use std::cell::Cell;
use std::io::Write as _;

use diesel::deserialize::{self, FromSqlRow};
use diesel::pg::{Pg, PgTypeMetadata};
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::result::QueryResult;
use diesel::row::{Field, Row};
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types::{HasSqlType, SingleValue, SqlType, TypeMetadata, Untyped};
use diesel_async::pooled_connection::bb8::{Pool, PooledConnection};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use streamgres::cache::{
    Bind, Connection, ConnectionFuture, Connections, Lease, RawColumn, RawRows, Session,
};

struct ApplicationPool {
    pool: Pool<AsyncPgConnection>,
    strings: tokio::sync::OnceCell<bool>,
}
struct Checkout(PooledConnection<'static, AsyncPgConnection>, bool);

/// Streamgres's connections from the application's pool: the same pool
/// for Diesel writes, direct reads and Streamgres. The pool must target the
/// replicated database with one role and session policy.
pub fn connections(pool: Pool<AsyncPgConnection>) -> impl Connections {
    ApplicationPool {
        pool,
        strings: tokio::sync::OnceCell::new(),
    }
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
            #[derive(diesel::QueryableByName)]
            struct Settings {
                #[diesel(sql_type = diesel::sql_types::Bool)]
                standard_strings: bool,
            }
            // All pool checkouts use one fixed session policy. Discover it once.
            let strings = *self.strings.get_or_try_init(|| async {
                let settings: Settings = diesel::sql_query("SELECT pg_catalog.current_setting('standard_conforming_strings')::bool AS standard_strings")
                    .get_result(&mut *connection).await.map_err(|error| error.to_string())?;
                Ok::<_, String>(settings.standard_strings)
            }).await?;
            Ok(Lease::new(Checkout(connection, strings)))
        })
    }
}

impl Session for Checkout {
    fn query<'a>(
        &'a mut self,
        sql: &'a str,
        binds: &'a [Bind],
    ) -> ConnectionFuture<'a, Result<RawRows, String>> {
        Box::pin(async move {
            let rows: Vec<RawRow> = RawQuery::with_strings(sql, binds, self.1)?
                .load(&mut *self.0)
                .await
                .map_err(|error| error.to_string())?;
            Ok(raw_rows(rows))
        })
    }

    fn execute<'a>(&'a mut self, sql: &'a str) -> ConnectionFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.0
                .batch_execute(sql)
                .await
                .map_err(|error| error.to_string())
        })
    }
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

/// One piece of a statement's text: plain SQL, or parameter `$n`.
#[derive(Debug, PartialEq, Eq)]
enum Piece<'a> {
    Sql(&'a str),
    Param(usize),
}

/// `sql` split at its parameters (`$1`, `$2`, …), outside string
/// literals, quoted names, comments and dollar-quoted strings.
fn pieces(sql: &str, standard_strings: bool) -> Vec<Piece<'_>> {
    let bytes = sql.as_bytes();
    let ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_' || b == b'$' || !b.is_ascii();
    let mut pieces = Vec::new();
    let (mut at, mut start) = (0, 0);
    while at < bytes.len() {
        match bytes[at] {
            quote @ (b'\'' | b'"') => {
                let escaped = quote == b'\''
                    && (!standard_strings
                        || (at > 0
                            && matches!(bytes[at - 1], b'e' | b'E')
                            && (at == 1 || !ident(bytes[at - 2]))));
                at += 1;
                while at < bytes.len() {
                    if escaped && bytes[at] == b'\\' {
                        at = (at + 2).min(bytes.len());
                        continue;
                    }
                    if bytes[at] == quote {
                        if bytes.get(at + 1) == Some(&quote) {
                            at += 2;
                            continue;
                        }
                        at += 1;
                        break;
                    }
                    at += 1;
                }
            }
            // A line comment ends at a line feed or, as Postgres also reads
            // it, a carriage return.
            b'-' if bytes.get(at + 1) == Some(&b'-') => {
                while at < bytes.len() && !matches!(bytes[at], b'\n' | b'\r') {
                    at += 1;
                }
            }
            b'/' if bytes.get(at + 1) == Some(&b'*') => {
                at += 2;
                let mut depth = 1usize;
                while at < bytes.len() && depth > 0 {
                    if bytes.get(at..at + 2) == Some(b"/*") {
                        depth += 1;
                        at += 2;
                    } else if bytes.get(at..at + 2) == Some(b"*/") {
                        depth -= 1;
                        at += 2;
                    } else {
                        at += 1;
                    }
                }
            }
            b'$' if at > 0 && ident(bytes[at - 1]) => at += 1,
            b'$' if bytes.get(at + 1).is_some_and(u8::is_ascii_digit) => {
                let digits = bytes[at + 1..]
                    .iter()
                    .take_while(|byte| byte.is_ascii_digit())
                    .count();
                if start < at {
                    pieces.push(Piece::Sql(&sql[start..at]));
                }
                pieces.push(Piece::Param(
                    sql[at + 1..at + 1 + digits].parse().unwrap_or(0),
                ));
                at += 1 + digits;
                start = at;
            }
            b'$' => {
                let tag_end = sql[at + 1..]
                    .find('$')
                    .filter(|end| {
                        sql[at + 1..at + 1 + end]
                            .bytes()
                            .all(|byte| ident(byte) && byte != b'$')
                    })
                    .map(|end| at + 1 + end + 1);
                at = match tag_end {
                    Some(end) => {
                        let tag = &sql[at..end];
                        sql[end..]
                            .find(tag)
                            .map_or(bytes.len(), |offset| end + offset + tag.len())
                    }
                    None => at + 1,
                };
            }
            _ => at += 1,
        }
    }
    if start < bytes.len() {
        pieces.push(Piece::Sql(&sql[start..]));
    }
    pieces
}

/// A statement of Streamgres's as a Diesel query: its text, with each
/// `$n` pushed as a bind of `binds[n - 1]`'s own type.
struct RawQuery<'a> {
    pieces: Vec<Piece<'a>>,
    binds: Vec<RuntimeValue<'a>>,
}

impl<'a> RawQuery<'a> {
    fn with_strings(
        sql: &'a str,
        binds: &'a [Bind],
        standard_strings: bool,
    ) -> Result<Self, String> {
        let pieces = pieces(sql, standard_strings);
        for piece in &pieces {
            if let Piece::Param(number) = piece
                && !(1..=binds.len()).contains(number)
            {
                return Err(format!("${number} has no bind value"));
            }
        }
        Ok(RawQuery {
            pieces,
            binds: binds
                .iter()
                .map(|bind| RuntimeValue {
                    type_oid: bind.type_oid,
                    bytes: bind.bytes.as_deref(),
                })
                .collect(),
        })
    }
}

impl QueryId for RawQuery<'_> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl Query for RawQuery<'_> {
    type SqlType = Untyped;
}

impl QueryFragment<Pg> for RawQuery<'_> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        // Each statement's text and bind types vary: keep none prepared.
        out.unsafe_to_cache_prepared();
        for piece in &self.pieces {
            match piece {
                Piece::Sql(sql) => out.push_sql(sql),
                Piece::Param(number) => {
                    out.push_bind_param::<RuntimeType, _>(&self.binds[number - 1])?
                }
            }
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Parameters are found outside quotes, comments and dollar quotes.
    #[test]
    fn parameters_are_split_out() {
        assert_eq!(
            pieces(
                r#"SELECT "a$1" FROM t WHERE x = $1 AND y = '$2' -- $3
                AND z = $$ $4 $$ /* $5 */ AND w IN ($2, $10)"#,
                true
            )
            .into_iter()
            .filter_map(|piece| match piece {
                Piece::Param(number) => Some(number),
                Piece::Sql(_) => None,
            })
            .collect::<Vec<_>>(),
            vec![1, 2, 10]
        );
        assert_eq!(pieces("SELECT 1", true), vec![Piece::Sql("SELECT 1")]);
        assert!(RawQuery::with_strings("SELECT $2", &[], true).is_err());
    }
    #[test]
    fn postgres_quoted_regions_do_not_supply_parameters() {
        let parameters = |sql, standard| {
            pieces(sql, standard)
                .into_iter()
                .filter_map(|p| match p {
                    Piece::Param(n) => Some(n),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(parameters(r"SELECT E'it\'s $1', $2", true), [2]);
        assert_eq!(
            parameters("SELECT 1 /* outer /* inner */ $1 */ + $2", true),
            [2]
        );
        assert_eq!(
            parameters("SELECT column$1, $é$ $2 $é$, $10, $1", true),
            [10, 1]
        );
        assert_eq!(parameters(r"SELECT 'it\'s $1', $2", false), [2]);
        assert_eq!(parameters(r"SELECT '\' AS value, $1", true), [1]);
        assert_eq!(parameters("SELECT 1 -- note\r$1", true), [1]);
        assert_eq!(parameters("SELECT 1 -- note\r\n$1 -- $2", true), [1]);
    }
}
