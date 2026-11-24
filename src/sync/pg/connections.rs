//! Application-owned Postgres connections for ordinary SQL.
//!
//! Streamgres needs two things of a connection ([`Session`]): run one
//! statement with binds, each sent in binary with its type's OID, and get
//! its rows back with each cell in binary and its type's OID
//! ([`Session::query`]); and run statements that return nothing
//! ([`Session::execute`]). Any pool
//! can supply that through its own public API: a `tokio_postgres::Client`
//! does here, a `diesel-async` pool does in `streamgres-diesel`.
//!
//! A checkout is exclusive and outside a transaction. Every connection must
//! target the replicated database with one role and session policy.
//! Replication and exported-snapshot minting use the dedicated direct
//! endpoint, never these connections.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use bytes::BytesMut;
use tokio_postgres::types::{FromSql, IsNull, Kind, ToSql, Type, to_sql_checked};

use super::binary::oid;
pub use crate::parser::Bind;

pub type ConnectionFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// One column of a [`RawRows`]: its name and its type's OID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawColumn {
    pub name: String,
    pub type_oid: u32,
}

/// A statement's rows, each cell in Postgres's binary format (`None` for
/// `NULL`). `columns` describes them; it may be empty when there are no
/// rows, for a connection that only learns the columns from a row.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RawRows {
    pub columns: Vec<RawColumn>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
}

/// Why a statement on a [`Session`] failed, kept structured so callers
/// can tell a timeout or a Postgres error apart without reading messages.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SessionError {
    /// Postgres refused or failed the statement; `code` is its SQLSTATE
    /// when the driver reports one.
    Database {
        code: Option<String>,
        message: String,
    },
    /// The statement ran out of time: Postgres cancelled it for its
    /// `statement_timeout`, or one of Streamgres's own deadlines passed.
    Timeout(String),
    /// Postgres cancelled the statement for another reason: a cancel
    /// request from a client or an administrator (`pg_cancel_backend`).
    Cancelled(String),
    /// Anything else: the connection, the protocol, the pool.
    Other(String),
}

impl SessionError {
    /// The SQLSTATE of a statement Postgres cancelled, for its timeout or
    /// on request alike.
    pub const QUERY_CANCELED: &'static str = "57014";

    /// A Postgres error with `code`. A cancelled statement (57014) is a
    /// [`SessionError::Timeout`] when its message names the statement
    /// timeout, else [`SessionError::Cancelled`]: the code alone does not
    /// tell them apart, and the message is Postgres's English one unless
    /// the server translates its messages.
    pub fn database(code: Option<&str>, message: impl Into<String>) -> Self {
        let message = message.into();
        if code == Some(Self::QUERY_CANCELED) {
            return if message.contains("statement timeout") {
                SessionError::Timeout(message)
            } else {
                SessionError::Cancelled(message)
            };
        }
        SessionError::Database {
            code: code.map(str::to_owned),
            message,
        }
    }
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionError::Database {
                code: Some(code),
                message,
            } => write!(f, "{message} (SQLSTATE {code})"),
            SessionError::Database {
                code: None,
                message,
            }
            | SessionError::Timeout(message)
            | SessionError::Cancelled(message)
            | SessionError::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for SessionError {}

impl From<String> for SessionError {
    fn from(message: String) -> Self {
        SessionError::Other(message)
    }
}

impl From<SessionError> for String {
    fn from(error: SessionError) -> Self {
        error.to_string()
    }
}

impl From<SessionError> for crate::sync::StorageError {
    fn from(error: SessionError) -> Self {
        crate::sync::StorageError(error.to_string())
    }
}

impl From<tokio_postgres::Error> for SessionError {
    fn from(error: tokio_postgres::Error) -> Self {
        match error.as_db_error() {
            Some(db) => SessionError::database(Some(db.code().code()), db.message()),
            None => SessionError::Other(error.to_string()),
        }
    }
}

/// What Streamgres runs on a connection.
pub trait Session: Send {
    /// Runs one statement with `binds`, each sent in binary and declared by
    /// its own type OID; every cell comes back in binary.
    fn query<'a>(
        &'a mut self,
        sql: &'a str,
        binds: &'a [Bind],
    ) -> ConnectionFuture<'a, Result<RawRows, SessionError>>;

    /// Runs statements that return no rows, `;`-separated, in one round
    /// trip.
    fn execute<'a>(&'a mut self, sql: &'a str) -> ConnectionFuture<'a, Result<(), SessionError>>;
}

/// An application's checked-out connection. Dropping it returns it to its
/// pool.
pub trait Connection: Session + 'static {
    /// Removes this connection from the pool and closes it. It may have an
    /// unfinished query or transaction and must not be handed to anyone.
    fn discard(self: Box<Self>);
}

/// A bind value sent as the bytes it was given, in binary format.
#[derive(Debug)]
struct RawParam<'a>(Option<&'a [u8]>);

impl ToSql for RawParam<'_> {
    fn to_sql(
        &self,
        _type: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self.0 {
            Some(bytes) => {
                out.extend_from_slice(bytes);
                Ok(IsNull::No)
            }
            None => Ok(IsNull::Yes),
        }
    }

    fn accepts(_type: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

/// A cell kept as the bytes Postgres sent for it.
struct RawCell(Option<Vec<u8>>);

impl<'a> FromSql<'a> for RawCell {
    fn from_sql(
        _type: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(RawCell(Some(raw.to_vec())))
    }

    fn from_sql_null(_type: &Type) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(RawCell(None))
    }

    fn accepts(_type: &Type) -> bool {
        true
    }
}

async fn prepare_binary(
    client: &tokio_postgres::Client,
    sql: &str,
    binds: &[Bind],
) -> Result<tokio_postgres::Statement, SessionError> {
    // Declare custom types by their own OID so Postgres never infers them
    // as unknown, which can fail when the bind alone gives the type.
    let types: Vec<Type> = binds
        .iter()
        .map(|bind| {
            Type::from_oid(bind.type_oid).unwrap_or_else(|| {
                Type::new(String::new(), bind.type_oid, Kind::Simple, String::new())
            })
        })
        .collect();
    Ok(client.prepare_typed(sql, &types).await?)
}

async fn query_binary(
    client: &tokio_postgres::Client,
    statement: &tokio_postgres::Statement,
    binds: &[Bind],
) -> Result<RawRows, SessionError> {
    let params: Vec<RawParam<'_>> = binds
        .iter()
        .map(|bind| RawParam(bind.bytes.as_deref()))
        .collect();
    let refs: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|param| param as &(dyn ToSql + Sync))
        .collect();
    let rows = client.query(statement, &refs).await?;
    let columns: Vec<RawColumn> = statement
        .columns()
        .iter()
        .map(|column| RawColumn {
            name: column.name().to_owned(),
            type_oid: column.type_().oid(),
        })
        .collect();
    let rows = rows
        .iter()
        .map(|row| {
            (0..columns.len())
                .map(|index| {
                    row.try_get::<_, RawCell>(index)
                        .map(|cell| cell.0)
                        .map_err(|error| error.to_string())
                })
                .collect::<Result<Vec<_>, String>>()
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(RawRows { columns, rows })
}

const FALLBACK_TIMEOUTS: &str = "SELECT pg_catalog.set_config(name,
    CASE WHEN setting::pg_catalog.int8 = 0 THEN $1
         ELSE LEAST(setting::pg_catalog.int8, $1::pg_catalog.int8)::pg_catalog.text
    END, true)
    FROM pg_catalog.pg_settings
    WHERE name IN ('statement_timeout', 'idle_in_transaction_session_timeout')";

async fn fallback_sequential(
    session: &mut dyn Session,
    sql: &str,
    binds: &[Bind],
    millis: u128,
) -> Result<RawRows, SessionError> {
    session.execute("BEGIN READ ONLY").await?;
    session
        .query(FALLBACK_TIMEOUTS, &[text(&millis.to_string())])
        .await?;
    let rows = session.query(sql, binds).await?;
    session.execute("ROLLBACK").await?;
    Ok(rows)
}

impl Session for tokio_postgres::Client {
    fn query<'a>(
        &'a mut self,
        sql: &'a str,
        binds: &'a [Bind],
    ) -> ConnectionFuture<'a, Result<RawRows, SessionError>> {
        Box::pin(async move {
            let statement = prepare_binary(self, sql, binds).await?;
            query_binary(self, &statement, binds).await
        })
    }

    fn execute<'a>(&'a mut self, sql: &'a str) -> ConnectionFuture<'a, Result<(), SessionError>> {
        Box::pin(async move { self.batch_execute(sql).await.map_err(SessionError::from) })
    }
}

impl Connection for tokio_postgres::Client {
    fn discard(self: Box<Self>) {
        drop(self);
    }
}

/// An exclusive checkout, discarded unless Streamgres explicitly
/// returns it to its pool.
pub struct Lease {
    connection: Option<LeasedConnection>,
}

enum LeasedConnection {
    Application(Box<dyn Connection>),
    BuiltIn(Pooled),
}

impl Lease {
    pub fn new(connection: impl Connection) -> Self {
        Self {
            connection: Some(LeasedConnection::Application(Box::new(connection))),
        }
    }

    /// Returns a clean connection to its pool.
    pub(crate) fn recycle(mut self) {
        self.connection.take();
    }

    fn connection(&mut self) -> &mut LeasedConnection {
        self.connection
            .as_mut()
            .expect("a live checkout has a connection")
    }

    fn session(&mut self) -> &mut dyn Session {
        match self.connection() {
            LeasedConnection::Application(connection) => connection.as_mut(),
            LeasedConnection::BuiltIn(connection) => connection,
        }
    }

    /// An outer error discards the checkout. A built-in checkout may
    /// return an inner query error after PostgreSQL confirms rollback.
    pub(crate) async fn fallback_read(
        &mut self,
        sql: &str,
        binds: &[Bind],
        remaining: Duration,
        reuse: bool,
    ) -> Result<Result<RawRows, SessionError>, SessionError> {
        let millis = remaining.as_millis().clamp(1, i32::MAX as u128);
        match self.connection() {
            LeasedConnection::BuiltIn(connection) => {
                connection.fallback_read(sql, binds, millis, reuse).await
            }
            LeasedConnection::Application(connection) => {
                let rows = fallback_sequential(connection.as_mut(), sql, binds, millis).await?;
                Ok(Ok(rows))
            }
        }
    }
}

impl Session for Lease {
    fn query<'a>(
        &'a mut self,
        sql: &'a str,
        binds: &'a [Bind],
    ) -> ConnectionFuture<'a, Result<RawRows, SessionError>> {
        self.session().query(sql, binds)
    }

    fn execute<'a>(&'a mut self, sql: &'a str) -> ConnectionFuture<'a, Result<(), SessionError>> {
        self.session().execute(sql)
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        match self.connection.take() {
            Some(LeasedConnection::Application(connection)) => connection.discard(),
            Some(LeasedConnection::BuiltIn(mut connection)) => {
                connection.client.take();
            }
            None => {}
        }
    }
}

/// The application's pool or connection factory. It stays the app's:
/// Streamgres neither closes the pool nor opens a second query pool.
pub trait Connections: Send + Sync + 'static {
    fn get(&self) -> ConnectionFuture<'_, Result<Lease, String>>;
}

impl<F, Fut> Connections for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Lease, String>> + Send + 'static,
{
    fn get(&self) -> ConnectionFuture<'_, Result<Lease, String>> {
        Box::pin(self())
    }
}

/// Streamgres's own pool: up to `size` connections to the database,
/// opened as needed and kept while idle. Unverified sessions are closed
/// when a checkout is dropped.
pub(crate) struct OwnPool(std::sync::Arc<OwnShared>);

struct OwnShared {
    config: tokio_postgres::Config,
    permits: std::sync::Arc<tokio::sync::Semaphore>,
    idle: std::sync::Mutex<Vec<DrivenClient>>,
}

impl OwnPool {
    /// A pool of up to `size` connections to `dsn`.
    pub fn new(dsn: &str, size: usize) -> Result<Self, String> {
        let config = dsn
            .parse()
            .map_err(|error| format!("Streamgres's DSN: {error}"))?;
        Ok(OwnPool(std::sync::Arc::new(OwnShared {
            config,
            permits: std::sync::Arc::new(tokio::sync::Semaphore::new(size.max(1))),
            idle: std::sync::Mutex::new(Vec::new()),
        })))
    }
}

impl Connections for OwnPool {
    fn get(&self) -> ConnectionFuture<'_, Result<Lease, String>> {
        let shared = self.0.clone();
        Box::pin(async move {
            let permit = shared
                .permits
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| "Streamgres's pool is closed".to_owned())?;
            let idle = loop {
                let client = shared
                    .idle
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .pop();
                match client {
                    Some(client) if client.client.is_closed() => continue,
                    other => break other,
                }
            };
            let client =
                match idle {
                    Some(client) => client,
                    None => {
                        let (client, connection) =
                            shared.config.connect(tokio_postgres::NoTls).await.map_err(
                                |error| format!("connecting Streamgres's pool: {error}"),
                            )?;
                        let driver = tokio::spawn(async move {
                            let _ = connection.await;
                        });
                        DrivenClient { client, driver }
                    }
                };
            Ok(Lease {
                connection: Some(LeasedConnection::BuiltIn(Pooled {
                    client: Some(client),
                    shared,
                    _permit: permit,
                })),
            })
        })
    }
}

/// Owns the socket's driver. A discarded session closes its socket
/// instead of leaving the detached driver draining a timed-out request.
struct DrivenClient {
    client: tokio_postgres::Client,
    driver: tokio::task::JoinHandle<()>,
}

impl Drop for DrivenClient {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

/// A connection of [`OwnPool`], returned to it when dropped.
struct Pooled {
    client: Option<DrivenClient>,
    shared: std::sync::Arc<OwnShared>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Pooled {
    fn client(&mut self) -> &mut tokio_postgres::Client {
        &mut self
            .client
            .as_mut()
            .expect("a pooled connection has its client until dropped")
            .client
    }

    async fn fallback_read(
        &mut self,
        sql: &str,
        binds: &[Bind],
        millis: u128,
        reuse: bool,
    ) -> Result<Result<RawRows, SessionError>, SessionError> {
        let client = self.client();
        // Only the computed integer enters this batch. The application
        // statement is prepared separately after the transaction is ready.
        let timeouts = FALLBACK_TIMEOUTS.replace("$1", &format!("'{millis}'"));
        client
            .batch_execute(&format!("BEGIN READ ONLY; {timeouts}"))
            .await?;
        let statement = prepare_binary(client, sql, binds).await?;
        if !reuse {
            // The lease closes the socket, which ends the transaction.
            let rows = query_binary(client, &statement, binds).await?;
            return Ok(Ok(rows));
        }
        // Poll execution first so both messages reach Postgres in order.
        // Wait for rollback even when execution reports an error.
        let (query, rollback) = tokio::join!(
            biased;
            query_binary(client, &statement, binds),
            client.batch_execute("ROLLBACK"),
        );
        match rollback {
            Ok(()) => Ok(query),
            Err(error) => Err(query.err().unwrap_or_else(|| error.into())),
        }
    }
}

impl Session for Pooled {
    fn query<'a>(
        &'a mut self,
        sql: &'a str,
        binds: &'a [Bind],
    ) -> ConnectionFuture<'a, Result<RawRows, SessionError>> {
        Session::query(self.client(), sql, binds)
    }

    fn execute<'a>(&'a mut self, sql: &'a str) -> ConnectionFuture<'a, Result<(), SessionError>> {
        Session::execute(self.client(), sql)
    }
}

impl Drop for Pooled {
    fn drop(&mut self) {
        if let Some(client) = self.client.take()
            && !client.client.is_closed()
        {
            self.shared
                .idle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(client);
        }
    }
}

/// Streamgres's own Tokio runtime: every connection, fallback read,
/// timeout and background task of the library runs here, so the
/// application may call it from any runtime or from none.
pub(crate) struct Io {
    runtime: Option<tokio::runtime::Runtime>,
    handle: tokio::runtime::Handle,
}

impl Io {
    /// Two worker threads, named `streamgres-io`.
    pub fn new() -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("streamgres-io")
            .enable_all()
            .build()
            .map_err(|error| format!("starting Streamgres's runtime: {error}"))?;
        Ok(Io {
            handle: runtime.handle().clone(),
            runtime: Some(runtime),
        })
    }

    pub fn handle(&self) -> &tokio::runtime::Handle {
        &self.handle
    }
}

impl Drop for Io {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            // Tokio permits background shutdown from an async context.
            runtime.shutdown_background();
        }
    }
}

/// Makes pool maintenance and connection driver tasks run on `runtime`,
/// whichever runtime asks: Streamgres's own for its pool, the
/// application's for an application pool (whose connections must not
/// depend on Streamgres's runtime). It keeps Streamgres's runtime alive.
pub(crate) struct OnRuntime {
    pub connections: std::sync::Arc<dyn Connections>,
    pub runtime: tokio::runtime::Handle,
    /// Held, not read: the engine owns this, so Streamgres's runtime lives
    /// as long as anything that may run on it.
    #[allow(dead_code, reason = "keeps the runtime alive")]
    pub io: std::sync::Arc<Io>,
}

impl Connections for OnRuntime {
    fn get(&self) -> ConnectionFuture<'_, Result<Lease, String>> {
        let connections = self.connections.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            Task::new(runtime.spawn(async move { connections.get().await }))
                .join()
                .await
                .map_err(|error| format!("application checkout ended: {error}"))?
        })
    }
}

/// A `text` bind.
pub fn text(value: &str) -> Bind {
    Bind {
        type_oid: oid::TEXT,
        bytes: Some(value.as_bytes().to_vec()),
    }
}

/// A one-dimensional `text[]` bind.
pub fn text_array<S: AsRef<str>>(values: &[S]) -> Bind {
    let mut bytes = Vec::new();
    let words = [
        1i32,
        0,
        oid::TEXT as i32,
        i32::try_from(values.len()).unwrap_or(i32::MAX),
        1,
    ];
    for word in words {
        bytes.extend_from_slice(&word.to_be_bytes());
    }
    for value in values {
        let value = value.as_ref().as_bytes();
        bytes.extend_from_slice(&i32::try_from(value.len()).unwrap_or(i32::MAX).to_be_bytes());
        bytes.extend_from_slice(value);
    }
    Bind {
        type_oid: 1009,
        bytes: Some(bytes),
    }
}

impl RawRows {
    /// The type of column `column`; `None` when the rows do not say.
    fn type_of(&self, column: usize) -> Option<u32> {
        self.columns.get(column).map(|column| column.type_oid)
    }

    /// The cell at `row`, `column`.
    fn cell(&self, row: usize, column: usize) -> Result<Option<&[u8]>, String> {
        self.rows
            .get(row)
            .and_then(|cells| cells.get(column))
            .map(Option::as_deref)
            .ok_or_else(|| format!("no cell at row {row}, column {column}"))
    }

    /// A text-like cell (`text`, `varchar`, `name`, `char`, `bpchar`) as
    /// text.
    pub fn text(&self, row: usize, column: usize) -> Result<Option<String>, String> {
        match self.cell(row, column)? {
            None => Ok(None),
            Some(bytes) => String::from_utf8(bytes.to_vec())
                .map(Some)
                .map_err(|_| "a text cell is not UTF-8".to_owned()),
        }
    }

    /// An integer cell (`int2`, `int4`, `int8`, `oid`).
    pub fn int(&self, row: usize, column: usize) -> Result<Option<i64>, String> {
        let Some(bytes) = self.cell(row, column)? else {
            return Ok(None);
        };
        let wrong = || format!("a {}-byte cell is not an integer", bytes.len());
        Ok(Some(match bytes.len() {
            2 => i64::from(i16::from_be_bytes(bytes.try_into().map_err(|_| wrong())?)),
            4 if self.type_of(column) == Some(oid::OID) => {
                i64::from(u32::from_be_bytes(bytes.try_into().map_err(|_| wrong())?))
            }
            4 => i64::from(i32::from_be_bytes(bytes.try_into().map_err(|_| wrong())?)),
            8 => i64::from_be_bytes(bytes.try_into().map_err(|_| wrong())?),
            _ => return Err(wrong()),
        }))
    }

    /// A `bool` cell.
    pub fn bool(&self, row: usize, column: usize) -> Result<Option<bool>, String> {
        Ok(self.cell(row, column)?.map(|bytes| bytes != [0]))
    }

    /// Every row as Postgres's text form of each cell, the form a
    /// simple-query read returns: what the storage reads parse.
    pub fn into_text(self) -> Result<Vec<TextRow>, String> {
        let RawRows { columns, rows } = self;
        rows.into_iter()
            .map(|cells| {
                cells
                    .into_iter()
                    .enumerate()
                    .map(|(index, cell)| match cell {
                        None => Ok(None),
                        Some(bytes) => {
                            let type_oid = columns.get(index).map_or(oid::TEXT, |c| c.type_oid);
                            text_of(type_oid, &bytes).map(Some)
                        }
                    })
                    .collect::<Result<Vec<_>, String>>()
                    .map(TextRow)
            })
            .collect()
    }
}

/// One row of a read, each cell as Postgres's text form (`None` for
/// `NULL`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TextRow(pub Vec<Option<String>>);

impl TextRow {
    /// The cell at `index` as text; `None` for `NULL` or a column the row
    /// does not have.
    pub fn get(&self, index: usize) -> Option<&str> {
        self.0.get(index).and_then(Option::as_deref)
    }

    /// How many cells the row has.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the row has no cells.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<&tokio_postgres::SimpleQueryRow> for TextRow {
    fn from(row: &tokio_postgres::SimpleQueryRow) -> Self {
        TextRow(
            (0..row.len())
                .map(|index| row.get(index).map(str::to_owned))
                .collect(),
        )
    }
}

/// The days and microseconds Postgres counts dates and timestamps from.
const POSTGRES_EPOCH: (i32, u32, u32) = (2000, 1, 1);

/// A binary cell of `type_oid` as the text Postgres's output function
/// writes for it, for the types the engine's storage reads select: the
/// integers, the floats, `bool`, the text types, `json`, `jsonb`, `date`
/// and `timestamp`.
fn text_of(type_oid: u32, bytes: &[u8]) -> Result<String, String> {
    let fixed = |len: usize| -> Result<&[u8], String> {
        (bytes.len() == len)
            .then_some(bytes)
            .ok_or_else(|| format!("a {}-byte cell of type {type_oid}", bytes.len()))
    };
    let utf8 = |bytes: &[u8]| {
        String::from_utf8(bytes.to_vec()).map_err(|_| "a text cell is not UTF-8".to_owned())
    };
    Ok(match type_oid {
        oid::BOOL => (if fixed(1)? == [0] { "f" } else { "t" }).to_owned(),
        oid::INT2 => i16::from_be_bytes(fixed(2)?.try_into().unwrap_or_default()).to_string(),
        oid::INT4 => i32::from_be_bytes(fixed(4)?.try_into().unwrap_or_default()).to_string(),
        oid::OID => u32::from_be_bytes(fixed(4)?.try_into().unwrap_or_default()).to_string(),
        oid::INT8 => i64::from_be_bytes(fixed(8)?.try_into().unwrap_or_default()).to_string(),
        oid::FLOAT4 => float_text(f32::from_be_bytes(fixed(4)?.try_into().unwrap_or_default())),
        oid::FLOAT8 => float_text(f64::from_be_bytes(fixed(8)?.try_into().unwrap_or_default())),
        oid::JSONB => match bytes.split_first() {
            Some((1, body)) => utf8(body)?,
            _ => return Err("a jsonb cell of an unknown version".to_owned()),
        },
        oid::DATE => {
            let days = i32::from_be_bytes(fixed(4)?.try_into().unwrap_or_default());
            match days {
                i32::MAX => "infinity".to_owned(),
                i32::MIN => "-infinity".to_owned(),
                days => epoch()
                    .date()
                    .checked_add_signed(chrono::Duration::days(i64::from(days)))
                    .ok_or_else(|| "a date out of range".to_owned())?
                    .format("%Y-%m-%d")
                    .to_string(),
            }
        }
        oid::TIMESTAMP => {
            let micros = i64::from_be_bytes(fixed(8)?.try_into().unwrap_or_default());
            match micros {
                i64::MAX => "infinity".to_owned(),
                i64::MIN => "-infinity".to_owned(),
                micros => {
                    let at = epoch()
                        .checked_add_signed(chrono::Duration::microseconds(micros))
                        .ok_or_else(|| "a timestamp out of range".to_owned())?;
                    let text = at.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
                    text.trim_end_matches('0').trim_end_matches('.').to_owned()
                }
            }
        }
        // The text types, `json`, `unknown` and enum labels are their text.
        _ => utf8(bytes)?,
    })
}

/// Postgres's epoch for dates and timestamps.
fn epoch() -> chrono::NaiveDateTime {
    let (year, month, day) = POSTGRES_EPOCH;
    chrono::NaiveDate::from_ymd_opt(year, month, day)
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .unwrap_or_default()
}

/// A float as Postgres writes it: its shortest exact digits, with
/// `NaN`, `Infinity` and `-Infinity` spelled as Postgres spells them.
fn float_text<F: std::fmt::Display + Into<f64> + Copy>(value: F) -> String {
    let wide: f64 = value.into();
    if wide.is_nan() {
        "NaN".to_owned()
    } else if wide.is_infinite() {
        if wide > 0.0 { "Infinity" } else { "-Infinity" }.to_owned()
    } else {
        value.to_string()
    }
}

/// Caller-owned queued work. Dropping its waiter cancels it; database work
/// that has already taken ownership of a permit has its own lifetime.
pub(crate) struct Task<T>(tokio::task::JoinHandle<T>);
impl<T> Task<T> {
    pub(crate) fn new(task: tokio::task::JoinHandle<T>) -> Self {
        Self(task)
    }
    pub(crate) async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        (&mut self.0).await
    }
}
impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Binary cells read back as the text Postgres would have sent.
    #[test]
    fn binary_cells_read_as_postgres_text() {
        assert_eq!(text_of(oid::INT8, &(-7i64).to_be_bytes()).unwrap(), "-7");
        assert_eq!(text_of(oid::BOOL, &[1]).unwrap(), "t");
        assert_eq!(text_of(oid::FLOAT8, &0.1f64.to_be_bytes()).unwrap(), "0.1");
        assert_eq!(text_of(oid::FLOAT4, &0.1f32.to_be_bytes()).unwrap(), "0.1");
        assert_eq!(
            text_of(oid::FLOAT8, &f64::NAN.to_be_bytes()).unwrap(),
            "NaN"
        );
        assert_eq!(
            text_of(oid::FLOAT8, &f64::NEG_INFINITY.to_be_bytes()).unwrap(),
            "-Infinity"
        );
        assert_eq!(text_of(oid::TEXT, b"it's").unwrap(), "it's");
        assert_eq!(
            text_of(oid::JSONB, b"\x01{\"a\": 1}").unwrap(),
            "{\"a\": 1}"
        );
        assert_eq!(
            text_of(oid::DATE, &9770i32.to_be_bytes()).unwrap(),
            "2026-10-01"
        );
        let micros = 844_164_000_500_000i64;
        assert_eq!(
            text_of(oid::TIMESTAMP, &micros.to_be_bytes()).unwrap(),
            "2026-10-01 10:00:00.5"
        );
        assert_eq!(
            text_of(oid::TIMESTAMP, &(micros - 500_000).to_be_bytes()).unwrap(),
            "2026-10-01 10:00:00"
        );
    }

    /// A `text[]` bind is Postgres's one-dimensional array format.
    #[test]
    fn text_arrays_encode() {
        let bind = text_array(&["a", "bc"]);
        assert_eq!(bind.type_oid, 1009);
        let bytes = bind.bytes.unwrap();
        assert_eq!(&bytes[..4], &1i32.to_be_bytes());
        assert_eq!(&bytes[12..16], &2i32.to_be_bytes());
        assert_eq!(&bytes[bytes.len() - 2..], b"bc");
    }

    /// Typed getters read the integer widths and booleans.
    #[test]
    fn raw_rows_read_typed_cells() {
        let rows = RawRows {
            columns: vec![
                RawColumn {
                    name: "a".into(),
                    type_oid: oid::INT4,
                },
                RawColumn {
                    name: "b".into(),
                    type_oid: oid::BOOL,
                },
                RawColumn {
                    name: "c".into(),
                    type_oid: oid::TEXT,
                },
            ],
            rows: vec![vec![Some(5i32.to_be_bytes().to_vec()), Some(vec![0]), None]],
        };
        assert_eq!(rows.int(0, 0).unwrap(), Some(5));
        assert_eq!(rows.bool(0, 1).unwrap(), Some(false));
        assert_eq!(rows.text(0, 2).unwrap(), None);
        assert!(rows.int(1, 0).is_err());
    }
}

#[cfg(test)]
#[path = "connections/fallback_tests.rs"]
mod fallback_tests;
