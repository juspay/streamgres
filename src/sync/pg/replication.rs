//! A minimal connection over Postgres's replication protocol, enough to
//! mint exported snapshots: the startup message asks for
//! `replication=database`, authentication follows the server's choice
//! (trust, password, MD5, SCRAM-SHA-256), and replication commands go
//! through the simple query protocol. `tokio-postgres` builds its startup
//! message from a fixed set of parameters and cannot open such a
//! connection, so the handful of messages involved are spoken directly
//! with `postgres-protocol`. Plain TCP or Unix sockets, no TLS.

use std::io;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::authentication::{self, sasl};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio_postgres::Config;
use tokio_postgres::config::Host;

use crate::sync::storage::StorageError;

/// The socket underneath, either flavor; `Detached` is a connection that
/// was never opened (a placeholder for tests of the alias bookkeeping).
enum Socket {
    Tcp(TcpStream),
    Unix(UnixStream),
    Detached,
}

impl Socket {
    /// Read into `buffer`; the number of bytes read (zero at end of stream).
    async fn read(&mut self, buffer: &mut BytesMut) -> io::Result<usize> {
        match self {
            Socket::Tcp(stream) => stream.read_buf(buffer).await,
            Socket::Unix(stream) => stream.read_buf(buffer).await,
            Socket::Detached => Err(io::ErrorKind::NotConnected.into()),
        }
    }

    /// Write all of `bytes`.
    async fn write(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self {
            Socket::Tcp(stream) => stream.write_all(bytes).await,
            Socket::Unix(stream) => stream.write_all(bytes).await,
            Socket::Detached => Err(io::ErrorKind::NotConnected.into()),
        }
    }
}

/// One open replication connection. Dropping it closes the socket, which
/// drops every temporary slot it created and invalidates the snapshot it
/// exported.
pub struct ReplicationConnection {
    socket: Socket,
    buffer: BytesMut,
}

impl ReplicationConnection {
    /// A connection that was never opened: every use fails with
    /// `NotConnected`. Lets tests build an [`super::Alias`] without a
    /// server.
    pub fn detached() -> Self {
        ReplicationConnection {
            socket: Socket::Detached,
            buffer: BytesMut::new(),
        }
    }

    /// Connect with `config`'s first host, user, password and database in
    /// replication mode and authenticate.
    pub async fn open(config: &Config) -> Result<Self, StorageError> {
        let host = config
            .get_hosts()
            .first()
            .ok_or_else(|| StorageError("the connection string names no host".to_owned()))?;
        let port = config.get_ports().first().copied().unwrap_or(5432);
        let socket = match host {
            Host::Tcp(host) => Socket::Tcp(
                TcpStream::connect((host.as_str(), port))
                    .await
                    .map_err(io_error)?,
            ),
            Host::Unix(dir) => {
                let path = dir.join(format!(".s.PGSQL.{port}"));
                Socket::Unix(UnixStream::connect(path).await.map_err(io_error)?)
            }
        };
        let user = config
            .get_user()
            .ok_or_else(|| StorageError("the connection string names no user".to_owned()))?
            .to_owned();
        let mut connection = ReplicationConnection {
            socket,
            buffer: BytesMut::with_capacity(8192),
        };
        let mut params = vec![
            ("client_encoding", "UTF8"),
            ("user", user.as_str()),
            ("replication", "database"),
        ];
        if let Some(dbname) = config.get_dbname() {
            params.push(("database", dbname));
        }
        let mut out = BytesMut::new();
        frontend::startup_message(params, &mut out).map_err(io_error)?;
        connection.socket.write(&out).await.map_err(io_error)?;
        connection
            .authenticate(&user, config.get_password())
            .await?;
        connection.until_ready().await?;
        Ok(connection)
    }

    /// Run one command and return its rows as text columns (`None` for
    /// SQL `NULL`).
    pub async fn simple_query(
        &mut self,
        sql: &str,
    ) -> Result<Vec<Vec<Option<String>>>, StorageError> {
        let mut out = BytesMut::new();
        frontend::query(sql, &mut out).map_err(io_error)?;
        self.socket.write(&out).await.map_err(io_error)?;
        let mut rows = Vec::new();
        loop {
            match self.next_message().await? {
                Message::DataRow(body) => {
                    let mut row = Vec::new();
                    let mut ranges = body.ranges();
                    while let Some(range) = ranges.next().map_err(io_error)? {
                        row.push(range.map(|range| {
                            String::from_utf8_lossy(&body.buffer()[range]).into_owned()
                        }));
                    }
                    rows.push(row);
                }
                Message::ReadyForQuery(_) => return Ok(rows),
                Message::ErrorResponse(body) => return Err(server_error(body.fields())),
                _ => {}
            }
        }
    }

    /// Answer the server's authentication request with the password.
    async fn authenticate(
        &mut self,
        user: &str,
        password: Option<&[u8]>,
    ) -> Result<(), StorageError> {
        let missing = || {
            StorageError(
                "the server asked for a password and the connection string has none".to_owned(),
            )
        };
        match self.next_message().await? {
            Message::AuthenticationOk => return Ok(()),
            Message::AuthenticationCleartextPassword => {
                self.send_password(password.ok_or_else(missing)?).await?;
            }
            Message::AuthenticationMd5Password(body) => {
                let hashed = authentication::md5_hash(
                    user.as_bytes(),
                    password.ok_or_else(missing)?,
                    body.salt(),
                );
                self.send_password(hashed.as_bytes()).await?;
            }
            Message::AuthenticationSasl(body) => {
                let mut offered = body.mechanisms();
                let mut scram = false;
                while let Some(mechanism) = offered.next().map_err(io_error)? {
                    scram |=
                        mechanism == sasl::SCRAM_SHA_256 || mechanism == sasl::SCRAM_SHA_256_PLUS;
                }
                if !scram {
                    return Err(StorageError(
                        "the server offers no SCRAM-SHA-256 authentication".to_owned(),
                    ));
                }
                let mut exchange = sasl::ScramSha256::new(
                    password.ok_or_else(missing)?,
                    sasl::ChannelBinding::unsupported(),
                );
                let mut out = BytesMut::new();
                frontend::sasl_initial_response(sasl::SCRAM_SHA_256, exchange.message(), &mut out)
                    .map_err(io_error)?;
                self.socket.write(&out).await.map_err(io_error)?;
                match self.next_message().await? {
                    Message::AuthenticationSaslContinue(body) => {
                        exchange.update(body.data()).map_err(io_error)?
                    }
                    Message::ErrorResponse(body) => return Err(server_error(body.fields())),
                    _ => return Err(StorageError("unexpected message during SCRAM".to_owned())),
                }
                let mut out = BytesMut::new();
                frontend::sasl_response(exchange.message(), &mut out).map_err(io_error)?;
                self.socket.write(&out).await.map_err(io_error)?;
                match self.next_message().await? {
                    Message::AuthenticationSaslFinal(body) => {
                        exchange.finish(body.data()).map_err(io_error)?
                    }
                    Message::ErrorResponse(body) => return Err(server_error(body.fields())),
                    _ => return Err(StorageError("unexpected message during SCRAM".to_owned())),
                }
            }
            Message::ErrorResponse(body) => return Err(server_error(body.fields())),
            _ => return Err(StorageError("unsupported authentication method".to_owned())),
        }
        match self.next_message().await? {
            Message::AuthenticationOk => Ok(()),
            Message::ErrorResponse(body) => Err(server_error(body.fields())),
            _ => Err(StorageError(
                "unexpected message after authentication".to_owned(),
            )),
        }
    }

    /// Send a password message.
    async fn send_password(&mut self, password: &[u8]) -> Result<(), StorageError> {
        let mut out = BytesMut::new();
        frontend::password_message(password, &mut out).map_err(io_error)?;
        self.socket.write(&out).await.map_err(io_error)
    }

    /// Consume the startup's parameter and key messages up to the first
    /// ready-for-query.
    async fn until_ready(&mut self) -> Result<(), StorageError> {
        loop {
            match self.next_message().await? {
                Message::ReadyForQuery(_) => return Ok(()),
                Message::ErrorResponse(body) => return Err(server_error(body.fields())),
                _ => {}
            }
        }
    }

    /// The next backend message, reading from the socket as needed.
    async fn next_message(&mut self) -> Result<Message, StorageError> {
        loop {
            if let Some(message) = Message::parse(&mut self.buffer).map_err(io_error)? {
                return Ok(message);
            }
            if self.socket.read(&mut self.buffer).await.map_err(io_error)? == 0 {
                return Err(StorageError("the replication connection closed".to_owned()));
            }
        }
    }
}

/// An I/O or protocol error as a storage error.
fn io_error(error: io::Error) -> StorageError {
    StorageError(error.to_string())
}

/// A server error response as a storage error carrying its message.
fn server_error(mut fields: postgres_protocol::message::backend::ErrorFields<'_>) -> StorageError {
    let mut message = String::from("server error");
    while let Ok(Some(field)) = fields.next() {
        if field.type_() == b'M' {
            message = String::from_utf8_lossy(field.value_bytes()).into_owned();
        }
    }
    StorageError(message)
}
