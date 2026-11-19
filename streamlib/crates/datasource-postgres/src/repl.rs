//! A small replication connection for creating snapshot slots, which tokio-postgres can't open.

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::authentication::{md5_hash, sasl};
use postgres_protocol::message::{backend::Message, frontend};
use streamlib_datasource::{Result, SourceError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::Config;
use tokio_postgres::config::Host;

/// Dropping it closes the connection, which removes its temporary slot and snapshot.
pub struct ReplConn {
    socket: TcpStream,
    buf: BytesMut,
}

/// Default wait for the TCP connection when the DSN sets none.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// One result row as text; `None` is SQL NULL.
pub type TextRow = Vec<Option<String>>;

fn io(e: std::io::Error) -> SourceError {
    SourceError::Transient(format!("replication connection: {e}"))
}

impl ReplConn {
    pub async fn open(cfg: &Config) -> Result<Self> {
        let Some(Host::Tcp(host)) = cfg.get_hosts().first() else {
            return Err(SourceError::Refused(
                "replication connection needs a TCP host".into(),
            ));
        };
        let port = cfg.get_ports().first().copied().unwrap_or(5432);
        let user = cfg.get_user().unwrap_or("postgres");
        let password = cfg.get_password().unwrap_or_default();
        let db = cfg.get_dbname().unwrap_or(user);

        let wait = cfg
            .get_connect_timeout()
            .copied()
            .unwrap_or(CONNECT_TIMEOUT);
        let socket = tokio::time::timeout(wait, TcpStream::connect((host.as_str(), port)))
            .await
            .map_err(|_| {
                SourceError::Transient(format!("replication connect timed out after {wait:?}"))
            })?
            .map_err(io)?;
        let mut conn = ReplConn {
            socket,
            buf: BytesMut::new(),
        };
        let mut out = BytesMut::new();
        let params = [
            ("user", user),
            ("database", db),
            ("replication", "database"),
            ("application_name", "streamlib"),
        ];
        frontend::startup_message(params, &mut out).map_err(io)?;
        conn.send(&mut out).await?;
        conn.authenticate(user, password).await?;
        Ok(conn)
    }

    async fn authenticate(&mut self, user: &str, password: &[u8]) -> Result<()> {
        let mut out = BytesMut::new();
        let mut scram: Option<sasl::ScramSha256> = None;
        loop {
            match self.recv().await? {
                Message::AuthenticationOk => {}
                Message::AuthenticationCleartextPassword => {
                    frontend::password_message(password, &mut out).map_err(io)?;
                    self.send(&mut out).await?;
                }
                Message::AuthenticationMd5Password(body) => {
                    let hash = md5_hash(user.as_bytes(), password, body.salt());
                    frontend::password_message(hash.as_bytes(), &mut out).map_err(io)?;
                    self.send(&mut out).await?;
                }
                Message::AuthenticationSasl(body) => {
                    let mut mechs = body.mechanisms();
                    let mut ok = false;
                    while let Some(m) = mechs.next().map_err(io)? {
                        ok |= m == sasl::SCRAM_SHA_256;
                    }
                    if !ok {
                        return Err(SourceError::Refused(
                            "server offers no SCRAM-SHA-256".into(),
                        ));
                    }
                    let s = sasl::ScramSha256::new(password, sasl::ChannelBinding::unsupported());
                    frontend::sasl_initial_response(sasl::SCRAM_SHA_256, s.message(), &mut out)
                        .map_err(io)?;
                    self.send(&mut out).await?;
                    scram = Some(s);
                }
                Message::AuthenticationSaslContinue(body) => {
                    let s = scram
                        .as_mut()
                        .ok_or_else(|| protocol("unexpected SASL continue"))?;
                    s.update(body.data()).map_err(io)?;
                    frontend::sasl_response(s.message(), &mut out).map_err(io)?;
                    self.send(&mut out).await?;
                }
                Message::AuthenticationSaslFinal(body) => {
                    let s = scram
                        .as_mut()
                        .ok_or_else(|| protocol("unexpected SASL final"))?;
                    s.finish(body.data()).map_err(io)?;
                }
                Message::ErrorResponse(body) => return Err(server_error(&body)),
                Message::ReadyForQuery(_) => return Ok(()),
                Message::AuthenticationGss
                | Message::AuthenticationKerberosV5
                | Message::AuthenticationScmCredential
                | Message::AuthenticationSspi
                | Message::AuthenticationGssContinue(_) => {
                    return Err(SourceError::Refused(
                        "unsupported authentication method".into(),
                    ));
                }
                _ => {}
            }
        }
    }

    /// Run one command and return its rows.
    pub async fn query(&mut self, sql: &str) -> Result<Vec<TextRow>> {
        let mut out = BytesMut::new();
        frontend::query(sql, &mut out).map_err(io)?;
        self.send(&mut out).await?;
        let mut rows = vec![];
        let mut failed = None;
        loop {
            match self.recv().await? {
                Message::DataRow(body) => {
                    let buf = body.buffer();
                    let row = body
                        .ranges()
                        .map(|r| Ok(r.map(|r| String::from_utf8_lossy(&buf[r]).into_owned())))
                        .collect()
                        .map_err(io)?;
                    rows.push(row);
                }
                Message::ErrorResponse(body) => failed = Some(server_error(&body)),
                Message::ReadyForQuery(_) => return failed.map_or(Ok(rows), Err),
                _ => {}
            }
        }
    }

    async fn send(&mut self, out: &mut BytesMut) -> Result<()> {
        self.socket.write_all(out).await.map_err(io)?;
        out.clear();
        Ok(())
    }

    async fn recv(&mut self) -> Result<Message> {
        loop {
            if let Some(m) = Message::parse(&mut self.buf).map_err(io)? {
                return Ok(m);
            }
            if self.socket.read_buf(&mut self.buf).await.map_err(io)? == 0 {
                return Err(io(std::io::ErrorKind::UnexpectedEof.into()));
            }
        }
    }
}

fn protocol(why: &str) -> SourceError {
    SourceError::Transient(format!("replication protocol: {why}"))
}

fn server_error(body: &postgres_protocol::message::backend::ErrorResponseBody) -> SourceError {
    let mut fields = body.fields();
    let mut message = String::from("server error");
    while let Ok(Some(f)) = fields.next() {
        if f.type_() == b'M' {
            message = String::from_utf8_lossy(f.value_bytes()).into_owned();
        }
    }
    SourceError::Transient(message)
}
