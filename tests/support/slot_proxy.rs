//! A byte-forwarding network gate. It never parses or matches cleanup SQL.

use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_postgres::config::Host;

trait Socket: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Socket for T {}

enum Gate {
    Pause(oneshot::Sender<()>),
    Resume(oneshot::Sender<()>),
}

pub struct Proxy {
    pub dsn: String,
    control: mpsc::Sender<Gate>,
    task: tokio::task::JoinHandle<()>,
}

fn quoted(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

impl Proxy {
    pub async fn start(dsn: &str, application: &str) -> Self {
        let config: tokio_postgres::Config = dsn.parse().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let user = config.get_user().expect("test DSN must specify a user");
        let password = std::str::from_utf8(config.get_password().unwrap_or_default()).unwrap();
        let proxy_dsn = format!(
            "host=127.0.0.1 port={port} user={} password={} dbname={} application_name={} sslmode=disable",
            quoted(user),
            quoted(password),
            quoted(config.get_dbname().unwrap_or(user)),
            quoted(application),
        );
        let (control, mut commands) = mpsc::channel(4);
        let task = tokio::spawn(async move {
            let (client, _) = listener.accept().await.unwrap();
            let upstream_port = config.get_ports().first().copied().unwrap_or(5432);
            let upstream: Box<dyn Socket> = match config.get_hosts().first().unwrap() {
                Host::Tcp(host) => Box::new(
                    TcpStream::connect((host.as_str(), upstream_port))
                        .await
                        .unwrap(),
                ),
                #[cfg(unix)]
                Host::Unix(path) => Box::new(
                    tokio::net::UnixStream::connect(path.join(format!(".s.PGSQL.{upstream_port}")))
                        .await
                        .unwrap(),
                ),
            };
            let (mut client_read, mut client_write) = tokio::io::split(client);
            let (mut server_read, mut server_write) = tokio::io::split(upstream);
            let replies = async {
                let mut paused = false;
                let mut buffer = [0u8; 8192];
                loop {
                    let command = if paused {
                        commands.recv().await
                    } else {
                        tokio::select! {
                            biased;
                            command = commands.recv() => command,
                            read = server_read.read(&mut buffer) => {
                                let count = read?;
                                if count == 0 { return Ok::<_, io::Error>(()); }
                                client_write.write_all(&buffer[..count]).await?;
                                continue;
                            }
                        }
                    };
                    match command {
                        Some(Gate::Pause(done)) => {
                            paused = true;
                            let _ = done.send(());
                        }
                        Some(Gate::Resume(done)) => {
                            paused = false;
                            let _ = done.send(());
                        }
                        None => return Ok(()),
                    }
                }
            };
            // Either peer closing ends both directions, including a paused reply stream.
            tokio::select! {
                _ = tokio::io::copy(&mut client_read, &mut server_write) => {}
                _ = replies => {}
            }
        });
        Self {
            dsn: proxy_dsn,
            control,
            task,
        }
    }

    pub async fn pause(&self) {
        let (done, ready) = oneshot::channel();
        self.control.send(Gate::Pause(done)).await.unwrap();
        ready.await.unwrap();
    }

    pub async fn resume(&self) {
        let (done, ready) = oneshot::channel();
        self.control.send(Gate::Resume(done)).await.unwrap();
        ready.await.unwrap();
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
