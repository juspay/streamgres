//! The socket to an engine: opening it (a WebSocket to a remote engine,
//! channels to one in this process), the loop that carries frames each
//! way, and the backoff between attempts.

use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::Connector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;


use super::worker::{
    Greeting, Outgoing, Shared, Target, Worker, bind_json, disconnected, inbound, subscribe_frame,
};


/// A stream must have stayed open this long before a later disconnect
/// starts again with the shortest delay. A successful handshake alone
/// does not reset the budget: a server that keeps closing new connections
/// must not make every client retry once a second forever.
const STABLE_CONNECTION: Duration = Duration::from_secs(30);

/// The ceiling doubles from one second to thirty. Each wait is chosen
/// between half that ceiling and the ceiling, so clients spread their
/// attempts without either a tight retry loop or an unbounded recovery wait.
pub(super) struct Backoff {
    ceiling_ms: u64,
}

impl Backoff {
    /// The first attempt is immediate; this is the delay after its failure.
    pub(super) fn new() -> Self {
        Self { ceiling_ms: 1_000 }
    }

    /// A fresh per-attempt sample, independent across workers and processes.
    /// It only staggers retries; it is not a cryptographic secret.
    pub(super) fn next_delay(&mut self) -> Duration {
        self.with_sample(RandomState::new().hash_one(()))
    }

    /// The delay for a sample, kept separate so the bounds can be tested
    /// without sleeping or depending on the random samples a run receives.
    pub(super) fn with_sample(&mut self, sample: u64) -> Duration {
        let floor = self.ceiling_ms / 2;
        let delay = floor + sample % (self.ceiling_ms - floor + 1);
        self.ceiling_ms = (self.ceiling_ms * 2).min(30_000);
        Duration::from_millis(delay)
    }

    /// A stable connection earns a short retry after its next disconnect.
    pub(super) fn reset(&mut self) {
        self.ceiling_ms = 1_000;
    }
}

/// How long one frame may take to go out before the socket is given up
/// on: an engine that holds the socket open but stops reading would
/// otherwise block the worker, and its thread, for good.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// The upgrade request for `url`, with the bearer token when there is
/// one; else why neither can go in a request. The worker then keeps
/// retrying like any other failed connect, every read is served live,
/// and the reason is in each refusal.
pub(super) fn upgrade_request(url: &str, token: Option<&str>) -> Result<Request, String> {
    let mut request = url
        .into_client_request()
        .map_err(|error| format!("`{url}` is not an engine URL: {error}"))?;
    if let Some(token) = token {
        let value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
            "STREAMGRES_S2S_TOKEN holds a character a header cannot carry".to_string()
        })?;
        request.headers_mut().insert(AUTHORIZATION, value);
    }
    Ok(request)
}

/// The TLS a `wss://` socket opens with: rustls with the ring provider
/// and the system's root certificates, built once and given to each
/// socket rather than installed for the whole process, which belongs to
/// the application. `None` when the roots cannot be loaded; a `wss://`
/// connect then fails, and says so in the refusals. A `ws://` socket
/// ignores it.
pub(super) fn tls() -> Option<Connector> {
    static CONFIG: OnceLock<Option<Arc<rustls::ClientConfig>>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let mut roots = rustls::RootCertStore::empty();
            for certificate in rustls_native_certs::load_native_certs().certs {
                let _ = roots.add(certificate);
            }
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let config = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .ok()?
                .with_root_certificates(roots)
                .with_no_client_auth();
            Some(Arc::new(config))
        })
        .clone()
        .map(Connector::Rustls)
}

/// Whether a bearer token sent to `url` would cross the network in plain
/// text: a `ws://` URL whose host is not this machine.
pub(super) fn token_in_clear(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("ws://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = match authority.rsplit_once(':') {
        Some((host, port)) if port.bytes().all(|byte| byte.is_ascii_digit()) => host,
        _ => authority,
    };
    !matches!(host, "localhost" | "127.0.0.1" | "[::1]")
}

/// Why a connect failed, as a refusal names it.
pub(super) fn connect_failure(error: &tokio_tungstenite::tungstenite::Error) -> String {
    use tokio_tungstenite::tungstenite::Error;
    match error {
        Error::Http(response) if response.status() == 401 => {
            "the engine refused the token (check STREAMGRES_S2S_TOKEN)".to_string()
        }
        Error::Http(response) if response.status() == 404 => {
            "the engine serves no S2S endpoint at this URL (it serves one only with STREAMGRES_S2S_TOKEN set, and only on STREAMGRES_S2S_ADDR when that is set)".to_string()
        }
        Error::Http(response) if response.status() == 503 => {
            "the engine is not serving yet, or has no free connection".to_string()
        }
        Error::Http(response) => format!("the engine answered {}", response.status()),
        other => other.to_string(),
    }
}

/// The socket a remote engine is reached on.
type Socket = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

/// The sending half of a connection to an engine.
enum Sender {
    Socket(futures_util::stream::SplitSink<Socket, Message>),
    #[cfg(feature = "embedded")]
    Local(mpsc::Sender<String>),
}

/// The receiving half of a connection to an engine.
enum Receiver {
    Socket(futures_util::stream::SplitStream<Socket>),
    #[cfg(feature = "embedded")]
    Local(streamgres::embedded::Session),
}

/// What a connection brought.
enum Heard {
    /// A frame: a socket's text stays in the socket's buffer.
    Socket(tokio_tungstenite::tungstenite::Utf8Bytes),
    #[cfg(feature = "embedded")]
    Local(String),
    /// Something else the socket carries (a ping, a pong, binary).
    Other,
    /// The connection ended.
    Closed,
}

impl Heard {
    /// The frame's text, for a frame.
    fn text(&self) -> Option<&str> {
        match self {
            Heard::Socket(text) => Some(text.as_str()),
            #[cfg(feature = "embedded")]
            Heard::Local(text) => Some(text),
            Heard::Other | Heard::Closed => None,
        }
    }
}

/// Opens a connection to `worker`'s engine: a socket to a remote one, a
/// session with one in this process. Why not, when it cannot.
async fn open(worker: &Worker) -> Result<(Sender, Receiver), String> {
    match &worker.target {
        Target::Remote(url) => {
            let request = upgrade_request(url, worker.shared.token.as_deref())?;
            let (socket, _) =
                tokio_tungstenite::connect_async_tls_with_config(request, None, false, tls())
                    .await
                    .map_err(|error| connect_failure(&error))?;
            let (sink, stream) = socket.split();
            Ok((Sender::Socket(sink), Receiver::Socket(stream)))
        }
        #[cfg(feature = "embedded")]
        Target::Local(engine) => {
            let session = engine.connect()?;
            Ok((Sender::Local(session.sender()), Receiver::Local(session)))
        }
    }
}

impl Sender {
    /// Sends one frame, giving the connection up if it does not take it
    /// in time.
    async fn send(&mut self, frame: String) -> bool {
        match self {
            Sender::Socket(sink) => matches!(
                timeout(SEND_TIMEOUT, sink.send(Message::Text(frame.into()))).await,
                Ok(Ok(()))
            ),
            #[cfg(feature = "embedded")]
            Sender::Local(engine) => {
                matches!(timeout(SEND_TIMEOUT, engine.send(frame)).await, Ok(Ok(())))
            }
        }
    }

    /// Closes the connection.
    async fn close(&mut self) {
        match self {
            Sender::Socket(sink) => {
                let _ = sink.close().await;
            }
            #[cfg(feature = "embedded")]
            Sender::Local(_) => {}
        }
    }
}

impl Receiver {
    /// What the connection brings next; cancelling it loses nothing.
    async fn next(&mut self) -> Heard {
        match self {
            Receiver::Socket(stream) => match stream.next().await {
                Some(Ok(Message::Text(text))) => Heard::Socket(text),
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => Heard::Closed,
                Some(Ok(_)) => Heard::Other,
            },
            #[cfg(feature = "embedded")]
            Receiver::Local(session) => session.recv().await.map_or(Heard::Closed, Heard::Local),
        }
    }
}

/// Connects, serves until the connection breaks either way, reconnects,
/// until the pool stops: one worker's connection, a socket to a remote
/// engine or a session with one in this process, the same frames over
/// either. A quiet connection is pinged every third of the silence
/// window, and how long it has been silent is looked at every thirtieth
/// (a second at most), so it is taken for dead within that of the
/// window, not at the next ping.
pub(super) async fn connection_loop(worker: Arc<Worker>, mut outgoing: mpsc::UnboundedReceiver<Outgoing>) {
    let mut first = true;
    let mut backoff = Backoff::new();
    loop {
        if !first && !retry_wait(&worker.shared, backoff.next_delay()).await {
            return;
        }
        first = false;
        if worker.shared.stopped.load(Ordering::Acquire) {
            return;
        }
        let (mut sender, mut receiver) = match open(&worker).await {
            Ok(link) => {
                worker.connect_failed(None);
                link
            }
            Err(error) => {
                worker.connect_failed(Some(error));
                continue;
            }
        };
        let opened = Instant::now();
        let mut heard = opened;
        let silence = worker.shared.silence;
        let every = (silence / 3).max(Duration::from_millis(1));
        let mut ping = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let glance = (silence / 30).clamp(Duration::from_millis(1), Duration::from_secs(1));
        let mut check = tokio::time::interval_at(tokio::time::Instant::now() + glance, glance);
        check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut resubscribe: std::collections::VecDeque<_> = worker.up().into();
        let mut restore = tokio::time::interval(Duration::from_millis(10));
        restore.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut up = true;
        while up {
            up = tokio::select! {
                _ = restore.tick(), if !resubscribe.is_empty() => {
                    let mut sent = true;
                    for _ in 0..32 {
                        let Some((id, key)) = resubscribe.pop_front() else { break };
                        // An unsubscribe processed while restoring must not
                        // resurrect a query that no consumer holds anymore.
                        let held = worker.state.lock().expect("state").by_id.get(&id) == Some(&key);
                        if held && !sender.send(subscribe_frame(id, &key.sql, &key.binds.iter().map(bind_json).collect::<Vec<_>>())).await {
                            sent = false;
                            break;
                        }
                    }
                    sent
                },
                frame = receiver.next() => {
                    heard = Instant::now();
                    if let Some(text) = frame.text() {
                        inbound(&worker, text);
                    }
                    !matches!(frame, Heard::Closed)
                },
                _ = ping.tick() => {
                    heard.elapsed() < silence && sender.send(json!(["ping"]).to_string()).await
                },
                _ = check.tick() => heard.elapsed() < silence,
                message = outgoing.recv() => match message {
                    Some(Outgoing::Subscribe { id, sql, binds }) => {
                        sender.send(subscribe_frame(id, &sql, &binds)).await
                    }
                    Some(Outgoing::Unsubscribe { id }) => {
                        sender.send(json!(["unsubscribe", {"id": id}]).to_string()).await
                    }
                    Some(Outgoing::Stop) | None => {
                        sender.close().await;
                        return;
                    }
                },
            };
        }
        if opened.elapsed() >= STABLE_CONNECTION
            && !matches!(
                *worker.greeting.lock().expect("greeting"),
                Greeting::Waiting
            )
        {
            backoff.reset();
        }
        disconnected(&worker);
    }
}

/// Waits for the next attempt without holding the worker alive through
/// shutdown. Register the notification before checking the flag so a
/// pool dropped at that boundary cannot leave a thirty-second sleep.
pub(super) async fn retry_wait(shared: &Shared, delay: Duration) -> bool {
    let stopped = shared.stop.notified();
    tokio::pin!(stopped);
    stopped.as_mut().enable();
    if shared.stopped.load(Ordering::Acquire) {
        return false;
    }
    tokio::select! {
        _ = tokio::time::sleep(delay) => !shared.stopped.load(Ordering::Acquire),
        _ = stopped => false,
    }
}
