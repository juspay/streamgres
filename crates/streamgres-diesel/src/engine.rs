//! The engine side of the cache: a client of the Streamgres S2S endpoint.
//!
//! One WebSocket per process carries every subscription. A read that
//! misses asks [`EngineClient::subscribe`], which sends the query's SQL
//! and bind bytes and waits for the engine's `accepted` or `refused`; the
//! engine's `touched` frames are turned into [`Cache::touched`] calls.
//!
//! The connection lives on its own thread with its own runtime, so the
//! wrapper never needs an async context: a subscribe blocks the calling
//! thread on a channel until the reply lands or the wait runs out. When
//! the engine is unreachable every subscribe is refused at once, so an
//! engine outage costs the application nothing but its cache.
//!
//! # Reconnecting
//!
//! Dropping the socket drops every subscription the engine held, and the
//! cache it kept honest with it. On reconnect the client clears the cache
//! (a change may have landed in between) and resubscribes every key it
//! still holds, under the same ids. Keys the engine then refuses are
//! forgotten.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value as Json, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::cache::Cache;
use crate::key::QueryKey;
use crate::subscriptions::{Subscriptions, Verdict};

/// How long a subscribe waits for the engine's verdict before the read is
/// served live instead.
pub const DEFAULT_VERDICT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long to wait before trying the engine again after a lost socket.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// What the connection task is asked to send.
enum Outgoing {
    Subscribe {
        id: u64,
        sql: String,
        binds: Vec<Json>,
    },
    Unsubscribe {
        id: u64,
    },
}

struct State {
    next_id: u64,
    by_key: HashMap<QueryKey, u64>,
    by_id: HashMap<u64, QueryKey>,
    /// Subscribes waiting for a verdict, with where to deliver it.
    pending: HashMap<u64, std::sync::mpsc::SyncSender<Verdict>>,
    connected: bool,
}

struct Inner {
    url: String,
    verdict_timeout: Duration,
    outgoing: mpsc::UnboundedSender<Outgoing>,
    state: Mutex<State>,
    cache: Mutex<Option<Weak<Cache>>>,
}

/// A client of the engine's S2S endpoint, shared by every connection of a
/// process.
#[derive(Clone)]
pub struct EngineClient {
    inner: Arc<Inner>,
}

impl EngineClient {
    /// Starts the connection thread for `url` (`ws://host:port/<base>/s2s/v1`).
    /// Returns at once; the socket connects in the background and
    /// subscribes are refused until it has.
    pub fn connect(url: impl Into<String>) -> EngineClient {
        Self::connect_with_timeout(url, DEFAULT_VERDICT_TIMEOUT)
    }

    pub fn connect_with_timeout(url: impl Into<String>, verdict_timeout: Duration) -> EngineClient {
        let (outgoing, outgoing_rx) = mpsc::unbounded_channel();
        let inner = Arc::new(Inner {
            url: url.into(),
            verdict_timeout,
            outgoing,
            state: Mutex::new(State {
                next_id: 1,
                by_key: HashMap::new(),
                by_id: HashMap::new(),
                pending: HashMap::new(),
                connected: false,
            }),
            cache: Mutex::new(None),
        });
        let task_inner = Arc::clone(&inner);
        std::thread::Builder::new()
            .name("streamgres-engine-client".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("engine client runtime");
                runtime.block_on(connection_loop(task_inner, outgoing_rx));
            })
            .expect("engine client thread");
        EngineClient { inner }
    }

    /// The cache whose entries this client's `touched` frames drop. Set
    /// once the cache exists; it holds the client, so the client holds it
    /// weakly.
    pub fn attach(&self, cache: &Arc<Cache>) {
        *self.inner.cache.lock().expect("cache") = Some(Arc::downgrade(cache));
    }

    /// Whether the socket to the engine is up.
    pub fn is_connected(&self) -> bool {
        self.inner.state.lock().expect("state").connected
    }

    /// How many subscriptions the engine holds for this client.
    pub fn subscribed(&self) -> usize {
        self.inner.state.lock().expect("state").by_key.len()
    }
}

impl Subscriptions for EngineClient {
    fn subscribe(&self, key: &QueryKey) -> Verdict {
        let (reply, verdict) = std::sync::mpsc::sync_channel(1);
        let id = {
            let mut state = self.inner.state.lock().expect("state");
            if !state.connected {
                return Verdict::Refused("the engine is not connected".to_string());
            }
            if let Some(id) = state.by_key.get(key) {
                // Already subscribed (a refill after a touch): nothing to
                // ask the engine.
                let _ = id;
                return Verdict::Accepted;
            }
            let id = state.next_id;
            state.next_id += 1;
            state.by_key.insert(key.clone(), id);
            state.by_id.insert(id, key.clone());
            state.pending.insert(id, reply);
            id
        };
        if self
            .inner
            .outgoing
            .send(Outgoing::Subscribe {
                id,
                sql: key.sql.clone(),
                binds: key.binds.iter().map(bind_json).collect(),
            })
            .is_err()
        {
            self.forget(id);
            return Verdict::Refused("the engine client has stopped".to_string());
        }
        match verdict.recv_timeout(self.inner.verdict_timeout) {
            Ok(verdict) => verdict,
            Err(_) => {
                self.forget(id);
                let _ = self.inner.outgoing.send(Outgoing::Unsubscribe { id });
                Verdict::Refused("the engine did not answer in time".to_string())
            }
        }
    }

    fn unsubscribe(&self, key: &QueryKey) {
        let id = {
            let mut state = self.inner.state.lock().expect("state");
            let Some(id) = state.by_key.remove(key) else {
                return;
            };
            state.by_id.remove(&id);
            state.pending.remove(&id);
            id
        };
        let _ = self.inner.outgoing.send(Outgoing::Unsubscribe { id });
    }
}

impl EngineClient {
    fn forget(&self, id: u64) {
        let mut state = self.inner.state.lock().expect("state");
        if let Some(key) = state.by_id.remove(&id) {
            state.by_key.remove(&key);
        }
        state.pending.remove(&id);
    }
}

fn bind_json(bind: &crate::key::Bind) -> Json {
    json!({
        "oid": bind.type_oid,
        "bytes": bind.bytes.as_ref().map(|bytes| BASE64.encode(bytes)),
    })
}

/// Connects, serves, reconnects, forever.
async fn connection_loop(inner: Arc<Inner>, mut outgoing: mpsc::UnboundedReceiver<Outgoing>) {
    let mut first = true;
    loop {
        if !first {
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
        first = false;
        let socket = match tokio_tungstenite::connect_async(&inner.url).await {
            Ok((socket, _)) => socket,
            Err(_) => continue,
        };
        let (mut sink, mut stream) = socket.split();

        // Up: a reconnect resubscribes what the cache still holds, under
        // the same ids, after clearing what may have gone stale meanwhile.
        let resubscribe: Vec<(u64, QueryKey)> = {
            let mut state = inner.state.lock().expect("state");
            state.connected = true;
            state
                .by_id
                .iter()
                .map(|(id, key)| (*id, key.clone()))
                .collect()
        };
        if !resubscribe.is_empty() {
            if let Some(cache) = cache_of(&inner) {
                cache.clear();
            }
            for (id, key) in resubscribe {
                let frame = subscribe_frame(id, &key.sql, &key.binds.iter().map(bind_json).collect::<Vec<_>>());
                if sink.send(Message::Text(frame.into())).await.is_err() {
                    break;
                }
            }
        }

        loop {
            tokio::select! {
                frame = stream.next() => match frame {
                    Some(Ok(Message::Text(text))) => inbound(&inner, &text),
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) | Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => {}
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                },
                message = outgoing.recv() => match message {
                    Some(Outgoing::Subscribe { id, sql, binds }) => {
                        if sink.send(Message::Text(subscribe_frame(id, &sql, &binds).into())).await.is_err() {
                            break;
                        }
                    }
                    Some(Outgoing::Unsubscribe { id }) => {
                        let frame = json!(["unsubscribe", {"id": id}]).to_string();
                        if sink.send(Message::Text(frame.into())).await.is_err() {
                            break;
                        }
                    }
                    None => return,
                },
            }
        }

        // Down: every wait ends refused, and nothing is trusted until the
        // engine is back.
        let pending: Vec<std::sync::mpsc::SyncSender<Verdict>> = {
            let mut state = inner.state.lock().expect("state");
            state.connected = false;
            state.pending.drain().map(|(_, reply)| reply).collect()
        };
        for reply in pending {
            let _ = reply.try_send(Verdict::Refused("the engine connection dropped".to_string()));
        }
        if let Some(cache) = cache_of(&inner) {
            cache.clear();
        }
    }
}

fn subscribe_frame(id: u64, sql: &str, binds: &[Json]) -> String {
    json!(["subscribe", {"id": id, "sql": sql, "binds": binds}]).to_string()
}

fn cache_of(inner: &Inner) -> Option<Arc<Cache>> {
    inner.cache.lock().expect("cache").as_ref()?.upgrade()
}

/// One frame from the engine.
fn inbound(inner: &Inner, text: &str) {
    let Ok(frame) = serde_json::from_str::<Json>(text) else {
        return;
    };
    let Some(array) = frame.as_array() else {
        return;
    };
    let tag = array.first().and_then(Json::as_str).unwrap_or("");
    let body = array.get(1).cloned().unwrap_or(Json::Null);
    match tag {
        "accepted" | "refused" => {
            let Some(id) = body.get("id").and_then(Json::as_u64) else {
                return;
            };
            let verdict = if tag == "accepted" {
                Verdict::Accepted
            } else {
                Verdict::Refused(
                    body.get("reason")
                        .and_then(Json::as_str)
                        .unwrap_or("refused by the engine")
                        .to_string(),
                )
            };
            let reply = {
                let mut state = inner.state.lock().expect("state");
                let reply = state.pending.remove(&id);
                if let Verdict::Refused(_) = verdict
                    && let Some(key) = state.by_id.remove(&id)
                {
                    state.by_key.remove(&key);
                    // A refusal after acceptance (the engine stopped
                    // following the query): the cache must not serve it.
                    if reply.is_none()
                        && let Some(cache) = cache_of(inner)
                    {
                        cache.touched(&key);
                    }
                }
                reply
            };
            if let Some(reply) = reply {
                let _ = reply.try_send(verdict);
            }
        }
        "touched" => {
            let Some(ids) = body.get("ids").and_then(Json::as_array) else {
                return;
            };
            let keys: Vec<QueryKey> = {
                let state = inner.state.lock().expect("state");
                ids.iter()
                    .filter_map(Json::as_u64)
                    .filter_map(|id| state.by_id.get(&id).cloned())
                    .collect()
            };
            if let Some(cache) = cache_of(inner) {
                for key in &keys {
                    cache.touched(key);
                }
            }
        }
        _ => {}
    }
}
