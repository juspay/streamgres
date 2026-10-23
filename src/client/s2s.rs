//! The server-to-server endpoint: the [`crate::live`] protocol over a
//! WebSocket at `{base}/s2s/v1`, for backends that reach the engine over
//! the network.
//!
//! Admission:
//! - With `STREAMGRES_S2S_TOKEN` set, the upgrade must carry
//!   `Authorization: Bearer <token>` and is answered 401 otherwise.
//!   Unset, any connection is taken, so the port belongs on a trusted
//!   network.
//! - At most `STREAMGRES_S2S_MAX_CONNECTIONS` connections are open at
//!   once; past that the upgrade is answered 503. With each connection's
//!   own subscription limit ([`crate::live::Limits`]) it bounds what one
//!   engine follows for its backends.
//!
//! Each upgraded socket is one [`crate::live::session`]: its text frames
//! go in, and a writer task drains the session's outgoing queue onto the
//! socket.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt, future};

use super::connection::AppState;
use crate::live::{self, Outgoing, REFUSAL_LOG_EVERY, Throttle, refusal};
use crate::log::log_warn;

/// How long a closing connection's writer may take to flush what is
/// queued. A backend that stopped reading never lets it finish, and
/// waiting for it would hold the connection's place and socket forever.
const WRITER_DRAIN: Duration = Duration::from_secs(5);

/// Who may connect: the connections open now against the most allowed,
/// and the warnings for refused upgrades, throttled per kind.
#[derive(Debug)]
pub struct Admission {
    /// Connections open now, against `max_connections` (0: no limit).
    open: Arc<AtomicUsize>,
    max_connections: usize,
    unauthorized: Throttle,
    full: Throttle,
}

impl Admission {
    /// Admitting at most `max_connections` at once (0: no limit).
    pub fn new(max_connections: usize) -> Admission {
        Admission {
            open: Arc::new(AtomicUsize::new(0)),
            max_connections,
            unauthorized: Throttle::new(REFUSAL_LOG_EVERY),
            full: Throttle::new(REFUSAL_LOG_EVERY),
        }
    }

    /// A place among the open connections, if one is free; it is given
    /// back when the [`Admitted`] drops.
    fn admit(&self) -> Option<Admitted> {
        let limit = self.max_connections;
        self.open
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |open| {
                (limit == 0 || open < limit).then_some(open + 1)
            })
            .ok()?;
        Some(Admitted(Arc::clone(&self.open)))
    }
}

/// One open connection's place, held for as long as the connection is.
struct Admitted(Arc<AtomicUsize>);

impl Drop for Admitted {
    /// Gives the connection's place back.
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The route handler: upgrades the socket once the engine is serving.
pub async fn connect(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    if let Some(token) = &state.config.s2s_token
        && !authorized(&headers, &token.0)
    {
        refusal(&state.s2s.unauthorized, "no valid bearer token");
        return (StatusCode::UNAUTHORIZED, "a bearer token is required").into_response();
    }
    if !*state.ready.borrow() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "the engine is not serving yet",
        )
            .into_response();
    }
    let Some(admitted) = state.s2s.admit() else {
        let what = format!(
            "{} connections are open, the most allowed",
            state.s2s.max_connections
        );
        refusal(&state.s2s.full, &what);
        return (StatusCode::SERVICE_UNAVAILABLE, "too many S2S connections").into_response();
    };
    let upgrade = upgrade.max_message_size(state.config.max_message_bytes);
    upgrade.on_upgrade(move |socket| async move {
        handle(socket, state).await;
        drop(admitted);
    })
}

/// Whether the request carries `Authorization: Bearer <token>`. The
/// comparison walks the whole token whatever was presented, so neither
/// a near miss nor a presented length says anything about the token.
fn authorized(headers: &HeaderMap, token: &str) -> bool {
    let Some(presented) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
        .map(|(_, presented)| presented.trim_start_matches(' '))
    else {
        return false;
    };
    let (presented, token) = (presented.as_bytes(), token.as_bytes());
    let lengths = u8::from(presented.len() != token.len());
    token
        .iter()
        .enumerate()
        .fold(lengths, |differ, (at, byte)| {
            differ | (presented.get(at).copied().unwrap_or(0) ^ byte)
        })
        == 0
}

/// One upgraded socket as a session: its text frames go in until a
/// close or an error (pings, pongs and binary frames are passed over),
/// and a writer task sends the session's frames. The writer gets
/// [`WRITER_DRAIN`] to flush once the session ends and is then cut off,
/// so a backend that stopped reading cannot hold the connection open.
async fn handle(socket: WebSocket, state: Arc<AppState>) {
    let (mut sink, stream) = socket.split();
    let (out, mut out_rx) = Outgoing::new();
    let drained = out.drained();
    let mut writer = tokio::spawn(async move {
        while let Some(text) = out_rx.recv().await {
            let len = text.len();
            let sent = sink.send(Message::Text(text.into())).await;
            drained.sent(len);
            if sent.is_err() {
                break;
            }
        }
        let _ = sink.send(Message::Close(None)).await;
    });
    let inbound = stream
        .take_while(|frame| future::ready(matches!(frame, Ok(message) if !matches!(message, Message::Close(_)))))
        .filter_map(|frame| {
            future::ready(match frame {
                Ok(Message::Text(text)) => Some(text.as_str().to_owned()),
                _ => None,
            })
        });
    live::session(&state.live, Box::pin(inbound), out).await;
    if tokio::time::timeout(WRITER_DRAIN, &mut writer).await.is_err() {
        writer.abort();
        log_warn!("an s2s connection's frames were not read in time; cut off");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Places are handed out up to the limit and come back when their
    /// connections close; a zero limit hands them out without one.
    #[test]
    fn connections_are_admitted_up_to_the_limit() {
        let handle = Admission::new(2);
        let first = handle.admit().expect("a place");
        let second = handle.admit().expect("a place");
        assert!(handle.admit().is_none());
        drop(first);
        let third = handle.admit().expect("the freed place");
        assert!(handle.admit().is_none());
        drop((second, third));
        assert_eq!(handle.open.load(Ordering::Acquire), 0);

        let open = Admission::new(0);
        let places: Vec<Admitted> = (0..100).map(|_| open.admit().expect("a place")).collect();
        assert_eq!(open.open.load(Ordering::Acquire), places.len());
    }

    /// Only the configured token, as a bearer, opens the endpoint.
    #[test]
    fn only_the_bearer_token_authorizes() {
        let with = |value: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(AUTHORIZATION, value.parse().unwrap());
            headers
        };
        assert!(authorized(&with("Bearer s3cret"), "s3cret"));
        assert!(authorized(&with("bearer s3cret"), "s3cret"));
        assert!(authorized(&with("BEARER  s3cret"), "s3cret"));
        assert!(!authorized(&with("Bearer s3cres"), "s3cret"));
        assert!(!authorized(&with("Bearer s3cret2"), "s3cret"));
        assert!(!authorized(&with("Bearer s3cre"), "s3cret"));
        assert!(!authorized(&with("Bearer "), "s3cret"));
        assert!(!authorized(&with("Basic s3cret"), "s3cret"));
        assert!(!authorized(&with("s3cret"), "s3cret"));
        assert!(!authorized(&HeaderMap::new(), "s3cret"));
    }
}
