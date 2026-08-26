//! The axum server: routes, the WebSocket upgrade, and one connection loop.
//!
//! Base setup only — enough to have a socket open and traffic flowing, so the
//! engine can be wired in on top. The connection loop currently echoes.
//!
//! ```bash
//! cargo run --bin server --features ws
//! ```

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::get;

/// The application's routes.
///
/// Returned rather than served so it can be composed — mount more routes on
/// it, or wrap it in state once there is state to hold.
pub fn router() -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws_upgrade))
}

/// Bind `address` and serve until ctrl-c.
pub async fn serve(address: &str) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("listening on http://{address}");
    println!("  GET /health   liveness check");
    println!("  GET /ws       websocket (ws://{address}/ws)");

    axum::serve(listener, router())
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            println!("\nshutting down");
        })
        .await
}

async fn health() -> &'static str {
    "ok"
}

/// The WebSocket endpoint.
///
/// Taking [`WebSocketUpgrade`] as an argument is what makes this a WebSocket
/// route: axum validates the handshake headers and rejects the request before
/// this body runs. A socket starts life as an ordinary `GET`, which is why it
/// is registered with `get(..)`.
async fn ws_upgrade(upgrade: WebSocketUpgrade) -> impl IntoResponse {
    // Replies `101 Switching Protocols` immediately; the closure runs
    // afterwards, on the upgraded connection.
    upgrade.on_upgrade(handle_socket)
}

/// One connection, start to finish.
///
/// This is the seam the engine plugs into: incoming frames become subscribe /
/// unsubscribe requests, and outgoing frames become the deltas pushed back.
/// For now it echoes, so the transport can be exercised on its own.
async fn handle_socket(mut socket: WebSocket) {
    while let Some(Ok(message)) = socket.recv().await {
        match message {
            Message::Text(text) => {
                let reply = format!("echo: {text}");
                if socket.send(Message::Text(reply.into())).await.is_err() {
                    break; // client went away mid-write
                }
            }
            Message::Close(_) => break,
            // Ping/pong are answered beneath us; binary is unused for now.
            _ => {}
        }
    }
}
