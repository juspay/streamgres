//! The server binary.
//!
//! ```bash
//! cargo run --bin server --features ws
//! ```

#[tokio::main]
async fn main() {
    let address = std::env::var("JUS_SYNC_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());

    if let Err(error) = jus_sync::ws::serve(&address).await {
        eprintln!("server error: {error}");
        std::process::exit(1);
    }
}
