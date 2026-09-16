//! The sync server binary.
//!
//! ```bash
//! cargo run --release --bin server
//! ```
//!
//! Configured by `XYNE_SYNC_*` environment variables (see `.env.example`);
//! a `.env` file in the working directory is read first, without
//! overriding variables already set.

fn main() {
    load_dotenv(".env");
    let outcome = xyne_sync::client::Config::from_env().and_then(xyne_sync::client::serve);
    if let Err(error) = outcome {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

/// Set every `KEY=VALUE` line of `path` that is not already set; a
/// missing file is fine.
fn load_dotenv(path: &str) {
    let Ok(text) = std::fs::read_to_string(path) else {
        return;
    };
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim().trim_matches('"').trim_matches('\'');
        if std::env::var_os(key).is_none() {
            // SAFETY: called from `main` before any other thread exists.
            unsafe { std::env::set_var(key, value) };
        }
    }
}
