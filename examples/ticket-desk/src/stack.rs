//! The cache and the connections the desk uses.
//!
//! Every connection opened after [`Stack::start`] is tracked
//! ([`streamgres_diesel::install`]), so `load_cached` can answer it from
//! the cache. The writer opens its own connections and never reads
//! through the cache, so its changes reach the cache the way another
//! service's would: through the replication stream.

use std::sync::Arc;
use std::time::Duration;

use diesel::connection::SimpleConnection;
use diesel::{Connection, PgConnection};
use streamgres_diesel::{Cache, CacheConfig};

/// The publication the cache streams.
pub const PUBLICATION: &str = "demo_pub";
/// The app id the schema-change trigger stack is installed for.
pub const APP: &str = "demo";

/// The cache's settings for the database at `database_url`.
pub fn config(database_url: &str) -> CacheConfig {
    let mut config = CacheConfig::new(database_url, PUBLICATION);
    config.app = APP.to_owned();
    if let Some(entries) = env_number("DEMO_CACHE_ENTRIES") {
        config.max_entries = usize::try_from(entries).unwrap_or(usize::MAX);
    }
    config
}

/// A number from the environment.
pub fn env_number(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse().ok()
}

/// The publication and the trigger stack: what an administrator installs
/// once per database. Safe to run again.
pub fn install_database(conn: &mut PgConnection, config: &CacheConfig) -> Result<(), String> {
    conn.batch_execute(&format!(
        "DO $$ BEGIN
            CREATE PUBLICATION {PUBLICATION} FOR ALL TABLES;
         EXCEPTION WHEN duplicate_object THEN NULL; END $$;"
    ))
    .map_err(|error| format!("publication: {error}"))?;
    conn.batch_execute(&config.setup_sql())
        .map_err(|error| format!("trigger stack: {error}"))
}

/// The process's cache and the URL of the database it follows.
pub struct Stack {
    pub cache: Arc<Cache>,
    pub database_url: String,
}

impl Stack {
    /// Starts the cache and waits up to `ready` for it to catch up.
    pub fn start(database_url: &str, ready: Duration) -> Result<Stack, String> {
        Self::start_config(database_url, config(database_url), ready)
    }

    /// Starts the cache with `config` and waits up to `ready` for it to
    /// catch up.
    pub fn start_config(
        database_url: &str,
        config: CacheConfig,
        ready: Duration,
    ) -> Result<Stack, String> {
        streamgres_diesel::install().map_err(|error| format!("install: {error}"))?;
        let cache = Arc::new(Cache::start(config));
        cache
            .wait_ready(ready)
            .map_err(|refusal| format!("the cache is not ready: {refusal}"))?;
        Ok(Stack {
            cache,
            database_url: database_url.to_owned(),
        })
    }

    /// A tracked connection.
    pub fn connect(&self) -> Result<PgConnection, String> {
        PgConnection::establish(&self.database_url).map_err(|error| error.to_string())
    }
}
