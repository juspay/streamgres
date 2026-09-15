//! The gateway's configuration, read from the environment: where to
//! listen, which database to follow, where the application server answers
//! query and mutation requests, and the timings of the connection
//! protocol. Every setting has a `XYNE_SYNC_` name; the database and
//! application-server URLs also accept the `ZERO_` names a zero-cache
//! deployment already sets, so one `.env` serves both.

use std::time::Duration;

/// Everything the gateway reads from the environment.
#[derive(Debug, Clone)]
pub struct Config {
    /// `XYNE_SYNC_ADDR`: the address to listen on (`0.0.0.0:4848`).
    pub bind: String,
    /// `XYNE_SYNC_BASE_PATH`: the path prefix the client connects under
    /// (`/zero`, so the connect route is `/zero/sync/v51/connect`).
    pub base_path: String,
    /// `XYNE_SYNC_PG_DSN` (or `ZERO_UPSTREAM_DB`): the database, with the
    /// user and password in the URL.
    pub dsn: String,
    /// `XYNE_SYNC_SCHEMAS`: the schemas whose tables the catalog carries
    /// (`public` plus the app's `zero_<shard>` schema by default).
    pub schemas: Vec<String>,
    /// `XYNE_SYNC_SLOT`: the permanent replication slot (`xyne_sync`).
    pub slot: String,
    /// `XYNE_SYNC_HEARTBEAT_MS`: how often the feed emits a heartbeat so
    /// the engine's position moves while nothing is written (1000).
    pub heartbeat: Duration,
    /// `XYNE_SYNC_SNAPSHOT_ROTATION_MS`: how often a fresh exported
    /// snapshot (a temporary replication slot) is minted for reads (1000).
    pub snapshot_rotation: Duration,
    /// `XYNE_SYNC_QUERY_URL` (or `ZERO_QUERY_URL`): the application
    /// server's query endpoint, which turns query names into ASTs.
    pub query_url: String,
    /// `XYNE_SYNC_MUTATE_URL` (or `ZERO_MUTATE_URL`): the application
    /// server's mutation endpoint.
    pub mutate_url: String,
    /// `XYNE_SYNC_FORWARD_COOKIES` (or `ZERO_QUERY_FORWARD_COOKIES`):
    /// whether the connection's cookies go along to those endpoints
    /// (true).
    pub forward_cookies: bool,
    /// `XYNE_SYNC_APP_ID` (or `ZERO_APP_ID`): Zero's application id
    /// (`zero`); with the shard it names the schema the application
    /// server records mutations in.
    pub app_id: String,
    /// `XYNE_SYNC_SHARD` (or `ZERO_SHARD_NUM`): the shard number (0).
    pub shard: u32,
    /// `XYNE_SYNC_PING_INTERVAL_MS`: how often a WebSocket ping frame goes
    /// to an idle connection (30000).
    pub ping_interval: Duration,
    /// `XYNE_SYNC_CLIENT_TIMEOUT_MS`: how long a connection may stay silent
    /// (no message, no pong) before it is closed (45000).
    pub client_timeout: Duration,
    /// `XYNE_SYNC_PONG_INTERVAL_MS`: how long the server lets a connection
    /// go without any downstream message before it sends a `pong` on its
    /// own (3000).
    pub pong_interval: Duration,
    /// `XYNE_SYNC_GROUP_TTL_MS`: how long a client group's subscriptions
    /// stay registered after its last connection closes (60000).
    pub group_ttl: Duration,
    /// `XYNE_SYNC_MAX_MESSAGE_BYTES`: the largest inbound WebSocket message
    /// accepted (16 MiB).
    pub max_message_bytes: usize,
    /// `XYNE_SYNC_ROWS_PER_PART`: how many row operations one poke part
    /// carries at most (500).
    pub rows_per_part: usize,
    /// `XYNE_SYNC_LOG`: `error`, `warn`, `info` or `debug` (`info`).
    pub log: Level,
}

/// A log level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Error,
    Warn,
    Info,
    Debug,
}

impl Config {
    /// Read the configuration; an error names the missing or malformed
    /// variable.
    pub fn from_env() -> Result<Config, String> {
        let first = |names: &[&str]| {
            names
                .iter()
                .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()))
        };
        let millis = |name: &str, default: u64| -> Result<Duration, String> {
            match first(&[name]) {
                Some(text) => text
                    .parse::<u64>()
                    .map(Duration::from_millis)
                    .map_err(|_| format!("{name} must be a number of milliseconds, got `{text}`")),
                None => Ok(Duration::from_millis(default)),
            }
        };
        let dsn = first(&["XYNE_SYNC_PG_DSN", "ZERO_UPSTREAM_DB"])
            .ok_or("XYNE_SYNC_PG_DSN (or ZERO_UPSTREAM_DB) must name the database")?;
        let query_url = first(&["XYNE_SYNC_QUERY_URL", "ZERO_QUERY_URL"])
            .ok_or("XYNE_SYNC_QUERY_URL (or ZERO_QUERY_URL) must name the query endpoint")?;
        let mutate_url = first(&["XYNE_SYNC_MUTATE_URL", "ZERO_MUTATE_URL"])
            .ok_or("XYNE_SYNC_MUTATE_URL (or ZERO_MUTATE_URL) must name the mutate endpoint")?;
        let app_id =
            first(&["XYNE_SYNC_APP_ID", "ZERO_APP_ID"]).unwrap_or_else(|| "zero".to_owned());
        let shard = match first(&["XYNE_SYNC_SHARD", "ZERO_SHARD_NUM"]) {
            Some(text) => text
                .parse::<u32>()
                .map_err(|_| format!("XYNE_SYNC_SHARD must be a number, got `{text}`"))?,
            None => 0,
        };
        let schemas = match first(&["XYNE_SYNC_SCHEMAS"]) {
            Some(text) => text
                .split(',')
                .map(|schema| schema.trim().to_owned())
                .filter(|schema| !schema.is_empty())
                .collect(),
            None => vec!["public".to_owned(), format!("{app_id}_{shard}")],
        };
        let forward_cookies = first(&["XYNE_SYNC_FORWARD_COOKIES", "ZERO_QUERY_FORWARD_COOKIES"])
            .is_none_or(|value| value != "false" && value != "0");
        let log = match first(&["XYNE_SYNC_LOG"]).as_deref() {
            None | Some("info") => Level::Info,
            Some("debug") => Level::Debug,
            Some("warn") => Level::Warn,
            Some("error") => Level::Error,
            Some(other) => {
                return Err(format!(
                    "XYNE_SYNC_LOG must be error, warn, info or debug, got `{other}`"
                ));
            }
        };
        let mut base_path = first(&["XYNE_SYNC_BASE_PATH"]).unwrap_or_else(|| "/zero".to_owned());
        if !base_path.starts_with('/') {
            base_path.insert(0, '/');
        }
        while base_path.len() > 1 && base_path.ends_with('/') {
            base_path.pop();
        }
        if base_path == "/" {
            base_path.clear();
        }
        Ok(Config {
            bind: first(&["XYNE_SYNC_ADDR"]).unwrap_or_else(|| "0.0.0.0:4848".to_owned()),
            base_path,
            dsn,
            schemas,
            slot: first(&["XYNE_SYNC_SLOT"]).unwrap_or_else(|| "xyne_sync".to_owned()),
            heartbeat: millis("XYNE_SYNC_HEARTBEAT_MS", 1_000)?,
            snapshot_rotation: millis("XYNE_SYNC_SNAPSHOT_ROTATION_MS", 1_000)?,
            query_url,
            mutate_url,
            forward_cookies,
            app_id,
            shard,
            ping_interval: millis("XYNE_SYNC_PING_INTERVAL_MS", 30_000)?,
            client_timeout: millis("XYNE_SYNC_CLIENT_TIMEOUT_MS", 45_000)?,
            pong_interval: millis("XYNE_SYNC_PONG_INTERVAL_MS", 3_000)?,
            group_ttl: millis("XYNE_SYNC_GROUP_TTL_MS", 60_000)?,
            max_message_bytes: match first(&["XYNE_SYNC_MAX_MESSAGE_BYTES"]) {
                Some(text) => text.parse().map_err(|_| {
                    format!("XYNE_SYNC_MAX_MESSAGE_BYTES must be a number, got `{text}`")
                })?,
                None => 16 * 1024 * 1024,
            },
            rows_per_part: match first(&["XYNE_SYNC_ROWS_PER_PART"]) {
                Some(text) => text.parse().map_err(|_| {
                    format!("XYNE_SYNC_ROWS_PER_PART must be a number, got `{text}`")
                })?,
                None => 500,
            },
            log,
        })
    }

    /// The schema the application server records mutations in,
    /// `<app>_<shard>` (`zero_0`).
    pub fn upstream_schema(&self) -> String {
        format!("{}_{}", self.app_id, self.shard)
    }

    /// The catalog name of the table holding each client's last mutation
    /// id, `<upstream schema>.clients`.
    pub fn clients_table(&self) -> String {
        format!("{}.clients", self.upstream_schema())
    }
}
