//! The server's configuration, read from the environment: where to
//! listen, which database the engine side follows, where the application
//! server answers query and mutation requests, and the timings of the
//! connection protocol. Every setting has a `XYNE_SYNC_` name; the
//! database and application-server URLs also accept the `ZERO_` names a
//! zero-cache deployment already sets, so one `.env` serves both.

use std::time::Duration;

use super::plan::{Policy, Side};
use crate::log::Level;
use crate::model::TableName;
use crate::sync::pg::Settings;

/// Everything the server reads from the environment.
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
    /// `XYNE_SYNC_READ_CONNECTIONS`: how many storage reads may hold a
    /// Postgres connection at once; the rest queue (16).
    pub read_connections: usize,
    /// `XYNE_SYNC_READ_THREADS`: how many threads run the storage reads,
    /// their connections and their row decoding (2).
    pub read_threads: usize,
    /// `XYNE_SYNC_GROUP_THREADS`: how many threads keep the client groups'
    /// views and build their pokes, each owning a share of the groups (1).
    pub group_threads: usize,
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
    /// `XYNE_SYNC_JOIN_LIMIT`: the most rows a join may read whole into
    /// memory on either side (100000). A side over it is driven from the
    /// other side when that one fits, and the query is refused when
    /// neither does; `0` turns the check off.
    pub join_limit: u64,
    /// `XYNE_SYNC_JOIN_PREFERRED_SIDE`: which side of an INNER join drives
    /// it when both fit, `parent` (the default: the parent is the query's
    /// own key-filtered rows and the child is usually an access rule over
    /// a whole table, so driving from the parent reads the child narrowed
    /// to the parent's join values instead of whole) or `child` (the
    /// subquery drives, Zero's `whereExists` as translated).
    pub join_preferred_side: Side,
    /// `XYNE_SYNC_PLAN_TTL_MS`: how long a join plan is remembered before
    /// the query is counted again (60000).
    pub plan_ttl: Duration,
    /// `XYNE_SYNC_PLAN_CACHE`: how many join plans are remembered at most
    /// (10000).
    pub plan_cache: usize,
    /// `XYNE_SYNC_LOG`: `error`, `warn`, `info` or `debug` (`info`).
    pub log: Level,
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
        let join_limit = match first(&["XYNE_SYNC_JOIN_LIMIT"]) {
            Some(text) => text.parse::<u64>().map_err(|_| {
                format!("XYNE_SYNC_JOIN_LIMIT must be a number of rows, got `{text}`")
            })?,
            None => 100_000,
        };
        let join_preferred_side = match first(&["XYNE_SYNC_JOIN_PREFERRED_SIDE"]).as_deref() {
            None | Some("parent") => Side::Parent,
            Some("child") => Side::Child,
            Some(other) => {
                return Err(format!(
                    "XYNE_SYNC_JOIN_PREFERRED_SIDE must be child or parent, got `{other}`"
                ));
            }
        };
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
            read_connections: match first(&["XYNE_SYNC_READ_CONNECTIONS"]) {
                Some(text) => text.parse().map_err(|_| {
                    format!("XYNE_SYNC_READ_CONNECTIONS must be a number, got `{text}`")
                })?,
                None => 16,
            },
            read_threads: match first(&["XYNE_SYNC_READ_THREADS"]) {
                Some(text) => text
                    .parse::<usize>()
                    .ok()
                    .filter(|threads| *threads > 0)
                    .ok_or_else(|| {
                        format!("XYNE_SYNC_READ_THREADS must be a positive number, got `{text}`")
                    })?,
                None => 2,
            },
            group_threads: match first(&["XYNE_SYNC_GROUP_THREADS"]) {
                Some(text) => text
                    .parse::<usize>()
                    .ok()
                    .filter(|threads| *threads > 0)
                    .ok_or_else(|| {
                        format!("XYNE_SYNC_GROUP_THREADS must be a positive number, got `{text}`")
                    })?,
                None => 1,
            },
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
            join_limit,
            join_preferred_side,
            plan_ttl: millis("XYNE_SYNC_PLAN_TTL_MS", 60_000)?,
            plan_cache: match first(&["XYNE_SYNC_PLAN_CACHE"]) {
                Some(text) => text
                    .parse()
                    .map_err(|_| format!("XYNE_SYNC_PLAN_CACHE must be a number, got `{text}`"))?,
                None => 10_000,
            },
            log,
        })
    }

    /// The join planner's settings.
    pub fn policy(&self) -> Policy {
        Policy {
            limit: self.join_limit,
            preferred: self.join_preferred_side,
        }
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

    /// What the engine side needs: the database to follow and read, and
    /// the one table whose rows the client side reads itself (the
    /// application's mutation ids, which travel with the rows of the
    /// mutation that produced them).
    pub fn engine_settings(&self) -> Settings {
        Settings {
            dsn: self.dsn.clone(),
            slot: self.slot.clone(),
            schemas: self.schemas.clone(),
            heartbeat: self.heartbeat,
            snapshot_rotation: self.snapshot_rotation,
            read_connections: self.read_connections,
            read_threads: self.read_threads,
            watched: vec![TableName::from(self.clients_table().as_str())],
        }
    }
}
