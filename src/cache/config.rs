//! How an embedded engine reaches its database and how much it keeps.

use std::time::Duration;

use crate::sync::pg::{Keepalive, Settings};

/// What [`super::Cache::start`] needs.
///
/// - `dsn`: the database, reached directly (the feed's replication
///   connection and the slot's lock need a session of their own, never a
///   pooled one).
/// - `read_dsn`: where snapshot reads go, when not `dsn`.
/// - `publication`: the publication the feed streams; it must exist.
/// - `schemas`: the schemas whose tables the engine carries.
/// - `app`, `shard`: the schema-change trigger stack the engine hears
///   changes through, as [`crate::sync::pg::ddl_triggers`] installs it.
/// - `max_entries`: how many queries are kept current at once; the one
///   read longest ago gives way to a new one.
/// - `idle_ttl`: how long a query nobody reads is kept current.
/// - `fill_timeout`: how long a read waits for a new query's rows.
/// - `max_lag`: how long the engine may go without hearing the feed
///   before reads are refused as behind.
/// - `refusal_ttl`: how long a refused query is answered as refused
///   without asking the engine again.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub dsn: String,
    pub read_dsn: Option<String>,
    pub publication: String,
    pub schemas: Vec<String>,
    pub app: String,
    pub shard: u32,
    pub max_entries: usize,
    pub idle_ttl: Duration,
    pub fill_timeout: Duration,
    pub max_lag: Duration,
    pub refusal_ttl: Duration,
    pub read_connections: usize,
    pub read_timeout: Duration,
    pub snapshot_rotation: Duration,
}

impl CacheConfig {
    /// The defaults for a database at `dsn` streaming `publication`,
    /// carrying the `public` schema.
    pub fn new(dsn: impl Into<String>, publication: impl Into<String>) -> Self {
        CacheConfig {
            dsn: dsn.into(),
            read_dsn: None,
            publication: publication.into(),
            schemas: vec!["public".to_owned()],
            app: "streamgres".to_owned(),
            shard: 0,
            max_entries: 10_000,
            idle_ttl: Duration::from_secs(300),
            fill_timeout: Duration::from_secs(5),
            max_lag: Duration::from_secs(2),
            refusal_ttl: Duration::from_secs(60),
            read_connections: 4,
            read_timeout: Duration::from_secs(10),
            snapshot_rotation: Duration::from_secs(1),
        }
    }

    /// The SQL that installs the schema-change trigger stack this engine
    /// hears changes through, for its publication; event triggers need a
    /// superuser, so an administrator runs it once per database.
    pub fn setup_sql(&self) -> String {
        crate::sync::pg::ddl_triggers::trigger_stack_sql(
            &self.app,
            self.shard,
            &[&self.publication],
        )
    }

    /// The SQL that removes what [`CacheConfig::setup_sql`] installs.
    pub fn teardown_sql(&self) -> String {
        crate::sync::pg::ddl_triggers::teardown_sql(&self.app, self.shard)
    }

    /// The event trigger schema changes are heard through.
    pub fn ddl_trigger(&self) -> String {
        format!("{}_ddl_end_{}", self.app, self.shard)
    }

    /// The engine side's settings, streaming from `slot`.
    pub(super) fn settings(&self, slot: String) -> Settings {
        Settings {
            dsn: self.dsn.clone(),
            read_dsn: self.read_dsn.clone().unwrap_or_else(|| self.dsn.clone()),
            slot,
            publication: self.publication.clone(),
            schemas: self.schemas.clone(),
            snapshot_rotation: self.snapshot_rotation,
            read_connections: self.read_connections,
            read_idle_connections: self.read_connections,
            read_timeout: self.read_timeout,
            read_threads: 2,
            keepalive: Keepalive {
                idle: Duration::from_secs(30),
                interval: Duration::from_secs(10),
                retries: 3,
            },
            watched: Vec::new(),
            ddl_trigger: self.ddl_trigger(),
            ddl_prefix: format!("{}/{}/ddl", self.app, self.shard),
        }
    }
}
