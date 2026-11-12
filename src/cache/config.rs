//! How an embedded engine reaches its database and how much it keeps.

use std::time::Duration;

use crate::sync::pg::{Keepalive, Settings};

/// What [`super::Cache::start`] needs. Start from [`CacheConfig::new`]
/// and set what differs.
///
/// Where the database is:
/// - `dsn`: the database, reached directly (the feed's replication
///   connection and the slot's lock need a session of their own, never a
///   pooled one).
/// - `read_dsn`: where snapshot reads go, when not `dsn`.
/// - `publication`: the publication the feed streams; it must exist.
/// - `schemas`: the schemas whose tables the engine carries.
/// - `app`, `shard`: the schema-change trigger stack the engine hears
///   changes through, as [`CacheConfig::setup_sql`] installs it.
///
/// How much is kept:
/// - `max_entries`: how many queries are kept current at once; the ones
///   read longest ago give way to a new one.
/// - `max_bytes`: what the kept rows may weigh together, by the cache's
///   own estimate of their heap; past it the queries read longest ago
///   give way.
/// - `max_entry_bytes`: what one query's rows may weigh; a heavier query
///   is refused.
/// - `max_rows_per_query`: the most rows one table of a query may
///   contribute; a query past it is refused.
/// - `idle_ttl`: how long a query nobody reads is kept current.
/// - `admit_after`: how many reads of a query it takes before it is kept
///   current; the reads before that are refused. 1 keeps every query on
///   its first read. Raise it where most queries are read once (a lookup
///   by an id that rarely repeats), so that only the repeated ones cost a
///   subscription.
///
/// When a read is refused:
/// - `fill_timeout`: how long a read waits for a new query's rows.
/// - `max_lag`: how stale an answer may be: the engine must have heard
///   the database within it and be no further behind the database's
///   commits than it.
/// - `refusal_ttl`: how long a query the engine refused is answered as
///   refused without asking the engine again.
///
/// How the engine runs:
/// - `start_timeout`: how long an engine may take to start before the
///   attempt is given up and tried again.
/// - `read_connections`: how many snapshot reads may run at once.
/// - `read_timeout`: how long one snapshot read may take.
/// - `snapshot_rotation`: how often the engine's reads move to a fresh
///   snapshot.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CacheConfig {
    pub dsn: String,
    pub read_dsn: Option<String>,
    pub publication: String,
    pub schemas: Vec<String>,
    pub app: String,
    pub shard: u32,
    pub max_entries: usize,
    pub max_bytes: usize,
    pub max_entry_bytes: usize,
    pub max_rows_per_query: usize,
    pub idle_ttl: Duration,
    pub admit_after: u32,
    pub fill_timeout: Duration,
    pub max_lag: Duration,
    pub refusal_ttl: Duration,
    pub start_timeout: Duration,
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
            max_bytes: 256 << 20,
            max_entry_bytes: 32 << 20,
            max_rows_per_query: 100_000,
            idle_ttl: Duration::from_secs(300),
            admit_after: 1,
            fill_timeout: Duration::from_secs(5),
            max_lag: Duration::from_secs(2),
            refusal_ttl: Duration::from_secs(60),
            start_timeout: Duration::from_secs(30),
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

    /// The database `dsn` names: its `dbname`, else its user's name, as
    /// Postgres defaults it.
    pub fn database(&self) -> String {
        let parsed: Option<tokio_postgres::Config> = self.dsn.parse().ok();
        parsed
            .and_then(|config| {
                config
                    .get_dbname()
                    .or_else(|| config.get_user())
                    .map(str::to_owned)
            })
            .unwrap_or_default()
    }

    /// The schema the trigger stack lives in.
    pub(super) fn stack_schema(&self) -> String {
        crate::sync::pg::ddl_triggers::stack_schema(&self.app, self.shard)
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
            settle_reports: true,
            row_limit: self.max_rows_per_query,
            memory_tables: Vec::new(),
        }
    }
}
