//! The Postgres engine as the cache runs it inside an application
//! ([`crate::cache::Engine`]).
//!
//! An instance streams from a replication slot of its own, reads its
//! snapshots and hydrations over a small pool, and runs its service on a
//! single-threaded runtime on the supervisor's thread, with a small
//! runtime of its own for storage reads, so the host needs no runtime of
//! any kind. While it runs, a connection of its own samples where the
//! source's WAL is, apart from the feed, so the cache can tell how fresh
//! the engine is. When the instance ends it stops its feed and drops its
//! slot.
//!
//! Fallback reads, reader checks, catalog and type queries use exclusive
//! origin pool checkouts. With an application pool, checkouts are obtained
//! on the application's runtime. WAL sampling, replication, snapshot
//! exporters and slot lifecycle operations use the direct endpoint.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::LocalSet;

use super::pg::connections::{self, Connections, Lease, Session, SessionError};

use super::CatalogHandle;
use super::pg::threads::{self, FeedHandle, Started};
use super::pg::{Keepalive, Lsn, Settings, Transport};
use crate::cache::{Column, Engine, EngineError, Instance, Rows, SendFuture, Serve, TypeOid};
use crate::log::log_warn;
use crate::model::Position;
use crate::parser::Bind;
use crate::shutdown::Shutdown;
use crate::stats::Stats;

/// How long dropping a stopped instance's slot may take.
const SLOT_DROP_GRACE: Duration = Duration::from_secs(5);
/// How long the storage reads' runtime may take to end.
const READS_STOP_GRACE: Duration = Duration::from_secs(5);
/// How often the source's WAL position is sampled.
const SAMPLE_EVERY: Duration = Duration::from_millis(200);
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(2);

/// Where the engine streams from and how it reads.
#[derive(Clone)]
pub struct PgConfig {
    /// The database: its replication feed, its snapshots, and the origin
    /// reads.
    pub dsn: String,
    /// The publication the feed streams.
    pub publication: String,
    /// The schemas whose tables the engine carries.
    pub schemas: Vec<String>,
    /// The application id and shard naming the schema-change trigger
    /// stack ([`PgConfig::setup_sql`]).
    pub app: String,
    pub shard: u32,
    /// How many connections the engine's storage reads over.
    pub read_connections: usize,
    /// How long one storage read may take.
    pub read_timeout: Duration,
    /// How often the engine takes a fresh read snapshot.
    pub snapshot_rotation: Duration,
    /// Replication slots of this kind left inactive this long are dropped
    /// when an instance starts (PostgreSQL 17 and later); zero keeps them.
    pub slot_cleanup_age: Duration,
}

impl std::fmt::Debug for PgConfig {
    /// Every field, the DSN without its password.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgConfig")
            .field("dsn", &redacted(&self.dsn))
            .field("publication", &self.publication)
            .field("schemas", &self.schemas)
            .field("app", &self.app)
            .field("shard", &self.shard)
            .field("read_connections", &self.read_connections)
            .field("read_timeout", &self.read_timeout)
            .field("snapshot_rotation", &self.snapshot_rotation)
            .field("slot_cleanup_age", &self.slot_cleanup_age)
            .finish()
    }
}

/// What `dsn` names, never the string itself: its user, hosts, ports and
/// database as Postgres's client parses them. A password, in whatever form
/// the DSN gives it, is never printed; neither is a DSN that does not parse.
fn redacted(dsn: &str) -> String {
    let Ok(config) = dsn.parse::<tokio_postgres::Config>() else {
        return "<a DSN that does not parse>".to_owned();
    };
    let hosts: Vec<String> = config
        .get_hosts()
        .iter()
        .map(|host| match host {
            tokio_postgres::config::Host::Tcp(name) => name.clone(),
            #[cfg(unix)]
            tokio_postgres::config::Host::Unix(path) => path.display().to_string(),
        })
        .collect();
    let ports: Vec<String> = config.get_ports().iter().map(u16::to_string).collect();
    format!(
        "user={} host={} port={} dbname={}{}",
        config.get_user().unwrap_or("-"),
        hosts.join(","),
        ports.join(","),
        config.get_dbname().unwrap_or("-"),
        if config.get_password().is_some() {
            " password=***"
        } else {
            ""
        }
    )
}

impl PgConfig {
    /// The defaults for the database at `dsn`, streaming publication
    /// `streamgres` of schema `public`.
    pub fn new(dsn: impl Into<String>) -> Self {
        PgConfig {
            dsn: dsn.into(),
            publication: "streamgres".to_owned(),
            schemas: vec!["public".to_owned()],
            app: "streamgres".to_owned(),
            shard: 0,
            read_connections: 4,
            read_timeout: Duration::from_secs(10),
            snapshot_rotation: Duration::from_secs(1),
            slot_cleanup_age: Duration::ZERO,
        }
    }

    /// The SQL that installs the schema-change trigger stack the engine
    /// needs, for an administrator to run once.
    pub fn setup_sql(&self) -> String {
        super::pg::ddl_triggers::trigger_stack_sql(&self.app, self.shard, &[&self.publication])
    }

    /// The event trigger the engine hears schema changes through.
    fn ddl_trigger(&self) -> String {
        format!("{}_ddl_end_{}", self.app, self.shard)
    }

    fn settings(&self, slot: String) -> Settings {
        Settings {
            dsn: self.dsn.clone(),
            read_dsn: self.dsn.clone(),
            slot,
            slot_cleanup_age: self.slot_cleanup_age,
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
        }
    }
}

/// The Postgres engine and its origin connections.
pub(crate) struct PgEngine {
    config: PgConfig,
    origin: Arc<Origin>,
    /// Whether storage reads run on the origin's connections (the
    /// application's) rather than the engine's own read connections, which
    /// read Postgres's text directly.
    reads_on_origin: bool,
}

impl PgEngine {
    /// The engine for `config`, its origin work run on `runtime`. Storage
    /// reads run on `connections` when `reads_on_origin`.
    pub fn new(
        config: PgConfig,
        connections: Arc<dyn Connections>,
        runtime: tokio::runtime::Handle,
        reads_on_origin: bool,
    ) -> Self {
        Self {
            origin: Arc::new(Origin {
                connections,
                runtime,
            }),
            config,
            reads_on_origin,
        }
    }

    /// Brings an instance up: checks the trigger stack, loads the catalog,
    /// starts the feed and the service, and starts sampling the source.
    async fn start(
        &self,
        reads: tokio::runtime::Handle,
        failing: Shutdown,
        slot: &str,
        cleanup: &std::cell::Cell<bool>,
    ) -> Result<(Instance, FeedHandle), EngineError> {
        let config = &self.config;
        let mut connection = self.origin.connections.get().await?;
        let present = super::pg::ddl::trigger_present(&mut connection, &config.ddl_trigger())
            .await
            .map_err(|error| format!("checking the DDL trigger: {error}"))?;
        if !present {
            return Err(EngineError::Setup(
                "the DDL trigger is missing; install Builder::setup_sql".to_owned(),
            ));
        }
        let (catalog, notes) = super::pg::load_catalog(&mut connection, &config.schemas)
            .await
            .map_err(|error| format!("reading the application catalog: {error}"))?;
        for note in notes {
            log_warn!("catalog: {note}");
        }
        connection.recycle();
        let catalog = Arc::new(CatalogHandle::new(catalog));
        let settings = config.settings(slot.to_owned());
        cleanup.set(true);
        let (started, service) = threads::start_with_connections(
            &settings,
            catalog.clone(),
            reads,
            1,
            Stats::shared(),
            failing.clone(),
            self.reads_on_origin
                .then(|| self.origin.connections.clone()),
        )
        .await?;
        let Started {
            commands,
            mut events,
            feed,
            ..
        } = started;
        let events = events
            .pop()
            .ok_or_else(|| "the engine gave no event stream".to_owned())?;
        let (samples, source) = mpsc::unbounded_channel();
        // Its own connection to the source, so a saturated pool cannot
        // starve the freshness samples and push every read to the fallback.
        let sampler: Arc<dyn Connections> = Arc::new(
            connections::OwnPool::new(&config.dsn, 1)
                .map_err(|error| format!("the sampling connection: {error}"))?,
        );
        tokio::task::spawn_local(sample_source(sampler, samples));
        let failed = Box::pin(async move {
            tokio::select! {
                () = failing.wait() => failing
                    .failure()
                    .unwrap_or_else(|| "the engine stopped".to_owned()),
                ended = service => match ended {
                    Ok(_) => "the engine's service stopped".to_owned(),
                    Err(error) => format!("the engine's service panicked: {error}"),
                },
            }
        });
        Ok((
            Instance {
                commands,
                events,
                catalog,
                source,
                failed,
                name: settings.slot.clone(),
            },
            feed,
        ))
    }
}

impl Engine for PgEngine {
    fn run(
        &self,
        stopping: &Shutdown,
        start_timeout: Duration,
        serve: Serve<'_>,
    ) -> Result<(), EngineError> {
        let engine = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("the engine runtime: {error}"))?;
        let reads = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("streamgres-reads")
            .enable_all()
            .build()
            .map_err(|error| format!("the read runtime: {error}"))?;
        let failing = Shutdown::new();
        let local = LocalSet::new();
        let slot = threads::slot_name();
        let cleanup = std::cell::Cell::new(false);
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            local.block_on(&engine, async {
                let started = tokio::select! {
                    started = self.start(reads.handle().clone(), failing.clone(), &slot, &cleanup) => started?,
                    () = tokio::time::sleep(start_timeout) => {
                        return Err(EngineError::Retry(format!("the engine did not start within {start_timeout:?}")));
                    }
                    () = stopping.wait() => return Ok(()),
                };
                let (instance, feed) = started;
                let outcome = serve(instance).await;
                failing.request();
                if let Err(error) = threads::stop_feed(feed).await {
                    log_warn!("stopping the embedded engine's feed: {error}");
                }
                outcome.map_err(EngineError::from)
            })
        }))
        .unwrap_or_else(|_| Err(EngineError::Retry("the engine panicked".to_owned())));
        failing.request();
        // Cancels the instance's tasks even when its future unwound.
        drop(local);
        if cleanup.get() {
            engine.block_on(async {
                match tokio::time::timeout(
                    SLOT_DROP_GRACE,
                    Transport::drop_slot_only(&self.config.dsn, &slot),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => log_warn!("dropping slot {slot}: {error}"),
                    Err(_) => log_warn!("dropping slot {slot} timed out"),
                }
            });
        }
        reads.shutdown_timeout(READS_STOP_GRACE);
        outcome
    }

    fn read_origin<'a>(
        &'a self,
        sql: &'a str,
        binds: &'a [Bind],
        timeout: Duration,
        reuse_session: bool,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> SendFuture<'a, Result<Rows, SessionError>> {
        if !is_read_statement(sql) {
            return Box::pin(async {
                Err(SessionError::Other(
                    "fallback accepts read statements only".to_owned(),
                ))
            });
        }
        let origin = self.origin.clone();
        let sql = sql.to_owned();
        let binds = binds.to_vec();
        Box::pin(async move {
            let started = Instant::now();
            // Keep this permit until the whole database task has returned,
            // even if the application drops its awaiter.
            origin
                .run_with_session_reuse(timeout, reuse_session, move |client| {
                    Box::pin(async move {
                        let _permit = permit;
                        let left = timeout.saturating_sub(started.elapsed());
                        if left.is_zero() {
                            return Err(SessionError::Timeout(
                                "the fallback deadline expired".to_owned(),
                            ));
                        }
                        client
                            .fallback_read(&sql, &binds, left, reuse_session)
                            .await
                            .map(|result| result.map(encode_rows))
                    })
                })
                .await?
        })
    }

    fn position_text(&self, position: Position) -> String {
        super::pg::lsn::text(position)
    }

    fn origin_catalog(&self) -> SendFuture<'_, Result<Arc<crate::model::Catalog>, String>> {
        let origin = self.origin.clone();
        let schemas = self.config.schemas.clone();
        Box::pin(async move {
            origin
                .run(Duration::from_secs(10), move |client| {
                    Box::pin(async move {
                        super::pg::load_catalog(client, &schemas)
                            .await
                            .map(|(catalog, _)| Arc::new(catalog))
                            .map_err(|error| format!("reading admission catalog: {error}"))
                    })
                })
                .await
        })
    }

    fn origin_types(&self, timeout: Duration) -> SendFuture<'_, Result<Vec<TypeOid>, String>> {
        let origin = self.origin.clone();
        Box::pin(async move {
            origin
                .run(timeout, move |client| {
                    Box::pin(async move {
                        let rows = client
                            .query(
                                "SELECT n.nspname::text, t.typname::text, t.oid::int8, t.typarray::int8, pg_catalog.pg_type_is_visible(t.oid)
                                 FROM pg_catalog.pg_type t
                                 JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                                 WHERE t.typtype IN ('e', 'd', 'c', 'b', 'r', 'm')
                                   AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                                   AND n.nspname NOT LIKE 'pg_toast%'
                                   AND t.typelem = 0",
                                &[],
                            )
                            .await
                            .map_err(|error| format!("reading the origin's types: {error}"))?;
                        let oid = |row: usize, column: usize| -> Result<u32, String> {
                            Ok(rows
                                .int(row, column)?
                                .and_then(|oid| u32::try_from(oid).ok())
                                .unwrap_or(0))
                        };
                        (0..rows.rows.len())
                            .map(|row| {
                                Ok(TypeOid {
                                    schema: rows.text(row, 0)?.unwrap_or_default(),
                                    name: rows.text(row, 1)?.unwrap_or_default(),
                                    oid: oid(row, 2)?,
                                    array_oid: oid(row, 3)?,
                                    visible: rows.bool(row, 4)?.unwrap_or(false),
                                })
                            })
                            .collect()
                    })
                })
                .await
        })
    }

    fn check_origin<'a>(
        &'a self,
        sql: &'a str,
        params: [Vec<String>; 3],
    ) -> SendFuture<'a, Result<Vec<String>, String>> {
        let origin = self.origin.clone();
        let sql = sql.to_owned();
        Box::pin(async move {
            origin
                .run(Duration::from_secs(10), move |client| {
                    Box::pin(async move {
                        let binds: Vec<Bind> = params
                            .iter()
                            .map(|values| connections::text_array(values))
                            .collect();
                        let rows = client
                            .query(&sql, &binds)
                            .await
                            .map_err(|error| format!("the reader check: {error}"))?;
                        if rows.rows.len() != 1 {
                            return Err("the reader check returned no row".to_owned());
                        }
                        (0..rows.rows[0].len())
                            .map(|column| Ok(rows.text(0, column)?.unwrap_or_default()))
                            .collect()
                    })
                })
                .await
        })
    }
}

/// The ordinary SQL connections. Work is spawned on Streamgres's runtime,
/// so caller cancellation does not release a fallback's concurrency
/// permit while its query is still running.
struct Origin {
    connections: Arc<dyn Connections>,
    runtime: tokio::runtime::Handle,
}
impl Origin {
    /// Runs `work` on a checkout within `timeout`. The deadline is a
    /// [`SessionError::Timeout`] and a failed checkout or task an `Other`;
    /// `work`'s own error is returned as it is.
    async fn run<T: Send + 'static, E: From<SessionError> + Send + 'static>(
        self: Arc<Self>,
        timeout: Duration,
        work: impl for<'c> FnOnce(&'c mut Lease) -> SendFuture<'c, Result<T, E>> + Send + 'static,
    ) -> Result<T, E> {
        self.run_with_session_reuse(timeout, true, work).await
    }

    async fn run_with_session_reuse<T: Send + 'static, E: From<SessionError> + Send + 'static>(
        self: Arc<Self>,
        timeout: Duration,
        reuse_session: bool,
        work: impl for<'c> FnOnce(&'c mut Lease) -> SendFuture<'c, Result<T, E>> + Send + 'static,
    ) -> Result<T, E> {
        let runtime = self.runtime.clone();
        runtime
            .spawn(async move {
                tokio::time::timeout(timeout, async {
                    let mut connection = self
                        .connections
                        .get()
                        .await
                        .map_err(|reason| E::from(SessionError::Other(reason)))?;
                    let outcome = work(&mut connection).await;
                    if outcome.is_ok() && reuse_session {
                        connection.recycle();
                    }
                    outcome
                })
                .await
                .unwrap_or_else(|_| {
                    Err(E::from(SessionError::Timeout(format!(
                        "the read took longer than {timeout:?}"
                    ))))
                })
            })
            .await
            .unwrap_or_else(|error| {
                Err(E::from(SessionError::Other(format!(
                    "the read task ended: {error}"
                ))))
            })
    }
}

/// Restricts fallback to statements that cannot end the protective
/// transaction. PostgreSQL enforces read-only execution inside it.
fn is_read_statement(mut sql: &str) -> bool {
    loop {
        sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace() || c == '(');
        if let Some(comment) = sql.strip_prefix("--") {
            sql = comment.find(['\n', '\r']).map_or("", |end| &comment[end..]);
        } else if let Some(comment) = sql.strip_prefix("/*") {
            let mut depth = 1usize;
            let mut rest = comment;
            while depth != 0 {
                let Some(delimiter) = rest.find(['/', '*']) else {
                    return false;
                };
                rest = &rest[delimiter..];
                if let Some(next) = rest.strip_prefix("/*") {
                    depth += 1;
                    rest = next;
                } else if let Some(next) = rest.strip_prefix("*/") {
                    depth -= 1;
                    rest = next;
                } else {
                    rest = &rest[1..];
                }
            }
            sql = rest;
        } else {
            let end = sql
                .find(|c: char| c.is_ascii_whitespace() || matches!(c, '(' | '/' | '-'))
                .unwrap_or(sql.len());
            let keyword = &sql[..end];
            return ["SELECT", "WITH", "TABLE", "VALUES", "SHOW", "EXPLAIN"]
                .iter()
                .any(|read| keyword.eq_ignore_ascii_case(read));
        }
    }
}

/// Packs a Postgres result into the public row representation.
fn encode_rows(raw: connections::RawRows) -> Rows {
    let columns: Arc<[Column]> = raw
        .columns
        .iter()
        .map(|column| Column::new(column.name.as_str(), column.type_oid))
        .collect();
    let mut buffer = Vec::new();
    let mut cells = Vec::with_capacity(raw.rows.len() * columns.len());
    for row in raw.rows {
        for cell in row {
            cells.push(cell.map(|bytes| {
                let start = buffer.len();
                buffer.extend_from_slice(&bytes);
                start..buffer.len()
            }));
        }
    }
    Rows::from_parts(columns, buffer, cells)
}

/// Samples where the source's WAL is every [`SAMPLE_EVERY`], apart from
/// the feed, with the instant before each round trip (so network delay
/// never looks fresh), until the connection fails or nobody listens.
async fn sample_source(
    connections: Arc<dyn Connections>,
    samples: mpsc::UnboundedSender<(Position, Instant)>,
) {
    let mut tick = tokio::time::interval(SAMPLE_EVERY);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        if samples.is_closed() {
            return;
        }
        let sampled = Instant::now();
        let sampled_position = tokio::time::timeout(SAMPLE_TIMEOUT, async {
            let mut connection = connections.get().await?;
            let rows = connection.query(
                "SELECT (CASE WHEN pg_catalog.pg_is_in_recovery() THEN pg_catalog.pg_last_wal_replay_lsn() ELSE pg_catalog.pg_current_wal_lsn() END)::text", &[],
            ).await?;
            let text = rows.text(0, 0)?.ok_or_else(|| "the source position is NULL".to_owned())?;
            let position = Lsn::parse(&text).map(Position::from)
                .ok_or_else(|| format!("invalid source position: {text}"))?;
            connection.recycle();
            Ok::<_, String>(position)
        }).await;
        let position = match sampled_position {
            Ok(Ok(position)) => position,
            Ok(Err(reason)) => {
                log_warn!("sampling the source: {reason}");
                continue;
            }
            Err(_) => {
                log_warn!("sampling the source timed out after {SAMPLE_TIMEOUT:?}");
                continue;
            }
        };
        if samples.send((position, sampled)).is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn fallback_classifies_read_and_control_statements() {
        for sql in ["SELECT 1", "/* outer /* nested */ tail */ SELECT 1", "-- note\rVALUES (1)"] {
            assert!(super::is_read_statement(sql));
        }
        for sql in ["BEGIN", "SET search_path = public", "ROLLBACK", "/* unfinished"] {
            assert!(!super::is_read_statement(sql));
        }
        let shown = format!("{:?}", crate::cache::Sql::new("SELECT 'private_literal'").bind("private_bind"));
        assert!(!shown.contains("private_literal") && !shown.contains("private_bind"));
    }

    /// No password survives in what `Debug` prints.
    #[test]
    fn the_dsn_is_redacted() {
        use super::redacted;
        for dsn in [
            "postgres://app:s3cret@db:5432/x",
            "postgres://app:s3%40cret@db/x",
            "postgres://db/x?password=s3cret&sslmode=require",
            "host=db password='s3cret' user=app",
            "host=db PASSWORD=s3cret user=app",
            "host=db password = s3cret user=app",
            r"host=db password='s3\'cret' user=app",
            "not a dsn s3cret",
        ] {
            let shown = redacted(dsn);
            assert!(!shown.contains("s3"), "{dsn} → {shown}");
        }
        assert_eq!(
            redacted("postgres://app:s3cret@db:5432/x"),
            "user=app host=db port=5432 dbname=x password=***"
        );
        let debug = format!("{:?}", super::PgConfig::new("postgres://app:s3cret@db/x"));
        assert!(!debug.contains("s3cret"), "{debug}");
    }

    use super::*;

    #[tokio::test]
    async fn source_sampling_retries_after_a_stalled_checkout() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let attempts = Arc::new(AtomicUsize::new(0));
        let checked = attempts.clone();
        let connections = Arc::new(move || {
            let checked = checked.clone();
            async move {
                checked.fetch_add(1, Ordering::Relaxed);
                std::future::pending::<Result<connections::Lease, String>>().await
            }
        });
        let (sender, receiver) = mpsc::unbounded_channel();
        let sampling = sample_source(connections, sender);
        tokio::pin!(sampling);
        tokio::select! {
            () = &mut sampling => panic!("sampling ended before its receiver closed"),
            () = tokio::time::sleep(SAMPLE_TIMEOUT + Duration::from_millis(500)) => {}
        }
        assert!(
            attempts.load(Ordering::Relaxed) >= 2,
            "a stalled checkout permanently stopped sampling"
        );
        drop(receiver);
    }

    /// The trigger stack is named by the application and shard.
    #[test]
    fn settings_follow_the_config() {
        let mut config = PgConfig::new("postgresql://localhost/app");
        config.app = "payments".to_owned();
        config.shard = 2;
        let settings = config.settings("slot_x".to_owned());
        assert_eq!(settings.ddl_trigger, "payments_ddl_end_2");
        assert_eq!(settings.ddl_prefix, "payments/2/ddl");
        assert!(settings.settle_reports);
        assert_eq!(settings.read_dsn, "postgresql://localhost/app");
        assert!(config.setup_sql().contains("payments"));
    }
}
