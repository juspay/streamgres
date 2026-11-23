//! Runtime lifecycle: start, follow the stream, shut down, clean up on failure.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use streamlib_datasource::testing::MemorySource;
use streamlib_datasource::{
    Catalog, Changes, Column, Feed, Lifecycle, Query, Result, Row, Snapshot, Snapshots,
    SourceError, Table, Txn, ValueType,
};
use streamlib_runtime::{Config, Error, Runtime};
use tokio::sync::mpsc;

const WAIT: Duration = Duration::from_secs(10);

fn catalog() -> Catalog {
    Catalog::new([Table {
        name: "items".into(),
        columns: vec![Column {
            name: "id".into(),
            ty: ValueType::Int,
        }],
        key: vec!["id".into()],
    }])
}

fn row(id: i64) -> Row {
    Row::from([("id".into(), id.into())])
}

/// A memory source that can fail `open` or break its stream on demand, and records `close`.
struct Probe {
    mem: MemorySource,
    fail_open: bool,
    slow_open: bool,
    panic_on_advance: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    breaker: std::sync::Mutex<Option<mpsc::Sender<Result<Txn<u64>>>>>,
}

impl Probe {
    fn new(fail_open: bool) -> Self {
        Probe {
            mem: MemorySource::new(catalog()),
            fail_open,
            slow_open: false,
            panic_on_advance: Arc::default(),
            closed: Arc::default(),
            breaker: Default::default(),
        }
    }

    /// Make the stream fail with `error`, as a dropped connection would.
    async fn break_stream(&self, error: SourceError) {
        let tx = self.breaker.lock().unwrap().clone().unwrap();
        tx.send(Err(error)).await.unwrap();
    }
}

impl Snapshots for Probe {
    type Position = u64;

    fn catalog(&self) -> Arc<Catalog> {
        self.mem.catalog()
    }

    async fn select(&self, query: &Query) -> Result<Snapshot<u64>> {
        self.mem.select(query).await
    }

    fn advance(&self, upto: &u64) {
        assert!(
            !self.panic_on_advance.load(Ordering::Relaxed),
            "advance blew up"
        );
        self.mem.advance(upto)
    }

    fn floor(&self) -> u64 {
        self.mem.floor()
    }
}

impl Changes for Probe {
    type Position = u64;

    async fn stream(&self, from: Option<&u64>) -> Result<Feed<u64>> {
        let mut inner = self.mem.stream(from).await?;
        let (tx, feed) = Feed::channel(inner.start, 64);
        *self.breaker.lock().unwrap() = Some(tx.clone());
        tokio::spawn(async move {
            while let Ok(txn) = inner.next().await {
                if tx.send(Ok(txn)).await.is_err() {
                    return;
                }
            }
        });
        Ok(feed)
    }

    fn ack(&self, upto: &u64) {
        self.mem.ack(upto)
    }
}

impl Lifecycle for Probe {
    async fn open(&self) -> Result<()> {
        if self.slow_open {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
        if self.fail_open {
            return Err(SourceError::Refused("can't open".into()));
        }
        Ok(())
    }

    async fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn follows_the_stream_and_shuts_down() {
    let rt = Runtime::start(Config::default(), Probe::new(false))
        .await
        .unwrap();
    assert!(rt.is_running());
    let mut last = 0;
    for id in 0..100 {
        last = rt.source().mem.insert("items", row(id)).await;
    }
    rt.wait_applied(&last, WAIT).await.unwrap();
    // The engine's advance lets reads see every write.
    let snap = rt.source().select(&Query::table("items")).await.unwrap();
    assert_eq!((snap.position, snap.rows.len()), (last, 100));

    let clone = rt.clone();
    rt.shutdown(WAIT).await;
    clone.shutdown(WAIT).await; // second call is a no-op
    assert!(!rt.is_running());
    assert!(rt.source().closed.load(Ordering::Relaxed));
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_open_returns_the_error_and_closes() {
    let probe = Probe::new(true);
    let closed = probe.closed.clone();
    let result = Runtime::start(Config::default(), probe).await;
    assert!(matches!(
        result.err(),
        Some(Error::Source(SourceError::Refused(_)))
    ));
    assert!(
        closed.load(Ordering::Relaxed),
        "close runs after a failed open"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn permanent_stream_failure_stops_the_engine_and_is_reported() {
    let rt = Runtime::start(Config::default(), Probe::new(false))
        .await
        .unwrap();
    rt.source()
        .break_stream(SourceError::Refused("permanent".into()))
        .await;
    for _ in 0..100 {
        if rt.failure().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(matches!(
        rt.failure(),
        Some(Error::Source(SourceError::Refused(_)))
    ));
    assert!(!rt.is_running());
    rt.shutdown(WAIT).await;
    assert!(rt.source().closed.load(Ordering::Relaxed));
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_without_shutdown_does_not_panic() {
    let rt = Runtime::start(Config::default(), Probe::new(false))
        .await
        .unwrap();
    rt.source().mem.insert("items", row(1)).await;
    drop(rt);
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn wait_applied_times_out_when_nothing_arrives() {
    let rt = Runtime::start(Config::default(), Probe::new(false))
        .await
        .unwrap();
    let never = rt.applied() + 1;
    let waited = rt.wait_applied(&never, Duration::from_millis(100)).await;
    assert!(matches!(
        waited,
        Err(Error::Source(SourceError::Transient(_)))
    ));
    rt.shutdown(WAIT).await;
}

mod postgres {
    use super::*;
    use streamlib_datasource_postgres::{Lsn, PgConfig, PgSource};

    /// Start on a real Postgres, follow writes, and leave no slot behind after shutdown.
    #[tokio::test(flavor = "multi_thread")]
    async fn runs_on_postgres_and_cleans_up() {
        let Ok(dsn) = std::env::var("STREAMLIB_PG_DSN") else {
            eprintln!("skipped: STREAMLIB_PG_DSN not set");
            return;
        };
        let tag = uuid::Uuid::new_v4().simple().to_string()[..8].to_owned();
        let (table, publication) = (format!("public.sl_items_{tag}"), format!("sl_pub_{tag}"));
        let (sql, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(conn);
        sql.batch_execute(&format!("CREATE TABLE {table} (id int PRIMARY KEY)"))
            .await
            .unwrap();

        let mut pg = PgConfig::new(&dsn, &publication);
        pg.tables = vec![table.clone()];
        pg.slot_prefix = format!("sl_{tag}");
        let cfg = Config {
            setup: true,
            ..Config::default()
        };
        let rt = Runtime::start(cfg, PgSource::new(pg).unwrap())
            .await
            .unwrap();
        sql.batch_execute(&format!("INSERT INTO {table} VALUES (1), (2)"))
            .await
            .unwrap();
        let lsn: String = sql
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await
            .unwrap()
            .get(0);
        rt.wait_applied(&Lsn::parse(&lsn).unwrap(), WAIT)
            .await
            .unwrap();

        let slot = rt.source().slot().unwrap();
        rt.shutdown(WAIT).await;
        let left: i64 = sql
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE starts_with(slot_name::text, $1)",
                &[&format!("sl_{tag}")],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(left, 0, "feed slot {slot} and snapshot slots removed");
        sql.batch_execute(&format!(
            "DROP PUBLICATION {publication}; DROP TABLE {table}"
        ))
        .await
        .unwrap();
    }
}

// Edge cases found in review: each fails without its fix.

async fn eventually(check: impl Fn() -> bool) -> bool {
    for _ in 0..200 {
        if check() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}

/// Cancelling `start` (for example under a timeout) must not panic or leak.
#[tokio::test(flavor = "multi_thread")]
async fn cancelled_start_does_not_panic() {
    let mut probe = Probe::new(false);
    probe.slow_open = true;
    let started = tokio::time::timeout(
        Duration::from_millis(100),
        Runtime::start(Config::default(), probe),
    )
    .await;
    assert!(started.is_err(), "start was cancelled");
}

/// A dropped connection resumes from the last applied position instead of stopping.
#[tokio::test(flavor = "multi_thread")]
async fn transient_stream_loss_resumes() {
    let rt = Runtime::start(Config::default(), Probe::new(false))
        .await
        .unwrap();
    let first = rt.source().mem.insert("items", row(1)).await;
    rt.wait_applied(&first, WAIT).await.unwrap();
    rt.source()
        .break_stream(SourceError::Transient("connection lost".into()))
        .await;
    assert!(eventually(|| rt.reconnects() > 0).await, "reconnected");
    let last = rt.source().mem.insert("items", row(2)).await;
    rt.wait_applied(&last, WAIT).await.unwrap();
    assert!(rt.is_running() && rt.failure().is_none());
    rt.shutdown(WAIT).await;
}

/// If the source can't resume (history gone), the engine stops and says why.
#[tokio::test(flavor = "multi_thread")]
async fn unresumable_stream_is_a_failure() {
    let rt = Runtime::start(Config::default(), Probe::new(false))
        .await
        .unwrap();
    // Write without the engine seeing it, so resuming from `applied` is impossible for memory.
    rt.source()
        .break_stream(SourceError::Transient("connection lost".into()))
        .await;
    rt.source().mem.insert("items", row(1)).await;
    assert!(eventually(|| rt.failure().is_some()).await);
    assert!(matches!(
        rt.failure(),
        Some(Error::Source(SourceError::CannotResume(_)))
    ));
    rt.shutdown(WAIT).await;
}

/// An engine panic is reported, not silently swallowed.
#[tokio::test(flavor = "multi_thread")]
async fn engine_panic_is_reported() {
    let probe = Probe::new(false);
    let boom = probe.panic_on_advance.clone();
    let rt = Runtime::start(Config::default(), probe).await.unwrap();
    boom.store(true, Ordering::Relaxed);
    rt.source().mem.insert("items", row(1)).await;
    assert!(eventually(|| !rt.is_running()).await, "panic noticed");
    assert!(matches!(rt.failure(), Some(Error::Engine(_))));
    boom.store(false, Ordering::Relaxed);
    rt.shutdown(WAIT).await;
}

/// Every concurrent `shutdown` returns only once the source is closed.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_shutdowns_all_wait() {
    let rt = Runtime::start(Config::default(), Probe::new(false))
        .await
        .unwrap();
    let closed = rt.source().closed.clone();
    let tasks: Vec<_> = (0..4)
        .map(|_| {
            let (rt, closed) = (rt.clone(), closed.clone());
            tokio::spawn(async move {
                rt.shutdown(WAIT).await;
                closed.load(Ordering::Relaxed)
            })
        })
        .collect();
    for t in tasks {
        assert!(t.await.unwrap(), "returned before the source was closed");
    }
}

/// Waiting on a stopped engine fails fast instead of waiting out the timeout.
#[tokio::test(flavor = "multi_thread")]
async fn wait_applied_fails_fast_once_stopped() {
    let rt = Runtime::start(Config::default(), Probe::new(false))
        .await
        .unwrap();
    rt.source()
        .break_stream(SourceError::Refused("permanent".into()))
        .await;
    let start = std::time::Instant::now();
    let waited = rt.wait_applied(&(rt.applied() + 1), WAIT).await;
    assert!(waited.is_err());
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "didn't wait out the timeout"
    );
    rt.shutdown(WAIT).await;
}

/// Dropping the last handle without shutdown still closes the source.
#[tokio::test(flavor = "multi_thread")]
async fn drop_without_shutdown_closes_the_source() {
    let rt = Runtime::start(Config::default(), Probe::new(false))
        .await
        .unwrap();
    let closed = rt.source().closed.clone();
    drop(rt);
    assert!(eventually(|| closed.load(Ordering::Relaxed)).await);
}

/// Postgres resilience: connection loss and drop without shutdown.
mod postgres_resilience {
    use super::*;
    use streamlib_datasource_postgres::{Lsn, PgConfig, PgSource};

    async fn setup_db(tag: &str) -> Option<(String, tokio_postgres::Client, String, String)> {
        let dsn = std::env::var("STREAMLIB_PG_DSN").ok()?;
        let (table, publication) = (format!("public.sl_items_{tag}"), format!("sl_pub_{tag}"));
        let (sql, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(conn);
        sql.batch_execute(&format!("CREATE TABLE {table} (id int PRIMARY KEY)"))
            .await
            .unwrap();
        Some((dsn, sql, table, publication))
    }

    fn pg_config(dsn: &str, table: &str, publication: &str, tag: &str) -> PgConfig {
        let mut pg = PgConfig::new(dsn, publication);
        pg.tables = vec![table.to_owned()];
        pg.slot_prefix = format!("sl_{tag}");
        pg
    }

    async fn current_lsn(sql: &tokio_postgres::Client) -> Lsn {
        let lsn: String = sql
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await
            .unwrap()
            .get(0);
        Lsn::parse(&lsn).unwrap()
    }

    async fn slots_left(sql: &tokio_postgres::Client, tag: &str) -> i64 {
        sql.query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE starts_with(slot_name::text, $1)",
            &[&format!("sl_{tag}")],
        )
        .await
        .unwrap()
        .get(0)
    }

    /// Killing the replication connection resumes the stream; no change is lost.
    #[tokio::test(flavor = "multi_thread")]
    async fn resumes_after_the_replication_connection_dies() {
        let tag = uuid::Uuid::new_v4().simple().to_string()[..8].to_owned();
        let Some((dsn, sql, table, publication)) = setup_db(&tag).await else {
            eprintln!("skipped: STREAMLIB_PG_DSN not set");
            return;
        };
        let cfg = Config {
            setup: true,
            ..Config::default()
        };
        let rt = Runtime::start(
            cfg,
            PgSource::new(pg_config(&dsn, &table, &publication, &tag)).unwrap(),
        )
        .await
        .unwrap();
        sql.batch_execute(&format!("INSERT INTO {table} VALUES (1)"))
            .await
            .unwrap();
        rt.wait_applied(&current_lsn(&sql).await, WAIT)
            .await
            .unwrap();

        let slot = rt.source().slot().unwrap();
        sql.query(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap();
        // Written while the stream is down; must still arrive after the resume.
        sql.batch_execute(&format!("INSERT INTO {table} VALUES (2)"))
            .await
            .unwrap();
        rt.wait_applied(&current_lsn(&sql).await, Duration::from_secs(30))
            .await
            .unwrap();
        assert!(rt.reconnects() >= 1, "the stream was resumed");
        assert!(rt.is_running(), "{:?}", rt.failure());

        rt.shutdown(WAIT).await;
        assert_eq!(slots_left(&sql, &tag).await, 0);
        sql.batch_execute(&format!(
            "DROP PUBLICATION {publication}; DROP TABLE {table}"
        ))
        .await
        .unwrap();
    }

    /// Dropping the runtime without shutdown still removes the feed slot.
    #[tokio::test(flavor = "multi_thread")]
    async fn drop_without_shutdown_removes_the_slot() {
        let tag = uuid::Uuid::new_v4().simple().to_string()[..8].to_owned();
        let Some((dsn, sql, table, publication)) = setup_db(&tag).await else {
            eprintln!("skipped: STREAMLIB_PG_DSN not set");
            return;
        };
        let cfg = Config {
            setup: true,
            ..Config::default()
        };
        let rt = Runtime::start(
            cfg,
            PgSource::new(pg_config(&dsn, &table, &publication, &tag)).unwrap(),
        )
        .await
        .unwrap();
        assert!(slots_left(&sql, &tag).await >= 1);
        drop(rt);
        let mut left = 1;
        for _ in 0..100 {
            left = slots_left(&sql, &tag).await;
            if left == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(left, 0, "cleanup removed every slot");
        sql.batch_execute(&format!(
            "DROP PUBLICATION {publication}; DROP TABLE {table}"
        ))
        .await
        .unwrap();
    }
}
