//! Reads from a physical standby, writes to its primary, one cache: a
//! refill is kept only once the standby has replayed past the last change
//! the engine will not report, and a write on the primary gives the reads
//! on the standby its position. The engine side is a fake that says what
//! the test sets.
//!
//! Needs `DATABASE_URL` (the primary) and `STREAMGRES_TEST_STANDBY_URL`
//! (a hot standby streaming from it); skips itself otherwise.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel_streamgres::cache::{Cache, WAL_POSITION_SQL, WalPosition};
use diesel_streamgres::key::QueryKey;
use diesel_streamgres::subscriptions::{Outcome, Subscriptions, Verdict};
use diesel_streamgres::{Route, Served, SyncedConnection};

diesel::table! {
    sg_configs (id) {
        id -> Int4,
        key -> Text,
        value -> Text,
    }
}

/// An engine side that accepts every read of `sg_configs` in touch mode,
/// naming `since` (0: none) as the last change it will not report, and
/// reporting `position` (0: none) as delivered.
#[derive(Default)]
struct FakeEngine {
    since: AtomicU64,
    position: AtomicU64,
    subscribed: Mutex<Vec<QueryKey>>,
}

impl Subscriptions for FakeEngine {
    /// Accepted for `sg_configs`, refused otherwise.
    fn subscribe(&self, key: &QueryKey) -> Outcome {
        if !key.sql.contains("sg_configs") {
            return Outcome::Verdict(Verdict::Refused("not a config read".into()));
        }
        self.subscribed.lock().unwrap().push(key.clone());
        let since = Some(self.since.load(Ordering::SeqCst)).filter(|&since| since != 0);
        Outcome::Verdict(Verdict::Accepted { since })
    }

    /// Never asked: no rows are shipped.
    fn materialize(
        &self,
        _: &QueryKey,
        _: diesel_streamgres::materialized::Snapshot,
        _: std::collections::HashMap<String, Arc<diesel_streamgres::types::TableTypes>>,
    ) -> Verdict {
        unreachable!("touch mode")
    }

    /// Nothing to end.
    fn unsubscribe(&self, _: &QueryKey) {}

    /// As set by the test.
    fn position(&self) -> Option<u64> {
        Some(self.position.load(Ordering::SeqCst)).filter(|&position| position != 0)
    }
}

/// One test's connections: a writer on the primary and a reader on the
/// standby, sharing a cache, as a backend's two pools do; a plain
/// primary connection for the test's own statements; its keys' prefix.
struct World {
    /// Replay is paused for the whole standby: one test at a time.
    _serial: std::sync::MutexGuard<'static, ()>,
    writer: SyncedConnection<PgConnection>,
    reader: SyncedConnection<PgConnection>,
    cache: Arc<Cache>,
    engine: Arc<FakeEngine>,
    admin: PgConnection,
    standby: PgConnection,
    prefix: String,
}

impl World {
    /// `None`, and the test skipped, without a standby to read.
    fn new(name: &str) -> Option<World> {
        let Ok(standby_url) = std::env::var("STREAMGRES_TEST_STANDBY_URL") else {
            eprintln!("STREAMGRES_TEST_STANDBY_URL not set; skipping");
            return None;
        };
        static SERIAL: Mutex<()> = Mutex::new(());
        let serial = SERIAL.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let primary_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
        let mut admin = PgConnection::establish(&primary_url).expect("primary");
        admin
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS sg_configs (
                    id serial PRIMARY KEY, key text NOT NULL UNIQUE, value text NOT NULL)",
            )
            .expect("schema");
        let engine = Arc::new(FakeEngine::default());
        let cache = Arc::new(Cache::new(64 * 1024 * 1024, engine.clone()));
        let mut writer = SyncedConnection::with_cache(
            PgConnection::establish(&primary_url).expect("primary"),
            Arc::clone(&cache),
            Route::Postgres,
        );
        writer.set_read_your_writes(Duration::from_secs(5));
        let mut reader = SyncedConnection::with_cache(
            PgConnection::establish(&standby_url).expect("standby"),
            Arc::clone(&cache),
            Route::Streamgres,
        );
        reader.set_read_your_writes(Duration::from_secs(5));
        let standby = PgConnection::establish(&standby_url).expect("standby");
        let prefix = format!("{name}-{}-", std::process::id());
        let mut world = World {
            _serial: serial,
            writer,
            reader,
            cache,
            engine,
            admin,
            standby,
            prefix,
        };
        world.resume();
        Some(world)
    }

    /// A key of this test's.
    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }

    /// Sets a key on the primary, through no cache.
    fn put(&mut self, name: &str, value: &str) {
        let key = self.key(name);
        diesel::insert_into(sg_configs::table)
            .values((sg_configs::key.eq(&key), sg_configs::value.eq(value)))
            .on_conflict(sg_configs::key)
            .do_update()
            .set(sg_configs::value.eq(value))
            .execute(&mut self.admin)
            .expect("put");
    }

    /// A key's value, read on the standby through the cache.
    fn read(&mut self, name: &str) -> Option<String> {
        let key = self.key(name);
        sg_configs::table
            .filter(sg_configs::key.eq(key))
            .select(sg_configs::value)
            .first(&mut self.reader)
            .optional()
            .expect("read")
    }

    /// Where the primary will put its next WAL record.
    fn primary_position(&mut self) -> u64 {
        diesel::sql_query(WAL_POSITION_SQL)
            .get_result::<WalPosition>(&mut self.admin)
            .expect("position")
            .position()
            .expect("a primary's position")
    }

    /// Waits until the standby has replayed up to `position`.
    fn replayed(&mut self, position: u64) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let replayed = diesel::sql_query(
                "SELECT pg_last_wal_replay_lsn()::text AS lsn",
            )
            .get_result::<WalPosition>(&mut self.standby)
            .expect("replay position")
            .position()
            .expect("a standby's position");
            if replayed >= position {
                return;
            }
            assert!(Instant::now() < deadline, "the standby never replayed");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Holds the standby's replay where it is.
    fn pause(&mut self) {
        self.standby
            .batch_execute("SELECT pg_wal_replay_pause()")
            .expect("pause replay");
    }

    /// Lets the standby replay again.
    fn resume(&mut self) {
        self.standby
            .batch_execute("SELECT pg_wal_replay_resume()")
            .expect("resume replay");
    }
}

impl Drop for World {
    /// Resumes replay and deletes this test's keys.
    fn drop(&mut self) {
        let _ = self.standby.batch_execute("SELECT pg_wal_replay_resume()");
        let _ = diesel::delete(
            sg_configs::table.filter(sg_configs::key.like(format!("{}%", self.prefix))),
        )
        .execute(&mut self.admin);
    }
}

/// A standby behind the engine's position serves the read but its rows
/// are not kept: nothing would ever report what it had not replayed yet.
/// Once it has replayed past it, the refill is kept.
#[test]
fn a_refill_from_a_standby_is_kept_only_once_it_has_replayed_past_the_engine() {
    let Some(mut w) = World::new("standby-refill") else {
        return;
    };
    w.put("mode", "a");
    let position = w.primary_position();
    w.replayed(position);
    w.pause();
    w.put("mode", "b");
    let since = w.primary_position();
    w.engine.since.store(since, Ordering::SeqCst);
    assert_eq!(w.read("mode").as_deref(), Some("a"), "the standby is behind");
    assert_eq!(w.cache.stats.unreplayed.load(Ordering::Relaxed), 1);
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    assert_eq!(w.reader.last_served(), Some(Served::Postgres), "nothing kept");
    assert_eq!(w.cache.stats.unreplayed.load(Ordering::Relaxed), 2);
    assert!(w.cache.reads_standbys());

    w.resume();
    w.replayed(since);
    assert_eq!(w.read("mode").as_deref(), Some("b"));
    assert_eq!(w.cache.stats.unreplayed.load(Ordering::Relaxed), 2, "kept");
    assert_eq!(w.read("mode").as_deref(), Some("b"));
    assert_eq!(w.reader.last_served(), Some(Served::Cache));

    let other = w.key("other");
    let unknown = sg_configs::table
        .filter(sg_configs::key.eq(other))
        .select(sg_configs::value);
    w.engine.since.store(0, Ordering::SeqCst);
    let _ = unknown.first::<String>(&mut w.reader).optional().expect("read");
    assert_eq!(
        w.cache.stats.unreplayed.load(Ordering::Relaxed),
        3,
        "no position to compare: not kept"
    );
}

/// On a primary the check is asked once per connection and never again.
#[test]
fn a_refill_on_a_primary_asks_once() {
    let Some(mut w) = World::new("primary-refill") else {
        return;
    };
    let primary_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let sent = Arc::new(AtomicU64::new(0));
    let mut inner = PgConnection::establish(&primary_url).expect("primary");
    inner.set_instrumentation(Count(Arc::clone(&sent)));
    let mut conn = SyncedConnection::with_cache(inner, Arc::clone(&w.cache), Route::Streamgres);
    w.engine.since.store(u64::MAX, Ordering::SeqCst);
    w.put("a", "1");
    w.put("b", "2");
    let read = |conn: &mut SyncedConnection<PgConnection>, key: String| {
        sg_configs::table
            .filter(sg_configs::key.eq(key))
            .select(sg_configs::value)
            .first::<String>(conn)
            .optional()
            .expect("read")
    };
    assert_eq!(read(&mut conn, w.key("a")).as_deref(), Some("1"));
    assert_eq!(sent.load(Ordering::SeqCst), 1, "the first refill asked");
    assert_eq!(read(&mut conn, w.key("b")).as_deref(), Some("2"));
    assert_eq!(sent.load(Ordering::SeqCst), 1, "the second did not");
    assert_eq!(read(&mut conn, w.key("a")).as_deref(), Some("1"));
    assert_eq!(conn.last_served(), Some(Served::Cache), "a primary's refill is kept");
    assert_eq!(w.cache.stats.unreplayed.load(Ordering::Relaxed), 0);
}

/// A write on the primary, once reads go to a standby, reads its own WAL
/// position: reads on the standby then use the cache as soon as the
/// engine side has delivered past it, rather than waiting out the
/// window, and not before.
#[test]
fn a_write_on_the_primary_gives_the_standbys_reads_its_position() {
    let Some(mut w) = World::new("standby-ryw") else {
        return;
    };
    w.put("mode", "a");
    let position = w.primary_position();
    w.replayed(position);
    w.engine.position.store(1, Ordering::SeqCst);
    w.engine.since.store(1, Ordering::SeqCst);
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    assert!(w.cache.reads_standbys(), "the refill asked the standby");
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    assert_eq!(w.reader.last_served(), Some(Served::Cache), "kept");

    let key = w.key("mode");
    diesel::update(sg_configs::table.filter(sg_configs::key.eq(&key)))
        .set(sg_configs::value.eq("b"))
        .execute(&mut w.writer)
        .expect("write");
    let written = w.primary_position();
    w.replayed(written);
    assert_eq!(
        w.read("mode").as_deref(),
        Some("b"),
        "not delivered yet: the standby"
    );
    assert_eq!(w.reader.last_served(), Some(Served::Postgres));
    w.engine.position.store(written, Ordering::SeqCst);
    assert_eq!(
        w.read("mode").as_deref(),
        Some("a"),
        "delivered: the cache, inside the window (the fake never touches it)"
    );
    assert_eq!(w.reader.last_served(), Some(Served::Cache));

    w.writer
        .transaction::<_, diesel::result::Error, _>(|conn| {
            diesel::update(sg_configs::table.filter(sg_configs::key.eq(&key)))
                .set(sg_configs::value.eq("c"))
                .execute(conn)?;
            Ok(())
        })
        .expect("a transaction");
    let committed = w.primary_position();
    w.replayed(committed);
    assert_eq!(w.read("mode").as_deref(), Some("c"), "the COMMIT read it");
    w.engine.position.store(committed, Ordering::SeqCst);
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    assert_eq!(w.reader.last_served(), Some(Served::Cache));

    let other = w.key("other");
    w.writer
        .batch_execute(&format!(
            "INSERT INTO sg_configs (key, value) VALUES ('{other}', 'x')"
        ))
        .expect("raw SQL");
    let raw = w.primary_position();
    w.replayed(raw);
    assert_eq!(w.read("mode").as_deref(), Some("c"), "raw SQL read it too");
    w.engine.position.store(raw, Ordering::SeqCst);
    assert_eq!(w.read("mode").as_deref(), Some("a"));

    let returned: String = diesel::insert_into(sg_configs::table)
        .values((sg_configs::key.eq(w.key("returning")), sg_configs::value.eq("r")))
        .returning(sg_configs::value)
        .get_result(&mut w.writer)
        .expect("insert returning");
    assert_eq!(returned, "r");
    let returning = w.primary_position();
    w.replayed(returning);
    assert_eq!(w.read("mode").as_deref(), Some("c"), "a RETURNING read it too");
}

/// Counts the replay-position queries a connection starts.
struct Count(Arc<AtomicU64>);

impl diesel::connection::Instrumentation for Count {
    /// Counts a query start of the replay position.
    fn on_connection_event(&mut self, event: diesel::connection::InstrumentationEvent<'_>) {
        if let diesel::connection::InstrumentationEvent::StartQuery { query, .. } = event
            && query.to_string().contains("pg_last_wal_replay_lsn")
        {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// The async wrapper keeps the same rules: a refill from a standby behind
/// the engine is not kept, one past it is; a write on the primary reads
/// its position, pipelined behind it, and the standby's reads use the
/// cache once the engine side has delivered past it.
#[cfg(feature = "async")]
#[test]
fn the_async_wrapper_reads_a_standby_by_the_same_rules() {
    asynchronous::the_same_rules();
}

/// The async test, apart from the blocking `RunQueryDsl`.
#[cfg(feature = "async")]
mod asynchronous {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use diesel::{ExpressionMethods, OptionalExtension, QueryDsl};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
    use diesel_streamgres::{AsyncSyncedConnection, Route, Served};

    use super::{World, sg_configs};

    /// [`super::the_async_wrapper_reads_a_standby_by_the_same_rules`].
    pub fn the_same_rules() {

        let Some(mut w) = World::new("async-standby") else {
            return;
        };
        let primary_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
        let standby_url = std::env::var("STREAMGRES_TEST_STANDBY_URL").expect("standby");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        w.put("mode", "a");
        let position = w.primary_position();
        w.replayed(position);
        w.pause();
        w.put("mode", "b");
        let since = w.primary_position();
        w.engine.since.store(since, Ordering::SeqCst);
        let key = w.key("mode");
        let cache = Arc::clone(&w.cache);
        let read = |conn: &mut AsyncSyncedConnection| {
            let key = key.clone();
            runtime.block_on(async move {
                sg_configs::table
                    .filter(sg_configs::key.eq(key))
                    .select(sg_configs::value)
                    .first::<String>(conn)
                    .await
                    .optional()
                    .expect("read")
            })
        };
        let (mut reader, mut writer) = runtime.block_on(async {
            let mut reader = AsyncSyncedConnection::with_cache(
                AsyncPgConnection::establish(&standby_url).await.expect("standby"),
                Arc::clone(&cache),
                Route::Streamgres,
            );
            reader.set_read_your_writes(Duration::from_secs(5));
            let writer = AsyncSyncedConnection::with_cache(
                AsyncPgConnection::establish(&primary_url).await.expect("primary"),
                Arc::clone(&cache),
                Route::Postgres,
            );
            (reader, writer)
        });
        assert_eq!(read(&mut reader).as_deref(), Some("a"), "the standby is behind");
        assert_eq!(AtomicU64::load(&cache.stats.unreplayed, Ordering::Relaxed), 1);
        w.resume();
        w.replayed(since);
        assert_eq!(read(&mut reader).as_deref(), Some("b"));
        assert_eq!(read(&mut reader).as_deref(), Some("b"));
        assert_eq!(reader.last_served(), Some(Served::Cache), "kept once replayed");

        w.engine.position.store(1, Ordering::SeqCst);
        runtime.block_on(async {
            diesel::update(sg_configs::table.filter(sg_configs::key.eq(&key)))
                .set(sg_configs::value.eq("c"))
                .execute(&mut writer)
                .await
                .expect("write");
        });
        let written = w.primary_position();
        w.replayed(written);
        assert_eq!(read(&mut reader).as_deref(), Some("c"), "not delivered: the standby");
        w.engine.position.store(written, Ordering::SeqCst);
        assert_eq!(read(&mut reader).as_deref(), Some("b"), "delivered: the cache");
        assert_eq!(reader.last_served(), Some(Served::Cache));

        runtime.block_on(async {
            writer
                .transaction::<_, diesel::result::Error, _>(|conn| {
                    let key = key.clone();
                    Box::pin(async move {
                        diesel::update(sg_configs::table.filter(sg_configs::key.eq(key)))
                            .set(sg_configs::value.eq("d"))
                            .execute(conn)
                            .await?;
                        Ok(())
                    })
                })
                .await
                .expect("a transaction");
        });
        let committed = w.primary_position();
        w.replayed(committed);
        assert_eq!(read(&mut reader).as_deref(), Some("d"), "the COMMIT read it");
        w.engine.position.store(committed, Ordering::SeqCst);
        assert_eq!(read(&mut reader).as_deref(), Some("b"));
    }
}
