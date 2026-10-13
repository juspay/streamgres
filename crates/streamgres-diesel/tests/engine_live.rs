//! The whole path against a running engine: a read is subscribed, served
//! from the cache, and refilled after a commit the engine reports.
//!
//! Needs `STREAMGRES_S2S_URL` (`ws://host:port/<base>/s2s/v1`) and
//! `DATABASE_URL` pointing at the database that engine streams. Skips
//! itself otherwise.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_types::Text;
use streamgres_diesel::cache::Cache;
use streamgres_diesel::engine::EngineClient;
use streamgres_diesel::{Consistency, SyncedConnection};

diesel::table! {
    sg_configs (id) {
        id -> Int4,
        key -> Text,
        value -> Text,
    }
}

#[derive(Queryable, Selectable, Debug, PartialEq, Clone)]
#[diesel(table_name = sg_configs)]
struct Config {
    id: i32,
    key: String,
    value: String,
}

struct Live {
    conn: SyncedConnection<PgConnection>,
    cache: Arc<Cache>,
    engine: EngineClient,
    other: PgConnection,
    prefix: String,
}

impl Live {
    fn start(name: &str) -> Option<Live> {
        let Ok(url) = std::env::var("STREAMGRES_S2S_URL") else {
            eprintln!("STREAMGRES_S2S_URL not set; skipping");
            return None;
        };
        let database = std::env::var("DATABASE_URL").expect("DATABASE_URL");
        let engine = EngineClient::connect(url);
        let cache = Arc::new(Cache::new(64 * 1024 * 1024, Arc::new(engine.clone())));
        engine.attach(&cache);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !engine.is_connected() {
            assert!(Instant::now() < deadline, "the engine did not connect");
            std::thread::sleep(Duration::from_millis(50));
        }
        let mut other = PgConnection::establish(&database).expect("connect");
        other
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS sg_configs (
                    id serial PRIMARY KEY, key text NOT NULL UNIQUE, value text NOT NULL)",
            )
            .expect("schema");
        let prefix = format!("{name}-{}-", std::process::id());
        let inner = PgConnection::establish(&database).expect("connect");
        let conn = SyncedConnection::with_cache(inner, cache.clone(), Consistency::Eventual);
        Some(Live {
            conn,
            cache,
            engine,
            other,
            prefix,
        })
    }

    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }

    fn put(&mut self, name: &str, value: &str) {
        let key = self.key(name);
        diesel::insert_into(sg_configs::table)
            .values((sg_configs::key.eq(&key), sg_configs::value.eq(value)))
            .on_conflict(sg_configs::key)
            .do_update()
            .set(sg_configs::value.eq(value))
            .execute(&mut self.other)
            .expect("put");
    }

    fn read(&mut self, name: &str) -> Option<String> {
        let key = self.key(name);
        sg_configs::table
            .filter(sg_configs::key.eq(key))
            .select(sg_configs::value)
            .first(&mut self.conn)
            .optional()
            .expect("read")
    }

    /// Reads until the value is seen or the wait runs out, returning how
    /// long it took.
    fn read_until(&mut self, name: &str, want: &str, wait: Duration) -> Option<Duration> {
        let started = Instant::now();
        while started.elapsed() < wait {
            if self.read(name).as_deref() == Some(want) {
                return Some(started.elapsed());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        None
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        let _ = diesel::delete(
            sg_configs::table.filter(sg_configs::key.like(format!("{}%", self.prefix))),
        )
        .execute(&mut self.other);
    }
}

#[test]
fn a_commit_the_engine_reports_refills_the_cache() {
    let Some(mut live) = Live::start("refill") else {
        return;
    };
    live.put("rate", "1");

    assert_eq!(live.read("rate").as_deref(), Some("1"), "first read, from Postgres");
    assert_eq!(live.engine.subscribed(), 1, "the engine accepted the query");
    assert_eq!(live.read("rate").as_deref(), Some("1"), "second read, from the cache");
    assert_eq!(live.cache.stats.hits.load(Ordering::Relaxed), 1);

    live.put("rate", "2");
    let took = live
        .read_until("rate", "2", Duration::from_secs(10))
        .expect("the change reached the cache through the engine");
    eprintln!("commit to refilled read: {took:?}");
    assert!(live.cache.stats.touched.load(Ordering::Relaxed) >= 1);
    assert_eq!(live.engine.subscribed(), 1, "a touch keeps the subscription");

    // A delete is a change too.
    diesel::delete(sg_configs::table.filter(sg_configs::key.eq(live.key("rate"))))
        .execute(&mut live.other)
        .expect("delete");
    let started = Instant::now();
    while live.read("rate").is_some() {
        assert!(started.elapsed() < Duration::from_secs(10), "the delete never reached the cache");
        std::thread::sleep(Duration::from_millis(20));
    }

    // And a row appearing under the key the query asks for.
    live.put("rate", "3");
    live.read_until("rate", "3", Duration::from_secs(10))
        .expect("the insert reached the cache");
}

#[test]
fn an_unrelated_commit_does_not_touch_the_entry() {
    let Some(mut live) = Live::start("unrelated") else {
        return;
    };
    live.put("a", "1");
    live.put("b", "1");
    assert_eq!(live.read("a").as_deref(), Some("1"));
    let touched_before = live.cache.stats.touched.load(Ordering::Relaxed);

    live.put("b", "2");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(live.read("a").as_deref(), Some("1"));
    assert_eq!(
        live.cache.stats.touched.load(Ordering::Relaxed),
        touched_before,
        "a change to another row's key is not a touch of this query"
    );
    assert_eq!(live.cache.stats.hits.load(Ordering::Relaxed), 1);
}

#[test]
fn a_query_outside_the_engines_subset_is_served_live() {
    let Some(mut live) = Live::start("live") else {
        return;
    };
    live.put("x", "1");
    let like = format!("{}%", live.prefix);
    let read = |live: &mut Live| -> Vec<Config> {
        sg_configs::table
            .filter(sg_configs::key.like(&like))
            .select(Config::as_select())
            .load(&mut live.conn)
            .expect("like")
    };
    assert_eq!(read(&mut live).len(), 1);
    assert_eq!(live.cache.stats.refused.load(Ordering::Relaxed), 1, "LIKE is refused");
    live.put("y", "1");
    assert_eq!(read(&mut live).len(), 2, "live, so the new row is seen at once");
    assert_eq!(live.engine.subscribed(), 0);

    // Raw SQL in the subset is accepted.
    #[derive(QueryableByName, Debug, PartialEq)]
    struct Val {
        #[diesel(sql_type = Text)]
        value: String,
    }
    let raw = |live: &mut Live| -> Vec<Val> {
        diesel::sql_query("SELECT value FROM sg_configs WHERE key = $1")
            .bind::<Text, _>(live.key("x"))
            .load(&mut live.conn)
            .expect("raw")
    };
    assert_eq!(raw(&mut live)[0].value, "1");
    assert_eq!(live.engine.subscribed(), 1);
    live.put("x", "2");
    let started = Instant::now();
    while raw(&mut live)[0].value != "2" {
        assert!(started.elapsed() < Duration::from_secs(10));
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn evicting_an_entry_unsubscribes_it_from_the_engine() {
    let Some(mut live) = Live::start("evict") else {
        return;
    };
    // A tiny budget: the second entry pushes the first out.
    let engine = live.engine.clone();
    let small = Arc::new(Cache::new(400, Arc::new(engine.clone())));
    engine.attach(&small);
    let inner = live.conn.inner();
    let database = std::env::var("DATABASE_URL").unwrap();
    let _ = inner;
    let mut conn = SyncedConnection::with_cache(
        PgConnection::establish(&database).unwrap(),
        small.clone(),
        Consistency::Eventual,
    );
    for i in 0..6 {
        live.put(&format!("e{i}"), &"v".repeat(120));
    }
    for i in 0..6 {
        let key = live.key(&format!("e{i}"));
        let _: Option<String> = sg_configs::table
            .filter(sg_configs::key.eq(key))
            .select(sg_configs::value)
            .first(&mut conn)
            .optional()
            .unwrap();
    }
    let held = small.len() as usize;
    assert!(held < 6, "held {held}");
    let deadline = Instant::now() + Duration::from_secs(2);
    while engine.subscribed() != held {
        assert!(Instant::now() < deadline, "subscribed {} held {held}", engine.subscribed());
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Needs `STREAMGRES_RESTART_CMD`, a shell command that restarts the
/// engine and returns once it serves again.
#[test]
fn an_engine_restart_is_survived_without_a_stale_read() {
    let Some(mut live) = Live::start("restart") else {
        return;
    };
    let Ok(restart) = std::env::var("STREAMGRES_RESTART_CMD") else {
        eprintln!("STREAMGRES_RESTART_CMD not set; skipping");
        return;
    };
    live.put("v", "1");
    assert_eq!(live.read("v").as_deref(), Some("1"));
    assert_eq!(live.engine.subscribed(), 1);

    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(&restart)
        .status()
        .expect("restart command");
    assert!(status.success(), "the restart command failed");

    // While the engine was away, the socket dropped and the cache was
    // cleared; a change made meanwhile must be seen once reads resume.
    live.put("v", "2");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !live.engine.is_connected() {
        assert!(Instant::now() < deadline, "the client did not reconnect");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(live.read("v").as_deref(), Some("2"), "no stale read after the restart");
    assert_eq!(live.engine.subscribed(), 1, "resubscribed under the same key");
    assert_eq!(live.cache.len(), 1, "the read after the restart was cached again");
    let touched_before = live.cache.stats.touched.load(Ordering::Relaxed);
    let hits_before = live.cache.stats.hits.load(Ordering::Relaxed);
    assert_eq!(live.read("v").as_deref(), Some("2"));
    assert_eq!(live.cache.stats.hits.load(Ordering::Relaxed), hits_before + 1, "served from the cache");

    // And the resubscription is live: a further change is reported.
    live.put("v", "3");
    let took = live
        .read_until("v", "3", Duration::from_secs(10))
        .expect("the change reached the cache through the resubscribed engine");
    eprintln!("after restart, commit to refilled read: {took:?}");
    assert!(
        live.cache.stats.touched.load(Ordering::Relaxed) > touched_before,
        "the refill came from a touch, not from a missing entry"
    );
}
