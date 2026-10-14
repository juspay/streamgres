//! Reads through a cache that a fake engine side keeps honest.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use diesel::connection::{Instrumentation, InstrumentationEvent, SimpleConnection};
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{Integer, Text};
use streamgres_diesel::cache::{Cache, Limits};
use streamgres_diesel::key::QueryKey;
use streamgres_diesel::subscriptions::{Subscriptions, Verdict};
use streamgres_diesel::{Consistency, Route, RouteDsl, SyncedConnection};

diesel::table! {
    sg_configs (id) {
        id -> Int4,
        key -> Text,
        value -> Text,
    }
}

#[derive(Queryable, QueryableByName, Selectable, Debug, PartialEq, Clone)]
#[diesel(table_name = sg_configs)]
struct Config {
    id: i32,
    key: String,
    value: String,
}

fn url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres:///streamgres_diesel_test?host=/tmp".to_string())
}

/// The engine side as a test sees it: accepts every `SELECT` on
/// `sg_configs`, refuses the rest, and remembers every call.
#[derive(Default)]
struct FakeEngine {
    subscribed: Mutex<Vec<QueryKey>>,
    unsubscribed: Mutex<Vec<QueryKey>>,
    refuse_all: std::sync::atomic::AtomicBool,
}

impl Subscriptions for FakeEngine {
    fn subscribe(&self, key: &QueryKey, _: &mut dyn streamgres_diesel::types::TypeLookup) -> Verdict {
        if self.refuse_all.load(Ordering::SeqCst) || !key.sql.contains("sg_configs") {
            return Verdict::Refused("not a config read".into());
        }
        self.subscribed.lock().unwrap().push(key.clone());
        Verdict::Accepted
    }

    fn unsubscribe(&self, key: &QueryKey) {
        self.unsubscribed.lock().unwrap().push(key.clone());
    }
}

struct CountQueries(Arc<AtomicUsize>);

impl Instrumentation for CountQueries {
    fn on_connection_event(&mut self, event: InstrumentationEvent<'_>) {
        if let InstrumentationEvent::StartQuery { .. } = event {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

struct World {
    conn: SyncedConnection<PgConnection>,
    cache: Arc<Cache>,
    engine: Arc<FakeEngine>,
    /// Queries the inner connection actually sent to Postgres.
    sent: Arc<AtomicUsize>,
    /// A second, plain connection for writes "from elsewhere".
    other: PgConnection,
    prefix: String,
}

impl World {
    /// Each test gets its own key prefix so the tests can run in parallel
    /// against committed rows, and cleans up after itself.
    fn new(name: &str) -> World {
        Self::with_budget(name, 64 * 1024 * 1024)
    }

    fn with_budget(name: &str, max_bytes: u64) -> World {
        Self::with_limits(
            name,
            Limits {
                max_bytes,
                ..Limits::default()
            },
        )
    }

    fn with_limits(name: &str, limits: Limits) -> World {
        static SCHEMA_READY: std::sync::Once = std::sync::Once::new();
        SCHEMA_READY.call_once(|| {
            let mut conn = PgConnection::establish(&url()).expect("connect");
            conn.batch_execute(
                "CREATE TABLE IF NOT EXISTS sg_configs (
                    id serial PRIMARY KEY, key text NOT NULL UNIQUE, value text NOT NULL)",
            )
            .expect("schema");
        });
        let engine = Arc::new(FakeEngine::default());
        let cache = Arc::new(Cache::with_limits(limits, engine.clone()));
        let mut inner = PgConnection::establish(&url()).expect("connect");
        let sent = Arc::new(AtomicUsize::new(0));
        inner.set_instrumentation(CountQueries(sent.clone()));
        let conn = SyncedConnection::with_cache(inner, cache.clone(), Consistency::Eventual);
        let mut other = PgConnection::establish(&url()).expect("connect");
        let prefix = format!("{name}-{}-", std::process::id());
        diesel::delete(sg_configs::table.filter(sg_configs::key.like(format!("{prefix}%"))))
            .execute(&mut other)
            .expect("clean");
        World {
            conn,
            cache,
            engine,
            sent,
            other,
            prefix,
        }
    }

    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }

    /// Writes a config from "elsewhere": another connection, no cache.
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

    /// The key the cache uses for `read(name)`: `first` adds `LIMIT 1` as
    /// a bind, so the key carries two.
    fn key_of(&mut self, name: &str) -> QueryKey {
        let key = self.key(name);
        let query = sg_configs::table
            .filter(sg_configs::key.eq(key))
            .select(sg_configs::value)
            .limit(1);
        QueryKey::of(&query, self.conn.inner()).expect("key")
    }

    fn sent(&self) -> usize {
        self.sent.load(Ordering::SeqCst)
    }

    /// `read(name)` with the query itself flagged `route`.
    fn read_via(&mut self, name: &str, route: Route) -> Option<String> {
        let key = self.key(name);
        sg_configs::table
            .filter(sg_configs::key.eq(key))
            .select(sg_configs::value)
            .via(route)
            .first(&mut self.conn)
            .optional()
            .expect("read via")
    }
}

impl Drop for World {
    fn drop(&mut self) {
        let _ = diesel::delete(sg_configs::table.filter(sg_configs::key.like(format!("{}%", self.prefix))))
            .execute(&mut self.other);
    }
}

#[test]
fn a_repeated_read_is_served_from_the_cache() {
    let mut w = World::new("repeat");
    w.put("timeout", "30");

    assert_eq!(w.read("timeout").as_deref(), Some("30"));
    let after_first = w.sent();
    assert_eq!(w.read("timeout").as_deref(), Some("30"));
    assert_eq!(w.read("timeout").as_deref(), Some("30"));

    assert_eq!(w.sent(), after_first, "the second and third read sent nothing to Postgres");
    assert_eq!(w.cache.stats.hits.load(Ordering::Relaxed), 2);
    assert_eq!(w.cache.stats.misses.load(Ordering::Relaxed), 1);
    assert_eq!(w.engine.subscribed.lock().unwrap().len(), 1, "subscribed once");
}

#[test]
fn the_subscription_is_registered_before_postgres_is_read() {
    let mut w = World::new("order");
    w.put("a", "1");
    let before = w.sent();
    w.read("a");
    let key = w.key_of("a");
    let subscribed = w.engine.subscribed.lock().unwrap();
    assert_eq!(subscribed.as_slice(), std::slice::from_ref(&key));
    assert_eq!(w.sent(), before + 1, "exactly one read reached Postgres");
    assert!(key.is_select());
    assert_eq!(key.binds.len(), 2, "the key's value and the LIMIT");
    assert_eq!(key.binds[0].type_oid, 25, "the first bind is a text");
    assert_eq!(key.binds[1].type_oid, 20, "the limit is an int8");
}

#[test]
fn different_binds_are_different_entries() {
    let mut w = World::new("binds");
    w.put("a", "1");
    w.put("b", "2");
    assert_eq!(w.read("a").as_deref(), Some("1"));
    assert_eq!(w.read("b").as_deref(), Some("2"));
    assert_eq!(w.read("a").as_deref(), Some("1"));
    assert_eq!(w.cache.len(), 2);
    assert_ne!(w.key_of("a"), w.key_of("b"));
}

#[test]
fn a_touch_makes_the_next_read_refill_from_postgres() {
    let mut w = World::new("touch");
    w.put("limit", "10");
    assert_eq!(w.read("limit").as_deref(), Some("10"));

    // A change from elsewhere: the cache does not see it...
    w.put("limit", "20");
    assert_eq!(w.read("limit").as_deref(), Some("10"), "stale until the engine says so");

    // ...until the engine reports the query touched.
    let key = w.key_of("limit");
    w.cache.touched(&key);
    let before = w.sent();
    assert_eq!(w.read("limit").as_deref(), Some("20"));
    assert_eq!(w.sent(), before + 1, "refilled with one read");
    assert_eq!(w.read("limit").as_deref(), Some("20"));
    assert_eq!(w.sent(), before + 1, "and cached again");
    assert!(w.engine.unsubscribed.lock().unwrap().is_empty(), "a touch keeps the subscription");
}

#[test]
fn a_touch_during_the_read_is_not_lost() {
    let mut w = World::new("race");
    w.put("flag", "off");
    let key = w.key_of("flag");

    // What `load` does on a miss, with a touch landing between the
    // reservation and the fill.
    let generation = w.cache.reserve(&key);
    w.put("flag", "on");
    w.cache.touched(&key);
    let stale_rows: Arc<[streamgres_diesel::row::OwnedRow]> = Arc::from(Vec::new());
    assert!(!w.cache.fill(key.clone(), generation, stale_rows), "the fill is refused");
    assert_eq!(w.cache.stats.stale_fills.load(Ordering::Relaxed), 1);
    assert!(w.cache.is_empty());

    assert_eq!(w.read("flag").as_deref(), Some("on"), "the next read sees the change");
}

#[test]
fn a_strong_connection_never_reads_the_cache() {
    let mut w = World::new("strong");
    w.put("mode", "a");
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    w.put("mode", "b");

    w.conn.set_consistency(Consistency::Strong);
    assert_eq!(w.read("mode").as_deref(), Some("b"));
    w.conn.set_consistency(Consistency::Eventual);
    assert_eq!(w.read("mode").as_deref(), Some("a"), "the eventual entry is untouched");
}

#[test]
fn inside_a_transaction_reads_see_the_connections_own_writes() {
    let mut w = World::new("txn");
    w.put("count", "1");
    assert_eq!(w.read("count").as_deref(), Some("1"));
    let count_key = w.key("count");

    let seen: String = w
        .conn
        .transaction(|c| {
            diesel::update(sg_configs::table.filter(sg_configs::key.eq(&count_key)))
                .set(sg_configs::value.eq("2"))
                .execute(c)?;
            sg_configs::table
                .filter(sg_configs::key.eq(&count_key))
                .select(sg_configs::value)
                .first(c)
        })
        .expect("transaction");
    assert_eq!(seen, "2", "the read inside the transaction went to Postgres");
    assert_eq!(w.read("count").as_deref(), Some("1"), "outside it the cache still holds the old row until a touch");
    assert_eq!(w.engine.subscribed.lock().unwrap().len(), 1, "the transactional read did not subscribe");
}

#[test]
fn a_write_returning_rows_is_never_cached() {
    let mut w = World::new("returning");
    let key = w.key("new");
    let inserted: Config = diesel::insert_into(sg_configs::table)
        .values((sg_configs::key.eq(&key), sg_configs::value.eq("x")))
        .get_result(&mut w.conn)
        .expect("insert returning");
    assert_eq!(inserted.value, "x");
    let updated: Config = diesel::update(sg_configs::table.filter(sg_configs::key.eq(&key)))
        .set(sg_configs::value.eq("y"))
        .get_result(&mut w.conn)
        .expect("update returning");
    assert_eq!(updated.value, "y");
    assert!(w.cache.is_empty());
    assert!(w.engine.subscribed.lock().unwrap().is_empty());
    assert_eq!(w.cache.stats.misses.load(Ordering::Relaxed), 0, "writes are not even looked up");
}

#[test]
fn a_refused_query_is_read_live_every_time() {
    let mut w = World::new("refused");
    w.put("k", "v");
    w.engine.refuse_all.store(true, Ordering::SeqCst);
    assert_eq!(w.read("k").as_deref(), Some("v"));
    w.put("k", "w");
    assert_eq!(w.read("k").as_deref(), Some("w"), "live, so the change is seen");
    assert_eq!(w.cache.stats.refused.load(Ordering::Relaxed), 2);
    assert!(w.cache.is_empty());

    // Raw SQL against another table is refused by this engine too.
    let rows: Vec<Config> = sql_query("SELECT id, key, value FROM sg_configs WHERE key = $1")
        .bind::<Text, _>(w.key("k"))
        .load(&mut w.conn)
        .expect("sql_query");
    assert_eq!(rows.len(), 1);
    let n: i32 = diesel::select(diesel::dsl::sql::<Integer>("41 + 1"))
        .get_result(&mut w.conn)
        .expect("select 42");
    assert_eq!(n, 42);
}

#[test]
fn over_budget_entries_are_evicted_and_unsubscribed() {
    let mut w = World::with_budget("evict", 600);
    for i in 0..20 {
        w.put(&format!("k{i}"), &"x".repeat(100));
    }
    for i in 0..20 {
        assert_eq!(w.read(&format!("k{i}")).unwrap().len(), 100);
    }
    let held = w.cache.len();
    assert!(held < 20, "the budget holds fewer than all 20, held {held}");
    assert!(w.cache.weighted_size() <= 600);
    let unsubscribed = w.engine.unsubscribed.lock().unwrap().len();
    assert_eq!(unsubscribed as u64, 20 - held, "every evicted entry ended its subscription");
    assert_eq!(w.cache.stats.evicted.load(Ordering::Relaxed), unsubscribed as u64);
}

#[test]
fn cached_rows_decode_whole_structs_by_name_and_count() {
    let mut w = World::new("shapes");
    w.put("p", "1");
    w.put("q", "2");
    let like = format!("{}%", w.prefix);

    let whole = || sg_configs::table
        .filter(sg_configs::key.like(&like))
        .order(sg_configs::key)
        .select(Config::as_select());
    let first: Vec<Config> = whole().load(&mut w.conn).expect("load");
    let second: Vec<Config> = whole().load(&mut w.conn).expect("load again");
    assert_eq!(first, second);
    assert_eq!(first.iter().map(|c| c.value.as_str()).collect::<Vec<_>>(), ["1", "2"]);

    let count = || sg_configs::table.filter(sg_configs::key.like(&like)).count();
    let n: i64 = count().get_result(&mut w.conn).expect("count");
    let m: i64 = count().get_result(&mut w.conn).expect("count again");
    assert_eq!((n, m), (2, 2));

    #[derive(QueryableByName, Debug, PartialEq)]
    struct Pair {
        #[diesel(sql_type = Text)]
        key: String,
        #[diesel(sql_type = Integer)]
        id: i32,
    }
    let by_name = || sql_query("SELECT key, id FROM sg_configs WHERE key LIKE $1 ORDER BY key")
        .bind::<Text, _>(&like);
    let a: Vec<Pair> = by_name().load(&mut w.conn).expect("by name");
    let b: Vec<Pair> = by_name().load(&mut w.conn).expect("by name again");
    assert_eq!(a, b);
    assert_eq!(a[0].key, w.key("p"));
    assert_eq!(w.cache.stats.hits.load(Ordering::Relaxed), 3);
}

#[test]
fn clearing_the_cache_refills_everything() {
    let mut w = World::new("clear");
    w.put("x", "1");
    w.read("x");
    w.put("x", "2");
    w.cache.clear();
    assert_eq!(w.read("x").as_deref(), Some("2"));
}

#[test]
fn a_query_flagged_postgres_bypasses_the_cache_on_a_streamgres_connection() {
    let mut w = World::new("via-pg");
    w.put("mode", "a");
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    w.put("mode", "b");

    // The flag wins over the connection: Postgres is read, the cached
    // entry is neither used nor replaced, and nothing is subscribed.
    let sent = w.sent();
    let subscribed = w.engine.subscribed.lock().unwrap().len();
    assert_eq!(w.read_via("mode", Route::Postgres).as_deref(), Some("b"));
    assert_eq!(w.sent(), sent + 1);
    assert_eq!(w.engine.subscribed.lock().unwrap().len(), subscribed);
    assert_eq!(w.read("mode").as_deref(), Some("a"), "the entry is untouched");
}

#[test]
fn a_query_flagged_streamgres_is_cached_on_a_postgres_connection() {
    let mut w = World::new("via-sg");
    w.conn.set_route(Route::Postgres);
    w.put("mode", "a");
    assert_eq!(w.read_via("mode", Route::Streamgres).as_deref(), Some("a"));
    assert_eq!(w.cache.len(), 1, "subscribed and filled");
    w.put("mode", "b");
    assert_eq!(w.read_via("mode", Route::Streamgres).as_deref(), Some("a"), "served from the cache");
    assert_eq!(w.read("mode").as_deref(), Some("b"), "the connection's own route is Postgres");
}

#[test]
fn the_marker_is_not_part_of_the_key_or_what_the_engine_sees() {
    let mut w = World::new("via-key");
    w.put("mode", "a");
    assert_eq!(w.read_via("mode", Route::Streamgres).as_deref(), Some("a"));
    let subscribed = w.engine.subscribed.lock().unwrap().clone();
    assert_eq!(subscribed.len(), 1);
    assert!(!subscribed[0].sql.contains("/*"), "{}", subscribed[0].sql);
    assert_eq!(subscribed[0], w.key_of("mode"), "flagged and unflagged reads share the entry");
    let sent = w.sent();
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    assert_eq!(w.sent(), sent, "the unflagged read hit the flagged read's entry");
}

#[test]
fn a_route_scope_covers_every_read_inside_it_and_the_query_flag_still_wins() {
    let mut w = World::new("scope");
    w.put("mode", "a");
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    w.put("mode", "b");

    let World { conn, prefix, .. } = &mut w;
    let key = format!("{prefix}mode");
    let (scoped, flagged) = conn.with_route(Route::Postgres, |conn| {
        let scoped: Option<String> = sg_configs::table
            .filter(sg_configs::key.eq(&key))
            .select(sg_configs::value)
            .first(conn)
            .optional()
            .unwrap();
        let flagged: Option<String> = sg_configs::table
            .filter(sg_configs::key.eq(&key))
            .select(sg_configs::value)
            .via_streamgres()
            .first(conn)
            .optional()
            .unwrap();
        (scoped, flagged)
    });
    assert_eq!(scoped.as_deref(), Some("b"), "the scope routed to Postgres");
    assert_eq!(flagged.as_deref(), Some("a"), "the query flag beat the scope");
    assert_eq!(w.read("mode").as_deref(), Some("a"), "the scope ended");
}

#[test]
fn an_entry_nobody_reads_expires_and_is_unsubscribed() {
    let mut w = World::with_limits(
        "idle",
        Limits {
            time_to_idle: Some(std::time::Duration::from_millis(80)),
            ..Limits::default()
        },
    );
    w.put("mode", "a");
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    // Reads keep it alive.
    for _ in 0..4 {
        std::thread::sleep(std::time::Duration::from_millis(40));
        assert_eq!(w.read("mode").as_deref(), Some("a"));
    }
    assert_eq!(w.cache.len(), 1);
    assert!(w.engine.unsubscribed.lock().unwrap().is_empty());

    std::thread::sleep(std::time::Duration::from_millis(160));
    assert_eq!(w.cache.len(), 0, "idle past the clock");
    assert_eq!(w.engine.unsubscribed.lock().unwrap().len(), 1);
    assert_eq!(w.cache.stats.expired.load(Ordering::Relaxed), 1);

    // The next read subscribes again.
    w.put("mode", "b");
    assert_eq!(w.read("mode").as_deref(), Some("b"));
    assert_eq!(w.engine.subscribed.lock().unwrap().len(), 2);
}

#[test]
fn time_to_live_drops_an_entry_however_often_it_is_read() {
    let mut w = World::with_limits(
        "ttl",
        Limits {
            time_to_live: Some(std::time::Duration::from_millis(100)),
            ..Limits::default()
        },
    );
    w.put("mode", "a");
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    let started = std::time::Instant::now();
    while started.elapsed() < std::time::Duration::from_millis(80) {
        assert_eq!(w.read("mode").as_deref(), Some("a"));
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    std::thread::sleep(std::time::Duration::from_millis(60));
    w.cache.run_pending_tasks();
    assert_eq!(w.engine.unsubscribed.lock().unwrap().len(), 1, "ended on the timer");
    w.put("mode", "b");
    assert_eq!(w.read("mode").as_deref(), Some("b"), "resubscribed and refilled");
    assert_eq!(w.engine.subscribed.lock().unwrap().len(), 2);
}
