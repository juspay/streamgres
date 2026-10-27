//! Reads through a cache that a fake engine side keeps honest.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use diesel::connection::{Instrumentation, InstrumentationEvent, SimpleConnection};
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{Integer, Text};
use diesel_streamgres::cache::{Cache, Limits};
use diesel_streamgres::key::QueryKey;
use diesel_streamgres::subscriptions::{Outcome, Subscriptions, Verdict};
use diesel_streamgres::{Consistency, Route, RouteDsl, Served, SyncedConnection};

diesel::table! {
    sg_configs (id) {
        id -> Int4,
        key -> Text,
        value -> Text,
    }
}

/// A config row.
#[derive(Queryable, QueryableByName, Selectable, Debug, PartialEq, Clone)]
#[diesel(table_name = sg_configs)]
struct Config {
    id: i32,
    key: String,
    value: String,
}

/// The test database's URL.
fn url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres:///diesel_streamgres_test?host=/tmp".to_string())
}

/// The engine side as a test sees it: accepts every `SELECT` on
/// `sg_configs`, refuses the rest, and remembers every call.
#[derive(Default)]
struct FakeEngine {
    subscribed: Mutex<Vec<QueryKey>>,
    unsubscribed: Mutex<Vec<QueryKey>>,
    refuse_all: std::sync::atomic::AtomicBool,
    /// Says the engine is too far behind for the cache to be trusted.
    behind: std::sync::atomic::AtomicBool,
    /// The next subscribe ships no rows, the engine's view, and leaves
    /// the engine side behind, the way a socket that just reconnected to
    /// a lagging engine hears its lag before the answer.
    rows_from_behind: std::sync::atomic::AtomicBool,
    /// The position the engine side says it has delivered every change
    /// up to; 0 says none, as an engine from before positions.
    position: std::sync::atomic::AtomicU64,
}

/// An engine-side store that holds no rows.
#[derive(Debug)]
struct NoRows;

impl diesel_streamgres::cache::Live for NoRows {
    /// Always empty.
    fn rows(&self) -> Option<Arc<[diesel_streamgres::row::OwnedRow]>> {
        Some(Arc::from(Vec::new()))
    }

    /// Weighs nothing.
    fn bytes(&self) -> usize {
        0
    }
}

impl Subscriptions for FakeEngine {
    /// Accepted for `sg_configs` unless told to refuse all, refused
    /// otherwise.
    fn subscribe(&self, key: &QueryKey) -> Outcome {
        if self.refuse_all.load(Ordering::SeqCst) || !key.sql.contains("sg_configs") {
            return Outcome::Verdict(Verdict::Refused("not a config read".into()));
        }
        self.subscribed.lock().unwrap().push(key.clone());
        if self.rows_from_behind.swap(false, Ordering::SeqCst) {
            self.behind.store(true, Ordering::SeqCst);
            return Outcome::Verdict(Verdict::Rows {
                rows: Arc::from(Vec::new()),
                live: Arc::new(NoRows),
            });
        }
        Outcome::Verdict(Verdict::Accepted { since: None })
    }

    /// Accepted, with no rows.
    fn materialize(
        &self,
        _: &QueryKey,
        _: diesel_streamgres::materialized::Snapshot,
        _: std::collections::HashMap<String, Arc<diesel_streamgres::types::TableTypes>>,
    ) -> Verdict {
        Verdict::Accepted { since: None }
    }

    /// Records the key.
    fn unsubscribe(&self, key: &QueryKey) {
        self.unsubscribed.lock().unwrap().push(key.clone());
    }

    /// As set by the test.
    fn behind(&self) -> bool {
        self.behind.load(Ordering::SeqCst)
    }

    /// As set by the test.
    fn position(&self) -> Option<u64> {
        Some(self.position.load(Ordering::SeqCst)).filter(|&position| position != 0)
    }
}

/// Counts the queries the connection starts.
struct CountQueries(Arc<AtomicUsize>);

impl Instrumentation for CountQueries {
    /// Counts a query start.
    fn on_connection_event(&mut self, event: InstrumentationEvent<'_>) {
        if let InstrumentationEvent::StartQuery { .. } = event {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// One test's wrapped connection and cache, its fake engine, a plain
/// connection for changes, and the prefix of its keys.
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

    /// A world whose cache holds at most `max_bytes`.
    fn with_budget(name: &str, max_bytes: u64) -> World {
        Self::with_limits(
            name,
            Limits {
                max_bytes,
                ..Limits::default()
            },
        )
    }

    /// A world whose cache has `limits`, the table created once.
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

    /// A key of this test's.
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

    /// A key's value through the wrapper.
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

    /// How many queries reached Postgres so far.
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
    /// Deletes this test's keys.
    fn drop(&mut self) {
        let _ = diesel::delete(
            sg_configs::table.filter(sg_configs::key.like(format!("{}%", self.prefix))),
        )
        .execute(&mut self.other);
    }
}

/// A repeated read is served from the cache, without reaching Postgres.
#[test]
fn a_repeated_read_is_served_from_the_cache() {
    let mut w = World::new("repeat");
    w.put("timeout", "30");

    assert_eq!(w.read("timeout").as_deref(), Some("30"));
    let after_first = w.sent();
    assert_eq!(w.read("timeout").as_deref(), Some("30"));
    assert_eq!(w.read("timeout").as_deref(), Some("30"));

    assert_eq!(
        w.sent(),
        after_first,
        "the second and third read sent nothing to Postgres"
    );
    assert_eq!(w.cache.stats.hits.load(Ordering::Relaxed), 2);
    assert_eq!(w.cache.stats.misses.load(Ordering::Relaxed), 1);
    assert_eq!(
        w.engine.subscribed.lock().unwrap().len(),
        1,
        "subscribed once"
    );
}

/// The connection says where each read was served from.
#[test]
fn the_connection_says_where_each_read_was_served_from() {
    let mut w = World::new("served");
    assert_eq!(w.conn.last_served(), None, "nothing read yet");
    w.put("k", "1");

    assert_eq!(w.read("k").as_deref(), Some("1"));
    assert_eq!(
        w.conn.last_served(),
        Some(Served::Postgres),
        "the miss the engine accepted"
    );
    assert_eq!(w.read("k").as_deref(), Some("1"));
    assert_eq!(w.conn.last_served(), Some(Served::Cache));

    let key = w.key("k");
    let value: Option<String> = sg_configs::table
        .filter(sg_configs::key.eq(&key))
        .select(sg_configs::value)
        .via(Route::Postgres)
        .first(&mut w.conn)
        .optional()
        .expect("read via");
    assert_eq!(value.as_deref(), Some("1"));
    assert_eq!(
        w.conn.last_served(),
        Some(Served::Postgres),
        "the flag sent it live"
    );

    w.engine.refuse_all.store(true, Ordering::SeqCst);
    assert_eq!(w.read("other").as_deref(), None);
    assert_eq!(
        w.conn.last_served(),
        Some(Served::Postgres),
        "refused, so live"
    );
}

/// The subscription is registered before Postgres is read, so no commit in
/// between goes unreported. The reads counted are the query's and, once
/// per connection, what it reads as and whether it is on a primary.
#[test]
fn the_subscription_is_registered_before_postgres_is_read() {
    let mut w = World::new("order");
    w.put("a", "1");
    let before = w.sent();
    w.read("a");
    let key = w.key_of("a");
    let subscribed = w.engine.subscribed.lock().unwrap();
    assert_eq!(subscribed.as_slice(), std::slice::from_ref(&key));
    assert_eq!(
        w.sent(),
        before + 3,
        "what it reads as, whether it is a primary, then the read"
    );
    assert!(key.is_select());
    assert_eq!(key.binds.len(), 2, "the key's value and the LIMIT");
    assert_eq!(key.binds[0].type_oid, 25, "the first bind is a text");
    assert_eq!(key.binds[1].type_oid, 20, "the limit is an int8");
}

/// A connection's first read through a cache reads what it reads as,
/// once, beside the query (and the connection's first refill whether it
/// is on a primary); the first admitted tells the cache its database.
/// Later reads ask neither.
#[test]
fn the_first_read_reads_the_context_once() {
    let mut w = World::new("database");
    w.put("a", "1");
    w.put("b", "2");
    assert!(!w.cache.knows_database());
    let before = w.sent();
    w.read("a");
    assert_eq!(w.sent(), before + 3, "the context, the primary and the read");
    let database = w.cache.database().expect("read").clone();
    assert!(!database.system.is_empty() && !database.name.is_empty(), "{database:?}");
    w.read("b");
    assert_eq!(w.sent(), before + 4, "only the read");
}

/// While the engine side says it is too far behind, reads go to Postgres
/// and see a change the engine has not reported; once it catches up the
/// cache answers again, with what it held.
#[test]
fn a_lagging_engine_sends_reads_to_postgres() {
    let mut w = World::new("lagging");
    w.put("mode", "a");
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    w.put("mode", "b");
    assert_eq!(w.read("mode").as_deref(), Some("a"), "cached, unreported");
    w.engine.behind.store(true, Ordering::SeqCst);
    assert_eq!(w.read("mode").as_deref(), Some("b"), "the engine is behind: Postgres");
    assert_eq!(w.conn.last_served(), Some(Served::Postgres));
    assert_eq!(w.cache.stats.lagging.load(Ordering::Relaxed), 1);
    w.engine.behind.store(false, Ordering::SeqCst);
    assert_eq!(w.read("mode").as_deref(), Some("a"), "caught up: the cache again");
    assert_eq!(w.conn.last_served(), Some(Served::Cache));
}

/// An engine side found behind only once its answer arrives does not
/// have that answer served: the read goes to Postgres.
#[test]
fn rows_from_a_lagging_engine_are_not_served() {
    let mut w = World::new("rows_lagging");
    w.put("mode", "a");
    w.engine.rows_from_behind.store(true, Ordering::SeqCst);
    assert_eq!(w.read("mode").as_deref(), Some("a"), "Postgres, not the empty rows");
    assert_eq!(w.conn.last_served(), Some(Served::Postgres));
    assert_eq!(w.cache.stats.lagging.load(Ordering::Relaxed), 1);
}

/// The same query with different binds makes different entries.
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

/// A change from elsewhere is not seen by the cache until the engine
/// reports the query touched; the next read then refills from Postgres.
#[test]
fn a_touch_makes_the_next_read_refill_from_postgres() {
    let mut w = World::new("touch");
    w.put("limit", "10");
    assert_eq!(w.read("limit").as_deref(), Some("10"));

    w.put("limit", "20");
    assert_eq!(
        w.read("limit").as_deref(),
        Some("10"),
        "stale until the engine says so"
    );

    let key = w.key_of("limit");
    w.cache.touched(&key);
    let before = w.sent();
    assert_eq!(w.read("limit").as_deref(), Some("20"));
    assert_eq!(w.sent(), before + 1, "refilled with one read");
    assert_eq!(w.read("limit").as_deref(), Some("20"));
    assert_eq!(w.sent(), before + 1, "and cached again");
    assert!(
        w.engine.unsubscribed.lock().unwrap().is_empty(),
        "a touch keeps the subscription"
    );
}

/// A touch that lands while a miss is being read is not lost: the fill is
/// dropped and the next read refills.
///
/// What `load` does on a miss, with a touch landing between the reservation
/// and the fill.
#[test]
fn a_touch_during_the_read_is_not_lost() {
    let mut w = World::new("race");
    w.put("flag", "off");
    let key = w.key_of("flag");

    let reservation = w.cache.reserve(&key);
    w.put("flag", "on");
    w.cache.touched(&key);
    let stale_rows: Arc<[diesel_streamgres::row::OwnedRow]> = Arc::from(Vec::new());
    assert!(
        !w.cache.fill(reservation, stale_rows),
        "the fill is refused"
    );
    assert_eq!(w.cache.stats.stale_fills.load(Ordering::Relaxed), 1);
    assert!(w.cache.get(&key).is_none(), "the key is stale, nothing is served");

    assert_eq!(
        w.read("flag").as_deref(),
        Some("on"),
        "the next read sees the change"
    );
}

/// A connection routed to Postgres never reads the cache.
#[test]
fn a_strong_connection_never_reads_the_cache() {
    let mut w = World::new("strong");
    w.put("mode", "a");
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    w.put("mode", "b");

    w.conn.set_consistency(Consistency::Strong);
    assert_eq!(w.read("mode").as_deref(), Some("b"));
    w.conn.set_consistency(Consistency::Eventual);
    assert_eq!(
        w.read("mode").as_deref(),
        Some("a"),
        "the eventual entry is untouched"
    );
}

/// Inside a transaction reads go to Postgres and see the connection's own
/// writes.
///
/// Right after the COMMIT the connection is in its read-your-writes window
/// and reads Postgres; the entry itself is untouched until the engine says
/// so.
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
    assert_eq!(
        seen, "2",
        "the read inside the transaction went to Postgres"
    );
    assert_eq!(
        w.engine.subscribed.lock().unwrap().len(),
        1,
        "the transactional read did not subscribe"
    );
    assert_eq!(w.read("count").as_deref(), Some("2"));
    let cache_key = w.key_of("count");
    assert_eq!(
        w.cache.get(&cache_key).map(|rows| rows.len()),
        Some(1),
        "the entry is still the old row"
    );
    w.conn.set_read_your_writes(std::time::Duration::ZERO);
    assert_eq!(
        w.read("count").as_deref(),
        Some("1"),
        "outside the window the cache still holds the old row until a touch"
    );
}

/// A write that returns rows is never cached.
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
    assert_eq!(
        w.cache.stats.misses.load(Ordering::Relaxed),
        0,
        "writes are not even looked up"
    );
}

/// A query the engine refuses is read live every time.
///
/// The engine was asked once; the refusal is remembered and the second read
/// went to Postgres without asking.
///
/// Raw SQL against another table is refused by this engine too.
#[test]
fn a_refused_query_is_read_live_every_time() {
    let mut w = World::new("refused");
    w.put("k", "v");
    w.engine.refuse_all.store(true, Ordering::SeqCst);
    assert_eq!(w.read("k").as_deref(), Some("v"));
    w.put("k", "w");
    assert_eq!(
        w.read("k").as_deref(),
        Some("w"),
        "live, so the change is seen"
    );
    assert_eq!(w.cache.stats.refused.load(Ordering::Relaxed), 1);
    assert_eq!(w.cache.stats.refused_recently.load(Ordering::Relaxed), 1);
    assert_eq!(w.engine.subscribed.lock().unwrap().len(), 0);
    assert!(w.cache.is_empty());

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

/// Entries over the byte budget are evicted, and their subscriptions end.
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
    assert_eq!(
        unsubscribed as u64,
        20 - held,
        "every evicted entry ended its subscription"
    );
    assert_eq!(
        w.cache.stats.evicted.load(Ordering::Relaxed),
        unsubscribed as u64
    );
}

/// Cached rows decode into whole structs, by name, and as counts.
#[test]
fn cached_rows_decode_whole_structs_by_name_and_count() {
    let mut w = World::new("shapes");
    w.put("p", "1");
    w.put("q", "2");
    let like = format!("{}%", w.prefix);

    let whole = || {
        sg_configs::table
            .filter(sg_configs::key.like(&like))
            .order(sg_configs::key)
            .select(Config::as_select())
    };
    let first: Vec<Config> = whole().load(&mut w.conn).expect("load");
    let second: Vec<Config> = whole().load(&mut w.conn).expect("load again");
    assert_eq!(first, second);
    assert_eq!(
        first.iter().map(|c| c.value.as_str()).collect::<Vec<_>>(),
        ["1", "2"]
    );

    let count = || {
        sg_configs::table
            .filter(sg_configs::key.like(&like))
            .count()
    };
    let n: i64 = count().get_result(&mut w.conn).expect("count");
    let m: i64 = count().get_result(&mut w.conn).expect("count again");
    assert_eq!((n, m), (2, 2));

    /// Two columns of a config, read by name.
    #[derive(QueryableByName, Debug, PartialEq)]
    struct Pair {
        #[diesel(sql_type = Text)]
        key: String,
        #[diesel(sql_type = Integer)]
        id: i32,
    }
    let by_name = || {
        sql_query("SELECT key, id FROM sg_configs WHERE key LIKE $1 ORDER BY key")
            .bind::<Text, _>(&like)
    };
    let a: Vec<Pair> = by_name().load(&mut w.conn).expect("by name");
    let b: Vec<Pair> = by_name().load(&mut w.conn).expect("by name again");
    assert_eq!(a, b);
    assert_eq!(a[0].key, w.key("p"));
    assert_eq!(w.cache.stats.hits.load(Ordering::Relaxed), 3);
}

/// Clearing the cache makes every next read refill.
#[test]
fn clearing_the_cache_refills_everything() {
    let mut w = World::new("clear");
    w.put("x", "1");
    w.read("x");
    w.put("x", "2");
    w.cache.clear();
    assert_eq!(w.read("x").as_deref(), Some("2"));
}

/// A query flagged Postgres bypasses the cache on a connection routed
/// through Streamgres.
///
/// The flag wins over the connection: Postgres is read, the cached entry is
/// neither used nor replaced, and nothing is subscribed.
#[test]
fn a_query_flagged_postgres_bypasses_the_cache_on_a_streamgres_connection() {
    let mut w = World::new("via-pg");
    w.put("mode", "a");
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    w.put("mode", "b");

    let sent = w.sent();
    let subscribed = w.engine.subscribed.lock().unwrap().len();
    assert_eq!(w.read_via("mode", Route::Postgres).as_deref(), Some("b"));
    assert_eq!(w.sent(), sent + 1);
    assert_eq!(w.engine.subscribed.lock().unwrap().len(), subscribed);
    assert_eq!(
        w.read("mode").as_deref(),
        Some("a"),
        "the entry is untouched"
    );
}

/// A query flagged Streamgres is cached on a connection routed to Postgres.
#[test]
fn a_query_flagged_streamgres_is_cached_on_a_postgres_connection() {
    let mut w = World::new("via-sg");
    w.conn.set_route(Route::Postgres);
    w.put("mode", "a");
    assert_eq!(w.read_via("mode", Route::Streamgres).as_deref(), Some("a"));
    assert_eq!(w.cache.len(), 1, "subscribed and filled");
    w.put("mode", "b");
    assert_eq!(
        w.read_via("mode", Route::Streamgres).as_deref(),
        Some("a"),
        "served from the cache"
    );
    assert_eq!(
        w.read("mode").as_deref(),
        Some("b"),
        "the connection's own route is Postgres"
    );
}

/// The route marker is part of neither the cache key nor the SQL the engine
/// sees.
#[test]
fn the_marker_is_not_part_of_the_key_or_what_the_engine_sees() {
    let mut w = World::new("via-key");
    w.put("mode", "a");
    assert_eq!(w.read_via("mode", Route::Streamgres).as_deref(), Some("a"));
    let subscribed = w.engine.subscribed.lock().unwrap().clone();
    assert_eq!(subscribed.len(), 1);
    assert!(!subscribed[0].sql.contains("/*"), "{}", subscribed[0].sql);
    assert_eq!(
        subscribed[0],
        w.key_of("mode"),
        "flagged and unflagged reads share the entry"
    );
    let sent = w.sent();
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    assert_eq!(
        w.sent(),
        sent,
        "the unflagged read hit the flagged read's entry"
    );
}

/// A route scope covers every read inside it, and a query's own flag still
/// wins.
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
    assert_eq!(
        flagged.as_deref(),
        Some("a"),
        "the query flag beat the scope"
    );
    assert_eq!(w.read("mode").as_deref(), Some("a"), "the scope ended");
}

/// An entry nobody reads expires on the idle clock, and its subscription
/// ends.
///
/// Reads keep it alive; once it has expired, the next read subscribes
/// again.
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

    w.put("mode", "b");
    assert_eq!(w.read("mode").as_deref(), Some("b"));
    assert_eq!(w.engine.subscribed.lock().unwrap().len(), 2);
}

/// The time-to-live clock drops an entry however often it is read.
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
    assert_eq!(
        w.engine.unsubscribed.lock().unwrap().len(),
        1,
        "ended on the timer"
    );
    w.put("mode", "b");
    assert_eq!(
        w.read("mode").as_deref(),
        Some("b"),
        "resubscribed and refilled"
    );
    assert_eq!(w.engine.subscribed.lock().unwrap().len(), 2);
}

/// After its own write the connection reads Postgres for the read-your-
/// writes window.
///
/// The write goes through the synced connection itself.
///
/// The window closes; the entry was neither used nor refreshed by the
/// bypass, so without an engine touch the old rows are back.
///
/// A transaction's writes count from its COMMIT.
#[test]
fn after_a_write_the_connection_reads_postgres_for_a_window() {
    let mut w = World::new("ryw");
    w.conn
        .set_read_your_writes(std::time::Duration::from_millis(150));
    w.put("mode", "a");
    assert_eq!(w.read("mode").as_deref(), Some("a"));

    let key = w.key("mode");
    diesel::update(sg_configs::table.filter(sg_configs::key.eq(&key)))
        .set(sg_configs::value.eq("b"))
        .execute(&mut w.conn)
        .unwrap();
    let sent = w.sent();
    assert_eq!(
        w.read("mode").as_deref(),
        Some("b"),
        "its own write is seen at once"
    );
    assert_eq!(w.sent(), sent + 1, "read from Postgres");
    assert_eq!(w.cache.stats.after_write.load(Ordering::Relaxed), 1);

    std::thread::sleep(std::time::Duration::from_millis(200));
    let sent = w.sent();
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    assert_eq!(w.sent(), sent, "served from the cache again");

    w.conn
        .transaction::<_, diesel::result::Error, _>(|conn| {
            diesel::update(sg_configs::table.filter(sg_configs::key.eq(&key)))
                .set(sg_configs::value.eq("c"))
                .execute(conn)?;
            Ok(())
        })
        .unwrap();
    assert_eq!(w.read("mode").as_deref(), Some("c"));
}

/// With the engine side saying how far it has delivered, a read after a
/// write goes to Postgres until that position is past the write's commit,
/// and to the cache as soon as it is, however long either takes: the
/// window plays no part.
///
/// The first read after a write looks the write's position up, one more
/// query; the reads after it do not.
///
/// The engine side gets past the write: the cache answers (with its old
/// rows, the fake engine never touching them).
///
/// A further write is looked up again.
#[test]
fn after_a_write_reads_wait_for_the_engine_side_to_deliver_it() {
    let mut w = World::new("ryw-position");
    w.conn
        .set_read_your_writes(std::time::Duration::from_millis(50));
    w.engine.position.store(1, Ordering::SeqCst);
    w.put("mode", "a");
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    let sent = w.sent();
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    assert_eq!(w.sent(), sent, "no write yet: from the cache");

    let key = w.key("mode");
    diesel::update(sg_configs::table.filter(sg_configs::key.eq(&key)))
        .set(sg_configs::value.eq("b"))
        .execute(&mut w.conn)
        .unwrap();
    let sent = w.sent();
    assert_eq!(w.read("mode").as_deref(), Some("b"));
    assert_eq!(w.sent(), sent + 2, "the write's position, then the read");
    std::thread::sleep(std::time::Duration::from_millis(100));
    let sent = w.sent();
    assert_eq!(
        w.read("mode").as_deref(),
        Some("b"),
        "past the window, still from Postgres: not delivered yet"
    );
    assert_eq!(w.sent(), sent + 1, "the position is known");
    assert_eq!(w.cache.stats.after_write.load(Ordering::Relaxed), 2);

    w.engine.position.store(u64::MAX, Ordering::SeqCst);
    let sent = w.sent();
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    assert_eq!(w.sent(), sent, "delivered: from the cache");

    diesel::update(sg_configs::table.filter(sg_configs::key.eq(&key)))
        .set(sg_configs::value.eq("c"))
        .execute(&mut w.conn)
        .unwrap();
    let sent = w.sent();
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    assert_eq!(
        w.sent(),
        sent + 1,
        "looked up, already delivered: from the cache"
    );
    assert_eq!(w.cache.stats.after_write.load(Ordering::Relaxed), 2);
}

/// A locking read always goes to Postgres.
#[test]
fn a_locking_read_always_goes_to_postgres() {
    let mut w = World::new("locking");
    w.put("row", "a");
    let key = w.key("row");
    for _ in 0..2 {
        let seen: Option<String> = sg_configs::table
            .filter(sg_configs::key.eq(&key))
            .select(sg_configs::value)
            .for_update()
            .first(&mut w.conn)
            .optional()
            .unwrap();
        assert_eq!(seen.as_deref(), Some("a"));
    }
    assert_eq!(w.cache.stats.locking.load(Ordering::Relaxed), 2);
    assert!(w.cache.is_empty(), "nothing cached");
    assert!(
        w.engine.subscribed.lock().unwrap().is_empty(),
        "nothing subscribed"
    );
}

/// A route scope restores the outer route when its closure panics.
#[test]
fn a_route_scope_ends_when_the_closure_panics() {
    let mut w = World::new("scope-panic");
    w.put("mode", "a");
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    w.put("mode", "b");
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        w.conn
            .with_route(Route::Postgres, |_| panic!("inside the scope"));
    }));
    assert!(outcome.is_err());
    assert_eq!(
        w.read("mode").as_deref(),
        Some("a"),
        "the scope was restored, the cache answers"
    );
}

/// Opening and abandoning a transaction is not a write: it opens no read-
/// your-writes window.
#[test]
fn opening_and_abandoning_a_transaction_is_not_a_write() {
    let mut w = World::new("txn-control");
    w.conn
        .set_read_your_writes(std::time::Duration::from_secs(5));
    w.put("mode", "a");
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    w.put("mode", "b");
    w.conn.batch_execute("BEGIN").unwrap();
    w.conn.batch_execute("ROLLBACK").unwrap();
    w.conn.batch_execute("SET statement_timeout = 0").unwrap();
    w.conn.batch_execute("SELECT 1").unwrap();
    diesel::select(diesel::dsl::sql::<Integer>("1"))
        .execute(&mut w.conn)
        .unwrap();
    assert_eq!(
        w.read("mode").as_deref(),
        Some("a"),
        "still from the cache: a pool's checkout ping is not a write"
    );
    assert_eq!(w.cache.stats.after_write.load(Ordering::Relaxed), 0);
    let key = w.key("other");
    w.conn
        .batch_execute(&format!(
            "INSERT INTO sg_configs (key, value) VALUES ('{key}', 'x') ON CONFLICT (key) DO NOTHING"
        ))
        .unwrap();
    assert_eq!(
        w.read("mode").as_deref(),
        Some("b"),
        "any other raw SQL counts as a write"
    );
    assert_eq!(w.cache.stats.after_write.load(Ordering::Relaxed), 1);
}

/// A write on one connection is seen by reads on another connection of the
/// same cache.
///
/// The second connection stands for another of the pool's: the same
/// cache, its own socket.
#[test]
fn a_write_on_one_connection_is_seen_by_reads_on_another_of_the_same_cache() {
    let mut w = World::new("ryw-pool");
    w.conn
        .set_read_your_writes(std::time::Duration::from_secs(5));
    w.put("mode", "a");
    assert_eq!(w.read("mode").as_deref(), Some("a"));

    let mut sibling = SyncedConnection::with_cache(
        PgConnection::establish(&url()).expect("connect"),
        Arc::clone(&w.cache),
        Consistency::Eventual,
    );
    let key = w.key("mode");
    diesel::update(sg_configs::table.filter(sg_configs::key.eq(&key)))
        .set(sg_configs::value.eq("b"))
        .execute(&mut sibling)
        .unwrap();
    assert_eq!(
        w.read("mode").as_deref(),
        Some("b"),
        "the first connection reads Postgres for the window"
    );
    assert_eq!(w.cache.stats.after_write.load(Ordering::Relaxed), 1);
}

/// The window runs from a write's end, not its start: a write that takes
/// longer than the window still sends the next read to Postgres. A
/// batch that starts with a `SELECT` writes all the same.
#[test]
fn a_slow_write_opens_the_window_as_it_ends() {
    let mut w = World::new("ryw-slow");
    w.conn
        .set_read_your_writes(std::time::Duration::from_millis(300));
    w.put("mode", "a");
    assert_eq!(w.read("mode").as_deref(), Some("a"));
    let key = w.key("mode");
    w.conn
        .batch_execute(&format!(
            "SELECT pg_sleep(0.5); UPDATE sg_configs SET value = 'b' WHERE key = '{key}'"
        ))
        .unwrap();
    assert_eq!(
        w.read("mode").as_deref(),
        Some("b"),
        "read from Postgres: the write ended inside the window"
    );
}

/// A read that fails after the engine accepted it ends its subscription.
#[test]
fn a_read_that_fails_after_the_engine_accepted_ends_its_subscription() {
    let mut w = World::new("accepted-error");
    let failing = sql_query("SELECT no_such_column FROM sg_configs WHERE key = $1")
        .bind::<Text, _>(w.key("x"));
    let outcome: Result<Vec<Config>, _> = failing.load(&mut w.conn);
    assert!(outcome.is_err());
    assert_eq!(
        w.engine.subscribed.lock().unwrap().len(),
        1,
        "the engine accepted it"
    );
    assert_eq!(
        w.engine.unsubscribed.lock().unwrap().len(),
        1,
        "and was told to drop it"
    );
    assert!(w.cache.is_empty());
}

/// A refusal is remembered for the query with the values it was for:
/// the engine refuses on values too (a time at infinity, more rows than
/// it follows), so another value is asked about afresh.
#[test]
fn a_refusal_is_remembered_for_its_values_not_the_shape() {
    let mut w = World::new("refused-values");
    w.put("a", "1");
    w.put("b", "2");
    w.engine.refuse_all.store(true, Ordering::SeqCst);
    assert_eq!(w.read("a").as_deref(), Some("1"));
    w.engine.refuse_all.store(false, Ordering::SeqCst);
    assert_eq!(w.read("a").as_deref(), Some("1"));
    assert_eq!(w.read("b").as_deref(), Some("2"));
    assert_eq!(
        w.cache.stats.refused.load(Ordering::Relaxed),
        1,
        "asked once about `a`"
    );
    assert_eq!(
        w.cache.stats.refused_recently.load(Ordering::Relaxed),
        1,
        "`a` again skipped the ask"
    );
    assert_eq!(
        w.engine.subscribed.lock().unwrap().len(),
        1,
        "`b` was asked about, and accepted"
    );
}

/// What a full cache keeps is decided by how often a key is asked for,
/// not by its size or age: a key asked for again and again gets in,
/// displacing an entry read once, and a run of keys each asked for
/// once cannot displace it. (moka counts reads, misses included, once
/// the cache is past half full; the pending work is run by hand so the
/// counts are applied before each decision.)
#[test]
fn a_hot_entry_outlives_cold_ones_under_the_budget() {
    let mut w = World::with_budget("hot", 2000);
    for i in 0..8 {
        w.put(&format!("p{i}"), &"x".repeat(100));
        assert_eq!(w.read(&format!("p{i}")).unwrap().len(), 100);
        w.cache.run_pending_tasks();
    }
    let full = w.cache.len();
    assert!(
        (2..8).contains(&full),
        "the budget holds some, not all: {full}"
    );
    assert!(w.cache.weighted_size() > 1000, "past half full");

    w.put("hot", &"h".repeat(100));
    let mut admitted_after = None;
    for read in 1..=8 {
        assert_eq!(w.read("hot").unwrap().len(), 100);
        w.cache.run_pending_tasks();
        if w.conn.last_served() == Some(Served::Cache) {
            admitted_after = Some(read);
            break;
        }
    }
    let admitted_after = admitted_after.expect("a key asked for repeatedly gets in");
    assert!(
        admitted_after > 1,
        "not on its first miss, the cache being full"
    );

    for i in 0..20 {
        w.put(&format!("c{i}"), &"x".repeat(100));
        assert_eq!(w.read(&format!("c{i}")).unwrap().len(), 100);
        w.cache.run_pending_tasks();
    }
    assert!(w.cache.len() <= full + 1, "the budget still holds");
    let hits = w.cache.stats.hits.load(Ordering::Relaxed);
    assert_eq!(w.read("hot").unwrap().len(), 100);
    assert_eq!(
        w.conn.last_served(),
        Some(Served::Cache),
        "the hot entry outlived the cold ones; held {}",
        w.cache.len()
    );
    assert_eq!(w.cache.stats.hits.load(Ordering::Relaxed), hits + 1);
}
