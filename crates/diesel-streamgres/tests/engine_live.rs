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
use diesel_streamgres::cache::{Cache, DATABASE_SQL, Database};
use diesel_streamgres::engine::EngineClient;
use diesel_streamgres::subscriptions::Subscriptions;
use diesel_streamgres::{Consistency, Served, SyncedConnection};

diesel::table! {
    sg_configs (id) {
        id -> Int4,
        key -> Text,
        value -> Text,
    }
}

/// A config row.
#[derive(Queryable, Selectable, Debug, PartialEq, Clone)]
#[diesel(table_name = sg_configs)]
struct Config {
    id: i32,
    key: String,
    value: String,
}

/// One test against a live engine: the wrapped connection and its
/// cache, the engine client, a plain connection for changes, and the
/// prefix of its keys.
struct Live {
    conn: SyncedConnection<PgConnection>,
    cache: Arc<Cache>,
    engine: EngineClient,
    other: PgConnection,
    prefix: String,
}

impl Live {
    /// Connects to the engine `STREAMGRES_S2S_URL` names; `None`, and the
    /// test skipped, when it names none. The cache is told which database
    /// it reads up front, so the statements a test counts are its own.
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
        let identity = diesel::sql_query(DATABASE_SQL)
            .load::<Database>(&mut other)
            .expect("the database's identity");
        cache.learn_database(identity.into_iter().next());
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

    /// A key of this test's.
    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }

    /// Sets a key on the plain connection, as another service would.
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

/// Changes the engine reported: touches (refill mode) plus replacements
/// (rows mode).
fn changes(cache: &Cache) -> u64 {
    cache.stats.touched.load(Ordering::Relaxed) + cache.stats.replaced.load(Ordering::Relaxed)
}

impl Drop for Live {
    /// Deletes this test's keys.
    fn drop(&mut self) {
        let _ = diesel::delete(
            sg_configs::table.filter(sg_configs::key.like(format!("{}%", self.prefix))),
        )
        .execute(&mut self.other);
    }
}

/// A commit the engine reports makes the next read current: an update,
/// a delete, and a row appearing under the key the query asks for.
#[test]
fn a_commit_the_engine_reports_refills_the_cache() {
    let Some(mut live) = Live::start("refill") else {
        return;
    };
    live.put("rate", "1");

    assert_eq!(
        live.read("rate").as_deref(),
        Some("1"),
        "first read, from Postgres"
    );
    assert_eq!(live.engine.subscribed(), 1, "the engine accepted the query");
    assert_eq!(
        live.read("rate").as_deref(),
        Some("1"),
        "second read, from the cache"
    );
    assert_eq!(live.cache.stats.hits.load(Ordering::Relaxed), 1);

    live.put("rate", "2");
    let took = live
        .read_until("rate", "2", Duration::from_secs(10))
        .expect("the change reached the cache through the engine");
    eprintln!("commit to refilled read: {took:?}");
    assert!(changes(&live.cache) >= 1);
    assert_eq!(
        live.engine.subscribed(),
        1,
        "a change keeps the subscription"
    );

    diesel::delete(sg_configs::table.filter(sg_configs::key.eq(live.key("rate"))))
        .execute(&mut live.other)
        .expect("delete");
    let started = Instant::now();
    while live.read("rate").is_some() {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the delete never reached the cache"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    live.put("rate", "3");
    live.read_until("rate", "3", Duration::from_secs(10))
        .expect("the insert reached the cache");
}

/// A commit to a row the query does not read leaves the entry alone.
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

/// A query outside the engine's subset is refused and served live.
///
/// Raw SQL in the subset is accepted.
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
    assert_eq!(
        live.cache.stats.refused.load(Ordering::Relaxed),
        1,
        "LIKE is refused"
    );
    live.put("y", "1");
    assert_eq!(
        read(&mut live).len(),
        2,
        "live, so the new row is seen at once"
    );
    assert_eq!(live.engine.subscribed(), 0);

    /// The value column, read by name.
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

/// Evicting an entry ends its subscription with the engine.
///
/// A tiny budget: the second entry pushes the first out.
#[test]
fn evicting_an_entry_unsubscribes_it_from_the_engine() {
    let Some(mut live) = Live::start("evict") else {
        return;
    };
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
        assert!(
            Instant::now() < deadline,
            "subscribed {} held {held}",
            engine.subscribed()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Needs `STREAMGRES_RESTART_CMD`, a shell command that restarts the
/// engine and returns once it serves again.
///
/// While the engine was away, the socket dropped and the cache was cleared;
/// a change made meanwhile must be seen once reads resume.
///
/// The change made while the socket was down reaches the cache: in rows
/// mode the entry is refilled from the engine's snapshot and then its
/// deltas, so the read is eventual by the engine's propagation.
///
/// And the resubscription is live: a further change is reported.
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

    live.put("v", "2");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !live.engine.is_connected() {
        assert!(Instant::now() < deadline, "the client did not reconnect");
        std::thread::sleep(Duration::from_millis(50));
    }
    let took = live
        .read_until("v", "2", Duration::from_secs(10))
        .expect("the change made during the restart was seen");
    eprintln!("after restart, change seen within: {took:?}");
    assert_eq!(
        live.engine.subscribed(),
        1,
        "resubscribed under the same key"
    );
    assert_eq!(live.cache.len(), 1, "the entry is back");
    let changes_before = changes(&live.cache);
    let hits_before = live.cache.stats.hits.load(Ordering::Relaxed);
    assert_eq!(live.read("v").as_deref(), Some("2"));
    assert_eq!(
        live.cache.stats.hits.load(Ordering::Relaxed),
        hits_before + 1,
        "served from the cache"
    );

    live.put("v", "3");
    let took = live
        .read_until("v", "3", Duration::from_secs(10))
        .expect("the change reached the cache through the resubscribed engine");
    eprintln!("after restart, commit to refilled read: {took:?}");
    assert!(
        changes(&live.cache) > changes_before,
        "the refill came from the engine, not from a missing entry"
    );
}

/// Counts the queries the connection starts.
struct CountQueries(Arc<std::sync::atomic::AtomicUsize>);

impl diesel::connection::Instrumentation for CountQueries {
    /// Counts a query start.
    fn on_connection_event(&mut self, event: diesel::connection::InstrumentationEvent<'_>) {
        if let diesel::connection::InstrumentationEvent::StartQuery { .. } = event {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// In rows mode Postgres is read only for the table's column types, once.
///
/// A second query on the same table reuses the types: no lookup.
#[test]
fn rows_mode_reads_postgres_only_for_the_table_types() {
    let Some(mut live) = Live::start("rows") else {
        return;
    };
    let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    live.conn
        .inner()
        .set_instrumentation(CountQueries(sent.clone()));
    live.put("r", "1");

    assert_eq!(live.read("r").as_deref(), Some("1"));
    assert_eq!(
        live.engine.materialized(),
        1,
        "served from the engine's rows"
    );
    assert_eq!(live.conn.last_served(), Some(Served::Engine));
    assert_eq!(
        sent.load(Ordering::SeqCst),
        1,
        "the only statement Postgres saw was the column-type lookup, not the rows"
    );
    assert_eq!(live.read("r").as_deref(), Some("1"));
    assert_eq!(live.conn.last_served(), Some(Served::Cache));

    live.put("r", "2");
    let took = live
        .read_until("r", "2", Duration::from_secs(10))
        .expect("the change arrived as rows");
    eprintln!("ROWS MODE commit to replaced entry: {took:?}");
    assert_eq!(
        sent.load(Ordering::SeqCst),
        1,
        "no Postgres read for the change"
    );
    assert!(
        live.cache.stats.replaced.load(Ordering::Relaxed) >= 1,
        "the change replaced the entry"
    );
    assert_eq!(live.cache.stats.touched.load(Ordering::Relaxed), 0);

    live.put("r2", "x");
    assert_eq!(live.read("r2").as_deref(), Some("x"));
    assert_eq!(sent.load(Ordering::SeqCst), 1);
    assert_eq!(live.engine.materialized(), 2);
}

/// Ordered, limited and counted results served from the engine's rows match
/// Postgres.
///
/// A change that reorders the window.
///
/// A count.
#[test]
fn ordered_limited_and_counted_results_match_postgres() {
    let Some(mut live) = Live::start("order") else {
        return;
    };
    for (name, value) in [("k1", "m"), ("k2", "z"), ("k3", "a"), ("k4", "q")] {
        live.put(name, value);
    }
    let keys: Vec<String> = ["k1", "k2", "k3", "k4"]
        .iter()
        .map(|k| live.key(k))
        .collect();
    let database = std::env::var("DATABASE_URL").unwrap();
    let mut plain = PgConnection::establish(&database).unwrap();

    let top = |conn: &mut SyncedConnection<PgConnection>, keys: &[String]| -> Vec<Config> {
        sg_configs::table
            .filter(sg_configs::key.eq_any(keys))
            .order(sg_configs::value.desc())
            .limit(2)
            .select(Config::as_select())
            .load(conn)
            .expect("top")
    };
    let expected: Vec<Config> = sg_configs::table
        .filter(sg_configs::key.eq_any(&keys))
        .order(sg_configs::value.desc())
        .limit(2)
        .select(Config::as_select())
        .load(&mut plain)
        .unwrap();
    assert_eq!(top(&mut live.conn, &keys), expected);
    assert_eq!(live.engine.materialized(), 1);
    assert_eq!(
        expected
            .iter()
            .map(|c| c.value.as_str())
            .collect::<Vec<_>>(),
        ["z", "q"]
    );

    live.put("k3", "zz");
    let started = Instant::now();
    loop {
        let now = top(&mut live.conn, &keys);
        if now.iter().map(|c| c.value.as_str()).collect::<Vec<_>>() == ["zz", "z"] {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "window never reordered: {now:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let count = |conn: &mut SyncedConnection<PgConnection>| -> i64 {
        sg_configs::table
            .filter(sg_configs::key.eq_any(&keys))
            .count()
            .get_result(conn)
            .expect("count")
    };
    assert_eq!(count(&mut live.conn), 4);
    assert_eq!(live.engine.materialized(), 2);
    diesel::delete(sg_configs::table.filter(sg_configs::key.eq(live.key("k1"))))
        .execute(&mut live.other)
        .unwrap();
    let started = Instant::now();
    while count(&mut live.conn) != 3 {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "count never dropped"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

diesel::table! {
    sg_items (id) {
        id -> Int4,
        name -> Text,
        qty -> Int4,
        note -> Nullable<Text>,
        tags -> Array<Text>,
        payload -> Jsonb,
        created -> Timestamptz,
        lead -> Nullable<Interval>,
    }
}

/// Microsecond timestamps are served exactly; an interval column falls the
/// query back to refills.
///
/// The engine learns a new column from the DDL trigger; give it a moment.
///
/// This row and any left by an earlier run that died before its own
/// cleanup: the passthrough and owned-row suites read the whole table.
///
/// `created` is timestamptz(6): served from engine rows, to the
/// microsecond.
///
/// An interval has no encoding here: the read goes to Postgres and refills
/// on touches.
#[test]
fn microsecond_timestamps_are_exact_and_an_interval_falls_back() {
    let Some(mut live) = Live::start("fallback") else {
        return;
    };
    live.other
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS sg_items (
                id serial PRIMARY KEY, name text NOT NULL, qty int NOT NULL, note text,
                tags text[] NOT NULL DEFAULT '{}', payload jsonb NOT NULL DEFAULT '{}',
                created timestamptz NOT NULL DEFAULT now());
             ALTER TABLE sg_items ADD COLUMN IF NOT EXISTS lead interval",
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(500));
    let name = live.key("item");
    diesel::delete(sg_items::table.filter(sg_items::name.like("fallback-%")))
        .execute(&mut live.other)
        .unwrap();
    diesel::insert_into(sg_items::table)
        .values((
            sg_items::name.eq(&name),
            sg_items::qty.eq(1),
            sg_items::tags.eq(vec!["a", "b"]),
            sg_items::payload.eq(serde_json::json!({"k": [1, 2]})),
            sg_items::lead.eq(diesel::data_types::PgInterval::from_microseconds(
                3_600_000_000,
            )),
        ))
        .execute(&mut live.other)
        .unwrap();
    let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    live.conn
        .inner()
        .set_instrumentation(CountQueries(sent.clone()));

    let with_created =
        |conn: &mut SyncedConnection<PgConnection>| -> (i32, chrono::DateTime<chrono::Utc>) {
            sg_items::table
                .filter(sg_items::name.eq(&name))
                .select((sg_items::qty, sg_items::created))
                .first(conn)
                .expect("with created")
        };
    let first = with_created(&mut live.conn);
    let from_postgres: (i32, chrono::DateTime<chrono::Utc>) = sg_items::table
        .filter(sg_items::name.eq(&name))
        .select((sg_items::qty, sg_items::created))
        .first(&mut live.other)
        .unwrap();
    assert_eq!(first, from_postgres, "exact to the microsecond");
    assert_eq!(
        live.engine.materialized(),
        1,
        "rows mode: {:?}",
        live.engine.fallbacks()
    );
    assert_eq!(sent.load(Ordering::SeqCst), 1, "the type lookup only");
    assert_eq!(with_created(&mut live.conn), first, "and cached");
    assert_eq!(sent.load(Ordering::SeqCst), 1);

    let with_lead = |conn: &mut SyncedConnection<PgConnection>| -> (i32, Option<diesel::data_types::PgInterval>) {
        sg_items::table
            .filter(sg_items::name.eq(&name))
            .select((sg_items::qty, sg_items::lead))
            .first(conn)
            .expect("with lead")
    };
    let lead = with_lead(&mut live.conn);
    assert_eq!(lead.1.map(|i| i.microseconds), Some(3_600_000_000));
    assert_eq!(live.engine.materialized(), 1, "fell back to refills");
    assert!(
        live.engine
            .fallbacks()
            .last()
            .is_some_and(|why| why.contains("oid 1186")),
        "{:?}",
        live.engine.fallbacks()
    );
    assert_eq!(live.engine.subscribed(), 2);
    assert_eq!(sent.load(Ordering::SeqCst), 2, "the row read");
    assert_eq!(with_lead(&mut live.conn), lead, "and cached");
    assert!(
        sent.load(Ordering::SeqCst) <= 3,
        "at most one redundant refill"
    );
    let settled = sent.load(Ordering::SeqCst);

    diesel::update(sg_items::table.filter(sg_items::name.eq(&name)))
        .set((sg_items::qty.eq(2), sg_items::tags.eq(vec!["c"])))
        .execute(&mut live.other)
        .unwrap();
    let started = Instant::now();
    loop {
        let a = with_created(&mut live.conn).0;
        let b = with_lead(&mut live.conn).0;
        if a == 2 && b == 2 {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the update never arrived: {a} {b}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        sent.load(Ordering::SeqCst),
        settled + 1,
        "one refill read for the fallback query, none for rows mode"
    );
    diesel::delete(sg_items::table.filter(sg_items::name.eq(&name)))
        .execute(&mut live.other)
        .unwrap();
}

diesel::table! {
    sg_authors (id) {
        id -> Int4,
        name -> Text,
    }
}

diesel::table! {
    sg_books (id) {
        id -> Int4,
        author_id -> Int4,
        title -> Text,
    }
}

diesel::joinable!(sg_books -> sg_authors (author_id));
diesel::allow_tables_to_appear_in_same_query!(sg_authors, sg_books);

/// Joins are composed from the engine's rows and stay current as either
/// side changes.
///
/// Clean, then seed: ann has two books, bob none, cy one.
///
/// authors.inner_join(books), filtered by the authors in play, ordered.
///
/// A book for bob appears; one of ann's goes; cy is renamed.
///
/// The cached query still filters on the old names, so the renamed author
/// leaves it, as it leaves the same query on Postgres.
///
/// A left join shows authors without books, and a count counts joined rows.
#[test]
fn joins_are_composed_from_the_engines_rows() {
    let Some(mut live) = Live::start("join") else {
        return;
    };
    live.other
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS sg_authors (id serial PRIMARY KEY, name text NOT NULL);
             CREATE TABLE IF NOT EXISTS sg_books (id serial PRIMARY KEY, author_id int NOT NULL REFERENCES sg_authors(id), title text NOT NULL)",
        )
        .unwrap();
    let database = std::env::var("DATABASE_URL").unwrap();
    let mut plain = PgConnection::establish(&database).unwrap();
    let p = live.prefix.clone();
    let names: Vec<String> = ["ann", "bob", "cy"]
        .iter()
        .map(|n| format!("{p}{n}"))
        .collect();
    diesel::delete(sg_books::table.filter(sg_books::title.like(format!("{p}%"))))
        .execute(&mut plain)
        .unwrap();
    diesel::delete(sg_authors::table.filter(sg_authors::name.like(format!("{p}%"))))
        .execute(&mut plain)
        .unwrap();
    let ids: Vec<i32> = diesel::insert_into(sg_authors::table)
        .values(
            names
                .iter()
                .map(|n| sg_authors::name.eq(n))
                .collect::<Vec<_>>(),
        )
        .returning(sg_authors::id)
        .get_results(&mut plain)
        .unwrap();
    diesel::insert_into(sg_books::table)
        .values(vec![
            (
                sg_books::author_id.eq(ids[0]),
                sg_books::title.eq(format!("{p}a1")),
            ),
            (
                sg_books::author_id.eq(ids[0]),
                sg_books::title.eq(format!("{p}a2")),
            ),
            (
                sg_books::author_id.eq(ids[2]),
                sg_books::title.eq(format!("{p}c1")),
            ),
        ])
        .execute(&mut plain)
        .unwrap();
    let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    live.conn
        .inner()
        .set_instrumentation(CountQueries(sent.clone()));

    type Pair = (String, String);
    let inner = |conn: &mut PgConnection, names: &[String]| -> Vec<Pair> {
        sg_authors::table
            .inner_join(sg_books::table)
            .filter(sg_authors::name.eq_any(names))
            .order(sg_authors::id)
            .select((sg_authors::name, sg_books::title))
            .load(conn)
            .unwrap()
    };
    let through = |conn: &mut SyncedConnection<PgConnection>, names: &[String]| -> Vec<Pair> {
        sg_authors::table
            .inner_join(sg_books::table)
            .filter(sg_authors::name.eq_any(names))
            .order(sg_authors::id)
            .select((sg_authors::name, sg_books::title))
            .load(conn)
            .unwrap()
    };
    let expected = inner(&mut plain, &names);
    assert_eq!(expected.len(), 3);
    assert_eq!(through(&mut live.conn, &names), expected);
    assert_eq!(
        live.engine.materialized(),
        1,
        "{:?}",
        live.engine.fallbacks()
    );
    assert_eq!(
        sent.load(Ordering::SeqCst),
        2,
        "two type lookups, no row read"
    );
    assert_eq!(through(&mut live.conn, &names), expected);
    assert_eq!(sent.load(Ordering::SeqCst), 2);

    diesel::insert_into(sg_books::table)
        .values((
            sg_books::author_id.eq(ids[1]),
            sg_books::title.eq(format!("{p}b1")),
        ))
        .execute(&mut plain)
        .unwrap();
    diesel::delete(sg_books::table.filter(sg_books::title.eq(format!("{p}a2"))))
        .execute(&mut plain)
        .unwrap();
    diesel::update(sg_authors::table.filter(sg_authors::id.eq(ids[2])))
        .set(sg_authors::name.eq(format!("{p}cyrus")))
        .execute(&mut plain)
        .unwrap();
    let expected = inner(&mut plain, &names);
    assert_eq!(expected.len(), 2);
    let started = Instant::now();
    loop {
        let now = through(&mut live.conn, &names);
        if now == expected {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "join never caught up: {now:?} vs {expected:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(sent.load(Ordering::SeqCst), 2, "still no Postgres read");

    let mut names_now = names.clone();
    names_now[2] = format!("{p}cyrus");
    type Left = (String, Option<String>);
    let left: Vec<Left> = sg_authors::table
        .left_join(sg_books::table)
        .filter(sg_authors::name.eq_any(&names_now))
        .order(sg_authors::id)
        .select((sg_authors::name, sg_books::title.nullable()))
        .load(&mut live.conn)
        .unwrap();
    let left_expected: Vec<Left> = sg_authors::table
        .left_join(sg_books::table)
        .filter(sg_authors::name.eq_any(&names_now))
        .order(sg_authors::id)
        .select((sg_authors::name, sg_books::title.nullable()))
        .load(&mut plain)
        .unwrap();
    assert_eq!(left, left_expected);
    let count: i64 = sg_authors::table
        .inner_join(sg_books::table)
        .filter(sg_authors::name.eq_any(&names_now))
        .count()
        .get_result(&mut live.conn)
        .unwrap();
    assert_eq!(count, 3);
    assert_eq!(
        live.engine.materialized(),
        3,
        "{:?}",
        live.engine.fallbacks()
    );

    diesel::delete(sg_books::table.filter(sg_books::title.like(format!("{p}%"))))
        .execute(&mut plain)
        .unwrap();
    diesel::delete(sg_authors::table.filter(sg_authors::name.like(format!("{p}%"))))
        .execute(&mut plain)
        .unwrap();
}

/// A pool of sockets spreads the subscriptions across them, and a key
/// stays with the socket that first took it.
///
/// A change on a key held by any socket reaches the cache.
///
/// Dropping one subscription frees its slot on its socket only.
#[test]
fn a_pool_spreads_subscriptions_across_its_sockets() {
    let Some(mut live) = Live::start("pool") else {
        return;
    };
    let url = std::env::var("STREAMGRES_S2S_URL").unwrap();
    let database = std::env::var("DATABASE_URL").unwrap();
    let engine = EngineClient::with_config(
        url,
        diesel_streamgres::engine::Config {
            connections: 3,
            ..diesel_streamgres::engine::Config::from_env()
        },
    );
    let cache = Arc::new(Cache::new(64 * 1024 * 1024, Arc::new(engine.clone())));
    engine.attach(&cache);
    let deadline = Instant::now() + Duration::from_secs(5);
    while engine.connections() < 3 {
        assert!(
            Instant::now() < deadline,
            "only {} sockets connected",
            engine.connections()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let mut conn = SyncedConnection::with_cache(
        PgConnection::establish(&database).unwrap(),
        cache.clone(),
        Consistency::Eventual,
    );
    for i in 0..6 {
        live.put(&format!("p{i}"), &format!("v{i}"));
    }
    let read = |conn: &mut SyncedConnection<PgConnection>, key: String| -> Option<String> {
        sg_configs::table
            .filter(sg_configs::key.eq(key))
            .select(sg_configs::value)
            .first(conn)
            .optional()
            .unwrap()
    };
    for i in 0..6 {
        assert_eq!(
            read(&mut conn, live.key(&format!("p{i}"))).as_deref(),
            Some(format!("v{i}").as_str())
        );
    }
    assert_eq!(engine.subscribed(), 6);
    assert_eq!(engine.per_worker(), vec![2, 2, 2], "least-loaded routing");
    assert_eq!(engine.materialized(), 6, "{:?}", engine.fallbacks());

    let before = changes(&cache);
    live.put("p4", "v4b");
    let started = Instant::now();
    loop {
        if read(&mut conn, live.key("p4")).as_deref() == Some("v4b") {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the change on a pooled key was not seen"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(changes(&cache) > before);
    assert_eq!(
        engine.per_worker(),
        vec![2, 2, 2],
        "a change keeps the key where it was"
    );
    assert_eq!(engine.subscribed(), 6);

    let key = query_key(&mut conn, live.key("p4"));
    engine.unsubscribe(&key);
    cache.touched(&key);
    assert_eq!(engine.subscribed(), 5);
    assert_eq!(engine.per_worker().iter().sum::<usize>(), 5);
    assert_eq!(read(&mut conn, live.key("p4")).as_deref(), Some("v4b"));
    assert_eq!(engine.subscribed(), 6, "the next miss subscribes again");
}

/// The cache key of the test's lookup: what `first` renders, bind values
/// included.
fn query_key(
    conn: &mut SyncedConnection<PgConnection>,
    key: String,
) -> diesel_streamgres::key::QueryKey {
    let query = sg_configs::table
        .filter(sg_configs::key.eq(key))
        .select(sg_configs::value)
        .limit(1);
    diesel_streamgres::key::QueryKey::of(&query, conn.inner()).unwrap()
}

/// Past the subscription cap the engine is not asked and the read is
/// served live, like a refused query; a held key still comes from the
/// cache.
///
/// The third key is live every time, so a change is seen at once; the held
/// keys are cached.
///
/// Freeing a slot lets the next miss subscribe.
#[test]
fn the_subscription_cap_serves_the_rest_live() {
    let Some(mut live) = Live::start("cap") else {
        return;
    };
    let url = std::env::var("STREAMGRES_S2S_URL").unwrap();
    let database = std::env::var("DATABASE_URL").unwrap();
    let engine = EngineClient::with_config(
        url,
        diesel_streamgres::engine::Config {
            connections: 1,
            max_subscriptions: 2,
            ..diesel_streamgres::engine::Config::from_env()
        },
    );
    let cache = Arc::new(Cache::new(64 * 1024 * 1024, Arc::new(engine.clone())));
    engine.attach(&cache);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !engine.is_connected() {
        assert!(Instant::now() < deadline, "the engine did not connect");
        std::thread::sleep(Duration::from_millis(50));
    }
    let mut conn = SyncedConnection::with_cache(
        PgConnection::establish(&database).unwrap(),
        cache.clone(),
        Consistency::Eventual,
    );
    for i in 0..3 {
        live.put(&format!("c{i}"), &format!("v{i}"));
    }
    let read = |conn: &mut SyncedConnection<PgConnection>, key: String| -> Option<String> {
        sg_configs::table
            .filter(sg_configs::key.eq(key))
            .select(sg_configs::value)
            .first(conn)
            .optional()
            .unwrap()
    };
    for i in 0..3 {
        assert_eq!(
            read(&mut conn, live.key(&format!("c{i}"))).as_deref(),
            Some(format!("v{i}").as_str())
        );
    }
    assert_eq!(engine.subscribed(), 2, "the cap holds");
    assert_eq!(cache.len(), 2);
    assert_eq!(
        cache.stats.refused.load(Ordering::Relaxed),
        1,
        "the third was refused by the client"
    );

    live.put("c2", "v2b");
    assert_eq!(read(&mut conn, live.key("c2")).as_deref(), Some("v2b"));
    let hits = cache.stats.hits.load(Ordering::Relaxed);
    assert_eq!(read(&mut conn, live.key("c0")).as_deref(), Some("v0"));
    assert_eq!(cache.stats.hits.load(Ordering::Relaxed), hits + 1);
    assert_eq!(engine.subscribed(), 2);

    let key = query_key(&mut conn, live.key("c0"));
    engine.unsubscribe(&key);
    cache.touched(&key);
    assert_eq!(engine.subscribed(), 1);
    assert_eq!(read(&mut conn, live.key("c2")).as_deref(), Some("v2b"));
    assert_eq!(
        engine.subscribed(),
        2,
        "the freed slot went to the next miss"
    );
}

diesel::table! {
    sg_blobs (id) {
        id -> Int4,
        name -> Text,
        data -> Nullable<Bytea>,
        n -> Int4,
    }
}

/// `bytea` travels as hex text and comes back as bytes; `OFFSET` is
/// applied by the client over the rows the engine keeps.
///
/// A change inside the page arrives as a delta; the NULL row and the offset
/// still apply.
#[test]
fn bytea_and_offset_are_served_from_engine_rows() {
    let Some(mut live) = Live::start("blobs") else {
        return;
    };
    live.other
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS sg_blobs (
                id serial PRIMARY KEY, name text NOT NULL, data bytea, n int NOT NULL)",
        )
        .unwrap();
    let prefix = live.key("b");
    diesel::delete(sg_blobs::table.filter(sg_blobs::name.like(format!("{prefix}%"))))
        .execute(&mut live.other)
        .unwrap();
    let rows: Vec<_> = (0..5)
        .map(|i| {
            (
                sg_blobs::name.eq(format!("{prefix}{i}")),
                sg_blobs::data.eq(if i == 3 {
                    None
                } else {
                    Some(vec![i as u8, 0xff, 0x10])
                }),
                sg_blobs::n.eq(i),
            )
        })
        .collect();
    diesel::insert_into(sg_blobs::table)
        .values(&rows)
        .execute(&mut live.other)
        .unwrap();

    let names: Vec<String> = (0..5).map(|i| format!("{prefix}{i}")).collect();
    let page = |conn: &mut SyncedConnection<PgConnection>| -> Vec<(i32, Option<Vec<u8>>)> {
        sg_blobs::table
            .filter(sg_blobs::name.eq_any(&names))
            .select((sg_blobs::n, sg_blobs::data))
            .order(sg_blobs::n.asc())
            .limit(2)
            .offset(1)
            .load(conn)
            .unwrap()
    };
    let got = page(&mut live.conn);
    assert_eq!(
        got,
        vec![
            (1, Some(vec![1, 0xff, 0x10])),
            (2, Some(vec![2, 0xff, 0x10]))
        ],
        "skipped one, took two, bytes intact"
    );
    assert_eq!(
        live.engine.materialized(),
        1,
        "rows mode: {:?}",
        live.engine.fallbacks()
    );
    assert_eq!(live.engine.subscribed(), 1);

    diesel::update(sg_blobs::table.filter(sg_blobs::name.eq(format!("{prefix}2"))))
        .set(sg_blobs::data.eq(Some(vec![0xab])))
        .execute(&mut live.other)
        .unwrap();
    let started = Instant::now();
    loop {
        let got = page(&mut live.conn);
        if got[1].1.as_deref() == Some(&[0xab][..]) {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the delta did not arrive: {got:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let third: Vec<(i32, Option<Vec<u8>>)> = sg_blobs::table
        .filter(sg_blobs::name.eq_any(&names))
        .select((sg_blobs::n, sg_blobs::data))
        .order(sg_blobs::n.asc())
        .limit(1)
        .offset(3)
        .load(&mut live.conn)
        .unwrap();
    assert_eq!(third, vec![(3, None)], "a NULL bytea stays NULL");
    assert_eq!(
        live.engine.materialized(),
        2,
        "{:?}",
        live.engine.fallbacks()
    );
    let _ = diesel::delete(sg_blobs::table.filter(sg_blobs::name.like(format!("{prefix}%"))))
        .execute(&mut live.other);
}

/// A row inserted and deleted in one transaction is not kept.
///
/// One transaction: a matching row comes and goes, and another row changes
/// so the delta's arrival can be seen. The engine folds the ghost into an
/// add and a delete; one frame must not leave it behind.
#[test]
fn a_row_inserted_and_deleted_in_one_transaction_is_not_kept() {
    let Some(mut live) = Live::start("ghost") else {
        return;
    };
    live.put("real", "1");
    let names = vec![live.key("real"), live.key("ghost")];
    let page = |conn: &mut SyncedConnection<PgConnection>| -> Vec<(String, String)> {
        sg_configs::table
            .filter(sg_configs::key.eq_any(&names))
            .select((sg_configs::key, sg_configs::value))
            .order(sg_configs::key.asc())
            .load(conn)
            .unwrap()
    };
    assert_eq!(
        page(&mut live.conn),
        vec![(live.key("real"), "1".to_string())]
    );
    assert_eq!(
        live.engine.materialized(),
        1,
        "rows mode: {:?}",
        live.engine.fallbacks()
    );

    let (real, ghost) = (live.key("real"), live.key("ghost"));
    live.other
        .transaction::<_, diesel::result::Error, _>(|conn| {
            diesel::insert_into(sg_configs::table)
                .values((sg_configs::key.eq(&ghost), sg_configs::value.eq("boo")))
                .execute(conn)?;
            diesel::delete(sg_configs::table.filter(sg_configs::key.eq(&ghost))).execute(conn)?;
            diesel::update(sg_configs::table.filter(sg_configs::key.eq(&real)))
                .set(sg_configs::value.eq("2"))
                .execute(conn)?;
            Ok(())
        })
        .unwrap();
    let started = Instant::now();
    loop {
        let got = page(&mut live.conn);
        if got.iter().any(|(_, value)| value == "2") {
            assert_eq!(
                got,
                vec![(real.clone(), "2".to_string())],
                "the ghost row must not be in the result"
            );
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the delta did not arrive: {got:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        live.cache.stats.touched.load(Ordering::Relaxed),
        0,
        "served from engine rows throughout"
    );
}
