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
use streamgres_diesel::subscriptions::Subscriptions;
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

/// Changes the engine reported: touches (refill mode) plus replacements
/// (rows mode).
fn changes(cache: &Cache) -> u64 {
    cache.stats.touched.load(Ordering::Relaxed) + cache.stats.replaced.load(Ordering::Relaxed)
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
    assert!(changes(&live.cache) >= 1);
    assert_eq!(live.engine.subscribed(), 1, "a change keeps the subscription");

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
    // The change made while the socket was down reaches the cache: in
    // rows mode the entry is refilled from the engine's snapshot and then
    // its deltas, so the read is eventual by the engine's propagation.
    let took = live
        .read_until("v", "2", Duration::from_secs(10))
        .expect("the change made during the restart was seen");
    eprintln!("after restart, change seen within: {took:?}");
    assert_eq!(live.engine.subscribed(), 1, "resubscribed under the same key");
    assert_eq!(live.cache.len(), 1, "the entry is back");
    let changes_before = changes(&live.cache);
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
        changes(&live.cache) > changes_before,
        "the refill came from the engine, not from a missing entry"
    );
}

struct CountQueries(Arc<std::sync::atomic::AtomicUsize>);

impl diesel::connection::Instrumentation for CountQueries {
    fn on_connection_event(&mut self, event: diesel::connection::InstrumentationEvent<'_>) {
        if let diesel::connection::InstrumentationEvent::StartQuery { .. } = event {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[test]
fn rows_mode_reads_postgres_only_for_the_table_types() {
    let Some(mut live) = Live::start("rows") else {
        return;
    };
    let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    live.conn.inner().set_instrumentation(CountQueries(sent.clone()));
    live.put("r", "1");

    assert_eq!(live.read("r").as_deref(), Some("1"));
    assert_eq!(live.engine.materialized(), 1, "served from the engine's rows");
    assert_eq!(
        sent.load(Ordering::SeqCst),
        1,
        "the only statement Postgres saw was the column-type lookup, not the rows"
    );
    assert_eq!(live.read("r").as_deref(), Some("1"));

    live.put("r", "2");
    let took = live
        .read_until("r", "2", Duration::from_secs(10))
        .expect("the change arrived as rows");
    eprintln!("ROWS MODE commit to replaced entry: {took:?}");
    assert_eq!(sent.load(Ordering::SeqCst), 1, "no Postgres read for the change");
    assert!(live.cache.stats.replaced.load(Ordering::Relaxed) >= 1, "the change replaced the entry");
    assert_eq!(live.cache.stats.touched.load(Ordering::Relaxed), 0);

    // A second query on the same table reuses the types: no lookup.
    live.put("r2", "x");
    assert_eq!(live.read("r2").as_deref(), Some("x"));
    assert_eq!(sent.load(Ordering::SeqCst), 1);
    assert_eq!(live.engine.materialized(), 2);
}

#[test]
fn ordered_limited_and_counted_results_match_postgres() {
    let Some(mut live) = Live::start("order") else {
        return;
    };
    for (name, value) in [("k1", "m"), ("k2", "z"), ("k3", "a"), ("k4", "q")] {
        live.put(name, value);
    }
    let keys: Vec<String> = ["k1", "k2", "k3", "k4"].iter().map(|k| live.key(k)).collect();
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
    assert_eq!(expected.iter().map(|c| c.value.as_str()).collect::<Vec<_>>(), ["z", "q"]);

    // A change that reorders the window.
    live.put("k3", "zz");
    let started = Instant::now();
    loop {
        let now = top(&mut live.conn, &keys);
        if now.iter().map(|c| c.value.as_str()).collect::<Vec<_>>() == ["zz", "z"] {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(10), "window never reordered: {now:?}");
        std::thread::sleep(Duration::from_millis(10));
    }

    // A count.
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
        assert!(started.elapsed() < Duration::from_secs(10), "count never dropped");
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
    }
}

#[test]
fn a_microsecond_timestamp_column_falls_back_to_refills() {
    let Some(mut live) = Live::start("fallback") else {
        return;
    };
    live.other
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS sg_items (
                id serial PRIMARY KEY, name text NOT NULL, qty int NOT NULL, note text,
                tags text[] NOT NULL DEFAULT '{}', payload jsonb NOT NULL DEFAULT '{}',
                created timestamptz NOT NULL DEFAULT now())",
        )
        .unwrap();
    let name = live.key("item");
    diesel::delete(sg_items::table.filter(sg_items::name.eq(&name)))
        .execute(&mut live.other)
        .unwrap();
    diesel::insert_into(sg_items::table)
        .values((
            sg_items::name.eq(&name),
            sg_items::qty.eq(1),
            sg_items::tags.eq(vec!["a", "b"]),
            sg_items::payload.eq(serde_json::json!({"k": [1, 2]})),
        ))
        .execute(&mut live.other)
        .unwrap();
    let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    live.conn.inner().set_instrumentation(CountQueries(sent.clone()));

    // Projecting `created` (microsecond precision) cannot be served from
    // engine rows: the read goes to Postgres and refills on touches.
    let with_created = |conn: &mut SyncedConnection<PgConnection>| -> (i32, chrono::DateTime<chrono::Utc>) {
        sg_items::table
            .filter(sg_items::name.eq(&name))
            .select((sg_items::qty, sg_items::created))
            .first(conn)
            .expect("with created")
    };
    let first = with_created(&mut live.conn);
    assert_eq!(live.engine.materialized(), 0, "fell back to refills");
    assert!(
        live.engine.fallbacks().last().is_some_and(|why| why.contains("finer than milliseconds")),
        "{:?}",
        live.engine.fallbacks()
    );
    assert_eq!(live.engine.subscribed(), 1);
    assert_eq!(sent.load(Ordering::SeqCst), 2, "the type lookup and the row read");
    // The insert committed just before the subscribe may be reported once
    // more after the fill (the documented redundant touch), costing one
    // refill; after that the entry holds.
    assert_eq!(with_created(&mut live.conn), first, "and cached");
    assert!(sent.load(Ordering::SeqCst) <= 3);
    let settled = sent.load(Ordering::SeqCst);
    assert_eq!(with_created(&mut live.conn), first);
    assert_eq!(sent.load(Ordering::SeqCst), settled, "served from the cache");

    // Not projecting it: arrays and jsonb are encodable, so engine rows.
    let without = |conn: &mut SyncedConnection<PgConnection>| -> (i32, Vec<String>, serde_json::Value) {
        sg_items::table
            .filter(sg_items::name.eq(&name))
            .select((sg_items::qty, sg_items::tags, sg_items::payload))
            .first(conn)
            .expect("without created")
    };
    assert_eq!(without(&mut live.conn), (1, vec!["a".into(), "b".into()], serde_json::json!({"k": [1, 2]})));
    assert_eq!(live.engine.materialized(), 1);
    assert_eq!(sent.load(Ordering::SeqCst), 2, "no Postgres read for the rows-mode query");

    diesel::update(sg_items::table.filter(sg_items::name.eq(&name)))
        .set((sg_items::qty.eq(2), sg_items::tags.eq(vec!["c"])))
        .execute(&mut live.other)
        .unwrap();
    let started = Instant::now();
    loop {
        let a = with_created(&mut live.conn).0;
        let b = without(&mut live.conn);
        if a == 2 && b.0 == 2 {
            assert_eq!(b.1, vec!["c".to_string()]);
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(10), "the update never arrived: {a} {b:?}");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(sent.load(Ordering::SeqCst), 3, "one refill read for the fallback query, none for rows mode");
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
    let names: Vec<String> = ["ann", "bob", "cy"].iter().map(|n| format!("{p}{n}")).collect();
    // Clean, then seed: ann has two books, bob none, cy one.
    diesel::delete(sg_books::table.filter(sg_books::title.like(format!("{p}%")))).execute(&mut plain).unwrap();
    diesel::delete(sg_authors::table.filter(sg_authors::name.like(format!("{p}%")))).execute(&mut plain).unwrap();
    let ids: Vec<i32> = diesel::insert_into(sg_authors::table)
        .values(names.iter().map(|n| sg_authors::name.eq(n)).collect::<Vec<_>>())
        .returning(sg_authors::id)
        .get_results(&mut plain)
        .unwrap();
    diesel::insert_into(sg_books::table)
        .values(vec![
            (sg_books::author_id.eq(ids[0]), sg_books::title.eq(format!("{p}a1"))),
            (sg_books::author_id.eq(ids[0]), sg_books::title.eq(format!("{p}a2"))),
            (sg_books::author_id.eq(ids[2]), sg_books::title.eq(format!("{p}c1"))),
        ])
        .execute(&mut plain)
        .unwrap();
    let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    live.conn.inner().set_instrumentation(CountQueries(sent.clone()));

    // authors.inner_join(books), filtered by the authors in play, ordered.
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
    assert_eq!(live.engine.materialized(), 1, "{:?}", live.engine.fallbacks());
    assert_eq!(sent.load(Ordering::SeqCst), 2, "two type lookups, no row read");
    assert_eq!(through(&mut live.conn, &names), expected);
    assert_eq!(sent.load(Ordering::SeqCst), 2);

    // A book for bob appears; one of ann's goes; cy is renamed.
    diesel::insert_into(sg_books::table)
        .values((sg_books::author_id.eq(ids[1]), sg_books::title.eq(format!("{p}b1"))))
        .execute(&mut plain)
        .unwrap();
    diesel::delete(sg_books::table.filter(sg_books::title.eq(format!("{p}a2")))).execute(&mut plain).unwrap();
    diesel::update(sg_authors::table.filter(sg_authors::id.eq(ids[2])))
        .set(sg_authors::name.eq(format!("{p}cyrus")))
        .execute(&mut plain)
        .unwrap();
    // The cached query still filters on the old names, so the renamed
    // author leaves it, as it leaves the same query on Postgres.
    let expected = inner(&mut plain, &names);
    assert_eq!(expected.len(), 2);
    let started = Instant::now();
    loop {
        let now = through(&mut live.conn, &names);
        if now == expected {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(10), "join never caught up: {now:?} vs {expected:?}");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(sent.load(Ordering::SeqCst), 2, "still no Postgres read");

    // A left join shows authors without books, and a count counts joined rows.
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
    assert_eq!(live.engine.materialized(), 3, "{:?}", live.engine.fallbacks());

    diesel::delete(sg_books::table.filter(sg_books::title.like(format!("{p}%")))).execute(&mut plain).unwrap();
    diesel::delete(sg_authors::table.filter(sg_authors::name.like(format!("{p}%")))).execute(&mut plain).unwrap();
}

/// A pool of sockets spreads the subscriptions across them, and a key
/// stays with the socket that first took it.
#[test]
fn a_pool_spreads_subscriptions_across_its_sockets() {
    let Some(mut live) = Live::start("pool") else {
        return;
    };
    let url = std::env::var("STREAMGRES_S2S_URL").unwrap();
    let database = std::env::var("DATABASE_URL").unwrap();
    let engine = EngineClient::with_config(
        url,
        streamgres_diesel::engine::Config {
            connections: 3,
            ..streamgres_diesel::engine::Config::default()
        },
    );
    let cache = Arc::new(Cache::new(64 * 1024 * 1024, Arc::new(engine.clone())));
    engine.attach(&cache);
    let deadline = Instant::now() + Duration::from_secs(5);
    while engine.connections() < 3 {
        assert!(Instant::now() < deadline, "only {} sockets connected", engine.connections());
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
        assert_eq!(read(&mut conn, live.key(&format!("p{i}"))).as_deref(), Some(format!("v{i}").as_str()));
    }
    assert_eq!(engine.subscribed(), 6);
    assert_eq!(engine.per_worker(), vec![2, 2, 2], "least-loaded routing");
    assert_eq!(engine.materialized(), 6, "{:?}", engine.fallbacks());

    // A change on a key held by any socket reaches the cache.
    let before = changes(&cache);
    live.put("p4", "v4b");
    let started = Instant::now();
    loop {
        if read(&mut conn, live.key("p4")).as_deref() == Some("v4b") {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(10), "the change on a pooled key was not seen");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(changes(&cache) > before);
    assert_eq!(engine.per_worker(), vec![2, 2, 2], "a change keeps the key where it was");
    assert_eq!(engine.subscribed(), 6);

    // Dropping one subscription frees its slot on its socket only.
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
fn query_key(conn: &mut SyncedConnection<PgConnection>, key: String) -> streamgres_diesel::key::QueryKey {
    let query = sg_configs::table
        .filter(sg_configs::key.eq(key))
        .select(sg_configs::value)
        .limit(1);
    streamgres_diesel::key::QueryKey::of(&query, conn.inner()).unwrap()
}

/// Past the subscription cap the engine is not asked and the read is
/// served live, like a refused query; a held key still comes from the
/// cache.
#[test]
fn the_subscription_cap_serves_the_rest_live() {
    let Some(mut live) = Live::start("cap") else {
        return;
    };
    let url = std::env::var("STREAMGRES_S2S_URL").unwrap();
    let database = std::env::var("DATABASE_URL").unwrap();
    let engine = EngineClient::with_config(
        url,
        streamgres_diesel::engine::Config {
            connections: 1,
            max_subscriptions: 2,
            ..streamgres_diesel::engine::Config::default()
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
        assert_eq!(read(&mut conn, live.key(&format!("c{i}"))).as_deref(), Some(format!("v{i}").as_str()));
    }
    assert_eq!(engine.subscribed(), 2, "the cap holds");
    assert_eq!(cache.len(), 2);
    assert_eq!(cache.stats.refused.load(Ordering::Relaxed), 1, "the third was refused by the client");

    // The third key is live every time, so a change is seen at once;
    // the held keys are cached.
    live.put("c2", "v2b");
    assert_eq!(read(&mut conn, live.key("c2")).as_deref(), Some("v2b"));
    let hits = cache.stats.hits.load(Ordering::Relaxed);
    assert_eq!(read(&mut conn, live.key("c0")).as_deref(), Some("v0"));
    assert_eq!(cache.stats.hits.load(Ordering::Relaxed), hits + 1);
    assert_eq!(engine.subscribed(), 2);

    // Freeing a slot lets the next miss subscribe.
    let key = query_key(&mut conn, live.key("c0"));
    engine.unsubscribe(&key);
    cache.touched(&key);
    assert_eq!(engine.subscribed(), 1);
    assert_eq!(read(&mut conn, live.key("c2")).as_deref(), Some("v2b"));
    assert_eq!(engine.subscribed(), 2, "the freed slot went to the next miss");
}
