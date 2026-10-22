//! The async wrapper against a fake engine side: the same rules as the
//! blocking one, reached through diesel-async.

#![cfg(feature = "async")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use diesel_streamgres::cache::Cache;
use diesel_streamgres::key::QueryKey;
use diesel_streamgres::materialized::Snapshot;
use diesel_streamgres::subscriptions::{Outcome, Subscriptions, Verdict};
use diesel_streamgres::types::TableTypes;
use diesel_streamgres::{AsyncSyncedConnection, Route, RouteDsl, Served};
use scoped_futures::ScopedFutureExt;

/// diesel-async's `RunQueryDsl::load` shadows `AtomicU64::load`.
fn stat(counter: &AtomicU64) -> u64 {
    AtomicU64::load(counter, Ordering::Relaxed)
}

diesel::table! {
    sg_configs (id) {
        id -> Int4,
        key -> Text,
        value -> Text,
    }
}

/// A user-defined type: Diesel must look its OIDs up, and an array of it
/// cannot even be rendered until it has.
#[derive(diesel::sql_types::SqlType, diesel::query_builder::QueryId)]
#[diesel(postgres_type(name = "sg_mood"))]
pub struct SgMood;

/// A value of the `sg_mood` enum.
#[derive(Debug, Clone, Copy, PartialEq, diesel::AsExpression, diesel::FromSqlRow)]
#[diesel(sql_type = SgMood)]
enum Mood {
    Happy,
    Sad,
}

impl diesel::serialize::ToSql<SgMood, diesel::pg::Pg> for Mood {
    /// The label.
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, diesel::pg::Pg>,
    ) -> diesel::serialize::Result {
        use std::io::Write;
        out.write_all(match self {
            Mood::Happy => b"happy",
            Mood::Sad => b"sad",
        })?;
        Ok(diesel::serialize::IsNull::No)
    }
}

impl diesel::deserialize::FromSql<SgMood, diesel::pg::Pg> for Mood {
    /// The variant the label names.
    fn from_sql(bytes: diesel::pg::PgValue<'_>) -> diesel::deserialize::Result<Self> {
        match bytes.as_bytes() {
            b"happy" => Ok(Mood::Happy),
            b"sad" => Ok(Mood::Sad),
            other => Err(format!("not a mood: {other:?}").into()),
        }
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::SgMood;
    sg_moods (id) {
        id -> Int4,
        key -> Text,
        mood -> SgMood,
    }
}

/// The test database's URL.
fn url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres:///diesel_streamgres_test?host=/tmp".to_string())
}

/// An engine side that accepts every read of a test table, refuses the
/// rest, and records what it was asked.
#[derive(Default)]
struct FakeEngine {
    subscribed: Mutex<Vec<QueryKey>>,
    unsubscribed: Mutex<Vec<QueryKey>>,
}

impl Subscriptions for FakeEngine {
    /// Accepted for a test table, refused otherwise.
    fn subscribe(&self, key: &QueryKey) -> Outcome {
        if !key.sql.contains("sg_") {
            return Outcome::Verdict(Verdict::Refused("not a config read".into()));
        }
        self.subscribed.lock().unwrap().push(key.clone());
        Outcome::Verdict(Verdict::Accepted)
    }

    /// Accepted, with no rows.
    fn materialize(
        &self,
        _: &QueryKey,
        _: Snapshot,
        _: HashMap<String, Arc<TableTypes>>,
    ) -> Verdict {
        Verdict::Accepted
    }

    /// Records the key.
    fn unsubscribe(&self, key: &QueryKey) {
        self.unsubscribed.lock().unwrap().push(key.clone());
    }
}

/// One test's wrapped connection and cache, its fake engine, a plain
/// connection for changes, and the prefix of its keys.
struct World {
    conn: AsyncSyncedConnection,
    cache: Arc<Cache>,
    engine: Arc<FakeEngine>,
    other: AsyncPgConnection,
    prefix: String,
}

impl World {
    /// A world for test `name`, the schema created and its keys cleared.
    /// The tests run in parallel; an advisory lock serialises the DDL.
    async fn new(name: &str) -> World {
        let mut other = AsyncPgConnection::establish(&url()).await.expect("connect");
        other
            .batch_execute(
                "BEGIN;
                 SELECT pg_advisory_xact_lock(424242);
                 CREATE TABLE IF NOT EXISTS sg_configs (
                    id serial PRIMARY KEY, key text NOT NULL UNIQUE, value text NOT NULL);
                 DO $$ BEGIN CREATE TYPE sg_mood AS ENUM ('happy', 'sad');
                    EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$;
                 CREATE TABLE IF NOT EXISTS sg_moods (
                    id serial PRIMARY KEY, key text NOT NULL UNIQUE, mood sg_mood NOT NULL);
                 COMMIT",
            )
            .await
            .expect("schema");
        let engine = Arc::new(FakeEngine::default());
        let cache = Arc::new(Cache::new(64 * 1024 * 1024, engine.clone()));
        let inner = AsyncPgConnection::establish(&url()).await.expect("connect");
        let conn = AsyncSyncedConnection::with_cache(inner, cache.clone(), Route::Streamgres);
        let prefix = format!("{name}-{}-", std::process::id());
        diesel::delete(sg_configs::table.filter(sg_configs::key.like(format!("{prefix}%"))))
            .execute(&mut other)
            .await
            .expect("clean");
        World {
            conn,
            cache,
            engine,
            other,
            prefix,
        }
    }

    /// A key of this test's.
    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }

    /// Sets a key on the plain connection, as another service would.
    async fn put(&mut self, name: &str, value: &str) {
        let key = self.key(name);
        diesel::insert_into(sg_configs::table)
            .values((sg_configs::key.eq(&key), sg_configs::value.eq(value)))
            .on_conflict(sg_configs::key)
            .do_update()
            .set(sg_configs::value.eq(value))
            .execute(&mut self.other)
            .await
            .expect("put");
    }

    /// A key's value through the wrapper.
    async fn read(&mut self, name: &str) -> Option<String> {
        let key = self.key(name);
        sg_configs::table
            .filter(sg_configs::key.eq(key))
            .select(sg_configs::value)
            .first(&mut self.conn)
            .await
            .optional()
            .expect("read")
    }

    /// A key's value through the wrapper, flagged with `route`.
    async fn read_via(&mut self, name: &str, route: Route) -> Option<String> {
        let key = self.key(name);
        sg_configs::table
            .filter(sg_configs::key.eq(key))
            .select(sg_configs::value)
            .via(route)
            .first(&mut self.conn)
            .await
            .optional()
            .expect("read via")
    }

    /// The cache key of a read of `name`. It binds built-in types only,
    /// so no type lookup is needed.
    fn key_of(&mut self, name: &str) -> QueryKey {
        let key = self.key(name);
        let query = sg_configs::table
            .filter(sg_configs::key.eq(key))
            .select(sg_configs::value)
            .limit(1);
        /// A lookup that is never asked.
        struct NoLookup;
        impl diesel::pg::PgMetadataLookup for NoLookup {
            /// Never called.
            fn lookup_type(&mut self, _: &str, _: Option<&str>) -> diesel::pg::PgTypeMetadata {
                unreachable!()
            }
        }
        let mut lookup = NoLookup;
        QueryKey::of(&query, &mut lookup).expect("key")
    }

    /// Deletes this test's keys.
    async fn clean(&mut self) {
        let _ = diesel::delete(
            sg_configs::table.filter(sg_configs::key.like(format!("{}%", self.prefix))),
        )
        .execute(&mut self.other)
        .await;
    }

    /// The cache's hits so far.
    fn hits(&self) -> u64 {
        stat(&self.cache.stats.hits)
    }
}

/// A read the engine accepted is subscribed before it is read, and a repeat
/// is served from the cache.
///
/// Without a touch the cache keeps the old row.
#[tokio::test]
async fn a_repeated_read_is_served_from_the_cache() {
    let mut w = World::new("async-hit").await;
    w.put("mode", "a").await;
    assert_eq!(w.read("mode").await.as_deref(), Some("a"));
    assert_eq!(
        w.engine.subscribed.lock().unwrap().len(),
        1,
        "subscribed before the read"
    );
    assert_eq!(w.cache.len(), 1);

    let hits = w.hits();
    assert_eq!(w.read("mode").await.as_deref(), Some("a"));
    assert_eq!(w.hits(), hits + 1, "served from the cache");

    w.put("mode", "b").await;
    assert_eq!(w.read("mode").await.as_deref(), Some("a"));
    w.clean().await;
}

/// The connection says where each read was served from: Postgres for the
/// accepted miss, the cache for the repeat, Postgres again under the flag.
#[tokio::test]
async fn the_connection_says_where_each_read_was_served_from() {
    let mut w = World::new("async-served").await;
    assert_eq!(w.conn.last_served(), None, "nothing read yet");
    w.put("k", "1").await;

    assert_eq!(w.read("k").await.as_deref(), Some("1"));
    assert_eq!(
        w.conn.last_served(),
        Some(Served::Postgres),
        "the miss the engine accepted"
    );
    assert_eq!(w.read("k").await.as_deref(), Some("1"));
    assert_eq!(w.conn.last_served(), Some(Served::Cache));
    assert_eq!(w.read_via("k", Route::Postgres).await.as_deref(), Some("1"));
    assert_eq!(
        w.conn.last_served(),
        Some(Served::Postgres),
        "the flag sent it live"
    );
    w.clean().await;
}

/// A touch makes the next read refill from Postgres, subscribing again.
#[tokio::test]
async fn a_touch_makes_the_next_read_refill() {
    let mut w = World::new("async-touch").await;
    w.put("mode", "a").await;
    assert_eq!(w.read("mode").await.as_deref(), Some("a"));
    w.put("mode", "b").await;
    let key = w.key_of("mode");
    w.cache.touched(&key);
    assert_eq!(
        w.read("mode").await.as_deref(),
        Some("b"),
        "refilled from Postgres"
    );
    assert_eq!(w.cache.len(), 1);
    assert_eq!(
        w.engine.subscribed.lock().unwrap().len(),
        2,
        "the refill subscribed again, the fake forgets"
    );
    w.clean().await;
}

/// The query's flag and a route scope each send a read to Postgres without
/// disturbing the entry; the scope ends with its closure.
#[tokio::test]
async fn the_query_flag_and_the_scope_route_a_read() {
    let mut w = World::new("async-route").await;
    w.put("mode", "a").await;
    assert_eq!(w.read("mode").await.as_deref(), Some("a"));
    w.put("mode", "b").await;
    assert_eq!(
        w.read_via("mode", Route::Postgres).await.as_deref(),
        Some("b"),
        "the flag bypassed the cache"
    );
    assert_eq!(
        w.read("mode").await.as_deref(),
        Some("a"),
        "the entry is untouched"
    );

    let key = w.key("mode");
    let scoped = w
        .conn
        .with_route(Route::Postgres, |conn| {
            async move {
                sg_configs::table
                    .filter(sg_configs::key.eq(key))
                    .select(sg_configs::value)
                    .first::<String>(conn)
                    .await
                    .optional()
                    .unwrap()
            }
            .boxed()
        })
        .await;
    assert_eq!(scoped.as_deref(), Some("b"));
    assert_eq!(
        w.read("mode").await.as_deref(),
        Some("a"),
        "the scope ended"
    );
    w.clean().await;
}

use futures_util::FutureExt;

/// Inside a transaction a read goes to Postgres and sees the connection's
/// own write, without subscribing; outside it the cache holds the old row
/// until a touch.
#[tokio::test]
async fn inside_a_transaction_reads_see_the_connections_own_writes() {
    let mut w = World::new("async-txn").await;
    w.conn.set_read_your_writes(Duration::ZERO);
    w.put("count", "1").await;
    assert_eq!(w.read("count").await.as_deref(), Some("1"));
    let count_key = w.key("count");

    let seen: String = w
        .conn
        .transaction::<_, diesel::result::Error, _>(|conn| {
            let count_key = count_key.clone();
            async move {
                diesel::update(sg_configs::table.filter(sg_configs::key.eq(&count_key)))
                    .set(sg_configs::value.eq("2"))
                    .execute(conn)
                    .await?;
                sg_configs::table
                    .filter(sg_configs::key.eq(&count_key))
                    .select(sg_configs::value)
                    .first(conn)
                    .await
            }
            .scope_boxed()
        })
        .await
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
    assert_eq!(
        w.read("count").await.as_deref(),
        Some("1"),
        "outside it the cache holds the old row until a touch"
    );
    w.clean().await;
}

/// After its own write the connection reads Postgres for the read-your-
/// writes window, and the cache again once it closes.
#[tokio::test]
async fn after_a_write_the_connection_reads_postgres_for_a_window() {
    let mut w = World::new("async-ryw").await;
    w.conn.set_read_your_writes(Duration::from_millis(150));
    w.put("mode", "a").await;
    assert_eq!(w.read("mode").await.as_deref(), Some("a"));
    let key = w.key("mode");
    diesel::update(sg_configs::table.filter(sg_configs::key.eq(&key)))
        .set(sg_configs::value.eq("b"))
        .execute(&mut w.conn)
        .await
        .unwrap();
    assert_eq!(
        w.read("mode").await.as_deref(),
        Some("b"),
        "its own write is seen at once"
    );
    assert_eq!(stat(&w.cache.stats.after_write), 1);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        w.read("mode").await.as_deref(),
        Some("a"),
        "the window closed; the entry was not refreshed"
    );
    w.clean().await;
}

/// A query the engine refuses is read live and not kept.
#[tokio::test]
async fn a_query_the_engine_refuses_is_read_live() {
    let mut w = World::new("async-refused").await;
    let count: i64 = diesel::sql_query("SELECT count(*) AS n FROM pg_class")
        .get_result::<Count>(&mut w.conn)
        .await
        .map(|c| c.n)
        .unwrap();
    assert!(count > 0);
    assert_eq!(
        w.cache.len(),
        0,
        "not a config read: the fake refused it, the read was live"
    );
    assert_eq!(stat(&w.cache.stats.refused), 1);
    w.clean().await;
}

/// A count, read by name.
#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

/// A read binding an array of an enum is live once, keyed and subscribed
/// the second time, cached the third.
///
/// The array's element OID is unknown: the query cannot be keyed, so it
/// goes live while the type is resolved.
///
/// The OIDs live in the cache: a sibling connection knows them at once.
#[tokio::test]
async fn an_enum_array_bind_is_live_once_then_cached() {
    let mut w = World::new("async-enum-array").await;
    let key = w.key("m");
    diesel::sql_query(
        "INSERT INTO sg_moods (key, mood) VALUES ($1, 'happy') ON CONFLICT (key) DO NOTHING",
    )
    .bind::<diesel::sql_types::Text, _>(&key)
    .execute(&mut w.other)
    .await
    .unwrap();
    /// The mood of `key`, if it is one of the two.
    async fn read(conn: &mut AsyncSyncedConnection, key: String) -> Option<Mood> {
        sg_moods::table
            .filter(sg_moods::key.eq(key))
            .filter(sg_moods::mood.eq_any(vec![Mood::Happy, Mood::Sad]))
            .select(sg_moods::mood)
            .first::<Mood>(conn)
            .await
            .optional()
            .unwrap()
    }
    assert_eq!(read(&mut w.conn, key.clone()).await, Some(Mood::Happy));
    assert_eq!(
        w.engine.subscribed.lock().unwrap().len(),
        0,
        "not subscribed the first time"
    );
    assert_eq!(read(&mut w.conn, key.clone()).await, Some(Mood::Happy));
    assert_eq!(
        w.engine.subscribed.lock().unwrap().len(),
        1,
        "keyed and subscribed the second time"
    );
    assert_eq!(read(&mut w.conn, key.clone()).await, Some(Mood::Happy));
    assert_eq!(w.hits(), 1, "cached the third time");

    let inner = AsyncPgConnection::establish(&url()).await.expect("connect");
    let mut sibling =
        AsyncSyncedConnection::with_cache(inner, Arc::clone(&w.cache), Route::Streamgres);
    assert_eq!(read(&mut sibling, key.clone()).await, Some(Mood::Happy));
    assert_eq!(
        w.hits(),
        2,
        "the sibling hit the same entry without a live read"
    );
    let _ = diesel::sql_query("DELETE FROM sg_moods WHERE key = $1")
        .bind::<diesel::sql_types::Text, _>(&key)
        .execute(&mut w.other)
        .await;
    w.clean().await;
}

/// Two reads pipelined on one connection do not ask for its transaction
/// state.
///
/// Two futures from one connection, both pending at once: diesel-async
/// shares its transaction state with each, and asking for it while the
/// first is pending is a panic.
#[tokio::test]
async fn pipelined_reads_do_not_touch_the_transaction_state() {
    let mut w = World::new("async-pipeline").await;
    w.put("a", "1").await;
    w.put("b", "2").await;
    let query = |key: String| {
        sg_configs::table
            .filter(sg_configs::key.eq(key))
            .select(sg_configs::value)
            .limit(1)
    };
    let (key_a, key_b) = (w.key("a"), w.key("b"));
    let first = AsyncConnection::load(&mut w.conn, query(key_a));
    let second = AsyncConnection::load(&mut w.conn, query(key_b));
    let (first, second) = futures_util::join!(first, second);
    let first: Vec<String> = first
        .unwrap()
        .map(|row| {
            row.and_then(|row| {
                <String as FromSqlRow<diesel::sql_types::Text, diesel::pg::Pg>>::build_from_row(
                    &row,
                )
                .map_err(diesel::result::Error::DeserializationError)
            })
        })
        .try_collect()
        .await
        .unwrap();
    let second: Vec<String> = second
        .unwrap()
        .map(|row| {
            row.and_then(|row| {
                <String as FromSqlRow<diesel::sql_types::Text, diesel::pg::Pg>>::build_from_row(
                    &row,
                )
                .map_err(diesel::result::Error::DeserializationError)
            })
        })
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        (first, second),
        (vec!["1".to_string()], vec!["2".to_string()])
    );
    w.clean().await;
}

/// A route scope dropped before it completes still restores the outer
/// route.
#[tokio::test]
async fn a_route_scope_ends_when_its_future_is_dropped() {
    let mut w = World::new("async-scope-drop").await;
    w.put("mode", "a").await;
    assert_eq!(w.read("mode").await.as_deref(), Some("a"));
    w.put("mode", "b").await;
    let dropped = tokio::time::timeout(
        Duration::from_millis(10),
        w.conn
            .with_route(Route::Postgres, |_| std::future::pending::<()>().boxed()),
    )
    .await;
    assert!(dropped.is_err(), "the scope's future was dropped mid-way");
    assert_eq!(
        w.read("mode").await.as_deref(),
        Some("a"),
        "the scope was restored: the cache answers"
    );
    w.clean().await;
}

/// A read dropped after its subscribe may have landed ends the subscription
/// it made, and releases its reservation.
///
/// Exactly one poll, then the drop: the subscribe may already have landed
/// on its blocking thread by then, and the Postgres read has at best been
/// sent.
///
/// The drop ends the subscription at once (a no-op on the engine side if
/// the subscribe has not landed yet); the blocking thread's subscribe lands
/// after, and finds the read gone.
///
/// The guard's drop and the blocking thread's check can both end it; the
/// second is a no-op on the engine side, so either count is right.
#[tokio::test]
async fn a_read_dropped_after_subscribing_ends_its_subscription() {
    let mut w = World::new("async-drop-read").await;
    w.put("mode", "a").await;
    let query = sg_configs::table
        .filter(sg_configs::key.eq(w.key("mode")))
        .select(sg_configs::value)
        .limit(1);
    let mut read = Box::pin(AsyncConnection::load(&mut w.conn, query));
    assert!(
        futures_util::poll!(read.as_mut()).is_pending(),
        "a miss cannot complete in one poll"
    );
    drop(read);
    for _ in 0..100 {
        let subscribed = w.engine.subscribed.lock().unwrap().len();
        let unsubscribed = w.engine.unsubscribed.lock().unwrap().len();
        if subscribed == 1 && unsubscribed >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let subscribed = w.engine.subscribed.lock().unwrap().len();
    let unsubscribed = w.engine.unsubscribed.lock().unwrap().len();
    assert_eq!(subscribed, 1, "the subscribe landed on its blocking thread");
    assert!(
        (1..=2).contains(&unsubscribed),
        "the subscription the dropped read made was ended: {unsubscribed}"
    );
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(w.cache.reservations(), 0, "and its reservation released");
    assert_eq!(w.cache.len(), 0);
    w.clean().await;
}

/// A write on a sibling connection opens the read-your-writes window of the
/// cache they share.
#[tokio::test]
async fn a_write_on_a_sibling_connection_is_seen_through_the_shared_cache() {
    let mut w = World::new("async-ryw-pool").await;
    w.conn.set_read_your_writes(Duration::from_secs(5));
    w.put("mode", "a").await;
    assert_eq!(w.read("mode").await.as_deref(), Some("a"));
    let inner = AsyncPgConnection::establish(&url()).await.expect("connect");
    let mut sibling =
        AsyncSyncedConnection::with_cache(inner, Arc::clone(&w.cache), Route::Streamgres);
    let key = w.key("mode");
    diesel::update(sg_configs::table.filter(sg_configs::key.eq(&key)))
        .set(sg_configs::value.eq("b"))
        .execute(&mut sibling)
        .await
        .unwrap();
    assert_eq!(
        w.read("mode").await.as_deref(),
        Some("b"),
        "the other connection reads Postgres for the window"
    );
    assert_eq!(stat(&w.cache.stats.after_write), 1);
    w.clean().await;
}

use diesel::deserialize::FromSqlRow;
use futures_util::{StreamExt, TryStreamExt};
