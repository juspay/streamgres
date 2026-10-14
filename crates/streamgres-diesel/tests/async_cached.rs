//! The async wrapper against a fake engine side: the same rules as the
//! blocking one, reached through diesel-async.

#![cfg(feature = "async")]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use scoped_futures::ScopedFutureExt;
use streamgres_diesel::cache::Cache;
use streamgres_diesel::key::QueryKey;
use streamgres_diesel::materialized::Snapshot;
use streamgres_diesel::subscriptions::{Outcome, Subscriptions, Verdict};
use streamgres_diesel::types::TableTypes;
use streamgres_diesel::{AsyncSyncedConnection, Route, RouteDsl};

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

fn url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres:///streamgres_diesel_test?host=/tmp".to_string())
}

#[derive(Default)]
struct FakeEngine {
    subscribed: Mutex<Vec<QueryKey>>,
    unsubscribed: Mutex<Vec<QueryKey>>,
}

impl Subscriptions for FakeEngine {
    fn subscribe(&self, key: &QueryKey) -> Outcome {
        if !key.sql.contains("sg_configs") {
            return Outcome::Verdict(Verdict::Refused("not a config read".into()));
        }
        self.subscribed.lock().unwrap().push(key.clone());
        Outcome::Verdict(Verdict::Accepted)
    }

    fn materialize(&self, _: &QueryKey, _: Snapshot, _: HashMap<String, Arc<TableTypes>>) -> Verdict {
        Verdict::Accepted
    }

    fn unsubscribe(&self, key: &QueryKey) {
        self.unsubscribed.lock().unwrap().push(key.clone());
    }
}

struct World {
    conn: AsyncSyncedConnection,
    cache: Arc<Cache>,
    engine: Arc<FakeEngine>,
    other: AsyncPgConnection,
    prefix: String,
}

impl World {
    async fn new(name: &str) -> World {
        let mut other = AsyncPgConnection::establish(&url()).await.expect("connect");
        other
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS sg_configs (
                    id serial PRIMARY KEY, key text NOT NULL UNIQUE, value text NOT NULL)",
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

    fn key(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }

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

    fn key_of(&mut self, name: &str) -> QueryKey {
        let key = self.key(name);
        let query = sg_configs::table
            .filter(sg_configs::key.eq(key))
            .select(sg_configs::value)
            .limit(1);
        // Built-in types only, so no lookup is needed.
        struct NoLookup;
        impl diesel::pg::PgMetadataLookup for NoLookup {
            fn lookup_type(&mut self, _: &str, _: Option<&str>) -> diesel::pg::PgTypeMetadata {
                unreachable!()
            }
        }
        let mut lookup = NoLookup;
        QueryKey::of(&query, &mut lookup).expect("key")
    }

    async fn clean(&mut self) {
        let _ = diesel::delete(sg_configs::table.filter(sg_configs::key.like(format!("{}%", self.prefix))))
            .execute(&mut self.other)
            .await;
    }

    fn hits(&self) -> u64 {
        stat(&self.cache.stats.hits)
    }
}

#[tokio::test]
async fn a_repeated_read_is_served_from_the_cache() {
    let mut w = World::new("async-hit").await;
    w.put("mode", "a").await;
    assert_eq!(w.read("mode").await.as_deref(), Some("a"));
    assert_eq!(w.engine.subscribed.lock().unwrap().len(), 1, "subscribed before the read");
    assert_eq!(w.cache.len(), 1);

    let hits = w.hits();
    assert_eq!(w.read("mode").await.as_deref(), Some("a"));
    assert_eq!(w.hits(), hits + 1, "served from the cache");

    // Without a touch the cache keeps the old row.
    w.put("mode", "b").await;
    assert_eq!(w.read("mode").await.as_deref(), Some("a"));
    w.clean().await;
}

#[tokio::test]
async fn a_touch_makes_the_next_read_refill() {
    let mut w = World::new("async-touch").await;
    w.put("mode", "a").await;
    assert_eq!(w.read("mode").await.as_deref(), Some("a"));
    w.put("mode", "b").await;
    let key = w.key_of("mode");
    w.cache.touched(&key);
    assert_eq!(w.read("mode").await.as_deref(), Some("b"), "refilled from Postgres");
    assert_eq!(w.cache.len(), 1);
    assert_eq!(w.engine.subscribed.lock().unwrap().len(), 2, "the refill subscribed again, the fake forgets");
    w.clean().await;
}

#[tokio::test]
async fn the_query_flag_and_the_scope_route_a_read() {
    let mut w = World::new("async-route").await;
    w.put("mode", "a").await;
    assert_eq!(w.read("mode").await.as_deref(), Some("a"));
    w.put("mode", "b").await;
    assert_eq!(w.read_via("mode", Route::Postgres).await.as_deref(), Some("b"), "the flag bypassed the cache");
    assert_eq!(w.read("mode").await.as_deref(), Some("a"), "the entry is untouched");

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
    assert_eq!(w.read("mode").await.as_deref(), Some("a"), "the scope ended");
    w.clean().await;
}

use futures_util::FutureExt;

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
    assert_eq!(seen, "2", "the read inside the transaction went to Postgres");
    assert_eq!(w.engine.subscribed.lock().unwrap().len(), 1, "the transactional read did not subscribe");
    assert_eq!(w.read("count").await.as_deref(), Some("1"), "outside it the cache holds the old row until a touch");
    w.clean().await;
}

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
    assert_eq!(w.read("mode").await.as_deref(), Some("b"), "its own write is seen at once");
    assert_eq!(stat(&w.cache.stats.after_write), 1);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(w.read("mode").await.as_deref(), Some("a"), "the window closed; the entry was not refreshed");
    w.clean().await;
}

#[tokio::test]
async fn a_query_the_engine_refuses_is_read_live() {
    let mut w = World::new("async-refused").await;
    let count: i64 = diesel::sql_query("SELECT count(*) AS n FROM pg_class")
        .get_result::<Count>(&mut w.conn)
        .await
        .map(|c| c.n)
        .unwrap();
    assert!(count > 0);
    assert_eq!(w.cache.len(), 0, "not a config read: the fake refused it, the read was live");
    assert_eq!(stat(&w.cache.stats.refused), 1);
    w.clean().await;
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}
