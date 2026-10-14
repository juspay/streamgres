//! Every lookup, through both wrappers, against a running engine: the
//! invariants the wrapper promises, checked on the shapes the desk
//! actually reads.
//!
//! Needs `STREAMGRES_S2S_URL` and `DATABASE_URL` (the database that
//! engine follows); skips itself otherwise. Reseeds the dataset, so it
//! must not share the database with a demo someone is watching.

use std::sync::Arc;
use std::time::{Duration, Instant};

use diesel::sql_query;
use diesel::{Connection, ExpressionMethods, PgConnection, QueryDsl};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl as _};
use serde_json::Value;
use streamgres_diesel::cache::Cache;
use streamgres_diesel::engine::EngineClient;
use streamgres_diesel::{AsyncSyncedConnection, Route, SyncedConnection};
use streamgres_example::queries::{Lookup, SyncConn};
use streamgres_example::seed;

struct Live {
    cache: Arc<Cache>,
    engine: EngineClient,
    /// The async wrapper, routed Streamgres.
    main: AsyncSyncedConnection,
    /// The blocking wrapper on the same cache.
    sync: SyncConn,
    /// No wrapper: what Postgres says, and where the changes come from.
    plain: AsyncPgConnection,
    database_url: String,
}

impl Live {
    async fn start() -> Option<Live> {
        let Ok(url) = std::env::var("STREAMGRES_S2S_URL") else {
            eprintln!("STREAMGRES_S2S_URL not set; skipping");
            return None;
        };
        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
        let mut seeder = PgConnection::establish(&database_url).expect("connect");
        seed::run(&mut seeder).expect("seed");
        let engine = EngineClient::connect(url);
        let cache = Arc::new(Cache::new(64 * 1024 * 1024, Arc::new(engine.clone())));
        engine.attach(&cache);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !engine.is_connected() {
            assert!(Instant::now() < deadline, "the engine did not connect");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let main = AsyncSyncedConnection::with_cache(
            AsyncPgConnection::establish(&database_url)
                .await
                .expect("connect"),
            Arc::clone(&cache),
            Route::Streamgres,
        );
        let sync = SyncedConnection::with_cache(
            PgConnection::establish(&database_url).expect("connect"),
            Arc::clone(&cache),
            Route::Streamgres,
        );
        let plain = AsyncPgConnection::establish(&database_url)
            .await
            .expect("connect");
        // The engine learns the tables from the seed's DDL; give the
        // trigger a moment on a fresh database.
        tokio::time::sleep(Duration::from_millis(300)).await;
        Some(Live {
            cache,
            engine,
            main,
            sync,
            plain,
            database_url,
        })
    }

    fn stat(&self, counter: &std::sync::atomic::AtomicU64) -> u64 {
        std::sync::atomic::AtomicU64::load(counter, std::sync::atomic::Ordering::Relaxed)
    }

    /// What Postgres says, through the blocking wrapper routed Postgres.
    fn truth(&mut self, lookup: &Lookup) -> Value {
        self.sync
            .with_route(Route::Postgres, |conn| lookup.run_sync(conn).unwrap())
    }

    async fn change(&mut self, sql: &str) {
        sql_query(sql)
            .execute(&mut self.plain)
            .await
            .expect("change");
    }

    /// Reads through the async wrapper until the result differs from
    /// `before` or the wait runs out.
    async fn until_changed(
        &mut self,
        lookup: &Lookup,
        before: &Value,
        wait: Duration,
    ) -> Option<(Value, Duration)> {
        let started = Instant::now();
        while started.elapsed() < wait {
            let now = lookup.run_async(&mut self.main).await.unwrap();
            if &now != before {
                return Some((now, started.elapsed()));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        None
    }
}

/// A change that each lookup's result reflects.
fn change_for(lookup: &Lookup) -> &'static str {
    match lookup {
        Lookup::Ticket { .. } => {
            "UPDATE tickets SET priority = 'high', updated_at = now() WHERE id = 1"
        }
        Lookup::Queue { .. } => {
            "UPDATE tickets SET title = title || '!' WHERE id = (SELECT id FROM tickets WHERE org_id = 1 AND status IN ('open', 'pending') ORDER BY created_at DESC, id ASC OFFSET 10 LIMIT 1)"
        }
        Lookup::QueueCount { .. } => "UPDATE tickets SET status = 'open' WHERE id = 2",
        Lookup::Comments { .. } => {
            "INSERT INTO comments (id, ticket_id, body) VALUES (99001, 1, 'scenario') ON CONFLICT (id) DO UPDATE SET body = comments.body || '!'"
        }
        Lookup::Attachment { .. } => {
            "UPDATE attachments SET data = data || '\\x00'::bytea WHERE id = 1"
        }
        Lookup::Agents { .. } => {
            "UPDATE agents SET skills = array_append(skills, 'scenario') WHERE id = 1"
        }
        Lookup::Sla { .. } => "UPDATE tickets SET sla = sla + interval '1 minute' WHERE id = 3",
        Lookup::Search { .. } => "UPDATE tickets SET title = title || '?' WHERE id = 1",
        Lookup::Recent { .. } => "UPDATE tickets SET updated_at = now() WHERE id = 7",
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_lookup_is_served_correctly_through_both_wrappers() {
    let Some(mut live) = Live::start().await else {
        return;
    };
    live.main.set_read_your_writes(Duration::ZERO);
    live.sync.set_read_your_writes(Duration::ZERO);
    println!(
        "{:<12} {:<9} {:<7} {:<9} {:>8}  exercises",
        "lookup", "mode", "hit", "sync", "seen in"
    );
    for lookup in Lookup::demo() {
        let hits_before = live.stat(&live.cache.stats.hits);
        let materialized_before = live.engine.materialized();
        let truth = live.truth(&lookup);
        // A hit by the third read: the first subscribes and fills, or,
        // for a query with an enum bind on the async wrapper, resolves
        // the type's OID and goes live, so the second subscribes.
        let mut hit_on = None;
        for read in 1..=3 {
            let seen = lookup.run_async(&mut live.main).await.unwrap();
            assert_eq!(
                seen,
                truth,
                "{}: read {read} matches Postgres",
                lookup.name()
            );
            if live.stat(&live.cache.stats.hits) > hits_before {
                hit_on = Some(read);
                break;
            }
        }
        let hits_so_far = live.stat(&live.cache.stats.hits);
        let through_sync = lookup.run_sync(&mut live.sync).unwrap();
        assert_eq!(
            through_sync,
            truth,
            "{}: the blocking wrapper agrees",
            lookup.name()
        );
        let sync_hit = live.stat(&live.cache.stats.hits) > hits_so_far;
        let mode = if !lookup.is_cacheable() {
            assert!(
                hit_on.is_none(),
                "{}: a refused shape never hits",
                lookup.name()
            );
            "live"
        } else {
            assert!(
                hit_on.is_some(),
                "{}: no hit in three reads; refusals {:?}",
                lookup.name(),
                live.engine.refusals()
            );
            assert!(
                sync_hit,
                "{}: the blocking wrapper shares the entry",
                lookup.name()
            );
            if live.engine.materialized() > materialized_before {
                "rows"
            } else {
                "refill"
            }
        };
        if matches!(lookup, Lookup::Sla { .. }) {
            assert_eq!(
                mode, "refill",
                "an interval column cannot be shipped as rows"
            );
        }

        // A change from elsewhere: the cache must reflect it, and the
        // result must again be what Postgres says.
        live.change(change_for(&lookup)).await;
        let seen = if lookup.is_cacheable() {
            let (after, seen_in) = live
                .until_changed(&lookup, &truth, Duration::from_secs(5))
                .await
                .unwrap_or_else(|| panic!("{}: the change was not seen", lookup.name()));
            assert_eq!(
                after,
                live.truth(&lookup),
                "{}: after the change, the cache matches Postgres",
                lookup.name()
            );
            seen_in
        } else {
            let after = lookup.run_async(&mut live.main).await.unwrap();
            assert_eq!(
                after,
                live.truth(&lookup),
                "{}: live, so the change is seen at once",
                lookup.name()
            );
            Duration::ZERO
        };
        println!(
            "{:<12} {:<9} {:<7} {:<9} {:>6}ms  {}{}",
            lookup.name(),
            mode,
            hit_on
                .map(|read| format!("read {read}"))
                .unwrap_or_else(|| "-".to_string()),
            if sync_hit { "shared" } else { "-" },
            seen.as_millis(),
            lookup.exercises(),
            if mode == "refill" {
                format!(
                    " [{}]",
                    live.engine.fallbacks().last().cloned().unwrap_or_default()
                )
            } else {
                String::new()
            }
        );
    }
    assert!(
        live.engine
            .refusals()
            .iter()
            .any(|reason| reason.contains("LIKE") || reason.contains("ILIKE")),
        "{:?}",
        live.engine.refusals()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_on_one_connection_is_seen_by_the_pools_other_connections() {
    let Some(mut live) = Live::start().await else {
        return;
    };
    live.main.set_read_your_writes(Duration::from_secs(5));
    live.sync.set_read_your_writes(Duration::from_secs(5));
    let lookup = Lookup::Ticket { id: 20 };
    let before = lookup.run_async(&mut live.main).await.unwrap();
    let _ = lookup.run_async(&mut live.main).await.unwrap();
    // The write goes through the async wrapper; the read right after,
    // through the blocking wrapper on the same cache, must see it
    // without waiting for the engine.
    diesel::update(
        streamgres_example::schema::tickets::table
            .filter(streamgres_example::schema::tickets::id.eq(20)),
    )
    .set(streamgres_example::schema::tickets::title.eq("read your writes"))
    .execute(&mut live.main)
    .await
    .unwrap();
    let after = lookup.run_sync(&mut live.sync).unwrap();
    assert_ne!(after, before);
    assert_eq!(after[0]["title"].as_str(), Some("read your writes"));
    assert!(live.stat(&live.cache.stats.after_write) >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_dropped_mid_flight_leaves_no_subscription_behind() {
    let Some(mut live) = Live::start().await else {
        return;
    };
    let lookup = Lookup::Queue {
        org: 2,
        statuses: vec![streamgres_example::models::TicketStatus::Closed],
        page: 0,
    };
    let subscribed = live.engine.subscribed();
    let mut read = Box::pin(lookup.run_async(&mut live.main));
    assert!(futures_util::poll!(read.as_mut()).is_pending());
    drop(read);
    let deadline = Instant::now() + Duration::from_secs(5);
    while live.engine.subscribed() != subscribed && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        live.engine.subscribed(),
        subscribed,
        "the subscription the dropped read made is gone"
    );
    assert!(live.cache.is_empty());
    // And the same read completed afterwards is cached as usual (the
    // first completed read may only resolve the enum's OID).
    for _ in 0..3 {
        let _ = lookup.run_async(&mut live.main).await.unwrap();
    }
    assert_eq!(
        live.engine.subscribed(),
        subscribed + 1,
        "refusals {:?}",
        live.engine.refusals()
    );
    let _ = &live.database_url;
}
