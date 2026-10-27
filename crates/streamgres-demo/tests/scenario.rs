//! Every lookup, through both wrappers, against a running engine: the
//! invariants the wrapper promises, checked on the shapes the desk
//! actually reads.
//!
//! Needs `STREAMGRES_S2S_URL` and `DATABASE_URL` (the database that
//! engine follows); skips itself otherwise. The tests that restart the
//! engine also need `STREAMGRES_RESTART_CMD`, a shell command that
//! restarts it and returns once it serves again, and skip without it.
//! Every test reseeds the dataset, so run them against a database
//! nobody is watching, one at a time (`--test-threads=1`).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use diesel::sql_query;
use diesel::{Connection, ExpressionMethods, PgConnection, QueryDsl, SelectableHelper};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl as _};
use diesel_streamgres::cache::{Cache, Limits};
use diesel_streamgres::embedded;
use diesel_streamgres::engine::{Config as EngineConfig, EngineClient};
use diesel_streamgres::{AsyncSyncedConnection, Route, RouteDsl, Served, SyncedConnection};
use serde_json::Value;
use streamgres_demo::migrate::Step;
use streamgres_demo::models::{Ticket, TicketKind};
use streamgres_demo::pools::Stack;
use streamgres_demo::queries::{Expected, Lookup, SyncConn};
use streamgres_demo::schema::tickets;
use streamgres_demo::{seed, verify, writer};

/// The demo against a live Postgres and engine: the wrappers on one
/// cache, and a plain connection for the truth and the changes.
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
    /// Reseeds, connects, and gives the engine a moment to hear of the
    /// seed's DDL on a fresh database. `None` without an engine.
    async fn start() -> Option<Live> {
        Self::start_with(Limits {
            refused_for: None,
            ..Limits::default()
        })
        .await
    }

    /// As [`Live::start`], with the cache's limits chosen. No refusal
    /// memory, so a refusal under test is re-asked rather than remembered.
    async fn start_with(limits: Limits) -> Option<Live> {
        let engine = engine_client()?;
        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
        let mut seeder = PgConnection::establish(&database_url).expect("connect");
        Step::Drop.run(&mut seeder).expect("undo the migrations");
        seed::run(&mut seeder).expect("seed");
        let cache = Arc::new(Cache::with_limits(limits, Arc::new(engine.clone())));
        engine.attach(&cache);
        if !connected(&engine, Duration::from_secs(30)).await {
            assert!(
                restart(&engine).await,
                "the engine did not connect (a migration left behind may have stopped it; set STREAMGRES_RESTART_CMD)"
            );
        }
        let mut main = AsyncSyncedConnection::with_cache(
            AsyncPgConnection::establish(&database_url)
                .await
                .expect("connect"),
            Arc::clone(&cache),
            Route::Streamgres,
        );
        // An async connection's first read goes to Postgres beside reading
        // what it reads as; the tests count from its second.
        let one: i32 = diesel::select(diesel::dsl::sql::<diesel::sql_types::Integer>("1"))
            .get_result(&mut main)
            .await
            .expect("a first read");
        assert_eq!(one, 1);
        let sync = SyncedConnection::with_cache(
            PgConnection::establish(&database_url).expect("connect"),
            Arc::clone(&cache),
            Route::Streamgres,
        );
        let plain = AsyncPgConnection::establish(&database_url)
            .await
            .expect("connect");
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

    /// One of the cache's counters now.
    fn stat(&self, counter: &AtomicU64) -> u64 {
        AtomicU64::load(counter, Ordering::Relaxed)
    }

    /// What Postgres says, through the blocking wrapper routed Postgres.
    fn truth(&mut self, lookup: &Lookup) -> Value {
        self.sync
            .with_route(Route::Postgres, |conn| lookup.run_sync(conn).unwrap())
    }

    /// Runs `sql` on the plain connection, as another service would.
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

    /// Runs `STREAMGRES_RESTART_CMD` and waits for the client to be
    /// back on the engine. `false` when the variable is not set.
    async fn restart_engine(&self) -> bool {
        restart(&self.engine).await
    }
}

/// Whether the client is on the engine within `wait`.
async fn connected(engine: &EngineClient, wait: Duration) -> bool {
    let deadline = Instant::now() + wait;
    while !engine.is_connected() {
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    true
}

/// Runs `STREAMGRES_RESTART_CMD` and waits for the client to be back on
/// the engine. `false` when the variable is not set.
async fn restart(engine: &EngineClient) -> bool {
    let Ok(restart) = std::env::var("STREAMGRES_RESTART_CMD") else {
        eprintln!("STREAMGRES_RESTART_CMD not set; skipping");
        return false;
    };
    let status = tokio::process::Command::new("sh")
        .arg("-c")
        .arg(&restart)
        .status()
        .await
        .expect("restart command");
    assert!(status.success(), "the restart command failed");
    assert!(
        connected(engine, Duration::from_secs(15)).await,
        "the engine did not come back"
    );
    true
}

/// A change that each lookup's result reflects, or, for a static
/// lookup, a change nearby that must leave it as it is.
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
        Lookup::OrgTicket { .. } => "UPDATE tickets SET title = title || '!' WHERE id = 5",
        Lookup::Unassigned { .. } => "UPDATE tickets SET agent_id = NULL WHERE id = 1",
        Lookup::Escalated { .. } => "UPDATE tickets SET priority = 'high' WHERE id = 2",
        Lookup::OpenExcept { .. } => "UPDATE tickets SET status = 'closed' WHERE id = 4",
        Lookup::CreatedBetween { .. } => {
            "UPDATE tickets SET created_at = '2026-10-01 11:59:00+00' WHERE id = 6"
        }
        Lookup::Many { .. } => "UPDATE tickets SET title = title || '!' WHERE id = 50",
        Lookup::QueueEnd { .. } | Lookup::Nothing { .. } => {
            "UPDATE tickets SET title = title || '!' WHERE id = 8"
        }
        Lookup::AllTickets { .. } => "UPDATE tickets SET title = title || '!' WHERE id = 9",
        Lookup::CommentAuthors { .. } => "UPDATE comments SET agent_id = 3 WHERE id = 12",
        Lookup::Skilled { .. } => {
            "UPDATE agents SET skills = array_append(skills, 'billing') WHERE id = 2"
        }
        Lookup::Statuses { .. } => {
            "UPDATE tickets SET status = 'open' WHERE org_id = 1 AND status = 'closed'"
        }
        Lookup::Workload { .. } => "UPDATE tickets SET agent_id = 1 WHERE id = 10",
        Lookup::LastActivity { .. } => "UPDATE tickets SET updated_at = now() WHERE id = 11",
        Lookup::OfKind { .. } => "UPDATE tickets SET kind = 'bug' WHERE id = 2",
    }
}

/// Every demo lookup matches Postgres on the first read and after a
/// change, is served the way it expects (rows, refills or live), and
/// shares its entry between the two wrappers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_lookup_is_served_correctly_through_both_wrappers() {
    let Some(mut live) = Live::start().await else {
        return;
    };
    live.main.set_read_your_writes(Duration::ZERO);
    live.sync.set_read_your_writes(Duration::ZERO);
    println!(
        "{:<16} {:<8} {:<7} {:<7} {:>8}  exercises",
        "lookup", "mode", "hit", "sync", "seen in"
    );
    for lookup in Lookup::demo() {
        let name = lookup.name();
        let hits_before = live.stat(&live.cache.stats.hits);
        let refused_before = live.stat(&live.cache.stats.refused);
        let materialized_before = live.engine.materialized();
        let truth = live.truth(&lookup);
        let mut hit_on = None;
        for read in 1..=3 {
            let seen = lookup.run_async(&mut live.main).await.unwrap();
            assert_eq!(seen, truth, "{name}: read {read} matches Postgres");
            if live.stat(&live.cache.stats.hits) > hits_before {
                hit_on = Some(read);
                break;
            }
        }
        let hits_so_far = live.stat(&live.cache.stats.hits);
        let through_sync = lookup.run_sync(&mut live.sync).unwrap();
        assert_eq!(through_sync, truth, "{name}: the blocking wrapper agrees");
        let sync_hit = live.stat(&live.cache.stats.hits) > hits_so_far;
        let mode = match lookup.expected() {
            Expected::Live => {
                assert!(hit_on.is_none(), "{name}: a refused shape never hits");
                assert!(
                    live.stat(&live.cache.stats.refused) > refused_before,
                    "{name}: the engine refused it"
                );
                "live"
            }
            expected => {
                assert!(
                    hit_on.is_some(),
                    "{name}: no hit in three reads; refusals {:?}",
                    live.engine.refusals()
                );
                assert!(sync_hit, "{name}: the blocking wrapper shares the entry");
                let rows = live.engine.materialized() > materialized_before;
                assert_eq!(
                    rows,
                    expected == Expected::Rows,
                    "{name}: expected {expected:?}; fallbacks {:?}",
                    live.engine.fallbacks()
                );
                if rows { "rows" } else { "refill" }
            }
        };

        live.change(change_for(&lookup)).await;
        let seen = if lookup.is_static() {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let after = lookup.run_async(&mut live.main).await.unwrap();
            assert_eq!(after, truth, "{name}: a change nearby leaves it as it is");
            assert_eq!(after, live.truth(&lookup), "{name}: and Postgres agrees");
            Duration::ZERO
        } else if lookup.is_cacheable() {
            let (after, seen_in) = live
                .until_changed(&lookup, &truth, Duration::from_secs(5))
                .await
                .unwrap_or_else(|| panic!("{name}: the change was not seen"));
            assert_eq!(
                after,
                live.truth(&lookup),
                "{name}: after the change, the cache matches Postgres"
            );
            seen_in
        } else {
            let after = lookup.run_async(&mut live.main).await.unwrap();
            assert_eq!(
                after,
                live.truth(&lookup),
                "{name}: live, so the change is seen at once"
            );
            Duration::ZERO
        };
        println!(
            "{:<16} {:<8} {:<7} {:<7} {:>6}ms  {}{}",
            name,
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
}

/// A write through one wrapper is seen by the next read on another of
/// the same cache, inside the read-your-writes window.
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
    diesel::update(tickets::table.filter(tickets::id.eq(20)))
        .set(tickets::title.eq("read your writes"))
        .execute(&mut live.main)
        .await
        .unwrap();
    let after = lookup.run_sync(&mut live.sync).unwrap();
    assert_ne!(after, before);
    assert_eq!(after[0]["title"].as_str(), Some("read your writes"));
    assert!(live.stat(&live.cache.stats.after_write) >= 1);
}

/// A flag on the query beats the connection's route both ways, a scope
/// routes everything inside it, and the read-your-writes window ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_query_flag_beats_the_route_and_the_write_window_ends() {
    let Some(mut live) = Live::start().await else {
        return;
    };
    live.main.set_read_your_writes(Duration::from_millis(300));
    let query = || {
        tickets::table
            .filter(tickets::id.eq(30))
            .select(Ticket::as_select())
    };
    let cached: Vec<Ticket> = query().load(&mut live.main).await.unwrap();
    let cached_again: Vec<Ticket> = query().load(&mut live.main).await.unwrap();
    assert_eq!(cached, cached_again);
    assert_eq!(live.main.last_served(), Some(Served::Cache));
    let hits = live.stat(&live.cache.stats.hits);

    let flagged: Vec<Ticket> = query().via_postgres().load(&mut live.main).await.unwrap();
    assert_eq!(flagged, cached);
    assert_eq!(live.main.last_served(), Some(Served::Postgres));
    assert_eq!(
        live.stat(&live.cache.stats.hits),
        hits,
        "the flag skipped the cache"
    );

    live.main.set_route(Route::Postgres);
    let _: Vec<Ticket> = query().load(&mut live.main).await.unwrap();
    assert_eq!(live.main.last_served(), Some(Served::Postgres));
    let _: Vec<Ticket> = query().via_streamgres().load(&mut live.main).await.unwrap();
    assert_eq!(
        live.main.last_served(),
        Some(Served::Cache),
        "the flag reached the cache from a Postgres-routed connection"
    );
    live.main.set_route(Route::Streamgres);

    let scoped = live
        .main
        .with_route(Route::Postgres, |conn| {
            Box::pin(async move {
                let rows: Vec<Ticket> = query().load(conn).await.unwrap();
                (rows, conn.last_served())
            })
        })
        .await;
    assert_eq!(
        scoped.1,
        Some(Served::Postgres),
        "the scope routed the read"
    );
    assert_eq!(
        live.main.route(),
        Route::Streamgres,
        "and the route came back"
    );

    diesel::update(tickets::table.filter(tickets::id.eq(30)))
        .set(tickets::title.eq("inside the window"))
        .execute(&mut live.main)
        .await
        .unwrap();
    let after_write = live.stat(&live.cache.stats.after_write);
    let inside: Vec<Ticket> = query().load(&mut live.main).await.unwrap();
    assert_eq!(inside[0].title, "inside the window");
    assert_eq!(live.main.last_served(), Some(Served::Postgres));
    assert_eq!(live.stat(&live.cache.stats.after_write), after_write + 1);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let outside: Vec<Ticket> = query().load(&mut live.main).await.unwrap();
    assert_eq!(
        outside[0].title, "inside the window",
        "the engine carried the write"
    );
    assert_eq!(
        live.main.last_served(),
        Some(Served::Cache),
        "the window ended, so the read is a hit again"
    );
}

/// A result over the per-entry cap is served and correct, but not kept:
/// no hit, no subscription left behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_oversized_result_is_served_but_not_kept() {
    let Some(mut live) = Live::start_with(Limits {
        max_entry_bytes: 4096,
        refused_for: None,
        ..Limits::default()
    })
    .await
    else {
        return;
    };
    let lookup = Lookup::AllTickets { org: 1 };
    let truth = live.truth(&lookup);
    let subscribed = live.engine.subscribed();
    for read in 1..=3 {
        let seen = lookup.run_async(&mut live.main).await.unwrap();
        assert_eq!(seen, truth, "read {read}");
        assert_ne!(
            live.main.last_served(),
            Some(Served::Cache),
            "read {read} is not a hit"
        );
    }
    assert!(live.stat(&live.cache.stats.oversized) >= 1);
    assert_eq!(live.stat(&live.cache.stats.hits), 0);
    let deadline = Instant::now() + Duration::from_secs(5);
    while live.engine.subscribed() != subscribed && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        live.engine.subscribed(),
        subscribed,
        "the subscription ended"
    );
    let small = Lookup::Ticket { id: 1 };
    let _ = small.run_async(&mut live.main).await.unwrap();
    let _ = small.run_async(&mut live.main).await.unwrap();
    assert_eq!(
        live.main.last_served(),
        Some(Served::Cache),
        "a small result is kept"
    );
}

/// A read dropped before it completed leaves no subscription behind,
/// and the same read completed afterwards is cached as usual.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_dropped_mid_flight_leaves_no_subscription_behind() {
    let Some(mut live) = Live::start().await else {
        return;
    };
    let lookup = Lookup::Queue {
        org: 2,
        statuses: vec![streamgres_demo::models::TicketStatus::Closed],
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
    for _ in 0..3 {
        let _ = lookup.run_async(&mut live.main).await.unwrap();
    }
    assert_eq!(
        live.engine.subscribed(),
        subscribed + 1,
        "refusals {:?}",
        live.engine.refusals()
    );
}

/// A client of the test's engine: the one `STREAMGRES_S2S_URL` names,
/// or, with `STREAMGRES_ENGINE=embedded`, one this test process runs,
/// configured from the environment, one for every test. `None`, and the
/// test skipped, when neither is named.
fn engine_client() -> Option<EngineClient> {
    if std::env::var("STREAMGRES_ENGINE").as_deref() == Ok("embedded") {
        static ENGINE: std::sync::OnceLock<embedded::Engine> = std::sync::OnceLock::new();
        let engine = ENGINE
            .get_or_init(|| {
                embedded::Engine::start(
                    embedded::Config::from_env().expect("an embedded engine's configuration"),
                )
            })
            .clone();
        return Some(EngineClient::embedded(engine, EngineConfig::from_env()));
    }
    let Ok(url) = std::env::var("STREAMGRES_S2S_URL") else {
        eprintln!("neither STREAMGRES_S2S_URL nor STREAMGRES_ENGINE=embedded set; skipping");
        return None;
    };
    Some(EngineClient::connect(url))
}

/// The process-wide stack the pool-based tests share: the wrapper's
/// settings install once per process.
async fn stack() -> Option<Arc<Stack>> {
    static STACK: tokio::sync::OnceCell<Arc<Stack>> = tokio::sync::OnceCell::const_new();
    let (Some(engine), Ok(database_url)) = (engine_client(), std::env::var("DATABASE_URL")) else {
        return None;
    };
    let stack = STACK
        .get_or_init(|| async {
            let stack = Stack::with_engine(&database_url, engine).await.expect("stack");
            assert!(stack.wait_for_engine(Duration::from_secs(30)).await);
            Arc::new(stack)
        })
        .await;
    Some(Arc::clone(stack))
}

/// A plain pool for the writer, as the binary gives it.
async fn plain_pool(database_url: &str) -> Pool<AsyncPgConnection> {
    Pool::builder()
        .max_size(4)
        .build(AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            database_url,
        ))
        .await
        .expect("plain pool")
}

/// The lookup's answer through a client that subscribes afresh, on its
/// own cache: what the engine says now, free of this process's stores.
/// Tells an engine-side answer from a client-side one in a mismatch.
async fn fresh_answer(name: &str) -> Value {
    let lookup = Lookup::named(name).expect("a demo lookup");
    let database_url = std::env::var("DATABASE_URL").unwrap();
    let engine = engine_client().expect("the engine the test started with");
    let cache = Arc::new(Cache::new(16 * 1024 * 1024, Arc::new(engine.clone())));
    engine.attach(&cache);
    assert!(connected(&engine, Duration::from_secs(10)).await);
    let mut conn = AsyncSyncedConnection::with_cache(
        AsyncPgConnection::establish(&database_url).await.unwrap(),
        cache,
        Route::Streamgres,
    );
    let mut last = Value::Null;
    for _ in 0..3 {
        last = lookup.run_async(&mut conn).await.unwrap();
        if conn.last_served() == Some(Served::Cache) {
            break;
        }
    }
    last
}

/// Pairs that never agreed, told apart by where the wrong answer lives:
/// in this process's cache (a fresh subscription agrees with Postgres)
/// or in the engine (a fresh subscription disagrees the same way).
#[derive(Debug, Default)]
struct Disagreements {
    client: u64,
    engine: u64,
}

impl Disagreements {
    /// Fails on either kind, named, so the failure says whose answer
    /// was wrong.
    fn assert_none(&self) {
        assert_eq!(self.client, 0, "pairs the cache got wrong: {self:?}");
        assert_eq!(self.engine, 0, "pairs the engine got wrong: {self:?}");
    }
}

/// Runs `readers` tasks for `duration`, each comparing every lookup
/// across the routes; the pairs that never agreed.
async fn read_under_load(stack: &Arc<Stack>, readers: usize, duration: Duration) -> Disagreements {
    let mut tasks = Vec::new();
    for _ in 0..readers {
        let stack = Arc::clone(stack);
        tasks.push(tokio::spawn(async move {
            let lookups = Lookup::demo();
            let started = Instant::now();
            let mut found = Disagreements::default();
            while started.elapsed() < duration {
                let outcome = verify::once(&stack, &lookups, Duration::from_secs(3)).await;
                for mismatch in &outcome.mismatches {
                    let fresh = fresh_answer(&mismatch.lookup).await;
                    let where_ = if fresh == mismatch.postgres {
                        found.client += 1;
                        "CLIENT"
                    } else {
                        found.engine += 1;
                        "ENGINE"
                    };
                    eprintln!(
                        "MISMATCH ({where_}) {}: cache {} | postgres {}",
                        mismatch.lookup, mismatch.cached, mismatch.postgres
                    );
                }
            }
            found
        }));
    }
    let mut found = Disagreements::default();
    for task in tasks {
        let part = task.await.expect("reader");
        found.client += part.client;
        found.engine += part.engine;
    }
    found
}

/// Sixteen readers comparing every lookup across the routes while the
/// writer runs its whole mix: no pair ever disagrees for long.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_readers_under_the_writer_never_see_a_wrong_row() {
    let Some(_seeded) = Live::start().await else {
        return;
    };
    let stack = stack().await.unwrap();
    let database_url = std::env::var("DATABASE_URL").unwrap();
    let counts = Arc::new(writer::Counts::default());
    let writer = writer::spawn(
        plain_pool(&database_url).await,
        Duration::from_millis(20),
        writer::OPS.to_vec(),
        Arc::clone(&counts),
    );
    let found = read_under_load(&stack, 16, Duration::from_secs(8)).await;
    writer.abort();
    println!("writer {}", counts.json());
    println!("verify {}", stack.verify.json());
    found.assert_none();
    assert!(counts.total() >= 100, "the writer ran: {}", counts.json());
    assert_eq!(counts.errors(), 0, "{}", counts.json());
    for op in writer::OPS {
        assert!(
            counts.of(op) > 0,
            "the mix reached `{op}`: {}",
            counts.json()
        );
    }
}

/// The engine restarted in the middle of the same load: the readers go
/// to Postgres while it is away, resubscribe when it is back, and
/// neither the cache nor the engine disagrees with Postgres for long.
/// This is the test that found the engine reading a timestamp range in
/// the server's own zone: after a restart, with no replay buffer to
/// hide it, every row of the range shape was missing until its next
/// write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_engine_restart_under_traffic_loses_no_correctness() {
    let Some(live) = Live::start().await else {
        return;
    };
    if std::env::var("STREAMGRES_RESTART_CMD").is_err() {
        eprintln!("STREAMGRES_RESTART_CMD not set; skipping");
        return;
    }
    let stack = stack().await.unwrap();
    let database_url = std::env::var("DATABASE_URL").unwrap();
    let counts = Arc::new(writer::Counts::default());
    let writer = writer::spawn(
        plain_pool(&database_url).await,
        Duration::from_millis(20),
        writer::OPS.to_vec(),
        Arc::clone(&counts),
    );
    let readers = {
        let stack = Arc::clone(&stack);
        tokio::spawn(async move { read_under_load(&stack, 8, Duration::from_secs(10)).await })
    };
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(live.restart_engine().await);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !stack.engine.is_connected() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(stack.engine.is_connected(), "the stack's client came back");
    let found = readers.await.expect("readers");
    writer.abort();
    println!("writer {}", counts.json());
    println!("verify {}", stack.verify.json());
    found.assert_none();
    assert!(stack.engine.subscribed() > 0, "subscriptions are back");
}

/// The three kinds of schema change: a widened table is absorbed and
/// the cache keeps serving; a new enum type is absorbed and learnt on
/// its first bind; a dropped column stops the engine, reads go to
/// Postgres meanwhile, and the restarted engine serves the schema as it
/// is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn migrations_are_absorbed_learnt_or_stop_the_engine() {
    let Some(mut live) = Live::start().await else {
        return;
    };
    if std::env::var("STREAMGRES_RESTART_CMD").is_err() {
        eprintln!("STREAMGRES_RESTART_CMD not set; skipping");
        return;
    }
    live.main.set_read_your_writes(Duration::ZERO);
    let mut migrator = PgConnection::establish(&live.database_url).unwrap();
    let ticket = Lookup::Ticket { id: 1 };
    let truth = live.truth(&ticket);
    assert_eq!(ticket.run_async(&mut live.main).await.unwrap(), truth);
    assert_eq!(ticket.run_async(&mut live.main).await.unwrap(), truth);
    assert_eq!(live.main.last_served(), Some(Served::Cache));

    Step::Widen.run(&mut migrator).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(live.engine.is_connected(), "a widened table is absorbed");
    assert_eq!(ticket.run_async(&mut live.main).await.unwrap(), truth);
    assert_eq!(
        live.main.last_served(),
        Some(Served::Cache),
        "the entry survived"
    );
    live.change(change_for(&ticket)).await;
    let (after, _) = live
        .until_changed(&ticket, &truth, Duration::from_secs(5))
        .await
        .expect("the change after the migration was seen");
    assert_eq!(after, live.truth(&ticket));

    Step::NewType.run(&mut migrator).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(live.engine.is_connected(), "a new type is absorbed");
    let of_kind = Lookup::OfKind {
        org: 1,
        kind: TicketKind::Bug,
    };
    let kinds = live.truth(&of_kind);
    assert!(kinds.as_array().is_some_and(|rows| !rows.is_empty()));
    for read in 1..=3 {
        assert_eq!(
            of_kind.run_async(&mut live.main).await.unwrap(),
            kinds,
            "read {read}"
        );
        assert_ne!(
            live.main.last_served(),
            Some(Served::Cache),
            "read {read}: refused"
        );
    }
    assert!(
        live.engine
            .refusals()
            .iter()
            .any(|reason| reason.contains("bind of type oid")),
        "{:?}",
        live.engine.refusals()
    );

    assert!(live.restart_engine().await);
    let materialized = live.engine.materialized();
    let mut hit = false;
    for _ in 1..=3 {
        assert_eq!(of_kind.run_async(&mut live.main).await.unwrap(), kinds);
        if live.main.last_served() == Some(Served::Cache) {
            hit = true;
            break;
        }
    }
    assert!(
        hit,
        "after the restart the new type's bind is served: {:?}",
        live.engine.refusals()
    );
    assert_eq!(
        live.engine.materialized(),
        materialized + 1,
        "in rows mode; fallbacks {:?}, refusals {:?}",
        live.engine.fallbacks(),
        live.engine.refusals()
    );

    Step::Drop.run(&mut migrator).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while live.engine.is_connected() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !live.engine.is_connected(),
        "a dropped column stops the engine"
    );
    let truth = live.truth(&ticket);
    assert_eq!(ticket.run_async(&mut live.main).await.unwrap(), truth);
    assert_eq!(
        live.main.last_served(),
        Some(Served::Postgres),
        "served live meanwhile"
    );

    assert!(live.restart_engine().await);
    for _ in 1..=3 {
        assert_eq!(ticket.run_async(&mut live.main).await.unwrap(), truth);
    }
    assert_eq!(live.main.last_served(), Some(Served::Cache), "cached again");
}

/// A column added while the engine is down. The wrapper learnt the
/// table's column types before the outage; when the engine is back and
/// serves a query that names the new column, those types lack it, so
/// they are looked up again and the query is served from engine rows.
#[tokio::test]
async fn a_column_added_while_the_engine_was_down_is_served_when_it_returns() {
    let Some(mut live) = Live::start().await else {
        return;
    };
    if std::env::var("STREAMGRES_RESTART_CMD").is_err() {
        eprintln!("STREAMGRES_RESTART_CMD not set; skipping");
        return;
    }
    live.main.set_read_your_writes(Duration::ZERO);
    let mut migrator = PgConnection::establish(&live.database_url).unwrap();
    let ticket = Lookup::Ticket { id: 1 };
    let truth = live.truth(&ticket);
    assert_eq!(ticket.run_async(&mut live.main).await.unwrap(), truth);
    assert_eq!(ticket.run_async(&mut live.main).await.unwrap(), truth);
    assert_eq!(live.main.last_served(), Some(Served::Cache));

    Step::Widen.run(&mut migrator).unwrap();
    Step::Drop.run(&mut migrator).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while live.engine.is_connected() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !live.engine.is_connected(),
        "the dropped column stopped the engine"
    );

    Step::NewType.run(&mut migrator).unwrap();
    assert_eq!(ticket.run_async(&mut live.main).await.unwrap(), truth);
    assert_eq!(
        live.main.last_served(),
        Some(Served::Postgres),
        "served live while the engine is down"
    );

    assert!(live.restart_engine().await);
    let of_kind = Lookup::OfKind {
        org: 1,
        kind: TicketKind::Bug,
    };
    let kinds = live.truth(&of_kind);
    assert!(kinds.as_array().is_some_and(|rows| !rows.is_empty()));
    let materialized = live.engine.materialized();
    let mut served = Vec::new();
    for _ in 1..=4 {
        assert_eq!(of_kind.run_async(&mut live.main).await.unwrap(), kinds);
        served.push(live.main.last_served());
    }
    assert_eq!(
        served,
        [
            Some(Served::Postgres),
            Some(Served::Postgres),
            Some(Served::Engine),
            Some(Served::Cache)
        ],
        "live while the new type's OID is resolved, live again while the table's types are fetched afresh, then engine rows, then the cache; refusals {:?}, fallbacks {:?}",
        live.engine.refusals(),
        live.engine.fallbacks()
    );
    assert_eq!(
        live.engine.materialized(),
        materialized + 1,
        "from engine rows, with the types looked up again"
    );
    for _ in 1..=3 {
        assert_eq!(ticket.run_async(&mut live.main).await.unwrap(), truth);
    }
    assert_eq!(
        live.main.last_served(),
        Some(Served::Cache),
        "the old entry is back too"
    );
}
