//! What a read reports ([`diesel_streamgres::metrics`]), the rollout share
//! ([`diesel_streamgres::rollout`]) and shadow reads
//! ([`diesel_streamgres::shadow`]), through a cache a fake engine side
//! keeps (it never hears of changes, so a cached answer can be made stale
//! at will).
//!
//! A test binary of its own: the metrics, the share and the sample rate are
//! the process's. The tests take turns.
//!
//! Needs a Postgres at `DATABASE_URL` (default: the local socket's
//! `diesel_streamgres_test`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel_streamgres::cache::Cache;
use diesel_streamgres::key::QueryKey;
use diesel_streamgres::metrics::{Metrics, set_metrics};
use diesel_streamgres::shadow::{Outcome, counts, set_shadow_sample};
use diesel_streamgres::subscriptions::{Outcome as Subscribed, Subscriptions, Verdict};
use diesel_streamgres::{Consistency, Route, RouteDsl, Served, SyncedConnection};

diesel::table! {
    sg_rollout (id) {
        id -> Int4,
        key -> Text,
        value -> Text,
    }
}

/// The tests take turns: they share the process's settings.
static TURN: Mutex<()> = Mutex::new(());

/// The test database's URL.
fn url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres:///diesel_streamgres_test?host=/tmp".to_string())
}

/// Accepts every read of `sg_rollout` as a refill and refuses the rest;
/// never reports a change. Says it has delivered every change up to
/// `position` (0: says nothing).
#[derive(Default)]
struct FakeEngine {
    position: AtomicU64,
}

impl Subscriptions for FakeEngine {
    fn subscribe(&self, key: &QueryKey) -> Subscribed {
        if key.sql.contains("sg_rollout") {
            Subscribed::Verdict(Verdict::Accepted { since: None })
        } else {
            Subscribed::Verdict(Verdict::Refused("not a rollout read".into()))
        }
    }

    fn materialize(
        &self,
        _: &QueryKey,
        _: diesel_streamgres::materialized::Snapshot,
        _: std::collections::HashMap<String, Arc<diesel_streamgres::types::TableTypes>>,
    ) -> Verdict {
        Verdict::Accepted { since: None }
    }

    fn unsubscribe(&self, _: &QueryKey) {}

    fn position(&self) -> Option<u64> {
        Some(self.position.load(Ordering::SeqCst)).filter(|&position| position != 0)
    }
}

/// Records where each read was served, and the shadow outcomes.
#[derive(Default)]
struct Recording {
    served: Mutex<Vec<Served>>,
    bypassed: AtomicU64,
    shadow: Mutex<Vec<Outcome>>,
}

impl Metrics for Recording {
    fn served(&self, served: Served) {
        self.served.lock().unwrap().push(served);
    }
    fn bypassed(&self) {
        self.bypassed.fetch_add(1, Ordering::SeqCst);
    }
    fn shadow(&self, outcome: Outcome) {
        self.shadow.lock().unwrap().push(outcome);
    }
}

/// A wrapped connection over a fresh cache, its fake engine, a plain
/// connection for writes from elsewhere, and recorded metrics.
struct World {
    conn: SyncedConnection<PgConnection>,
    engine: Arc<FakeEngine>,
    other: PgConnection,
    recording: Arc<Recording>,
    prefix: String,
}

impl World {
    fn new(name: &str) -> World {
        static SCHEMA: std::sync::Once = std::sync::Once::new();
        SCHEMA.call_once(|| {
            PgConnection::establish(&url())
                .expect("connect")
                .batch_execute(
                    "CREATE TABLE IF NOT EXISTS sg_rollout (
                        id serial PRIMARY KEY, key text NOT NULL UNIQUE, value text NOT NULL)",
                )
                .expect("schema");
        });
        let engine = Arc::new(FakeEngine::default());
        let cache = Arc::new(Cache::new(64 * 1024 * 1024, engine.clone()));
        let conn = SyncedConnection::with_cache(
            PgConnection::establish(&url()).expect("connect"),
            cache,
            Consistency::Eventual,
        );
        let recording = Arc::new(Recording::default());
        set_metrics(recording.clone());
        let prefix = format!("{name}-{}-", std::process::id());
        let mut other = PgConnection::establish(&url()).expect("connect");
        diesel::delete(sg_rollout::table.filter(sg_rollout::key.like(format!("{prefix}%"))))
            .execute(&mut other)
            .expect("clean");
        World {
            conn,
            engine,
            other,
            recording,
            prefix,
        }
    }

    /// Writes `name` = `value` from elsewhere: the cache never hears.
    fn put(&mut self, name: &str, value: &str) {
        let key = format!("{}{name}", self.prefix);
        diesel::insert_into(sg_rollout::table)
            .values((sg_rollout::key.eq(&key), sg_rollout::value.eq(value)))
            .on_conflict(sg_rollout::key)
            .do_update()
            .set(sg_rollout::value.eq(value))
            .execute(&mut self.other)
            .expect("put");
    }

    /// `name`'s value through the wrapper.
    fn read(&mut self, name: &str) -> String {
        let key = format!("{}{name}", self.prefix);
        sg_rollout::table
            .filter(sg_rollout::key.eq(key))
            .select(sg_rollout::value)
            .first(&mut self.conn)
            .expect("read")
    }

    fn served(&self) -> Vec<Served> {
        self.recording.served.lock().unwrap().clone()
    }
}

/// Each read the cache could serve reports where it was served, once; a
/// read routed to Postgres, or refused, is not one the cache served.
#[test]
fn reads_report_where_they_were_served() {
    let _turn = TURN.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    diesel_streamgres::rollout::set_cache_share(1.0);
    set_shadow_sample(0.0);
    let mut world = World::new("served");
    world.put("a", "1");
    assert_eq!(world.read("a"), "1");
    assert_eq!(world.read("a"), "1");
    assert_eq!(world.served(), [Served::Postgres, Served::Cache]);

    let key = format!("{}a", world.prefix);
    let _: String = sg_rollout::table
        .filter(sg_rollout::key.eq(key))
        .select(sg_rollout::value)
        .via(Route::Postgres)
        .first(&mut world.conn)
        .expect("routed read");
    assert_eq!(world.served().len(), 2, "a read routed to Postgres is not reported");

    let _: i32 = diesel::select(diesel::dsl::sql::<diesel::sql_types::Integer>("1"))
        .get_result(&mut world.conn)
        .expect("refused read");
    assert_eq!(world.served().last(), Some(&Served::Postgres), "a refused read is read from Postgres");
}

/// With a share of none, reads go to Postgres, counted as bypassed, and
/// the engine side is not asked.
#[test]
fn a_share_of_none_sends_reads_to_postgres() {
    let _turn = TURN.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    set_shadow_sample(0.0);
    let mut world = World::new("share");
    world.put("a", "1");
    diesel_streamgres::rollout::set_cache_share(0.0);
    let before = diesel_streamgres::rollout::bypassed();
    assert_eq!(world.read("a"), "1");
    assert_eq!(world.read("a"), "1");
    diesel_streamgres::rollout::set_cache_share(1.0);
    assert_eq!(diesel_streamgres::rollout::bypassed() - before, 2);
    assert_eq!(world.recording.bypassed.load(Ordering::SeqCst), 2);
    assert_eq!(world.served(), [Served::Postgres, Served::Postgres]);
    assert_eq!(world.conn.last_served(), Some(Served::Postgres));
}

/// Shadow reads: a current cached answer agrees; a stale one, with the
/// engine past Postgres's position, is a mismatch; with no position to
/// judge by, unverified. The caller still gets the cached answer.
#[test]
fn shadow_reads_judge_cached_answers() {
    let _turn = TURN.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    diesel_streamgres::rollout::set_cache_share(1.0);
    let mut world = World::new("shadow");
    world.put("a", "1");
    assert_eq!(world.read("a"), "1", "the first read fills the cache");
    set_shadow_sample(1.0);
    let start = counts();

    world.engine.position.store(u64::MAX / 2, Ordering::SeqCst);
    assert_eq!(world.read("a"), "1");
    assert_eq!(counts().agreed - start.agreed, 1);

    world.put("a", "2");
    assert_eq!(world.read("a"), "1", "the caller gets the cached answer");
    assert_eq!(counts().mismatched - start.mismatched, 1);

    world.engine.position.store(0, Ordering::SeqCst);
    assert_eq!(world.read("a"), "1");
    assert_eq!(counts().unverified - start.unverified, 1);

    set_shadow_sample(0.0);
    assert_eq!(
        *world.recording.shadow.lock().unwrap(),
        [Outcome::Agreed, Outcome::Mismatched, Outcome::Unverified]
    );
    let mismatch_logged = diesel_streamgres::metrics::history()
        .iter()
        .any(|(_, event)| event.contains("disagrees with Postgres"));
    assert!(mismatch_logged, "a mismatch is in the history");
}
