/// Back-to-back failures grow the range, cap it, and never retry at zero.
#[test]
fn failures_back_off_with_bounded_jitter() {
    let mut low = Backoff::new();
    let mut high = Backoff::new();
    for ceiling in [1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000] {
        assert_eq!(low.with_sample(0), Duration::from_millis(ceiling / 2));
        assert_eq!(
            high.with_sample(ceiling / 2),
            Duration::from_millis(ceiling)
        );
    }
    for _ in 0..100 {
        let delay = high.next_delay();
        assert!((Duration::from_secs(15)..=Duration::from_secs(30)).contains(&delay));
    }
}

/// A recovered stream restores the initial retry range.
#[test]
fn a_stable_connection_resets_the_retry_range() {
    let mut backoff = Backoff::new();
    for _ in 0..20 {
        backoff.with_sample(0);
    }
    backoff.reset();
    assert_eq!(backoff.with_sample(0), Duration::from_millis(500));
}

use std::time::Instant;

use serde_json::{Value as Json, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;

use super::transport::{Backoff, connect_failure, retry_wait, tls, upgrade_request};
use super::worker::{disconnected, inbound, on_hello};
use super::*;
use crate::cache::Database;
use crate::row::OwnedRow;
use crate::types::{ColumnType, TypeKind};
use diesel::deserialize::FromSqlRow;
use diesel::pg::Pg;
use diesel::sql_types::{Integer, Text};

/// Dropping the pool wakes a long retry wait, including a drop that
/// happens before the worker has registered its notification.
#[tokio::test]
async fn shutdown_interrupts_reconnect_backoff() {
    let (_, _, shared) = worker();
    let waiting = Arc::clone(&shared);
    let task = tokio::spawn(async move { retry_wait(&waiting, Duration::from_secs(30)).await });
    tokio::task::yield_now().await;
    shared.stopped.store(true, Ordering::Release);
    shared.stop.notify_waiters();
    assert!(
        !tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
    );
    assert!(!retry_wait(&shared, Duration::from_secs(30)).await);
}

/// A worker with no socket: `connected` is set by hand and frames are
/// fed to `inbound` directly, so the orderings the reader thread and
/// the subscribing thread can take are forced rather than hoped for.
fn worker() -> (Arc<Worker>, mpsc::UnboundedReceiver<Outgoing>, Arc<Shared>) {
    worker_waiting(Duration::from_secs(5))
}

/// As [`worker`], giving a verdict `wait` to arrive.
fn worker_waiting(
    wait: Duration,
) -> (Arc<Worker>, mpsc::UnboundedReceiver<Outgoing>, Arc<Shared>) {
    worker_budgeted(wait, crate::materialized::DEFAULT_JOIN_WORK)
}

/// [`worker_waiting`] whose joined results may take `join_work` steps.
fn worker_budgeted(
    wait: Duration,
    join_work: u64,
) -> (Arc<Worker>, mpsc::UnboundedReceiver<Outgoing>, Arc<Shared>) {
    let shared = Arc::new(Shared {
        by_key: Mutex::new(HashMap::new()),
        max_subscriptions: 100,
        token: None,
        silence: DEFAULT_SILENCE,
        max_lag: DEFAULT_MAX_LAG,
        max_join_work: join_work,
        types: Mutex::new(HashMap::new()),
        fallbacks: Mutex::new(Vec::new()),
        refusals: Mutex::new(Vec::new()),
        cache: Mutex::new(None),
        listeners: Mutex::new(HashMap::new()),
        stopped: AtomicBool::new(false),
        stop: tokio::sync::Notify::new(),
        watches: Default::default(),
        was_behind: AtomicBool::new(false),
    });
    let (worker, outgoing) = Worker::new(
        0,
        Target::Remote("ws://127.0.0.1:1/never".to_string()),
        wait,
        Arc::clone(&shared),
    );
    greeted(&worker);
    (worker, outgoing, shared)
}

/// The socket is up and the engine has greeted, naming no database.
fn greeted(worker: &Worker) {
    worker.connected.store(true, Ordering::Release);
    *worker.greeting.lock().unwrap() = Greeting::Unnamed;
}

/// The types of table `t`: an `int4` id and a `text` name.
fn t_types() -> Arc<TableTypes> {
    let base = |name: &str, oid: u32| ColumnType {
        name: name.into(),
        oid,
        typmod: -1,
        kind: TypeKind::Base,
    };
    Arc::new(TableTypes {
        columns: vec![base("id", 23), base("name", 25)],
    })
}

/// The key every test subscribes.
fn key() -> QueryKey {
    QueryKey {
        sql: "SELECT id, name FROM t".to_string(),
        binds: Vec::new(),
        context: 0,
    }
}

/// An `accepted` for `id` carrying row 1 of `t`.
fn accepted_with_rows(id: u64) -> String {
    json!(["accepted", {
        "id": id,
        "nodes": [{"table": "t", "key": ["id"], "parent": null, "rows": [{"id": 1, "name": "a"}]}],
        "columns": [["t", "id"], ["t", "name"]],
        "names": ["id", "name"],
        "count": false,
        "order": [["id", "asc"]],
        "limit": null,
        "offset": 0
    }])
    .to_string()
}

/// A `delta` for `id` putting row 2 of `t`.
fn delta(id: u64) -> String {
    json!(["delta", {"id": id, "changes": [{"table": "t", "put": [{"id": 2, "name": "b"}], "del": []}]}]).to_string()
}

/// The rows as `(id, name)` pairs.
fn decoded(rows: &Arc<[OwnedRow]>) -> Vec<(i32, String)> {
    rows.iter()
        .map(|row| {
            <(i32, String) as FromSqlRow<(Integer, Text), Pg>>::build_from_row(row).unwrap()
        })
        .collect()
}

/// Starts a subscribe on another thread and returns once the worker
/// has it pending.
fn subscribing(worker: &Arc<Worker>) -> std::thread::JoinHandle<Outcome> {
    let subscriber = Arc::clone(worker);
    let handle = std::thread::spawn(move || subscriber.subscribe(&key()));
    let started = Instant::now();
    while worker.state.lock().unwrap().pending.is_empty() {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the subscribe never became pending"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    handle
}

/// An `accepted` for `id` carrying `rows` of `t`.
fn accepted_holding(id: u64, rows: Json) -> String {
    let mut frame: Json = serde_json::from_str(&accepted_with_rows(id)).unwrap();
    frame[1]["nodes"][0]["rows"] = rows;
    frame.to_string()
}

/// A `delta` for `id` with `put` rows and `del` keys of `t`.
fn delta_of(id: u64, put: Json, del: Json) -> String {
    json!(["delta", {"id": id, "changes": [{"table": "t", "put": put, "del": del}]}]).to_string()
}

/// `key()` followed for a callback, through `listening`, its tables'
/// types known.
fn listened(shared: &Shared, url: &str) -> Arc<Listening> {
    shared.types.lock().unwrap().insert("t".to_string(), t_types());
    let listening = Listening::new(
        url.to_string(),
        crate::cache::Database {
            system: "1".to_string(),
            name: "test".to_string(),
        },
    );
    shared
        .listeners
        .lock()
        .unwrap()
        .insert(key(), Arc::clone(&listening));
    listening
}

/// A subscription's changes: its first rows as present, then each
/// delta's rows as added, updated and removed; after a reconnect,
/// what differs between the result before and the engine's new
/// snapshot. Its store never reaches a cache.
#[test]
fn a_listened_query_hands_its_changes_over() {
    use crate::listen::Change;
    let (worker, _outgoing, shared) = worker();
    let listening = listened(&shared, "postgres://unused");
    let changes = || listening.take::<(Integer, Text), (i32, String)>();
    let handle = subscribing(&worker);
    inbound(&worker, &accepted_with_rows(1));
    let Outcome::Verdict(Verdict::Rows { .. }) = handle.join().unwrap() else {
        panic!("rows mode");
    };
    assert_eq!(changes(), vec![Change::Present((1, "a".into()))]);

    inbound(&worker, &delta(1));
    inbound(&worker, &delta_of(1, json!([{"id": 1, "name": "z"}]), json!([])));
    inbound(&worker, &delta_of(1, json!([]), json!([{"id": 2}])));
    assert_eq!(
        changes(),
        vec![
            Change::Added((2, "b".into())),
            Change::Updated {
                old: (1, "a".into()),
                new: (1, "z".into())
            },
            Change::Removed((2, "b".into())),
        ]
    );

    disconnected(&worker);
    worker.up();
    greeted(&worker);
    shared.types.lock().unwrap().insert("t".to_string(), t_types());
    inbound(
        &worker,
        &accepted_holding(1, json!([{"id": 1, "name": "y"}, {"id": 3, "name": "c"}])),
    );
    assert_eq!(
        changes(),
        vec![
            Change::Updated {
                old: (1, "z".into()),
                new: (1, "y".into())
            },
            Change::Added((3, "c".into())),
        ],
        "the difference across the reconnect"
    );
    inbound(&worker, &delta_of(1, json!([]), json!([{"id": 3}])));
    assert_eq!(changes(), vec![Change::Removed((3, "c".into()))], "followed again");
}

/// The types of table `c`, joined under `t` on `t.id = c.up`.
fn c_types() -> Arc<TableTypes> {
    let base = |name: &str| ColumnType {
        name: name.into(),
        oid: 23,
        typmod: -1,
        kind: TypeKind::Base,
    };
    Arc::new(TableTypes {
        columns: vec![base("id"), base("up")],
    })
}

/// An `accepted` in rows mode for `t` row 1 joined to `children`
/// rows of `c`, projecting `t.id` and `c.id`.
fn accepted_join(id: u64, children: i32) -> String {
    let rows: Vec<Json> = (0..children).map(|c| json!({"id": c, "up": 1})).collect();
    json!(["accepted", {
        "id": id,
        "nodes": [
            {"table": "t", "key": ["id"], "parent": null, "rows": [{"id": 1, "name": "a"}]},
            {"table": "c", "key": ["id"], "parent": "t", "parent_column": "id",
             "child_column": "up", "inner": true, "rows": rows}
        ],
        "columns": [["t", "id"], ["c", "id"]],
        "names": ["id", "child"],
        "count": false,
        "order": [["id", "asc"]],
        "limit": null,
        "offset": 0
    }])
    .to_string()
}

/// A cached join whose result needs more work than the budget goes
/// to Postgres refills, its reason recorded as a fallback: not a
/// refusal, which would keep the query from the engine for a while,
/// since the rows may shrink.
#[test]
fn a_cached_join_over_its_budget_falls_back_without_a_refusal() {
    let (worker, _outgoing, shared) = worker_budgeted(Duration::from_secs(5), 10);
    shared.types.lock().unwrap().insert("t".to_string(), t_types());
    shared.types.lock().unwrap().insert("c".to_string(), c_types());
    let handle = subscribing(&worker);
    inbound(&worker, &accepted_join(1, 20));
    let Outcome::Verdict(Verdict::Accepted { .. }) = handle.join().unwrap() else {
        panic!("refills");
    };
    let fallbacks = shared.fallbacks.lock().unwrap().clone();
    assert!(
        fallbacks
            .last()
            .is_some_and(|reason| reason.contains("needs more work than its budget allows")),
        "{fallbacks:?}"
    );
    assert!(shared.refusals.lock().unwrap().is_empty(), "not refused");
    assert!(!worker.state.lock().unwrap().materialized.contains_key(&1), "its store went");
}

/// A callback on a join is followed while its result fits the
/// budget; once a change takes it past, there is no Postgres to read
/// instead, so the subscription ends and says why, and later deltas
/// build nothing.
#[test]
fn a_listened_join_past_its_budget_ends_and_says_why() {
    use crate::listen::Change;
    let (worker, _outgoing, shared) = worker_budgeted(Duration::from_secs(5), 40);
    let listening = listened(&shared, "postgres://unused");
    shared.types.lock().unwrap().insert("c".to_string(), c_types());
    let changes = || listening.take::<(Integer, Integer), (i32, i32)>();
    let handle = subscribing(&worker);
    inbound(&worker, &accepted_join(1, 3));
    let Outcome::Verdict(Verdict::Rows { .. }) = handle.join().unwrap() else {
        panic!("rows mode");
    };
    assert_eq!(
        changes(),
        vec![
            Change::Present((1, 0)),
            Change::Present((1, 1)),
            Change::Present((1, 2))
        ]
    );
    assert!(listening.ended().is_none());

    let more: Vec<Json> = (3..40).map(|c| json!({"id": c, "up": 1})).collect();
    let delta = |put: Vec<Json>| {
        json!(["delta", {"id": 1, "changes": [{"table": "c", "put": put, "del": []}]}]).to_string()
    };
    inbound(&worker, &delta(more));
    let reason = listening.ended().expect("ended");
    assert!(reason.contains("needs more work than its budget allows"), "{reason}");
    assert!(listening.over_budget());
    assert_eq!(changes(), Vec::new(), "no changes past the budget");
    inbound(&worker, &delta(vec![json!({"id": 99, "up": 1})]));
    assert_eq!(changes(), Vec::new(), "nothing after it ended");
}

/// A callback on a join whose first result is already past the
/// budget is not started: the subscribe learns it ended over budget,
/// which it reports apart from a query the engine cannot serve.
#[test]
fn a_listened_join_that_starts_past_its_budget_is_refused_as_such() {
    let (worker, _outgoing, shared) = worker_budgeted(Duration::from_secs(5), 40);
    let listening = listened(&shared, "postgres://unused");
    shared.types.lock().unwrap().insert("c".to_string(), c_types());
    let handle = subscribing(&worker);
    inbound(&worker, &accepted_join(1, 40));
    let Outcome::Verdict(Verdict::Accepted { .. }) = handle.join().unwrap() else {
        panic!("not in rows mode");
    };
    assert!(listening.over_budget());
    let reason = listening.ended().expect("ended");
    assert!(reason.contains("needs more work than its budget allows"), "{reason}");
}

/// A subscription is checked against the database it was rendered
/// on, whatever a cache's is: an engine streaming another is not
/// asked.
#[test]
fn a_listened_query_is_not_asked_of_an_engine_on_another_database() {
    let hello = |system: &str, name: &str| json!({"database": {"system": system, "name": name}});
    let (worker, mut outgoing, shared) = worker();
    listened(&shared, "postgres://unused");
    on_hello(&worker, &hello("1", "elsewhere"));
    let Outcome::Verdict(Verdict::Refused(reason)) = worker.subscribe(&key()) else {
        panic!("refused");
    };
    assert!(reason.contains("reads here go to `test` of system 1"), "{reason}");
    assert!(outgoing.try_recv().is_err(), "the engine was not asked");
    on_hello(&worker, &hello("1", "test"));
    assert!(worker.wrong_database(&key()).is_none());
}

/// After a reconnect, a subscription whose tables' types were
/// forgotten looks them up on its own connection; one that cannot
/// lets the key go and says why, rather than stopping silently.
#[test]
fn a_listened_query_that_cannot_relearn_its_types_says_so() {
    let (worker, _outgoing, shared) = worker();
    let listening = listened(&shared, "postgresql://nobody@127.0.0.1:1/none");
    let handle = subscribing(&worker);
    inbound(&worker, &accepted_with_rows(1));
    let Outcome::Verdict(Verdict::Rows { .. }) = handle.join().unwrap() else {
        panic!("rows mode");
    };
    disconnected(&worker);
    worker.up();
    greeted(&worker);
    inbound(&worker, &accepted_with_rows(1));
    let started = Instant::now();
    while worker.state.lock().unwrap().by_key.contains_key(&key()) {
        assert!(started.elapsed() < Duration::from_secs(10), "never let go");
        std::thread::sleep(Duration::from_millis(5));
    }
    let errors = listening.errors();
    assert!(
        errors.iter().any(|error| error.contains("connecting to look the column types up")),
        "{errors:?}"
    );
}

/// The reader thread handles `accepted` and a delta back to back;
/// whether the subscribing thread has woken up in between must not
/// matter: the store has the delta's row.
#[test]
fn a_delta_right_behind_accepted_is_in_the_store() {
    let (worker, _outgoing, shared) = worker();
    shared
        .types
        .lock()
        .unwrap()
        .insert("t".to_string(), t_types());
    let handle = subscribing(&worker);
    inbound(&worker, &accepted_with_rows(1));
    inbound(&worker, &delta(1));
    let Outcome::Verdict(Verdict::Rows { .. }) = handle.join().unwrap() else {
        panic!("rows mode");
    };
    let Outcome::Verdict(Verdict::Rows { rows, .. }) = worker.subscribe(&key()) else {
        panic!("held in rows mode");
    };
    assert_eq!(
        decoded(&rows),
        vec![(1, "a".into()), (2, "b".into())],
        "the store has the delta's row"
    );
}

/// While the first reader looks the types up, the store waits for
/// its rows: a second read of the key goes to Postgres and must fill
/// nothing, or its rows could land after the store's entry and
/// replace it with rows no delta ever updates. Once built, the store
/// serves.
#[test]
fn a_read_while_the_store_is_built_fills_nothing() {
    let (worker, _outgoing, _shared) = worker();
    let handle = subscribing(&worker);
    inbound(&worker, &accepted_with_rows(1));
    let Outcome::NeedsTypes { snapshot, .. } = handle.join().unwrap() else {
        panic!("the types are looked up");
    };
    let Outcome::Verdict(Verdict::Refused(reason)) = worker.subscribe(&key()) else {
        panic!("not a refill while the store is built");
    };
    assert!(reason.contains("being built"), "{reason}");
    assert!(
        Shared::TRANSIENT.iter().any(|transient| reason.starts_with(transient)),
        "not final: asked again next time"
    );
    worker
        .shared
        .types
        .lock()
        .unwrap()
        .insert("t".to_string(), t_types());
    let Verdict::Rows { .. } = worker.materialize_key(&key(), snapshot) else {
        panic!("rows mode");
    };
    let Outcome::Verdict(Verdict::Rows { .. }) = worker.subscribe(&key()) else {
        panic!("held in rows mode");
    };
}

/// Types learnt before a migration lack the column it added: a
/// snapshot naming that column asks for the table's types again,
/// and the fresh ones serve the rows.
#[test]
fn a_column_the_known_types_lack_asks_for_the_types_again() {
    let (worker, _outgoing, shared) = worker();
    let stale = Arc::new(TableTypes {
        columns: vec![ColumnType {
            name: "id".into(),
            oid: 23,
            typmod: -1,
            kind: TypeKind::Base,
        }],
    });
    shared.types.lock().unwrap().insert("t".to_string(), stale);
    let handle = subscribing(&worker);
    inbound(&worker, &accepted_with_rows(1));
    let Outcome::NeedsTypes { snapshot, missing } = handle.join().unwrap() else {
        panic!("stale types are looked up again");
    };
    assert_eq!(missing, vec!["t".to_string()]);
    shared
        .types
        .lock()
        .unwrap()
        .insert("t".to_string(), t_types());
    let Verdict::Rows { .. } = worker.materialize_key(&key(), snapshot) else {
        panic!("rows mode with the fresh types");
    };
}

/// A delta that lands while the caller is off looking the types up
/// queues and is applied once the store is built; a delta after that
/// reaches the same store.
#[test]
fn deltas_during_the_type_lookup_queue_and_apply_once() {
    let (worker, _outgoing, shared) = worker();
    let handle = subscribing(&worker);
    inbound(&worker, &accepted_with_rows(1));
    let Outcome::NeedsTypes { snapshot, missing } = handle.join().unwrap() else {
        panic!("the types of t are not known yet");
    };
    assert_eq!(missing, vec!["t".to_string()]);
    inbound(&worker, &delta(1));
    shared
        .types
        .lock()
        .unwrap()
        .insert("t".to_string(), t_types());
    let Verdict::Rows { rows, .. } = worker.materialize_key(&key(), snapshot) else {
        panic!(
            "rows mode once the types are in: {:?}",
            shared.fallbacks.lock().unwrap()
        );
    };
    assert_eq!(decoded(&rows), vec![(1, "a".into()), (2, "b".into())]);
    inbound(&worker, &json!(["delta", {"id": 1, "changes": [{"table": "t", "put": [], "del": [{"id": 1}]}]}]).to_string());
    let Outcome::Verdict(Verdict::Rows { rows, .. }) = worker.subscribe(&key()) else {
        panic!("held in rows mode");
    };
    assert_eq!(decoded(&rows), vec![(2, "b".into())]);
}

/// A subscribe whose socket drops is refused and forgotten. Its frame
/// was still queued and went out on the next socket, so the engine's
/// acceptance names an id nobody holds: the engine is told to drop
/// it.
#[test]
fn a_wait_cut_by_a_socket_drop_is_forgotten_and_a_late_accept_is_unsubscribed() {
    let (worker, mut outgoing, _shared) = worker();
    let handle = subscribing(&worker);
    disconnected(&worker);
    let Outcome::Verdict(Verdict::Refused(reason)) = handle.join().unwrap() else {
        panic!("refused");
    };
    assert_eq!(reason, "the engine connection dropped");
    assert!(
        worker.state.lock().unwrap().by_id.is_empty(),
        "nothing to resubscribe"
    );
    assert_eq!(worker.held.load(Ordering::Relaxed), 0);
    greeted(&worker);
    inbound(&worker, &accepted_with_rows(1));
    let mut unsubscribed = false;
    while let Ok(message) = outgoing.try_recv() {
        if matches!(message, Outgoing::Unsubscribe { id: 1 }) {
            unsubscribed = true;
        }
    }
    assert!(unsubscribed, "the engine is told to drop it");
    assert!(
        worker.state.lock().unwrap().materialized.is_empty(),
        "no store for an id nobody holds"
    );
}

/// When the socket drops and comes back, an accepted key is held,
/// resubscribed, and not to be trusted until the engine says so: a
/// read in the window goes live.
#[test]
fn a_resubscribed_key_is_pending_until_the_engine_accepts_it() {
    let (worker, _outgoing, _shared) = worker();
    let handle = subscribing(&worker);
    inbound(&worker, &json!(["accepted", {"id": 1}]).to_string());
    assert!(matches!(
        handle.join().unwrap(),
        Outcome::Verdict(Verdict::Accepted { since: None })
    ));
    disconnected(&worker);
    assert!(
        worker.state.lock().unwrap().by_id.contains_key(&1),
        "an accepted key stays held"
    );
    greeted(&worker);
    let Outcome::Verdict(Verdict::Refused(reason)) = worker.subscribe(&key()) else {
        panic!("a read in the window goes live");
    };
    assert_eq!(reason, "the subscription is pending");
    inbound(&worker, &json!(["accepted", {"id": 1}]).to_string());
    assert!(matches!(
        worker.subscribe(&key()),
        Outcome::Verdict(Verdict::Accepted { since: None })
    ));
}

/// A dropped socket forgets the types, so the rows a resubscribe
/// brings back cannot be built: the key is let go, not kept
/// refilling from Postgres, and the next read subscribes it afresh
/// and is asked for the types. With the types known, a resubscribe
/// is served from rows again at once.
#[test]
fn a_resubscribe_without_types_lets_the_key_go() {
    let (worker, mut outgoing, shared) = worker();
    shared.types.lock().unwrap().insert("t".to_string(), t_types());
    let handle = subscribing(&worker);
    inbound(&worker, &accepted_with_rows(1));
    assert!(matches!(handle.join().unwrap(), Outcome::Verdict(Verdict::Rows { .. })));

    disconnected(&worker);
    greeted(&worker);
    shared.types.lock().unwrap().insert("t".to_string(), t_types());
    inbound(&worker, &accepted_with_rows(1));
    assert!(
        worker.state.lock().unwrap().materialized.contains_key(&1),
        "types known: rows again"
    );

    disconnected(&worker);
    greeted(&worker);
    while outgoing.try_recv().is_ok() {}
    inbound(&worker, &accepted_with_rows(1));
    assert!(matches!(outgoing.try_recv(), Ok(Outgoing::Unsubscribe { id: 1 })));
    {
        let state = worker.state.lock().unwrap();
        assert!(state.by_id.is_empty(), "the key is let go");
        assert!(state.materialized.is_empty());
    }
    assert!(shared.fallbacks.lock().unwrap().is_empty(), "not turned to refills");

    let handle = subscribing(&worker);
    inbound(&worker, &accepted_with_rows(2));
    let Outcome::NeedsTypes { missing, .. } = handle.join().unwrap() else {
        panic!("the next read looks the types up");
    };
    assert_eq!(missing, vec!["t".to_string()]);
}

/// An engine that takes the subscribe and never answers: the wait
/// ends at the verdict timeout, refused for a reason that passes,
/// and the id is withdrawn, so a late `accepted` registers nothing.
#[test]
fn a_subscribe_nobody_answers_is_given_up_in_time() {
    let (worker, mut outgoing, shared) = worker_waiting(Duration::from_millis(50));
    let started = Instant::now();
    let Outcome::Verdict(Verdict::Refused(reason)) = worker.subscribe(&key()) else {
        panic!("refused");
    };
    assert!(started.elapsed() >= Duration::from_millis(50));
    assert_eq!(reason, "the engine did not answer in time");
    assert!(Shared::TRANSIENT.contains(&reason.as_str()));
    assert!(matches!(
        outgoing.try_recv(),
        Ok(Outgoing::Subscribe { id: 1, .. })
    ));
    assert!(matches!(
        outgoing.try_recv(),
        Ok(Outgoing::Unsubscribe { id: 1 })
    ));
    let state = worker.state.lock().unwrap();
    assert!(state.pending.is_empty(), "nothing waits");
    assert!(
        state.by_key.is_empty() && state.by_id.is_empty(),
        "nothing is held"
    );
    assert!(
        shared.by_key.lock().unwrap().is_empty(),
        "the key is not counted against the cap"
    );
}

/// The engine that comes back after a dropped socket may know a type
/// it refused before, so its refusals are forgotten with the socket.
#[test]
fn a_dropped_socket_forgets_the_engines_refusals() {
    let (worker, _outgoing, shared) = worker();
    let cache = Arc::new(Cache::new(
        1 << 20,
        Arc::new(crate::subscriptions::Unsynced),
    ));
    *shared.cache.lock().unwrap() = Some(Arc::downgrade(&cache));
    cache.refused(&key(), "`$2`: a bind of type oid 16406 is not supported");
    assert!(
        cache.refused_recently(&key()).is_some(),
        "remembered while the engine is the same"
    );
    disconnected(&worker);
    assert!(
        cache.refused_recently(&key()).is_none(),
        "forgotten with the socket"
    );
}

/// An engine that streams another database than the cache's reads go
/// to is not asked for rows, which would be served as this one's;
/// the right one is, and one that names no database is taken on
/// trust. A key resubscribed after a reconnect to the wrong one is
/// let go and its entry left stale.
#[test]
fn an_engine_on_another_database_is_not_asked() {
    let ours = Database {
        system: "7699115118699778086".into(),
        name: "tickets".into(),
    };
    let hello = |system: &str, name: &str| json!({"database": {"system": system, "name": name}});
    let (worker, mut outgoing, shared) = worker_waiting(Duration::from_millis(50));
    let cache = Arc::new(Cache::new(1 << 20, Arc::new(crate::subscriptions::Unsynced)));
    *shared.cache.lock().unwrap() = Some(Arc::downgrade(&cache));
    cache.learn_database(Some(ours.clone()));

    on_hello(&worker, &hello("7699114362382213158", "tickets"));
    let Outcome::Verdict(Verdict::Refused(reason)) = worker.subscribe(&key()) else {
        panic!("refused");
    };
    assert!(reason.contains("streams database `tickets` of system 7699114362382213158"), "{reason}");
    assert!(outgoing.try_recv().is_err(), "the engine was not asked");

    on_hello(&worker, &json!({}));
    assert!(worker.wrong_database(&key()).is_none(), "an engine naming no database is trusted");
    on_hello(&worker, &hello(&ours.system, &ours.name));
    assert!(worker.wrong_database(&key()).is_none());
    let handle = subscribing(&worker);
    inbound(&worker, &json!(["accepted", {"id": 1}]).to_string());
    assert!(matches!(handle.join().unwrap(), Outcome::Verdict(Verdict::Accepted { .. })));
    assert!(matches!(outgoing.try_recv(), Ok(Outgoing::Subscribe { id: 1, .. })));

    disconnected(&worker);
    assert!(worker.state.lock().unwrap().by_id.contains_key(&1), "held to resubscribe");
    greeted(&worker);
    on_hello(&worker, &hello(&ours.system, "staging"));
    inbound(&worker, &json!(["accepted", {"id": 1}]).to_string());
    assert!(matches!(outgoing.try_recv(), Ok(Outgoing::Unsubscribe { id: 1 })));
    assert!(worker.state.lock().unwrap().by_id.is_empty(), "the key is let go");
}

/// The engine's lag is what its latest frame said plus the time since,
/// so an engine that stops reporting counts as falling further behind;
/// none is known until a frame says, nor while it says `null`, nor
/// once the socket drops.
#[test]
fn the_lag_grows_while_the_engine_says_nothing() {
    let (worker, _outgoing, _shared) = worker();
    assert_eq!(worker.lag(), None);
    inbound(&worker, &json!(["lag", {"ms": null}]).to_string());
    assert_eq!(worker.lag(), None);
    inbound(&worker, &json!(["lag", {"ms": 1500}]).to_string());
    let lag = worker.lag().expect("said");
    assert!(lag >= Duration::from_millis(1500) && lag < Duration::from_millis(1600), "{lag:?}");
    *worker.lag.lock().unwrap() = Some((Duration::from_millis(1500), Instant::now() - Duration::from_secs(1)));
    assert!(worker.lag().unwrap() >= Duration::from_millis(2500), "a second of silence adds a second");
    disconnected(&worker);
    assert_eq!(worker.lag(), None, "a dropped socket says nothing");
}

/// Reads stop trusting the cache once the engine is further behind
/// than the limit, and trust it again when it catches up; a zero
/// limit always trusts it.
#[test]
fn a_lagging_engine_sends_reads_to_postgres() {
    let client = EngineClient::with_config("ws://127.0.0.1:1/never/s2s/v1", Config::default());
    let worker = &client.pool.workers[0];
    worker.connected.store(true, Ordering::Release);
    assert!(!client.behind(), "no frame, no lag");
    inbound(worker, &json!(["lag", {"ms": 50}]).to_string());
    assert!(!client.behind());
    inbound(worker, &json!(["lag", {"ms": 2500}]).to_string());
    assert!(client.behind());
    assert!(client.lag().unwrap() >= Duration::from_millis(2500));
    inbound(worker, &json!(["lag", {"ms": 0}]).to_string());
    assert!(!client.behind(), "caught up");
    let trusting = EngineClient::with_config(
        "ws://127.0.0.1:1/never/s2s/v1",
        Config {
            max_lag: Duration::ZERO,
            ..Config::default()
        },
    );
    trusting.pool.workers[0].connected.store(true, Ordering::Release);
    inbound(&trusting.pool.workers[0], &json!(["lag", {"ms": 60000}]).to_string());
    assert!(!trusting.behind());
}

/// The client's position is the least any connected socket has
/// heard, and unknown while one has heard none: its keys may be
/// behind the others'. A socket that drops forgets its own, and only
/// connected sockets count.
#[test]
fn the_position_is_the_least_every_connected_socket_has_heard() {
    let client = EngineClient::with_config(
        "ws://127.0.0.1:1/never/s2s/v1",
        Config {
            connections: 2,
            ..Config::default()
        },
    );
    let (first, second) = (&client.pool.workers[0], &client.pool.workers[1]);
    assert_eq!(client.position(), None, "nothing connected");
    first.connected.store(true, Ordering::Release);
    second.connected.store(true, Ordering::Release);
    inbound(first, &json!(["lag", {"ms": 0, "lsn": 900}]).to_string());
    assert_eq!(client.position(), None, "the second has heard none");
    inbound(second, &json!(["lag", {"ms": 0, "lsn": 700}]).to_string());
    assert_eq!(client.position(), Some(700));
    inbound(second, &json!(["lag", {"ms": 0, "lsn": 600}]).to_string());
    assert_eq!(client.position(), Some(700), "a position never moves back");
    inbound(second, &json!(["lag", {"ms": 0, "lsn": null}]).to_string());
    assert_eq!(client.position(), Some(700), "an older frame shape keeps it");
    disconnected(second);
    assert_eq!(client.position(), Some(900), "only connected sockets count");
    second.connected.store(true, Ordering::Release);
    assert_eq!(client.position(), None, "reconnected, it has heard none");
}

/// This client's own refusal reasons all pass; the engine's are final.
#[test]
fn every_client_side_reason_is_transient() {
    let client = EngineClient::with_config("ws://127.0.0.1:1/never/s2s/v1", Config::default());
    for reason in Shared::TRANSIENT {
        assert!(
            !client.refusal_is_final(&format!("{reason} and more")),
            "{reason}"
        );
    }
    assert!(client.refusal_is_final("the query joins more tables than the engine follows"));
}

/// The defaults are the documented ones, and an unset variable
/// leaves its default.
#[test]
fn config_defaults_and_env() {
    let defaults = Config::default();
    assert_eq!(defaults.connections, DEFAULT_CONNECTIONS);
    assert_eq!(defaults.max_subscriptions, DEFAULT_MAX_SUBSCRIPTIONS);
    assert_eq!(defaults.verdict_timeout, DEFAULT_VERDICT_TIMEOUT);
    assert_eq!(env_number("STREAMGRES_S2S_TEST_UNSET_VARIABLE"), None);
}

/// A token is in plain text over `ws://` to anything but this
/// machine, and never over `wss://`.
#[test]
fn a_token_over_plain_ws_to_another_host_is_named() {
    assert!(token_in_clear("ws://engine:4848/zero/s2s/v1"));
    assert!(token_in_clear("ws://10.0.0.7/zero/s2s/v1"));
    assert!(!token_in_clear("ws://127.0.0.1:4848/zero/s2s/v1"));
    assert!(!token_in_clear("ws://localhost/zero/s2s/v1"));
    assert!(!token_in_clear("ws://[::1]:4848/zero/s2s/v1"));
    assert!(!token_in_clear("wss://engine:4848/zero/s2s/v1"));
    assert!(tls().is_some(), "the TLS config builds");
}

/// The token goes out as a bearer, and only when there is one; it
/// never shows in the configuration's `Debug`.
#[test]
fn the_token_goes_out_as_a_bearer() {
    let url = "ws://127.0.0.1:4848/zero/s2s/v1";
    let plain = upgrade_request(url, None).expect("a request");
    assert!(plain.headers().get(AUTHORIZATION).is_none());
    let with = upgrade_request(url, Some("s3cret")).expect("a request");
    assert_eq!(with.headers()[AUTHORIZATION], "Bearer s3cret");
    let bad = upgrade_request(url, Some("bad\nline")).expect_err("no header");
    assert!(bad.contains("STREAMGRES_S2S_TOKEN"), "{bad}");
    assert!(upgrade_request("not a url", None).is_err());
    let config = Config {
        token: Some("s3cret".into()),
        ..Config::default()
    };
    assert!(!format!("{config:?}").contains("s3cret"));
}

/// A pool over `workers`, as `with_config` would build it.
fn pool_of(workers: Vec<Arc<Worker>>, shared: Arc<Shared>) -> EngineClient {
    EngineClient {
        pool: Arc::new(Pool {
            workers,
            shared,
            config: Config::default(),
        }),
    }
}

/// The engine's `hello` names its per-connection limit: a full socket
/// is passed over, and with every socket full the subscribe is
/// refused here, transiently, without asking the engine.
#[test]
fn a_socket_at_the_engine_limit_is_passed_over() {
    let (worker, mut outgoing, shared) = worker();
    assert!(worker.has_room(), "no limit until the engine names one");
    inbound(
        &worker,
        r#"["hello", {"protocol": 1, "max_subscriptions": 1}]"#,
    );
    assert_eq!(worker.room.load(Ordering::Relaxed), 1);
    let client = pool_of(vec![Arc::clone(&worker)], Arc::clone(&shared));
    assert!(client.least_loaded().is_ok());

    worker.held.store(1, Ordering::Relaxed);
    let reason = client.least_loaded().err().expect("full");
    assert!(
        reason.starts_with("the subscription limit of 1 per connection"),
        "{reason}"
    );
    assert!(!client.refusal_is_final(&reason));
    match Subscriptions::subscribe(&client, &key()) {
        Outcome::Verdict(Verdict::Refused(refused)) => assert_eq!(refused, reason),
        _ => panic!("expected a refusal"),
    }
    assert!(outgoing.try_recv().is_err(), "the engine was not asked");

    inbound(
        &worker,
        r#"["hello", {"protocol": 1, "max_subscriptions": null}]"#,
    );
    assert!(worker.has_room(), "a later hello with no limit lifts it");
}

/// While a socket is down, its refusals say why it last failed to
/// connect, and the client lists it; a success clears it.
#[test]
fn a_failed_connect_is_named_in_the_refusals() {
    let (worker, _outgoing, shared) = worker();
    worker.connected.store(false, Ordering::Release);
    let client = pool_of(vec![Arc::clone(&worker)], Arc::clone(&shared));
    assert_eq!(
        client.least_loaded().err().as_deref(),
        Some("the engine is not connected")
    );

    let response = tokio_tungstenite::tungstenite::http::Response::builder()
        .status(401)
        .body(None)
        .unwrap();
    let error = tokio_tungstenite::tungstenite::Error::Http(Box::new(response));
    worker.connect_failed(Some(connect_failure(&error)));
    let reason = client.least_loaded().err().expect("down");
    assert_eq!(
        reason,
        "the engine is not connected: the engine refused the token (check STREAMGRES_S2S_TOKEN)"
    );
    assert!(!client.refusal_is_final(&reason));
    assert_eq!(client.connect_errors().len(), 1);

    worker.connect_failed(None);
    assert!(client.connect_errors().is_empty());
}

/// A pool asked for no sockets opens one.
#[test]
fn zero_connections_become_one() {
    let client = EngineClient::with_config(
        "ws://127.0.0.1:1/never/s2s/v1",
        Config {
            connections: 0,
            ..Config::default()
        },
    );
    assert_eq!(client.config().connections, 1);
    assert_eq!(client.per_worker(), vec![0]);
    assert!(!client.is_connected());
}
