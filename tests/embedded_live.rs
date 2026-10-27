//! An embedded engine against a live PostgreSQL: it starts, serves a
//! session the S2S frames over channels, starts over on a fresh
//! temporary slot when its feed is cut, and leaves no slot behind when it
//! stops. Needs `STREAMGRES_PG_DSN` with `wal_level = logical`; skips
//! itself otherwise. Each test runs in a database of its own
//! ([`support::Database`]), its trigger stack with it.

use std::time::{Duration, Instant};

use serde_json::{Value, json};
use streamgres::embedded::{Config, Engine, Session, Setup, Slot, State, setup_sql};
use tokio_postgres::{Client, NoTls};

mod support;

use support::Database;

/// A database of the test tagged `tag`'s own, when a server is given.
fn dsn(tag: &str) -> Option<Database> {
    Database::create(&format!("embedded_{tag}"))
}

/// A connection for the test's own statements.
async fn admin(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// A test's own names, so tests run side by side: its table
/// (`embedded_live_<tag>_items`), its publication, and the app its
/// schema-change trigger stack is installed for.
struct Names {
    table: String,
    publication: String,
    app: String,
}

/// The names of the test tagged `tag`.
fn names(tag: &str) -> Names {
    Names {
        table: format!("embedded_live_{tag}_items"),
        publication: format!("embedded_live_{tag}_pub"),
        app: format!("embedded_{tag}"),
    }
}

/// The table, three rows, the publication and the schema-change trigger
/// stack, as an application's migrations would set them up.
async fn set_up(client: &Client, names: &Names) {
    let (table, publication) = (&names.table, &names.publication);
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (id int8 PRIMARY KEY, name text NOT NULL, n int4 NOT NULL);
             INSERT INTO {table} VALUES (1, 'one', 1), (2, 'two', 2), (3, 'three', 3);
             DROP PUBLICATION IF EXISTS {publication};
             CREATE PUBLICATION {publication} FOR ALL TABLES;"
        ))
        .await
        .expect("set up the table");
    client
        .batch_execute(&setup_sql(&names.app, 0, publication))
        .await
        .expect("set up the trigger stack");
}

/// An engine's configuration for a test's names.
fn config(dsn: &str, names: &Names) -> Config {
    let mut config = Config::new(dsn);
    config.publication = names.publication.clone();
    config.app = names.app.clone();
    config.schemas = vec!["public".to_owned()];
    config.ddl_trigger = format!("{}_ddl_end_0", names.app);
    config.ddl_prefix = format!("{}/0/ddl", names.app);
    config.restart_after = Duration::from_millis(200);
    config
}

/// Whether `slot` exists, and whether it is temporary.
async fn slot(client: &Client, slot: &str) -> Option<bool> {
    client
        .query_opt(
            "SELECT temporary FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .expect("slots")
        .map(|row| row.get(0))
}

/// Waits up to `limit` for `engine` to be in a state `wanted` accepts.
async fn reach(engine: &Engine, wanted: impl Fn(&State) -> bool, limit: Duration) {
    let mut states = engine.states();
    let reached = tokio::time::timeout(limit, states.wait_for(|state| wanted(state))).await;
    assert!(reached.is_ok(), "never reached; still {:?}", engine.state());
}

/// The next frame within five seconds, `lag` frames passed over.
async fn next(session: &mut Session) -> Option<Value> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let text = tokio::time::timeout(left, session.recv()).await.ok()??;
        let frame: Value = serde_json::from_str(&text).expect("json");
        if frame[0] != "lag" {
            return Some(frame);
        }
    }
}

/// The engine's whole life: start, serve, a cut feed, a fresh start, stop.
#[test]
fn an_embedded_engine_serves_restarts_and_leaves_no_slot() {
    let Some(dsn) = dsn("life") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let names = names("life");
        let table = &names.table;
        let client = admin(&dsn).await;
        set_up(&client, &names).await;
        let engine = Engine::start(config(&dsn, &names));
        reach(&engine, |state| *state == State::Serving, Duration::from_secs(30)).await;
        let first = engine.slot().expect("a slot while it runs");
        assert_eq!(slot(&client, &first).await, Some(true), "a temporary slot");

        let mut session = engine.connect().expect("a session");
        let hello = next(&mut session).await.expect("hello");
        assert_eq!(hello[0], "hello");
        assert!(hello[1]["database"]["name"].is_string(), "{hello}");
        let sql = format!(r#"SELECT "id", "name" FROM "{table}" WHERE "n" > $1 ORDER BY "id" ASC"#);
        let bind = json!({"oid": 23, "bytes": "AAAAAQ=="});
        session
            .sender()
            .send(json!(["subscribe", {"id": 1, "sql": sql, "binds": [bind], "rows": true}]).to_string())
            .await
            .expect("send");
        let accepted = next(&mut session).await.expect("accepted");
        assert_eq!(accepted[0], "accepted", "{accepted}");
        let rows = accepted[1]["nodes"][0]["rows"].as_array().expect("rows").len();
        assert_eq!(rows, 2, "n > 1: two rows");
        client
            .batch_execute(&format!("INSERT INTO {table} VALUES (4, 'four', 4)"))
            .await
            .expect("insert");
        let delta = next(&mut session).await.expect("delta");
        assert_eq!(delta[0], "delta", "{delta}");
        assert_eq!(delta[1]["changes"][0]["put"][0]["id"], 4);

        client
            .execute(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
                 WHERE slot_name = $1 AND active_pid IS NOT NULL",
                &[&first],
            )
            .await
            .expect("cut the feed");
        let ended = tokio::time::timeout(Duration::from_secs(10), async {
            while session.recv().await.is_some() {}
        })
        .await;
        assert!(ended.is_ok(), "the session ends with its engine");
        reach(&engine, |state| matches!(state, State::Down(_) | State::Starting), Duration::from_secs(10)).await;
        reach(&engine, |state| *state == State::Serving, Duration::from_secs(30)).await;
        let mut again = engine.connect().expect("a session on the fresh engine");
        assert_eq!(next(&mut again).await.expect("hello")[0], "hello");
        let second = engine.slot().expect("a slot while it runs");
        assert_ne!(second, first, "a fresh slot");
        assert_eq!(slot(&client, &first).await, None, "the cut slot is gone");

        let stopping = engine.clone();
        tokio::task::spawn_blocking(move || stopping.stop())
            .await
            .expect("stop");
        assert_eq!(engine.state(), State::Stopped);
        let deadline = Instant::now() + Duration::from_secs(5);
        while slot(&client, &second).await.is_some() {
            assert!(Instant::now() < deadline, "the slot outlived the engine");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(engine.connect().is_err(), "no session once stopped");
        assert_eq!(engine.slot(), None);
    });
}

/// Stopping an engine whose start a transaction holds back returns at
/// once, not when the transaction ends, and leaves no slot behind.
#[test]
fn a_stop_while_the_start_is_held_back_returns_at_once() {
    let Some(dsn) = dsn("stop") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let names = names("stop");
        let client = admin(&dsn).await;
        set_up(&client, &names).await;
        for kind in [Slot::Temporary, Slot::Permanent { cleanup_age: Duration::from_secs(3600) }] {
            let holder = admin(&dsn).await;
            holder
                .batch_execute("BEGIN; SELECT pg_current_xact_id();")
                .await
                .expect("an open transaction");
            let mut held = config(&dsn, &names);
            held.slot = kind;
            let engine = Engine::start(held);
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(engine.state(), State::Starting, "{kind:?}: held back by the transaction");
            let called = Instant::now();
            let stopping = engine.clone();
            tokio::task::spawn_blocking(move || stopping.stop())
                .await
                .expect("stop");
            assert!(
                called.elapsed() < Duration::from_secs(3),
                "{kind:?}: stop waited {:?} for the transaction",
                called.elapsed()
            );
            assert_eq!(engine.state(), State::Stopped);
            holder.batch_execute("COMMIT").await.expect("commit");
            // The slot whose making was abandoned may still be made once
            // the transaction ends, before its maker notices it is gone.
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let leftover: i64 = client
                    .query_one(
                        "SELECT count(*) FROM pg_replication_slots WHERE database = current_database()",
                        &[],
                    )
                    .await
                    .expect("slots")
                    .get(0);
                if leftover == 0 {
                    break;
                }
                assert!(Instant::now() < deadline, "{kind:?}: {leftover} slot(s) left behind");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    });
}

/// On a permanent slot the engine serves on through a dropped feed
/// connection, its sessions and slot kept; it drops a dead process's
/// slot once inactive past the cleanup age, and its own when it stops.
#[test]
fn a_permanent_slot_keeps_the_engine_serving_through_a_dropped_feed() {
    let Some(dsn) = dsn("permanent") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let names = names("permanent");
        let table = &names.table;
        let client = admin(&dsn).await;
        set_up(&client, &names).await;
        let dead = "xyne_sync_slot_00000000000000000000000000000dead";
        client
            .execute("SELECT pg_create_logical_replication_slot($1, 'pgoutput')", &[&dead])
            .await
            .expect("a dead process's slot");
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut permanent = config(&dsn, &names);
        permanent.slot = Slot::Permanent { cleanup_age: Duration::from_millis(200) };
        let engine = Engine::start(permanent);
        reach(&engine, |state| *state == State::Serving, Duration::from_secs(30)).await;
        assert_eq!(slot(&client, dead).await, None, "the dead process's slot is swept");
        let own = engine.slot().expect("a slot while it runs");
        assert_eq!(slot(&client, &own).await, Some(false), "a permanent slot");

        let mut session = engine.connect().expect("a session");
        assert_eq!(next(&mut session).await.expect("hello")[0], "hello");
        let sql = format!(r#"SELECT "id", "name" FROM "{table}" WHERE "n" > $1 ORDER BY "id" ASC"#);
        let bind = json!({"oid": 23, "bytes": "AAAAAQ=="});
        session
            .sender()
            .send(json!(["subscribe", {"id": 1, "sql": sql, "binds": [bind], "rows": true}]).to_string())
            .await
            .expect("send");
        assert_eq!(next(&mut session).await.expect("accepted")[0], "accepted");

        client
            .execute(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
                 WHERE slot_name = $1 AND active_pid IS NOT NULL",
                &[&own],
            )
            .await
            .expect("cut the feed");
        client
            .batch_execute(&format!("INSERT INTO {table} VALUES (4, 'four', 4)"))
            .await
            .expect("insert");
        let deadline = Instant::now() + Duration::from_secs(15);
        let delta = loop {
            assert!(Instant::now() < deadline, "no delta after the feed reopened");
            if let Some(frame) = next(&mut session).await
                && frame[0] == "delta"
            {
                break frame;
            }
        };
        assert_eq!(delta[1]["changes"][0]["put"][0]["id"], 4, "the change made while it was cut");
        assert_eq!(engine.state(), State::Serving, "served on, no fresh start");
        assert_eq!(engine.slot(), Some(own.clone()), "the same slot");

        let stopping = engine.clone();
        tokio::task::spawn_blocking(move || stopping.stop())
            .await
            .expect("stop");
        assert_eq!(slot(&client, &own).await, None, "its slot goes with it");
    });
}

/// A table with row-level security is refused: the engine reads as its
/// own role and cannot apply a backend's policies. Turned on or off on a
/// served table, the engine starts over on a catalog that knows it.
#[test]
fn a_table_with_row_level_security_is_refused() {
    let Some(dsn) = dsn("rls") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let names = names("rls");
        let table = &names.table;
        let client = admin(&dsn).await;
        set_up(&client, &names).await;
        client
            .batch_execute(
                "CREATE TABLE secured (id int8 PRIMARY KEY, tenant text NOT NULL);
                 ALTER TABLE secured ENABLE ROW LEVEL SECURITY;",
            )
            .await
            .expect("a secured table");
        let engine = Engine::start(config(&dsn, &names));
        reach(&engine, |state| *state == State::Serving, Duration::from_secs(30)).await;
        let mut session = engine.connect().expect("a session");
        assert_eq!(next(&mut session).await.expect("hello")[0], "hello");
        let subscribe = async |session: &mut Session, id: u64, sql: &str| -> Value {
            session
                .sender()
                .send(json!(["subscribe", {"id": id, "sql": sql, "binds": [], "rows": true}]).to_string())
                .await
                .expect("send");
            next(session).await.expect("an answer")
        };

        let refused = subscribe(&mut session, 1, r#"SELECT "id", "tenant" FROM "secured""#).await;
        assert_eq!(refused[0], "refused", "{refused}");
        let reason = refused[1]["reason"].as_str().unwrap_or_default();
        assert!(reason.contains("row-level security"), "{reason}");
        let open = format!(r#"SELECT "id", "name" FROM "{table}""#);
        assert_eq!(subscribe(&mut session, 2, &open).await[0], "accepted");

        let mut id = 3;
        for (change, answer) in [("ENABLE", "refused"), ("DISABLE", "accepted")] {
            let before = engine.slot().expect("a slot while it serves");
            client
                .batch_execute(&format!("ALTER TABLE {table} {change} ROW LEVEL SECURITY"))
                .await
                .expect("row security changed");
            let ended = tokio::time::timeout(Duration::from_secs(10), async {
                while session.recv().await.is_some() {}
            })
            .await;
            assert!(ended.is_ok(), "{change}: the engine starts over, ending its sessions");
            // The old engine may still say it serves while it stops: wait
            // for the fresh one, on a slot of its own.
            let deadline = Instant::now() + Duration::from_secs(30);
            while engine.state() != State::Serving || engine.slot().is_none_or(|slot| slot == before) {
                assert!(Instant::now() < deadline, "{change}: no fresh engine; {:?}", engine.state());
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            session = engine.connect().expect("a session on the fresh engine");
            assert_eq!(next(&mut session).await.expect("hello")[0], "hello");
            let got = subscribe(&mut session, id, &open).await;
            assert_eq!(got[0], answer, "{change}: {got}");
            id += 1;
        }
        let stopping = engine.clone();
        tokio::task::spawn_blocking(move || stopping.stop()).await.expect("stop");
    });
}

/// An engine on a database nobody set up sets it up itself: the
/// publication and the trigger stack, once, however many engines start
/// together; it then serves, and follows a schema change. One told to
/// require the setup refuses a database without it.
#[test]
fn an_engine_sets_up_a_database_nobody_set_up() {
    let Some(dsn) = dsn("setup") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let names = names("setup");
        let table = &names.table;
        let client = admin(&dsn).await;
        client
            .batch_execute(&format!(
                "CREATE TABLE {table} (id int8 PRIMARY KEY, name text NOT NULL, n int4 NOT NULL);
                 INSERT INTO {table} VALUES (1, 'one', 1);"
            ))
            .await
            .expect("the table");

        let mut requiring = config(&dsn, &names);
        requiring.setup = Setup::Require;
        let refused = Engine::start(requiring);
        reach(&refused, |state| matches!(state, State::Down(_)), Duration::from_secs(30)).await;
        let State::Down(reason) = refused.state() else { unreachable!() };
        assert!(reason.contains("event trigger"), "{reason}");
        let stopping = refused.clone();
        tokio::task::spawn_blocking(move || stopping.stop()).await.expect("stop");

        let mut installing = config(&dsn, &names);
        installing.setup = Setup::Install;
        let engines: Vec<Engine> = (0..2).map(|_| Engine::start(installing.clone())).collect();
        for engine in &engines {
            reach(engine, |state| *state == State::Serving, Duration::from_secs(60)).await;
        }
        let published: i64 = client
            .query_one("SELECT count(*) FROM pg_publication WHERE pubname = $1", &[&names.publication])
            .await
            .expect("publications")
            .get(0);
        assert_eq!(published, 1);
        let triggers: i64 = client
            .query_one(
                "SELECT count(*) FROM pg_event_trigger WHERE evtname = $1 AND evtenabled <> 'D'",
                &[&format!("{}_ddl_end_0", names.app)],
            )
            .await
            .expect("triggers")
            .get(0);
        assert_eq!(triggers, 1);

        let mut session = engines[0].connect().expect("a session");
        assert_eq!(next(&mut session).await.expect("hello")[0], "hello");
        client
            .batch_execute(&format!("ALTER TABLE {table} ADD COLUMN note text"))
            .await
            .expect("a schema change");
        let sql = format!(r#"SELECT "id", "note" FROM "{table}" WHERE "n" > $1"#);
        let bind = json!({"oid": 23, "bytes": "AAAAAA=="});
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut id = 1;
        loop {
            session
                .sender()
                .send(json!(["subscribe", {"id": id, "sql": sql, "binds": [bind], "rows": true}]).to_string())
                .await
                .expect("send");
            let answer = next(&mut session).await.expect("an answer");
            if answer[0] == "accepted" {
                break;
            }
            assert!(Instant::now() < deadline, "the new column never served: {answer}");
            id += 1;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        for engine in engines {
            tokio::task::spawn_blocking(move || engine.stop()).await.expect("stop");
        }
        drop(session);
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let held: i64 = client
                .query_one(
                    "SELECT count(*) FROM pg_replication_slots WHERE database = current_database()",
                    &[],
                )
                .await
                .expect("slots")
                .get(0);
            if held == 0 {
                break;
            }
            assert!(Instant::now() < deadline, "the slots outlived their engines");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
}

/// Making a slot waits for every transaction open at the time, so an
/// engine started behind a long one stays `Starting` until it ends; the
/// caller never waits, and the engine serves once it has.
#[test]
fn a_start_behind_an_open_transaction_does_not_block_the_caller() {
    let Some(dsn) = dsn("held") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let names = names("held");
        let client = admin(&dsn).await;
        set_up(&client, &names).await;
        let holder = admin(&dsn).await;
        holder
            .batch_execute("BEGIN; SELECT pg_current_xact_id();")
            .await
            .expect("an open transaction");
        let called = Instant::now();
        let engine = Engine::start(config(&dsn, &names));
        assert!(called.elapsed() < Duration::from_millis(100), "start returns at once");
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(engine.state(), State::Starting, "held back by the transaction");
        assert!(engine.connect().is_err(), "no session before it serves");
        holder.batch_execute("COMMIT").await.expect("commit");
        reach(&engine, |state| *state == State::Serving, Duration::from_secs(30)).await;
        let stopping = engine.clone();
        tokio::task::spawn_blocking(move || stopping.stop())
            .await
            .expect("stop");
    });
}

/// A table a migration creates while the engine runs is ordered as one
/// it loaded at startup: an `ORDER BY` on a column under a byte-order
/// collation is followed, one under a linguistic collation refused,
/// whichever way the engine learned the table.
#[test]
fn a_table_created_while_serving_is_ordered_as_one_loaded_at_start() {
    let Some(dsn) = dsn("order") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let names = names("order");
        let client = admin(&dsn).await;
        set_up(&client, &names).await;
        let linguistic: String = client
            .query_one("SELECT datcollate FROM pg_database WHERE datname = current_database()", &[])
            .await
            .expect("the default collation")
            .get(0);
        let engine = Engine::start(config(&dsn, &names));
        reach(&engine, |state| *state == State::Serving, Duration::from_secs(30)).await;
        let later = format!("{}_later", names.table);
        client
            .batch_execute(&format!(
                r#"CREATE TABLE {later} (id int8 PRIMARY KEY, code text COLLATE "C" NOT NULL, label text NOT NULL);
                   INSERT INTO {later} VALUES (1, 'b', 'b'), (2, 'a', 'a');"#
            ))
            .await
            .expect("a table created while the engine serves");
        let mut session = engine.connect().expect("a session");
        assert_eq!(next(&mut session).await.expect("hello")[0], "hello");
        let answer = |id: u64, column: &str| {
            json!(["subscribe", {
                "id": id,
                "sql": format!(r#"SELECT "id", "{column}" FROM "{later}" ORDER BY "{column}" ASC, "id" ASC"#),
                "binds": [],
                "rows": true
            }])
            .to_string()
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut id = 0;
        let by_code = loop {
            id += 1;
            session.sender().send(answer(id, "code")).await.expect("send");
            let frame = next(&mut session).await.expect("an answer");
            if frame[0] == "accepted" || Instant::now() >= deadline {
                break frame;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert_eq!(by_code[0], "accepted", "a byte-order column is followed: {by_code}");
        let ids: Vec<i64> = by_code[1]["nodes"][0]["rows"]
            .as_array()
            .expect("rows")
            .iter()
            .map(|row| row["id"].as_i64().expect("id"))
            .collect();
        assert_eq!(ids.len(), 2);
        if !matches!(linguistic.as_str(), "C" | "POSIX" | "C.UTF-8" | "C.utf8") {
            session.sender().send(answer(id + 1, "label")).await.expect("send");
            let by_label = next(&mut session).await.expect("an answer");
            assert_eq!(
                by_label[0], "refused",
                "a column under the default {linguistic} collation is not: {by_label}"
            );
        }
        let stopping = engine.clone();
        tokio::task::spawn_blocking(move || stopping.stop())
            .await
            .expect("stop");
    });
}
