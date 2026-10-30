//! An embedded engine against a live PostgreSQL: it starts, serves a
//! session the S2S frames over channels, starts over on a fresh
//! temporary slot when its feed is cut, and leaves no slot behind when it
//! stops. Needs `STREAMGRES_PG_DSN` with `wal_level = logical`; skips
//! itself otherwise. Each test runs in a database of its own
//! ([`support::Database`]), its trigger stack with it.

use std::time::{Duration, Instant};

use serde_json::{Value, json};
use streamgres::embedded::{Config, Engine, Session, Setup, State, setup_sql};
use streamgres::sync::pg::ddl::stack_version;
use streamgres::sync::pg::slots::{OWNER_LOCK, sweep};
use streamgres::sync::pg::ddl_triggers::STACK_VERSION;
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

/// The backend holding the owner lock of `slot`, if one does.
async fn owner_of(client: &Client, slot: &str) -> Option<i32> {
    client
        .query_opt(
            "SELECT pid FROM pg_locks
              WHERE locktype = 'advisory' AND granted AND objsubid = 2
                AND classid::int8 = $1::int8
                AND objid::int8 = (hashtext($2)::int8 + 4294967296) % 4294967296",
            &[&i64::from(OWNER_LOCK), &slot],
        )
        .await
        .expect("the owner lock")
        .map(|row| row.get(0))
}

/// The engine's whole life: start on a permanent slot it owns, serve,
/// start over on a fresh slot when its hold on the slot is lost, stop
/// with no slot left behind.
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
        assert_eq!(slot(&client, &first).await, Some(false), "a permanent slot");
        let owner = owner_of(&client, &first).await.expect("the engine owns its slot");

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

        // A sweep may drop a slot whose owner lock is gone, so an engine
        // that loses its hold starts over rather than stream on.
        client
            .execute("SELECT pg_terminate_backend($1)", &[&owner])
            .await
            .expect("cut the owner lock's connection");
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
        assert!(owner_of(&client, &second).await.is_some(), "owned too");
        assert_eq!(slot(&client, &first).await, None, "the old slot is dropped");

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
        let holder = admin(&dsn).await;
        holder
            .batch_execute("BEGIN; SELECT pg_current_xact_id();")
            .await
            .expect("an open transaction");
        let engine = Engine::start(config(&dsn, &names));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(engine.state(), State::Starting, "held back by the transaction");
        let called = Instant::now();
        let stopping = engine.clone();
        tokio::task::spawn_blocking(move || stopping.stop())
            .await
            .expect("stop");
        assert!(
            called.elapsed() < Duration::from_secs(3),
            "stop waited {:?} for the transaction",
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
            assert!(Instant::now() < deadline, "{leftover} slot(s) left behind");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
}

/// On its permanent slot the engine serves on through a dropped feed
/// connection, its sessions and slot kept; at its start it drops the slot
/// a dead process left, whose owner lock nobody holds, on any version;
/// and its own when it stops.
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

        let engine = Engine::start(config(&dsn, &names));
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

/// A sweep drops a slot of the prefix only once nobody holds its owner
/// lock: one whose owner lives is kept however long it sits idle, one
/// whose owner is gone is dropped, and so is the first once its owner's
/// connection ends. A slot of another name is never the sweep's.
#[test]
fn a_slot_is_swept_only_once_its_owner_is_gone() {
    let Some(dsn) = dsn("sweep") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let client = admin(&dsn).await;
        let (owned, dead, other) = (
            "xyne_sync_slot_000000000000000000000000000000owned",
            "xyne_sync_slot_0000000000000000000000000000000dead",
            "someone_elses_slot",
        );
        let owner = admin(&dsn).await;
        owner
            .execute("SELECT pg_advisory_lock($1, hashtext($2))", &[&OWNER_LOCK, &owned])
            .await
            .expect("the owner's lock");
        for name in [owned, dead, other] {
            client
                .execute("SELECT pg_create_logical_replication_slot($1, 'pgoutput')", &[&name])
                .await
                .expect("a slot");
        }

        assert_eq!(sweep(&dsn, Duration::ZERO).await.expect("a sweep"), 1);
        assert_eq!(slot(&client, owned).await, Some(false), "its owner lives");
        assert_eq!(slot(&client, dead).await, None, "its owner is gone");
        assert_eq!(slot(&client, other).await, Some(false), "not the sweep's");

        drop(owner);
        let deadline = Instant::now() + Duration::from_secs(5);
        while sweep(&dsn, Duration::ZERO).await.expect("a sweep") == 0 {
            assert!(Instant::now() < deadline, "the owner's lock outlived its connection");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(slot(&client, owned).await, None, "dropped once its owner went");
        client
            .execute("SELECT pg_drop_replication_slot($1)", &[&other])
            .await
            .expect("drop the other slot");
    });
}

/// A table with row-level security is refused: the engine reads as its
/// own role and cannot apply a backend's policies. Turned on for a served
/// table, the engine starts over on a catalog that knows it; turned off,
/// or turned on in the transaction that creates a table, the engine
/// follows it without starting over.
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

        let before = engine.slot().expect("a slot while it serves");
        client
            .batch_execute(&format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"))
            .await
            .expect("secure the served table");
        let ended = tokio::time::timeout(Duration::from_secs(10), async {
            while session.recv().await.is_some() {}
        })
        .await;
        assert!(ended.is_ok(), "the engine starts over, ending its sessions");
        // The old engine may still say it serves while it stops: wait for
        // the fresh one, on a slot of its own.
        let deadline = Instant::now() + Duration::from_secs(30);
        while engine.state() != State::Serving || engine.slot().is_none_or(|slot| slot == before) {
            assert!(Instant::now() < deadline, "no fresh engine; {:?}", engine.state());
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let mut session = engine.connect().expect("a session on the fresh engine");
        assert_eq!(next(&mut session).await.expect("hello")[0], "hello");
        assert_eq!(subscribe(&mut session, 3, &open).await[0], "refused", "known once started over");

        let serving = engine.slot();
        client
            .batch_execute(&format!(
                "ALTER TABLE {table} DISABLE ROW LEVEL SECURITY;
                 BEGIN;
                 CREATE TABLE secured_later (id int8 PRIMARY KEY, tenant text NOT NULL);
                 ALTER TABLE secured_later ENABLE ROW LEVEL SECURITY;
                 COMMIT;"
            ))
            .await
            .expect("open one table, create and secure another");
        let later = r#"SELECT "id", "tenant" FROM "secured_later""#;
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut id = 4;
        let reason = loop {
            let opened = subscribe(&mut session, id, &open).await;
            let created = subscribe(&mut session, id + 1, later).await;
            id += 2;
            if opened[0] == "accepted" && created[0] == "refused" {
                let reason = created[1]["reason"].as_str().unwrap_or_default().to_owned();
                if reason.contains("row-level security") {
                    break reason;
                }
            }
            assert!(Instant::now() < deadline, "not followed: {opened} {created}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert!(reason.contains("secured_later"), "{reason}");
        assert_eq!(engine.slot(), serving, "followed without starting over");
        assert_eq!(engine.state(), State::Serving);
        let stopping = engine.clone();
        tokio::task::spawn_blocking(move || stopping.stop()).await.expect("stop");
    });
}

/// A trigger stack an older setup of this crate installed, and marked as
/// its own, is brought up to date by an engine that installs its setup:
/// here one from before the engine read `rowSecurity`.
#[test]
fn an_outdated_trigger_stack_is_updated() {
    let Some(dsn) = dsn("stack") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let names = names("stack");
        let client = admin(&dsn).await;
        set_up(&client, &names).await;
        let old = older_stack(&names).replace(
            &format!("'version', {STACK_VERSION},"),
            &format!("'version', {},", STACK_VERSION - 1),
        );
        client.batch_execute(&old).await.expect("the older stack");
        assert!(!reads_row_security(&client, &names).await);
        assert_eq!(marked(&client, &names).await, Some(STACK_VERSION - 1));

        let mut installing = config(&dsn, &names);
        installing.setup = Setup::Install;
        let engine = Engine::start(installing);
        reach(&engine, |state| *state == State::Serving, Duration::from_secs(30)).await;
        assert!(reads_row_security(&client, &names).await, "updated before serving");
        assert_eq!(marked(&client, &names).await, Some(STACK_VERSION));
        let stopping = engine.clone();
        tokio::task::spawn_blocking(move || stopping.stop()).await.expect("stop");
    });
}

/// A trigger stack without the ownership marker, zero-cache's or one an
/// unmarked older setup installed, is not this crate's to replace: an
/// engine that installs its setup serves on it as it is.
#[test]
fn an_unmarked_trigger_stack_is_left_alone() {
    let Some(dsn) = dsn("unmarked") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let names = names("unmarked");
        let client = admin(&dsn).await;
        set_up(&client, &names).await;
        client
            .batch_execute(&older_stack(&names))
            .await
            .expect("the older stack");
        client
            .batch_execute(&format!("DROP FUNCTION \"{}_0\".streamgres_stack()", names.app))
            .await
            .expect("unmarked");
        assert_eq!(marked(&client, &names).await, None);
        assert_eq!(
            stack_version(&client, "no_such_trigger").await.expect("a missing trigger"),
            None,
            "no trigger, no stack"
        );

        let mut installing = config(&dsn, &names);
        installing.setup = Setup::Install;
        let engine = Engine::start(installing);
        reach(&engine, |state| *state == State::Serving, Duration::from_secs(30)).await;
        assert!(!reads_row_security(&client, &names).await, "left as it was");
        assert_eq!(marked(&client, &names).await, None);
        let stopping = engine.clone();
        tokio::task::spawn_blocking(move || stopping.stop()).await.expect("stop");
    });
}

/// An engine told to adopt the stack at its names replaces one that is
/// not marked as this crate's, and marks it.
#[test]
fn an_engine_told_to_adopt_replaces_an_unmarked_stack() {
    let Some(dsn) = dsn("adopt") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let names = names("adopt");
        let client = admin(&dsn).await;
        set_up(&client, &names).await;
        client
            .batch_execute(&older_stack(&names))
            .await
            .expect("the older stack");
        client
            .batch_execute(&format!("DROP FUNCTION \"{}_0\".streamgres_stack()", names.app))
            .await
            .expect("unmarked");

        let mut adopting = config(&dsn, &names);
        adopting.setup = Setup::Adopt;
        let engine = Engine::start(adopting);
        reach(&engine, |state| *state == State::Serving, Duration::from_secs(30)).await;
        assert!(reads_row_security(&client, &names).await, "replaced before serving");
        assert_eq!(marked(&client, &names).await, Some(STACK_VERSION));
        let stopping = engine.clone();
        tokio::task::spawn_blocking(move || stopping.stop()).await.expect("stop");
    });
}

/// A stack this crate installed is zero-cache's again once zero-cache
/// installs its own functions over it: the marker left behind no longer
/// matches them, and an engine leaves the stack alone.
#[test]
fn a_stack_zero_cache_installed_over_is_not_this_crates() {
    let Some(dsn) = dsn("retaken") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let names = names("retaken");
        let client = admin(&dsn).await;
        set_up(&client, &names).await;
        assert_eq!(marked(&client, &names).await, Some(STACK_VERSION));
        let schema = format!("{}_0", names.app);
        let zero_cache_start = format!(
            "CREATE OR REPLACE FUNCTION \"{schema}\".emit_ddl_start()
             RETURNS event_trigger AS $$
             BEGIN
               PERFORM pg_advisory_xact_lock(4353719051050729648);
               PERFORM \"{schema}\".update_schemas('ddlStart', TG_TAG, NULL);
             END
             $$ LANGUAGE plpgsql;"
        );
        client.batch_execute(&zero_cache_start).await.expect("zero-cache's start trigger");
        assert_eq!(marked(&client, &names).await, None, "the marker no longer matches");

        let mut installing = config(&dsn, &names);
        installing.setup = Setup::Install;
        let engine = Engine::start(installing);
        reach(&engine, |state| *state == State::Serving, Duration::from_secs(30)).await;
        let start: String = client
            .query_one(
                "SELECT prosrc FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                 WHERE n.nspname = $1 AND p.proname = 'emit_ddl_start'",
                &[&schema],
            )
            .await
            .expect("the start function")
            .get(0);
        assert!(start.contains("ddlStart"), "left as zero-cache installed it: {start}");
        let stopping = engine.clone();
        tokio::task::spawn_blocking(move || stopping.stop()).await.expect("stop");
    });
}

/// An engine sets up no stack under zero-cache's default app id: one for
/// app `zero` on a database nobody set up stays down, and says why.
#[test]
fn an_engine_sets_nothing_up_under_zero_caches_app() {
    let Some(dsn) = dsn("zeroapp") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let names = names("zeroapp");
        let mut zero = config(&dsn, &names);
        zero.app = "zero".to_owned();
        zero.ddl_trigger = "zero_ddl_end_0".to_owned();
        zero.ddl_prefix = "zero/0/ddl".to_owned();
        zero.setup = Setup::Install;
        let engine = Engine::start(zero);
        reach(
            &engine,
            |state| matches!(state, State::Down(reason) if reason.contains("zero-cache's default app id")),
            Duration::from_secs(30),
        )
        .await;
        let stopping = engine.clone();
        tokio::task::spawn_blocking(move || stopping.stop()).await.expect("stop");
        let client = admin(&dsn).await;
        let triggers: i64 = client
            .query_one("SELECT count(*) FROM pg_event_trigger", &[])
            .await
            .expect("the event triggers")
            .get(0);
        assert_eq!(triggers, 0, "nothing installed");
    });
}

/// The stack as it was before the engine read `rowSecurity`.
fn older_stack(names: &Names) -> String {
    let old = setup_sql(&names.app, 0, &names.publication)
        .replace("  pc.relrowsecurity AS \"rowSecurity\",\n", "")
        .replace("  'rowSecurity', \"rowSecurity\",\n", "")
        .replace(", \"rowSecurity\")", ")");
    assert!(!old.contains("rowSecurity"), "an older stack");
    old
}

/// Whether the test's stack describes tables with `rowSecurity`.
async fn reads_row_security(client: &Client, names: &Names) -> bool {
    client
        .query_one(
            "SELECT bool_or(prosrc LIKE '%\"rowSecurity\"%') FROM pg_proc p
             JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1",
            &[&format!("{}_0", names.app)],
        )
        .await
        .expect("the stack's functions")
        .get::<_, Option<bool>>(0)
        .unwrap_or(false)
}

/// The setup version the test's stack is marked with, if any.
async fn marked(client: &Client, names: &Names) -> Option<i32> {
    stack_version(client, &format!("{}_ddl_end_0", names.app))
        .await
        .expect("the stack's marker")
}

/// What the stack writes into the WAL, read through a `test_decoding`
/// slot: nothing for a statement that changes nothing (`IF NOT EXISTS`,
/// a rolled back change) and never a `ddlStart`; one `ddlUpdate` per
/// change otherwise, each starting from the schema the one before it
/// left, through several statements in a transaction, statements a `DO`
/// block runs, and two migrations at once (the second waits for the
/// first's lock). Setting the stack up twice changes nothing.
#[test]
fn the_stack_writes_one_message_per_change_and_none_for_a_noop() {
    let Some(dsn) = dsn("messages") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let names = names("messages");
        let (table, more) = (names.table.clone(), format!("{}_more", names.table));
        let client = admin(&dsn).await;
        set_up(&client, &names).await;
        client
            .batch_execute(&setup_sql(&names.app, 0, &names.publication))
            .await
            .expect("set up again");
        client
            .batch_execute(&format!(
                "CREATE TABLE {more} (id int8 PRIMARY KEY);
                 CREATE INDEX {table}_name ON {table} (name);"
            ))
            .await
            .expect("the second table and an index");
        client
            .batch_execute("SELECT pg_create_logical_replication_slot('embedded_messages', 'test_decoding')")
            .await
            .expect("a slot");

        client
            .batch_execute(&format!(
                "CREATE TABLE IF NOT EXISTS {table} (id int8 PRIMARY KEY);
                 CREATE TABLE IF NOT EXISTS {table} (id int8 PRIMARY KEY);
                 CREATE INDEX IF NOT EXISTS {table}_name ON {table} (name);
                 CREATE INDEX IF NOT EXISTS {table}_name ON {table} (name);
                 BEGIN; ALTER TABLE {table} ADD COLUMN gone int4; ROLLBACK;
                 BEGIN;
                 ALTER TABLE {table} ADD COLUMN a int4;
                 ALTER TABLE {table} ADD COLUMN b int4;
                 COMMIT;
                 DO $$ BEGIN
                   EXECUTE 'ALTER TABLE {table} ADD COLUMN c int4';
                   EXECUTE 'ALTER TABLE {table} ADD COLUMN d int4';
                 END $$;
                 BEGIN; ALTER TABLE {table} ADD COLUMN e int4;"
            ))
            .await
            .expect("the migrations");
        let other = admin(&dsn).await;
        let second = format!("ALTER TABLE {more} ADD COLUMN f int4");
        let waiting = tokio::spawn(async move { other.batch_execute(&second).await });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!waiting.is_finished(), "the second migration waits for the first's lock");
        client.batch_execute("COMMIT").await.expect("commit the first");
        waiting.await.expect("join").expect("the second migration");
        client
            .batch_execute(&format!(
                "CREATE SCHEMA dropped;
                 CREATE TABLE dropped.items (id int8 PRIMARY KEY);
                 DROP SCHEMA dropped CASCADE;
                 DROP TABLE {more};
                 COMMENT ON PUBLICATION {} IS 'followed';",
                names.publication
            ))
            .await
            .expect("drops and a comment");

        let messages: Vec<Value> = client
            .query(
                "SELECT data FROM pg_logical_slot_get_changes('embedded_messages', NULL, NULL)
                 WHERE data LIKE 'message:%'",
                &[],
            )
            .await
            .expect("the slot's changes")
            .iter()
            .map(|row| {
                let data: String = row.get(0);
                assert!(data.contains(&format!("prefix: {}/0/ddl,", names.app)), "{data}");
                let (_, content) = data.split_once("content:").expect("a message's content");
                serde_json::from_str(content).expect("a message's JSON")
            })
            .collect();
        let kinds: Vec<&str> = messages.iter().filter_map(|m| m["type"].as_str()).collect();
        let mut expected = vec!["ddlUpdate"; 9];
        expected.push("schemaSnapshot");
        assert_eq!(kinds, expected, "one update per change, no start, a snapshot for the comment");
        let tables = |schema: &Value| -> Vec<String> {
            let mut tables: Vec<String> = schema["tables"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|t| format!("{}.{}", t["schema"].as_str().unwrap_or(""), t["name"].as_str().unwrap_or("")))
                .collect();
            tables.sort();
            tables
        };
        let moved: Vec<(Vec<String>, Vec<String>)> = messages[6..9]
            .iter()
            .map(|message| {
                let (before, after) = (tables(&message["previousSchema"]), tables(&message["schema"]));
                (
                    after.iter().filter(|t| !before.contains(t)).cloned().collect(),
                    before.iter().filter(|t| !after.contains(t)).cloned().collect(),
                )
            })
            .collect();
        let none = Vec::<String>::new();
        assert_eq!(
            moved,
            [
                (vec!["dropped.items".to_owned()], none.clone()),
                (none.clone(), vec!["dropped.items".to_owned()]),
                (none.clone(), vec![format!("public.{more}")]),
            ],
            "a table created, then reported gone by DROP SCHEMA itself, then a DROP TABLE"
        );

        let columns = |schema: &Value, name: &str| -> Vec<String> {
            let mut columns: Vec<String> = schema["tables"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|t| t["name"] == name)
                .flat_map(|t| t["columns"].as_object().into_iter().flatten())
                .map(|(column, _)| column.clone())
                .collect();
            columns.sort();
            columns
        };
        let added: Vec<Vec<String>> = messages[..6]
            .iter()
            .map(|message| {
                let (before, after) = (&message["previousSchema"], &message["schema"]);
                [&table, &more]
                    .into_iter()
                    .flat_map(|name| {
                        let before = columns(before, name);
                        columns(after, name).into_iter().filter(move |c| !before.contains(c))
                    })
                    .collect()
            })
            .collect();
        assert_eq!(added, [["a"], ["b"], ["c"], ["d"], ["e"], ["f"]], "in order, one each");
        for pair in messages.windows(2) {
            assert_eq!(pair[1]["previousSchema"], pair[0]["schema"], "each starts where the last ended");
        }
        assert!(
            messages.iter().all(|m| !m.to_string().contains("\"gone\"")),
            "a rolled back change writes nothing"
        );
    });
}

/// The tables a publication of `name` holds, as `schema.table`, sorted.
async fn publication_tables(client: &Client, name: &str) -> Vec<String> {
    client
        .query(
            "SELECT schemaname || '.' || tablename FROM pg_publication_tables
             WHERE pubname = $1 ORDER BY 1",
            &[&name],
        )
        .await
        .expect("the publication's tables")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

/// The engine's answer to subscribing `sql` with no binds.
async fn answer(session: &mut Session, id: u64, sql: &str) -> Value {
    session
        .sender()
        .send(json!(["subscribe", {"id": id, "sql": sql, "binds": [], "rows": true}]).to_string())
        .await
        .expect("send");
    next(session).await.expect("an answer")
}

/// An engine told which tables to publish (`publication_tables`) creates
/// its publication of those alone, a bare name standing for that table
/// in every served schema, and follows only them: a query on another
/// table of a served schema is refused as unknown, not served from a
/// snapshot that would never change. A listed table created later, a new
/// workspace's say, is added on the next start, and none is taken out.
#[test]
fn a_publication_of_listed_tables_follows_only_them() {
    let Some(dsn) = dsn("listed") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let names = names("listed");
        let client = admin(&dsn).await;
        client
            .batch_execute(
                "CREATE SCHEMA ws_a; CREATE SCHEMA ws_b;
                 CREATE TABLE ws_a.followed (id int8 PRIMARY KEY);
                 CREATE TABLE ws_a.ignored (id int8 PRIMARY KEY);
                 CREATE TABLE ws_b.ignored (id int8 PRIMARY KEY);",
            )
            .await
            .expect("the workspaces");
        let mut listing = config(&dsn, &names);
        listing.setup = Setup::Install;
        listing.schemas = vec!["ws_a".to_owned(), "ws_b".to_owned()];
        listing.publication_tables = vec!["followed".to_owned()];

        let engine = Engine::start(listing.clone());
        reach(&engine, |state| *state == State::Serving, Duration::from_secs(30)).await;
        assert_eq!(publication_tables(&client, &names.publication).await, ["ws_a.followed"]);
        let recorded: i32 = client
            .query_one(
                &format!(
                    "SELECT json_array_length(current->'tables') FROM \"{}_0\".\"publishedSchema\"",
                    names.app
                ),
                &[],
            )
            .await
            .expect("the recorded schema")
            .get(0);
        assert_eq!(recorded, 1, "the stack records the publication's one table, not every table");
        let mut session = engine.connect().expect("a session");
        assert_eq!(next(&mut session).await.expect("hello")[0], "hello");
        let followed = answer(&mut session, 1, r#"SELECT "id" FROM "ws_a"."followed""#).await;
        assert_eq!(followed[0], "accepted", "{followed}");
        let ignored = answer(&mut session, 2, r#"SELECT "id" FROM "ws_a"."ignored""#).await;
        assert_eq!(ignored[0], "refused", "{ignored}");
        assert!(ignored.to_string().contains("unknown table"), "{ignored}");
        drop(session);
        let stopping = engine.clone();
        tokio::task::spawn_blocking(move || stopping.stop()).await.expect("stop");

        client
            .batch_execute("CREATE TABLE ws_b.followed (id int8 PRIMARY KEY)")
            .await
            .expect("a new workspace's table");
        let listing_again = listing.clone();
        let engine = Engine::start(listing);
        reach(&engine, |state| *state == State::Serving, Duration::from_secs(30)).await;
        assert_eq!(
            publication_tables(&client, &names.publication).await,
            ["ws_a.followed", "ws_b.followed"],
            "added on the next start, none taken out"
        );
        tokio::task::spawn_blocking(move || engine.stop()).await.expect("stop");

        // A migration drops the publication, as narrowing one of every
        // table does, while the stack stays: the engine creates it again
        // and has the stack record its tables, not what it held before.
        client
            .batch_execute(&format!(
                "DROP PUBLICATION {pub};
                 CREATE PUBLICATION {pub} FOR ALL TABLES;
                 SELECT \"{app}_0\".update_schemas();
                 DROP PUBLICATION {pub};",
                pub = names.publication,
                app = names.app
            ))
            .await
            .expect("the migration");
        let recorded = async || -> i32 {
            client
                .query_one(
                    &format!(
                        "SELECT json_array_length(current->'tables') FROM \"{}_0\".\"publishedSchema\"",
                        names.app
                    ),
                    &[],
                )
                .await
                .expect("the recorded schema")
                .get(0)
        };
        assert!(recorded().await > 2, "the record still has every table");
        let engine = Engine::start(listing_again);
        reach(&engine, |state| *state == State::Serving, Duration::from_secs(30)).await;
        assert_eq!(
            publication_tables(&client, &names.publication).await,
            ["ws_a.followed", "ws_b.followed"]
        );
        assert_eq!(recorded().await, 2, "the record taken afresh");
        tokio::task::spawn_blocking(move || engine.stop()).await.expect("stop");
    });
}

/// A publication of every table is not narrowed by listing tables: the
/// engine serves on it as it is, since narrowing it is a migration's to
/// do, and says so.
#[test]
fn a_publication_of_every_table_is_left_as_it_is() {
    let Some(dsn) = dsn("alltables") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let names = names("alltables");
        let client = admin(&dsn).await;
        set_up(&client, &names).await;
        let mut listing = config(&dsn, &names);
        listing.setup = Setup::Install;
        listing.publication_tables = vec![names.table.clone()];
        let engine = Engine::start(listing);
        reach(&engine, |state| *state == State::Serving, Duration::from_secs(30)).await;
        let all: bool = client
            .query_one("SELECT puballtables FROM pg_publication WHERE pubname = $1", &[&names.publication])
            .await
            .expect("the publication")
            .get(0);
        assert!(all, "still every table");
        tokio::task::spawn_blocking(move || engine.stop()).await.expect("stop");
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
