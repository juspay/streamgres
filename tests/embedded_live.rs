//! An embedded engine against a live PostgreSQL: it starts, serves a
//! session the S2S frames over channels, starts over on a fresh
//! temporary slot when its feed is cut, and leaves no slot behind when it
//! stops. Needs `STREAMGRES_PG_DSN` with `wal_level = logical`; skips
//! itself otherwise.

use std::time::{Duration, Instant};

use serde_json::{Value, json};
use streamgres::embedded::{Config, Engine, Session, State, setup_sql};
use tokio_postgres::{Client, NoTls};


/// The database, when the test is given one.
fn dsn() -> Option<String> {
    let dsn = std::env::var("STREAMGRES_PG_DSN").ok();
    if dsn.is_none() {
        eprintln!("STREAMGRES_PG_DSN not set: embedded live scenario skipped");
    }
    dsn
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

/// Removes what [`set_up`] made, the trigger stack included: its event
/// triggers fire on every statement that changes a schema in the
/// database, other tests' too, for as long as they are installed.
async fn tear_down(client: &Client, names: &Names) {
    let app = &names.app;
    let _ = client
        .batch_execute(&format!(
            "DROP EVENT TRIGGER IF EXISTS {app}_ddl_start_0;
             DROP EVENT TRIGGER IF EXISTS {app}_ddl_end_0;
             DROP SCHEMA IF EXISTS {app}_0 CASCADE;
             DROP TABLE IF EXISTS {table};
             DROP PUBLICATION IF EXISTS {publication};",
            table = names.table,
            publication = names.publication,
        ))
        .await;
}

/// An engine's configuration for a test's names.
fn config(dsn: &str, names: &Names) -> Config {
    let mut config = Config::new(dsn);
    config.publication = names.publication.clone();
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
    let Some(dsn) = dsn() else { return };
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
        tear_down(&client, &names).await;
    });
}

/// Making a slot waits for every transaction open at the time, so an
/// engine started behind a long one stays `Starting` until it ends; the
/// caller never waits, and the engine serves once it has.
#[test]
fn a_start_behind_an_open_transaction_does_not_block_the_caller() {
    let Some(dsn) = dsn() else { return };
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
        tear_down(&client, &names).await;
    });
}
