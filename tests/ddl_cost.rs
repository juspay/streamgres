//! What the schema-change trigger stack costs a database's DDL, on a
//! database of many published tables, the shape that made a backend's
//! startup bootstrap take minutes: every `CREATE TABLE IF NOT EXISTS` and
//! `CREATE INDEX IF NOT EXISTS` fired a start trigger that built the whole
//! published schema and wrote it into the WAL. Needs `STREAMGRES_PG_DSN`
//! with `wal_level = logical` and the `test_decoding` plugin; skips
//! itself otherwise ([`support::skip`]).
//!
//! The checks are exact, not timings: a statement that writes nothing of
//! its own leaves its transaction without an id
//! (`pg_current_xact_id_if_assigned`), since a transactional logical
//! message or a catalog row would assign one; a `test_decoding` slot shows
//! every message the stack wrote. The WAL a no-op run adds is bounded too,
//! loosely, as the whole cluster writes into it meanwhile. Timings are
//! printed, for reading, never asserted.

use std::time::Instant;

use serde_json::Value;
use streamgres::embedded::setup_sql;
use tokio_postgres::{Client, NoTls};

mod support;

use support::Database;

/// The app the test's stack is installed for, and its messages' prefix.
const APP: &str = "ddlcost";
/// Workspace-like schemas, and tables in each, published.
const SCHEMAS: usize = 30;
const TABLES: usize = 6;

/// A connection for the test's statements.
async fn admin(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// `SCHEMAS` schemas of `TABLES` tables each, every table with an index,
/// published by one publication `FOR ALL TABLES`, with the stack on top.
async fn set_up(client: &Client) {
    let mut ddl = String::new();
    for schema in 0..SCHEMAS {
        ddl.push_str(&format!("CREATE SCHEMA ws_{schema};\n"));
        for table in 0..TABLES {
            ddl.push_str(&format!(
                "CREATE TABLE ws_{schema}.t_{table} (id int8 PRIMARY KEY, key text NOT NULL, value jsonb, at timestamptz DEFAULT now());
                 CREATE INDEX t_{table}_key ON ws_{schema}.t_{table} (key);\n"
            ));
        }
    }
    client.batch_execute(&ddl).await.expect("the schemas");
    client
        .batch_execute(&format!(
            "CREATE PUBLICATION ddlcost_pub FOR ALL TABLES;\n{}",
            setup_sql(APP, 0, "ddlcost_pub")
        ))
        .await
        .expect("the publication and the stack");
    client
        .batch_execute("SELECT pg_create_logical_replication_slot('ddlcost', 'test_decoding')")
        .await
        .expect("a test_decoding slot");
}

/// The bootstrap a backend runs at every start: each table and index of
/// every schema created again, if it does not exist.
fn bootstrap() -> String {
    let mut ddl = String::new();
    for schema in 0..SCHEMAS {
        for table in 0..TABLES {
            ddl.push_str(&format!(
                "CREATE TABLE IF NOT EXISTS ws_{schema}.t_{table} (id int8 PRIMARY KEY);
                 CREATE INDEX IF NOT EXISTS t_{table}_key ON ws_{schema}.t_{table} (key);\n"
            ));
        }
    }
    ddl
}

/// The stack's messages the slot holds since it was last read, decoded.
async fn messages(client: &Client) -> Vec<Value> {
    client
        .query(
            "SELECT data FROM pg_logical_slot_get_changes('ddlcost', NULL, NULL)
             WHERE data LIKE 'message:%'",
            &[],
        )
        .await
        .expect("the slot's changes")
        .iter()
        .filter_map(|row| {
            let data: String = row.get(0);
            if !data.contains(&format!("prefix: {APP}/0/ddl,")) {
                return None;
            }
            let (_, content) = data.split_once("content:")?;
            Some(serde_json::from_str(content).expect("a message's JSON"))
        })
        .collect()
}

/// Runs `ddl` in one transaction; whether the transaction was given an id
/// (it wrote something of its own), and the WAL the cluster wrote
/// meanwhile, in bytes.
async fn run(client: &Client, ddl: &str) -> (bool, i64) {
    let before: String = client
        .query_one("SELECT pg_current_wal_insert_lsn()::text", &[])
        .await
        .expect("the WAL position")
        .get(0);
    let started = Instant::now();
    client
        .batch_execute(&format!("BEGIN;\n{ddl}"))
        .await
        .expect("the statements");
    // Read in the still open transaction: reading writes nothing, so it
    // assigns no id itself.
    let assigned: bool = client
        .query_one("SELECT pg_current_xact_id_if_assigned() IS NOT NULL", &[])
        .await
        .expect("the transaction's id")
        .get(0);
    client.batch_execute("ROLLBACK").await.expect("rollback");
    let written: i64 = client
        .query_one("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::text::pg_lsn)::int8", &[&before])
        .await
        .expect("the WAL written")
        .get(0);
    eprintln!("{} statements: {:?}, {written} bytes of WAL cluster-wide", ddl.lines().count(), started.elapsed());
    (assigned, written)
}

/// A backend's bootstrap of `IF NOT EXISTS` statements over 30 schemas
/// writes nothing: its transaction gets no id, the slot sees no message,
/// and the WAL written meanwhile stays far below one schema-sized message
/// per statement. A real change writes exactly one `ddlUpdate`, which
/// carries the schema before and after (what an incremental message
/// would cut). And as a control, zero-cache's start trigger, which builds
/// and writes the schema on every statement, fails each of the checks.
#[test]
fn no_op_ddl_writes_nothing_and_a_change_writes_one_message() {
    let Some(dsn) = Database::create("ddlcost") else { return };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let client = admin(&dsn).await;
        set_up(&client).await;
        let _ = messages(&client).await;
        let schema_bytes: i64 = client
            .query_one(
                &format!("SELECT length(current::text)::int8 FROM \"{APP}_0\".\"publishedSchema\""),
                &[],
            )
            .await
            .expect("the published schema")
            .get(0);
        let statements = i64::try_from(SCHEMAS * TABLES * 2).expect("a small count");
        assert!(schema_bytes > 50_000, "a schema worth the cost: {schema_bytes} bytes");

        // The stack as installed: nothing written.
        let (assigned, written) = run(&client, &bootstrap()).await;
        assert!(!assigned, "the bootstrap's transaction wrote something of its own");
        assert_eq!(messages(&client).await, Vec::<Value>::new(), "no message for a no-op");
        assert!(
            written < schema_bytes,
            "{written} bytes of WAL for {statements} no-op statements, more than one schema ({schema_bytes} bytes)"
        );

        // A real change: one message, carrying the schema twice.
        client
            .batch_execute("ALTER TABLE ws_0.t_0 ADD COLUMN extra int4")
            .await
            .expect("a real change");
        let changed = messages(&client).await;
        let kinds: Vec<&str> = changed.iter().filter_map(|m| m["type"].as_str()).collect();
        assert_eq!(kinds, ["ddlUpdate"], "one message for one change");
        let size = i64::try_from(changed[0].to_string().len()).expect("a message's size");
        assert!(
            size > schema_bytes && size < 3 * schema_bytes,
            "a {size}-byte message for a {schema_bytes}-byte schema: the schema before and after, today"
        );

        // The control: zero-cache's start trigger.
        client
            .batch_execute(&format!(
                "CREATE OR REPLACE FUNCTION \"{APP}_0\".emit_ddl_start()
                 RETURNS event_trigger AS $$
                 BEGIN
                   PERFORM pg_advisory_xact_lock(4353719051050729648);
                   PERFORM \"{APP}_0\".update_schemas('ddlStart', TG_TAG, NULL);
                 END
                 $$ LANGUAGE plpgsql;
                 CREATE OR REPLACE FUNCTION \"{APP}_0\".update_schemas(event_type text, tag text, target record)
                 RETURNS void AS $$
                 DECLARE
                   prev_schema_specs JSON;
                   schema_specs JSON;
                   message TEXT;
                 BEGIN
                   SELECT current FROM \"{APP}_0\".\"publishedSchema\" INTO prev_schema_specs;
                   SELECT \"{APP}_0\".schema_specs() INTO schema_specs;
                   IF prev_schema_specs::text != schema_specs::text THEN
                     UPDATE \"{APP}_0\".\"publishedSchema\" SET current = schema_specs;
                   ELSIF event_type = 'ddlStart' THEN
                     prev_schema_specs = NULL;
                   ELSIF event_type = 'ddlUpdate' THEN
                     RETURN;
                   END IF;
                   SELECT json_build_object('type', event_type, 'version', 1,
                     'previousSchema', prev_schema_specs, 'schema', schema_specs,
                     'event', json_build_object('tag', tag)) INTO message;
                   PERFORM pg_logical_emit_message(true, '{APP}/0/ddl', message);
                 END
                 $$ LANGUAGE plpgsql;"
            ))
            .await
            .expect("zero-cache's start trigger");
        let few: String = bootstrap().lines().take(10).collect::<Vec<_>>().join("\n");
        let (assigned, written) = run(&client, &few).await;
        assert!(assigned, "the control writes a message, which assigns an id");
        assert!(
            written > schema_bytes,
            "the control writes the schema for each statement: {written} bytes"
        );
        let _ = messages(&client).await;
    });
}
