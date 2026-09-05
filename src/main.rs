//! Demo scenario: the full pipeline, SQL text → parser → IVM routing.
//!
//! Registers a handful of subscriptions on a `tickets` table, then plays an
//! insert / insert / update / delete sequence through
//! [`IVM::incremental_update`]. For every write it prints which
//! subscriptions the engine found, the operations it emitted, and what the
//! routing actually cost (condition evaluations, disjunct bumps and
//! firings, membership probes) — the counters to watch while iterating on
//! the routing strategy.
//!
//! One subscription (`q-open-dup`) deliberately duplicates another's
//! condition to show that a single indexed condition fans out to all of its
//! subscribers.
//!
//! Run with `cargo run`. The assertable version of this scenario lives in
//! `tests/ivm_scenarios.rs`; the parser's own tests live in
//! `src/parser/mod.rs`.

use jus_sync::ivm::{IvmStats, IVM};
use jus_sync::model::*;
use jus_sync::parser::{parse_read, parse_write, point_at, Catalog};

/// Runs the demo end to end: registers the six subscriptions against the
/// `tickets` catalog, plays the insert / insert / update / delete sequence
/// through [`run_write`], then prints each subscription's final frame size
/// and the engine's cumulative counters.
fn main() {
    println!("== jus_sync IVM demo ==================================================");

    let catalog = Catalog::new(vec![tickets_table()]);
    let mut ivm = IVM::new();

    let subscriptions = [
        ("q-open", "SELECT * FROM tickets WHERE status = 'OPEN'"),
        (
            "q-mine-active",
            "SELECT * FROM tickets WHERE assigned_to = 'aniket' AND status != 'DONE'",
        ),
        (
            "q-hot",
            "SELECT * FROM tickets WHERE priority IN ('HIGH', 'URGENT')",
        ),
        ("q-big", "SELECT * FROM tickets WHERE points >= 8"),
        ("q-all", "SELECT * FROM tickets"),
        ("q-open-dup", "SELECT * FROM tickets WHERE status = 'OPEN'"),
    ];

    println!("\nregistering {} subscriptions:", subscriptions.len());
    for (uuid, sql) in subscriptions {
        println!("  {uuid:<14} {sql}");
        let query = parse_read(sql, &catalog)
            .unwrap_or_else(|error| panic!("{}", point_at(sql, &error)));
        ivm.register_query(uuid.to_owned(), query);
    }
    println!(
        "  -> {} disjuncts, {} condition links indexed (q-open and q-open-dup both subscribe to status = 'OPEN')",
        ivm.stats().disjuncts_registered,
        ivm.stats().conditions_indexed
    );

    run_write(
        &mut ivm,
        &catalog,
        "INSERT INTO tickets (id, status, priority, assigned_to, points) \
         VALUES (1, 'OPEN', 'LOW', 'aniket', 3)",
        &["q-all", "q-mine-active", "q-open", "q-open-dup"],
        Some("q-open and q-open-dup share one status = 'OPEN' counter — a single bump fires both"),
    );

    run_write(
        &mut ivm,
        &catalog,
        "INSERT INTO tickets (id, status, priority, assigned_to, points) \
         VALUES (2, 'TODO', 'URGENT', 'vipul', 9)",
        &["q-all", "q-big", "q-hot"],
        Some(
            "q-mine-active's `status != 'DONE'` condition matched and bumped its \
             disjunct to 1 of 2 — the assigned_to conjunct never matched, so it \
             never fired",
        ),
    );

    run_write(
        &mut ivm,
        &catalog,
        "UPDATE tickets SET status = 'DONE', priority = 'LOW', assigned_to = 'aniket', \
         points = 3 WHERE id = 1",
        &["q-all", "q-mine-active", "q-open", "q-open-dup"],
        Some(
            "row moves OUT of q-open, q-open-dup and q-mine-active (Delete), refreshes \
             in place for q-all (Add)",
        ),
    );

    run_write(
        &mut ivm,
        &catalog,
        "DELETE FROM tickets WHERE id = 2",
        &["q-all", "q-big", "q-hot"],
        Some("deletes carry no row data — these were found purely via frame membership"),
    );

    println!("\n== final materialized frames ==========================================");
    for (uuid, _) in subscriptions {
        let frame = ivm.dataframe_for(uuid).expect("registered above");
        println!("  {uuid:<14} {} row(s)", frame.len());
    }

    println!("\n== cumulative counters ================================================");
    println!("{}", ivm.stats());
}

/// Parse one write, feed it through the engine, then print found-vs-expected
/// impacted subscriptions, the emitted operations, and the routing-cost delta.
fn run_write(
    ivm: &mut IVM,
    catalog: &Catalog,
    sql: &str,
    expected_impacted: &[&str],
    note: Option<&str>,
) {
    println!("\n-- {sql}");
    let write = parse_write(sql, catalog)
        .unwrap_or_else(|error| panic!("{}", point_at(sql, &error)));

    let before: IvmStats = ivm.stats().clone();
    let ops = ivm.incremental_update(&write);
    let cost = ivm.stats().diff(&before);

    let found: Vec<&str> = ops.iter().map(|(uuid, _)| uuid.as_str()).collect();
    let verdict = if found == expected_impacted {
        "PASS"
    } else {
        "MISMATCH"
    };

    println!("   expected : {expected_impacted:?}");
    println!("   impacted : {found:?}   [{verdict}]");
    for (uuid, op) in &ops {
        println!("   op       : {uuid:<14} <- {}", fmt_op(op));
    }
    println!("   cost     : {}", cost.routing_summary());
    if let Some(note) = note {
        println!("   note     : {note}");
    }
}

/// Builds the demo `tickets` schema: integer primary key `id`, string
/// columns `status` / `priority` / `assigned_to`, and integer `points`.
fn tickets_table() -> DbTable {
    DbTable::new(
        "tickets",
        ["id"],
        vec![
            DbColumn::new("id", ValueType::Int),
            DbColumn::new("status", ValueType::String),
            DbColumn::new("priority", ValueType::String),
            DbColumn::new("assigned_to", ValueType::String),
            DbColumn::new("points", ValueType::Int),
        ],
    )
}

/// Renders a [`DataFrameOperation`] as `Add(key)` or `Delete(key)`, showing
/// only the primary-key values (Add payloads are elided).
fn fmt_op(op: &DataFrameOperation) -> String {
    match op {
        DataFrameOperation::Add(key, _) => format!("Add({})", fmt_key(key)),
        DataFrameOperation::Delete(key) => format!("Delete({})", fmt_key(key)),
    }
}

/// Formats a [`DataFrameKey`] as `column=value` pairs, sorted by column name
/// so the output is deterministic regardless of map iteration order.
fn fmt_key(key: &DataFrameKey) -> String {
    let mut parts: Vec<String> = key
        .pkey_value
        .iter()
        .map(|(column, value)| format!("{column}={}", fmt_value(value)))
        .collect();
    parts.sort();
    parts.join(", ")
}

/// Renders a [`Value`] as SQL-flavored literal text: quoted strings, bare
/// numerics and booleans, `NULL`, and `Debug` output for any other variant.
fn fmt_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::String(s) => format!("'{s}'"),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        other => format!("{other:?}"),
    }
}
