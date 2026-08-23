//! End-to-end routing scenarios for the IVM engine — the assertable version
//! of the demo in `src/main.rs`.

use std::collections::HashMap;

use jus_sync::ivm::IVM;
use jus_sync::model::*;

// -- fixture helpers -------------------------------------------------------

fn table(name: &str) -> DbTable {
    DbTable::new(
        name,
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

fn query(table: &DbTable, filter: Where) -> ReadQuery {
    ReadQuery::new(
        table.name.clone(),
        filter,
        OrderBy::new(DbColumn::new("id", ValueType::Int), Order::ASC),
        100,
    )
}

fn pkey(id: i32) -> HashMap<String, Value> {
    HashMap::from([("id".to_owned(), Value::Int(id))])
}

fn row(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(col, val)| ((*col).to_owned(), val.clone()))
        .collect()
}

fn insert(table: &DbTable, id: i32, pairs: &[(&str, Value)]) -> WriteQuery {
    WriteQuery::INSERT(InsertQuery {
        table: table.name.clone(),
        pkey_value: pkey(id),
        record: DbRecord {
            table: table.name.clone(),
            pkey_value: pkey(id),
            data: row(pairs),
        },
    })
}

fn update(table: &DbTable, id: i32, pairs: &[(&str, Value)]) -> WriteQuery {
    WriteQuery::UPDATE(UpdateQuery {
        table: table.name.clone(),
        pkey_value: pkey(id),
        record: DbRecord {
            table: table.name.clone(),
            pkey_value: pkey(id),
            data: row(pairs),
        },
    })
}

fn delete(table: &DbTable, id: i32) -> WriteQuery {
    WriteQuery::DELETE(DeleteQuery {
        table: table.name.clone(),
        pkey_value: pkey(id),
    })
}

/// The standard fixture: four distinct-condition subscriptions plus a
/// full-table one, on `tickets`.
fn standard_ivm(tickets: &DbTable) -> IVM {
    let mut ivm = IVM::new();
    ivm.register_query(
        "q-open".into(),
        query(
            tickets,
            Where::condition("status", ComparisonOperator::EQ, "OPEN"),
        ),
    );
    ivm.register_query(
        "q-mine-active".into(),
        query(
            tickets,
            Where::AND(vec![
                Where::condition("assigned_to", ComparisonOperator::EQ, "aniket"),
                Where::condition("status", ComparisonOperator::NEQ, "DONE"),
            ]),
        ),
    );
    ivm.register_query(
        "q-hot".into(),
        query(
            tickets,
            Where::condition(
                "priority",
                ComparisonOperator::IN,
                Value::List(vec!["HIGH".into(), "URGENT".into()]),
            ),
        ),
    );
    ivm.register_query(
        "q-big".into(),
        query(
            tickets,
            Where::condition("points", ComparisonOperator::GTE, 8),
        ),
    );
    ivm.register_query("q-all".into(), query(tickets, Where::AND(vec![])));
    ivm
}

fn open_ticket_row() -> Vec<(&'static str, Value)> {
    vec![
        ("status", "OPEN".into()),
        ("priority", "LOW".into()),
        ("assigned_to", "aniket".into()),
        ("points", 3.into()),
    ]
}

fn impacted(ops: &[(String, DataFrameOperation)]) -> Vec<&str> {
    ops.iter().map(|(uuid, _)| uuid.as_str()).collect()
}

// -- scenarios -------------------------------------------------------------

#[test]
fn insert_routes_to_matching_queries_only() {
    let tickets = table("tickets");
    let mut ivm = standard_ivm(&tickets);

    let ops = ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));

    assert_eq!(impacted(&ops), vec!["q-all", "q-mine-active", "q-open"]);
    assert!(ops
        .iter()
        .all(|(_, op)| matches!(op, DataFrameOperation::Add(..))));
    assert_eq!(ivm.dataframe_for("q-open").unwrap().len(), 1);
    assert_eq!(ivm.dataframe_for("q-hot").unwrap().len(), 0);
}

#[test]
fn index_hit_still_requires_full_predicate_match() {
    let tickets = table("tickets");
    let mut ivm = standard_ivm(&tickets);

    // status != 'DONE' (a leaf of q-mine-active) matches this row, but the
    // AND's other leaf (assigned_to = 'aniket') does not.
    let ops = ivm.incremental_update(&insert(
        &tickets,
        2,
        &[
            ("status", "TODO".into()),
            ("priority", "URGENT".into()),
            ("assigned_to", "vipul".into()),
            ("points", 9.into()),
        ],
    ));

    assert_eq!(impacted(&ops), vec!["q-all", "q-big", "q-hot"]);
    assert!(ivm.stats().index_hits > 0);
}

#[test]
fn update_moves_row_out_with_delete() {
    let tickets = table("tickets");
    let mut ivm = standard_ivm(&tickets);
    ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));

    let mut done_row = open_ticket_row();
    done_row[0] = ("status", "DONE".into());
    let ops = ivm.incremental_update(&update(&tickets, 1, &done_row));

    assert_eq!(impacted(&ops), vec!["q-all", "q-mine-active", "q-open"]);
    let by_uuid: HashMap<&str, &DataFrameOperation> =
        ops.iter().map(|(u, op)| (u.as_str(), op)).collect();
    assert!(matches!(by_uuid["q-open"], DataFrameOperation::Delete(_)));
    assert!(matches!(
        by_uuid["q-mine-active"],
        DataFrameOperation::Delete(_)
    ));
    // Still matches the unfiltered subscription: refreshed in place.
    assert!(matches!(by_uuid["q-all"], DataFrameOperation::Add(..)));

    assert_eq!(ivm.dataframe_for("q-open").unwrap().len(), 0);
    assert_eq!(ivm.dataframe_for("q-all").unwrap().len(), 1);
}

#[test]
fn update_refreshes_row_in_place_with_new_data() {
    let tickets = table("tickets");
    let mut ivm = standard_ivm(&tickets);
    ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));

    let mut renamed = open_ticket_row();
    renamed[3] = ("points", 4.into());
    ivm.incremental_update(&update(&tickets, 1, &renamed));

    let frame = ivm.dataframe_for("q-open").unwrap();
    let key = DataFrameKey::new(pkey(1));
    assert_eq!(frame.records[&key].data["points"], Value::Int(4));
}

#[test]
fn delete_reaches_only_queries_holding_the_row() {
    let tickets = table("tickets");
    let mut ivm = standard_ivm(&tickets);
    ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));

    let ops = ivm.incremental_update(&delete(&tickets, 1));

    assert_eq!(impacted(&ops), vec!["q-all", "q-mine-active", "q-open"]);
    assert!(ops
        .iter()
        .all(|(_, op)| matches!(op, DataFrameOperation::Delete(_))));
    assert!(ivm.dataframe_for("q-all").unwrap().is_empty());

    // Deleting an unknown row impacts nothing.
    let ops = ivm.incremental_update(&delete(&tickets, 99));
    assert!(ops.is_empty());
}

#[test]
fn conditionless_query_sees_every_write_on_its_table() {
    let tickets = table("tickets");
    let mut ivm = standard_ivm(&tickets);

    for id in 1..=3 {
        let ops = ivm.incremental_update(&insert(
            &tickets,
            id,
            &[("status", "WHATEVER".into()), ("points", id.into())],
        ));
        assert!(impacted(&ops).contains(&"q-all"));
    }
    assert_eq!(ivm.dataframe_for("q-all").unwrap().len(), 3);
}

#[test]
fn vacuously_true_filter_is_found_even_when_its_leaf_fails() {
    let tickets = table("tickets");
    let mut ivm = IVM::new();
    // `status = 'NOPE' OR TRUE` — always true, but its only leaf condition
    // does not match the row below, so the reverse index alone cannot find
    // it. The vacuous-satisfiability path must.
    ivm.register_query(
        "q-weird".into(),
        query(
            &tickets,
            Where::OR(vec![
                Where::condition("status", ComparisonOperator::EQ, "NOPE"),
                Where::AND(vec![]),
            ]),
        ),
    );

    let ops = ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));
    assert_eq!(impacted(&ops), vec!["q-weird"]);
}

#[test]
fn writes_on_other_tables_are_isolated() {
    let tickets = table("tickets");
    let calls = table("calls");
    let mut ivm = standard_ivm(&tickets);
    ivm.register_query(
        "q-calls-active".into(),
        query(
            &calls,
            Where::condition("status", ComparisonOperator::EQ, "ACTIVE"),
        ),
    );

    let ops = ivm.incremental_update(&insert(&calls, 1, &[("status", "ACTIVE".into())]));
    assert_eq!(impacted(&ops), vec!["q-calls-active"]);

    // A tickets write, even one whose row matches q-calls-active's condition
    // textually, must not reach the calls subscription.
    let ops = ivm.incremental_update(&insert(&tickets, 1, &[("status", "ACTIVE".into())]));
    assert!(!impacted(&ops).contains(&"q-calls-active"));
}

#[test]
fn search_impacted_queries_matches_incremental_update_routing() {
    let tickets = table("tickets");
    let mut ivm = standard_ivm(&tickets);

    let write = insert(&tickets, 1, &open_ticket_row());
    let found = ivm.search_impacted_queries(&write);
    let ops = ivm.incremental_update(&write);

    assert_eq!(
        found,
        impacted(&ops)
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    );
}

/// The reverse index holds every subscriber of a condition
/// (`HashMap<Condition, Vec<String>>`), so two subscriptions sharing an
/// identical condition are both routed — one probe, full fan-out.
#[test]
fn duplicate_condition_routes_to_every_subscriber() {
    let tickets = table("tickets");
    let mut ivm = standard_ivm(&tickets);
    ivm.register_query(
        "q-open-dup".into(),
        query(
            &tickets,
            Where::condition("status", ComparisonOperator::EQ, "OPEN"),
        ),
    );

    let ops = ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));
    let found = impacted(&ops);
    assert!(found.contains(&"q-open"));
    assert!(found.contains(&"q-open-dup"));

    // And the row later moves out of both subscriptions consistently.
    let mut done_row = open_ticket_row();
    done_row[0] = ("status", "DONE".into());
    let ops = ivm.incremental_update(&update(&tickets, 1, &done_row));
    let found = impacted(&ops);
    assert!(found.contains(&"q-open"));
    assert!(found.contains(&"q-open-dup"));
}

/// Every subscription owns its frame (the forward index is keyed by uuid),
/// so identical queries under different uuids track the same rows in
/// independent frames.
#[test]
fn identical_queries_maintain_independent_frames() {
    let tickets = table("tickets");
    let mut ivm = IVM::new();
    let filter = Where::condition("status", ComparisonOperator::EQ, "OPEN");
    let snapshot = ivm.register_query("q-a".into(), query(&tickets, filter.clone()));
    assert!(snapshot.is_empty());
    ivm.register_query("q-b".into(), query(&tickets, filter));

    let ops = ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));
    assert_eq!(impacted(&ops), vec!["q-a", "q-b"]);

    // Same contents, separate frames.
    assert_eq!(ivm.dataframe_for("q-a"), ivm.dataframe_for("q-b"));
    assert_eq!(ivm.dataframe_for("q-a").unwrap().len(), 1);
}

/// A subscription registered late starts from its own empty frame — it is
/// never handed rows (or membership-routed `Delete`s) for writes that
/// happened before it existed.
#[test]
fn late_identical_registration_starts_empty() {
    let tickets = table("tickets");
    let mut ivm = IVM::new();
    ivm.register_query("early".into(), query(&tickets, Where::AND(vec![])));
    for id in 1..=3 {
        ivm.incremental_update(&insert(&tickets, id, &open_ticket_row()));
    }

    let snapshot = ivm.register_query("late".into(), query(&tickets, Where::AND(vec![])));
    assert!(snapshot.is_empty(), "no inherited rows from the earlier twin");

    // A delete of a pre-registration row reaches only the subscription that
    // actually holds it.
    let ops = ivm.incremental_update(&delete(&tickets, 1));
    assert_eq!(impacted(&ops), vec!["early"]);

    // New writes reach both.
    let ops = ivm.incremental_update(&insert(&tickets, 4, &open_ticket_row()));
    assert_eq!(impacted(&ops), vec!["early", "late"]);
    assert_eq!(ivm.dataframe_for("early").unwrap().len(), 3);
    assert_eq!(ivm.dataframe_for("late").unwrap().len(), 1);
}
