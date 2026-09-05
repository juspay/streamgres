//! End-to-end routing scenarios for the IVM engine — the assertable version
//! of the demo in `src/main.rs`.

use std::collections::HashMap;

use jus_sync::ivm::{QueryId, IVM};
use jus_sync::model::*;

/// Builds the standard five-column test table (`id` pkey, plus `status`,
/// `priority`, `assigned_to`, `points`) under the given name.
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

/// Wraps a filter into a [`ReadQuery`] on `table`, ordered by `id` ASC with
/// a limit of 100.
fn query(table: &DbTable, filter: Where) -> ReadQuery {
    ReadQuery::new(
        table.name.clone(),
        filter,
        OrderBy::new(DbColumn::new("id", ValueType::Int), Order::ASC),
        100,
    )
}

/// Builds the single-column primary-key map `{"id": id}`.
fn pkey(id: i32) -> HashMap<String, Value> {
    HashMap::from([("id".to_owned(), Value::Int(id))])
}

/// Collects `(column, value)` pairs into a row map.
fn row(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(col, val)| ((*col).to_owned(), val.clone()))
        .collect()
}

/// Builds an INSERT [`WriteQuery`] for row `id` with the given column data.
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

/// Builds an UPDATE [`WriteQuery`] carrying the full new row image for `id`.
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

/// Builds a DELETE [`WriteQuery`] for row `id`.
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
        "q-open",
        query(
            tickets,
            Where::condition("status", ComparisonOperator::EQ, "OPEN"),
        ),
    );
    ivm.register_query(
        "q-mine-active",
        query(
            tickets,
            Where::AND(vec![
                Where::condition("assigned_to", ComparisonOperator::EQ, "aniket"),
                Where::condition("status", ComparisonOperator::NEQ, "DONE"),
            ]),
        ),
    );
    ivm.register_query(
        "q-hot",
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
        "q-big",
        query(
            tickets,
            Where::condition("points", ComparisonOperator::GTE, 8),
        ),
    );
    ivm.register_query("q-all", query(tickets, Where::AND(vec![])));
    ivm
}

/// The canonical row: OPEN, LOW priority, assigned to aniket, 3 points —
/// matches `q-open`, `q-mine-active`, and `q-all` of the standard fixture.
fn open_ticket_row() -> Vec<(&'static str, Value)> {
    vec![
        ("status", "OPEN".into()),
        ("priority", "LOW".into()),
        ("assigned_to", "aniket".into()),
        ("points", 3.into()),
    ]
}

/// Extracts the impacted subscription uuids from an operation batch.
fn impacted(ops: &[(QueryId, DataFrameOperation)]) -> Vec<&str> {
    ops.iter().map(|(uuid, _)| uuid.as_str()).collect()
}

/// An insert produces `Add`s only for the subscriptions whose filters the
/// row satisfies; non-matching frames stay empty.
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

/// A disjunct that only partially matches never fires: `status != 'DONE'`
/// (a conjunct of q-mine-active's single disjunct) matches the inserted row
/// and bumps the counter to 1 of 2, but the other conjunct
/// (`assigned_to = 'aniket'`) never matches — so q-mine-active is not
/// impacted, and increments outnumber fires in the stats.
#[test]
fn partial_disjunct_does_not_fire() {
    let tickets = table("tickets");
    let mut ivm = standard_ivm(&tickets);

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
    assert!(ivm.stats().disjunct_increments > ivm.stats().disjuncts_fired);
}

/// An update that stops a held row from matching emits `Delete` for the
/// subscriptions it leaves; the row still matches the unfiltered
/// subscription (`q-all`), which is refreshed in place with an `Add`.
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
    assert!(matches!(by_uuid["q-all"], DataFrameOperation::Add(..)));

    assert_eq!(ivm.dataframe_for("q-open").unwrap().len(), 0);
    assert_eq!(ivm.dataframe_for("q-all").unwrap().len(), 1);
}

/// An update whose row keeps matching replaces the stored record's data in
/// the frame rather than duplicating or dropping it.
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

/// A delete emits `Delete` only to the subscriptions actually holding the
/// row; deleting an unknown row impacts nothing.
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

    let ops = ivm.incremental_update(&delete(&tickets, 99));
    assert!(ops.is_empty());
}

/// A `WHERE TRUE` subscription (empty AND) is impacted by every write on
/// its table, regardless of row contents.
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

/// `status = 'NOPE' OR TRUE` is always true, but its only leaf condition
/// does not match the inserted row, so the reverse index alone cannot find
/// the subscription — the vacuous-satisfiability path must route it.
#[test]
fn vacuously_true_filter_is_found_even_when_its_leaf_fails() {
    let tickets = table("tickets");
    let mut ivm = IVM::new();
    ivm.register_query(
        "q-weird",
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

/// Routing is table-scoped: a `tickets` write, even one whose row matches
/// the calls subscription's condition textually, must not reach the
/// subscription registered on `calls`.
#[test]
fn writes_on_other_tables_are_isolated() {
    let tickets = table("tickets");
    let calls = table("calls");
    let mut ivm = standard_ivm(&tickets);
    ivm.register_query(
        "q-calls-active",
        query(
            &calls,
            Where::condition("status", ComparisonOperator::EQ, "ACTIVE"),
        ),
    );

    let ops = ivm.incremental_update(&insert(&calls, 1, &[("status", "ACTIVE".into())]));
    assert_eq!(impacted(&ops), vec!["q-calls-active"]);

    let ops = ivm.incremental_update(&insert(&tickets, 1, &[("status", "ACTIVE".into())]));
    assert!(!impacted(&ops).contains(&"q-calls-active"));
}

/// The read-only probe and the mutating path agree: `search_impacted_queries`
/// returns exactly the uuids that `incremental_update` then emits ops for.
#[test]
fn search_impacted_queries_matches_incremental_update_routing() {
    let tickets = table("tickets");
    let mut ivm = standard_ivm(&tickets);

    let write = insert(&tickets, 1, &open_ticket_row());
    let found = ivm.search_impacted_queries(&write);
    let ops = ivm.incremental_update(&write);

    let found: Vec<&str> = found.iter().map(QueryId::as_str).collect();
    assert_eq!(found, impacted(&ops));
}

/// Subscriptions sharing an identical disjunct shape share one counter, so
/// both are routed by a single condition evaluation — one probe, full
/// fan-out — and the row later moves out of both subscriptions
/// consistently.
#[test]
fn duplicate_condition_routes_to_every_subscriber() {
    let tickets = table("tickets");
    let mut ivm = standard_ivm(&tickets);
    ivm.register_query(
        "q-open-dup",
        query(
            &tickets,
            Where::condition("status", ComparisonOperator::EQ, "OPEN"),
        ),
    );

    let ops = ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));
    let found = impacted(&ops);
    assert!(found.contains(&"q-open"));
    assert!(found.contains(&"q-open-dup"));

    let mut done_row = open_ticket_row();
    done_row[0] = ("status", "DONE".into());
    let ops = ivm.incremental_update(&update(&tickets, 1, &done_row));
    let found = impacted(&ops);
    assert!(found.contains(&"q-open"));
    assert!(found.contains(&"q-open-dup"));
}

/// A filter with OR across ANDs routes through whichever disjunct matches.
/// Filter: `(status = 'OPEN' AND priority = 'LOW') OR points >= 8`. Row 1:
/// the second disjunct fires (points) while the first stays partial (wrong
/// priority). Row 2: the first disjunct fires, the second does not. Row 3:
/// neither fires.
#[test]
fn or_of_ands_routes_by_either_disjunct() {
    let tickets = table("tickets");
    let mut ivm = IVM::new();
    ivm.register_query(
        "q-either",
        query(
            &tickets,
            Where::OR(vec![
                Where::AND(vec![
                    Where::condition("status", ComparisonOperator::EQ, "OPEN"),
                    Where::condition("priority", ComparisonOperator::EQ, "LOW"),
                ]),
                Where::condition("points", ComparisonOperator::GTE, 8),
            ]),
        ),
    );

    let ops = ivm.incremental_update(&insert(
        &tickets,
        1,
        &[("status", "OPEN".into()), ("priority", "HIGH".into()), ("points", 9.into())],
    ));
    assert_eq!(impacted(&ops), vec!["q-either"]);

    let ops = ivm.incremental_update(&insert(
        &tickets,
        2,
        &[("status", "OPEN".into()), ("priority", "LOW".into()), ("points", 1.into())],
    ));
    assert_eq!(impacted(&ops), vec!["q-either"]);

    let ops = ivm.incremental_update(&insert(
        &tickets,
        3,
        &[("status", "DONE".into()), ("priority", "LOW".into()), ("points", 1.into())],
    ));
    assert!(ops.is_empty());
}

/// `WHERE FALSE` normalizes to zero disjuncts: nothing to fire, ever.
#[test]
fn where_false_never_matches() {
    let tickets = table("tickets");
    let mut ivm = IVM::new();
    ivm.register_query("q-never", query(&tickets, Where::OR(vec![])));

    let ops = ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));
    assert!(ops.is_empty());
    assert!(ivm.dataframe_for("q-never").unwrap().is_empty());
}

/// Re-registering a uuid with a different query must fully replace its
/// routing — a stale condition link would corrupt the new query's disjunct
/// counters, since firing is exact counting. Replacing the status filter
/// with a points filter resets the frame, the old condition no longer
/// routes here, and the new one does; unregistering removes the
/// subscription entirely.
#[test]
fn reregistration_replaces_routing_and_unregister_removes_it() {
    let tickets = table("tickets");
    let mut ivm = IVM::new();
    ivm.register_query(
        "q",
        query(&tickets, Where::condition("status", ComparisonOperator::EQ, "OPEN")),
    );
    ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));
    assert_eq!(ivm.dataframe_for("q").unwrap().len(), 1);

    ivm.register_query(
        "q",
        query(&tickets, Where::condition("points", ComparisonOperator::GTE, 8)),
    );
    assert!(ivm.dataframe_for("q").unwrap().is_empty());

    let ops = ivm.incremental_update(&insert(&tickets, 2, &open_ticket_row()));
    assert!(ops.is_empty(), "an OPEN low-points row no longer matches");

    let mut big = open_ticket_row();
    big[3] = ("points", 9.into());
    let ops = ivm.incremental_update(&insert(&tickets, 3, &big));
    assert_eq!(impacted(&ops), vec!["q"]);

    ivm.unregister_query("q");
    assert!(ivm.dataframe_for("q").is_none());
    let ops = ivm.incremental_update(&insert(&tickets, 4, &big));
    assert!(ops.is_empty());
}

/// Interleaved writes across tables keep their counting state fully
/// independent — the invariant that will make table-sharded parallelism
/// safe: two tables carrying the textually identical two-condition filter
/// get separate counters, a partial bump left behind on one table must not
/// leak into the other, and the stale count must be discarded (not resumed)
/// when its own table is written again with the other half of the filter.
#[test]
fn interleaved_writes_across_tables_keep_counters_independent() {
    let tickets = table("tickets");
    let calls = table("calls");
    let filter = || {
        Where::AND(vec![
            Where::condition("status", ComparisonOperator::EQ, "OPEN"),
            Where::condition("points", ComparisonOperator::GTE, 5),
        ])
    };
    let mut ivm = IVM::new();
    ivm.register_query("q-tickets", query(&tickets, filter()));
    ivm.register_query("q-calls", query(&calls, filter()));

    let ops = ivm.incremental_update(&insert(
        &tickets,
        1,
        &[("status", "OPEN".into()), ("points", 1.into())],
    ));
    assert!(ops.is_empty(), "tickets counter stays partial at 1 of 2");

    let ops = ivm.incremental_update(&insert(
        &calls,
        1,
        &[("status", "OPEN".into()), ("points", 9.into())],
    ));
    assert_eq!(impacted(&ops), vec!["q-calls"]);

    let ops = ivm.incremental_update(&insert(
        &tickets,
        2,
        &[("status", "DONE".into()), ("points", 9.into())],
    ));
    assert!(
        ops.is_empty(),
        "the stale 1-of-2 from write one must not combine with this write's other half"
    );

    let ops = ivm.incremental_update(&insert(
        &tickets,
        3,
        &[("status", "OPEN".into()), ("points", 9.into())],
    ));
    assert_eq!(impacted(&ops), vec!["q-tickets"]);
}

/// Two subscriptions with the same filter share one disjunct counter;
/// unregistering one must leave the counter firing for the survivor, and
/// unregistering the survivor must silence it entirely.
#[test]
fn shared_counter_survives_partial_unregistration() {
    let tickets = table("tickets");
    let mut ivm = IVM::new();
    let filter = Where::condition("status", ComparisonOperator::EQ, "OPEN");
    ivm.register_query("q-a", query(&tickets, filter.clone()));
    ivm.register_query("q-b", query(&tickets, filter));

    let ops = ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));
    assert_eq!(impacted(&ops), vec!["q-a", "q-b"]);

    ivm.unregister_query("q-a");
    let ops = ivm.incremental_update(&insert(&tickets, 2, &open_ticket_row()));
    assert_eq!(impacted(&ops), vec!["q-b"]);

    ivm.unregister_query("q-b");
    let ops = ivm.incremental_update(&insert(&tickets, 3, &open_ticket_row()));
    assert!(ops.is_empty());
}

/// The DNF counting result must agree with plain tree evaluation of the
/// filter — `evaluate` is the semantic oracle. Each row is inserted under a
/// fresh pkey to keep membership out of the picture: impacted must equal
/// exactly the filters the row image satisfies.
#[test]
fn counting_agrees_with_tree_evaluation() {
    use jus_sync::ivm::evaluate;
    use ComparisonOperator::*;

    let tickets = table("tickets");
    let filters: Vec<Where> = vec![
        Where::condition("status", EQ, "OPEN"),
        Where::AND(vec![
            Where::condition("status", EQ, "OPEN"),
            Where::condition("points", GTE, 5),
        ]),
        Where::OR(vec![
            Where::condition("status", EQ, "OPEN"),
            Where::condition("points", GTE, 5),
        ]),
        Where::AND(vec![
            Where::OR(vec![
                Where::condition("status", EQ, "OPEN"),
                Where::condition("status", EQ, "TODO"),
            ]),
            Where::OR(vec![
                Where::condition("priority", EQ, "HIGH"),
                Where::condition("points", GTE, 5),
            ]),
        ]),
        Where::AND(vec![]),
        Where::OR(vec![]),
        Where::OR(vec![
            Where::condition("status", EQ, "NOPE"),
            Where::AND(vec![]),
        ]),
        Where::AND(vec![
            Where::condition("status", NEQ, "DONE"),
            Where::condition("priority", NOT_IN, Value::List(vec!["LOW".into()])),
        ]),
        Where::AND(vec![
            Where::condition("status", EQ, "OPEN"),
            Where::condition("status", EQ, "OPEN"),
        ]),
    ];
    let rows: Vec<Vec<(&str, Value)>> = vec![
        vec![("status", "OPEN".into()), ("priority", "LOW".into()), ("points", 3.into())],
        vec![("status", "OPEN".into()), ("priority", "HIGH".into()), ("points", 9.into())],
        vec![("status", "TODO".into()), ("priority", "MEDIUM".into()), ("points", 5.into())],
        vec![("status", "DONE".into()), ("priority", "LOW".into()), ("points", 9.into())],
        vec![("status", "NOPE".into()), ("points", 1.into())],
    ];

    let mut ivm = IVM::new();
    for (index, filter) in filters.iter().enumerate() {
        ivm.register_query(format!("q{index:02}"), query(&tickets, filter.clone()));
    }

    for (row_index, pairs) in rows.iter().enumerate() {
        let write = insert(&tickets, row_index as i32, pairs);
        let image = write.new_row_image().expect("inserts carry a row image");
        let expected: Vec<String> = filters
            .iter()
            .enumerate()
            .filter(|(_, filter)| evaluate(filter, &image, &mut 0))
            .map(|(index, _)| format!("q{index:02}"))
            .collect();
        let ops = ivm.incremental_update(&write);
        let got: Vec<&str> = impacted(&ops);
        assert_eq!(got, expected, "row {row_index}: {pairs:?}");
    }
}

/// Every subscription owns its frame (the forward index is keyed by uuid),
/// so identical queries under different uuids track the same rows in
/// independent frames — same contents, separate frames.
#[test]
fn identical_queries_maintain_independent_frames() {
    let tickets = table("tickets");
    let mut ivm = IVM::new();
    let filter = Where::condition("status", ComparisonOperator::EQ, "OPEN");
    let snapshot = ivm.register_query("q-a", query(&tickets, filter.clone()));
    assert!(snapshot.is_empty());
    ivm.register_query("q-b", query(&tickets, filter));

    let ops = ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));
    assert_eq!(impacted(&ops), vec!["q-a", "q-b"]);

    assert_eq!(ivm.dataframe_for("q-a"), ivm.dataframe_for("q-b"));
    assert_eq!(ivm.dataframe_for("q-a").unwrap().len(), 1);
}

/// A subscription registered late starts from its own empty frame — it is
/// never handed rows (or membership-routed `Delete`s) for writes that
/// happened before it existed: a delete of a pre-registration row reaches
/// only the subscription that actually holds it, while new writes reach
/// both.
#[test]
fn late_identical_registration_starts_empty() {
    let tickets = table("tickets");
    let mut ivm = IVM::new();
    ivm.register_query("early", query(&tickets, Where::AND(vec![])));
    for id in 1..=3 {
        ivm.incremental_update(&insert(&tickets, id, &open_ticket_row()));
    }

    let snapshot = ivm.register_query("late", query(&tickets, Where::AND(vec![])));
    assert!(snapshot.is_empty(), "no inherited rows from the earlier twin");

    let ops = ivm.incremental_update(&delete(&tickets, 1));
    assert_eq!(impacted(&ops), vec!["early"]);

    let ops = ivm.incremental_update(&insert(&tickets, 4, &open_ticket_row()));
    assert_eq!(impacted(&ops), vec!["early", "late"]);
    assert_eq!(ivm.dataframe_for("early").unwrap().len(), 3);
    assert_eq!(ivm.dataframe_for("late").unwrap().len(), 1);
}
