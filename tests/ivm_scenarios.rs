//! End-to-end routing scenarios for the IVM engine — the assertable version
//! of the demo in `src/main.rs`.

use std::collections::HashMap;

use jus_sync::ivm::{MemoryStorage, PgStorage, QueryId, SingleTableIVM, SingleTableUpdate, Storage};
use jus_sync::model::*;
use std::rc::Rc;

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

/// Wraps a filter into a [`SingleTableReadQuery`] on `table`, ordered by `id` ASC with
/// a limit of 100.
fn query(table: &DbTable, filter: Where) -> SingleTableReadQuery {
    SingleTableReadQuery::new(
        table.name.clone(),
        filter,
        OrderBy::new("id", Order::ASC),
        100,
    )
}

/// Builds the single-column primary-key map `{"id": id}`.
fn pkey(id: i64) -> HashMap<String, Value> {
    HashMap::from([("id".to_owned(), Value::Int(id))])
}

/// Collects `(column, value)` pairs plus the `id` primary key into a full
/// row image — records must carry every column, pkey included.
fn full_row(id: i64, pairs: &[(&str, Value)]) -> DataFrameRow {
    let mut data: HashMap<String, Value> = pairs
        .iter()
        .map(|(col, val)| ((*col).to_owned(), val.clone()))
        .collect();
    data.insert("id".to_owned(), Value::Int(id));
    DataFrameRow { data }
}

/// Builds an INSERT [`WriteQuery`] for row `id` with the given column data.
fn insert(table: &DbTable, id: i64, pairs: &[(&str, Value)]) -> WriteQuery {
    WriteQuery::INSERT(InsertQuery {
        table: table.name.clone(),
        pkey_value: DataFrameKey::new(pkey(id)),
        record: full_row(id, pairs),
    })
}

/// Builds an UPDATE [`WriteQuery`] carrying the full new row image for `id`.
fn update(table: &DbTable, id: i64, pairs: &[(&str, Value)]) -> WriteQuery {
    WriteQuery::UPDATE(UpdateQuery {
        table: table.name.clone(),
        pkey_value: DataFrameKey::new(pkey(id)),
        record: full_row(id, pairs),
    })
}

/// Builds a DELETE [`WriteQuery`] for row `id`.
fn delete(table: &DbTable, id: i64) -> WriteQuery {
    WriteQuery::DELETE(DeleteQuery {
        table: table.name.clone(),
        pkey_value: DataFrameKey::new(pkey(id)),
    })
}

/// The standard fixture: four distinct-condition subscriptions plus a
/// full-table one, on `tickets`.
fn standard_ivm(tickets: &DbTable) -> SingleTableIVM {
    let mut ivm = SingleTableIVM::new(Rc::new(PgStorage));
    ivm.register_query(
        "q-open",
        query(
            tickets,
            Where::condition("status", ComparisonOperator::EQ, "OPEN"),
        ),
        None,
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
        None,
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
        None,
    );
    ivm.register_query(
        "q-big",
        query(
            tickets,
            Where::condition("points", ComparisonOperator::GTE, 8),
        ),
        None,
    );
    ivm.register_query("q-all", query(tickets, Where::AND(vec![])), None);
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

/// Extracts the impacted subscription uuids from an update batch,
/// deduplicated in emission order — an in-place replace contributes an
/// adjacent `Delete` + `Add` pair under one uuid.
fn impacted(ops: &[SingleTableUpdate]) -> Vec<&str> {
    let mut uuids: Vec<&str> = Vec::new();
    for update in ops {
        if !uuids.contains(&update.query.as_str()) {
            uuids.push(update.query.as_str());
        }
    }
    uuids
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
        .all(|update| matches!(update.op, DataFrameOperation::Add(..))));
    assert!(ops.iter().all(|update| update.table == "tickets"));
    assert_eq!(ivm.rows_for("q-open").unwrap().len(), 1);
    assert_eq!(ivm.rows_for("q-hot").unwrap().len(), 0);
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

/// An update that stops a held row from matching emits `Delete` — carrying
/// the removed image — for the subscriptions it leaves; the row still
/// matching the unfiltered subscription (`q-all`) is replaced in place with
/// an adjacent `Delete(old)` + `Add(new)` pair.
#[test]
fn update_moves_row_out_with_delete() {
    let tickets = table("tickets");
    let mut ivm = standard_ivm(&tickets);
    ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));

    let mut done_row = open_ticket_row();
    done_row[0] = ("status", "DONE".into());
    let ops = ivm.incremental_update(&update(&tickets, 1, &done_row));

    assert_eq!(impacted(&ops), vec!["q-all", "q-mine-active", "q-open"]);
    let ops_for = |uuid: &str| -> Vec<&DataFrameOperation> {
        ops.iter()
            .filter(|update| update.query.as_str() == uuid)
            .map(|update| &update.op)
            .collect()
    };
    let q_open = ops_for("q-open");
    let [DataFrameOperation::Delete(_, removed)] = q_open.as_slice() else {
        panic!("q-open expects exactly one Delete, got {q_open:?}");
    };
    assert_eq!(removed.data["status"], Value::String("OPEN".into()));
    assert!(matches!(
        ops_for("q-mine-active").as_slice(),
        [DataFrameOperation::Delete(..)]
    ));
    let q_all = ops_for("q-all");
    let [DataFrameOperation::Delete(_, old), DataFrameOperation::Add(_, new)] = q_all.as_slice()
    else {
        panic!("q-all expects the replace pair, got {q_all:?}");
    };
    assert_eq!(old.data["status"], Value::String("OPEN".into()));
    assert_eq!(new.data["status"], Value::String("DONE".into()));

    assert_eq!(ivm.rows_for("q-open").unwrap().len(), 0);
    assert_eq!(ivm.rows_for("q-all").unwrap().len(), 1);
}

/// An update whose row keeps matching replaces the stored row's data in
/// the shared frame rather than duplicating or dropping it, and the row's
/// subscriber tags name exactly the subscriptions holding it.
#[test]
fn update_refreshes_row_in_place_with_new_data() {
    let tickets = table("tickets");
    let mut ivm = standard_ivm(&tickets);
    ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));

    let mut renamed = open_ticket_row();
    renamed[3] = ("points", 4.into());
    ivm.incremental_update(&update(&tickets, 1, &renamed));

    let rows = ivm.rows_for("q-open").unwrap();
    let key = DataFrameKey::new(pkey(1));
    assert_eq!(rows[&key].data["points"], Value::Int(4));
    assert_eq!(
        ivm.holders_of(&TableName::from("tickets"), &key),
        ["q-all", "q-mine-active", "q-open"].map(QueryId::from)
    );
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
        .all(|update| matches!(update.op, DataFrameOperation::Delete(..))));
    assert!(ivm.rows_for("q-all").unwrap().is_empty());

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
    assert_eq!(ivm.rows_for("q-all").unwrap().len(), 3);
}

/// `status = 'NOPE' OR TRUE` is always true, but its only leaf condition
/// does not match the inserted row, so the reverse index alone cannot find
/// the subscription — the vacuous-satisfiability path must route it.
#[test]
fn vacuously_true_filter_is_found_even_when_its_leaf_fails() {
    let tickets = table("tickets");
    let mut ivm = SingleTableIVM::new(Rc::new(PgStorage));
    ivm.register_query(
        "q-weird",
        query(
            &tickets,
            Where::OR(vec![
                Where::condition("status", ComparisonOperator::EQ, "NOPE"),
                Where::AND(vec![]),
            ]),
        ),
        None,
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
        None,
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
        None,
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
    let mut ivm = SingleTableIVM::new(Rc::new(PgStorage));
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
        None,
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
    let mut ivm = SingleTableIVM::new(Rc::new(PgStorage));
    ivm.register_query("q-never", query(&tickets, Where::OR(vec![])), None);

    let ops = ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));
    assert!(ops.is_empty());
    assert!(ivm.rows_for("q-never").unwrap().is_empty());
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
    let mut ivm = SingleTableIVM::new(Rc::new(PgStorage));
    ivm.register_query(
        "q",
        query(&tickets, Where::condition("status", ComparisonOperator::EQ, "OPEN")),
        None,
    );
    ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));
    assert_eq!(ivm.rows_for("q").unwrap().len(), 1);

    ivm.register_query(
        "q",
        query(&tickets, Where::condition("points", ComparisonOperator::GTE, 8)),
        None,
    );
    assert!(ivm.rows_for("q").unwrap().is_empty());

    let ops = ivm.incremental_update(&insert(&tickets, 2, &open_ticket_row()));
    assert!(ops.is_empty(), "an OPEN low-points row no longer matches");

    let mut big = open_ticket_row();
    big[3] = ("points", 9.into());
    let ops = ivm.incremental_update(&insert(&tickets, 3, &big));
    assert_eq!(impacted(&ops), vec!["q"]);

    ivm.unregister_query("q");
    assert!(ivm.rows_for("q").is_none());
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
    let mut ivm = SingleTableIVM::new(Rc::new(PgStorage));
    ivm.register_query("q-tickets", query(&tickets, filter()), None);
    ivm.register_query("q-calls", query(&calls, filter()), None);

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
    let mut ivm = SingleTableIVM::new(Rc::new(PgStorage));
    let filter = Where::condition("status", ComparisonOperator::EQ, "OPEN");
    ivm.register_query("q-a", query(&tickets, filter.clone()), None);
    ivm.register_query("q-b", query(&tickets, filter), None);

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

    let mut ivm = SingleTableIVM::new(Rc::new(PgStorage));
    for (index, filter) in filters.iter().enumerate() {
        ivm.register_query(format!("q{index:02}"), query(&tickets, filter.clone()), None);
    }

    for (row_index, pairs) in rows.iter().enumerate() {
        let write = insert(&tickets, row_index as i64, pairs);
        let image = write.new_row_image().expect("inserts carry a row image");
        let expected: Vec<String> = filters
            .iter()
            .enumerate()
            .filter(|(_, filter)| evaluate(filter, &image.data, &mut 0))
            .map(|(index, _)| format!("q{index:02}"))
            .collect();
        let ops = ivm.incremental_update(&write);
        let got: Vec<&str> = impacted(&ops);
        assert_eq!(got, expected, "row {row_index}: {pairs:?}");
    }
}

/// Identical queries under different uuids share the stored rows but keep
/// independently tagged views — same contents, separate subscriptions,
/// each receiving its own operations.
#[test]
fn identical_queries_maintain_independent_frames() {
    let tickets = table("tickets");
    let mut ivm = SingleTableIVM::new(Rc::new(PgStorage));
    let filter = Where::condition("status", ComparisonOperator::EQ, "OPEN");
    let snapshot = ivm.register_query("q-a", query(&tickets, filter.clone()), None);
    assert!(snapshot.is_empty());
    ivm.register_query("q-b", query(&tickets, filter), None);

    let ops = ivm.incremental_update(&insert(&tickets, 1, &open_ticket_row()));
    assert_eq!(impacted(&ops), vec!["q-a", "q-b"]);

    assert_eq!(ivm.rows_for("q-a"), ivm.rows_for("q-b"));
    assert_eq!(ivm.rows_for("q-a").unwrap().len(), 1);
}

/// A subscription registered late with a query structurally identical to
/// an existing one is served from the shared frame: the twin's current
/// rows come back as its snapshot `Add`s — storage is a stub here, so the
/// rows can only have come from the frame — and from then on both twins
/// route together.
#[test]
fn late_identical_registration_inherits_the_twins_rows() {
    let tickets = table("tickets");
    let mut ivm = SingleTableIVM::new(Rc::new(PgStorage));
    ivm.register_query("early", query(&tickets, Where::AND(vec![])), None);
    for id in 1..=3 {
        ivm.incremental_update(&insert(&tickets, id, &open_ticket_row()));
    }

    let snapshot = ivm.register_query("late", query(&tickets, Where::AND(vec![])), None);
    assert_eq!(snapshot.len(), 3, "the twin's three rows arrive as Adds");
    assert!(snapshot
        .iter()
        .all(|op| matches!(op, DataFrameOperation::Add(..))));
    assert_eq!(ivm.rows_for("late"), ivm.rows_for("early"));
    assert_eq!(ivm.stats().snapshots_shared, 1);

    let ops = ivm.incremental_update(&delete(&tickets, 1));
    assert_eq!(impacted(&ops), vec!["early", "late"]);
    assert_eq!(ivm.rows_for("early").unwrap().len(), 2);
    assert_eq!(ivm.rows_for("late").unwrap().len(), 2);
}

/// ORDER BY + LIMIT: the engine buffers twice the requested limit, admits
/// only rows strictly better than the storage frontier (evicting the
/// worst past capacity), keeps a held row that worsens in place until a
/// better arrival evicts it, and refills from storage when a removal
/// drains the buffer to the requested limit.
#[test]
fn limit_window_admits_evicts_and_refills() {
    let tickets = table("tickets");
    let storage = Rc::new(MemoryStorage::new());
    let mut ivm = SingleTableIVM::new(storage.clone() as Rc<dyn Storage>);
    for (id, points) in [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)] {
        storage.apply(&insert(&tickets, id, &[("points", points.into())]));
    }
    let windowed = SingleTableReadQuery::new(
        tickets.name.clone(),
        Where::AND(vec![]),
        OrderBy::new("points", Order::ASC),
        2,
    );
    let snapshot = ivm.register_query("w", windowed, None);
    assert_eq!(snapshot.len(), 4, "the buffer holds twice the limit");

    let admit = insert(&tickets, 7, &[("points", 5.into())]);
    storage.apply(&admit);
    let ops = ivm.incremental_update(&admit);
    assert_eq!(ops.len(), 2, "admission plus eviction, got {ops:?}");
    assert!(
        matches!(&ops[0].op, DataFrameOperation::Add(key, _) if key.pkey_value["id"] == Value::Int(7))
    );
    assert!(
        matches!(&ops[1].op, DataFrameOperation::Delete(key, _) if key.pkey_value["id"] == Value::Int(4))
    );

    let reject = insert(&tickets, 8, &[("points", 100.into())]);
    storage.apply(&reject);
    assert!(
        ivm.incremental_update(&reject).is_empty(),
        "worse than the boundary — the admission guard rejects it"
    );

    for id in [7, 1] {
        let removal = delete(&tickets, id);
        storage.apply(&removal);
        ivm.incremental_update(&removal);
    }
    assert_eq!(
        ivm.rows_for("w").unwrap().len(),
        4,
        "draining to the limit refilled the buffer from storage"
    );
    assert_eq!(ivm.stats().window_evictions, 1);
    assert!(ivm.stats().window_refills >= 1);

    let worsen = update(&tickets, 2, &[("points", 999.into())]);
    storage.apply(&worsen);
    let ops = ivm.incremental_update(&worsen);
    assert_eq!(
        impacted(&ops),
        vec!["w"],
        "a held row worsening keeps its slot (replace pair), got {ops:?}"
    );
    assert_eq!(ops.len(), 2);
    assert_eq!(ivm.rows_for("w").unwrap().len(), 4);

    let better = insert(&tickets, 9, &[("points", 45.into())]);
    storage.apply(&better);
    let ops = ivm.incremental_update(&better);
    assert_eq!(ops.len(), 2);
    assert!(
        matches!(&ops[1].op, DataFrameOperation::Delete(key, _) if key.pkey_value["id"] == Value::Int(2)),
        "the worsened row is the one a better arrival evicts, got {ops:?}"
    );
}

/// Regression (review): a subscription between `replace_query` and the
/// caller's declared reconciliation has a filter ahead of its held rows.
/// Registering an identical query in that window must NOT be served from
/// the stale view — it falls back to storage — and only the caller's
/// explicit `mark_reconciled` (a fetch alone might be half of a swap)
/// re-enables twin donation.
#[test]
fn replaced_query_is_not_a_twin_donor_until_reconciled() {
    let tickets = table("tickets");
    let storage = Rc::new(MemoryStorage::new());
    let mut ivm = SingleTableIVM::new(storage.clone() as Rc<dyn Storage>);
    let narrow = query(
        &tickets,
        Where::condition(
            "points",
            ComparisonOperator::IN,
            Value::List(vec![Value::Int(10)]),
        ),
    );
    let wide = query(
        &tickets,
        Where::condition(
            "points",
            ComparisonOperator::IN,
            Value::List(vec![Value::Int(10), Value::Int(20)]),
        ),
    );
    storage.apply(&insert(&tickets, 1, &[("points", 10.into())]));
    storage.apply(&insert(&tickets, 2, &[("points", 20.into())]));
    let snapshot = ivm.register_query("s", narrow, None);
    assert_eq!(snapshot.len(), 1);

    ivm.replace_query("s", wide.clone());
    let snapshot = ivm.register_query("t", wide.clone(), None);
    assert_eq!(
        snapshot.len(),
        2,
        "mid-maintenance `s` must not donate; storage serves the full set"
    );

    ivm.unregister_query("t");
    ivm.fetch("s", "points", &[Value::Int(20)]);
    assert_eq!(ivm.rows_for("s").unwrap().len(), 2);
    let snapshot = ivm.register_query("u", wide.clone(), None);
    assert_eq!(
        snapshot.len(),
        2,
        "a fetch alone might be half of a swap — still no donation (served by storage)"
    );
    assert_eq!(ivm.stats().snapshots_shared, 0);

    ivm.unregister_query("u");
    ivm.mark_reconciled("s");
    let snapshot = ivm.register_query("v", wide, None);
    assert_eq!(snapshot.len(), 2, "a declared-reconciled twin donates");
    assert_eq!(ivm.stats().snapshots_shared, 1);
}

/// Regression (review): `replace_condition` opens the same maintenance
/// window as `replace_query` — until the caller declares reconciliation,
/// a registration with the identical (post-edit) query must be served by
/// storage, not by the half-reconciled subscription. And the edit itself
/// must not re-register anything: only `conditions_replaced` moves.
#[test]
fn replaced_condition_view_is_not_a_twin_donor_until_reconciled() {
    let tickets = table("tickets");
    let storage = Rc::new(MemoryStorage::new());
    let mut ivm = SingleTableIVM::new(storage.clone() as Rc<dyn Storage>);
    let in_points = |points: i64| {
        Condition::new(
            "points",
            ComparisonOperator::IN,
            Value::List(vec![Value::Int(points)]),
        )
    };
    let with = |points: i64| query(&tickets, Where::Condition(in_points(points)));
    storage.apply(&insert(&tickets, 1, &[("points", 10.into())]));
    storage.apply(&insert(&tickets, 2, &[("points", 20.into())]));
    let snapshot = ivm.register_query("a", with(10), None);
    assert_eq!(snapshot.len(), 1);

    let before = ivm.stats().clone();
    ivm.replace_condition("a", &in_points(10), in_points(20));
    let after = ivm.stats();
    assert_eq!(after.disjuncts_registered, before.disjuncts_registered);
    assert_eq!(after.conditions_indexed, before.conditions_indexed);
    assert!(after.conditions_replaced > before.conditions_replaced);

    let snapshot = ivm.register_query("b", with(20), None);
    assert_eq!(snapshot.len(), 1, "served by storage, not the stale view");
    assert_eq!(snapshot[0].key(), &DataFrameKey::new(pkey(2)));
    assert_eq!(ivm.stats().snapshots_shared, 0);

    ivm.unregister_query("b");
    ivm.fetch("a", "points", &[Value::Int(20)]);
    ivm.delete_rows("a", "points", &[Value::Int(10)]);
    ivm.mark_reconciled("a");
    let snapshot = ivm.register_query("c", with(20), None);
    assert_eq!(snapshot.len(), 1, "a declared-reconciled twin donates");
    assert_eq!(ivm.stats().snapshots_shared, 1);

    let admitted = insert(&tickets, 3, &[("points", 20.into())]);
    storage.apply(&admitted);
    assert_eq!(
        impacted(&ivm.incremental_update(&admitted)),
        vec!["a", "c"],
        "the edited IN condition routes new writes"
    );
    let ignored = insert(&tickets, 4, &[("points", 10.into())]);
    storage.apply(&ignored);
    assert!(ivm.incremental_update(&ignored).is_empty());
}

/// A `LIMIT 0` subscription is permanently empty: an empty registration
/// snapshot, and later matching writes never admit.
#[test]
fn limit_zero_subscription_stays_empty() {
    let tickets = table("tickets");
    let storage = Rc::new(MemoryStorage::new());
    let mut ivm = SingleTableIVM::new(storage.clone() as Rc<dyn Storage>);
    storage.apply(&insert(&tickets, 1, &[("points", 10.into())]));
    let zero = SingleTableReadQuery::new(
        tickets.name.clone(),
        Where::AND(vec![]),
        OrderBy::new("points", Order::ASC),
        0,
    );
    assert!(ivm.register_query("z", zero, None).is_empty());

    let w = insert(&tickets, 2, &[("points", 20.into())]);
    storage.apply(&w);
    assert!(ivm.incremental_update(&w).is_empty());
    assert!(ivm.rows_for("z").unwrap().is_empty());
}

/// A DESC window mirrors the ASC behaviors: the snapshot loads the top
/// rows, admission requires strictly beating the smallest held value,
/// eviction drops the smallest, and refills walk downward.
#[test]
fn desc_window_admits_evicts_and_refills() {
    let tickets = table("tickets");
    let storage = Rc::new(MemoryStorage::new());
    let mut ivm = SingleTableIVM::new(storage.clone() as Rc<dyn Storage>);
    for (id, points) in [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)] {
        storage.apply(&insert(&tickets, id, &[("points", points.into())]));
    }
    let windowed = SingleTableReadQuery::new(
        tickets.name.clone(),
        Where::AND(vec![]),
        OrderBy::new("points", Order::DESC),
        2,
    );
    let snapshot = ivm.register_query("w", windowed, None);
    assert_eq!(snapshot.len(), 4);
    let mut held: Vec<Value> = ivm
        .rows_for("w")
        .unwrap()
        .values()
        .map(|row| row.data["points"].clone())
        .collect();
    held.sort_by_key(|value| match value {
        Value::Int(i) => *i,
        _ => 0,
    });
    assert_eq!(
        held,
        vec![Value::Int(30), Value::Int(40), Value::Int(50), Value::Int(60)],
        "DESC loads the four LARGEST"
    );

    let admit = insert(&tickets, 7, &[("points", 100.into())]);
    storage.apply(&admit);
    let ops = ivm.incremental_update(&admit);
    assert_eq!(ops.len(), 2, "the best row is admitted and 30 evicted, got {ops:?}");
    assert!(
        matches!(&ops[1].op, DataFrameOperation::Delete(key, _) if key.pkey_value["id"] == Value::Int(3))
    );

    let reject = insert(&tickets, 8, &[("points", 5.into())]);
    storage.apply(&reject);
    assert!(ivm.incremental_update(&reject).is_empty());

    for id in [7, 6] {
        let removal = delete(&tickets, id);
        storage.apply(&removal);
        ivm.incremental_update(&removal);
    }
    assert_eq!(
        ivm.rows_for("w").unwrap().len(),
        4,
        "drained to the limit — refilled downward from storage"
    );
}

/// A fetch into a windowed subscription maintains the window: fetched
/// rows enter it and overflow evicts the worst, so the buffer never
/// silently overruns.
#[test]
fn fetch_respects_the_window() {
    let tickets = table("tickets");
    let storage = Rc::new(MemoryStorage::new());
    let mut ivm = SingleTableIVM::new(storage.clone() as Rc<dyn Storage>);
    for (id, points) in [(1, 10), (2, 20), (3, 30), (4, 40)] {
        storage.apply(&insert(&tickets, id, &[("points", points.into())]));
    }
    let windowed = SingleTableReadQuery::new(
        tickets.name.clone(),
        Where::AND(vec![]),
        OrderBy::new("points", Order::ASC),
        2,
    );
    assert_eq!(ivm.register_query("w", windowed, None).len(), 4);
    storage.apply(&insert(&tickets, 5, &[("points", 1.into())]));

    let ops = ivm.fetch("w", "points", &[Value::Int(1)]);
    assert_eq!(ops.len(), 2, "fetched Add plus overflow eviction, got {ops:?}");
    assert!(matches!(&ops[0], DataFrameOperation::Add(key, _) if key.pkey_value["id"] == Value::Int(5)));
    assert!(
        matches!(&ops[1], DataFrameOperation::Delete(key, _) if key.pkey_value["id"] == Value::Int(4))
    );
    assert_eq!(ivm.rows_for("w").unwrap().len(), 4);
}

/// The identical-query snapshot happens WITHOUT a storage round-trip: a
/// row committed to storage but not yet routed through the engine is
/// invisible to a twin's snapshot, while a registration with no twin
/// still reads storage and sees it.
#[test]
fn identical_registration_skips_storage() {
    let tickets = table("tickets");
    let storage = Rc::new(MemoryStorage::new());
    let mut ivm = SingleTableIVM::new(storage.clone() as Rc<dyn Storage>);
    let open = Where::condition("status", ComparisonOperator::EQ, "OPEN");
    ivm.register_query("q-a", query(&tickets, open.clone()), None);

    let routed = insert(&tickets, 1, &open_ticket_row());
    storage.apply(&routed);
    ivm.incremental_update(&routed);
    storage.apply(&insert(&tickets, 2, &open_ticket_row()));

    let snapshot = ivm.register_query("q-b", query(&tickets, open), None);
    assert_eq!(snapshot.len(), 1, "served from q-a's rows, not storage");
    assert_eq!(snapshot[0].key(), &DataFrameKey::new(pkey(1)));

    let snapshot = ivm.register_query("q-c", query(&tickets, Where::AND(vec![])), None);
    assert_eq!(snapshot.len(), 2, "no twin — storage is consulted");
}

/// Regression (review): the admission boundary is anchored at the storage
/// frontier, not at the worst held row. After a delete leaves the buffer
/// below capacity, an arrival beyond the frontier is still rejected (the
/// rows still in storage are better), a twin inherits the same frontier,
/// and the refill that follows a drain fetches from the frontier so the
/// held top-L stays exact.
#[test]
fn window_boundary_survives_a_deletion() {
    let tickets = table("tickets");
    let storage = Rc::new(MemoryStorage::new());
    let mut ivm = SingleTableIVM::new(storage.clone() as Rc<dyn Storage>);
    for id in 1..=8 {
        storage.apply(&insert(&tickets, id, &[("points", (id * 10).into())]));
    }
    let windowed = SingleTableReadQuery::new(
        tickets.name.clone(),
        Where::AND(vec![]),
        OrderBy::new("points", Order::ASC),
        2,
    );
    assert_eq!(ivm.register_query("w", windowed.clone(), None).len(), 4);
    assert_eq!(ivm.register_query("twin", windowed, None).len(), 4);

    let removal = delete(&tickets, 1);
    storage.apply(&removal);
    ivm.incremental_update(&removal);
    assert_eq!(ivm.rows_for("w").unwrap().len(), 3, "below capacity, above the limit: no refill");

    let beyond = insert(&tickets, 9, &[("points", 1000.into())]);
    storage.apply(&beyond);
    assert!(
        ivm.incremental_update(&beyond).is_empty(),
        "beyond the frontier (40): rejected for both subscriptions even with room in the buffer"
    );

    let within = insert(&tickets, 10, &[("points", 35.into())]);
    storage.apply(&within);
    let ops = ivm.incremental_update(&within);
    assert_eq!(impacted(&ops), vec!["twin", "w"], "inside the frontier: admitted, got {ops:?}");
    assert_eq!(ops.len(), 2, "no eviction while the buffer has room");

    for id in [2, 3] {
        let removal = delete(&tickets, id);
        storage.apply(&removal);
        ivm.incremental_update(&removal);
    }
    let mut held: Vec<i64> = ivm
        .rows_for("w")
        .unwrap()
        .values()
        .map(|row| match row.data["points"] {
            Value::Int(points) => points,
            _ => unreachable!(),
        })
        .collect();
    held.sort_unstable();
    assert_eq!(
        held,
        vec![35, 40, 50, 60],
        "the refill walked storage from the frontier; the top-2 is exact"
    );
}

/// Regression (review): rows tying the frontier are reachable. With four
/// rows sharing the boundary value and two of them held, refills fetch
/// from the frontier inclusive and dedup the held ties, so the held top-L
/// never contains a strictly worse row while a tied one sits in storage.
#[test]
fn window_refill_reaches_rows_tying_the_frontier() {
    let tickets = table("tickets");
    let storage = Rc::new(MemoryStorage::new());
    let mut ivm = SingleTableIVM::new(storage.clone() as Rc<dyn Storage>);
    for (id, points) in [(1, 1), (2, 2), (3, 3), (4, 3), (5, 3), (6, 3), (7, 5)] {
        storage.apply(&insert(&tickets, id, &[("points", points.into())]));
    }
    let windowed = SingleTableReadQuery::new(
        tickets.name.clone(),
        Where::AND(vec![]),
        OrderBy::new("points", Order::ASC),
        2,
    );
    assert_eq!(ivm.register_query("w", windowed, None).len(), 4);

    for id in [1, 2] {
        let removal = delete(&tickets, id);
        storage.apply(&removal);
        ivm.incremental_update(&removal);
    }
    let points_of = |ivm: &SingleTableIVM| -> Vec<i64> {
        let mut held: Vec<i64> = ivm
            .rows_for("w")
            .unwrap()
            .values()
            .map(|row| match row.data["points"] {
                Value::Int(points) => points,
                _ => unreachable!(),
            })
            .collect();
        held.sort_unstable();
        held
    };
    assert_eq!(points_of(&ivm), vec![3, 3, 3, 3], "the refill fetched the unheld ties, not the 5");

    let held_ids: Vec<i64> = ivm
        .rows_for("w")
        .unwrap()
        .keys()
        .map(|key| match key.pkey_value["id"] {
            Value::Int(id) => id,
            _ => unreachable!(),
        })
        .collect();
    for id in held_ids.into_iter().take(2) {
        let removal = delete(&tickets, id);
        storage.apply(&removal);
        ivm.incremental_update(&removal);
    }
    assert_eq!(
        points_of(&ivm),
        vec![3, 3, 5],
        "storage exhausted: the two remaining ties and the 5 are all held"
    );
    assert!(
        ivm.incremental_update(&insert(&tickets, 8, &[("points", 4.into())])).len() == 1,
        "with storage exhausted there is no boundary: a matching arrival is admitted"
    );
}
