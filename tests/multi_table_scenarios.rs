//! End-to-end scenarios for the multi-table (LEFT JOIN) layer, driven
//! through `MemoryStorage` — every write is mirrored into storage first and
//! then routed, the same order of events a real database produces.

use std::collections::HashMap;
use std::rc::Rc;

use jus_sync::ivm::{MemoryStorage, MultiTableIVM, MultiTableUpdate, QueryPart, Storage};
use jus_sync::model::*;

/// A `tickets(id, status, assigned_to, reviewer, project, team_id)` table.
fn tickets_table() -> DbTable {
    DbTable::new(
        "tickets",
        ["id"],
        vec![
            DbColumn::new("id", ValueType::Int),
            DbColumn::new("status", ValueType::String),
            DbColumn::new("assigned_to", ValueType::Int),
            DbColumn::new("reviewer", ValueType::Int),
            DbColumn::new("project", ValueType::Int),
            DbColumn::new("team_id", ValueType::Int),
        ],
    )
}

/// A `members(id, team)` table — its join column (`team`) is NOT the pkey,
/// so several members can share a team.
fn members_table() -> DbTable {
    DbTable::new(
        "members",
        ["id"],
        vec![
            DbColumn::new("id", ValueType::Int),
            DbColumn::new("team", ValueType::Int),
        ],
    )
}

/// A generic `(id, name)` sub table.
fn sub_table(name: &str) -> DbTable {
    DbTable::new(
        name,
        ["id"],
        vec![
            DbColumn::new("id", ValueType::Int),
            DbColumn::new("name", ValueType::String),
        ],
    )
}

/// A single-table query on `table` with the given filter, pkey-ordered,
/// unbounded.
fn query(table: &DbTable, filter: Where) -> SingleTableReadQuery {
    SingleTableReadQuery::new(
        table.name.clone(),
        filter,
        OrderBy::new("id", Order::ASC),
        u32::MAX,
    )
}

/// The standard spec: OPEN tickets LEFT JOIN `users` on
/// `tickets.assigned_to = users.id`.
fn tickets_users_query() -> MultiTableReadQuery {
    MultiTableReadQuery {
        main_table: query(
            &tickets_table(),
            Where::condition("status", ComparisonOperator::EQ, "OPEN"),
        ),
        left_joins: vec![LeftJoin {
            sub_table: query(&sub_table("users"), Where::AND(vec![])),
            main_table_column: "assigned_to".into(),
            sub_table_column: "id".into(),
        }],
    }
}

/// OPEN tickets LEFT JOIN `members` on `tickets.team_id = members.team` —
/// a join on a non-pkey sub column, so one value can match several rows.
fn tickets_members_query() -> MultiTableReadQuery {
    MultiTableReadQuery {
        main_table: query(
            &tickets_table(),
            Where::condition("status", ComparisonOperator::EQ, "OPEN"),
        ),
        left_joins: vec![LeftJoin {
            sub_table: query(&members_table(), Where::AND(vec![])),
            main_table_column: "team_id".into(),
            sub_table_column: "team".into(),
        }],
    }
}

/// OPEN tickets LEFT JOINed to `users` TWICE — `assigned_to = users.id`
/// (join 0) and `reviewer = users.id` (join 1) — so one user row can be
/// referenced by both joins at once.
fn two_users_joins_query() -> MultiTableReadQuery {
    MultiTableReadQuery {
        main_table: query(
            &tickets_table(),
            Where::condition("status", ComparisonOperator::EQ, "OPEN"),
        ),
        left_joins: vec![
            LeftJoin {
                sub_table: query(&sub_table("users"), Where::AND(vec![])),
                main_table_column: "assigned_to".into(),
                sub_table_column: "id".into(),
            },
            LeftJoin {
                sub_table: query(&sub_table("users"), Where::AND(vec![])),
                main_table_column: "reviewer".into(),
                sub_table_column: "id".into(),
            },
        ],
    }
}

fn pkey(id: i64) -> HashMap<String, Value> {
    HashMap::from([("id".to_owned(), Value::Int(id))])
}

/// Collects `(column, value)` pairs plus the `id` primary key into a full
/// row image — records must carry every column, pkey included.
fn full_row(id: i64, pairs: &[(&str, Value)]) -> DataFrameRow {
    let mut data: HashMap<String, Value> = pairs
        .iter()
        .map(|(column, value)| ((*column).to_owned(), value.clone()))
        .collect();
    data.insert("id".to_owned(), Value::Int(id));
    DataFrameRow { data }
}

fn insert(table: &str, id: i64, pairs: &[(&str, Value)]) -> WriteQuery {
    WriteQuery::INSERT(InsertQuery {
        table: TableName::from(table),
        pkey_value: DataFrameKey::new(pkey(id)),
        record: full_row(id, pairs),
    })
}

fn update(table: &str, id: i64, pairs: &[(&str, Value)]) -> WriteQuery {
    WriteQuery::UPDATE(UpdateQuery {
        table: TableName::from(table),
        pkey_value: DataFrameKey::new(pkey(id)),
        record: full_row(id, pairs),
    })
}

fn delete(table: &str, id: i64) -> WriteQuery {
    WriteQuery::DELETE(DeleteQuery {
        table: TableName::from(table),
        pkey_value: DataFrameKey::new(pkey(id)),
    })
}

fn ticket(id: i64, status: &str, user: i64) -> WriteQuery {
    insert(
        "tickets",
        id,
        &[("status", status.into()), ("assigned_to", Value::Int(user))],
    )
}

/// A ticket UPDATE carrying a full row image.
fn update_ticket(id: i64, status: &str, user: i64) -> WriteQuery {
    update(
        "tickets",
        id,
        &[("status", status.into()), ("assigned_to", Value::Int(user))],
    )
}

fn user(id: i64, name: &str) -> WriteQuery {
    insert("users", id, &[("name", name.into())])
}

fn member(id: i64, team: i64) -> WriteQuery {
    insert("members", id, &[("team", Value::Int(team))])
}

fn team_ticket(id: i64, team: i64) -> WriteQuery {
    insert(
        "tickets",
        id,
        &[("status", "OPEN".into()), ("team_id", Value::Int(team))],
    )
}

/// Mirror the write into storage, then route it — commit first, notify
/// second, like a real database.
fn write(
    ivm: &mut MultiTableIVM,
    storage: &MemoryStorage,
    w: WriteQuery,
) -> Vec<MultiTableUpdate> {
    storage.apply(&w);
    ivm.incremental_update(&w)
}

/// A fresh engine + shared storage handle.
fn engine() -> (MultiTableIVM, Rc<MemoryStorage>) {
    let storage = Rc::new(MemoryStorage::new());
    let ivm = MultiTableIVM::new(storage.clone() as Rc<dyn Storage>);
    (ivm, storage)
}

/// Compact rendering of updates for order-insensitive assertions.
fn tags(ops: &[MultiTableUpdate]) -> Vec<String> {
    let mut rendered: Vec<String> = ops
        .iter()
        .map(|update| {
            let part = match update.part {
                QueryPart::Main => "main".to_owned(),
                QueryPart::Join(index) => format!("join{index}"),
            };
            let op = match &update.op {
                DataFrameOperation::Add(key, _) => format!("add:{:?}", key.pkey_value["id"]),
                DataFrameOperation::Delete(key, _) => format!("del:{:?}", key.pkey_value["id"]),
            };
            format!("{}/{part}/{op}", update.query)
        })
        .collect();
    rendered.sort();
    rendered
}

fn frame_len(ivm: &MultiTableIVM, uuid: &str, part: QueryPart) -> usize {
    ivm.rows_for(uuid, part).map_or(0, |rows| rows.len())
}

/// Registration loads the whole snapshot from storage: every main row
/// matching the main `WHERE` is visible — with or without sub rows (left
/// join) — and each referenced join value's sub rows arrive from one
/// narrowed sub query.
#[test]
fn registration_returns_left_join_snapshot() {
    let (mut ivm, storage) = engine();
    storage.apply(&user(7, "meera"));
    storage.apply(&ticket(1, "OPEN", 7));
    storage.apply(&ticket(2, "OPEN", 99));
    storage.apply(&ticket(3, "DONE", 7));

    let snapshot = ivm.register_query("q", tickets_users_query());
    assert_eq!(
        tags(&snapshot),
        vec![
            "q/join0/add:Int(7)",
            "q/main/add:Int(1)",
            "q/main/add:Int(2)"
        ]
    );
    assert_eq!(frame_len(&ivm, "q", QueryPart::Main), 2);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 1);
}

/// A main row's first reference to a join value fetches the sub rows; a
/// second reference reuses them without fetching.
#[test]
fn first_reference_fetches_sub_rows() {
    let (mut ivm, storage) = engine();
    storage.apply(&user(7, "meera"));
    ivm.register_query("q", tickets_users_query());

    let ops = write(&mut ivm, &storage, ticket(1, "OPEN", 7));
    assert_eq!(tags(&ops), vec!["q/join0/add:Int(7)", "q/main/add:Int(1)"]);

    let ops = write(&mut ivm, &storage, ticket(2, "OPEN", 7));
    assert_eq!(tags(&ops), vec!["q/main/add:Int(2)"]);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 1);
}

/// Left join: a main row whose join value has no sub rows is fully
/// visible — the sub side is simply empty.
#[test]
fn main_row_visible_without_sub_rows() {
    let (mut ivm, storage) = engine();
    ivm.register_query("q", tickets_users_query());

    let ops = write(&mut ivm, &storage, ticket(1, "OPEN", 99));
    assert_eq!(tags(&ops), vec!["q/main/add:Int(1)"]);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Main), 1);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 0);
}

/// A sub row arriving for a referenced value routes natively through the
/// registered `IN` condition and fills the previously empty sub side; the
/// main part is untouched.
#[test]
fn sub_arrival_fills_referenced_value() {
    let (mut ivm, storage) = engine();
    ivm.register_query("q", tickets_users_query());
    write(&mut ivm, &storage, ticket(1, "OPEN", 9));

    let ops = write(&mut ivm, &storage, user(9, "kiran"));
    assert_eq!(tags(&ops), vec!["q/join0/add:Int(9)"]);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Main), 1);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 1);
}

/// A sub row for a value no main row references does not match the
/// registered `IN` condition — no operations, no frame residue.
#[test]
fn unreferenced_sub_write_is_ignored() {
    let (mut ivm, storage) = engine();
    ivm.register_query("q", tickets_users_query());

    let ops = write(&mut ivm, &storage, user(42, "nobody"));
    assert!(ops.is_empty());
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 0);
}

/// A write to a referenced sub row flows straight through as a join-part
/// replace — the `Delete(old)` + `Add(new)` pair for the row.
#[test]
fn referenced_sub_update_forwards() {
    let (mut ivm, storage) = engine();
    storage.apply(&user(7, "meera"));
    ivm.register_query("q", tickets_users_query());
    write(&mut ivm, &storage, ticket(1, "OPEN", 7));

    let ops = write(
        &mut ivm,
        &storage,
        insert("users", 7, &[("name", "meera k".into())]),
    );
    assert_eq!(tags(&ops), vec!["q/join0/add:Int(7)", "q/join0/del:Int(7)"]);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 1);
}

/// Left join: deleting the last sub row for a referenced value leaves the
/// main row in place — its sub side just goes empty.
#[test]
fn sub_deletion_leaves_main_row() {
    let (mut ivm, storage) = engine();
    storage.apply(&user(7, "meera"));
    ivm.register_query("q", tickets_users_query());
    write(&mut ivm, &storage, ticket(1, "OPEN", 7));

    let ops = write(&mut ivm, &storage, delete("users", 7));
    assert_eq!(tags(&ops), vec!["q/join0/del:Int(7)"]);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Main), 1);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 0);
}

/// Releasing references: only the LAST main row for a value prunes the sub
/// rows it kept alive.
#[test]
fn last_reference_prunes_sub_rows() {
    let (mut ivm, storage) = engine();
    storage.apply(&user(7, "meera"));
    ivm.register_query("q", tickets_users_query());
    write(&mut ivm, &storage, ticket(1, "OPEN", 7));
    write(&mut ivm, &storage, ticket(2, "OPEN", 7));

    let ops = write(&mut ivm, &storage, delete("tickets", 1));
    assert_eq!(tags(&ops), vec!["q/main/del:Int(1)"]);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 1);

    let ops = write(&mut ivm, &storage, delete("tickets", 2));
    assert_eq!(tags(&ops), vec!["q/join0/del:Int(7)", "q/main/del:Int(2)"]);
    let pruned = ops
        .iter()
        .find(|update| update.part == QueryPart::Join(0))
        .expect("the prune delete");
    assert!(
        matches!(&pruned.op, DataFrameOperation::Delete(_, row) if row.data["name"] == Value::String("meera".into()))
    );
    assert_eq!(frame_len(&ivm, "q", QueryPart::Main), 0);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 0);
}

/// A main update moving the join value replaces the main row (the
/// `Delete(old)` + `Add(new)` pair), fetches the new side, and prunes the
/// vacated side.
#[test]
fn main_update_moves_join_reference() {
    let (mut ivm, storage) = engine();
    storage.apply(&user(7, "meera"));
    storage.apply(&user(8, "arjun"));
    ivm.register_query("q", tickets_users_query());
    write(&mut ivm, &storage, ticket(1, "OPEN", 7));

    let before = ivm.stats().clone();
    let ops = write(&mut ivm, &storage, update_ticket(1, "OPEN", 8));
    assert_eq!(
        tags(&ops),
        vec![
            "q/join0/add:Int(8)",
            "q/join0/del:Int(7)",
            "q/main/add:Int(1)",
            "q/main/del:Int(1)"
        ]
    );
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 1);
    let after = ivm.stats();
    assert_eq!(
        after.disjuncts_registered, before.disjuncts_registered,
        "a reference move swaps the IN guard in place — no re-registration"
    );
    assert_eq!(after.conditions_indexed, before.conditions_indexed);
}

/// A main update moving to a value with no sub rows keeps the main row
/// (left join) and just empties its sub side.
#[test]
fn main_update_to_unmatched_value_keeps_main_row() {
    let (mut ivm, storage) = engine();
    storage.apply(&user(7, "meera"));
    ivm.register_query("q", tickets_users_query());
    write(&mut ivm, &storage, ticket(1, "OPEN", 7));

    let ops = write(&mut ivm, &storage, update_ticket(1, "OPEN", 99));
    assert_eq!(
        tags(&ops),
        vec![
            "q/join0/del:Int(7)",
            "q/main/add:Int(1)",
            "q/main/del:Int(1)"
        ]
    );
    assert_eq!(frame_len(&ivm, "q", QueryPart::Main), 1);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 0);
}

/// Regression (review): a main-row rewrite that KEEPS its join value must
/// emit exactly the ordered main `Delete(old)` + `Add(new)` pair and
/// nothing else — the pair is recognized and its join values diffed, so
/// the value's `left` count never swings through zero and the sub side is
/// neither pruned nor refetched. Also pins the carried images: the
/// `Delete` holds the pre-rewrite row, the `Add` the new one.
#[test]
fn main_update_keeping_join_value_emits_only_the_replace_pair() {
    let (mut ivm, storage) = engine();
    storage.apply(&user(7, "meera"));
    ivm.register_query("q", tickets_users_query());
    write(&mut ivm, &storage, ticket(1, "OPEN", 7));

    let ops = write(
        &mut ivm,
        &storage,
        update(
            "tickets",
            1,
            &[
                ("status", "OPEN".into()),
                ("assigned_to", Value::Int(7)),
                ("reviewer", Value::Int(42)),
            ],
        ),
    );
    let [del, add] = ops.as_slice() else {
        panic!("expected exactly the main replace pair, got {:?}", tags(&ops));
    };
    assert_eq!(del.part, QueryPart::Main);
    assert_eq!(add.part, QueryPart::Main);
    assert!(
        matches!(&del.op, DataFrameOperation::Delete(_, row) if !row.data.contains_key("reviewer"))
    );
    assert!(matches!(&add.op, DataFrameOperation::Add(_, row) if row.data["reviewer"] == Value::Int(42)));
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 1);
}

/// A referenced sub row moving to an unreferenced value stops matching the
/// registered `IN` condition — membership routing delivers the `Delete`
/// natively, and the main row stays.
#[test]
fn sub_move_to_unreferenced_value_emits_delete() {
    let (mut ivm, storage) = engine();
    storage.apply(&member(1, 5));
    storage.apply(&member(2, 5));
    ivm.register_query("q", tickets_members_query());
    write(&mut ivm, &storage, team_ticket(10, 5));

    let ops = write(&mut ivm, &storage, insert("members", 1, &[("team", Value::Int(9))]));
    assert_eq!(tags(&ops), vec!["q/join0/del:Int(1)"]);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Main), 1);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 1);
}

/// A sub row moving between two referenced values is replaced in place —
/// the `Delete(old)` + `Add(new)` pair, still one row, `right` counts
/// moved by the pair's two images.
#[test]
fn sub_move_between_referenced_values_refreshes() {
    let (mut ivm, storage) = engine();
    storage.apply(&member(1, 5));
    storage.apply(&member(2, 6));
    ivm.register_query("q", tickets_members_query());
    write(&mut ivm, &storage, team_ticket(10, 5));
    write(&mut ivm, &storage, team_ticket(11, 6));

    let ops = write(&mut ivm, &storage, insert("members", 1, &[("team", Value::Int(6))]));
    assert_eq!(tags(&ops), vec!["q/join0/add:Int(1)", "q/join0/del:Int(1)"]);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 2);
}

/// One user row referenced by two joins of the same query: deleting it
/// reaches BOTH join parts natively — each part's registered `IN`
/// condition held the row independently. The main row stays (left join).
#[test]
fn shared_sub_row_across_two_joins_deletes_both_parts() {
    let (mut ivm, storage) = engine();
    storage.apply(&user(7, "meera"));
    ivm.register_query("q", two_users_joins_query());
    let ops = write(
        &mut ivm,
        &storage,
        insert(
            "tickets",
            1,
            &[
                ("status", "OPEN".into()),
                ("assigned_to", Value::Int(7)),
                ("reviewer", Value::Int(7)),
            ],
        ),
    );
    assert_eq!(
        tags(&ops),
        vec![
            "q/join0/add:Int(7)",
            "q/join1/add:Int(7)",
            "q/main/add:Int(1)"
        ]
    );

    let ops = write(&mut ivm, &storage, delete("users", 7));
    assert_eq!(tags(&ops), vec!["q/join0/del:Int(7)", "q/join1/del:Int(7)"]);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Main), 1);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 0);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(1)), 0);
}

/// A second subscription with the same spec gets its own full snapshot,
/// served from the shared per-table frames — and each subscription keeps
/// routing independently.
#[test]
fn second_subscription_shares_rows_and_gets_its_own_snapshot() {
    let (mut ivm, storage) = engine();
    storage.apply(&user(7, "meera"));
    ivm.register_query("q1", tickets_users_query());
    write(&mut ivm, &storage, ticket(1, "OPEN", 7));

    let snapshot = ivm.register_query("q2", tickets_users_query());
    assert_eq!(
        tags(&snapshot),
        vec!["q2/join0/add:Int(7)", "q2/main/add:Int(1)"]
    );

    let ops = write(&mut ivm, &storage, ticket(2, "OPEN", 7));
    assert_eq!(tags(&ops), vec!["q1/main/add:Int(2)", "q2/main/add:Int(2)"]);
    ivm.unregister_query("q1");
    assert_eq!(frame_len(&ivm, "q2", QueryPart::Main), 2);
    assert_eq!(frame_len(&ivm, "q2", QueryPart::Join(0)), 1);
}

/// Regression (review): self-join — `people LEFT JOIN people ON mgr = id`.
/// One write is then both a main-part and a sub-part event. The sub op is
/// captured against the pre-write `IN` list, so it must be forwarded
/// BEFORE the main side's reference moves prune the frame — otherwise a
/// stale `Add` resurrects a pruned row on the client. The client replay
/// (ops applied in emitted order) must converge to the engine's frames.
#[test]
fn self_join_write_converges_for_the_client() {
    let people = DbTable::new(
        "people",
        ["id"],
        vec![
            DbColumn::new("id", ValueType::Int),
            DbColumn::new("mgr", ValueType::Int),
        ],
    );
    let spec = MultiTableReadQuery {
        main_table: query(&people, Where::AND(vec![])),
        left_joins: vec![LeftJoin {
            sub_table: query(&people, Where::AND(vec![])),
            main_table_column: "mgr".into(),
            sub_table_column: "id".into(),
        }],
    };
    let (mut ivm, storage) = engine();
    ivm.register_query("q", spec);

    let ops = write(
        &mut ivm,
        &storage,
        insert("people", 2, &[("mgr", Value::Int(2))]),
    );
    assert_eq!(tags(&ops), vec!["q/join0/add:Int(2)", "q/main/add:Int(2)"]);

    let ops = write(
        &mut ivm,
        &storage,
        update("people", 2, &[("mgr", Value::Int(1))]),
    );
    let join0_key2: Vec<&MultiTableUpdate> = ops
        .iter()
        .filter(|update| {
            update.part == QueryPart::Join(0)
                && update.op.key().pkey_value["id"] == Value::Int(2)
        })
        .collect();
    assert!(
        matches!(
            join0_key2.last().map(|update| &update.op),
            Some(DataFrameOperation::Delete(..))
        ),
        "client must end WITHOUT row 2 on the sub side, got ops: {:?}",
        tags(&ops)
    );
    assert!(ops.iter().all(|update| update.table == "people"));
    assert_eq!(frame_len(&ivm, "q", QueryPart::Main), 1);
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 0);
}

/// A finite `limit` on a join's sub side is normalized away — it has no
/// SQL meaning for a join, so every referenced value's sub rows arrive
/// regardless of it.
#[test]
fn sub_table_limit_is_normalized_away() {
    let (mut ivm, storage) = engine();
    for id in 1..=3 {
        storage.apply(&member(id, 5));
    }
    let mut spec = tickets_members_query();
    spec.left_joins[0].sub_table.limit = 1;
    ivm.register_query("q", spec);

    let ops = write(&mut ivm, &storage, team_ticket(10, 5));
    assert_eq!(
        tags(&ops),
        vec![
            "q/join0/add:Int(1)",
            "q/join0/add:Int(2)",
            "q/join0/add:Int(3)",
            "q/main/add:Int(10)"
        ]
    );
    assert_eq!(frame_len(&ivm, "q", QueryPart::Join(0)), 3);
}

/// Every update names the table its operation lands on: the main table for
/// the main part, the join's sub table for each join part.
#[test]
fn updates_carry_their_destination_table() {
    let (mut ivm, storage) = engine();
    storage.apply(&user(7, "meera"));
    storage.apply(&ticket(1, "OPEN", 7));

    let snapshot = ivm.register_query("q", tickets_users_query());
    assert!(snapshot.iter().any(|update| update.part == QueryPart::Main));
    assert!(snapshot
        .iter()
        .any(|update| update.part == QueryPart::Join(0)));
    for update in &snapshot {
        match update.part {
            QueryPart::Main => assert_eq!(update.table, "tickets"),
            QueryPart::Join(_) => assert_eq!(update.table, "users"),
        }
    }
}

/// Unregistering removes every part frame and stops routing entirely.
#[test]
fn unregister_removes_all_parts() {
    let (mut ivm, storage) = engine();
    storage.apply(&user(7, "meera"));
    ivm.register_query("q", tickets_users_query());
    write(&mut ivm, &storage, ticket(1, "OPEN", 7));

    ivm.unregister_query("q");
    assert!(ivm.rows_for("q", QueryPart::Main).is_none());
    assert!(ivm.rows_for("q", QueryPart::Join(0)).is_none());
    let ops = write(&mut ivm, &storage, ticket(2, "OPEN", 7));
    assert!(ops.is_empty());
}
