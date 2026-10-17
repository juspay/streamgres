//! Composite correlations from Xyne Spaces' `sourceItemLinks` and
//! `sameTargetLinks` relationships, through Zero AST, planning, and IVM.

use std::rc::Rc;
use std::time::Duration;

use serde_json::{Value as Json, json};
use streamgres::client::{ast, plan};
use streamgres::ivm::{MultiTableIVM, QueryPart};
use streamgres::model::*;
use streamgres::sync::pg::sql::count_sql;
use streamgres::sync::{Local, MemoryStorage, Storage};

fn catalog() -> Catalog {
    Catalog::new([DbTable::new(
        "sdlc_entity_links",
        ["id"],
        vec![
            DbColumn::new("id", ValueType::String),
            DbColumn::new("sourceType", ValueType::String),
            DbColumn::new("sourceId", ValueType::String),
            DbColumn::new("targetType", ValueType::String),
            DbColumn::new("targetId", ValueType::String),
            DbColumn::new("relationType", ValueType::String),
            DbColumn::new("channelId", ValueType::String),
        ],
    )])
}

fn related(parent: &[&str], child: &[&str], filter: Option<Json>, nested: Vec<Json>) -> Json {
    let mut subquery = json!({"table": "sdlc_entity_links", "related": nested});
    if let Some(filter) = filter {
        subquery["where"] = filter;
    }
    json!({
        "correlation": {"parentField": parent, "childField": child},
        "subquery": subquery
    })
}

fn eq(column: &str, value: &str) -> Json {
    json!({"type": "simple", "op": "=", "left": {"type": "column", "name": column},
        "right": {"type": "literal", "value": value}})
}

fn exists(related: Json) -> Json {
    json!({"type": "correlatedSubquery", "op": "EXISTS", "related": related})
}

fn translate(value: Json) -> ast::Translated {
    ast::translate(&serde_json::from_value(value).unwrap(), &catalog()).unwrap()
}

fn row(
    id: &str,
    source_type: &str,
    source_id: &str,
    target_type: &str,
    target_id: &str,
    relation: &str,
) -> WriteQuery {
    row_values(
        id,
        source_type.into(),
        source_id.into(),
        target_type.into(),
        target_id.into(),
        relation,
        "ch",
    )
}

fn row_values(
    id: &str,
    source_type: Value,
    source_id: Value,
    target_type: Value,
    target_id: Value,
    relation: &str,
    channel: &str,
) -> WriteQuery {
    let key = DataFrameKey::new([("id", Value::from(id))]);
    WriteQuery::INSERT(InsertQuery {
        table: "sdlc_entity_links".into(),
        pkey_value: key,
        record: DataFrameRow::new([
            ("id", Value::from(id)),
            ("sourceType", source_type),
            ("sourceId", source_id),
            ("targetType", target_type),
            ("targetId", target_id),
            ("relationType", Value::from(relation)),
            ("channelId", Value::from(channel)),
        ]),
    })
}

fn row_in_channel(
    id: &str,
    source_type: &str,
    source_id: &str,
    target_type: &str,
    target_id: &str,
    relation: &str,
    channel: &str,
) -> WriteQuery {
    row_values(
        id,
        source_type.into(),
        source_id.into(),
        target_type.into(),
        target_id.into(),
        relation,
        channel,
    )
}

fn update(insert: WriteQuery) -> WriteQuery {
    let WriteQuery::INSERT(insert) = insert else {
        panic!("full row insert expected")
    };
    WriteQuery::UPDATE(UpdateQuery {
        table: insert.table,
        pkey_value: insert.pkey_value,
        record: insert.record,
    })
}

fn delete(id: &str) -> WriteQuery {
    WriteQuery::DELETE(DeleteQuery {
        table: "sdlc_entity_links".into(),
        pkey_value: DataFrameKey::new([("id", Value::from(id))]),
    })
}

fn ids(ivm: &Local<MultiTableIVM, MemoryStorage>, sub: SubId, part: QueryPart) -> Vec<String> {
    let mut ids: Vec<_> = ivm
        .engine()
        .rows_for(sub, part)
        .unwrap_or_default()
        .keys()
        .map(|key| match &key.pkey_value["id"] {
            Value::String(id) => id.to_string(),
            other => panic!("unexpected id: {other:?}"),
        })
        .collect();
    ids.sort();
    ids
}

fn write(
    ivm: &mut Local<MultiTableIVM, MemoryStorage>,
    storage: &MemoryStorage,
    write: WriteQuery,
) {
    storage.apply(&write);
    ivm.incremental_update(&write);
}

fn setup() -> (Local<MultiTableIVM, MemoryStorage>, Rc<MemoryStorage>) {
    let storage = Rc::new(MemoryStorage::new());
    (Local::new(MultiTableIVM::new(), storage.clone()), storage)
}

#[tokio::test]
async fn xyne_nested_same_target_and_source_item_links() {
    // getSdlcTrackItems: .related('sameTargetLinks', above =>
    // above.related('sourceItemLinks', filing => ...)).
    let filing = related(
        &["sourceType", "sourceId"],
        &["targetType", "targetId"],
        Some(eq("relationType", "CONTAINMENT")),
        vec![],
    );
    let above = related(
        &["targetType", "targetId"],
        &["targetType", "targetId"],
        Some(eq("relationType", "FOLDER_FLAT")),
        vec![filing],
    );
    let translated = translate(json!({"table": "sdlc_entity_links",
        "where": eq("id", "base"), "related": [above]}));
    assert_eq!(translated.query.joins[0].on.pairs().len(), 2);
    assert_eq!(translated.query.joins[0].sub.joins[0].on.pairs().len(), 2);

    let (mut ivm, storage) = setup();
    for write_row in [
        row("base", "FOLDER", "root", "CANVAS", "a", "CONTAINMENT"),
        row("above", "TRACK", "t", "CANVAS", "a", "FOLDER_FLAT"),
        row("wrong_type", "TRACK", "t", "FILE", "a", "FOLDER_FLAT"),
        row("filing", "FOLDER", "root", "TRACK", "t", "CONTAINMENT"),
        row(
            "wrong_id",
            "FOLDER",
            "root",
            "TRACK",
            "other",
            "CONTAINMENT",
        ),
    ] {
        storage.apply(&write_row);
    }
    let cache = plan::PlanCache::new(Duration::from_secs(30), 16, Duration::from_secs(30));
    let planned = plan::plan(
        "getSdlcTrackItems",
        translated,
        plan::Policy {
            limit: 1000,
            preferred: plan::Side::Parent,
        },
        &cache,
        &*storage,
    )
    .await
    .unwrap();
    let (sub, _) = ivm.register_query(planned.query);
    assert_eq!(ids(&ivm, sub, QueryPart::main()), ["base"]);
    assert_eq!(ids(&ivm, sub, QueryPart::join(0)), ["above"]);
    assert_eq!(ids(&ivm, sub, QueryPart::new(&[0, 0])), ["filing"]);

    write(
        &mut ivm,
        &storage,
        row("new_above", "TRACK", "u", "CANVAS", "a", "FOLDER_FLAT"),
    );
    assert_eq!(ids(&ivm, sub, QueryPart::join(0)), ["above", "new_above"]);
    write(
        &mut ivm,
        &storage,
        row("new_filing", "FOLDER", "root", "TRACK", "u", "CONTAINMENT"),
    );
    assert_eq!(
        ids(&ivm, sub, QueryPart::new(&[0, 0])),
        ["filing", "new_filing"]
    );
    write(&mut ivm, &storage, delete("new_above"));
    assert_eq!(ids(&ivm, sub, QueryPart::join(0)), ["above"]);
    assert_eq!(ids(&ivm, sub, QueryPart::new(&[0, 0])), ["filing"]);
}

#[tokio::test]
async fn xyne_source_item_exists_under_or_checks_both_columns() {
    // queries.ts's discussion filter: source is a track, or a canvas
    // whose sourceItemLinks has the requested track and relation type.
    let linked = related(
        &["sourceType", "sourceId"],
        &["targetType", "targetId"],
        Some(
            json!({"type": "and", "conditions": [eq("sourceType", "TRACK"),
            eq("sourceId", "t"), eq("relationType", "TRACK_FLAT")]}),
        ),
        vec![],
    );
    let translated = translate(json!({"table": "sdlc_entity_links",
    "where": {"type": "and", "conditions": [
        eq("relationType", "DISCUSSION"),
        {"type": "or", "conditions": [
            {"type": "and", "conditions": [eq("sourceType", "TRACK"), eq("sourceId", "t")]},
            {"type": "and", "conditions": [eq("sourceType", "CANVAS"), exists(linked)]}
        ]}
    ]}}));
    let sql = count_sql(&translated.query, &catalog(), 1000).unwrap();
    assert!(
        sql.contains("t1.\"targetType\" = t0.\"sourceType\""),
        "{sql}"
    );
    assert!(sql.contains("t1.\"targetId\" = t0.\"sourceId\""), "{sql}");
    assert!(sql.contains(" OR "), "{sql}");
    let (mut ivm, storage) = setup();
    for write_row in [
        row("direct", "TRACK", "t", "CONVERSATION", "c1", "DISCUSSION"),
        row("canvas", "CANVAS", "a", "CONVERSATION", "c2", "DISCUSSION"),
        row("link", "TRACK", "t", "CANVAS", "a", "TRACK_FLAT"),
        row("wrong_type", "TRACK", "t", "FILE", "a", "TRACK_FLAT"),
    ] {
        storage.apply(&write_row);
    }
    assert_eq!(storage.count(&translated.query, 1000).await.unwrap(), 2);
    let cache = plan::PlanCache::new(Duration::from_secs(30), 16, Duration::from_secs(30));
    let planned = plan::plan(
        "discussion",
        translated,
        plan::Policy {
            limit: 1000,
            preferred: plan::Side::Parent,
        },
        &cache,
        &*storage,
    )
    .await
    .unwrap();
    let (sub, _) = ivm.register_query(planned.query);
    assert_eq!(ids(&ivm, sub, QueryPart::main()), ["canvas", "direct"]);
    assert_eq!(ids(&ivm, sub, QueryPart::join(0)), ["link"]);
    write(
        &mut ivm,
        &storage,
        row(
            "new_canvas",
            "CANVAS",
            "b",
            "CONVERSATION",
            "c3",
            "DISCUSSION",
        ),
    );
    assert_eq!(ids(&ivm, sub, QueryPart::main()), ["canvas", "direct"]);
    write(
        &mut ivm,
        &storage,
        row("new_link", "TRACK", "t", "CANVAS", "b", "TRACK_FLAT"),
    );
    assert_eq!(
        ids(&ivm, sub, QueryPart::main()),
        ["canvas", "direct", "new_canvas"]
    );
    write(&mut ivm, &storage, delete("new_link"));
    assert_eq!(ids(&ivm, sub, QueryPart::main()), ["canvas", "direct"]);
}

#[tokio::test]
async fn composite_inner_join_tracks_both_columns_with_either_driver() {
    for driver in [Driver::Main, Driver::Sub] {
        let mut query = translate(json!({"table": "sdlc_entity_links",
        "where": {"type": "and", "conditions": [
            eq("relationType", "ROOT"),
            exists(related(&["sourceType", "sourceId"], &["targetType", "targetId"],
                Some(eq("relationType", "EDGE")), vec![]))
        ]}}))
        .query;
        query.joins[0].driver = driver;
        let (mut ivm, storage) = setup();
        for image in [
            row("p1", "CANVAS", "a", "NONE", "", "ROOT"),
            row("p2", "CANVAS", "b", "NONE", "", "ROOT"),
            row("p3", "FILE", "a", "NONE", "", "ROOT"),
            row_values(
                "p4",
                "CANVAS".into(),
                Value::Null,
                "NONE".into(),
                "".into(),
                "ROOT",
                "ch",
            ),
            row("c1", "TRACK", "t", "CANVAS", "a", "EDGE"),
            row("c2", "TRACK", "t", "FILE", "b", "EDGE"),
            row_values(
                "c3",
                "TRACK".into(),
                "t".into(),
                "CANVAS".into(),
                Value::Null,
                "EDGE",
                "ch",
            ),
        ] {
            storage.apply(&image);
        }
        assert_eq!(storage.count(&query, 100).await.unwrap(), 1, "{driver:?}");
        let (sub, _) = ivm.register_query(query);
        assert_eq!(ids(&ivm, sub, QueryPart::main()), ["p1"], "{driver:?}");

        write(
            &mut ivm,
            &storage,
            update(row("c2", "TRACK", "t", "CANVAS", "b", "EDGE")),
        );
        assert_eq!(
            ids(&ivm, sub, QueryPart::main()),
            ["p1", "p2"],
            "{driver:?}"
        );
        write(
            &mut ivm,
            &storage,
            update(row("c1", "TRACK", "t", "CANVAS", "x", "EDGE")),
        );
        assert_eq!(ids(&ivm, sub, QueryPart::main()), ["p2"], "{driver:?}");
        write(
            &mut ivm,
            &storage,
            update(row("p3", "CANVAS", "b", "NONE", "", "ROOT")),
        );
        assert_eq!(
            ids(&ivm, sub, QueryPart::main()),
            ["p2", "p3"],
            "{driver:?}"
        );
        write(&mut ivm, &storage, delete("c2"));
        assert!(ids(&ivm, sub, QueryPart::main()).is_empty(), "{driver:?}");
        write(
            &mut ivm,
            &storage,
            update(row("c1", "TRACK", "t", "CANVAS", "a", "EDGE")),
        );
        assert_eq!(ids(&ivm, sub, QueryPart::main()), ["p1"], "{driver:?}");
        write(
            &mut ivm,
            &storage,
            update(row("p4", "CANVAS", "a", "NONE", "", "ROOT")),
        );
        assert_eq!(
            ids(&ivm, sub, QueryPart::main()),
            ["p1", "p4"],
            "{driver:?}"
        );
    }
}

#[test]
fn planner_cache_distinguishes_the_complete_correlation() {
    let base = translate(json!({"table": "sdlc_entity_links", "related": [
        related(&["sourceType", "sourceId"], &["targetType", "targetId"], None, vec![])
    ]}))
    .query;
    let changed = translate(json!({"table": "sdlc_entity_links", "related": [
        related(&["sourceType", "sourceId"], &["targetType", "sourceId"], None, vec![])
    ]}))
    .query;
    let cache = plan::PlanCache::new(Duration::from_secs(30), 16, Duration::from_secs(30));
    cache.remember("links", &base);
    assert!(cache.by_name("links", &base).is_some());
    assert!(cache.by_name("links", &changed).is_none());
}

#[test]
fn two_exists_on_a_to_many_composite_relation_need_two_witnesses() {
    let mut query = translate(json!({"table": "sdlc_entity_links",
    "where": {"type": "and", "conditions": [
        eq("relationType", "ROOT"),
        exists(related(&["sourceType", "sourceId"], &["targetType", "targetId"],
            Some(eq("relationType", "EDGE_A")), vec![])),
        exists(related(&["sourceType", "sourceId"], &["targetType", "targetId"],
            Some(eq("relationType", "EDGE_B")), vec![]))
    ]}}))
    .query;
    assert_eq!(
        query.joins.len(),
        2,
        "the child id is the primary key, so each correlation is to-many"
    );
    for join in &mut query.joins {
        join.driver = Driver::Main;
    }
    let (mut ivm, storage) = setup();
    storage.apply(&row("parent", "CANVAS", "a", "NONE", "", "ROOT"));
    storage.apply(&row("a", "TRACK", "t", "CANVAS", "a", "EDGE_A"));
    let (sub, _) = ivm.register_query(query);
    assert!(ids(&ivm, sub, QueryPart::main()).is_empty());
    write(
        &mut ivm,
        &storage,
        row("b", "TRACK", "t", "CANVAS", "a", "EDGE_B"),
    );
    assert_eq!(ids(&ivm, sub, QueryPart::main()), ["parent"]);
    write(
        &mut ivm,
        &storage,
        update(row("b", "TRACK", "t", "FILE", "a", "EDGE_B")),
    );
    assert!(ids(&ivm, sub, QueryPart::main()).is_empty());
}

#[test]
fn composite_outer_join_limits_each_complete_parent_key() {
    let mut edge = related(
        &["sourceType", "sourceId"],
        &["targetType", "targetId"],
        Some(eq("relationType", "EDGE")),
        vec![],
    );
    edge["subquery"]["orderBy"] = json!([["id", "asc"]]);
    edge["subquery"]["limit"] = json!(1);
    let query = translate(json!({"table": "sdlc_entity_links",
        "where": eq("relationType", "ROOT"), "related": [edge]}))
    .query;
    let (mut ivm, storage) = setup();
    for image in [
        row("parent_canvas", "CANVAS", "a", "NONE", "", "ROOT"),
        row("parent_file", "FILE", "a", "NONE", "", "ROOT"),
        row("a1", "TRACK", "t", "CANVAS", "a", "EDGE"),
        row("a2", "TRACK", "t", "CANVAS", "a", "EDGE"),
        row("f1", "TRACK", "t", "FILE", "a", "EDGE"),
    ] {
        storage.apply(&image);
    }
    let (sub, _) = ivm.register_query(query);
    assert_eq!(
        ids(&ivm, sub, QueryPart::main()),
        ["parent_canvas", "parent_file"]
    );
    assert_eq!(ids(&ivm, sub, QueryPart::join(0)), ["a1", "f1"]);
    write(&mut ivm, &storage, delete("a1"));
    assert_eq!(ids(&ivm, sub, QueryPart::join(0)), ["a2", "f1"]);
    write(
        &mut ivm,
        &storage,
        row("a0", "TRACK", "t", "CANVAS", "a", "EDGE"),
    );
    assert_eq!(ids(&ivm, sub, QueryPart::join(0)), ["a0", "f1"]);
    write(
        &mut ivm,
        &storage,
        update(row("f1", "TRACK", "t", "CANVAS", "a", "EDGE")),
    );
    assert_eq!(ids(&ivm, sub, QueryPart::join(0)), ["a0"]);
    assert_eq!(
        ids(&ivm, sub, QueryPart::main()),
        ["parent_canvas", "parent_file"]
    );
}

#[test]
fn sub_driven_composite_fan_respects_global_page_and_other_gate() {
    let node = |filter, limit| {
        SingleTableReadQuery::new(
            "sdlc_entity_links",
            filter,
            OrderBy::new("id", Order::ASC),
            limit,
        )
    };
    let child = MultiTableReadQuery::single(node(
        Where::condition("relationType", ComparisonOperator::EQ, "EDGE"),
        u32::MAX,
    ));
    let gate = MultiTableReadQuery::single(node(
        Where::condition("relationType", ComparisonOperator::EQ, "GATE"),
        u32::MAX,
    ));
    let filter = Where::AND(vec![
        Where::condition("relationType", ComparisonOperator::EQ, "ROOT"),
        Where::exists("channelId", 0),
    ]);
    let query = MultiTableReadQuery::new(
        node(filter, 2),
        vec![
            Join::with_columns(
                child,
                [
                    ("sourceType".into(), "targetType".into()),
                    ("sourceId".into(), "targetId".into()),
                ],
                Driver::Sub,
                false,
            )
            .unwrap(),
            Join::inner_from_main(gate, "channelId", "channelId"),
        ],
    );
    let (mut ivm, storage) = setup();
    for image in [
        row_in_channel("p1", "CANVAS", "a", "NONE", "", "ROOT", "red"),
        row_in_channel("p2", "FILE", "a", "NONE", "", "ROOT", "blue"),
        row_in_channel("p3", "CANVAS", "b", "NONE", "", "ROOT", "green"),
        row_in_channel("p4", "FILE", "b", "NONE", "", "ROOT", "yellow"),
        row_in_channel("edge_a", "TRACK", "t", "CANVAS", "a", "EDGE", "x"),
        row_in_channel("edge_b", "TRACK", "t", "CANVAS", "b", "EDGE", "x"),
        row_in_channel("edge_file_a", "TRACK", "t", "FILE", "a", "EDGE", "x"),
        row_in_channel("edge_file_b", "TRACK", "t", "FILE", "b", "EDGE", "x"),
        row_in_channel("gate_p1", "GATE", "1", "NONE", "", "GATE", "red"),
        row_in_channel("gate_p3", "GATE", "3", "NONE", "", "GATE", "green"),
        row_in_channel("gate_p4", "GATE", "4", "NONE", "", "GATE", "yellow"),
    ] {
        storage.apply(&image);
    }

    let (sub, _) = ivm.register_query(query);
    assert_eq!(ids(&ivm, sub, QueryPart::main()), ["p1", "p3"]);
    assert_eq!(
        ids(&ivm, sub, QueryPart::join(0)),
        ["edge_a", "edge_b", "edge_file_a", "edge_file_b"]
    );

    // Removing a gate lets the next eligible root row enter the global page.
    write(&mut ivm, &storage, delete("gate_p1"));
    assert_eq!(ids(&ivm, sub, QueryPart::main()), ["p3", "p4"]);

    // A new earlier eligible row displaces the last row in the page again.
    write(
        &mut ivm,
        &storage,
        row_in_channel("gate_p1", "GATE", "1", "NONE", "", "GATE", "red"),
    );
    assert_eq!(ids(&ivm, sub, QueryPart::main()), ["p1", "p3"]);
}

#[test]
fn three_column_right_outer_join_matches_the_complete_tuple() {
    let node = |filter| {
        SingleTableReadQuery::new(
            "sdlc_entity_links",
            filter,
            OrderBy::new("id", Order::ASC),
            u32::MAX,
        )
    };
    let child = MultiTableReadQuery::single(node(Where::condition(
        "relationType",
        ComparisonOperator::EQ,
        "EDGE",
    )));
    let join = Join::with_columns(
        child,
        [
            ("sourceType".into(), "targetType".into()),
            ("sourceId".into(), "targetId".into()),
            ("channelId".into(), "channelId".into()),
        ],
        Driver::Sub,
        false,
    )
    .unwrap();
    let query = MultiTableReadQuery::new(
        node(Where::condition(
            "relationType",
            ComparisonOperator::EQ,
            "ROOT",
        )),
        vec![join],
    );
    let (mut ivm, storage) = setup();
    for image in [
        row_in_channel("parent_a", "CANVAS", "a", "NONE", "", "ROOT", "ch"),
        row_in_channel("parent_b", "CANVAS", "a", "NONE", "", "ROOT", "other"),
        row_in_channel("edge_a", "TRACK", "t", "CANVAS", "a", "EDGE", "ch"),
        row_in_channel("edge_b", "TRACK", "t", "CANVAS", "a", "EDGE", "third"),
    ] {
        storage.apply(&image);
    }
    let (sub, _) = ivm.register_query(query);
    assert_eq!(ids(&ivm, sub, QueryPart::join(0)), ["edge_a", "edge_b"]);
    assert_eq!(ids(&ivm, sub, QueryPart::main()), ["parent_a"]);
    write(
        &mut ivm,
        &storage,
        update(row_in_channel(
            "parent_b", "CANVAS", "a", "NONE", "", "ROOT", "third",
        )),
    );
    assert_eq!(ids(&ivm, sub, QueryPart::main()), ["parent_a", "parent_b"]);
    write(&mut ivm, &storage, delete("edge_b"));
    assert_eq!(ids(&ivm, sub, QueryPart::join(0)), ["edge_a"]);
    assert_eq!(ids(&ivm, sub, QueryPart::main()), ["parent_a"]);
}

#[test]
fn exists_merges_only_when_composite_correlation_covers_child_primary_key() {
    let first = exists(related(
        &["sourceType", "sourceId"],
        &["targetType", "targetId"],
        Some(eq("sourceType", "TRACK")),
        vec![],
    ));
    let second = exists(related(
        &["sourceType", "sourceId"],
        &["targetType", "targetId"],
        Some(eq("relationType", "TRACK_FLAT")),
        vec![],
    ));
    let ast = serde_json::from_value(json!({"table": "sdlc_entity_links",
        "where": {"type": "and", "conditions": [first, second]}}))
    .unwrap();
    let to_many = ast::translate(&ast, &catalog()).unwrap();
    assert_eq!(to_many.query.joins.len(), 2);

    let one_catalog = Catalog::new([DbTable::new(
        "sdlc_entity_links",
        ["targetType", "targetId"],
        vec![
            DbColumn::new("sourceType", ValueType::String),
            DbColumn::new("sourceId", ValueType::String),
            DbColumn::new("targetType", ValueType::String),
            DbColumn::new("targetId", ValueType::String),
            DbColumn::new("relationType", ValueType::String),
        ],
    )]);
    let merged = ast::translate(&ast, &one_catalog).unwrap();
    assert_eq!(merged.query.joins.len(), 1);
    assert_eq!(merged.query.joins[0].on.pairs().len(), 2);
    assert!(matches!(&merged.query.joins[0].sub.main_table.filter,
        Where::AND(conditions) if conditions.len() == 2));
}

#[test]
fn composite_correlation_rejects_missing_and_mismatched_columns() {
    for (parent, child) in [
        (json!([]), json!([])),
        (json!(["sourceType", "sourceId"]), json!(["targetType"])),
        (
            json!(["sourceType", "missing"]),
            json!(["targetType", "targetId"]),
        ),
    ] {
        let value = json!({"table": "sdlc_entity_links", "related": [{
            "correlation": {"parentField": parent, "childField": child},
            "subquery": {"table": "sdlc_entity_links"}}]});
        let ast = serde_json::from_value(value).unwrap();
        assert!(ast::translate(&ast, &catalog()).is_err());
    }
}
