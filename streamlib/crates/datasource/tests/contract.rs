//! Contract tests on the memory source, plus broken sources the kit must catch.

use std::sync::Arc;
use std::time::Duration;

use streamlib_datasource::testing::{Fault, MemorySource, Mirror};
use streamlib_datasource::{
    Catalog, Change, Checked, Column, Dir, Filter, Op, Pred, Query, Row, SourceError, Table, Value,
    ValueType,
};

const T: &str = "items";
const SETTLE: Duration = Duration::from_secs(5);

fn catalog() -> Catalog {
    let col = |name: &str, ty| Column {
        name: name.into(),
        ty,
    };
    Catalog::new([Table {
        name: T.into(),
        columns: vec![
            col("id", ValueType::Int),
            col("status", ValueType::Text),
            col("body", ValueType::Text),
            col("n", ValueType::Int),
        ],
        key: vec!["id".into()],
    }])
}

fn row(id: i64, status: &str, body: &str, n: i64) -> Row {
    Row::from([
        ("id".into(), id.into()),
        ("status".into(), status.into()),
        ("body".into(), body.into()),
        ("n".into(), n.into()),
    ])
}

/// The row without `body`, like Postgres sends when a big column is unchanged.
fn without_body(mut r: Row) -> Row {
    r.remove("body");
    r
}

fn source(fault: Fault) -> Arc<Checked<MemorySource>> {
    let mem = MemorySource::new(catalog())
        .with_partial(&["body"])
        .with_fault(fault);
    Arc::new(Checked::new(mem))
}

fn open_items() -> Query {
    Query::table(T).filter(Filter::and(vec![Pred::cmp("status", Op::Eq, "open")]))
}

#[tokio::test]
async fn converges_under_concurrent_writes() {
    let src = source(Fault::None);
    for id in 0..20 {
        src.inner().insert(T, row(id, "open", "big", 0)).await;
    }
    let mut mirror = Mirror::start(src.clone(), open_items()).await.unwrap();

    let writer = {
        let src = src.clone();
        tokio::spawn(async move {
            let mut seed: u64 = 7;
            for i in 0..500 {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let id = (seed >> 33) as i64 % 40;
                let status = if seed & 1 == 0 { "open" } else { "closed" };
                let change = match (seed >> 8) % 4 {
                    0 => Change::Delete {
                        table: T.into(),
                        key: vec![id.into()],
                    },
                    1 => Change::Insert {
                        table: T.into(),
                        row: row(id, status, "big", i),
                    },
                    _ if src
                        .inner()
                        .live(T)
                        .iter()
                        .any(|r| r["id"] == Value::Int(id)) =>
                    {
                        // Keep the body unchanged so the update goes out without it.
                        let body = src
                            .inner()
                            .live(T)
                            .into_iter()
                            .find(|r| r["id"] == Value::Int(id))
                            .unwrap()["body"]
                            .clone();
                        let Value::Text(body) = body else {
                            unreachable!()
                        };
                        Change::Update {
                            table: T.into(),
                            old_key: None,
                            row: row(id, status, &body, i),
                        }
                    }
                    _ => Change::Insert {
                        table: T.into(),
                        row: row(id, status, "big", i),
                    },
                };
                src.inner().write(vec![change]).await;
                if i % 50 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        })
    };
    while !writer.is_finished() {
        mirror.pump(Duration::from_millis(5)).await.unwrap();
    }
    mirror.settle(SETTLE).await.unwrap();
    assert!(mirror.stats.txns >= 500);
}

#[tokio::test]
async fn partial_update_filled_from_held_row() {
    let src = source(Fault::None);
    src.inner().insert(T, row(1, "open", "big", 0)).await;
    let mut mirror = Mirror::start(src.clone(), open_items()).await.unwrap();

    src.inner().update(T, row(1, "open", "big", 5)).await;
    mirror.settle(SETTLE).await.unwrap();
    assert_eq!(mirror.view(), vec![row(1, "open", "big", 5)]);
    assert_eq!((mirror.stats.completed, mirror.stats.rereads), (1, 0));
}

/// Known bug 1: the filter uses the missing column and we don't hold the row.
#[tokio::test]
async fn filter_on_absent_column_rereads() {
    let src = source(Fault::None);
    src.inner().insert(T, row(1, "closed", "big", 0)).await;
    let query = Query::table(T).filter(Filter::and(vec![
        Pred::cmp("status", Op::Eq, "open"),
        Pred::cmp("body", Op::Eq, "big"),
    ]));
    let mut mirror = Mirror::start(src.clone(), query).await.unwrap();
    assert!(mirror.view().is_empty());

    src.inner().update(T, row(1, "open", "big", 1)).await;
    mirror.settle(SETTLE).await.unwrap();
    assert_eq!(mirror.view(), vec![row(1, "open", "big", 1)]);
    assert_eq!(mirror.stats.rereads, 1);
}

/// Known bug 2: the primary key changes while the big column is missing.
#[tokio::test]
async fn key_change_with_absent_column() {
    for held in [true, false] {
        let src = source(Fault::None);
        let status = if held { "open" } else { "closed" };
        src.inner().insert(T, row(1, status, "big", 0)).await;
        let mut mirror = Mirror::start(src.clone(), open_items()).await.unwrap();

        let moved = without_body(row(2, "open", "", 1));
        src.inner()
            .write(vec![Change::Update {
                table: T.into(),
                old_key: Some(vec![1.into()]),
                row: moved,
            }])
            .await;
        mirror.settle(SETTLE).await.unwrap();
        assert_eq!(
            mirror.view(),
            vec![row(2, "open", "big", 1)],
            "held = {held}"
        );
    }
}

#[tokio::test]
async fn deletes_by_key_and_truncate() {
    let src = source(Fault::None);
    for id in 0..3 {
        src.inner().insert(T, row(id, "open", "big", 0)).await;
    }
    let mut mirror = Mirror::start(src.clone(), open_items()).await.unwrap();

    src.inner().delete(T, vec![0.into()]).await;
    mirror.settle(SETTLE).await.unwrap();
    assert_eq!(mirror.view().len(), 2);

    src.inner()
        .write(vec![Change::Truncate { table: T.into() }])
        .await;
    mirror.settle(SETTLE).await.unwrap();
    assert!(mirror.view().is_empty());
}

#[tokio::test]
async fn ordered_limited_view() {
    let src = source(Fault::None);
    for id in 0..10 {
        src.inner()
            .insert(T, row(id, "open", "big", 100 - id))
            .await;
    }
    let query = open_items().order("n", Dir::Asc).limit(3);
    let mut mirror = Mirror::start(src.clone(), query).await.unwrap();
    src.inner().insert(T, row(50, "open", "big", -1)).await;
    mirror.settle(SETTLE).await.unwrap();
    let ids: Vec<_> = mirror.view().iter().map(|r| r["id"].clone()).collect();
    assert_eq!(ids, vec![Value::Int(50), Value::Int(9), Value::Int(8)]);
}

// Broken sources: each test checks the kit catches one.

fn is_contract<T>(r: Result<T, SourceError>) -> bool {
    matches!(r, Err(SourceError::Contract(_)))
}

#[tokio::test]
async fn catches_snapshot_ahead_of_advance() {
    use streamlib_datasource::Snapshots;
    let src = source(Fault::AheadOfAdvance);
    let _mirror = Mirror::start(src.clone(), open_items()).await.unwrap();
    // Written but not applied yet, so a read must not claim this position.
    src.inner().insert(T, row(1, "open", "big", 0)).await;
    assert!(is_contract(src.select(&open_items()).await));
}

#[tokio::test]
async fn catches_partial_insert() {
    let src = source(Fault::PartialInsert);
    let mut mirror = Mirror::start(src.clone(), open_items()).await.unwrap();
    src.inner().insert(T, row(1, "open", "big", 0)).await;
    assert!(is_contract(mirror.settle(SETTLE).await));
}

#[tokio::test]
async fn catches_stale_rows_with_honest_looking_label() {
    let src = source(Fault::StaleRows);
    let mut mirror = Mirror::start(src.clone(), open_items()).await.unwrap();
    src.inner().insert(T, row(1, "open", "big", 0)).await;
    assert!(is_contract(mirror.settle(SETTLE).await));
}

#[tokio::test]
async fn catches_select_before_advance() {
    let src = source(Fault::None);
    use streamlib_datasource::Snapshots;
    assert!(is_contract(src.select(&open_items()).await));
}
