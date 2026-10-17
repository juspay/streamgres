//! Regression checks for the default, in-memory count path. These keep
//! ordinary single-column joins beside EXISTS in AND and OR filters.

use streamgres::model::*;
use streamgres::sync::{MemoryStorage, Storage};

fn ticket(id: i64, status: &str, assigned_to: Value, points: i64) -> (DataFrameKey, DataFrameRow) {
    (
        DataFrameKey::new([("id", Value::Int(id))]),
        DataFrameRow::new([
            ("id", Value::Int(id)),
            ("status", Value::from(status)),
            ("assigned_to", assigned_to),
            ("points", Value::Int(points)),
        ]),
    )
}

fn user(id: i64, name: &str) -> (DataFrameKey, DataFrameRow) {
    (
        DataFrameKey::new([("id", Value::Int(id))]),
        DataFrameRow::new([("id", Value::Int(id)), ("name", Value::from(name))]),
    )
}

fn query(table: &str, filter: Where) -> SingleTableReadQuery {
    SingleTableReadQuery::new(table, filter, OrderBy::new("id", Order::ASC), u32::MAX)
}

#[tokio::test]
async fn ordinary_joins_and_exists_filters_count_the_expected_rows() {
    let storage = MemoryStorage::new();
    storage.load(
        &"tickets".into(),
        vec![
            ticket(1, "OPEN", Value::Int(7), 1),
            ticket(2, "OPEN", Value::Int(8), 3),
            ticket(3, "DONE", Value::Int(7), 3),
            ticket(4, "OPEN", Value::Null, 0),
        ],
    );
    storage.load(&"users".into(), vec![user(7, "meera"), user(8, "arjun")]);

    let open = Where::condition("status", ComparisonOperator::EQ, "OPEN");
    let alone = MultiTableReadQuery::single(query("tickets", open.clone()));
    assert_eq!(storage.count(&alone, 100).await.unwrap(), 3);
    assert_eq!(
        storage.count(&alone, 2).await.unwrap(),
        2,
        "the cap still applies"
    );

    let edge = |name: &str, inner: bool| {
        Join::new(
            MultiTableReadQuery::single(query(
                "users",
                Where::condition("name", ComparisonOperator::EQ, name),
            )),
            "assigned_to",
            "id",
            Driver::Main,
            inner,
        )
    };
    let left = MultiTableReadQuery::new(query("tickets", open.clone()), vec![edge("meera", false)]);
    assert_eq!(
        storage.count(&left, 100).await.unwrap(),
        3,
        "an outer join cannot narrow the count"
    );

    let unnamed =
        MultiTableReadQuery::new(query("tickets", open.clone()), vec![edge("meera", true)]);
    assert_eq!(
        storage.count(&unnamed, 100).await.unwrap(),
        1,
        "an unnamed inner edge is required"
    );

    let named = MultiTableReadQuery::new(
        query(
            "tickets",
            Where::AND(vec![open.clone(), Where::exists("assigned_to", 0)]),
        ),
        vec![edge("meera", true)],
    );
    assert_eq!(storage.count(&named, 100).await.unwrap(), 1);

    let or = MultiTableReadQuery::new(
        query(
            "tickets",
            Where::OR(vec![
                Where::condition("points", ComparisonOperator::EQ, 3),
                Where::exists("assigned_to", 0),
            ]),
        ),
        vec![edge("meera", true)],
    );
    assert_eq!(
        storage.count(&or, 100).await.unwrap(),
        3,
        "the ordinary branch admits rows without a matching user"
    );

    let no_user = MultiTableReadQuery::new(
        query(
            "tickets",
            Where::OR(vec![
                Where::condition("points", ComparisonOperator::EQ, 3),
                Where::exists("assigned_to", 0),
            ]),
        ),
        vec![edge("nobody", true)],
    );
    assert_eq!(
        storage.count(&no_user, 100).await.unwrap(),
        2,
        "a missing user does not make EXISTS true"
    );
}
