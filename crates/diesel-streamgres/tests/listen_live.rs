//! Callbacks on a query's changes against a running engine
//! ([`EngineClient::subscribe`]): the rows present, then each commit's
//! rows added, updated and removed, whoever writes them.
//!
//! Needs `STREAMGRES_S2S_URL` and `DATABASE_URL`, as `engine_live` does.
//! Skips itself otherwise.

mod transport;

use std::time::{Duration, Instant};

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel_streamgres::Change;
use diesel_streamgres::engine::EngineClient;
use tokio::sync::mpsc;

diesel::table! {
    sg_configs (id) {
        id -> Int4,
        key -> Text,
        value -> Text,
    }
}

/// A config row.
#[derive(Queryable, Selectable, Debug, PartialEq, Clone)]
#[diesel(table_name = sg_configs)]
struct Config {
    id: i32,
    key: String,
    value: String,
}

/// A connected engine client, the database's URL and a plain connection
/// for changes; `None`, and the test skipped, without an engine.
fn start() -> Option<(EngineClient, String, PgConnection)> {
    let engine = transport::client(diesel_streamgres::engine::Config::from_env())?;
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !engine.is_connected() {
        assert!(Instant::now() < deadline, "the engine did not connect");
        std::thread::sleep(Duration::from_millis(50));
    }
    let mut other = PgConnection::establish(&database).expect("connect");
    other
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS sg_configs (
                id serial PRIMARY KEY, key text NOT NULL UNIQUE, value text NOT NULL)",
        )
        .expect("schema");
    Some((engine, database, other))
}

/// The next change the callback was given, within a few seconds.
async fn next<T>(changes: &mut mpsc::UnboundedReceiver<Change<T>>) -> Change<T> {
    tokio::time::timeout(Duration::from_secs(10), changes.recv())
        .await
        .expect("a change within the wait")
        .expect("the subscription is running")
}

/// Whether the callback stays quiet for a moment.
async fn quiet<T: std::fmt::Debug>(changes: &mut mpsc::UnboundedReceiver<Change<T>>) {
    if let Ok(change) = tokio::time::timeout(Duration::from_millis(500), changes.recv()).await {
        panic!("an unexpected change: {change:?}");
    }
}

/// A filtered table: the rows matching when it starts are present; a row
/// inserted to match is added, one changed and still matching is
/// updated, one changed to no longer match or deleted is removed; a row
/// that never matches is never reported. Dropping the subscription lets
/// the engine stop following the query.
#[tokio::test(flavor = "multi_thread")]
async fn a_filtered_table_reports_its_rows_changes() {
    let Some((engine, database, mut other)) = start() else {
        return;
    };
    let tag = format!("listen-{}", std::process::id());
    let key = |name: &str| format!("{tag}-{name}");
    diesel::insert_into(sg_configs::table)
        .values((sg_configs::key.eq(key("a")), sg_configs::value.eq(&tag)))
        .execute(&mut other)
        .expect("insert a");

    let held = engine.subscribed();
    let (sender, mut changes) = mpsc::unbounded_channel();
    let subscription = engine
        .subscribe(
            &database,
            sg_configs::table
                .filter(sg_configs::value.eq(tag.clone()))
                .select(Config::as_select()),
            move |change: Change<Config>| {
                let sender = sender.clone();
                async move {
                    let _ = sender.send(change);
                }
            },
        )
        .await
        .expect("subscribed");
    assert_eq!(engine.subscribed(), held + 1);

    let first = next(&mut changes).await;
    let Change::Present(a) = first else {
        panic!("the row there first is present: {first:?}");
    };
    assert_eq!(
        (a.key.as_str(), a.value.as_str()),
        (key("a").as_str(), tag.as_str())
    );
    quiet(&mut changes).await;

    diesel::insert_into(sg_configs::table)
        .values((sg_configs::key.eq(key("b")), sg_configs::value.eq(&tag)))
        .execute(&mut other)
        .expect("insert b");
    let Change::Added(b) = next(&mut changes).await else {
        panic!("an inserted row is added");
    };
    assert_eq!(b.key, key("b"));

    diesel::insert_into(sg_configs::table)
        .values((
            sg_configs::key.eq(key("never")),
            sg_configs::value.eq("elsewhere"),
        ))
        .execute(&mut other)
        .expect("insert a row the filter leaves out");
    diesel::update(sg_configs::table.filter(sg_configs::id.eq(a.id)))
        .set(sg_configs::key.eq(key("a2")))
        .execute(&mut other)
        .expect("rename a");
    assert_eq!(
        next(&mut changes).await,
        Change::Updated {
            old: a.clone(),
            new: Config {
                key: key("a2"),
                ..a.clone()
            }
        },
        "a row changed and still matching is updated; the other is never reported"
    );

    diesel::update(sg_configs::table.filter(sg_configs::id.eq(b.id)))
        .set(sg_configs::value.eq("elsewhere"))
        .execute(&mut other)
        .expect("move b out");
    assert_eq!(
        next(&mut changes).await,
        Change::Removed(b.clone()),
        "moved out"
    );
    diesel::delete(sg_configs::table.filter(sg_configs::id.eq(a.id)))
        .execute(&mut other)
        .expect("delete a");
    let Change::Removed(gone) = next(&mut changes).await else {
        panic!("a deleted row is removed");
    };
    assert_eq!(gone.key, key("a2"), "as it was last");
    quiet(&mut changes).await;
    assert!(
        subscription.errors().is_empty(),
        "{:?}",
        subscription.errors()
    );
    assert_eq!(subscription.lagged(), 0);

    drop(subscription);
    assert_eq!(engine.subscribed(), held, "the engine lets the query go");
    diesel::delete(sg_configs::table.filter(sg_configs::key.like(format!("{tag}-%"))))
        .execute(&mut other)
        .expect("clean up");
}

/// A count is compared whole after each commit: it starts present, and
/// each commit that changes it updates it.
#[tokio::test(flavor = "multi_thread")]
async fn a_count_is_updated_as_rows_come_and_go() {
    let Some((engine, database, mut other)) = start() else {
        return;
    };
    let tag = format!("listen-count-{}", std::process::id());
    let (sender, mut changes) = mpsc::unbounded_channel();
    let subscription = engine
        .subscribe(
            &database,
            sg_configs::table
                .filter(sg_configs::value.eq(tag.clone()))
                .count(),
            move |change: Change<i64>| {
                let sender = sender.clone();
                async move {
                    let _ = sender.send(change);
                }
            },
        )
        .await
        .expect("subscribed");
    assert_eq!(next(&mut changes).await, Change::Present(0));
    for (at, name) in ["x", "y"].iter().enumerate() {
        diesel::insert_into(sg_configs::table)
            .values((
                sg_configs::key.eq(format!("{tag}-{name}")),
                sg_configs::value.eq(&tag),
            ))
            .execute(&mut other)
            .expect("insert");
        let at = i64::try_from(at).expect("small");
        assert_eq!(
            next(&mut changes).await,
            Change::Updated {
                old: at,
                new: at + 1
            }
        );
    }
    diesel::delete(sg_configs::table.filter(sg_configs::value.eq(&tag)))
        .execute(&mut other)
        .expect("delete both in one commit");
    assert_eq!(next(&mut changes).await, Change::Updated { old: 2, new: 0 });
    quiet(&mut changes).await;
    drop(subscription);
}

/// A query the engine cannot follow is refused at subscribe, with the
/// engine's reason, rather than never calling back.
#[tokio::test(flavor = "multi_thread")]
async fn a_query_the_engine_cannot_follow_is_refused() {
    let Some((engine, database, _other)) = start() else {
        return;
    };
    let refused = engine
        .subscribe(
            &database,
            diesel::select(diesel::dsl::now),
            |_: Change<chrono::NaiveDateTime>| async {},
        )
        .await;
    let Err(error) = refused else {
        panic!("a query on no table cannot be followed");
    };
    assert!(!error.0.is_empty());
}
