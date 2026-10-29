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

diesel::table! {
    sg_live_ws.sg_items (id) {
        id -> Int4,
        label -> Text,
    }
}

/// A row of a table outside `public`.
#[derive(Queryable, Selectable, Debug, PartialEq, Clone)]
#[diesel(table_name = sg_items)]
struct Item {
    id: i32,
    label: String,
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
    assert!(!error.to_string().is_empty());
}

/// A query given as plain SQL reports the same changes, its rows read by
/// column name: present, added, updated, removed.
#[tokio::test(flavor = "multi_thread")]
async fn a_plain_sql_query_reports_its_rows_changes() {
    let Some((engine, database, mut other)) = start() else {
        return;
    };
    let tag = format!("sql-{}", std::process::id());
    let key = |name: &str| format!("{tag}-{name}");
    diesel::insert_into(sg_configs::table)
        .values((sg_configs::key.eq(key("a")), sg_configs::value.eq(&tag)))
        .execute(&mut other)
        .expect("insert a");

    let (sender, mut changes) = mpsc::unbounded_channel();
    let _subscription = engine
        .subscribe_sql(
            &database,
            format!("SELECT id, key, value FROM sg_configs WHERE value = '{tag}'"),
            move |change: Change<diesel_streamgres::SqlRow>| {
                let sender = sender.clone();
                async move {
                    let _ = sender.send(change);
                }
            },
        )
        .await
        .expect("subscribed");

    let Change::Present(a) = next(&mut changes).await else {
        panic!("the row there first is present");
    };
    assert_eq!(a.text("key"), Some(key("a")));
    assert_eq!(a.columns(), ["id", "key", "value"]);
    assert!(a.get::<diesel::sql_types::Integer, i32>("id").expect("id") > 0);

    diesel::insert_into(sg_configs::table)
        .values((sg_configs::key.eq(key("b")), sg_configs::value.eq(&tag)))
        .execute(&mut other)
        .expect("insert b");
    let Change::Added(b) = next(&mut changes).await else {
        panic!("an inserted row is added");
    };
    assert_eq!(b.text("key"), Some(key("b")));

    diesel::update(sg_configs::table.filter(sg_configs::key.eq(key("b"))))
        .set(sg_configs::key.eq(key("b2")))
        .execute(&mut other)
        .expect("update b");
    let Change::Updated { old, new } = next(&mut changes).await else {
        panic!("a changed row is updated");
    };
    assert_eq!((old.text("key"), new.text("key")), (Some(key("b")), Some(key("b2"))));

    diesel::delete(sg_configs::table.filter(sg_configs::value.eq(&tag)))
        .execute(&mut other)
        .expect("delete");
    for _ in 0..2 {
        let change = next(&mut changes).await;
        assert!(matches!(change, Change::Removed(_)), "a deleted row is removed: {change:?}");
    }
    quiet(&mut changes).await;
}

/// Watchers of the same SQL share one subscription and each receives its
/// changes; another SQL has its own; the last watcher dropped stops it.
#[tokio::test(flavor = "multi_thread")]
async fn watchers_of_one_query_share_its_subscription() {
    let Some((engine, database, mut other)) = start() else {
        return;
    };
    let tag = format!("watch-{}", std::process::id());
    let sql = format!("SELECT id, key FROM sg_configs WHERE value = '{tag}'");
    let watching = engine.watched().await;
    let subscribed = engine.subscribed();

    let mut first = engine.watch_sql(&database, sql.clone()).await.expect("first watcher");
    let mut second = engine.watch_sql(&database, sql.clone()).await.expect("second watcher");
    let mut third = second.clone();
    assert_eq!(engine.watched().await, watching + 1, "one shared subscription");
    assert_eq!(engine.subscribed(), subscribed + 1, "the engine follows the query once");

    let other_sql = format!("SELECT id FROM sg_configs WHERE value = '{tag}-other'");
    let elsewhere = engine.watch_sql(&database, other_sql).await.expect("another query");
    assert_eq!(engine.watched().await, watching + 2, "another SQL, another subscription");
    drop(elsewhere);

    diesel::insert_into(sg_configs::table)
        .values((sg_configs::key.eq(format!("{tag}-a")), sg_configs::value.eq(&tag)))
        .execute(&mut other)
        .expect("insert");
    for watch in [&mut first, &mut second, &mut third] {
        let change = tokio::time::timeout(Duration::from_secs(10), watch.recv())
            .await
            .expect("a change within the wait")
            .expect("running")
            .expect("not lagged");
        let Change::Added(row) = change else {
            panic!("each watcher sees the row added: {change:?}");
        };
        assert_eq!(row.text("key"), Some(format!("{tag}-a")));
    }

    drop((first, second, third));
    assert_eq!(engine.watched().await, watching, "the last watcher stops it");
    diesel::delete(sg_configs::table.filter(sg_configs::value.eq(&tag)))
        .execute(&mut other)
        .expect("clean up");
}

/// A typed Diesel query on a table outside `public`, as Diesel renders it
/// (`"s"."t"."c"` throughout), is followed like one in `public`. Needs the
/// engine to follow `sg_live_ws` (`STREAMGRES_SCHEMAS`); skips otherwise.
#[tokio::test(flavor = "multi_thread")]
async fn a_typed_query_on_another_schema_reports_its_rows_changes() {
    let followed = std::env::var("STREAMGRES_SCHEMAS").unwrap_or_default();
    if !followed.split(',').any(|schema| schema.trim() == "sg_live_ws") {
        eprintln!("STREAMGRES_SCHEMAS does not name sg_live_ws; skipping");
        return;
    }
    let Some((engine, database, mut other)) = start() else {
        return;
    };
    other
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS sg_live_ws.sg_items (id int PRIMARY KEY, label text NOT NULL)",
        )
        .expect("table");
    let tag = format!("typed-{}", std::process::id());
    let (sender, mut changes) = mpsc::unbounded_channel();
    let _subscription = engine
        .subscribe(
            &database,
            sg_items::table
                .filter(sg_items::label.eq(tag.clone()))
                .select(Item::as_select()),
            move |change: Change<Item>| {
                let sender = sender.clone();
                async move {
                    let _ = sender.send(change);
                }
            },
        )
        .await
        .expect("a schema-qualified Diesel query is followed");
    let id = i32::try_from(std::process::id() % 1_000_000).expect("id");
    diesel::insert_into(sg_items::table)
        .values((sg_items::id.eq(id), sg_items::label.eq(&tag)))
        .execute(&mut other)
        .expect("insert");
    let Change::Added(item) = next(&mut changes).await else {
        panic!("an inserted row is added");
    };
    assert_eq!(item, Item { id, label: tag.clone() });
    diesel::delete(sg_items::table.filter(sg_items::label.eq(&tag)))
        .execute(&mut other)
        .expect("clean up");
}

/// HTTP adapters declare named queries rather than assembling watcher
/// streams themselves. Two responses share the watch, and setup failure
/// and dropping the last response release their subscription ownership.
#[cfg(feature = "sse")]
#[tokio::test(flavor = "multi_thread")]
async fn notification_bodies_share_and_release_their_watches() {
    use diesel_streamgres::sse::{Feed, Query};
    use futures_util::StreamExt;
    let Some((engine, database, mut other)) = start() else {
        return;
    };
    let tag = format!("notification-{}", std::process::id());
    let sql = format!("SELECT id, key FROM sg_configs WHERE value = '{tag}'");
    let watching = engine.watched().await;
    let feed = Feed::new(engine.clone(), database);
    feed.ready(Duration::from_secs(15)).await.expect("ready");
    assert_eq!(format!("{feed:?}"), "Feed { .. }");
    assert!(
        feed.notifications([Query::new("bad\nname", &sql)])
            .await
            .is_err()
    );
    assert_eq!(engine.watched().await, watching);
    assert!(
        feed.notifications([
            Query::new("config", &sql),
            Query::new("bad", "SELECT unknown FROM no_such_notification_table")
        ])
        .await
        .is_err()
    );
    assert_eq!(
        engine.watched().await,
        watching,
        "failed setup released its first watch"
    );
    let mut first = feed
        .notifications([Query::new("config", &sql)])
        .await
        .unwrap()
        .boxed();
    let mut second = feed
        .notifications([Query::new("config", &sql)])
        .await
        .unwrap()
        .boxed();
    assert_eq!(engine.watched().await, watching + 1);
    assert_eq!(first.next().await.as_deref(), Some(": connected\n\n"));
    assert_eq!(second.next().await.as_deref(), Some(": connected\n\n"));
    diesel::insert_into(sg_configs::table)
        .values((sg_configs::key.eq(&tag), sg_configs::value.eq(&tag)))
        .execute(&mut other)
        .unwrap();
    for response in [&mut first, &mut second] {
        let event = tokio::time::timeout(Duration::from_secs(10), response.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event, "event: config\ndata: config\n\n");
    }
    drop(first);
    assert_eq!(engine.watched().await, watching + 1);
    drop(second);
    assert_eq!(engine.watched().await, watching);
    diesel::delete(sg_configs::table.filter(sg_configs::value.eq(&tag)))
        .execute(&mut other)
        .unwrap();
}
