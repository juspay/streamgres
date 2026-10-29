//! A watcher is told when the engine no longer follows its query, and
//! its stream ends: here the watched table is dropped, the engine restarts
//! without it, and refuses the query when the client resubscribes.
//!
//! A test binary of its own, with its own engine: the drop restarts it.
//! Needs the `embedded` feature, `STREAMGRES_TEST_EMBEDDED`,
//! `STREAMGRES_PG_DSN` and `DATABASE_URL`; skips itself otherwise.

mod transport;

use std::time::Duration;

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel_streamgres::{Change, WatchError};

#[tokio::test(flavor = "multi_thread")]
async fn a_watcher_of_a_dropped_table_is_told_and_its_stream_ends() {
    if std::env::var_os("STREAMGRES_TEST_EMBEDDED").is_none() {
        eprintln!("STREAMGRES_TEST_EMBEDDED not set; skipping (the drop restarts the engine)");
        return;
    }
    let Some(engine) = transport::client(diesel_streamgres::engine::Config::from_env()) else {
        return;
    };
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let table = format!("sg_watch_drop_{}", std::process::id());
    let mut other = PgConnection::establish(&database).expect("connect");
    other
        .batch_execute(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (id int PRIMARY KEY, note text NOT NULL)"
        ))
        .expect("table");
    // The engine hears the new table through its schema-change trigger.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut watch = loop {
        match engine
            .watch_sql(&database, format!("SELECT id, note FROM {table}"))
            .await
        {
            Ok(watch) => break watch,
            Err(error) if std::time::Instant::now() < deadline => {
                eprintln!("not yet followed: {error}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(error) => panic!("the table was never followed: {error}"),
        }
    };
    assert_eq!(engine.watched().await, 1);

    other
        .batch_execute(&format!("INSERT INTO {table} VALUES (1, 'a')"))
        .expect("insert");
    let change = tokio::time::timeout(Duration::from_secs(10), watch.recv())
        .await
        .expect("a change within the wait")
        .expect("running");
    assert!(matches!(change, Ok(Change::Added(_))), "{change:?}");

    other
        .batch_execute(&format!("DROP TABLE {table}"))
        .expect("drop");
    let ended = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match watch.recv().await {
                Some(Err(WatchError::Failed(reason))) => break reason,
                Some(other) => eprintln!("before the end: {other:?}"),
                None => panic!("the stream ended without saying why"),
            }
        }
    })
    .await
    .expect("told within the wait");
    eprintln!("ended: {ended}");
    assert!(watch.recv().await.is_none(), "nothing follows the end");
    assert_eq!(engine.watched().await, 0, "an ended subscription is not counted");
}
