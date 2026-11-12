//! The embedded cache against a live Postgres: answers equal to
//! Postgres's, kept current through writes, and refusals for what it
//! cannot answer exactly. Runs only when `STREAMGRES_PG_DSN` names a
//! database with `wal_level = logical`.

use std::time::Duration;

use streamgres::cache::{Bind, Cache, CacheConfig, Lsn, Refusal};
use streamgres::model::{Numeric, Value};
use tokio_postgres::NoTls;

const TABLE: &str = "live_cache_orders";
const PUBLICATION: &str = "live_cache_pub";

fn text(value: &str) -> Bind {
    Bind {
        type_oid: 25,
        bytes: Some(value.as_bytes().to_vec()),
    }
}

fn int8(value: i64) -> Bind {
    Bind {
        type_oid: 20,
        bytes: Some(value.to_be_bytes().to_vec()),
    }
}

fn amount(text: &str) -> Value {
    Value::Numeric(Numeric::parse(text).unwrap())
}

#[test]
fn reads_are_answered_and_kept_current() {
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        eprintln!("STREAMGRES_PG_DSN not set: live cache scenario skipped");
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = runtime.block_on(async {
        let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(connection);
        client
    });
    let config = CacheConfig::new(dsn.clone(), PUBLICATION);
    runtime
        .block_on(client.batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {PUBLICATION};
             DROP TABLE IF EXISTS {TABLE};
             CREATE TABLE {TABLE} (id int8 PRIMARY KEY, merchant text COLLATE \"C\", amount numeric(12, 2), note char(4));
             CREATE PUBLICATION {PUBLICATION} FOR TABLE {TABLE};
             INSERT INTO {TABLE} VALUES (1, 'm1', 10.50, 'a'), (2, 'm1', 3.00, 'b'), (3, 'm2', 7.25, 'c');
             {setup}",
            setup = config.setup_sql()
        )))
        .expect("prepare");

    let teardown = config.teardown_sql();
    let cache = Cache::start(config);
    cache
        .wait_ready(Duration::from_secs(30))
        .expect("the engine comes up");

    let by_merchant = format!(
        r#"SELECT "{TABLE}"."id", "{TABLE}"."amount" FROM "{TABLE}" WHERE ("{TABLE}"."merchant" = $1) ORDER BY "{TABLE}"."amount" DESC LIMIT $2"#
    );
    let read = |merchant: &str| {
        cache
            .read(&by_merchant, &[text(merchant), int8(10)])
            .expect("served")
            .rows
    };
    assert_eq!(
        read("m1"),
        vec![
            vec![Value::Int(1), amount("10.50")],
            vec![Value::Int(2), amount("3.00")]
        ]
    );
    let first = cache.read(&by_merchant, &[text("m1"), int8(10)]).unwrap();
    assert_eq!(first.columns[1].type_oid, 1700, "the column's type travels with it");

    let wait_for_write = |sql: &str| {
        runtime.block_on(client.batch_execute(sql)).expect("write");
        let at: String = runtime
            .block_on(client.query_one("SELECT pg_current_wal_lsn()::text", &[]))
            .unwrap()
            .get(0);
        assert!(
            cache.wait_for(Lsn::parse(&at).unwrap(), Duration::from_secs(10)),
            "the engine reaches the write"
        );
    };
    wait_for_write(&format!("INSERT INTO {TABLE} VALUES (4, 'm1', 99.99, 'd')"));
    assert_eq!(read("m1")[0], vec![Value::Int(4), amount("99.99")]);
    wait_for_write(&format!("UPDATE {TABLE} SET amount = 1.00 WHERE id = 4"));
    assert_eq!(read("m1")[2], vec![Value::Int(4), amount("1.00")]);
    wait_for_write(&format!("DELETE FROM {TABLE} WHERE id = 1"));
    assert_eq!(read("m1").len(), 2);

    let count = cache
        .read(&format!(r#"SELECT COUNT(*) FROM "{TABLE}""#), &[])
        .expect("counted");
    assert_eq!(count.rows, vec![vec![Value::Int(3)]]);

    let refused = cache.read(
        &format!(r#"SELECT * FROM "{TABLE}" WHERE "{TABLE}"."note" = $1"#),
        &[text("a")],
    );
    assert!(
        matches!(refused, Err(Refusal::Unsupported(_))),
        "char(n) compares ignoring its padding, which the engine does not: {refused:?}"
    );
    let whole = cache
        .read(&format!(r#"SELECT * FROM "{TABLE}" WHERE "{TABLE}"."id" = $1"#), &[int8(2)])
        .expect("a whole row");
    let names: Vec<&str> = whole.columns.iter().map(|column| column.name.as_str()).collect();
    assert_eq!(names, ["id", "merchant", "amount", "note"], "in the table's own order");
    assert_eq!(whole.rows[0][3], Value::from("b   "));

    cache.stop();
    runtime
        .block_on(client.batch_execute(&format!(
            "{teardown} DROP PUBLICATION IF EXISTS {PUBLICATION}; DROP TABLE IF EXISTS {TABLE};"
        )))
        .unwrap();
}

/// An engine whose slot is taken away stops rather than skip changes, and
/// the supervisor brings a fresh one up on a fresh slot; reads are
/// refused as not ready meanwhile, never answered from stale rows.
#[test]
fn a_lost_slot_brings_a_fresh_engine() {
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        return;
    };
    let table = "live_cache_restart";
    let publication = "live_cache_restart_pub";
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = runtime.block_on(async {
        let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(connection);
        client
    });
    let mut config = CacheConfig::new(dsn.clone(), publication);
    config.app = "streamgres_restart".to_owned();
    runtime
        .block_on(client.batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {publication};
             DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (id int8 PRIMARY KEY);
             CREATE PUBLICATION {publication} FOR TABLE {table};
             INSERT INTO {table} VALUES (1);
             {setup}",
            setup = config.setup_sql()
        )))
        .expect("prepare");
    let teardown = config.teardown_sql();
    let cache = Cache::start(config);
    cache.wait_ready(Duration::from_secs(30)).expect("up");
    let sql = format!(r#"SELECT "{table}"."id" FROM "{table}""#);
    assert_eq!(cache.read(&sql, &[]).unwrap().rows.len(), 1);

    let slot = cache.slot().expect("running on a slot");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let dropped = runtime.block_on(client.batch_execute(&format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '{0}' AND active_pid IS NOT NULL;
             SELECT pg_drop_replication_slot('{0}');",
            slot
        )));
        if dropped.is_ok() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the slot could not be taken away");
        std::thread::sleep(Duration::from_millis(20));
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while cache.slot().is_none_or(|now| now == slot) {
        assert!(std::time::Instant::now() < deadline, "no fresh engine on a fresh slot");
        std::thread::sleep(Duration::from_millis(100));
    }
    cache
        .wait_ready(Duration::from_secs(30))
        .expect("the fresh engine comes up");
    runtime
        .block_on(client.batch_execute(&format!("INSERT INTO {table} VALUES (2)")))
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(rows) = cache.read(&sql, &[])
            && rows.rows.len() == 2
        {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the fresh engine never served the write");
        std::thread::sleep(Duration::from_millis(50));
    }
    cache.stop();
    runtime
        .block_on(client.batch_execute(&format!(
            "{teardown} DROP PUBLICATION IF EXISTS {publication}; DROP TABLE IF EXISTS {table};"
        )))
        .unwrap();
}
