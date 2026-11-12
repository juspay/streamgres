//! The embedded cache against a live Postgres: answers equal to
//! Postgres's, kept current through writes, and refusals for what it
//! cannot answer exactly. Runs only when `STREAMGRES_PG_DSN` names a
//! database with `wal_level = logical`.

use std::time::Duration;

use streamgres::cache::{Bind, Cache, CacheConfig, Position, Reader, Refusal};
use streamgres::model::{Numeric, Value};
use tokio_postgres::NoTls;

const TABLE: &str = "live_cache_orders";

/// Held for each scenario's whole run: their schema changes fire one
/// another's schema-change triggers, whose shared advisory lock deadlocks
/// concurrent DDL transactions, so the scenarios run one at a time.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Takes [`SERIAL`] whatever an earlier scenario's panic left it as.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
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

/// `client`'s connection checked as a reader of `cache`.
fn reader_of(
    runtime: &tokio::runtime::Runtime,
    client: &tokio_postgres::Client,
    cache: &Cache,
) -> Reader {
    let check = cache.reader_check().expect("the engine is running");
    let row = runtime
        .block_on(client.query_one(
            check.sql,
            &[&check.names, &check.as_written, &check.qualified],
        ))
        .expect("the reader check");
    Reader::new(row.get(0), row.get(1), row.get(2), check.version)
}

fn amount(text: &str) -> Value {
    Value::Numeric(Numeric::parse(text).unwrap())
}

#[test]
fn reads_are_answered_and_kept_current() {
    let _serial = serial();
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
             DROP TABLE IF EXISTS {TABLE}_secret;
             CREATE TABLE {TABLE}_secret (id int8 PRIMARY KEY);
             ALTER TABLE {TABLE}_secret ENABLE ROW LEVEL SECURITY;
             CREATE PUBLICATION {PUBLICATION} FOR TABLE {TABLE}, {TABLE}_secret;
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
    let reader = reader_of(&runtime, &client, &cache);

    let by_merchant = format!(
        r#"SELECT "{TABLE}"."id", "{TABLE}"."amount" FROM "{TABLE}" WHERE ("{TABLE}"."merchant" = $1) ORDER BY "{TABLE}"."amount" DESC LIMIT $2"#
    );
    let read = |merchant: &str| {
        cache
            .read(&by_merchant, &[text(merchant), int8(10)], &reader)
            .expect("served")
            .rows()
            .iter()
            .map(|row| row.to_vec())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        read("m1"),
        vec![
            vec![Value::Int(1), amount("10.50")],
            vec![Value::Int(2), amount("3.00")]
        ]
    );
    let first = cache
        .read(&by_merchant, &[text("m1"), int8(10)], &reader)
        .unwrap();
    assert_eq!(
        first.columns[1].type_oid, 1700,
        "the column's type travels with it"
    );

    // Repeated identical reads populate the thread-local answer before
    // crossing a WAL barrier. The barrier must invalidate that answer.
    assert_eq!(read("m1"), read("m1"));
    let wait_for_write = |sql: &str| {
        runtime.block_on(client.batch_execute(sql)).expect("write");
        let at: String = runtime
            .block_on(client.query_one("SELECT pg_current_wal_lsn()::text", &[]))
            .unwrap()
            .get(0);
        assert!(
            cache.wait_for(
                Position::from(streamgres::sync::pg::Lsn::parse(&at).unwrap()),
                Duration::from_secs(10)
            ),
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
        .read(&format!(r#"SELECT COUNT(*) FROM "{TABLE}""#), &[], &reader)
        .expect("counted");
    assert_eq!(count.rows(), [Box::from([Value::Int(3)])]);

    let refused = cache.read(
        &format!(r#"SELECT * FROM "{TABLE}" WHERE "{TABLE}"."note" = $1"#),
        &[text("a")],
        &reader,
    );
    assert!(
        matches!(refused, Err(Refusal::Unsupported(_))),
        "char(n) compares ignoring its padding, which the engine does not: {refused:?}"
    );
    let secret = cache.read(&format!(r#"SELECT * FROM "{TABLE}_secret""#), &[], &reader);
    assert!(
        matches!(secret, Err(Refusal::Unsupported(_))),
        "row-level security is Postgres's to apply: {secret:?}"
    );
    let whole = cache
        .read(
            &format!(r#"SELECT * FROM "{TABLE}" WHERE "{TABLE}"."id" = $1"#),
            &[int8(2)],
            &reader,
        )
        .expect("a whole row");
    let names: Vec<&str> = whole
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(
        names,
        ["id", "merchant", "amount", "note"],
        "in the table's own order"
    );
    assert_eq!(whole.rows()[0][3], Value::from("b   "));

    let count_sql = format!(r#"SELECT COUNT(*) FROM "{TABLE}""#);
    runtime
        .block_on(client.batch_execute(&format!(
            "DROP SCHEMA IF EXISTS live_cache_shadow CASCADE;
             CREATE SCHEMA live_cache_shadow;
             CREATE TABLE live_cache_shadow.{TABLE} (id int8 PRIMARY KEY);
             SET search_path = live_cache_shadow, public;"
        )))
        .unwrap();
    let shadowed = reader_of(&runtime, &client, &cache);
    assert!(
        matches!(
            cache.read(&count_sql, &[], &shadowed),
            Err(Refusal::Unsupported(_))
        ),
        "the reader's search path names another table"
    );
    runtime
        .block_on(client.batch_execute(
            "RESET search_path; DROP SCHEMA live_cache_shadow CASCADE;
             DROP ROLE IF EXISTS live_cache_limited; CREATE ROLE live_cache_limited;
             SET ROLE live_cache_limited;",
        ))
        .unwrap();
    let limited = reader_of(&runtime, &client, &cache);
    assert!(
        matches!(
            cache.read(&count_sql, &[], &limited),
            Err(Refusal::Unsupported(_))
        ),
        "a role that may not read the table is not answered from it"
    );
    runtime
        .block_on(client.batch_execute("RESET ROLE; DROP ROLE live_cache_limited;"))
        .unwrap();
    assert!(
        cache
            .read(&count_sql, &[], &reader_of(&runtime, &client, &cache))
            .is_ok()
    );

    let _ = read("m1");
    let _ = read("m1");
    cache.stop();
    assert!(
        cache
            .read(&by_merchant, &[text("m1"), int8(10)], &reader)
            .is_err(),
        "stop invalidates the local answer"
    );
    runtime
        .block_on(client.batch_execute(&format!(
            "{teardown} DROP PUBLICATION IF EXISTS {PUBLICATION}; DROP TABLE IF EXISTS {TABLE}; DROP TABLE IF EXISTS {TABLE}_secret;"
        )))
        .unwrap();
}

/// An engine whose slot is taken away stops rather than skip changes, and
/// the supervisor brings a fresh one up on a fresh slot; reads are
/// refused as not ready meanwhile, never answered from stale rows.
#[test]
fn a_lost_slot_brings_a_fresh_engine() {
    let _serial = serial();
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
    let reader = reader_of(&runtime, &client, &cache);
    let sql = format!(r#"SELECT "{table}"."id" FROM "{table}""#);
    assert_eq!(cache.read(&sql, &[], &reader).unwrap().rows().len(), 1);

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
        assert!(
            std::time::Instant::now() < deadline,
            "the slot could not be taken away"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while cache.slot().is_none_or(|now| now == slot) {
        assert!(
            std::time::Instant::now() < deadline,
            "no fresh engine on a fresh slot"
        );
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
        let fresh = reader_of(&runtime, &client, &cache);
        assert!(
            matches!(cache.read(&sql, &[], &reader), Err(Refusal::NotReady(_))),
            "a reader checked against the engine before is checked again"
        );
        if let Ok(rows) = cache.read(&sql, &[], &fresh)
            && rows.rows().len() == 2
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the fresh engine never served the write"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    cache.stop();
    runtime
        .block_on(client.batch_execute(&format!(
            "{teardown} DROP PUBLICATION IF EXISTS {publication}; DROP TABLE IF EXISTS {table};"
        )))
        .unwrap();
}

/// How long a hot read takes from the cache and from Postgres: run with
/// `cargo test --release --test cache_live -- --ignored --nocapture`.
#[test]
#[ignore = "a measurement, not a check"]
fn read_latency() {
    let _serial = serial();
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = runtime.block_on(async {
        let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(connection);
        client
    });
    let mut config = CacheConfig::new(dsn.clone(), "live_bench_pub");
    config.app = "streamgres_bench".to_owned();
    runtime
        .block_on(client.batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS live_bench_pub;
             DROP TABLE IF EXISTS live_bench_items; DROP TABLE IF EXISTS live_bench_owners;
             CREATE TABLE live_bench_owners (id int8 PRIMARY KEY, name text COLLATE \"C\" NOT NULL);
             CREATE TABLE live_bench_items (id int8 PRIMARY KEY, owner int8 NOT NULL, price numeric(12,2) NOT NULL, label text NOT NULL);
             INSERT INTO live_bench_owners SELECT g, 'owner ' || g FROM generate_series(1, 1000) g;
             INSERT INTO live_bench_items SELECT g, 1 + g % 1000, g * 1.25, 'item ' || g FROM generate_series(1, 100000) g;
             CREATE INDEX ON live_bench_items (owner);
             CREATE PUBLICATION live_bench_pub FOR TABLE live_bench_items, live_bench_owners;
             {}",
            config.setup_sql()
        )))
        .unwrap();
    let teardown = config.teardown_sql();
    let cache = Cache::start(config);
    cache.wait_ready(Duration::from_secs(30)).unwrap();
    let reader = reader_of(&runtime, &client, &cache);
    let single = r#"SELECT "live_bench_items"."id", "live_bench_items"."price", "live_bench_items"."label" FROM "live_bench_items" WHERE ("live_bench_items"."owner" = $1) ORDER BY "live_bench_items"."id" LIMIT $2"#;
    let joined = r#"SELECT "live_bench_items"."id", "live_bench_owners"."name" FROM ("live_bench_items" INNER JOIN "live_bench_owners" ON ("live_bench_items"."owner" = "live_bench_owners"."id")) WHERE ("live_bench_items"."owner" = $1) ORDER BY "live_bench_items"."id" LIMIT $2"#;
    for (name, sql) in [("single table", single), ("joined", joined)] {
        let binds = [int8(42), int8(20)];
        cache.read(sql, &binds, &reader).expect("warm");
        let rounds = 20_000;
        let mut cached = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            let started = std::time::Instant::now();
            let rows = cache.read(sql, &binds, &reader).expect("served");
            cached.push(started.elapsed());
            assert_eq!(rows.rows().len(), 20);
        }
        let statement = runtime.block_on(client.prepare(sql)).unwrap();
        let mut postgres = Vec::with_capacity(2_000);
        for _ in 0..2_000 {
            let started = std::time::Instant::now();
            let rows = runtime
                .block_on(client.query(&statement, &[&42i64, &20i64]))
                .unwrap();
            postgres.push(started.elapsed());
            assert_eq!(rows.len(), 20);
        }
        let percentile = |samples: &mut Vec<Duration>, p: f64| {
            samples.sort();
            samples[((samples.len() as f64 - 1.0) * p) as usize]
        };
        println!(
            "{name}: cache p50 {:?} p99 {:?}; Postgres (prepared, local) p50 {:?} p99 {:?}",
            percentile(&mut cached, 0.5),
            percentile(&mut cached, 0.99),
            percentile(&mut postgres, 0.5),
            percentile(&mut postgres, 0.99)
        );
    }
    cache.stop();
    runtime
        .block_on(client.batch_execute(&format!(
            "{teardown} DROP PUBLICATION live_bench_pub; DROP TABLE live_bench_items; DROP TABLE live_bench_owners;"
        )))
        .unwrap();
}

/// An update that changes only how a value is spelled (a numeric's scale,
/// a json document's text) reaches the cache: its answer is Postgres's
/// spelling, not the one it held before.
#[test]
fn a_change_of_spelling_alone_reaches_the_cache() {
    let _serial = serial();
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = runtime.block_on(async {
        let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(connection);
        client
    });
    let mut config = CacheConfig::new(dsn.clone(), "live_spelling_pub");
    config.app = "streamgres_spelling".to_owned();
    runtime
        .block_on(client.batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS live_spelling_pub; DROP TABLE IF EXISTS live_spelling;
             CREATE TABLE live_spelling (id int8 PRIMARY KEY, n numeric, doc json);
             INSERT INTO live_spelling VALUES (1, 1.5, '{{\"a\":1}}');
             CREATE PUBLICATION live_spelling_pub FOR TABLE live_spelling;
             {}",
            config.setup_sql()
        )))
        .unwrap();
    let teardown = config.teardown_sql();
    let cache = Cache::start(config);
    cache.wait_ready(Duration::from_secs(30)).unwrap();
    let reader = reader_of(&runtime, &client, &cache);
    let sql = r#"SELECT "live_spelling"."n", "live_spelling"."doc" FROM "live_spelling""#;
    let spelled =
        |rows: &streamgres::cache::CachedRows| match (&rows.rows()[0][0], &rows.rows()[0][1]) {
            (Value::Numeric(n), Value::Json(doc)) => (n.to_string(), doc.text().to_owned()),
            other => panic!("{other:?}"),
        };
    assert_eq!(
        spelled(&cache.read(sql, &[], &reader).unwrap()),
        ("1.5".to_owned(), "{\"a\":1}".to_owned())
    );
    runtime
        .block_on(client.batch_execute("UPDATE live_spelling SET n = 1.50, doc = '{ \"a\" : 1 }'"))
        .unwrap();
    let at: String = runtime
        .block_on(client.query_one("SELECT pg_current_wal_lsn()::text", &[]))
        .unwrap()
        .get(0);
    assert!(cache.wait_for(
        Position::from(streamgres::sync::pg::Lsn::parse(&at).unwrap()),
        Duration::from_secs(10)
    ));
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let mut seen = spelled(&cache.read(sql, &[], &reader).unwrap());
    while seen.0 != "1.50" && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
        seen = spelled(&cache.read(sql, &[], &reader).unwrap());
    }
    assert_eq!(seen, ("1.50".to_owned(), "{ \"a\" : 1 }".to_owned()));
    cache.stop();
    runtime
        .block_on(client.batch_execute(&format!(
            "{teardown} DROP PUBLICATION live_spelling_pub; DROP TABLE live_spelling;"
        )))
        .unwrap();
}

/// A column the engine absorbs leaves the table's other columns as the
/// catalog carried them: their types and collations still decide what is
/// answered (an order by a `C` text, the new column's values) and what is
/// refused (a `char(n)` comparison), as before the migration.
#[test]
fn an_absorbed_column_keeps_the_table_answerable() {
    let _serial = serial();
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = runtime.block_on(async {
        let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(connection);
        client
    });
    let mut config = CacheConfig::new(dsn.clone(), "live_widen_pub");
    config.app = "streamgres_widen".to_owned();
    runtime
        .block_on(client.batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS live_widen_pub; DROP TABLE IF EXISTS live_widen;
             CREATE TABLE live_widen (id int8 PRIMARY KEY, name text COLLATE \"C\" NOT NULL, code char(4));
             INSERT INTO live_widen VALUES (1, 'b', 'ab'), (2, 'a', 'cd');
             CREATE PUBLICATION live_widen_pub FOR TABLE live_widen;
             {}",
            config.setup_sql()
        )))
        .unwrap();
    let teardown = config.teardown_sql();
    let cache = Cache::start(config);
    cache.wait_ready(Duration::from_secs(30)).unwrap();
    let ordered = r#"SELECT "live_widen"."id" FROM "live_widen" ORDER BY "live_widen"."name""#;
    let by_code = r#"SELECT "live_widen"."id" FROM "live_widen" WHERE "live_widen"."code" = $1"#;
    let note = r#"SELECT "live_widen"."id", "live_widen"."note" FROM "live_widen" ORDER BY "live_widen"."id""#;
    let code = Bind {
        type_oid: 1042,
        bytes: Some(b"ab".to_vec()),
    };
    let reader = reader_of(&runtime, &client, &cache);
    let ids = |rows: streamgres::cache::CachedRows| -> Vec<Value> {
        rows.rows().iter().map(|row| row[0].clone()).collect()
    };
    assert_eq!(
        ids(cache.read(ordered, &[], &reader).unwrap()),
        vec![Value::Int(2), Value::Int(1)]
    );
    assert!(
        cache
            .read(by_code, std::slice::from_ref(&code), &reader)
            .is_err()
    );

    runtime
        .block_on(
            client.batch_execute(
                "ALTER TABLE live_widen ADD COLUMN note text NOT NULL DEFAULT 'none'",
            ),
        )
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let widened = loop {
        let reader = reader_of(&runtime, &client, &cache);
        if let Ok(rows) = cache.read(note, &[], &reader) {
            break (reader, rows);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the new column was never answered"
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    let (reader, rows) = widened;
    assert_eq!(rows.columns[1].type_oid, 25, "the new column's type");
    assert_eq!(rows.rows()[0][1], Value::from("none"));
    assert_eq!(
        ids(cache
            .read(ordered, &[], &reader)
            .expect("the held columns keep their types and collations")),
        vec![Value::Int(2), Value::Int(1)]
    );
    assert!(
        cache.read(by_code, &[code], &reader).is_err(),
        "char(n) still compares otherwise than the engine"
    );
    cache.stop();
    runtime
        .block_on(client.batch_execute(&format!(
            "{teardown} DROP PUBLICATION live_widen_pub; DROP TABLE live_widen;"
        )))
        .unwrap();
}

/// Turning RLS on after warming a result must retire that engine and its
/// rows; a restarted engine must continue refusing the protected table.
#[test]
fn enabling_row_security_retires_cached_rows() {
    let _serial = serial();
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = runtime.block_on(async {
        let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(connection);
        client
    });
    let mut config = CacheConfig::new(dsn, "review_rls_pub");
    config.app = "review_rls".to_owned();
    runtime
        .block_on(client.batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS review_rls_pub;
         DROP TABLE IF EXISTS review_rls_rows;
         CREATE TABLE review_rls_rows (id int8 PRIMARY KEY);
         INSERT INTO review_rls_rows VALUES (1), (2);
         CREATE PUBLICATION review_rls_pub FOR TABLE review_rls_rows; {}",
            config.setup_sql()
        )))
        .unwrap();
    let teardown = config.teardown_sql();
    let cache = Cache::start(config);
    cache.wait_ready(Duration::from_secs(10)).unwrap();
    let reader = reader_of(&runtime, &client, &cache);
    let sql = "SELECT id FROM review_rls_rows ORDER BY id";
    assert_eq!(cache.read(sql, &[], &reader).unwrap().rows().len(), 2);
    runtime
        .block_on(client.batch_execute(
            "ALTER TABLE review_rls_rows ENABLE ROW LEVEL SECURITY;
         CREATE POLICY one_row ON review_rls_rows USING (id = 1);",
        ))
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while cache.metrics().restarts == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        cache.metrics().restarts > 0,
        "the RLS migration restarts the engine"
    );
    cache.wait_ready(Duration::from_secs(10)).unwrap();
    let reader = reader_of(&runtime, &client, &cache);
    assert!(
        cache.read(sql, &[], &reader).is_err(),
        "protected rows must never be cached"
    );
    cache.stop();
    runtime
        .block_on(client.batch_execute(&format!(
            "{teardown} DROP PUBLICATION review_rls_pub; DROP TABLE review_rls_rows;"
        )))
        .unwrap();
}

/// A repeatable concurrency measurement, deliberately outside correctness CI.
#[test]
#[ignore = "concurrent cache throughput measurement"]
fn concurrent_read_scaling() {
    let _serial = serial();
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(async {
        let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(connection);
        client
    });
    let mut config = CacheConfig::new(dsn, "scaling_pub");
    config.app = "scaling_test".into();
    rt.block_on(client.batch_execute(&format!(
        "DROP PUBLICATION IF EXISTS scaling_pub; DROP TABLE IF EXISTS scaling_rows;
        CREATE TABLE scaling_rows(id int8 PRIMARY KEY); INSERT INTO scaling_rows VALUES (1);
        CREATE PUBLICATION scaling_pub FOR TABLE scaling_rows; {}",
        config.setup_sql()
    )))
    .unwrap();
    let teardown = config.teardown_sql();
    let cache = std::sync::Arc::new(Cache::start(config));
    cache.wait_ready(Duration::from_secs(10)).unwrap();
    let reader = reader_of(&rt, &client, &cache);
    let sql = "SELECT id FROM scaling_rows WHERE id = $1";
    let binds = [int8(1)];
    cache.read(sql, &binds, &reader).unwrap();
    for threads in [1, 4, 8, 16] {
        let gate = std::sync::Arc::new(std::sync::Barrier::new(threads + 1));
        let mut workers = Vec::new();
        for _ in 0..threads {
            let cache = cache.clone();
            let reader = reader.clone();
            let binds = binds.clone();
            let gate = gate.clone();
            workers.push(std::thread::spawn(move || {
                let mut times = Vec::new();
                gate.wait();
                for n in 0..100_000 {
                    let at = std::time::Instant::now();
                    let rows = cache.read(sql, &binds, &reader).unwrap();
                    assert_eq!(rows.rows().len(), 1);
                    if n % 64 == 0 {
                        times.push(at.elapsed());
                    }
                }
                times
            }));
        }
        let at = std::time::Instant::now();
        gate.wait();
        let mut times: Vec<_> = workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect();
        let elapsed = at.elapsed();
        times.sort_unstable();
        println!(
            "threads={threads} reads_per_second={:.0} p50={:?} p99={:?}",
            threads as f64 * 100_000.0 / elapsed.as_secs_f64(),
            times[times.len() / 2],
            times[times.len() * 99 / 100]
        );
    }
    cache.stop();
    rt.block_on(client.batch_execute(&format!(
        "{teardown} DROP PUBLICATION scaling_pub; DROP TABLE scaling_rows"
    )))
    .unwrap();
}
