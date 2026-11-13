//! The cache at the edges of what it accepts, against Postgres: JSONB
//! binds, numeric against float, timestamp against timestamptz, quoted and
//! dotted names, reader exclusions and typed `NULL` parameters. Every
//! answer the cache gives must equal Postgres's; what it cannot give
//! exactly it must refuse. Runs only when `STREAMGRES_PG_DSN` names a
//! database with `wal_level = logical`.
use std::time::Duration;
use streamgres::cache::{Bind, Cache, CacheConfig, Reader};
use streamgres::model::Value;
use tokio_postgres::{Client, NoTls};
async fn connect(dsn: &str) -> Client {
    let (c, io) = tokio_postgres::connect(dsn, NoTls).await.unwrap();
    tokio::spawn(io);
    c
}
async fn reader(c: &Client, cache: &Cache) -> Reader {
    let check = cache.reader_check().unwrap();
    let row = c
        .query_one(
            check.sql,
            &[&check.names, &check.as_written, &check.qualified],
        )
        .await
        .unwrap();
    Reader::new(row.get(0), row.get(1), row.get(2), check.version)
}
fn bind(oid: u32, bytes: Vec<u8>) -> Bind {
    Bind {
        type_oid: oid,
        bytes: Some(bytes),
    }
}
#[allow(
    clippy::too_many_arguments,
    reason = "the oracle and cache have independent typed inputs"
)]
async fn compare(
    c: &Client,
    cache: &Cache,
    who: &Reader,
    name: &str,
    sql: &str,
    binds: Vec<Bind>,
    truth: &str,
    refused: bool,
) {
    let live = c.query(truth, &[]).await.map(|rows| {
        rows.into_iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>()
    });
    let cached = cache.read(sql, &binds, who).map(|rows| {
        rows.rows()
            .iter()
            .map(|row| match row.first() {
                Some(Value::Int(v)) => *v,
                _ => -999,
            })
            .collect::<Vec<_>>()
    });
    if refused {
        assert!(
            cached.is_err(),
            "{name}: must refuse rather than change SQL semantics: {cached:?}"
        );
    } else {
        assert_eq!(cached.unwrap(), live.unwrap(), "{name}");
    }
}
async fn check_boundaries(dsn: &str) {
    let c = connect(dsn).await;
    let mut config = CacheConfig::new(dsn, "rv_pub");
    config.app = "rv_review".into();
    config.schemas = vec!["public".into(), "rv".into()];
    c.batch_execute(&format!(r#"
      SET TIME ZONE 'Asia/Kolkata';
      CREATE SCHEMA IF NOT EXISTS rv;
      CREATE TABLE IF NOT EXISTS rv_vals(id int8 PRIMARY KEY,jb jsonb,j json,n numeric,a int4[],ts timestamp);
      TRUNCATE rv_vals;
      INSERT INTO rv_vals VALUES(1,'{{"aa":1,"b":2}}','{{"aa":1,"b":2}}',9007199254740993,'[0:1]={{7,8}}','2026-01-01 00:00:00');
      INSERT INTO rv_vals VALUES(2,'{{"a":1.0,"b":2}}','{{"a":1.0,"b":2}}',0.100000000000000001,'{{9}}','2026-01-01 05:30:00');
      INSERT INTO rv_vals(id,jb,n) VALUES(3,'{{"n":9007199254740993.00}}',8),(4,'0.100000000000000001',9);
      CREATE TABLE IF NOT EXISTS rv_floats(id int8 PRIMARY KEY,f float8);
      TRUNCATE rv_floats; INSERT INTO rv_floats VALUES (1,'Infinity'),(2,0);
      CREATE TABLE IF NOT EXISTS rv_zoned(id int8 PRIMARY KEY,tz timestamptz);
      TRUNCATE rv_zoned; INSERT INTO rv_zoned VALUES(10,'2026-01-01 00:00:00+00');
      CREATE TABLE IF NOT EXISTS "rv.dot"(id int8 PRIMARY KEY);
      TRUNCATE "rv.dot"; INSERT INTO "rv.dot" VALUES(111);
      CREATE TABLE IF NOT EXISTS rv.dot(id int8 PRIMARY KEY);
      TRUNCATE rv.dot; INSERT INTO rv.dot VALUES(222);
      CREATE TABLE IF NOT EXISTS "rv_secret{sep}rows"(id int8 PRIMARY KEY);
      TRUNCATE "rv_secret{sep}rows"; INSERT INTO "rv_secret{sep}rows" VALUES(777);
      DROP PUBLICATION IF EXISTS rv_pub; CREATE PUBLICATION rv_pub FOR ALL TABLES;
      DO $$ BEGIN CREATE ROLE rv_limited; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
      GRANT USAGE ON SCHEMA public,rv TO rv_limited;
      GRANT SELECT ON rv_vals,rv_zoned TO rv_limited;
      {setup}
    "#,sep='\u{1f}',setup=config.setup_sql())).await.unwrap();
    let teardown = config.teardown_sql();
    let mut bounded = config.clone();
    bounded.max_entry_bytes = 4096;
    let cache = Cache::start(config);
    cache.wait_ready(Duration::from_secs(20)).unwrap();
    let admin = reader(&c, &cache).await;
    for (name, text, truth) in [
        (
            "jsonb key order",
            r#"{"aa":1,"b":2}"#,
            r#"SELECT id FROM rv_vals WHERE jb='{"aa":1,"b":2}'::jsonb ORDER BY id"#,
        ),
        (
            "jsonb whitespace",
            r#"{"a":1,"b":2}"#,
            r#"SELECT id FROM rv_vals WHERE jb='{"a":1,"b":2}'::jsonb ORDER BY id"#,
        ),
        (
            "jsonb exact integer",
            r#"{"n":9007199254740993}"#,
            r#"SELECT id FROM rv_vals WHERE jb='{"n":9007199254740993}'::jsonb ORDER BY id"#,
        ),
        (
            "jsonb exact fraction",
            "0.100000000000000001",
            "SELECT id FROM rv_vals WHERE jb='0.100000000000000001'::jsonb ORDER BY id",
        ),
    ] {
        let mut bytes = vec![1];
        bytes.extend_from_slice(text.as_bytes());
        compare(
            &c,
            &cache,
            &admin,
            name,
            "SELECT id FROM rv_vals WHERE jb = $1 ORDER BY id",
            vec![bind(3802, bytes)],
            truth,
            false,
        )
        .await;
    }
    compare(
        &c,
        &cache,
        &admin,
        "numeric vs float wide",
        "SELECT id FROM rv_vals WHERE n = $1 ORDER BY id",
        vec![bind(701, 9007199254740992f64.to_be_bytes().to_vec())],
        "SELECT id FROM rv_vals WHERE n = 9007199254740992::float8 ORDER BY id",
        true,
    )
    .await;
    compare(
        &c,
        &cache,
        &admin,
        "numeric vs float fraction",
        "SELECT id FROM rv_vals WHERE n = $1 ORDER BY id",
        vec![bind(701, 0.1f64.to_be_bytes().to_vec())],
        "SELECT id FROM rv_vals WHERE n = 0.1::float8 ORDER BY id",
        true,
    )
    .await;
    compare(&c,&cache,&admin,"mixed timestamp join","SELECT rv_vals.id FROM rv_vals INNER JOIN rv_zoned ON rv_vals.ts = rv_zoned.tz ORDER BY rv_vals.id",vec![],"SELECT rv_vals.id FROM rv_vals INNER JOIN rv_zoned ON rv_vals.ts = rv_zoned.tz ORDER BY rv_vals.id",true).await;
    compare(
        &c,
        &cache,
        &admin,
        "quoted dot table",
        "SELECT id FROM \"rv.dot\" ORDER BY id",
        vec![],
        "SELECT id FROM \"rv.dot\" ORDER BY id",
        true,
    )
    .await;
    compare(
        &c,
        &cache,
        &admin,
        "schema table",
        "SELECT id FROM rv.dot ORDER BY id",
        vec![],
        "SELECT id FROM rv.dot ORDER BY id",
        false,
    )
    .await;
    let limited = connect(dsn).await;
    limited.batch_execute("SET ROLE rv_limited").await.unwrap();
    let limited_reader = reader(&limited, &cache).await;
    let secret_sql = format!("SELECT id FROM \"rv_secret{}rows\"", '\u{1f}');
    compare(
        &limited,
        &cache,
        &limited_reader,
        "separator privileges",
        &secret_sql,
        vec![],
        &secret_sql,
        true,
    )
    .await;
    for sql in [
        "SELECT id FROM rv_vals WHERE ts = '2026-01-01 00:00:00+05:30' ORDER BY id",
        "SELECT id FROM rv_zoned WHERE tz = '2026-01-01 00:00:00' ORDER BY id",
        "SELECT id FROM rv_vals WHERE ts = 0 ORDER BY id",
    ] {
        compare(
            &c,
            &cache,
            &admin,
            "timestamp literal semantics",
            sql,
            vec![],
            sql,
            true,
        )
        .await;
    }
    for number in ["1e1000", "1e-1000"] {
        let sql = format!("SELECT id FROM rv_floats WHERE f = {number} ORDER BY id");
        compare(
            &c,
            &cache,
            &admin,
            "numeric float range",
            &sql,
            vec![],
            &sql,
            true,
        )
        .await;
        let value = streamgres::model::Numeric::parse(number).unwrap();
        compare(
            &c,
            &cache,
            &admin,
            "numeric float bind range",
            "SELECT id FROM rv_floats WHERE f = $1 ORDER BY id",
            vec![bind(1700, value.to_binary())],
            &sql,
            true,
        )
        .await;
    }
    let a = cache
        .read("SELECT a FROM rv_vals WHERE id = 1", &[], &admin)
        .unwrap();
    let value = &a.rows()[0][0];
    let raw: Vec<u8> = c
        .query_one("SELECT array_send(a) FROM rv_vals WHERE id=1", &[])
        .await
        .unwrap()
        .get(0);
    assert!(matches!(value, Value::Raw(_)));
    assert_eq!(
        streamgres::sync::pg::binary::encode(value, 1007).unwrap(),
        raw
    );
    let json_array = streamgres::sync::pg::binary::encode_array(
        &[Value::Json(streamgres::model::JsonText::new(
            r#"{"b": 2, "aa": 1}"#.into(),
            r#"{"aa":1,"b":2}"#,
        ))],
        3802,
    )
    .unwrap();
    compare(
        &c,
        &cache,
        &admin,
        "jsonb array input",
        "SELECT id FROM rv_vals WHERE jb = ANY($1) ORDER BY id",
        vec![bind(3807, json_array)],
        r#"SELECT id FROM rv_vals WHERE jb = ANY(ARRAY['{"aa":1,"b":2}'::jsonb]) ORDER BY id"#,
        false,
    )
    .await;
    // A typed parameter must have a valid PostgreSQL comparison even
    // when NULL: jsonb = json and timestamp = text are not such operators.
    for (sql, oid) in [
        ("SELECT id FROM rv_vals WHERE jb = $1", 114),
        ("SELECT id FROM rv_vals WHERE ts = $1", 25),
    ] {
        assert!(
            cache
                .read(
                    sql,
                    &[Bind {
                        type_oid: oid,
                        bytes: None
                    }],
                    &admin
                )
                .is_err()
        );
    }
    // Dotted relation names must not be routed onto a distinct schema.table.
    c.batch_execute("INSERT INTO \"rv.dot\" VALUES (112); UPDATE rv.dot SET id = 223; UPDATE rv_vals SET jb = '{\"n\":9007199254740993.00}' WHERE id = 2;")
        .await
        .unwrap();
    let lsn: String = c
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    let position =
        streamgres::cache::Position::from(streamgres::sync::pg::Lsn::parse(&lsn).unwrap());
    assert!(cache.wait_for(position, Duration::from_secs(10)));
    compare(
        &c,
        &cache,
        &admin,
        "schema routing after writes",
        "SELECT id FROM rv.dot ORDER BY id",
        vec![],
        "SELECT id FROM rv.dot ORDER BY id",
        false,
    )
    .await;
    let bytes = [&[1][..], br#"{"n":9007199254740993}"#].concat();
    compare(
        &c,
        &cache,
        &admin,
        "jsonb after replication",
        "SELECT id FROM rv_vals WHERE jb = $1 ORDER BY id",
        vec![bind(3802, bytes)],
        r#"SELECT id FROM rv_vals WHERE jb='{"n":9007199254740993}'::jsonb ORDER BY id"#,
        false,
    )
    .await;
    cache.stop();
    c.batch_execute("CREATE TABLE IF NOT EXISTS rv_large(id int8 PRIMARY KEY, payload text); TRUNCATE rv_large; INSERT INTO rv_large SELECT i, repeat('x', 8192) FROM generate_series(1, 100) i;").await.unwrap();
    let small = Cache::start(bounded);
    small.wait_ready(Duration::from_secs(20)).unwrap();
    let who = reader(&c, &small).await;
    let reason = small
        .read(
            "SELECT id FROM rv_large ORDER BY id LIMIT 1000000",
            &[],
            &who,
        )
        .unwrap_err();
    assert!(reason.to_string().contains("byte budget"), "{reason}");
    small.stop();
    drop(limited);
    c.batch_execute(&format!("{teardown} DROP PUBLICATION rv_pub; DROP TABLE rv_vals, rv_zoned, rv_floats, \"rv.dot\", \"rv_secret{sep}rows\", rv_large; DROP SCHEMA rv CASCADE; REVOKE USAGE ON SCHEMA public FROM rv_limited; DROP ROLE rv_limited;", sep='\u{1f}')).await.unwrap();
}

#[test]
fn postgres_boundaries_remain_exact() {
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        eprintln!("STREAMGRES_PG_DSN not set: boundary scenarios skipped");
        return;
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(check_boundaries(&dsn));
}
