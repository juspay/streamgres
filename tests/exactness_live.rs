//! The cache's answers against Postgres's own, shape by shape: filters,
//! orders, pages and joins over rows chosen for their edges (`NULL`s,
//! `NaN`, signed zero, infinities, empty and `NULL` arrays), read once
//! from a fresh subscription and again after writes the engine applies
//! in place. A shape the cache refuses is not a wrong answer, but only
//! the shapes named in `REFUSED` may be refused. Runs only when
//! `STREAMGRES_PG_DSN` names a database with `wal_level = logical`.
use std::time::Duration;
use streamgres::cache::{Bind, Cache, CacheConfig, Position, Reader};
use streamgres::model::Value;
use streamgres::sync::pg::binary;
use tokio_postgres::NoTls;

fn b(oid: u32, bytes: Vec<u8>) -> Bind {
    Bind {
        type_oid: oid,
        bytes: Some(bytes),
    }
}
fn i8b(v: i64) -> Bind {
    b(20, v.to_be_bytes().to_vec())
}
fn i4b(v: i32) -> Bind {
    b(23, v.to_be_bytes().to_vec())
}
fn f8(v: f64) -> Bind {
    b(701, v.to_be_bytes().to_vec())
}
fn txt(v: &str) -> Bind {
    b(25, v.as_bytes().to_vec())
}
fn null(oid: u32) -> Bind {
    Bind {
        type_oid: oid,
        bytes: None,
    }
}
fn arr4(items: &[Option<i64>]) -> Bind {
    let vals: Vec<Value> = items
        .iter()
        .map(|i| i.map_or(Value::Null, Value::Int))
        .collect();
    b(1007, binary::encode_array(&vals, 23).unwrap())
}
fn tz(micros: i64) -> Bind {
    b(1184, micros.to_be_bytes().to_vec())
}

/// The shapes the cache must refuse: Postgres compares them by rules the
/// engine does not follow.
const REFUSED: &[&str] = &["n_eq_f", "ts_vs_tz_bind", "ts_ge_tz_bind"];

fn null_array() -> Bind {
    Bind {
        type_oid: 1007,
        bytes: None,
    }
}

#[test]
fn answers_equal_postgres() {
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(async {
        let (c, conn) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(conn);
        c
    });
    let mut config = CacheConfig::new(dsn.clone(), "probe_pub");
    config.app = "streamgres_probe".to_owned();
    rt.block_on(client.batch_execute(&format!(
        "SET TIME ZONE 'Asia/Kolkata';
         DROP PUBLICATION IF EXISTS probe_pub; DROP TABLE IF EXISTS probe_c; DROP TABLE IF EXISTS probe_t;
         CREATE TABLE probe_t (id int8 PRIMARY KEY, n int4, f float8, t text COLLATE \"C\", b bool, ts timestamp, tz timestamptz, num numeric, parent int8);
         CREATE TABLE probe_c (id int8 PRIMARY KEY, t_id int8, v int4);
         INSERT INTO probe_t VALUES
          (1, 1, 0.0, 'a', true, '2026-01-01 00:00:00', '2026-01-01 00:00:00+00', 1.00, NULL),
          (2, 5, '-0'::float8, 'B', false, '2026-01-01 05:30:00', '2026-01-01 05:30:00+05:30', 1, 1),
          (3, NULL, 'NaN', NULL, NULL, NULL, NULL, NULL, 1),
          (4, 3, 2.5, '', true, 'infinity', 'infinity', 2.50, 2),
          (5, 7, NULL, 'a ', false, '-infinity', '-infinity', -1, NULL),
          (6, 4, 'Infinity', 'b', true, '2026-06-01 12:00:00', '2026-06-01 12:00:00+00', 10, 3),
          (7, 5, '-Infinity', 'Z', NULL, '2025-12-31 18:30:00', '2025-12-31 18:30:00+00', 0.10, 3),
          (8, 2147483647, 1e300, 'é', false, '2026-01-01 00:00:00.000001', '2026-01-01 00:00:00.000001+00', 1e20, 4);
         INSERT INTO probe_c VALUES (10, 3, 1), (11, 3, 2), (12, 6, 3), (13, NULL, 4), (14, 99, 5), (15, 8, 6), (16, 8, 7);
         CREATE PUBLICATION probe_pub FOR TABLE probe_t, probe_c; {}",
        config.setup_sql()))).unwrap();
    let teardown = config.teardown_sql();
    let cache = Cache::start(config);
    cache.wait_ready(Duration::from_secs(30)).unwrap();
    let check = cache.reader_check().unwrap();
    let row = rt
        .block_on(client.query_one(
            check.sql,
            &[&check.names, &check.as_written, &check.qualified],
        ))
        .unwrap();
    let reader = Reader::new(row.get(0), row.get(1), check.version);

    let t = r#"SELECT "probe_t"."id" FROM "probe_t""#;
    let jc = r#"SELECT "probe_t"."id" FROM ("probe_t" INNER JOIN "probe_c" ON ("probe_c"."t_id" = "probe_t"."id"))"#;
    let jl = r#"SELECT "probe_t"."id" FROM ("probe_t" LEFT OUTER JOIN "probe_c" ON ("probe_c"."t_id" = "probe_t"."id"))"#;
    let jcp = r#"SELECT "probe_c"."id" FROM ("probe_c" INNER JOIN "probe_t" ON ("probe_c"."t_id" = "probe_t"."id"))"#;
    let jself = r#"SELECT "probe_c"."id" FROM ("probe_c" LEFT OUTER JOIN "probe_t" ON ("probe_t"."parent" = "probe_c"."t_id"))"#;
    let o = r#" ORDER BY "probe_t"."id""#;
    // (name, cache sql, binds, postgres sql with literals)
    let probes: Vec<(&str, String, Vec<Bind>, String)> = vec![
        ("ne", format!(r#"{t} WHERE "probe_t"."n" <> $1{o}"#), vec![i4b(5)], format!(r#"{t} WHERE n <> 5{o}"#)),
        ("ne_all_null", format!(r#"{t} WHERE "probe_t"."n" != ALL($1){o}"#), vec![arr4(&[Some(1), None])], format!(r#"{t} WHERE n != ALL(ARRAY[1,NULL]::int4[]){o}"#)),
        ("eq_any_null", format!(r#"{t} WHERE "probe_t"."n" = ANY($1){o}"#), vec![arr4(&[Some(1), None])], format!(r#"{t} WHERE n = ANY(ARRAY[1,NULL]::int4[]){o}"#)),
        ("ne_all_empty", format!(r#"{t} WHERE "probe_t"."n" != ALL($1){o}"#), vec![arr4(&[])], format!(r#"{t} WHERE n != ALL('{{}}'::int4[]){o}"#)),
        ("eq_any_empty", format!(r#"{t} WHERE "probe_t"."n" = ANY($1){o}"#), vec![arr4(&[])], format!(r#"{t} WHERE n = ANY('{{}}'::int4[]){o}"#)),
        ("not_eq", format!(r#"{t} WHERE NOT ("probe_t"."n" = $1){o}"#), vec![i4b(5)], format!(r#"{t} WHERE NOT (n = 5){o}"#)),
        ("not_and", format!(r#"{t} WHERE NOT (("probe_t"."n" = $1) AND ("probe_t"."b" = $2)){o}"#), vec![i4b(5), b(16, vec![0])], format!(r#"{t} WHERE NOT (n = 5 AND b = false){o}"#)),
        ("not_or", format!(r#"{t} WHERE NOT (("probe_t"."n" = $1) OR ("probe_t"."b" = $2)){o}"#), vec![i4b(5), b(16, vec![1])], format!(r#"{t} WHERE NOT (n = 5 OR b = true){o}"#)),
        ("ne_all_null_array", format!(r#"{t} WHERE "probe_t"."n" != ALL($1){o}"#), vec![null_array()], format!(r#"{t} WHERE n != ALL(NULL::int4[]){o}"#)),
        ("eq_any_null_array", format!(r#"{t} WHERE "probe_t"."n" = ANY($1){o}"#), vec![null_array()], format!(r#"{t} WHERE n = ANY(NULL::int4[]){o}"#)),
        ("not_any_null_array", format!(r#"{t} WHERE NOT ("probe_t"."n" = ANY($1)){o}"#), vec![null_array()], format!(r#"{t} WHERE NOT (n = ANY(NULL::int4[])){o}"#)),
        ("not_any", format!(r#"{t} WHERE NOT ("probe_t"."n" = ANY($1)){o}"#), vec![arr4(&[Some(1), Some(5)])], format!(r#"{t} WHERE NOT (n = ANY(ARRAY[1,5])){o}"#)),
        ("not_all_empty", format!(r#"{t} WHERE NOT ("probe_t"."n" != ALL($1)){o}"#), vec![arr4(&[])], format!(r#"{t} WHERE NOT (n != ALL('{{}}'::int4[])){o}"#)),
        ("not_between", format!(r#"{t} WHERE "probe_t"."n" NOT BETWEEN $1 AND $2{o}"#), vec![i4b(3), i4b(5)], format!(r#"{t} WHERE n NOT BETWEEN 3 AND 5{o}"#)),
        ("not_bare", format!(r#"{t} WHERE NOT "probe_t"."b"{o}"#), vec![], format!(r#"{t} WHERE NOT b{o}"#)),
        ("not_null_test", format!(r#"{t} WHERE NOT ("probe_t"."n" IS NULL){o}"#), vec![], format!(r#"{t} WHERE NOT (n IS NULL){o}"#)),
        ("not_f_gt", format!(r#"{t} WHERE NOT ("probe_t"."f" > $1){o}"#), vec![f8(1.0)], format!(r#"{t} WHERE NOT (f > 1){o}"#)),
        ("bare_and", format!(r#"{t} WHERE ("probe_t"."b" AND ("probe_t"."n" > $1)){o}"#), vec![i4b(1)], format!(r#"{t} WHERE b AND n > 1{o}"#)),
        ("eq_null_bind", format!(r#"{t} WHERE "probe_t"."n" = $1{o}"#), vec![null(23)], format!(r#"{t} WHERE n = NULL{o}"#)),
        ("ne_null_bind", format!(r#"{t} WHERE "probe_t"."n" <> $1{o}"#), vec![null(23)], format!(r#"{t} WHERE n <> NULL{o}"#)),
        ("f_zero", format!(r#"{t} WHERE "probe_t"."f" = $1{o}"#), vec![f8(0.0)], format!(r#"{t} WHERE f = 0{o}"#)),
        ("f_negzero", format!(r#"{t} WHERE "probe_t"."f" = $1{o}"#), vec![f8(-0.0)], format!(r#"{t} WHERE f = '-0'::float8{o}"#)),
        ("f_nan", format!(r#"{t} WHERE "probe_t"."f" = $1{o}"#), vec![f8(f64::NAN)], format!(r#"{t} WHERE f = 'NaN'::float8{o}"#)),
        ("f_ne_nan", format!(r#"{t} WHERE "probe_t"."f" <> $1{o}"#), vec![f8(f64::NAN)], format!(r#"{t} WHERE f <> 'NaN'::float8{o}"#)),
        ("f_gt", format!(r#"{t} WHERE "probe_t"."f" > $1{o}"#), vec![f8(1.0)], format!(r#"{t} WHERE f > 1{o}"#)),
        ("f_lt_nan", format!(r#"{t} WHERE "probe_t"."f" < $1{o}"#), vec![f8(f64::NAN)], format!(r#"{t} WHERE f < 'NaN'::float8{o}"#)),
        ("f_ge_inf", format!(r#"{t} WHERE "probe_t"."f" >= $1{o}"#), vec![f8(f64::INFINITY)], format!(r#"{t} WHERE f >= 'Infinity'::float8{o}"#)),
        ("order_f", format!(r#"{t} ORDER BY "probe_t"."f", "probe_t"."id""#), vec![], format!(r#"{t} ORDER BY f, id"#)),
        ("order_f_desc", format!(r#"{t} ORDER BY "probe_t"."f" DESC, "probe_t"."id""#), vec![], format!(r#"{t} ORDER BY f DESC, id"#)),
        ("order_f_desc_lim", format!(r#"{t} ORDER BY "probe_t"."f" DESC, "probe_t"."id" LIMIT $1"#), vec![i8b(3)], format!(r#"{t} ORDER BY f DESC, id LIMIT 3"#)),
        ("order_n_desc_lim", format!(r#"{t} ORDER BY "probe_t"."n" DESC, "probe_t"."id" LIMIT $1 OFFSET $2"#), vec![i8b(3), i8b(1)], format!(r#"{t} ORDER BY n DESC, id LIMIT 3 OFFSET 1"#)),
        ("order_n_lim", format!(r#"{t} ORDER BY "probe_t"."n", "probe_t"."id" DESC LIMIT $1"#), vec![i8b(4)], format!(r#"{t} ORDER BY n, id DESC LIMIT 4"#)),
        ("order_t", format!(r#"{t} ORDER BY "probe_t"."t", "probe_t"."id""#), vec![], format!(r#"{t} ORDER BY t, id"#)),
        ("order_t_desc", format!(r#"{t} ORDER BY "probe_t"."t" DESC, "probe_t"."id""#), vec![], format!(r#"{t} ORDER BY t DESC, id"#)),
        ("t_gt", format!(r#"{t} WHERE "probe_t"."t" > $1{o}"#), vec![txt("a")], format!(r#"{t} WHERE t > 'a'{o}"#)),
        ("t_eq_empty", format!(r#"{t} WHERE "probe_t"."t" = $1{o}"#), vec![txt("")], format!(r#"{t} WHERE t = ''{o}"#)),
        ("t_eq_trail", format!(r#"{t} WHERE "probe_t"."t" = $1{o}"#), vec![txt("a")], format!(r#"{t} WHERE t = 'a'{o}"#)),
        ("order_ts", format!(r#"{t} ORDER BY "probe_t"."ts", "probe_t"."id""#), vec![], format!(r#"{t} ORDER BY ts, id"#)),
        ("order_tz_desc", format!(r#"{t} ORDER BY "probe_t"."tz" DESC, "probe_t"."id""#), vec![], format!(r#"{t} ORDER BY tz DESC, id"#)),
        // 2026-01-01 00:00:00+00 = 820540800 s after 2000-01-01
        ("ts_vs_tz_bind", format!(r#"{t} WHERE "probe_t"."ts" = $1{o}"#), vec![tz(820_540_800_000_000)], format!(r#"{t} WHERE ts = '2026-01-01 00:00:00+00'::timestamptz{o}"#)),
        ("ts_ge_tz_bind", format!(r#"{t} WHERE "probe_t"."ts" >= $1{o}"#), vec![tz(820_540_800_000_000)], format!(r#"{t} WHERE ts >= '2026-01-01 00:00:00+00'::timestamptz{o}"#)),
        ("tz_eq", format!(r#"{t} WHERE "probe_t"."tz" = $1{o}"#), vec![tz(820_540_800_000_000)], format!(r#"{t} WHERE tz = '2026-01-01 00:00:00+00'{o}"#)),
        ("tz_lt_inf", format!(r#"{t} WHERE "probe_t"."tz" < $1{o}"#), vec![tz(i64::MAX)], format!(r#"{t} WHERE tz < 'infinity'{o}"#)),
        ("num_eq_int", format!(r#"{t} WHERE "probe_t"."num" = $1{o}"#), vec![i8b(1)], format!(r#"{t} WHERE num = 1{o}"#)),
        ("order_num", format!(r#"{t} ORDER BY "probe_t"."num" DESC, "probe_t"."id""#), vec![], format!(r#"{t} ORDER BY num DESC, id"#)),
        ("n_lt_big", format!(r#"{t} WHERE "probe_t"."n" < $1{o}"#), vec![i8b(1 << 40)], format!(r#"{t} WHERE n < 1099511627776{o}"#)),
        ("n_eq_f", format!(r#"{t} WHERE "probe_t"."n" = $1{o}"#), vec![f8(5.0)], format!(r#"{t} WHERE n = 5.0::float8{o}"#)),
        ("between", format!(r#"{t} WHERE "probe_t"."n" BETWEEN $1 AND $2{o}"#), vec![i4b(3), i4b(5)], format!(r#"{t} WHERE n BETWEEN 3 AND 5{o}"#)),
        ("null_or_gt", format!(r#"{t} WHERE (("probe_t"."n" IS NULL) OR ("probe_t"."n" > $1)){o}"#), vec![i4b(4)], format!(r#"{t} WHERE n IS NULL OR n > 4{o}"#)),
        ("and_or_prec", format!(r#"{t} WHERE "probe_t"."n" = $1 OR "probe_t"."n" = $2 AND "probe_t"."b" = $3{o}"#), vec![i4b(1), i4b(5), b(16, vec![0])], format!(r#"{t} WHERE n = 1 OR n = 5 AND b = false{o}"#)),
        ("b_true", format!(r#"{t} WHERE "probe_t"."b" = $1{o}"#), vec![b(16, vec![1])], format!(r#"{t} WHERE b = true{o}"#)),
        ("b_bare", format!(r#"{t} WHERE "probe_t"."b"{o}"#), vec![], format!(r#"{t} WHERE b{o}"#)),
        ("b_ne", format!(r#"{t} WHERE "probe_t"."b" <> $1{o}"#), vec![b(16, vec![1])], format!(r#"{t} WHERE b <> true{o}"#)),
        ("limit0", format!(r#"{t}{o} LIMIT $1"#), vec![i8b(0)], format!(r#"{t}{o} LIMIT 0"#)),
        ("offset_far", format!(r#"{t}{o} LIMIT $1 OFFSET $2"#), vec![i8b(5), i8b(100)], format!(r#"{t}{o} LIMIT 5 OFFSET 100"#)),
        ("offset_only", format!(r#"{t}{o} OFFSET $1"#), vec![i8b(6)], format!(r#"{t}{o} OFFSET 6"#)),
        ("count", r#"SELECT COUNT(*) FROM "probe_t" WHERE "probe_t"."n" <> $1"#.to_owned(), vec![i4b(5)], "SELECT COUNT(*) FROM probe_t WHERE n <> 5".to_owned()),
        ("count_lim", r#"SELECT COUNT(*) FROM "probe_t" LIMIT $1"#.to_owned(), vec![i8b(0)], "SELECT COUNT(*) FROM probe_t LIMIT 0".to_owned()),
        ("inner", format!(r#"{jc}{o}"#), vec![], format!(r#"{jc}{o}"#)),
        ("inner_lim", format!(r#"{jc}{o} LIMIT $1"#), vec![i8b(3)], format!(r#"{jc}{o}, probe_c.id LIMIT 3"#)),
        ("inner_lim_off", format!(r#"{jc}{o} LIMIT $1 OFFSET $2"#), vec![i8b(2), i8b(2)], format!(r#"{jc}{o}, probe_c.id LIMIT 2 OFFSET 2"#)),
        ("inner_desc_lim", format!(r#"{jc} ORDER BY "probe_t"."id" DESC LIMIT $1"#), vec![i8b(3)], format!(r#"{jc} ORDER BY probe_t.id DESC, probe_c.id LIMIT 3"#)),
        ("inner_count", r#"SELECT COUNT(*) FROM ("probe_t" INNER JOIN "probe_c" ON ("probe_c"."t_id" = "probe_t"."id"))"#.to_owned(), vec![], "SELECT COUNT(*) FROM probe_t JOIN probe_c ON probe_c.t_id = probe_t.id".to_owned()),
        ("inner_where_child", format!(r#"{jc} WHERE "probe_c"."v" > $1{o}"#), vec![i4b(2)], format!(r#"{jc} WHERE probe_c.v > 2{o}"#)),
        ("inner_where_child_lim", format!(r#"{jc} WHERE "probe_c"."v" > $1{o} LIMIT $2"#), vec![i4b(2), i8b(2)], format!(r#"{jc} WHERE probe_c.v > 2{o}, probe_c.id LIMIT 2"#)),
        ("left", format!(r#"{jl}{o}"#), vec![], format!(r#"{jl}{o}"#)),
        ("left_lim", format!(r#"{jl}{o} LIMIT $1 OFFSET $2"#), vec![i8b(4), i8b(2)], format!(r#"{jl}{o}, probe_c.id LIMIT 4 OFFSET 2"#)),
        ("left_count", r#"SELECT COUNT(*) FROM ("probe_t" LEFT OUTER JOIN "probe_c" ON ("probe_c"."t_id" = "probe_t"."id"))"#.to_owned(), vec![], "SELECT COUNT(*) FROM probe_t LEFT JOIN probe_c ON probe_c.t_id = probe_t.id".to_owned()),
        ("left_on_extra", r#"SELECT "probe_t"."id" FROM ("probe_t" LEFT OUTER JOIN "probe_c" ON (("probe_c"."t_id" = "probe_t"."id") AND ("probe_c"."v" > $1))) ORDER BY "probe_t"."id""#.to_owned(), vec![i4b(1)], "SELECT probe_t.id FROM probe_t LEFT JOIN probe_c ON probe_c.t_id = probe_t.id AND probe_c.v > 1 ORDER BY probe_t.id".to_owned()),
        ("child_root", format!(r#"{jcp} ORDER BY "probe_c"."id""#), vec![], format!(r#"{jcp} ORDER BY probe_c.id"#)),
        ("child_root_lim", format!(r#"{jcp} ORDER BY "probe_c"."id" LIMIT $1"#), vec![i8b(2)], format!(r#"{jcp} ORDER BY probe_c.id LIMIT 2"#)),
        ("nonkey_join", format!(r#"{jself} ORDER BY "probe_c"."id""#), vec![], format!(r#"{jself} ORDER BY probe_c.id, probe_t.id"#)),
        ("nonkey_join_count", r#"SELECT COUNT(*) FROM ("probe_c" INNER JOIN "probe_t" ON ("probe_t"."parent" = "probe_c"."t_id"))"#.to_owned(), vec![], "SELECT COUNT(*) FROM probe_c JOIN probe_t ON probe_t.parent = probe_c.t_id".to_owned()),
    ];
    let compare = |phase: &str| -> Vec<String> {
        let mut problems = Vec::new();
        for (name, sql, binds, pg) in &probes {
            let truth: Vec<i64> = rt
                .block_on(client.query(pg.as_str(), &[]))
                .unwrap_or_else(|error| panic!("{name}: {error}"))
                .iter()
                .map(|row| row.get::<_, i64>(0))
                .collect();
            match cache.read(sql, binds, &reader) {
                Ok(rows) => {
                    let got: Vec<i64> = rows
                        .rows()
                        .iter()
                        .map(|row| match &row[0] {
                            Value::Int(id) => *id,
                            other => panic!("{name}: {other:?}"),
                        })
                        .collect();
                    if got != truth {
                        problems.push(format!("{phase} {name}: cache {got:?}, postgres {truth:?}"));
                    } else if REFUSED.contains(name) {
                        problems.push(format!("{phase} {name}: answered, expected a refusal"));
                    }
                }
                Err(_) if REFUSED.contains(name) => {}
                Err(refusal) => problems.push(format!("{phase} {name}: {refusal}")),
            }
        }
        problems
    };
    let mut problems = compare("fresh");
    rt.block_on(client.batch_execute(
        "INSERT INTO probe_t VALUES (9, NULL, 'NaN', 'c', true, NULL, NULL, 'NaN', 2), (10, 5, 3.5, NULL, NULL, '2026-01-01 00:00:00', '2026-01-01 00:00:00+00', 1.0, NULL);
         UPDATE probe_t SET f = 'NaN', n = NULL, b = NOT b WHERE id = 1;
         UPDATE probe_t SET f = 0, t = 'a' WHERE id = 3;
         DELETE FROM probe_t WHERE id = 7;
         INSERT INTO probe_c VALUES (17, 9, 8), (18, 1, 9), (19, 10, 1);
         UPDATE probe_c SET t_id = 2 WHERE id = 13;
         DELETE FROM probe_c WHERE id = 12;",
    ))
    .unwrap();
    let at: String = rt
        .block_on(client.query_one("SELECT pg_current_wal_lsn()::text", &[]))
        .unwrap()
        .get(0);
    assert!(cache.wait_for(
        Position::from(streamgres::sync::pg::Lsn::parse(&at).unwrap()),
        Duration::from_secs(10)
    ));
    // A join's rows settle a moment after the transaction itself.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut after = compare("after writes");
    while !after.is_empty() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
        after = compare("after writes");
    }
    problems.extend(after);
    cache.stop();
    rt.block_on(client.batch_execute(&format!(
        "{teardown} DROP PUBLICATION probe_pub; DROP TABLE probe_c; DROP TABLE probe_t;"
    )))
    .unwrap();
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}
