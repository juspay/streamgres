//! `load_cached` against a live Postgres: for every query shape and
//! column type, the cache answers (it does not fall back) and its rows
//! equal what `.load` reads from Postgres, before and after writes. Runs
//! only when `STREAMGRES_PG_DSN` names a database with
//! `wal_level = logical`.

use std::str::FromStr;
use std::time::Duration;

use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use diesel::connection::SimpleConnection;
use diesel::dsl::count_star;
use diesel::prelude::*;
use streamgres_diesel::{Cache, CacheConfig, LoadCached, Tracker};

diesel::table! {
    adapter_orders (id) {
        id -> Int8,
        merchant -> Text,
        quantity -> Int4,
        rank -> Int2,
        paid -> Bool,
        ratio -> Float8,
        amount -> Numeric,
        created_at -> Timestamptz,
        local_at -> Timestamp,
        due -> Date,
        payload -> Bytea,
        meta -> Jsonb,
        tags -> Array<Int4>,
        note -> Nullable<Text>,
    }
}

#[derive(Debug, Clone, PartialEq, Queryable, Selectable)]
#[diesel(table_name = adapter_orders)]
struct Order {
    id: i64,
    merchant: String,
    quantity: i32,
    rank: i16,
    paid: bool,
    ratio: f64,
    amount: BigDecimal,
    created_at: DateTime<Utc>,
    local_at: NaiveDateTime,
    due: NaiveDate,
    payload: Vec<u8>,
    meta: serde_json::Value,
    tags: Vec<i32>,
    note: Option<String>,
}

const PUBLICATION: &str = "adapter_orders_pub";

/// Asserts the cache answers `query` itself, with Postgres's rows.
macro_rules! same {
    ($cache:expr, $conn:expr, $query:expr, $row:ty) => {{
        let live: Vec<$row> = $query.load($conn).expect("from Postgres");
        let cached: Option<Vec<$row>> = $query.try_load_cached($cache, $conn).expect("rendered");
        let sql = diesel::debug_query::<diesel::pg::Pg, _>(&$query).to_string();
        assert_eq!(cached.as_ref(), Some(&live), "{sql}");
        live
    }};
}

#[test]
fn load_cached_returns_what_load_returns() {
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        eprintln!("STREAMGRES_PG_DSN not set: live adapter scenario skipped");
        return;
    };
    let mut conn = PgConnection::establish(&dsn).expect("connect");
    let mut config = CacheConfig::new(dsn.clone(), PUBLICATION);
    config.app = "streamgres_adapter".to_owned();
    conn.batch_execute(&format!(
        "DROP PUBLICATION IF EXISTS {PUBLICATION};
         DROP TABLE IF EXISTS adapter_orders;
         CREATE TABLE adapter_orders (
           id int8 PRIMARY KEY, merchant text COLLATE \"C\" NOT NULL, quantity int4 NOT NULL,
           rank int2 NOT NULL, paid bool NOT NULL, ratio float8 NOT NULL,
           amount numeric(14, 4) NOT NULL, created_at timestamptz NOT NULL,
           local_at timestamp NOT NULL, due date NOT NULL, payload bytea NOT NULL,
           meta jsonb NOT NULL, tags int4[] NOT NULL, note text);
         CREATE PUBLICATION {PUBLICATION} FOR TABLE adapter_orders;"
    ))
    .expect("prepare");
    conn.batch_execute(&config.setup_sql())
        .expect("install the trigger stack");
    let teardown = config.teardown_sql();

    let order = |id: i64, merchant: &str, amount: &str, note: Option<&str>| Order {
        id,
        merchant: merchant.to_owned(),
        quantity: i32::try_from(id).unwrap() * 3,
        rank: i16::try_from(id % 5).unwrap(),
        paid: id % 2 == 0,
        ratio: 0.1 * id as f64,
        amount: BigDecimal::from_str(amount).unwrap(),
        created_at: DateTime::from_timestamp_micros(1_757_000_000_123_456 + id).unwrap(),
        local_at: DateTime::from_timestamp_micros(1_700_000_000_000_001 * id)
            .unwrap()
            .naive_utc(),
        due: NaiveDate::from_ymd_opt(2026, 9, 1 + u32::try_from(id).unwrap()).unwrap(),
        payload: vec![0, 255, u8::try_from(id).unwrap()],
        meta: serde_json::json!({"id": id, "price": 1.5, "tags": ["a", "b"]}),
        tags: vec![1, 2, i32::try_from(id).unwrap()],
        note: note.map(str::to_owned),
    };
    let insert = |conn: &mut PgConnection, row: &Order| {
        diesel::insert_into(adapter_orders::table)
            .values((
                adapter_orders::id.eq(row.id),
                adapter_orders::merchant.eq(&row.merchant),
                adapter_orders::quantity.eq(row.quantity),
                adapter_orders::rank.eq(row.rank),
                adapter_orders::paid.eq(row.paid),
                adapter_orders::ratio.eq(row.ratio),
                adapter_orders::amount.eq(&row.amount),
                adapter_orders::created_at.eq(row.created_at),
                adapter_orders::local_at.eq(row.local_at),
                adapter_orders::due.eq(row.due),
                adapter_orders::payload.eq(&row.payload),
                adapter_orders::meta.eq(&row.meta),
                adapter_orders::tags.eq(&row.tags),
                adapter_orders::note.eq(&row.note),
            ))
            .execute(conn)
            .expect("insert");
    };
    for row in [
        order(1, "m1", "10.5000", Some("first")),
        order(2, "m1", "3.0000", None),
        order(3, "m2", "1234567890.1234", Some("third")),
        order(4, "m1", "-0.0001", None),
    ] {
        insert(&mut conn, &row);
    }

    let cache = Cache::start(config);
    cache
        .wait_ready(Duration::from_secs(30))
        .expect("the engine comes up");

    let check = |cache: &Cache, conn: &mut PgConnection| {
        use adapter_orders::dsl::{amount, created_at, id, merchant, note, paid, quantity};
        let orders = adapter_orders::table;
        let all = same!(
            cache,
            conn,
            orders.select(Order::as_select()).order(id),
            Order
        );
        same!(
            cache,
            conn,
            orders
                .filter(merchant.eq("m1"))
                .select(Order::as_select())
                .order(amount.desc()),
            Order
        );
        same!(
            cache,
            conn,
            orders
                .filter(id.eq_any(vec![1i64, 3, 9]))
                .select((id, amount, note))
                .order(id),
            (i64, BigDecimal, Option<String>)
        );
        same!(
            cache,
            conn,
            orders
                .filter(
                    merchant
                        .eq("m1")
                        .and(paid.eq(false).or(amount.gt(BigDecimal::from(5))))
                )
                .select(id)
                .order((merchant.asc(), id.desc())),
            i64
        );
        same!(
            cache,
            conn,
            orders
                .filter(
                    created_at.ge(DateTime::from_timestamp_micros(1_757_000_000_123_458).unwrap())
                )
                .filter(note.is_null())
                .select(id)
                .order(created_at.desc()),
            i64
        );
        same!(
            cache,
            conn,
            orders
                .filter(quantity.between(3, 9))
                .filter(id.ne_all(vec![2i64]))
                .select(id)
                .order(id)
                .limit(5)
                .offset(1),
            i64
        );
        same!(cache, conn, orders.select(count_star()), i64);
        same!(
            cache,
            conn,
            orders.filter(merchant.eq("m1")).select(count_star()),
            i64
        );
        same!(
            cache,
            conn,
            orders.find(3i64).select(Order::as_select()).limit(1),
            Order
        );
        all
    };

    let mut untracked = PgConnection::establish(&dsn).expect("connect");
    assert_eq!(
        adapter_orders::table
            .select(adapter_orders::id)
            .try_load_cached(&cache, &mut untracked)
            .expect("rendered"),
        None::<Vec<i64>>,
        "a connection without a tracker is never answered from the cache"
    );
    conn.set_instrumentation(Tracker::new());
    let before = check(&cache, &mut conn);
    assert_eq!(before.len(), 4);

    insert(&mut conn, &order(5, "m1", "7.7700", Some("fifth")));
    diesel::update(adapter_orders::table.find(2i64))
        .set((
            adapter_orders::amount.eq(BigDecimal::from_str("99.9900").unwrap()),
            adapter_orders::note.eq(Some("now noted")),
        ))
        .execute(&mut conn)
        .expect("update");
    diesel::delete(adapter_orders::table.find(1i64))
        .execute(&mut conn)
        .expect("delete");
    assert!(
        streamgres_diesel::wait_for_writes(&cache, &mut conn, Duration::from_secs(10)).expect("read the WAL"),
        "the engine reaches the writes"
    );
    let after = check(&cache, &mut conn);
    assert_eq!(after.len(), 4);

    let inside: Vec<i64> = conn
        .transaction(|conn| {
            insert(conn, &order(6, "m1", "1.0000", None));
            assert_eq!(
                adapter_orders::table
                    .select(adapter_orders::id)
                    .try_load_cached(&cache, conn)?,
                None::<Vec<i64>>,
                "a transaction's reads are never the cache's"
            );
            let ids: Vec<i64> = adapter_orders::table
                .select(adapter_orders::id)
                .order(adapter_orders::id)
                .load_cached(&cache, conn)?;
            Ok::<_, diesel::result::Error>(ids)
        })
        .expect("read inside a transaction");
    assert!(inside.contains(&6), "the transaction's own insert is seen");

    same!(
        &cache,
        &mut conn,
        adapter_orders::table
            .select(adapter_orders::meta)
            .order(adapter_orders::id),
        serde_json::Value
    );

    conn.batch_execute(
        "CREATE SCHEMA adapter_shadow; CREATE TABLE adapter_shadow.adapter_orders (id int8 PRIMARY KEY);
         SET search_path = adapter_shadow, public;",
    )
    .expect("shadow the table");
    assert_eq!(
        adapter_orders::table
            .select(adapter_orders::id)
            .try_load_cached(&cache, &mut conn)
            .expect("rendered"),
        None::<Vec<i64>>,
        "the search path now names another table"
    );
    conn.batch_execute("RESET search_path; DROP SCHEMA adapter_shadow CASCADE;")
        .expect("unshadow");
    same!(
        &cache,
        &mut conn,
        adapter_orders::table
            .select(adapter_orders::id)
            .order(adapter_orders::id),
        i64
    );

    let filtered: Vec<i64> = adapter_orders::table
        .filter(adapter_orders::note.like("%noted"))
        .select(adapter_orders::id)
        .load_cached(&cache, &mut conn)
        .expect("refused by the cache, read from Postgres");
    assert_eq!(filtered, vec![2]);

    cache.stop();
    conn.batch_execute(&format!(
        "{teardown} DROP PUBLICATION IF EXISTS {PUBLICATION}; DROP TABLE IF EXISTS adapter_orders;"
    ))
    .expect("clean up");
}
