//! Contract tests against a real Postgres; skipped unless `STREAMLIB_PG_DSN` is set.

use std::sync::Arc;
use std::time::Duration;

use streamlib_datasource::testing::Mirror;
use streamlib_datasource::{
    Changes, Checked, Dir, Filter, Lifecycle, Op, Pred, Query, Snapshots, SourceError, Value,
    ValueType,
};
use streamlib_datasource_postgres::{Lsn, PgCodec, PgConfig, PgSource, PgType, TextCodec};
use tokio_postgres::Client;

const SETTLE: Duration = Duration::from_secs(20);
/// A value big enough that Postgres stores it out of line (TOAST).
const BIG: &str = "repeat(md5(id::text), 400)";

struct Db {
    src: Arc<Checked<PgSource>>,
    sql: Client,
    table: String,
    publication: String,
    prefix: String,
}

impl Db {
    async fn new(columns: &str) -> Option<Db> {
        let db = Db::unopened(columns, Arc::new(TextCodec)).await?;
        db.src.open().await.unwrap();
        Some(db)
    }

    /// Creates the table and publication without opening the source.
    async fn unopened(columns: &str, codec: Arc<dyn PgCodec>) -> Option<Db> {
        Db::custom(columns, None, codec, |_| {}).await
    }

    /// Like `unopened`, with an optional `CREATE PUBLICATION` clause and config changes.
    async fn custom(
        columns: &str,
        publication_clause: Option<&str>,
        codec: Arc<dyn PgCodec>,
        tweak: impl FnOnce(&mut PgConfig),
    ) -> Option<Db> {
        let Ok(dsn) = std::env::var("STREAMLIB_PG_DSN") else {
            eprintln!("skipped: STREAMLIB_PG_DSN not set");
            return None;
        };
        let tag = uuid::Uuid::new_v4().simple().to_string()[..8].to_owned();
        let name = format!("sl_items_{tag}");
        let table = format!("public.{name}");
        let publication = format!("sl_pub_{tag}");

        let (sql, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(conn);
        sql.batch_execute(&format!("CREATE TABLE {name} ({columns})"))
            .await
            .unwrap();
        if let Some(clause) = publication_clause {
            let clause = clause.replace("$T", &table);
            sql.batch_execute(&format!("CREATE PUBLICATION {publication} {clause}"))
                .await
                .unwrap();
        }

        let mut cfg = PgConfig::new(&dsn, &publication);
        cfg.tables = vec![table.clone()];
        // Own slot names, so tests that kill slots only touch their own.
        cfg.slot_prefix = format!("sl_{tag}");
        tweak(&mut cfg);
        let src = Arc::new(Checked::new(PgSource::with_codec(cfg, codec).unwrap()));
        src.setup().await.unwrap();
        Some(Db {
            src,
            sql,
            table,
            publication,
            prefix: format!("sl_{tag}"),
        })
    }

    async fn items() -> Option<Db> {
        let db = Db::new("id int PRIMARY KEY, status text, body text, n int").await?;
        db.exec(&format!(
            "ALTER TABLE {} ALTER body SET STORAGE EXTERNAL",
            db.table
        ))
        .await;
        Some(db)
    }

    async fn exec(&self, sql: &str) {
        let sql = sql.replace("$T", &self.table);
        self.sql
            .batch_execute(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    async fn insert(&self, id: i64, status: &str) {
        self.exec(&format!(
            "INSERT INTO $T VALUES ({id}, '{status}', {}, 0)",
            BIG.replace("id", &id.to_string())
        ))
        .await;
    }

    fn query(&self) -> Query {
        Query::table(&self.table)
    }

    fn open_items(&self) -> Query {
        self.query()
            .filter(Filter::and(vec![Pred::cmp("status", Op::Eq, "open")]))
    }

    /// The big body Postgres holds for row `id`.
    async fn big(&self, id: i64) -> Value {
        let text: String = self
            .sql
            .query_one(
                &format!("SELECT {}", BIG.replace("id", &id.to_string())),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        Value::Text(text)
    }

    async fn drop(self) {
        self.src.close().await;
        self.exec(&format!(
            "DROP PUBLICATION IF EXISTS {}; DROP TABLE IF EXISTS $T",
            self.publication
        ))
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn converges_under_concurrent_writes() {
    let Some(db) = Db::items().await else { return };
    for id in 0..20 {
        db.insert(id, "open").await;
    }
    let mut mirror = Mirror::start(db.src.clone(), db.open_items())
        .await
        .unwrap();

    let writer = {
        let dsn = std::env::var("STREAMLIB_PG_DSN").unwrap();
        let table = db.table.clone();
        tokio::spawn(async move {
            let (sql, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .unwrap();
            tokio::spawn(conn);
            let mut seed: u64 = 11;
            for i in 0..300 {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let id = (seed >> 33) % 40;
                let status = if seed & 1 == 0 { "open" } else { "closed" };
                let stmt = match (seed >> 8) % 5 {
                    0 => format!("DELETE FROM {table} WHERE id = {id}"),
                    1 => format!(
                        "INSERT INTO {table} VALUES ({id}, '{status}', repeat(md5('{i}'), 400), {i}) \
                         ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, n = EXCLUDED.n"
                    ),
                    2 => format!(
                        "UPDATE {table} SET body = repeat(md5('{i}x'), 400) WHERE id = {id}"
                    ),
                    _ => format!("UPDATE {table} SET status = '{status}', n = {i} WHERE id = {id}"),
                };
                sql.batch_execute(&stmt).await.unwrap();
            }
        })
    };
    while !writer.is_finished() {
        mirror.pump(Duration::from_millis(20)).await.unwrap();
    }
    writer.await.unwrap();
    mirror.settle(SETTLE).await.unwrap();
    eprintln!("stats: {:?}", mirror.stats);
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn toast_update_keeps_body() {
    let Some(db) = Db::items().await else { return };
    db.insert(1, "open").await;
    let mut mirror = Mirror::start(db.src.clone(), db.open_items())
        .await
        .unwrap();

    db.exec("UPDATE $T SET n = 5 WHERE id = 1").await;
    mirror.settle(SETTLE).await.unwrap();
    let view = mirror.view();
    assert_eq!(view[0]["body"], db.big(1).await);
    assert_eq!(view[0]["n"], Value::Int(5));
    assert_eq!(mirror.stats.completed, 1, "filled from the held row");
    db.drop().await;
}

/// Known bug 1: the filter uses the big column and we don't hold the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filter_on_toasted_column() {
    let Some(db) = Db::items().await else { return };
    db.insert(1, "closed").await;
    let big = db.big(1).await;
    let query = db.query().filter(Filter::and(vec![
        Pred::cmp("status", Op::Eq, "open"),
        Pred::Cmp {
            column: "body".into(),
            op: Op::Eq,
            value: big.clone(),
        },
    ]));
    let mut mirror = Mirror::start(db.src.clone(), query).await.unwrap();
    assert!(mirror.view().is_empty());

    db.exec("UPDATE $T SET status = 'open' WHERE id = 1").await;
    mirror.settle(SETTLE).await.unwrap();
    assert_eq!(mirror.view().len(), 1);
    assert_eq!(mirror.stats.rereads, 1);
    db.drop().await;
}

/// Known bug 2: the primary key changes while the big column is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_change_with_toast() {
    for status in ["open", "closed"] {
        let Some(db) = Db::items().await else { return };
        db.insert(1, status).await;
        let mut mirror = Mirror::start(db.src.clone(), db.open_items())
            .await
            .unwrap();

        db.exec("UPDATE $T SET id = 2, status = 'open' WHERE id = 1")
            .await;
        mirror.settle(SETTLE).await.unwrap();
        let view = mirror.view();
        assert_eq!(view.len(), 1, "held = {}", status == "open");
        assert_eq!(view[0]["id"], Value::Int(2));
        assert_eq!(view[0]["body"], db.big(1).await);
        db.drop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_and_truncate() {
    let Some(db) = Db::items().await else { return };
    for id in 0..3 {
        db.insert(id, "open").await;
    }
    let mut mirror = Mirror::start(db.src.clone(), db.open_items())
        .await
        .unwrap();

    db.exec("DELETE FROM $T WHERE id = 0").await;
    mirror.settle(SETTLE).await.unwrap();
    assert_eq!(mirror.view().len(), 2);

    db.exec("TRUNCATE $T").await;
    mirror.settle(SETTLE).await.unwrap();
    assert!(mirror.view().is_empty());
    db.drop().await;
}

/// Ordered reads must sort text by bytes like the engine, not by Postgres collation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_limit_follows_engine_rules() {
    let Some(db) = Db::items().await else { return };
    for (id, status) in [(1, "b"), (2, "B"), (3, "a"), (4, "C"), (5, "_")] {
        db.insert(id, status).await;
    }
    let query = db.query().order("status", Dir::Asc).limit(3);
    let mut mirror = Mirror::start(db.src.clone(), query.clone()).await.unwrap();
    db.insert(6, "A").await;
    mirror.settle(SETTLE).await.unwrap();
    let order: Vec<_> = mirror.view().iter().map(|r| r["status"].clone()).collect();
    assert_eq!(
        order,
        vec![Value::from("A"), Value::from("B"), Value::from("C")]
    );
    db.drop().await;
}

/// A row read from the stream and from a snapshot must decode the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn codec_parity() {
    let Some(db) = Db::new(
        "id int8 PRIMARY KEY, b bool, i int4, s int2, f float8, r float4, num numeric, \
         ts timestamptz, d date, j jsonb, t text, u uuid, nul text",
    )
    .await
    else {
        return;
    };
    let mut feed = db.src.stream(None).await.unwrap();
    db.src.advance(&feed.start);
    db.exec(
        "INSERT INTO $T VALUES (1, true, -7, 3, 0.1, 'NaN', 123456789.123456789, \
         '2026-01-02 03:04:05.678+05:30', '2026-01-02', '{\"b\": [1, 2.50], \"a\": null}', \
         'héllo''\"', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', NULL)",
    )
    .await;
    let streamed = loop {
        let txn = feed.next().await.unwrap();
        db.src.advance(&txn.position);
        if let Some(streamlib_datasource::Change::Insert { row, .. }) =
            txn.changes.into_iter().next()
        {
            break row;
        }
    };
    // Keep pumping while the first snapshot becomes usable.
    let read = {
        let query = db.query();
        let select = db.src.select(&query);
        tokio::pin!(select);
        loop {
            tokio::select! {
                snap = &mut select => break snap.unwrap().rows.into_iter().next().unwrap(),
                txn = feed.next() => db.src.advance(&txn.unwrap().position),
            }
        }
    };
    assert_eq!(streamed, read);
    assert_eq!(read["f"], Value::Float(0.1));
    assert_eq!(read["nul"], Value::Null);
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_and_close() {
    let Some(db) = Db::items().await else { return };
    let slot = db.src.inner().slot().unwrap();
    let mut feed = db.src.stream(None).await.unwrap();

    // Acknowledge a commit, so resuming from before it must fail.
    db.insert(1, "open").await;
    let txn = loop {
        let txn = feed.next().await.unwrap();
        if !txn.is_mark() {
            break txn;
        }
    };
    db.src.ack(&txn.position);
    tokio::time::sleep(Duration::from_secs(3)).await; // status updates go out each second
    drop(feed);
    let resumed = db.src.stream(Some(&Lsn(1))).await;
    assert!(
        matches!(resumed, Err(SourceError::CannotResume(_))),
        "{:?}",
        resumed.err()
    );

    let check = format!("SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'");
    let sql = db.sql.query_one(&check, &[]).await.unwrap();
    assert_eq!(sql.get::<_, i64>(0), 1);
    db.src.close().await;
    let sql = db.sql.query_one(&check, &[]).await.unwrap();
    assert_eq!(sql.get::<_, i64>(0), 0, "close drops the feed slot");
    assert!(matches!(
        db.src.stream(None).await,
        Err(SourceError::Closed)
    ));
    db.drop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn counts_with_cap() {
    let Some(db) = Db::items().await else { return };
    for id in 0..5 {
        db.insert(id, "open").await;
    }
    db.insert(9, "closed").await;
    assert_eq!(db.src.count(&db.open_items(), 3).await.unwrap(), 3);
    assert_eq!(db.src.count(&db.open_items(), 100).await.unwrap(), 5);
    db.drop().await;
}

/// The default codec refuses unknown types when the source opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_unsupported_type_at_open() {
    let Some(db) = Db::unopened("id int PRIMARY KEY, tags int4[]", Arc::new(TextCodec)).await
    else {
        return;
    };
    let opened = db.src.open().await;
    assert!(
        matches!(&opened, Err(SourceError::Refused(m)) if m.contains("_int4")),
        "{opened:?}"
    );
    db.drop().await;
}

/// A custom codec can be plugged in without changing the source.
struct ArraysAsText;

impl PgCodec for ArraysAsText {
    fn value_type(&self, ty: &PgType) -> Option<ValueType> {
        TextCodec
            .value_type(ty)
            .or_else(|| ty.name.starts_with('_').then_some(ValueType::Text))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn custom_codec() {
    let Some(db) = Db::unopened("id int PRIMARY KEY, tags int4[]", Arc::new(ArraysAsText)).await
    else {
        return;
    };
    db.src.open().await.unwrap();
    db.exec("INSERT INTO $T VALUES (1, '{3,1,2}')").await;
    let mut mirror = Mirror::start(db.src.clone(), db.query()).await.unwrap();
    db.exec("UPDATE $T SET tags = '{4}' WHERE id = 1").await;
    mirror.settle(SETTLE).await.unwrap();
    assert_eq!(mirror.view()[0]["tags"], Value::from("{4}"));
    db.drop().await;
}

// Bugs found in review: each test fails without its fix.

fn refused<T: std::fmt::Debug>(r: Result<T, SourceError>, needle: &str) -> bool {
    matches!(&r, Err(SourceError::Refused(m)) if m.contains(needle))
}

/// Postgres treats -0 as 0 and NaN as equal to itself and above every number; so must the engine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn floats_follow_postgres() {
    let Some(db) = Db::new("id int PRIMARY KEY, f float8").await else {
        return;
    };
    db.exec("INSERT INTO $T VALUES (1, '-0'), (2, 'NaN'), (3, 1.5)")
        .await;
    let zero = db
        .query()
        .filter(Filter::and(vec![Pred::cmp("f", Op::Eq, 0.0)]));
    let mut mirror = Mirror::start(db.src.clone(), zero).await.unwrap();
    mirror.settle(SETTLE).await.unwrap();
    assert_eq!(mirror.view().len(), 1);

    let top = db.query().order("f", Dir::Desc).limit(1);
    let mut mirror = Mirror::start(db.src.clone(), top).await.unwrap();
    mirror.settle(SETTLE).await.unwrap();
    assert_eq!(mirror.view()[0]["id"], Value::Int(2));
    db.drop().await;
}

/// A cap above Postgres's bigint range used to render an invalid LIMIT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn count_with_huge_cap() {
    let Some(db) = Db::items().await else { return };
    db.insert(1, "open").await;
    assert_eq!(db.src.count(&db.query(), u64::MAX).await.unwrap(), 1);
    db.drop().await;
}

/// A second fresh stream must start where Postgres resumes, not at the slot's creation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_stream_after_ack_starts_at_confirmed() {
    let Some(db) = Db::items().await else { return };
    let mut feed = db.src.stream(None).await.unwrap();
    db.insert(1, "open").await;
    let acked = loop {
        let txn = feed.next().await.unwrap();
        if !txn.is_mark() {
            break txn.position;
        }
    };
    db.src.ack(&acked);
    tokio::time::sleep(Duration::from_secs(3)).await;
    drop(feed);

    let mut mirror = Mirror::start(db.src.clone(), db.open_items())
        .await
        .unwrap();
    db.insert(2, "open").await;
    mirror.settle(SETTLE).await.unwrap();
    assert_eq!(mirror.view().len(), 2);
    db.drop().await;
}

/// Reads must format values like the replication stream, even when the DSN changes session settings.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_settings_match_the_stream() {
    let Some(db) = Db::custom(
        "id int PRIMARY KEY, ts timestamptz, iv interval",
        None,
        Arc::new(TextCodec),
        |cfg| {
            cfg.dsn.push_str(
                "?options=-c%20TimeZone%3DPacific%2FChatham%20-c%20IntervalStyle%3Diso_8601",
            )
        },
    )
    .await
    else {
        return;
    };
    db.src.open().await.unwrap();
    db.exec("INSERT INTO $T VALUES (1, '2026-01-02 03:04:05+00', '1 day 2 hours')")
        .await;
    let mut mirror = Mirror::start(db.src.clone(), db.query()).await.unwrap();
    db.exec("UPDATE $T SET iv = '3 hours' WHERE id = 1").await;
    // settle compares streamed values with read values; a format mismatch fails here.
    mirror.settle(SETTLE).await.unwrap();
    db.drop().await;
}

/// A publication column list publishes fewer columns; the catalog must match it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publication_column_list() {
    let Some(db) = Db::custom(
        "id int PRIMARY KEY, status text, secret text",
        Some("FOR TABLE $T (id, status)"),
        Arc::new(TextCodec),
        |_| {},
    )
    .await
    else {
        return;
    };
    db.src.open().await.unwrap();
    let table = db.src.catalog().table(&db.table).unwrap().clone();
    assert!(table.column("secret").is_none());
    db.exec("INSERT INTO $T VALUES (1, 'open', 'x')").await;
    let mut mirror = Mirror::start(db.src.clone(), db.open_items())
        .await
        .unwrap();
    db.exec("UPDATE $T SET status = 'open', secret = 'y' WHERE id = 1")
        .await;
    mirror.settle(SETTLE).await.unwrap();
    db.drop().await;
}

/// Generated columns aren't streamed, so they must not be in the catalog either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_columns() {
    let Some(db) =
        Db::new("id int PRIMARY KEY, n int, twice int GENERATED ALWAYS AS (n * 2) STORED").await
    else {
        return;
    };
    let mut mirror = Mirror::start(db.src.clone(), db.query()).await.unwrap();
    db.exec("INSERT INTO $T (id, n) VALUES (1, 2)").await;
    mirror.settle(SETTLE).await.unwrap();
    assert_eq!(mirror.view().len(), 1);
    db.drop().await;
}

/// Setups the stream can't follow are refused at open, with a clear reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_unsafe_publications() {
    let cases = [
        (
            "id int PRIMARY KEY, n int",
            "FOR TABLE $T WHERE (n > 0)",
            "row filter",
        ),
        (
            "id int PRIMARY KEY, n int",
            "FOR TABLE $T WITH (publish = 'insert')",
            "must publish",
        ),
        (
            "id int PRIMARY KEY, n int",
            "FOR TABLE $T (n)",
            "primary key column",
        ),
    ];
    for (columns, clause, reason) in cases {
        let Some(db) = Db::custom(columns, Some(clause), Arc::new(TextCodec), |_| {}).await else {
            return;
        };
        assert!(refused(db.src.open().await, reason), "{clause}");
        db.drop().await;
    }
    let Some(db) = Db::unopened("id int PRIMARY KEY, n int", Arc::new(TextCodec)).await else {
        return;
    };
    db.exec("ALTER TABLE $T REPLICA IDENTITY NOTHING").await;
    assert!(refused(db.src.open().await, "REPLICA IDENTITY"));
    db.drop().await;
}

/// When a snapshot's connection dies, reads fail as retryable and recover with a newer snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_snapshot_recovers() {
    let Some(db) = Db::items().await else { return };
    db.insert(1, "open").await;
    let mut mirror = Mirror::start(db.src.clone(), db.open_items())
        .await
        .unwrap();
    db.exec(&format!(
        "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
         WHERE starts_with(slot_name::text, '{}_snap_') AND active_pid IS NOT NULL",
        db.prefix
    ))
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let read = db.src.select(&db.open_items()).await;
    assert!(
        matches!(read, Err(SourceError::Transient(_))),
        "a lost snapshot is retryable, not refused: {read:?}"
    );
    db.insert(2, "open").await;
    mirror.settle(SETTLE).await.unwrap();
    assert_eq!(mirror.view().len(), 2);
    db.drop().await;
}

/// A read stuck behind a lock ends at the read timeout as a retryable error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_timeout() {
    let Some(db) = Db::custom(
        "id int PRIMARY KEY, n int",
        None,
        Arc::new(TextCodec),
        |cfg| cfg.read_timeout = Duration::from_millis(300),
    )
    .await
    else {
        return;
    };
    db.src.open().await.unwrap();
    let _mirror = Mirror::start(db.src.clone(), db.query()).await.unwrap();
    db.exec("BEGIN; LOCK TABLE $T IN ACCESS EXCLUSIVE MODE")
        .await;
    let read = db.src.select(&db.query()).await;
    assert!(matches!(read, Err(SourceError::Transient(_))), "{read:?}");
    db.exec("ROLLBACK").await;
    assert!(db.src.select(&db.query()).await.is_ok());
    db.drop().await;
}

/// Reads queue for the capped pool instead of opening unbounded connections.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_connection_cap() {
    let Some(db) = Db::custom("id int PRIMARY KEY", None, Arc::new(TextCodec), |cfg| {
        cfg.read_connections = 1
    })
    .await
    else {
        return;
    };
    db.src.open().await.unwrap();
    let _mirror = Mirror::start(db.src.clone(), db.query()).await.unwrap();
    let query = db.query();
    let reads = (0..8).map(|_| db.src.select(&query));
    for read in futures_join(reads).await {
        read.unwrap();
    }
    db.drop().await;
}

async fn futures_join<F: std::future::Future>(futures: impl Iterator<Item = F>) -> Vec<F::Output> {
    let mut out = vec![];
    let futures: Vec<_> = futures.map(Box::pin).collect();
    for f in futures {
        out.push(f.await);
    }
    out
}

/// Reads before `open` say the source is closed rather than "no table".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn select_before_open_is_closed() {
    let Some(db) = Db::unopened("id int PRIMARY KEY", Arc::new(TextCodec)).await else {
        return;
    };
    let read = db.src.inner().select(&db.query()).await;
    assert!(matches!(read, Err(SourceError::Closed)), "{read:?}");
    db.drop().await;
}

/// Dropping an open source without `close` still removes its feed slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_without_close_removes_slot() {
    let Some(db) = Db::items().await else { return };
    let slot = db.src.inner().slot().unwrap();
    let Db {
        src,
        sql,
        table,
        publication,
        ..
    } = db;
    drop(src);
    let check = format!("SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'");
    let mut left = 1;
    for _ in 0..50 {
        left = sql.query_one(&check, &[]).await.unwrap().get::<_, i64>(0);
        if left == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(left, 0);
    sql.batch_execute(&format!(
        "DROP PUBLICATION {publication}; DROP TABLE {table}"
    ))
    .await
    .unwrap();
}

/// `setup` adds tables configured after the publication was created.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn setup_adds_missing_tables() {
    let Some(db) = Db::items().await else { return };
    let extra = format!("{}_extra", db.table);
    db.exec(&format!("CREATE TABLE {extra} (id int PRIMARY KEY)"))
        .await;
    let mut cfg = PgConfig::new(&std::env::var("STREAMLIB_PG_DSN").unwrap(), &db.publication);
    cfg.tables = vec![db.table.clone(), extra.clone()];
    PgSource::new(cfg).unwrap().setup().await.unwrap();
    let published: i64 = db
        .sql
        .query_one(
            "SELECT count(*) FROM pg_publication_tables WHERE pubname = $1",
            &[&db.publication],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(published, 2);
    db.exec(&format!(
        "ALTER PUBLICATION {} DROP TABLE {extra}; DROP TABLE {extra}",
        db.publication
    ))
    .await;
    db.drop().await;
}

/// Leftover feed slots from crashed processes can be cleaned up; active ones are kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cleanup_inactive_slots() {
    let Some(db) = Db::items().await else { return };
    let _feed = db.src.stream(None).await.unwrap(); // keeps our own slot active
    let stale = format!("{}_feed_stale", db.prefix);
    db.exec(&format!(
        "SELECT pg_create_logical_replication_slot('{stale}', 'pgoutput')"
    ))
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let dropped = db
        .src
        .inner()
        .cleanup_inactive_slots(Duration::from_millis(500))
        .await
        .unwrap();
    assert_eq!(dropped, vec![stale]);
    db.drop().await;
}
