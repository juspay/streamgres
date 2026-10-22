//! Production-shaped lookups through the synced connection, one shape
//! after another, side by side with Postgres: whether the wrapper's
//! answer equals Postgres's before and after a change, that the engine
//! serves each shape from its rows, and how long a change takes to reach
//! the cached answer.
//!
//! Every case runs the same query on a passthrough connection and on the
//! synced one, compares the two, changes the rows behind it with raw
//! SQL, waits for the synced answer to move, and compares again. The
//! shapes are the ones an application's model layer produces: a primary
//! key, a two-column key, `AND` and `OR` predicates, an enum bind, a
//! list, `ORDER BY ... LIMIT`, `LIMIT OFFSET`, a `bytea`, a timestamp
//! bind and a count.
//!
//! Needs `STREAMGRES_S2S_URL` and `DATABASE_URL` as `engine_live.rs`
//! does; skips itself otherwise. The enum type must exist before the
//! engine starts, since the engine reads its type map at start.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{NaiveDate, NaiveDateTime};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel_streamgres::cache::{Cache, Limits};
use diesel_streamgres::engine::EngineClient;
use diesel_streamgres::{Route, Served, SyncedConnection};

/// A user-defined type, so the enum bind goes through Diesel's OID
/// lookup like an application's would.
#[derive(diesel::sql_types::SqlType, diesel::query_builder::QueryId)]
#[diesel(postgres_type(name = "sg_mood"))]
pub struct SgMood;

/// The enum's values on the Rust side.
#[derive(Debug, Clone, Copy, PartialEq, diesel::AsExpression, diesel::FromSqlRow)]
#[diesel(sql_type = SgMood)]
enum Mood {
    Happy,
    Sad,
}

impl diesel::serialize::ToSql<SgMood, diesel::pg::Pg> for Mood {
    /// The label Postgres knows the value by.
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, diesel::pg::Pg>,
    ) -> diesel::serialize::Result {
        use std::io::Write;
        out.write_all(match self {
            Mood::Happy => b"happy",
            Mood::Sad => b"sad",
        })?;
        Ok(diesel::serialize::IsNull::No)
    }
}

impl diesel::deserialize::FromSql<SgMood, diesel::pg::Pg> for Mood {
    /// The value from its label.
    fn from_sql(bytes: diesel::pg::PgValue<'_>) -> diesel::deserialize::Result<Self> {
        match bytes.as_bytes() {
            b"happy" => Ok(Mood::Happy),
            b"sad" => Ok(Mood::Sad),
            other => Err(format!("not a mood: {other:?}").into()),
        }
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::SgMood;

    sg_shapes (id) {
        id -> Text,
        merchant -> Text,
        mood -> SgMood,
        amount -> Int8,
        created -> Timestamp,
        blob -> Nullable<Bytea>,
        note -> Nullable<Text>,
        active -> Bool,
    }
}

/// A whole row, the way a model struct reads one.
#[derive(Queryable, Selectable, Debug, Clone, PartialEq)]
#[diesel(table_name = sg_shapes)]
struct Shape {
    id: String,
    merchant: String,
    mood: Mood,
    amount: i64,
    created: NaiveDateTime,
    blob: Option<Vec<u8>>,
    note: Option<String>,
    active: bool,
}

/// The blocking wrapper every shape is read through.
type Conn = SyncedConnection<PgConnection>;

/// The names this run's rows go by, unique per process so parallel runs
/// on one database do not meet.
struct Ids {
    prefix: String,
}

impl Ids {
    /// The id of seeded row `n`.
    fn id(&self, n: u8) -> String {
        format!("{}{n}", self.prefix)
    }

    /// The merchant four of the five seeded rows belong to.
    fn merchant(&self) -> String {
        format!("{}m", self.prefix)
    }

    /// The SQL with `{p}` standing for this run's prefix.
    fn sql(&self, template: &str) -> String {
        template.replace("{p}", &self.prefix)
    }
}

/// One query shape, how to run it, and a change that alters its answer.
struct Case {
    name: &'static str,
    run: fn(&mut Conn, &Ids) -> String,
    change: &'static str,
}

/// The rows as text, so answers compare whatever the shape returns.
fn text<T: std::fmt::Debug>(rows: T) -> String {
    format!("{rows:?}")
}

/// The shapes in the order they run; each change leaves the rows in a
/// state the later cases still read correctly.
fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "primary key",
            run: by_primary_key,
            change: "UPDATE sg_shapes SET note = coalesce(note, '') || 'x' WHERE id = '{p}1'",
        },
        Case {
            name: "two-column key",
            run: by_two_columns,
            change: "UPDATE sg_shapes SET note = coalesce(note, '') || 'x' WHERE id = '{p}2'",
        },
        Case {
            name: "AND of three predicates",
            run: and_of_three,
            change: "UPDATE sg_shapes SET active = NOT active WHERE id = '{p}3'",
        },
        Case {
            name: "OR predicate",
            run: or_predicate,
            change: "UPDATE sg_shapes SET amount = 100 WHERE id = '{p}2'",
        },
        Case {
            name: "enum bind",
            run: enum_bind,
            change: "UPDATE sg_shapes SET mood = 'sad' WHERE id = '{p}1'",
        },
        Case {
            name: "list of keys",
            run: list_of_keys,
            change: "UPDATE sg_shapes SET note = coalesce(note, '') || 'x' WHERE id = '{p}5'",
        },
        Case {
            name: "ORDER BY timestamp DESC LIMIT",
            run: latest_two,
            change: "UPDATE sg_shapes SET created = '2026-01-01 01:00:00' WHERE id = '{p}4'",
        },
        Case {
            name: "LIMIT OFFSET",
            run: second_page,
            change: "UPDATE sg_shapes SET note = coalesce(note, '') || 'x' WHERE id = '{p}3'",
        },
        Case {
            name: "bytea",
            run: blobs,
            change: "UPDATE sg_shapes SET blob = '\\xab'::bytea WHERE id = '{p}2'",
        },
        Case {
            name: "timestamp bind",
            run: since,
            change: "UPDATE sg_shapes SET created = '2026-01-01 00:09:00' WHERE id = '{p}1'",
        },
        Case {
            name: "COUNT",
            run: count,
            change: "UPDATE sg_shapes SET merchant = '{p}m' WHERE id = '{p}5'",
        },
    ]
}

/// `WHERE id = $1`.
fn by_primary_key(conn: &mut Conn, ids: &Ids) -> String {
    text(
        sg_shapes::table
            .filter(sg_shapes::id.eq(ids.id(1)))
            .select(Shape::as_select())
            .first::<Shape>(conn)
            .optional()
            .unwrap(),
    )
}

/// `WHERE merchant = $1 AND id = $2`, a composite lookup.
fn by_two_columns(conn: &mut Conn, ids: &Ids) -> String {
    text(
        sg_shapes::table
            .filter(
                sg_shapes::merchant
                    .eq(ids.merchant())
                    .and(sg_shapes::id.eq(ids.id(2))),
            )
            .select(Shape::as_select())
            .first::<Shape>(conn)
            .optional()
            .unwrap(),
    )
}

/// Three predicates on a text, a bool and an integer column.
fn and_of_three(conn: &mut Conn, ids: &Ids) -> String {
    text(
        sg_shapes::table
            .filter(sg_shapes::merchant.eq(ids.merchant()))
            .filter(sg_shapes::active.eq(true))
            .filter(sg_shapes::amount.gt(20))
            .order(sg_shapes::id)
            .select(Shape::as_select())
            .load::<Shape>(conn)
            .unwrap(),
    )
}

/// `WHERE merchant = $1 AND (mood = $2 OR amount >= $3)`.
fn or_predicate(conn: &mut Conn, ids: &Ids) -> String {
    text(
        sg_shapes::table
            .filter(
                sg_shapes::merchant.eq(ids.merchant()).and(
                    sg_shapes::mood
                        .eq(Mood::Happy)
                        .or(sg_shapes::amount.ge(100)),
                ),
            )
            .order(sg_shapes::id)
            .select(Shape::as_select())
            .load::<Shape>(conn)
            .unwrap(),
    )
}

/// A single enum bind, which Diesel sends by its looked-up OID.
fn enum_bind(conn: &mut Conn, ids: &Ids) -> String {
    text(
        sg_shapes::table
            .filter(sg_shapes::merchant.eq(ids.merchant()))
            .filter(sg_shapes::mood.eq(Mood::Sad))
            .order(sg_shapes::id)
            .select(Shape::as_select())
            .load::<Shape>(conn)
            .unwrap(),
    )
}

/// `WHERE id = ANY($1)`.
fn list_of_keys(conn: &mut Conn, ids: &Ids) -> String {
    text(
        sg_shapes::table
            .filter(sg_shapes::id.eq_any([ids.id(1), ids.id(3), ids.id(5)]))
            .order(sg_shapes::id)
            .select(Shape::as_select())
            .load::<Shape>(conn)
            .unwrap(),
    )
}

/// The two newest rows of the merchant.
fn latest_two(conn: &mut Conn, ids: &Ids) -> String {
    text(
        sg_shapes::table
            .filter(sg_shapes::merchant.eq(ids.merchant()))
            .order(sg_shapes::created.desc())
            .limit(2)
            .select(Shape::as_select())
            .load::<Shape>(conn)
            .unwrap(),
    )
}

/// The second page of two, by id.
fn second_page(conn: &mut Conn, ids: &Ids) -> String {
    text(
        sg_shapes::table
            .filter(sg_shapes::merchant.eq(ids.merchant()))
            .order(sg_shapes::id)
            .limit(2)
            .offset(1)
            .select(Shape::as_select())
            .load::<Shape>(conn)
            .unwrap(),
    )
}

/// A `bytea` column, present and `NULL`.
fn blobs(conn: &mut Conn, ids: &Ids) -> String {
    text(
        sg_shapes::table
            .filter(sg_shapes::id.eq_any([ids.id(1), ids.id(2), ids.id(3)]))
            .order(sg_shapes::id)
            .select((sg_shapes::id, sg_shapes::blob))
            .load::<(String, Option<Vec<u8>>)>(conn)
            .unwrap(),
    )
}

/// A `timestamp(6)` bound and compared at microsecond precision.
fn since(conn: &mut Conn, ids: &Ids) -> String {
    let cutoff = NaiveDate::from_ymd_opt(2026, 1, 1)
        .unwrap()
        .and_hms_micro_opt(0, 2, 30, 500_000)
        .unwrap();
    text(
        sg_shapes::table
            .filter(sg_shapes::merchant.eq(ids.merchant()))
            .filter(sg_shapes::created.ge(cutoff))
            .order(sg_shapes::id)
            .select(Shape::as_select())
            .load::<Shape>(conn)
            .unwrap(),
    )
}

/// `SELECT count(*)` under a predicate.
fn count(conn: &mut Conn, ids: &Ids) -> String {
    text(
        sg_shapes::table
            .filter(sg_shapes::merchant.eq(ids.merchant()))
            .count()
            .get_result::<i64>(conn)
            .unwrap(),
    )
}

/// The two connections under test, a third for seeding and changes,
/// and the engine client behind the cache.
struct Live {
    synced: Conn,
    plain: Conn,
    other: PgConnection,
    engine: EngineClient,
    ids: Ids,
}

impl Live {
    /// Connects to the engine and the database, makes sure the table and
    /// the enum exist, waits until the engine serves the table, and seeds
    /// five rows under this run's prefix. `None` when the environment
    /// names no engine. The cache keeps no refusal memory here: the
    /// engine hears of a new table through the DDL trigger a moment after
    /// it is created, and a subscribe refused in that moment would
    /// otherwise be remembered for the run.
    fn start() -> Option<Live> {
        let Ok(url) = std::env::var("STREAMGRES_S2S_URL") else {
            eprintln!("STREAMGRES_S2S_URL not set; skipping");
            return None;
        };
        let database = std::env::var("DATABASE_URL").expect("DATABASE_URL");
        let engine = EngineClient::connect(url);
        let limits = Limits {
            refused_for: None,
            ..Limits::default()
        };
        let cache = Arc::new(Cache::with_limits(limits, Arc::new(engine.clone())));
        engine.attach(&cache);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !engine.is_connected() {
            assert!(Instant::now() < deadline, "the engine did not connect");
            std::thread::sleep(Duration::from_millis(50));
        }
        let mut other = PgConnection::establish(&database).expect("connect");
        other
            .batch_execute(
                "BEGIN;
                 SELECT pg_advisory_xact_lock(424242);
                 DO $$ BEGIN CREATE TYPE sg_mood AS ENUM ('happy', 'sad');
                    EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$;
                 CREATE TABLE IF NOT EXISTS sg_shapes (
                    id text PRIMARY KEY,
                    merchant text NOT NULL,
                    mood sg_mood NOT NULL,
                    amount int8 NOT NULL,
                    created timestamp(6) NOT NULL,
                    blob bytea,
                    note text,
                    active bool NOT NULL);
                 COMMIT",
            )
            .expect("schema");
        let ids = Ids {
            prefix: format!("shapes-{}-", std::process::id()),
        };
        let mut live = Live {
            synced: SyncedConnection::with_cache(
                PgConnection::establish(&database).expect("connect"),
                cache,
                Route::Streamgres,
            ),
            plain: SyncedConnection::new(PgConnection::establish(&database).expect("connect")),
            other,
            engine,
            ids,
        };
        live.seed();
        live.wait_until_known();
        Some(live)
    }

    /// Asks the engine for a count on the table until it answers from
    /// its rows, which is when the DDL trigger's message has landed.
    fn wait_until_known(&mut self) {
        let started = Instant::now();
        loop {
            let before = self.engine.materialized();
            let probe = sg_shapes::table
                .filter(sg_shapes::id.eq(self.ids.sql("{p}probe")))
                .count()
                .get_result::<i64>(&mut self.synced)
                .expect("probe");
            assert_eq!(probe, 0);
            if self.engine.materialized() > before {
                return;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "the engine never served the table: {:?}",
                self.engine.refusals()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Five rows: four of one merchant and one of another, with both
    /// moods, a `NULL` and a present `bytea`, and distinct microsecond
    /// timestamps a minute apart.
    fn seed(&mut self) {
        self.cleanup();
        self.change(
            "INSERT INTO sg_shapes (id, merchant, mood, amount, created, blob, note, active) VALUES
             ('{p}1', '{p}m', 'happy', 10, '2026-01-01 00:01:00.123456', '\\x01ff10'::bytea, 'one', true),
             ('{p}2', '{p}m', 'sad', 50, '2026-01-01 00:02:00.123456', '\\x02ff10'::bytea, NULL, true),
             ('{p}3', '{p}m', 'happy', 100, '2026-01-01 00:03:00.123456', NULL, 'three', true),
             ('{p}4', '{p}m', 'sad', 150, '2026-01-01 00:04:00.123456', '\\x04'::bytea, 'four', false),
             ('{p}5', '{p}other', 'happy', 7, '2026-01-01 00:05:00.123456', NULL, 'five', false)",
        );
    }

    /// Runs a change on the third connection, outside the wrapper.
    fn change(&mut self, template: &str) {
        let sql = self.ids.sql(template);
        self.other.batch_execute(&sql).expect("change");
    }

    /// Removes this run's rows.
    fn cleanup(&mut self) {
        self.change("DELETE FROM sg_shapes WHERE id LIKE '{p}%'");
    }
}

/// Every shape agrees with Postgres before and after its change, is
/// served from the engine's rows, and sees the change within seconds.
#[test]
fn every_shape_matches_postgres_and_is_served_from_engine_rows() {
    let Some(mut live) = Live::start() else {
        return;
    };
    let materialized = live.engine.materialized();
    let subscribed = live.engine.subscribed();
    for (n, case) in cases().into_iter().enumerate() {
        let name = case.name;
        let expected = (case.run)(&mut live.plain, &live.ids);
        let first = (case.run)(&mut live.synced, &live.ids);
        assert_eq!(first, expected, "{name}: first answer");
        assert_eq!(
            live.engine.materialized(),
            materialized + n + 1,
            "{name}: rows mode; fallbacks {:?}, refusals {:?}",
            live.engine.fallbacks(),
            live.engine.refusals()
        );
        let again = (case.run)(&mut live.synced, &live.ids);
        assert_eq!(again, expected, "{name}: repeat");
        assert_eq!(
            live.synced.last_served(),
            Some(Served::Cache),
            "{name}: repeat is a hit"
        );

        live.change(case.change);
        let started = Instant::now();
        let moved = loop {
            let now = (case.run)(&mut live.synced, &live.ids);
            if now != first {
                break now;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "{name}: the change never reached the cached answer"
            );
            std::thread::sleep(Duration::from_millis(5));
        };
        let took = started.elapsed();
        let expected = (case.run)(&mut live.plain, &live.ids);
        assert_eq!(moved, expected, "{name}: answer after the change");
        assert_eq!(
            live.synced.last_served(),
            Some(Served::Cache),
            "{name}: still cached"
        );
        eprintln!("SHAPE {name}: rows mode, change seen after {took:?}");
    }
    assert_eq!(live.engine.subscribed(), subscribed + cases().len());
    live.cleanup();
}
