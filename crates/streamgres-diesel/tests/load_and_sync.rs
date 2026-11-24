//! Diesel queries through Streamgres against a live Postgres: the SQL
//! Diesel renders is answered from memory, follows writes, and decodes
//! through Diesel's own `FromSql`, enums included.
//!
//! Set `STREAMGRES_PG_DSN` (a database with `wal_level = logical`); without
//! it the test is skipped.

#[path = "../../../tests/support/connections.rs"]
mod application;

use std::time::{Duration, Instant};

use diesel::prelude::*;
use streamgres_diesel::{LoadAndSync, OnUnsupported, QueryState, Served, Streamgres, Ttl};

#[derive(diesel::sql_types::SqlType, diesel::query_builder::QueryId)]
#[diesel(postgres_type(name = "fresh_state", schema = "desk_live"))]
struct FreshStateType;

#[derive(Debug, diesel::AsExpression)]
#[diesel(sql_type = FreshStateType)]
struct FreshState;

impl diesel::serialize::ToSql<FreshStateType, diesel::pg::Pg> for FreshState {
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, diesel::pg::Pg>,
    ) -> diesel::serialize::Result {
        use std::io::Write;
        out.write_all(b"open")?;
        Ok(diesel::serialize::IsNull::No)
    }
}

mod schema {
    pub mod sql_types {
        #[derive(diesel::sql_types::SqlType, diesel::query_builder::QueryId)]
        #[diesel(postgres_type(name = "ticket_state", schema = "desk_live"))]
        pub struct TicketState;
    }

    diesel::table! {
        use diesel::sql_types::*;
        use super::sql_types::TicketState;

        desk_live.tickets (id) {
            id -> Int8,
            team_id -> Int8,
            title -> Text,
            state -> TicketState,
            priority -> Int4,
        }
    }

    diesel::table! {
        desk_live.teams (id) {
            id -> Int8,
            name -> Text,
        }
    }

    diesel::joinable!(tickets -> teams (team_id));
    diesel::allow_tables_to_appear_in_same_query!(tickets, teams);
}

use schema::{teams, tickets};

#[derive(Debug, Clone, Copy, PartialEq, Eq, diesel::FromSqlRow, diesel::AsExpression)]
#[diesel(sql_type = schema::sql_types::TicketState)]
enum State {
    Open,
    Closed,
}

impl diesel::serialize::ToSql<schema::sql_types::TicketState, diesel::pg::Pg> for State {
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, diesel::pg::Pg>,
    ) -> diesel::serialize::Result {
        use std::io::Write;
        out.write_all(match self {
            State::Open => b"open",
            State::Closed => b"closed",
        })?;
        Ok(diesel::serialize::IsNull::No)
    }
}

impl diesel::deserialize::FromSql<schema::sql_types::TicketState, diesel::pg::Pg> for State {
    fn from_sql(bytes: diesel::pg::PgValue<'_>) -> diesel::deserialize::Result<Self> {
        match bytes.as_bytes() {
            b"open" => Ok(State::Open),
            b"closed" => Ok(State::Closed),
            _ => Err("an unknown ticket state".into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Queryable, Selectable)]
#[diesel(table_name = tickets)]
struct Ticket {
    id: i64,
    team_id: i64,
    title: String,
    state: State,
    priority: i32,
}

fn dsn() -> Option<String> {
    let dsn = std::env::var("STREAMGRES_PG_DSN").ok();
    if dsn.is_none() {
        eprintln!("STREAMGRES_PG_DSN not set: live Diesel scenario skipped");
    }
    dsn
}

async fn admin(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    client
}

#[tokio::test(flavor = "multi_thread")]
async fn diesel_queries_are_answered_from_memory_and_follow_writes() {
    let Some(dsn) = dsn() else { return };
    let builder = Streamgres::connect(&dsn)
        .connections(application::connections(&dsn))
        .publication("desk_live_pub")
        .schemas(["desk_live"])
        .app("desklive", 0)
        .max_lag(Duration::from_secs(5))
        // The bind-type refresh below runs a statement Streamgres does not
        // support (no FROM) through the fallback.
        .on_unsupported(OnUnsupported::Fallback);
    #[cfg(feature = "async-pool")]
    let application_pool = {
        use diesel_async::pooled_connection::{AsyncDieselConnectionManager, bb8};
        let manager = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(&dsn);
        bb8::Pool::builder()
            .max_size(2)
            .build(manager)
            .await
            .expect("app pool")
    };
    #[cfg(feature = "async-pool")]
    let builder = builder.connections(streamgres_diesel::connections(application_pool.clone()));
    let client = admin(&dsn).await;
    client
        .batch_execute(
            "DROP PUBLICATION IF EXISTS desk_live_pub;
             DROP SCHEMA IF EXISTS desk_live CASCADE;
             CREATE SCHEMA desk_live;
             CREATE TYPE desk_live.ticket_state AS ENUM ('open', 'closed');
             CREATE TABLE desk_live.teams (id int8 PRIMARY KEY, name text NOT NULL);
             CREATE TABLE desk_live.tickets (id int8 PRIMARY KEY, team_id int8 NOT NULL,
                 title text NOT NULL, state desk_live.ticket_state NOT NULL, priority int4 NOT NULL);
             INSERT INTO desk_live.teams VALUES (1, 'billing'), (2, 'support');
             INSERT INTO desk_live.tickets VALUES
                 (1, 1, 'refund stuck', 'open', 2), (2, 1, 'invoice typo', 'closed', 1),
                 (3, 2, 'login loop', 'open', 3), (4, 1, 'card declined', 'open', 1);
             CREATE PUBLICATION desk_live_pub FOR TABLES IN SCHEMA desk_live;",
        )
        .await
        .expect("prepare");
    client
        .batch_execute(&builder.setup_sql())
        .await
        .expect("trigger stack");
    let lib = builder.start().await.expect("start");

    assert!(lib.type_oids("fresh_state", Some("desk_live")).is_none());
    client
        .batch_execute("CREATE TYPE desk_live.fresh_state AS ENUM ('open')")
        .await
        .expect("create a type after startup");
    let fresh = || {
        diesel::select(
            diesel::dsl::sql::<diesel::sql_types::Text>("CAST(")
                .bind::<FreshStateType, _>(FreshState)
                .sql(" AS text)"),
        )
    };
    let value: Vec<String> = fresh()
        .load_and_sync(&lib)
        .await
        .expect("refresh unknown bind type");
    assert_eq!(value, ["open"]);
    let (value, _) = fresh().load_and_sync::<String>(&lib).inspect().await;
    assert_eq!(value.expect("inspected custom bind"), ["open"]);

    let open_for_team = |team: i64| {
        tickets::table
            .filter(tickets::team_id.eq(team))
            .filter(tickets::state.eq(State::Open))
            .order(tickets::id)
            .limit(10)
            .select(Ticket::as_select())
    };
    let open: Vec<Ticket> = open_for_team(1)
        .load_and_sync(&lib)
        .await
        .expect("first read");
    assert_eq!(
        open.iter().map(|ticket| ticket.id).collect::<Vec<_>>(),
        vec![1, 4],
        "the first read, from Postgres while the query loads"
    );
    let open = until_memory(|| open_for_team(1).load_and_sync(&lib).inspect()).await;
    assert_eq!(
        open.iter().map(|ticket| ticket.id).collect::<Vec<_>>(),
        vec![1, 4]
    );
    assert_eq!(open[0].state, State::Open, "an enum decoded from memory");
    assert_eq!(open[0].title, "refund stuck");

    client
        .batch_execute("UPDATE desk_live.tickets SET state = 'open' WHERE id = 2")
        .await
        .expect("write");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let open: Vec<Ticket> = open_for_team(1).load_and_sync(&lib).await.expect("read");
        if open.len() == 3 {
            break;
        }
        assert!(Instant::now() < deadline, "the update never arrived");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let titled: Vec<(String, String)> = until_memory(|| {
        tickets::table
            .inner_join(teams::table)
            .filter(teams::name.eq("support"))
            .order(tickets::id)
            .select((tickets::title, teams::name))
            .load_and_sync(&lib)
            .ttl(Ttl::Forever)
            .inspect()
    })
    .await;
    assert_eq!(
        titled,
        vec![("login loop".to_owned(), "support".to_owned())]
    );

    let (a, b) = diesel::alias!(schema::tickets as a, schema::tickets as b);
    let pairs: Vec<(i64, i64)> = until_memory(|| {
        a.inner_join(b.on(a.field(tickets::team_id).eq(b.field(tickets::team_id))))
            .filter(a.field(tickets::id).eq(1i64))
            .filter(b.field(tickets::id).eq(4i64))
            .select((a.field(tickets::id), b.field(tickets::id)))
            .load_and_sync(&lib)
            .inspect()
    })
    .await;
    assert_eq!(pairs, vec![(1, 4)]);
    let page: Vec<i64> = until_memory(|| {
        tickets::table
            .inner_join(teams::table)
            .order((teams::name, tickets::id))
            .limit(2)
            .offset(1)
            .select(tickets::id)
            .load_and_sync(&lib)
            .inspect()
    })
    .await;
    assert_eq!(page, vec![2, 4]);

    let urgent: Vec<i64> = until_memory(|| {
        tickets::table
            .filter(tickets::priority.ge(2))
            .order(tickets::priority.desc())
            .select(tickets::id)
            .load_and_sync(&lib)
            .inspect()
    })
    .await;
    assert_eq!(urgent, vec![3, 1]);

    let (rows, trace) = open_for_team(1).load_and_sync(&lib).inspect().await;
    let trace = trace.expect("a rendered read has a trace");
    assert_eq!(rows.expect("read").len(), 3);
    assert_eq!(trace.served, Served::Memory, "{trace}");
    assert!(
        trace.at.file().ends_with("load_and_sync.rs"),
        "the caller's place: {}",
        trace.at
    );
    let inspected = open_for_team(1).inspect(&lib).expect("inspect");
    assert_eq!(inspected.state, QueryState::Ready);
    assert!(inspected.rows >= 3);

    lib.shutdown().await;
    #[cfg(feature = "async-pool")]
    {
        use diesel_async::SimpleAsyncConnection;
        let mut connection = application_pool
            .get()
            .await
            .expect("application pool survives library shutdown");
        connection
            .batch_execute("SELECT 1")
            .await
            .expect("pool remains usable");
        assert!(application_pool.state().connections <= 2);
    }
    // The trigger stack's DDL triggers would fire for every later test.
    client
        .batch_execute(
            "DROP PUBLICATION desk_live_pub; DROP SCHEMA desk_live CASCADE;
             DROP SCHEMA desklive_0 CASCADE;",
        )
        .await
        .expect("clean up");
}

/// Reads until memory answers, for at most ten seconds: a new query's
/// reads go to Postgres while it loads.
async fn until_memory<U, F>(read: impl Fn() -> F) -> Vec<U>
where
    F: std::future::IntoFuture<
            Output = (
                diesel::QueryResult<Vec<U>>,
                Option<streamgres_diesel::Trace>,
            ),
        >,
{
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (rows, trace) = read().await;
        if trace.is_some_and(|trace| trace.served == Served::Memory) {
            return rows.expect("a read from memory");
        }
        assert!(Instant::now() < deadline, "never answered from memory");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[cfg(feature = "async-pool")]
#[tokio::test]
async fn interrupted_raw_transactions_are_not_returned_to_the_app() {
    use diesel_async::pooled_connection::{AsyncDieselConnectionManager, bb8};
    use streamgres::cache::{Connections, Session};
    let Some(dsn) = dsn() else { return };
    let manager = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(&dsn);
    let pool = bb8::Pool::builder()
        .max_size(1)
        .build(manager)
        .await
        .expect("pool");
    let connections = streamgres_diesel::connections(pool.clone());
    let mut checkout = connections.get().await.expect("checkout");
    let pid = |rows: streamgres::cache::RawRows| rows.int(0, 0).unwrap().unwrap();
    let before = pid(checkout
        .query("SELECT pg_backend_pid()", &[])
        .await
        .unwrap());
    checkout.execute("BEGIN; SELECT 1").await.unwrap();
    drop(checkout);
    let mut replacement = connections.get().await.expect("replacement checkout");
    let after = pid(replacement
        .query("SELECT pg_backend_pid()", &[])
        .await
        .unwrap());
    assert_ne!(
        before, after,
        "an interrupted transaction must discard its physical connection"
    );
}

#[cfg(feature = "async-pool")]
#[tokio::test]
async fn raw_bridge_preserves_postgres_quoted_regions() {
    use diesel_async::pooled_connection::{AsyncDieselConnectionManager, bb8};
    use streamgres::cache::{Connections, Session};
    let Some(dsn) = dsn() else { return };
    let manager = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(&dsn);
    let pool = bb8::Pool::builder()
        .max_size(1)
        .build(manager)
        .await
        .unwrap();
    let connections = streamgres_diesel::connections(pool);
    let mut lease = connections.get().await.unwrap();
    for sql in [
        r"SELECT E'it\'s $1' AS value",
        "SELECT 1 AS value /* outer /* inner */ $1 */",
        "SELECT $é$ $1 $é$ AS value",
    ] {
        lease
            .query(sql, &[])
            .await
            .expect("valid Postgres quoted SQL");
    }
    let binds = [streamgres::cache::Bind {
        type_oid: 20,
        bytes: Some(7i64.to_be_bytes().to_vec()),
    }];
    let rows = lease
        .query("SELECT $1::bigint + $1::bigint", &binds)
        .await
        .unwrap();
    assert_eq!(rows.int(0, 0).unwrap(), Some(14));
    let more = [
        binds[0].clone(),
        streamgres::cache::Bind {
            type_oid: 20,
            bytes: Some(3i64.to_be_bytes().to_vec()),
        },
        streamgres::cache::Bind {
            type_oid: 20,
            bytes: None,
        },
    ];
    let rows = lease
        .query("SELECT $2 - $1 AS value, $3::bigint AS empty", &more)
        .await
        .unwrap();
    assert_eq!(rows.int(0, 0).unwrap(), Some(-4));
    assert_eq!(rows.int(0, 1).unwrap(), None);
    lease
        .execute("CREATE TYPE pg_temp.audit_mood AS ENUM ('ready')")
        .await
        .unwrap();
    let oid = lease
        .query("SELECT 'pg_temp.audit_mood'::regtype::oid::bigint", &[])
        .await
        .unwrap()
        .int(0, 0)
        .unwrap()
        .unwrap();
    let rows = lease
        .query(
            "SELECT pg_typeof($1)::oid::bigint",
            &[streamgres::cache::Bind {
                type_oid: u32::try_from(oid).unwrap(),
                bytes: Some(b"ready".to_vec()),
            }],
        )
        .await
        .unwrap();
    assert_eq!(rows.int(0, 0).unwrap(), Some(oid));
}

/// The bridge reports a statement timeout as a timeout and a Postgres
/// error with the SQLSTATE Diesel keeps.
#[cfg(feature = "async-pool")]
#[tokio::test(flavor = "multi_thread")]
async fn bridge_errors_are_structured() {
    use diesel_async::pooled_connection::{AsyncDieselConnectionManager, bb8};
    use streamgres::cache::{Connections, Session, SessionError};
    let Some(dsn) = dsn() else { return };
    let manager = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(&dsn);
    let pool = bb8::Pool::builder()
        .max_size(1)
        .build(manager)
        .await
        .expect("pool");
    let connections = streamgres_diesel::connections(pool);
    let mut checkout = connections.get().await.expect("checkout");
    checkout
        .execute("SET statement_timeout = 50")
        .await
        .unwrap();
    let error = checkout.query("SELECT pg_sleep(1)", &[]).await.unwrap_err();
    assert!(matches!(error, SessionError::Timeout(_)), "{error:?}");
    checkout.execute("RESET statement_timeout").await.unwrap();
    let error = checkout
        .execute("CREATE TEMP TABLE once (id int UNIQUE); INSERT INTO once VALUES (1), (1)")
        .await
        .unwrap_err();
    assert!(
        matches!(&error, SessionError::Database { code: Some(code), .. } if code == "23505"),
        "{error:?}"
    );
}
