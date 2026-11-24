//! The library against a live Postgres: `load_and_sync` answers from
//! memory once a query is kept, follows writes, falls back for what the
//! engine cannot answer, and cleans up on shutdown.
//!
//! Set `STREAMGRES_PG_DSN` (a database with `wal_level = logical` and a
//! user that may create publications, slots and event triggers); without
//! it every test here is skipped.

use std::time::{Duration, Instant};

use streamgres::cache::{
    Error, Fallback, OnUnsupported, QueryState, Rows, Served, Sql, Streamgres, Ttl,
};

fn dsn() -> Option<String> {
    let dsn = std::env::var("STREAMGRES_PG_DSN").ok();
    if dsn.is_none() {
        eprintln!("STREAMGRES_PG_DSN not set: live library scenario skipped");
    }
    dsn
}

/// A connection for the test's own writes.
async fn admin(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    client
}

/// A fresh schema `schema` with its publication and the trigger stack of
/// app `app`, set up as an administrator would.
/// Setups and cleanups run one at a time: each one's DDL fires every
/// installed DDL trigger, and concurrent ones can deadlock on their locks.
static SETUP: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn prepare(dsn: &str, schema: &str, app: &str, tables: &str) {
    let _setup = SETUP.lock().await;
    let client = admin(dsn).await;
    let setup = Streamgres::connect(dsn)
        .publication(format!("{schema}_pub"))
        .schemas([schema])
        .app(app, 0)
        .setup_sql();
    client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {schema}_pub;
             DROP SCHEMA IF EXISTS {schema} CASCADE;
             CREATE SCHEMA {schema};
             {tables}
             CREATE PUBLICATION {schema}_pub FOR TABLES IN SCHEMA {schema};"
        ))
        .await
        .expect("prepare");
    client.batch_execute(&setup).await.expect("trigger stack");
}

/// Removes `schema`, its publication and `app`'s trigger stack, whose DDL
/// triggers would otherwise fire for every later test on the server.
/// Runs schema changes under the same lock as [`prepare`] and [`clean`]:
/// each test's DDL fires every other test's DDL triggers.
async fn ddl(dsn: &str, sql: &str) {
    let _setup = SETUP.lock().await;
    admin(dsn).await.batch_execute(sql).await.expect(sql);
}

async fn clean(dsn: &str, schema: &str, app: &str) {
    let _setup = SETUP.lock().await;
    admin(dsn)
        .await
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {schema}_pub;
             DROP SCHEMA IF EXISTS {schema} CASCADE;
             DROP SCHEMA IF EXISTS {app}_0 CASCADE;"
        ))
        .await
        .expect("clean up");
}

fn int8(rows: &Rows, row: usize, column: usize) -> Option<i64> {
    rows.row(row)?
        .bytes(column)
        .map(|bytes| i64::from_be_bytes(bytes.try_into().unwrap()))
}

fn text(rows: &Rows, row: usize, column: usize) -> Option<String> {
    rows.row(row)?
        .bytes(column)
        .map(|bytes| String::from_utf8(bytes.to_vec()).unwrap())
}

/// Reads `sql` until memory answers it, for at most ten seconds: a new
/// query's reads go to the fallback while it loads.
async fn until_memory(lib: &Streamgres, sql: &Sql) -> Rows {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (rows, trace) = lib.load_and_sync(sql.clone()).inspect().await;
        if trace.served == Served::Memory {
            return rows.expect("a read from memory");
        }
        assert!(
            Instant::now() < deadline,
            "never answered from memory: {trace}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Reads until `done` holds for the rows, for at most ten seconds.
async fn eventually(lib: &Streamgres, sql: &Sql, done: impl Fn(&Rows) -> bool) -> Rows {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let rows = lib.load_and_sync(sql.clone()).await.expect("read");
        if done(&rows) {
            return rows;
        }
        assert!(Instant::now() < deadline, "the rows never got there");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_query_is_loaded_kept_current_and_answered_from_memory() {
    let Some(dsn) = dsn() else { return };
    prepare(
        &dsn,
        "lib_live",
        "liblive",
        "CREATE TABLE lib_live.merchants (id int8 PRIMARY KEY, name text NOT NULL, region text);
         CREATE TABLE lib_live.payments (id int8 PRIMARY KEY, merchant_id int8 NOT NULL,
             status text NOT NULL, amount numeric(12, 2) NOT NULL);
         INSERT INTO lib_live.merchants VALUES (1, 'acme', 'in'), (2, 'globex', 'us');
         INSERT INTO lib_live.payments VALUES (10, 1, 'success', 5.00), (11, 1, 'pending', 7.50),
             (12, 2, 'success', 1.25);",
    )
    .await;
    let lib = Streamgres::connect(&dsn)
        .publication("lib_live_pub")
        .schemas(["lib_live"])
        .app("liblive", 0)
        .max_lag(Duration::from_secs(5))
        .start()
        .await
        .expect("start");

    let by_merchant = Sql::new(
        r#"SELECT "payments"."id", "payments"."status" FROM "lib_live"."payments" WHERE "payments"."merchant_id" = $1 ORDER BY "payments"."id""#,
    )
    .bind(1i64);
    let rows = lib
        .load_and_sync(by_merchant.clone())
        .await
        .expect("first read");
    assert_eq!(rows.len(), 2);
    assert_eq!(
        lib.metrics().fallbacks,
        1,
        "the first read goes to Postgres while the query loads"
    );
    let rows = until_memory(&lib, &by_merchant).await;
    assert_eq!(rows.columns()[0].name(), "id");
    assert_eq!(
        (int8(&rows, 0, 0), text(&rows, 0, 1)),
        (Some(10), Some("success".into()))
    );
    let answered = lib.metrics().answered;
    lib.load_and_sync(by_merchant.clone())
        .await
        .expect("a later read");
    assert!(lib.metrics().answered > answered, "a later read is a hit");

    let client = admin(&dsn).await;
    client
        .batch_execute(
            "UPDATE lib_live.payments SET status = 'success' WHERE id = 11;
             INSERT INTO lib_live.payments VALUES (13, 1, 'failed', 2.00);",
        )
        .await
        .expect("write");
    let rows = eventually(&lib, &by_merchant, |rows| rows.len() == 3).await;
    assert_eq!(
        text(&rows, 1, 1),
        Some("success".into()),
        "the update arrived"
    );
    assert_eq!(int8(&rows, 2, 0), Some(13), "the insert arrived");

    let joined = Sql::new(
        r#"SELECT "payments"."id", "merchants"."name" FROM "lib_live"."payments" INNER JOIN "lib_live"."merchants" ON "merchants"."id" = "payments"."merchant_id" WHERE "merchants"."region" = $1 ORDER BY "payments"."id""#,
    )
    .bind("us");
    lib.load_and_sync(joined.clone())
        .ttl(Ttl::Forever)
        .await
        .expect("join");
    let rows = until_memory(&lib, &joined).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (int8(&rows, 0, 0), text(&rows, 0, 1)),
        (Some(12), Some("globex".into()))
    );

    let fallbacks = lib.metrics().fallbacks;
    let amounts = Sql::new(
        r#"SELECT "payments"."amount" FROM "lib_live"."payments" WHERE "payments"."id" = $1"#,
    )
    .bind(10i64);
    let rows = lib.load_and_sync(amounts).await.expect("numeric read");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows.columns()[0].type_oid(), 1700, "Postgres's own numeric");
    assert!(
        lib.metrics().fallbacks > fallbacks,
        "a numeric column is not held exactly, so Postgres answers"
    );

    // Other tests in this file run their own engines on the same server.
    let instance = lib.engine().instance;
    let slots = || async {
        admin(&dsn)
            .await
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
                &[&instance],
            )
            .await
            .unwrap()
            .get::<_, i64>(0)
    };
    assert_eq!(slots().await, 1, "the engine streams from one slot");
    lib.shutdown().await;
    assert_eq!(slots().await, 0, "shutting down drops it");
    clean(&dsn, "lib_live", "liblive").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_immediate_ttl_frees_the_query_and_error_fallback_refuses() {
    let Some(dsn) = dsn() else { return };
    prepare(
        &dsn,
        "lib_ttl",
        "libttl",
        "CREATE TABLE lib_ttl.flags (id int8 PRIMARY KEY, name text NOT NULL, rate numeric);
         INSERT INTO lib_ttl.flags VALUES (1, 'beta', 0.5);",
    )
    .await;
    let lib = Streamgres::connect(&dsn)
        .publication("lib_ttl_pub")
        .schemas(["lib_ttl"])
        .app("libttl", 0)
        .max_lag(Duration::from_secs(5))
        .fallback(Fallback::Error)
        .start()
        .await
        .expect("start");

    let flags = Sql::new(r#"SELECT "flags"."id", "flags"."name" FROM "lib_ttl"."flags""#);
    assert!(
        matches!(
            lib.load_and_sync(flags.clone()).ttl(Ttl::Immediate).await,
            Err(Error::Refused(_))
        ),
        "with the error fallback, a read of a query still loading is refused"
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let rows = loop {
        if let Ok(rows) = lib.load_and_sync(flags.clone()).ttl(Ttl::Immediate).await {
            break rows;
        }
        assert!(Instant::now() < deadline, "the query never loaded");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(text(&rows, 0, 1), Some("beta".into()));
    let deadline = Instant::now() + Duration::from_secs(5);
    while lib.metrics().entries > 0 {
        assert!(Instant::now() < deadline, "the query was never freed");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let rates = Sql::new(r#"SELECT "flags"."rate" FROM "lib_ttl"."flags""#);
    assert!(matches!(
        lib.load_and_sync(rates).await,
        Err(Error::Refused(_))
    ));
    lib.shutdown().await;
    clean(&dsn, "lib_ttl", "libttl").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_are_traced_and_queries_inspected() {
    let Some(dsn) = dsn() else { return };
    prepare(
        &dsn,
        "lib_inspect",
        "libinspect",
        "CREATE TABLE lib_inspect.rates (id int8 PRIMARY KEY, code text NOT NULL, rate numeric);
         INSERT INTO lib_inspect.rates VALUES (1, 'inr', 1.0), (2, 'usd', 83.1);",
    )
    .await;
    let lib = Streamgres::connect(&dsn)
        .publication("lib_inspect_pub")
        .schemas(["lib_inspect"])
        .app("libinspect", 0)
        .max_lag(Duration::from_secs(5))
        .debug_history(8)
        .start()
        .await
        .expect("start");

    let codes = Sql::new(
        r#"SELECT "rates"."id", "rates"."code" FROM "lib_inspect"."rates" ORDER BY "rates"."id""#,
    );
    assert_eq!(lib.inspect(&codes).state, QueryState::NotKept);
    let (rows, trace) = lib.load_and_sync(codes.clone()).inspect().await;
    assert_eq!(rows.expect("read").len(), 2);
    assert_eq!(trace.served, Served::Postgres, "{trace}");
    assert!(
        trace
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("loading")),
        "{trace}"
    );
    assert_eq!(trace.rows, 2);
    assert!(trace.at.file().ends_with("library_live.rs"));
    until_memory(&lib, &codes).await;
    let (_, trace) = lib
        .load_and_sync(codes.clone())
        .ttl(Ttl::Forever)
        .inspect()
        .await;
    assert_eq!(trace.served, Served::Memory, "{trace}");
    assert_eq!(trace.ttl, Some(Ttl::Forever));
    assert!(
        trace.position.as_deref().is_some_and(|at| at.contains('/')),
        "a WAL location: {trace:?}"
    );

    let inspected = lib.inspect(&codes);
    assert_eq!(inspected.state, QueryState::Ready);
    assert_eq!(inspected.rows, 2);
    assert_eq!(inspected.ttl, Some(Ttl::Forever));
    assert!(inspected.engine.running);
    assert!(inspected.engine.instance.is_some());
    assert_eq!(
        inspected.callers.len(),
        3,
        "three places read it: {:?}",
        inspected.callers
    );
    assert_eq!(inspected.recent[0].served, Served::Memory, "newest first");
    assert!(
        lib.queries()
            .iter()
            .any(|kept| kept.sql == codes.text() && kept.rows == 2)
    );

    let rates = Sql::new(r#"SELECT "rates"."rate" FROM "lib_inspect"."rates""#);
    let (rows, trace) = lib.load_and_sync(rates.clone()).inspect().await;
    assert_eq!(rows.expect("fallback read").len(), 2);
    assert_eq!(trace.served, Served::Postgres);
    assert!(
        trace
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("rate")),
        "{trace}"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while !matches!(
            lib.inspect(&rates).state,
            QueryState::Ready | QueryState::Refused(_)
        ) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("numeric projection is admitted and loaded");
    let (rows, trace) = lib.load_and_sync(rates.clone()).inspect().await;
    assert_eq!(rows.expect("numeric encoding still uses fallback").len(), 2);
    assert_eq!(trace.served, Served::Postgres);
    assert!(
        trace
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("encode")),
        "{trace}"
    );
    assert!(matches!(lib.inspect(&rates).state, QueryState::Refused(_)));
    lib.shutdown().await;
    clean(&dsn, "lib_inspect", "libinspect").await;
}

/// One subscription can share rows across orderings without sharing the
/// resulting order. A local hit must also apply its retention request.
#[tokio::test(flavor = "current_thread")]
async fn output_order_and_local_retention_follow_each_read() {
    let Some(dsn) = dsn() else { return };
    prepare(
        &dsn,
        "lib_reuse",
        "libreuse",
        "CREATE TABLE lib_reuse.parents (id bigint PRIMARY KEY);
         CREATE TABLE lib_reuse.children (id bigint PRIMARY KEY, parent_id bigint);
         INSERT INTO lib_reuse.parents VALUES (1), (2);
         INSERT INTO lib_reuse.children VALUES (10,1), (20,1), (15,2);",
    )
    .await;
    let lib = Streamgres::connect(&dsn)
        .publication("lib_reuse_pub")
        .schemas(["lib_reuse"])
        .app("libreuse", 0)
        .max_lag(Duration::from_secs(5))
        .start()
        .await
        .expect("start");
    let query = |order: &str| {
        Sql::new(format!(
            "SELECT parents.id, children.id FROM lib_reuse.parents JOIN lib_reuse.children ON children.parent_id=parents.id ORDER BY {order}"
        ))
    };
    for (order, expected) in [
        ("children.id ASC", vec![10, 15, 20]),
        ("children.id DESC", vec![20, 15, 10]),
        ("children.id ASC", vec![10, 15, 20]),
        ("parents.id ASC, children.id DESC", vec![20, 10, 15]),
        ("children.id DESC, parents.id ASC", vec![20, 15, 10]),
    ] {
        let rows = until_memory(&lib, &query(order)).await;
        let ids: Vec<_> = (0..rows.len())
            .map(|row| int8(&rows, row, 1).unwrap())
            .collect();
        assert_eq!(ids, expected, "{order}");
    }
    let point = Sql::new("SELECT id FROM lib_reuse.parents WHERE id=1");
    until_memory(&lib, &point).await;
    // Install the local answer, then upgrade its ttl without yielding to
    // the supervisor (which would invalidate the thread-local lease).
    lib.load_and_sync(point.clone())
        .await
        .expect("warm local answer");
    let (_, trace) = lib
        .load_and_sync(point.clone())
        .ttl(Ttl::Forever)
        .inspect()
        .await;
    assert_eq!(trace.served, Served::Memory);
    assert_eq!(trace.ttl, Some(Ttl::Forever));
    assert_eq!(lib.inspect(&point).ttl, Some(Ttl::Forever));
    for sql in [
        "SELECT *, 42 AS extra FROM lib_reuse.parents",
        "SELECT COUNT(*), 42 AS extra FROM lib_reuse.parents",
    ] {
        let (rows, trace) = lib.load_and_sync(Sql::new(sql)).inspect().await;
        assert_eq!(trace.served, Served::Unsupported, "{trace}");
        assert!(
            matches!(rows, Err(Error::Unsupported(_))),
            "an unsupported projection is an error by default"
        );
    }
    lib.shutdown().await;
    clean(&dsn, "lib_reuse", "libreuse").await;
}

/// With `OnUnsupported::Fallback`, a statement Streamgres can never answer
/// is answered by Postgres instead of failing.
#[tokio::test(flavor = "multi_thread")]
async fn unsupported_statements_can_fall_back_when_asked() {
    let Some(dsn) = dsn() else { return };
    prepare(
        &dsn,
        "lib_unsup",
        "libunsup",
        "CREATE TABLE lib_unsup.parents (id bigint PRIMARY KEY, tags text[]);
         INSERT INTO lib_unsup.parents VALUES (1, '{a}'), (2, '{b,c}');",
    )
    .await;
    let lib = Streamgres::connect(&dsn)
        .publication("lib_unsup_pub")
        .schemas(["lib_unsup"])
        .app("libunsup", 0)
        .max_lag(Duration::from_secs(5))
        .on_unsupported(OnUnsupported::Fallback)
        .start()
        .await
        .expect("start");
    for sql in [
        "SELECT *, 42 AS extra FROM lib_unsup.parents",
        r#"SELECT "parents"."tags" FROM "lib_unsup"."parents""#,
    ] {
        let (rows, trace) = lib.load_and_sync(Sql::new(sql)).inspect().await;
        assert_eq!(trace.served, Served::Postgres, "{trace}");
        assert!(
            trace
                .reason
                .as_deref()
                .is_some_and(|reason| reason.starts_with("not supported by Streamgres")),
            "{trace}"
        );
        assert_eq!(rows.expect("fallback read").len(), 2);
    }
    lib.shutdown().await;
    clean(&dsn, "lib_unsup", "libunsup").await;
}

/// Dropping the caller cannot release a turn while its origin task is
/// still running. Once that task ends, the next caller can use the turn.
#[tokio::test(flavor = "multi_thread")]
async fn cancelled_fallback_keeps_its_concurrency_permit() {
    let Some(dsn) = dsn() else { return };
    prepare(
        &dsn,
        "lib_cancel",
        "libcancel",
        "CREATE TABLE lib_cancel.t (id bigint PRIMARY KEY);",
    )
    .await;
    let lib = Streamgres::connect(&dsn)
        .publication("lib_cancel_pub")
        .schemas(["lib_cancel"])
        .app("libcancel", 0)
        // `pg_sleep` is not a statement Streamgres answers: let it fall
        // back, to hold a fallback turn.
        .on_unsupported(OnUnsupported::Fallback)
        .fallback(Fallback::Postgres {
            max_concurrent: 1,
            wait: Duration::from_millis(30),
            timeout: Duration::from_secs(5),
        })
        .start()
        .await
        .expect("start");
    let sleep = Sql::new("SELECT pg_sleep(1)");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), lib.load_and_sync(sleep))
            .await
            .is_err()
    );
    for _ in 0..3 {
        assert!(
            matches!(
                lib.load_and_sync(Sql::new("SELECT 1")).await,
                Err(Error::Busy)
            ),
            "a cancelled read's origin task still owns the only turn"
        );
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        lib.load_and_sync(Sql::new("SELECT 1"))
            .await
            .expect("turn returned")
            .len(),
        1
    );
    lib.shutdown().await;
    clean(&dsn, "lib_cancel", "libcancel").await;
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_last_handle_cleans_up_its_slot_in_background() {
    let Some(dsn) = dsn() else { return };
    prepare(
        &dsn,
        "lib_drop",
        "libdrop",
        "CREATE TABLE lib_drop.flags (id int8 PRIMARY KEY);",
    )
    .await;
    let lib = Streamgres::connect(&dsn)
        .publication("lib_drop_pub")
        .schemas(["lib_drop"])
        .app("libdrop", 0)
        .start()
        .await
        .expect("start");
    let slot = lib.engine().instance.expect("running slot");
    drop(lib);
    let client = admin(&dsn).await;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let exists: bool = client
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
                    &[&slot],
                )
                .await
                .expect("find slot")
                .get(0);
            if !exists {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("background cleanup drops slot");
    clean(&dsn, "lib_drop", "libdrop").await;
}

/// RLS changes admission even when the table shape stays unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn row_security_is_excluded_and_enabling_it_invalidates_memory() {
    let Some(dsn) = dsn() else { return };
    prepare(&dsn, "lib_rls", "librls", "CREATE TABLE lib_rls.t (id bigint PRIMARY KEY, tenant bigint); INSERT INTO lib_rls.t VALUES (1,1);").await;
    let lib = Streamgres::connect(&dsn)
        .schemas(["lib_rls"])
        .publication("lib_rls_pub")
        .app("librls", 0)
        .start()
        .await
        .unwrap();
    let sql = Sql::new("SELECT id FROM lib_rls.t");
    until_memory(&lib, &sql).await;
    ddl(
        &dsn,
        "ALTER TABLE lib_rls.t ENABLE ROW LEVEL SECURITY; CREATE POLICY tenant ON lib_rls.t USING (tenant=1);",
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (result, trace) = lib.load_and_sync(sql.clone()).inspect().await;
        if matches!(result, Err(Error::Unsupported(_))) {
            assert_eq!(trace.served, Served::Unsupported);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "RLS admission was not invalidated: {trace}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    lib.shutdown().await;
    let lib = Streamgres::connect(&dsn)
        .schemas(["lib_rls"])
        .publication("lib_rls_pub")
        .app("librls", 0)
        .start()
        .await
        .unwrap();
    assert!(matches!(
        lib.load_and_sync(sql).await,
        Err(Error::Unsupported(_))
    ));
    lib.shutdown().await;
    clean(&dsn, "lib_rls", "librls").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unsupported_policy_does_not_depend_on_engine_readiness() {
    let Some(dsn) = dsn() else { return };
    prepare(
        &dsn,
        "lib_unready",
        "libunready",
        "CREATE TABLE lib_unready.t (id bigint PRIMARY KEY);",
    )
    .await;
    let lib = Streamgres::connect(&dsn)
        .schemas(["lib_unready"])
        .publication("lib_unready_pub")
        .app("libunready", 0)
        .start_timeout(Duration::ZERO)
        .allow_degraded_start()
        .start()
        .await
        .unwrap();
    let (result, trace) = lib
        .load_and_sync(Sql::new("SELECT 42 AS value"))
        .inspect()
        .await;
    assert!(matches!(result, Err(Error::Unsupported(_))));
    assert_eq!(trace.served, Served::Unsupported);
    assert_eq!(lib.metrics().fallbacks, 0);
    lib.shutdown().await;
    clean(&dsn, "lib_unready", "libunready").await;
}

/// A missing DDL trigger fails `start()` whether or not a degraded start
/// is allowed; an engine not ready in time fails it unless allowed.
#[tokio::test(flavor = "multi_thread")]
async fn startup_refuses_a_missing_trigger_and_requires_readiness() {
    let Some(dsn) = dsn() else { return };
    for degraded in [false, true] {
        let builder = Streamgres::connect(&dsn).app("lib_no_trigger", 0);
        let builder = if degraded {
            builder.allow_degraded_start()
        } else {
            builder
        };
        let started = builder.start().await;
        assert!(
            matches!(started, Err(Error::Setup(_))),
            "degraded {degraded}: {:?}",
            started.err()
        );
    }

    prepare(
        &dsn,
        "lib_ready",
        "libready",
        "CREATE TABLE lib_ready.t (id bigint PRIMARY KEY);",
    )
    .await;
    let builder = Streamgres::connect(&dsn)
        .schemas(["lib_ready"])
        .publication("lib_ready_pub")
        .app("libready", 0);
    let started = builder.clone().start_timeout(Duration::ZERO).start().await;
    assert!(
        matches!(started, Err(Error::NotReady(_))),
        "{:?}",
        started.err()
    );
    let lib = builder
        .start()
        .await
        .expect("ready within the default timeout");
    assert!(lib.engine().running);
    lib.shutdown().await;
    clean(&dsn, "lib_ready", "libready").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_queued_fallback_never_executes() {
    let Some(dsn) = dsn() else { return };
    prepare(
        &dsn,
        "lib_queue",
        "libqueue",
        "CREATE TABLE lib_queue.t (id bigint PRIMARY KEY);",
    )
    .await;
    let lib = Streamgres::connect(&dsn)
        .schemas(["lib_queue"])
        .publication("lib_queue_pub")
        .app("libqueue", 0)
        .on_unsupported(OnUnsupported::Fallback)
        .fallback(Fallback::Postgres {
            max_concurrent: 1,
            wait: Duration::from_secs(5),
            timeout: Duration::from_secs(5),
        })
        .start()
        .await
        .unwrap();
    let client = admin(&dsn).await;
    let first_lib = lib.clone();
    let first = tokio::spawn(async move {
        first_lib
            .load_and_sync(Sql::new("SELECT pg_sleep(0.5) /* occupy_turn */"))
            .await
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let active:i64=client.query_one("SELECT count(*) FROM pg_stat_activity WHERE query='SELECT pg_sleep(0.5) /* occupy_turn */' AND state='active'",&[]).await.unwrap().get(0);
        if active > 0 {
            break;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let queued_lib = lib.clone();
    let queued = tokio::spawn(async move {
        queued_lib
            .load_and_sync(Sql::new("SELECT pg_sleep(1) /* cancelled_queue */"))
            .await
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    queued.abort();
    let _ = queued.await;
    first.await.unwrap().unwrap();
    // An uncancelled detached queue would have taken this turn for a second.
    let read = tokio::time::timeout(
        Duration::from_millis(300),
        lib.load_and_sync(Sql::new("SELECT 1")),
    )
    .await;
    assert!(
        read.is_ok(),
        "a cancelled queue still occupied the fallback"
    );
    read.unwrap().unwrap();
    let active:i64=client.query_one("SELECT count(*) FROM pg_stat_activity WHERE query='SELECT pg_sleep(1) /* cancelled_queue */' AND state='active'",&[]).await.unwrap().get(0);
    assert_eq!(active, 0);
    lib.shutdown().await;
    clean(&dsn, "lib_queue", "libqueue").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn encoding_refusal_is_plan_scoped_and_never_counted_as_memory() {
    let Some(dsn) = dsn() else { return };
    prepare(&dsn,"lib_encode","libencode","CREATE TABLE lib_encode.t (id bigint PRIMARY KEY, amount numeric); INSERT INTO lib_encode.t VALUES (1,1.234567890123456789);").await;
    let lib = Streamgres::connect(&dsn)
        .schemas(["lib_encode"])
        .publication("lib_encode_pub")
        .app("libencode", 0)
        .start()
        .await
        .unwrap();
    let id = Sql::new("SELECT id FROM lib_encode.t");
    until_memory(&lib, &id).await;
    let amount = Sql::new("SELECT amount FROM lib_encode.t");
    let before = lib.metrics().answered;
    for _ in 0..3 {
        let (result, trace) = lib.load_and_sync(amount.clone()).inspect().await;
        result.unwrap();
        assert_eq!(trace.served, Served::Postgres);
    }
    assert_eq!(lib.metrics().answered, before);
    assert!(matches!(lib.inspect(&amount).state, QueryState::Refused(_)));
    let (result, trace) = lib.load_and_sync(id).inspect().await;
    result.unwrap();
    assert_eq!(
        trace.served,
        Served::Memory,
        "failed numeric projection poisoned id"
    );
    admin(&dsn)
        .await
        .batch_execute("UPDATE lib_encode.t SET amount = NULL")
        .await
        .unwrap();
    let recovered = until_memory(&lib, &amount).await;
    assert!(
        recovered.row(0).unwrap().bytes(0).is_none(),
        "an old encoding refusal survived a row generation change"
    );
    lib.shutdown().await;
    clean(&dsn, "lib_encode", "libencode").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unqualified_type_metadata_follows_session_visibility() {
    use streamgres::cache::Lease;
    let Some(dsn) = dsn() else { return };
    prepare(&dsn,"lib_typepath","libtypepath","CREATE TYPE lib_typepath.shadow_type AS ENUM ('a'); CREATE TYPE lib_typepath.hidden_type AS ENUM ('b'); CREATE SCHEMA lib_typehidden; CREATE TYPE lib_typehidden.shadow_type AS ENUM ('b'); CREATE TABLE lib_typepath.t (id bigint PRIMARY KEY);").await;
    let mut config: tokio_postgres::Config = dsn.parse().unwrap();
    config.options("-csearch_path=lib_typehidden,lib_typepath,public");
    let connections = move || {
        let config = config.clone();
        async move {
            let (client, connection) = config
                .connect(tokio_postgres::NoTls)
                .await
                .map_err(|e| e.to_string())?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            Ok(Lease::new(client))
        }
    };
    let lib = Streamgres::connect(&dsn)
        .connections(connections)
        .schemas(["lib_typepath"])
        .publication("lib_typepath_pub")
        .app("libtypepath", 0)
        .start()
        .await
        .unwrap();
    let expected: i64 = admin(&dsn)
        .await
        .query_one(
            "SELECT 'lib_typehidden.shadow_type'::regtype::oid::bigint",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        lib.type_oids("shadow_type", None).unwrap().0,
        u32::try_from(expected).unwrap()
    );
    assert_ne!(
        lib.type_oids("shadow_type", Some("lib_typepath"))
            .unwrap()
            .0,
        u32::try_from(expected).unwrap()
    );
    lib.shutdown().await;
    ddl(&dsn, "DROP SCHEMA lib_typehidden CASCADE").await;
    clean(&dsn, "lib_typepath", "libtypepath").await;
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_app_pool_operations_reject_a_current_thread_runtime() {
    use streamgres::cache::Lease;
    let Some(dsn) = dsn() else { return };
    prepare(
        &dsn,
        "lib_block",
        "libblock",
        "CREATE TABLE lib_block.t (id bigint PRIMARY KEY);",
    )
    .await;
    let app_dsn = dsn.clone();
    let connections = move || {
        let dsn = app_dsn.clone();
        async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .map_err(|e| e.to_string())?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            Ok(Lease::new(client))
        }
    };
    let lib = Streamgres::connect(&dsn)
        .connections(connections)
        .schemas(["lib_block"])
        .publication("lib_block_pub")
        .app("libblock", 0)
        .on_unsupported(OnUnsupported::Fallback)
        .start()
        .await
        .unwrap();
    let result = lib.load_and_sync(Sql::new("SELECT 1")).wait();
    assert_eq!(result.unwrap_err(), Error::BlockingContext);
    assert_eq!(
        lib.refresh_types_blocking().unwrap_err(),
        Error::BlockingContext
    );
    assert_eq!(lib.shutdown_blocking().unwrap_err(), Error::BlockingContext);
    lib.shutdown().await;
    clean(&dsn, "lib_block", "libblock").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn refresh_errors_preserve_the_pool_failure() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use streamgres::cache::Lease;
    let Some(dsn) = dsn() else { return };
    prepare(
        &dsn,
        "lib_refresh",
        "librefresh",
        "CREATE TABLE lib_refresh.t (id bigint PRIMARY KEY);",
    )
    .await;
    let fail = Arc::new(AtomicBool::new(false));
    let gate = fail.clone();
    let app_dsn = dsn.clone();
    let connections = move || {
        let fail = gate.clone();
        let dsn = app_dsn.clone();
        async move {
            if fail.load(Ordering::Acquire) {
                return Err("pool unavailable for metadata".to_owned());
            }
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .map_err(|e| e.to_string())?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            Ok(Lease::new(client))
        }
    };
    let lib = Streamgres::connect(&dsn)
        .connections(connections)
        .schemas(["lib_refresh"])
        .publication("lib_refresh_pub")
        .app("librefresh", 0)
        .start()
        .await
        .unwrap();
    // Let the first background refresh complete before forcing the next checkout.
    tokio::time::sleep(Duration::from_millis(400)).await;
    fail.store(true, Ordering::Release);
    let error = lib.refresh_types().await.unwrap_err();
    assert!(
        error.to_string().contains("pool unavailable for metadata"),
        "{error}"
    );
    fail.store(false, Ordering::Release);
    lib.refresh_types().await.unwrap();
    lib.shutdown().await;
    clean(&dsn, "lib_refresh", "librefresh").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_first_catalog_observation_refreshes_custom_types() {
    let Some(dsn) = dsn() else { return };
    prepare(
        &dsn,
        "lib_firsttype",
        "libfirsttype",
        "CREATE TABLE lib_firsttype.t (id bigint PRIMARY KEY);",
    )
    .await;
    let lib = Streamgres::connect(&dsn)
        .schemas(["lib_firsttype"])
        .publication("lib_firsttype_pub")
        .app("libfirsttype", 0)
        .start()
        .await
        .unwrap();
    // No explicit refresh or Diesel lookup retry: background discovery must
    // include a type created before its first catalog observation.
    ddl(&dsn, "CREATE TYPE lib_firsttype.first_type AS ENUM ('a')").await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while lib.type_oids("first_type", Some("lib_firsttype")).is_none() {
        assert!(
            Instant::now() < deadline,
            "the first catalog observation skipped metadata"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    lib.shutdown().await;
    clean(&dsn, "lib_firsttype", "libfirsttype").await;
}

/// Bind values too large for the cache's budget never decide whether a
/// statement may reach Postgres: an unsupported one is still an error,
/// and only a supported one falls back.
#[tokio::test(flavor = "multi_thread")]
async fn oversized_binds_do_not_bypass_the_unsupported_policy() {
    let Some(dsn) = dsn() else { return };
    prepare(
        &dsn,
        "lib_budget",
        "libbudget",
        "CREATE TABLE lib_budget.t (id bigint PRIMARY KEY, name text); INSERT INTO lib_budget.t VALUES (1, 'a');",
    )
    .await;
    let lib = Streamgres::connect(&dsn)
        .schemas(["lib_budget"])
        .publication("lib_budget_pub")
        .app("libbudget", 0)
        .start()
        .await
        .unwrap();
    until_memory(&lib, &Sql::new("SELECT id FROM lib_budget.t")).await;
    let large = "x".repeat(2 << 20);

    let before = lib.metrics();
    let unsupported =
        Sql::new("SELECT id FROM lib_budget.t WHERE name ILIKE $1").bind(large.as_str());
    let (read, trace) = lib.load_and_sync(unsupported).inspect().await;
    assert!(matches!(read, Err(Error::Unsupported(_))), "{read:?}");
    assert_eq!(trace.served, Served::Unsupported);
    let after = lib.metrics();
    assert_eq!(
        (after.fallbacks, after.fallback_errors),
        (before.fallbacks, before.fallback_errors),
        "the statement never reached Postgres"
    );

    // Supported SQL, but a bind whose type has no Postgres comparison with
    // its column: decided by the types alone, without reading the value.
    let before = lib.metrics();
    let mistyped = Sql::new("SELECT id FROM lib_budget.t WHERE id = $1").bind(large.as_str());
    let (read, trace) = lib.load_and_sync(mistyped).inspect().await;
    assert!(matches!(read, Err(Error::Unsupported(_))), "{read:?}");
    assert_eq!(trace.served, Served::Unsupported);
    let after = lib.metrics();
    assert_eq!(
        (after.fallbacks, after.fallback_errors),
        (before.fallbacks, before.fallback_errors)
    );

    let supported = Sql::new("SELECT id FROM lib_budget.t WHERE name = $1").bind(large.as_str());
    let (read, trace) = lib.load_and_sync(supported).inspect().await;
    assert_eq!(read.expect("Postgres answers").len(), 0);
    assert_eq!(trace.served, Served::Postgres);

    lib.shutdown().await;
    clean(&dsn, "lib_budget", "libbudget").await;
}

/// A custom type's bind keeps its own OID on Streamgres's pool: an
/// optional filter (`$1 IS NULL OR …`), which Postgres can type only from
/// the bind, reads on the fallback as it does for the application.
#[tokio::test(flavor = "multi_thread")]
async fn custom_type_binds_keep_their_oid_on_the_default_pool() {
    let Some(dsn) = dsn() else { return };
    prepare(
        &dsn,
        "lib_oid",
        "liboid",
        "CREATE TYPE lib_oid.state AS ENUM ('open', 'closed');
         CREATE TABLE lib_oid.t (id bigint PRIMARY KEY, state lib_oid.state NOT NULL);
         INSERT INTO lib_oid.t VALUES (1, 'open'), (2, 'closed');",
    )
    .await;
    let lib = Streamgres::connect(&dsn)
        .schemas(["lib_oid"])
        .publication("lib_oid_pub")
        .app("liboid", 0)
        .on_unsupported(OnUnsupported::Fallback)
        .start()
        .await
        .unwrap();
    let (state, _) = lib
        .type_oids("state", Some("lib_oid"))
        .expect("the enum's OIDs");
    let sql = Sql::new("SELECT id FROM lib_oid.t WHERE $1 IS NULL OR state = $1")
        .bind_raw(state, Some(b"open".to_vec()));

    let (read, trace) = lib.load_and_sync(sql).inspect().await;
    assert_eq!(trace.served, Served::Postgres, "{trace}");
    let rows = read.expect("the fallback types the bind by its OID");
    assert_eq!((rows.len(), int8(&rows, 0, 0)), (1, Some(1)));

    lib.shutdown().await;
    clean(&dsn, "lib_oid", "liboid").await;
}

/// A driver connection reports Postgres's SQLSTATE and a statement
/// timeout as such, without its message being read.
#[tokio::test(flavor = "multi_thread")]
async fn session_errors_keep_their_sqlstate_and_timeouts() {
    use streamgres::cache::{Session, SessionError};
    let Some(dsn) = dsn() else { return };
    let mut client = admin(&dsn).await;
    let error = Session::query(&mut client, "SELECT 1 / 0", &[])
        .await
        .unwrap_err();
    assert!(
        matches!(&error, SessionError::Database { code: Some(code), .. } if code == "22012"),
        "{error:?}"
    );
    Session::execute(&mut client, "SET statement_timeout = 50")
        .await
        .unwrap();
    let error = Session::query(&mut client, "SELECT pg_sleep(1)", &[])
        .await
        .unwrap_err();
    assert!(matches!(error, SessionError::Timeout(_)), "{error:?}");
}

/// A trigger check that never completes does not hold `start()` past
/// its start timeout.
#[tokio::test(flavor = "multi_thread")]
async fn a_hung_trigger_check_is_bounded_by_the_start_timeout() {
    let Some(dsn) = dsn() else { return };
    let never = || std::future::pending::<Result<streamgres::cache::Lease, String>>();
    let started = tokio::time::timeout(
        Duration::from_secs(5),
        Streamgres::connect(&dsn)
            .app("lib_hung_check", 0)
            .connections(never)
            .start_timeout(Duration::from_millis(300))
            .start(),
    )
    .await
    .expect("start() returns within its timeout");
    assert!(
        matches!(started, Err(Error::NotReady(_))),
        "{:?}",
        started.err()
    );
}

/// A statement cancelled on request is a cancellation, not a timeout.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_statement_is_not_a_timeout() {
    use streamgres::cache::{Session, SessionError};
    let Some(dsn) = dsn() else { return };
    let mut client = admin(&dsn).await;
    let pid = Session::query(&mut client, "SELECT pg_backend_pid()::int8", &[])
        .await
        .unwrap()
        .int(0, 0)
        .unwrap()
        .unwrap();
    let canceller = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        admin(&dsn)
            .await
            .execute("SELECT pg_cancel_backend($1::int4)", &[&(pid as i32)])
            .await
            .unwrap();
    });
    let error = Session::query(&mut client, "SELECT pg_sleep(5)", &[])
        .await
        .unwrap_err();
    canceller.await.unwrap();
    assert!(matches!(error, SessionError::Cancelled(_)), "{error:?}");
}

/// A failed fallback read keeps its structure through the public API.
#[tokio::test(flavor = "multi_thread")]
async fn fallback_failures_are_structured_in_the_public_error() {
    use streamgres::cache::SessionError;
    let Some(dsn) = dsn() else { return };
    prepare(
        &dsn,
        "lib_origin",
        "liborigin",
        "CREATE TABLE lib_origin.t (id bigint PRIMARY KEY);",
    )
    .await;
    let lib = Streamgres::connect(&dsn)
        .schemas(["lib_origin"])
        .publication("lib_origin_pub")
        .app("liborigin", 0)
        .on_unsupported(OnUnsupported::Fallback)
        .fallback(Fallback::Postgres {
            max_concurrent: 2,
            wait: Duration::from_secs(1),
            timeout: Duration::from_millis(300),
        })
        .start()
        .await
        .unwrap();
    let read = lib.load_and_sync(Sql::new("SELECT 1 / 0")).await;
    assert!(
        matches!(&read, Err(Error::Origin(SessionError::Database { code: Some(code), .. })) if code == "22012"),
        "{read:?}"
    );
    let read = lib.load_and_sync(Sql::new("SELECT pg_sleep(2)")).await;
    assert!(
        matches!(read, Err(Error::Origin(SessionError::Timeout(_)))),
        "{read:?}"
    );
    lib.shutdown().await;
    clean(&dsn, "lib_origin", "liborigin").await;
}
