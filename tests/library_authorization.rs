//! Live check of cache authorization after a PostgreSQL ACL change.

use std::time::{Duration, Instant};

use streamgres::cache::{Error, Served, Sql, Streamgres};

#[tokio::test(flavor = "multi_thread")]
async fn cached_reads_follow_database_local_revocation() {
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        return;
    };
    let (admin, driver) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(driver);
    let id = uuid::Uuid::new_v4().simple();
    let schema = format!("acl_test_{id}");
    let role = format!("acl_reader_{id}");
    let app = format!("aclapp{id}");
    // information_schema.table_constraints needs a privilege beyond SELECT.
    admin
        .batch_execute(&format!(
            "CREATE ROLE {role} LOGIN REPLICATION PASSWORD 'reader'; CREATE SCHEMA {schema};
             CREATE TABLE {schema}.t (id bigint PRIMARY KEY);
             INSERT INTO {schema}.t VALUES (1);
             GRANT USAGE ON SCHEMA {schema} TO {role};
             GRANT SELECT, REFERENCES ON {schema}.t TO {role};
             CREATE PUBLICATION {schema}_pub FOR TABLES IN SCHEMA {schema};"
        ))
        .await
        .expect("fixtures");
    // Streamgres connects as the reader itself: its own pool checks what
    // that role may read.
    let Some(reader_dsn) = as_role(&dsn, &role) else {
        eprintln!("STREAMGRES_PG_DSN is not a URL: authorization scenario skipped");
        return;
    };
    let builder = Streamgres::connect(&reader_dsn)
        .app(&app, 0)
        .schemas([schema.as_str()])
        .publication(format!("{schema}_pub"));
    admin
        .batch_execute(&builder.setup_sql())
        .await
        .expect("setup");
    let lib = builder.start().await.expect("start");
    let sql = Sql::new(format!("SELECT id FROM {schema}.t"));
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let (rows, trace) = lib.load_and_sync(sql.clone()).inspect().await;
        rows.expect("authorized read");
        if trace.served == Served::Memory {
            break;
        }
        assert!(Instant::now() < deadline, "query did not warm");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    admin
        .batch_execute(&format!("REVOKE SELECT ON {schema}.t FROM {role}"))
        .await
        .expect("revoke");
    // The DDL signal must take effect before the five-second TTL expires.
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let result = lib.load_and_sync(sql.clone()).await;
        if matches!(result, Err(Error::Unsupported(_))) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "authorization refresh did not follow REVOKE"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    lib.shutdown().await;
    admin
        .batch_execute(&format!(
            "DROP SCHEMA {app}_0 CASCADE; DROP PUBLICATION {schema}_pub;
             DROP SCHEMA {schema} CASCADE; DROP ROLE {role};"
        ))
        .await
        .expect("cleanup");
}

/// `dsn` (a URL) with `role` and its password as the user.
fn as_role(dsn: &str, role: &str) -> Option<String> {
    let (scheme, rest) = dsn.split_once("://")?;
    let (_, host) = rest.split_once('@')?;
    Some(format!("{scheme}://{role}:reader@{host}"))
}
