//! A database of its own for each live test. Event triggers fire on every
//! statement in their database, other tests' too, so tests that share one
//! trip over each other's schema-change trigger stacks: one dropping its
//! stack while another's DDL runs fails that DDL. Each test that installs
//! a stack, or follows schema changes, takes a [`Database`] instead.
//!
//! A live test skips itself when what it needs is not given, so the suite
//! runs on a laptop with nothing set up. CI says what it gives instead:
//! `STREAMGRES_REQUIRE_LIVE` lists requirements (`postgres`, `s2s`,
//! `s2s-token`), and a test that would skip for want of one listed fails
//! ([`skip`]). `STREAMGRES_TEST_PG_MAJOR` names the major version the
//! server must be, so a job meant for one version cannot run on another;
//! a test that needs a later one than the server skips by saying so
//! ([`Database::at_least`]).

use std::fmt;
use std::ops::Deref;
use std::time::{Duration, Instant};

use tokio_postgres::NoTls;

/// Whether `STREAMGRES_REQUIRE_LIVE` lists `requirement`.
fn required(requirement: &str) -> bool {
    std::env::var("STREAMGRES_REQUIRE_LIVE")
        .is_ok_and(|list| list.split(',').any(|listed| listed.trim() == requirement))
}

/// A live test skips itself for want of `requirement`, saying `why`; or,
/// when `STREAMGRES_REQUIRE_LIVE` lists the requirement, fails instead,
/// so a run that was meant to exercise it cannot pass without doing so.
#[allow(dead_code, reason = "each test binary uses what it needs of the support module")]
pub fn skip(requirement: &str, why: &str) {
    assert!(
        !required(requirement),
        "{why}; STREAMGRES_REQUIRE_LIVE lists `{requirement}`, so this test must run here"
    );
    eprintln!("{why}: skipped");
}

/// A fresh database on the server `STREAMGRES_PG_DSN` names, dropped with
/// its slots when this goes. Dereferences to its own DSN.
pub struct Database {
    dsn: String,
    name: String,
    server: String,
    /// The server's major version (16, 17).
    major: u32,
}

impl Database {
    /// A fresh database for the test tagged `tag`; `None`, and the test
    /// skipped, when no server is named ([`skip`], requirement
    /// `postgres`). A server of another major version than
    /// `STREAMGRES_TEST_PG_MAJOR` names fails the test.
    pub fn create(tag: &str) -> Option<Database> {
        let Ok(server) = std::env::var("STREAMGRES_PG_DSN") else {
            skip("postgres", "STREAMGRES_PG_DSN not set: live Postgres scenario");
            return None;
        };
        let name = format!("sg_test_{tag}_{}", std::process::id());
        let statements = [
            format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"),
            format!("CREATE DATABASE {name}"),
        ];
        let on = server.clone();
        let (sender, version) = std::sync::mpsc::channel();
        on_own_runtime(async move {
            let client = connect(&on).await;
            for statement in statements {
                client.batch_execute(&statement).await.expect("a database of its own");
            }
            let number: i32 = client
                .query_one("SELECT current_setting('server_version_num')::int4", &[])
                .await
                .expect("the server's version")
                .get(0);
            let _ = sender.send(number);
        });
        let major = u32::try_from(version.recv().expect("the server's version") / 10_000)
            .expect("a positive version");
        if let Ok(expected) = std::env::var("STREAMGRES_TEST_PG_MAJOR") {
            assert_eq!(
                expected.trim(),
                major.to_string(),
                "STREAMGRES_TEST_PG_MAJOR names the server this run is for, and it is {major}"
            );
        }
        Some(Database {
            dsn: with_database(&server, &name),
            name,
            server,
            major,
        })
    }

    /// Whether the server is PostgreSQL `major` or later; a test that
    /// needs it skips itself otherwise, saying what it needs. Not a
    /// requirement [`skip`] can insist on: the run for that version
    /// (`STREAMGRES_TEST_PG_MAJOR`) is the one that covers it.
    #[allow(dead_code, reason = "each test binary uses what it needs of the support module")]
    pub fn at_least(&self, major: u32, test: &str) -> bool {
        if self.major >= major {
            return true;
        }
        eprintln!("{test} needs PostgreSQL {major}, and the server is {}: skipped", self.major);
        false
    }
}

impl Deref for Database {
    type Target = String;

    /// The database's DSN.
    fn deref(&self) -> &String {
        &self.dsn
    }
}

impl fmt::Display for Database {
    /// The database's DSN.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.dsn)
    }
}

impl Drop for Database {
    /// Drops the database: its slots first (a database a slot points at
    /// cannot be dropped), once their walsenders have let go of them.
    fn drop(&mut self) {
        let (server, name) = (self.server.clone(), self.name.clone());
        on_own_runtime(async move {
            let client = connect(&server).await;
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let _ = client
                    .execute(
                        "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
                         WHERE database = $1 AND active_pid IS NOT NULL",
                        &[&name],
                    )
                    .await;
                let _ = client
                    .execute(
                        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
                         WHERE database = $1 AND NOT active",
                        &[&name],
                    )
                    .await;
                let left: i64 = client
                    .query_one(
                        "SELECT count(*) FROM pg_replication_slots WHERE database = $1",
                        &[&name],
                    )
                    .await
                    .map_or(0, |row| row.get(0));
                if left == 0 || Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let _ = client
                .batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                .await;
        });
    }
}

/// A connection to `dsn`, its driver spawned.
async fn connect(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// Runs `work` to the end on a runtime of its own, on a thread of its
/// own, so it can run from anywhere, inside another runtime included.
fn on_own_runtime(work: impl Future<Output = ()> + Send + 'static) {
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(work);
    })
    .join()
    .expect("database work");
}

/// `dsn` with its database replaced by `database`, as a URL or as
/// `key=value` pairs.
fn with_database(dsn: &str, database: &str) -> String {
    if !dsn.contains("://") {
        return format!("{dsn} dbname={database}");
    }
    let (base, query) = dsn.split_once('?').map_or((dsn, None), |(base, query)| (base, Some(query)));
    let after_scheme = base.find("://").map_or(0, |at| at + 3);
    let server = match base[after_scheme..].find('/') {
        Some(slash) => &base[..after_scheme + slash],
        None => base,
    };
    match query {
        Some(query) => format!("{server}/{database}?{query}"),
        None => format!("{server}/{database}"),
    }
}
