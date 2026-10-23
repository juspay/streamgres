//! A database of its own for each live test. Event triggers fire on every
//! statement in their database, other tests' too, so tests that share one
//! trip over each other's schema-change trigger stacks: one dropping its
//! stack while another's DDL runs fails that DDL. Each test that installs
//! a stack, or follows schema changes, takes a [`Database`] instead.

use std::fmt;
use std::ops::Deref;
use std::time::{Duration, Instant};

use tokio_postgres::NoTls;

/// A fresh database on the server `STREAMGRES_PG_DSN` names, dropped with
/// its slots when this goes. Dereferences to its own DSN.
pub struct Database {
    dsn: String,
    name: String,
    server: String,
}

impl Database {
    /// A fresh database for the test tagged `tag`; `None`, and the test
    /// skipped, when no server is named.
    pub fn create(tag: &str) -> Option<Database> {
        let Ok(server) = std::env::var("STREAMGRES_PG_DSN") else {
            eprintln!("STREAMGRES_PG_DSN not set: live Postgres scenario skipped");
            return None;
        };
        let name = format!("sg_test_{tag}_{}", std::process::id());
        let statements = [
            format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"),
            format!("CREATE DATABASE {name}"),
        ];
        let on = server.clone();
        on_own_runtime(async move {
            let client = connect(&on).await;
            for statement in statements {
                client.batch_execute(&statement).await.expect("a database of its own");
            }
        });
        Some(Database {
            dsn: with_database(&server, &name),
            name,
            server,
        })
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
