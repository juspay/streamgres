//! The desk's two ways to the data: a pool of `diesel-async` connections
//! for writes and for reads straight from Postgres, and one [`Streamgres`]
//! for reads through memory.
//!
//! The writer and every write use the pool, never Streamgres, so their
//! changes reach memory the way another service's would: through the
//! replication stream.

use std::time::Duration;

use diesel::QueryResult;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::{self, PooledConnection};
use diesel_async::{AsyncPgConnection, SimpleAsyncConnection};
use streamgres_diesel::{Builder, Streamgres};

use crate::seed;

/// The publication Streamgres streams.
pub const PUBLICATION: &str = "demo_pub";
/// The app id the schema-change trigger stack is installed for.
pub const APP: &str = "demo";

/// A pool of Postgres connections.
pub type Pool = bb8::Pool<AsyncPgConnection>;
/// One connection of the pool.
pub type Conn<'a> = PooledConnection<'a, AsyncPgConnection>;

/// How Streamgres is set up for the desk.
#[derive(Debug, Clone)]
pub struct Settings {
    pub cache_budget: usize,
    pub max_lag: Duration,
    /// Reads recorded per query for inspection.
    pub debug_history: usize,
    /// Connections in Streamgres's own pool.
    pub pool_size: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            cache_budget: 256 << 20,
            max_lag: Duration::from_secs(2),
            debug_history: 32,
            pool_size: 8,
        }
    }
}

/// A pool of `size` connections to `database_url`, each giving up on a
/// statement after ten seconds.
pub async fn pool(database_url: &str, size: u32) -> Result<Pool, String> {
    let mut config = diesel_async::pooled_connection::ManagerConfig::default();
    config.custom_setup = Box::new(|url| {
        Box::pin(async move {
            let mut conn =
                <AsyncPgConnection as diesel_async::AsyncConnection>::establish(url).await?;
            conn.batch_execute(
                "SET statement_timeout = '10s'; SET idle_in_transaction_session_timeout = '10s'",
            )
            .await
            .map_err(|error| diesel::ConnectionError::BadConnection(error.to_string()))?;
            Ok(conn)
        })
    });
    let manager =
        AsyncDieselConnectionManager::<AsyncPgConnection>::new_with_config(database_url, config);
    bb8::Pool::builder()
        .max_size(size)
        .connection_timeout(Duration::from_secs(3))
        .build(manager)
        .await
        .map_err(|error| format!("the connection pool: {error}"))
}

/// The schema, the data when there is none (or always, with `reseed`),
/// the publication and the trigger stack: what an administrator does
/// once per database. Safe to run again.
pub async fn install(conn: &mut AsyncPgConnection, reseed: bool) -> QueryResult<()> {
    conn.batch_execute(seed::SCHEMA).await?;
    if reseed || !seed::present(conn).await? {
        seed::load(conn).await?;
    }
    conn.batch_execute(&format!(
        "DO $$ BEGIN
            CREATE PUBLICATION {PUBLICATION} FOR ALL TABLES;
         EXCEPTION WHEN duplicate_object THEN NULL; END $$;"
    ))
    .await?;
    conn.batch_execute(&builder("", &Settings::default()).setup_sql())
        .await
}

/// Streamgres for the desk, before it starts.
fn builder(database_url: &str, settings: &Settings) -> Builder {
    Streamgres::connect(database_url)
        .publication(PUBLICATION)
        .app(APP, 0)
        .cache_budget(settings.cache_budget)
        .max_lag(settings.max_lag)
        .debug_history(settings.debug_history)
        .pool_size(settings.pool_size)
}

/// Starts Streamgres on `database_url`, with its own pool.
pub async fn start(database_url: &str, settings: &Settings) -> Result<Streamgres, String> {
    builder(database_url, settings)
        .start()
        .await
        .map_err(|error| format!("Streamgres: {error}"))
}
