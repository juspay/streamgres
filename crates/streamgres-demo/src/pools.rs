//! The connections the desk uses, and how each is routed.
//!
//! Three pools against one database:
//!
//! - `main`: the application's pool, through the cache, routed
//!   Streamgres. Every read in the HTTP API goes through it.
//! - `reporting`: the same wrapper, routed Postgres by its URL, the way
//!   a pool against a primary would be. Shows the per-pool flag.
//! - `plain`: diesel-async's own connection, no wrapper. The background
//!   writer uses it, so its changes reach the cache the way another
//!   service's would: through the engine, not the read-your-writes
//!   window.
//!
//! And a small blocking pool of the sync wrapper, sharing the same
//! cache, for the paths that are not async.

use std::sync::Arc;
use std::time::Duration;

use diesel::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool as SyncPool};
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::Pool;
use diesel_streamgres::cache::{Cache, Limits};
use diesel_streamgres::engine::EngineClient;
use diesel_streamgres::{Attachment, Route, Settings, install};

use crate::queries::{AsyncConn, SyncConn};
use crate::{verify, writer};

/// Everything a process holds: the cache, the engine client, the pools,
/// and the counters of the writer and the verify loop.
pub struct Stack {
    pub cache: Arc<Cache>,
    pub engine: EngineClient,
    pub main: Pool<AsyncConn>,
    pub reporting: Pool<AsyncConn>,
    pub plain: Pool<AsyncPgConnection>,
    pub sync: SyncPool<ConnectionManager<SyncConn>>,
    pub writer: Arc<writer::Counts>,
    pub verify: Arc<verify::Counts>,
}

/// The URL a pool is established with decides its route; the settings
/// read the application name off it.
pub fn pool_url(database_url: &str, application_name: &str) -> String {
    let separator = if database_url.contains('?') { '&' } else { '?' };
    format!("{database_url}{separator}application_name={application_name}")
}

/// Installs the process-wide settings once: one cache for the database,
/// a Postgres route for the reporting pool, Streamgres for the rest.
fn install_settings(cache: &Arc<Cache>) {
    let cache = Arc::clone(cache);
    let _ = install(Settings::by_url(move |url| {
        Some(Attachment {
            cache: Arc::clone(&cache),
            route: if url.contains("application_name=reporting") {
                Route::Postgres
            } else {
                Route::Streamgres
            },
        })
    }));
}

impl Stack {
    /// Connects to the engine and opens the pools. The engine's sockets
    /// come up in the background; reads before that go live.
    pub async fn connect(database_url: &str, s2s_url: &str) -> Result<Stack, String> {
        let engine = EngineClient::connect(s2s_url);
        let cache = Arc::new(Cache::with_limits(
            Limits::from_env(),
            Arc::new(engine.clone()),
        ));
        engine.attach(&cache);
        install_settings(&cache);

        let pool = |name: &str| {
            let manager =
                AsyncDieselConnectionManager::<AsyncConn>::new(pool_url(database_url, name));
            Pool::builder()
                .max_size(16)
                .connection_timeout(Duration::from_secs(10))
                .build(manager)
        };
        let main = pool("main")
            .await
            .map_err(|error| format!("main pool: {error}"))?;
        let reporting = pool("reporting")
            .await
            .map_err(|error| format!("reporting pool: {error}"))?;
        let plain = Pool::builder()
            .max_size(4)
            .build(AsyncDieselConnectionManager::<AsyncPgConnection>::new(
                pool_url(database_url, "plain"),
            ))
            .await
            .map_err(|error| format!("plain pool: {error}"))?;
        let sync = SyncPool::builder()
            .max_size(4)
            .build(ConnectionManager::<SyncConn>::new(pool_url(
                database_url,
                "sync",
            )))
            .map_err(|error| format!("sync pool: {error}"))?;
        Ok(Stack {
            cache,
            engine,
            main,
            reporting,
            plain,
            sync,
            writer: Arc::default(),
            verify: Arc::default(),
        })
    }

    /// Waits for at least one engine socket, up to `wait`.
    pub async fn wait_for_engine(&self, wait: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + wait;
        while !self.engine.is_connected() {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        true
    }

    /// A plain, unwrapped blocking connection: for the seed.
    pub fn plain_sync(database_url: &str) -> diesel::ConnectionResult<PgConnection> {
        use diesel::Connection;
        PgConnection::establish(database_url)
    }
}
