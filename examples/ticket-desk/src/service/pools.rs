use diesel::connection::SimpleConnection;
use diesel::connection::{Instrumentation, InstrumentationEvent, TransactionManager};
use diesel::r2d2::{self, ConnectionManager, CustomizeConnection};
use diesel::{Connection, PgConnection};
use diesel_async::pooled_connection::bb8;
use diesel_async::pooled_connection::{AsyncDieselConnectionManager, ManagerConfig};
use diesel_async::{AsyncConnection, AsyncPgConnection, SimpleAsyncConnection};
use futures_util::FutureExt;
use std::time::Duration;
use streamgres_diesel::Tracker;

pub type Pool = r2d2::Pool<ConnectionManager<PgConnection>>;
pub type AsyncPool = bb8::Pool<AsyncPgConnection>;
#[derive(Debug)]
struct Tracking(bool);
struct Quiet;
impl Instrumentation for Quiet {
    fn on_connection_event(&mut self, _: InstrumentationEvent<'_>) {}
}
impl CustomizeConnection<PgConnection, r2d2::Error> for Tracking {
    fn on_acquire(&self, conn: &mut PgConnection) -> Result<(), r2d2::Error> {
        conn.batch_execute(
            "SET statement_timeout='5s'; SET idle_in_transaction_session_timeout='8s'",
        )
        .map_err(r2d2::Error::QueryError)?;
        if self.0 {
            conn.set_instrumentation(Tracker::new());
        } else {
            conn.set_instrumentation(Quiet);
        }
        Ok(())
    }
}
pub fn create(url: &str, size: u32, tracked: bool) -> Result<Pool, String> {
    r2d2::Pool::builder()
        .max_size(size)
        .min_idle(Some(1))
        .connection_timeout(Duration::from_secs(3))
        // Plain pools validate before reads/writes. Tracked cache hits avoid a
        // checkout round trip; a failed sync query explicitly retires its lease.
        .test_on_check_out(!tracked)
        .connection_customizer(Box::new(Tracking(tracked)))
        .build(ConnectionManager::new(url))
        .map_err(|error| format!("opening the connection pool: {error}"))
}
pub async fn create_async(url: &str, size: u32) -> Result<AsyncPool, String> {
    let mut config = ManagerConfig::default();
    config.custom_setup = Box::new(|url| {
        async move {
            let mut conn = AsyncPgConnection::establish(url).await?;
            conn.batch_execute(
                "SET statement_timeout='5s'; SET idle_in_transaction_session_timeout='8s'",
            )
            .await
            .map_err(diesel::ConnectionError::CouldntSetupConfiguration)?;
            conn.set_instrumentation(Tracker::new());
            Ok(conn)
        }
        .boxed()
    });
    bb8::Pool::builder()
        .max_size(size)
        .min_idle(Some(1))
        .connection_timeout(Duration::from_secs(3))
        .test_on_check_out(false)
        .build(AsyncDieselConnectionManager::new_with_config(url, config))
        .await
        .map_err(|error| format!("opening the async pool: {error}"))
}

/// Diesel's sync pool check only inspects transaction state. An idle libpq
/// socket can be disconnected without that state becoming broken. Retire any
/// failed sync read conservatively so a stale socket cannot poison the pool.
pub fn discard(conn: &mut PgConnection) {
    <PgConnection as Connection>::TransactionManager::transaction_manager_status_mut(conn)
        .set_in_error();
}
