//! One primary-side standby snapshot writer per database, elected with a
//! session advisory lock. The lock and every call use the same direct session.

use std::time::Duration;

use tokio_postgres::{Config, NoTls};

use crate::log::{log_info, log_warn};
use crate::shutdown::Shutdown;
use crate::sync::pg::Keepalive;

// A stable pair shared by every Streamgres pod connected to this database.
const LOCK_CLASS: i32 = 0x5354_4752; // STGR
const LOCK_RESOURCE: i32 = 0x534e_4150; // SNAP
const RETRY_DELAY: Duration = Duration::from_secs(5);
const CALL_TIMEOUT: Duration = Duration::from_secs(15);

pub fn start(
    dsn: &str,
    interval: Duration,
    keepalive: Keepalive,
    shutdown: Shutdown,
    runtime: &tokio::runtime::Runtime,
) -> Result<(), String> {
    let mut config: Config = dsn
        .parse()
        .map_err(|error| format!("STREAMGRES_MASTER_PG_DSN: {error}"))?;
    config.application_name("streamgres-standby-snapshot");
    config.connect_timeout(Duration::from_secs(10));
    keepalive.apply(&mut config);
    runtime.spawn(run(config, interval, shutdown));
    Ok(())
}

async fn run(config: Config, interval: Duration, shutdown: Shutdown) {
    loop {
        let connected = tokio::select! {
            _ = shutdown.wait() => return,
            result = config.connect(NoTls) => result,
        };
        match connected {
            Ok((client, connection)) => {
                // Driving the connection is essential: it also detects a broken
                // session while a follower waits for the advisory lock.
                let driver = tokio::spawn(connection);
                let outcome = lead(&client, interval, &shutdown).await;
                drop(client);
                driver.abort(); // Close the session and release its lock.
                if shutdown.requested() {
                    return;
                }
                if let Err(error) = outcome {
                    log_warn!("primary standby snapshot worker: {error}");
                }
            }
            Err(error) => log_warn!("primary standby snapshot connection: {error}"),
        }
        tokio::select! {
            _ = shutdown.wait() => return,
            _ = tokio::time::sleep(RETRY_DELAY) => {},
        }
    }
}

async fn lead(
    client: &tokio_postgres::Client,
    interval: Duration,
    shutdown: &Shutdown,
) -> Result<(), String> {
    tokio::select! {
        _ = shutdown.wait() => return Ok(()),
        result = client.query(
            "SELECT pg_catalog.pg_advisory_lock($1::integer, $2::integer)",
            &[&LOCK_CLASS, &LOCK_RESOURCE],
        ) => result.map_err(|error| format!("acquiring leader lock: {error}"))?,
    };
    log_info!("primary standby snapshot leader acquired the database lock");

    let mut ticks = tokio::time::interval(interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.wait() => return Ok(()),
            _ = ticks.tick() => {},
        }
        let called = tokio::select! {
            _ = shutdown.wait() => return Ok(()),
            result = tokio::time::timeout(
                CALL_TIMEOUT,
                client.simple_query("SELECT pg_catalog.pg_log_standby_snapshot()"),
            ) => result,
        };
        match called {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => return Err(format!("logging standby snapshot: {error}")),
            Err(_) => {
                return Err(format!(
                    "logging standby snapshot exceeded {CALL_TIMEOUT:?}"
                ));
            }
        }
    }
}
