//! Destructive live tests: use only a dedicated disposable PostgreSQL 17+ server.
//! Tests intentionally sweep matching slots; do not point this at a shared instance.

#[path = "support/slot_proxy.rs"]
mod slot_proxy;

use std::sync::Arc;
use streamgres::client::Config;
use streamgres::model::Catalog;
use streamgres::shutdown::Shutdown;
use streamgres::stats::Stats;
use streamgres::sync::CatalogHandle;
use streamgres::sync::pg::{Feed, threads};
use tokio_postgres::Client;

use std::time::Duration;
use streamgres::sync::pg::Transport;
const LOCK_KEY: &str = "streamgres.slot_cleanup";

async fn database() -> (String, tokio_postgres::Client) {
    let dsn = std::env::var("STREAMGRES_PG_DSN")
        .expect("set STREAMGRES_PG_DSN to a disposable PostgreSQL 17+ server");
    let (client, driver) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    (dsn, client)
}

// Read PostgreSQL directly for the expected state rather than using another
// production helper as the test oracle.
async fn exists(client: &Client, slot: &str) -> bool {
    client
        .query_opt(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap()
        .is_some()
}

// Wait for a stopped fixture feed to release its slot, without terminating the
// backend that owns this test's temporary slot or calling production shutdown code.
async fn remove_slot(client: &Client, slot: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match client
                .query("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", &[&slot])
                .await
            {
                Ok(_) => return,
                Err(error) if error.code() == Some(&tokio_postgres::error::SqlState::OBJECT_IN_USE) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("fixture cleanup failed: {error}"),
            }
        }
    }).await.expect("fixture feed did not release its slot");
}

async fn create_slot(client: &tokio_postgres::Client) -> String {
    let slot = format!("xyne_sync_slot_{}", uuid::Uuid::new_v4().simple());
    client
        .query_one(
            "SELECT * FROM pg_create_logical_replication_slot($1, 'test_decoding')",
            &[&slot],
        )
        .await
        .unwrap();
    slot
}

#[tokio::test]
#[ignore = "destructive: run explicitly with --ignored --test-threads=1 on disposable PostgreSQL"]
async fn cleanup_uses_supplied_age_and_zero_means_immediate() {
    let (dsn, client) = database().await;
    let slot = create_slot(&client).await;
    Transport::cleanup_inactive_slots(&dsn, "", Duration::from_secs(3600), LOCK_KEY)
        .await
        .unwrap();
    assert!(
        exists(&client, &slot).await,
        "fresh slot retained before its age threshold"
    );
    Transport::cleanup_inactive_slots(&dsn, &slot, Duration::ZERO, LOCK_KEY)
        .await
        .unwrap();
    assert!(
        exists(&client, &slot).await,
        "own slot excluded even at zero age"
    );
    Transport::cleanup_inactive_slots(&dsn, "", Duration::ZERO, LOCK_KEY)
        .await
        .unwrap();
    assert!(
        !exists(&client, &slot).await,
        "zero immediately deletes an inactive candidate"
    );
    Transport::cleanup_inactive_slots(&dsn, "", Duration::ZERO, LOCK_KEY)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "destructive: run explicitly with --ignored --test-threads=1 on disposable PostgreSQL"]
async fn cleanup_observes_subsecond_age_and_reports_missing_own_slot() {
    let (dsn, client) = database().await;
    let slot = create_slot(&client).await;
    assert!(
        Transport::slot_failure(&dsn, &slot)
            .await
            .unwrap()
            .is_none()
    );
    // Fresh-slot retention is tested with a long threshold above. Do not require
    // several network round trips to finish within 250ms on a loaded CI worker.
    tokio::time::sleep(Duration::from_millis(300)).await;
    Transport::cleanup_inactive_slots(&dsn, "", Duration::from_millis(250), LOCK_KEY)
        .await
        .unwrap();
    assert!(!exists(&client, &slot).await);
    assert!(
        Transport::slot_failure(&dsn, &slot)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
#[ignore = "destructive: run explicitly with --ignored --test-threads=1 on disposable PostgreSQL"]
async fn impossible_age_fails_without_deleting_candidates() {
    let (dsn, client) = database().await;
    let slot = create_slot(&client).await;
    assert!(
        Transport::cleanup_inactive_slots(&dsn, "", Duration::MAX, LOCK_KEY)
            .await
            .is_err()
    );
    assert!(exists(&client, &slot).await);
    client
        .query_one("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "destructive: run explicitly with --ignored --test-threads=1 on disposable PostgreSQL"]
async fn initial_cleanup_frees_capacity_before_own_slot_creation() {
    let (dsn, client) = database().await;
    let capacity: i32 = client
        .query_one("SELECT current_setting('max_replication_slots')::int", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        (1..=64).contains(&capacity),
        "use the disposable test runner's small cluster"
    );
    // Other CI tests may leave unrelated slots. Fill only the remaining capacity;
    // the cleaner must free the slots created here without assuming an empty server.
    let used: i64 = client
        .query_one("SELECT count(*) FROM pg_replication_slots", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        used < i64::from(capacity),
        "capacity test needs at least one free slot"
    );
    for _ in used..i64::from(capacity) {
        create_slot(&client).await;
    }
    let publication = format!("cleanup_{}", uuid::Uuid::new_v4().simple());
    let own = format!("xyne_sync_slot_{}", uuid::Uuid::new_v4().simple());
    client
        .batch_execute(&format!("CREATE PUBLICATION {publication}"))
        .await
        .unwrap();
    assert!(
        Transport::prepare(&dsn, &own, &publication).await.is_err(),
        "full capacity should prevent slot creation before cleanup"
    );
    Transport::cleanup_slots_at_startup(
        &dsn,
        &own,
        Duration::ZERO,
        Duration::from_secs(10),
        LOCK_KEY,
    )
    .await
    .unwrap();
    Transport::prepare(&dsn, &own, &publication).await.unwrap();
    assert!(exists(&client, &own).await);
    client
        .query_one("SELECT pg_drop_replication_slot($1)", &[&own])
        .await
        .unwrap();
    client
        .batch_execute(&format!("DROP PUBLICATION {publication}"))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "destructive: run explicitly with --ignored --test-threads=1 on disposable PostgreSQL"]
async fn large_inactivity_age_retains_slots_without_timestamp_overflow() {
    let (dsn, client) = database().await;
    let slot = create_slot(&client).await;
    let age = Duration::from_secs(10_000 * 365 * 24 * 60 * 60);
    Transport::cleanup_inactive_slots(&dsn, "", age, LOCK_KEY)
        .await
        .unwrap();
    assert!(exists(&client, &slot).await);
    client
        .query_one("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await
        .unwrap();
}

// PostgreSQL's advisory lock is part of the cleanup coordination contract.
// Test fixtures hold that lock externally; no production helper is called.
async fn lock(client: &Client, key: &str) {
    tokio::time::timeout(
        Duration::from_secs(5),
        client.query_one("SELECT pg_advisory_lock(hashtextextended($1, 0))", &[&key]),
    )
    .await
    .unwrap()
    .unwrap();
}

async fn unlock(client: &Client, key: &str) {
    assert!(
        client
            .query_one(
                "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                &[&key],
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
}

async fn wait_for_free_lock(client: &Client, key: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let free: bool = client
                .query_one(
                    "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                    &[&key],
                )
                .await
                .unwrap()
                .get(0);
            if free {
                unlock(client, key).await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cleanup did not release its lock");
}

async fn wait_for_lock(client: &Client, application: &str, granted: bool) -> i32 {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(row) = client
                .query_opt(
                    "SELECT l.pid FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
                 WHERE l.locktype = 'advisory' AND l.granted = $2
                   AND a.application_name = $1",
                    &[&application, &granted],
                )
                .await
                .unwrap()
            {
                return row.get(0);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cleanup did not reach the expected lock state")
}

type Sweep = tokio::task::JoinHandle<Result<(), streamgres::sync::StorageError>>;

// Pause a complete public startup sweep after PostgreSQL grants ownership, by
// holding its network replies. Synchronization observes pg_locks, not SQL text,
// helper boundaries, candidate order, or a particular query count.
async fn paused_sweep(dsn: &str, client: &Client) -> (slot_proxy::Proxy, Sweep, i32) {
    lock(client, LOCK_KEY).await;
    let application = format!("cleanup_{}", uuid::Uuid::new_v4().simple());
    let proxy = slot_proxy::Proxy::start(dsn, &application).await;
    let endpoint = proxy.dsn.clone();
    let sweep = tokio::spawn(async move {
        Transport::cleanup_slots_at_startup(
            &endpoint,
            "",
            Duration::ZERO,
            Duration::from_secs(30),
            LOCK_KEY,
        )
        .await
    });
    wait_for_lock(client, &application, false).await;
    tokio::time::timeout(Duration::from_secs(5), proxy.pause())
        .await
        .unwrap();
    unlock(client, LOCK_KEY).await;
    let pid = wait_for_lock(client, &application, true).await;
    (proxy, sweep, pid)
}

#[tokio::test]
#[ignore = "destructive: run explicitly with --ignored --test-threads=1 on disposable PostgreSQL"]
async fn competing_public_sweeps_skip_while_a_cleaner_owns_the_lock() {
    let (dsn, client) = database().await;
    let slot = create_slot(&client).await;
    let (proxy, owner, _) = paused_sweep(&dsn, &client).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        Transport::cleanup_inactive_slots(&dsn, "", Duration::ZERO, LOCK_KEY),
    )
    .await
    .expect("contender should skip the busy lock")
    .unwrap();
    assert!(
        exists(&client, &slot).await,
        "contender deleted while another cleaner owned the lock"
    );
    proxy.resume().await;
    tokio::time::timeout(Duration::from_secs(5), owner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!exists(&client, &slot).await);
    wait_for_free_lock(&client, LOCK_KEY).await;
}

#[tokio::test]
#[ignore = "destructive: run explicitly with --ignored --test-threads=1 on disposable PostgreSQL"]
async fn cleanup_releases_ownership_after_cancellation_or_connection_loss() {
    let (dsn, client) = database().await;
    for failure in ["cancel", "terminate", "disconnect"] {
        let slot = create_slot(&client).await;
        let (proxy, sweep, pid) = paused_sweep(&dsn, &client).await;
        match failure {
            "cancel" => {
                sweep.abort();
                assert!(sweep.await.unwrap_err().is_cancelled());
                drop(proxy);
            }
            "terminate" => {
                client
                    .query_one("SELECT pg_terminate_backend($1)", &[&pid])
                    .await
                    .unwrap();
                proxy.resume().await;
                let _ = tokio::time::timeout(Duration::from_secs(5), sweep)
                    .await
                    .unwrap()
                    .unwrap();
            }
            "disconnect" => {
                drop(proxy);
                let _ = tokio::time::timeout(Duration::from_secs(5), sweep)
                    .await
                    .unwrap()
                    .unwrap();
            }
            _ => unreachable!(),
        }
        wait_for_free_lock(&client, LOCK_KEY).await;
        assert!(
            exists(&client, &slot).await,
            "paused cleanup unexpectedly deleted a slot"
        );
        Transport::cleanup_inactive_slots(&dsn, "", Duration::ZERO, LOCK_KEY)
            .await
            .unwrap();
        assert!(
            !exists(&client, &slot).await,
            "a later sweep must recover after {failure}"
        );
        wait_for_free_lock(&client, LOCK_KEY).await;
    }
}

#[tokio::test]
#[ignore = "destructive: run explicitly with --ignored --test-threads=1 on disposable PostgreSQL"]
async fn configured_lock_key_controls_contention_and_startup_budget() {
    let (dsn, client) = database().await;
    let key = "orders:cleanup:掃除'; SELECT 1 --";
    let slot = create_slot(&client).await;
    lock(&client, key).await;
    Transport::cleanup_inactive_slots(&dsn, "", Duration::ZERO, key)
        .await
        .unwrap();
    assert!(exists(&client, &slot).await);
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        Transport::cleanup_slots_at_startup(
            &dsn,
            "",
            Duration::ZERO,
            Duration::from_millis(100),
            key,
        ),
    )
    .await
    .expect("startup exceeded its cleanup budget");
    assert!(result.is_err());
    assert!(exists(&client, &slot).await);
    // A different configured key does not contend with the external owner.
    Transport::cleanup_inactive_slots(&dsn, "", Duration::ZERO, LOCK_KEY)
        .await
        .unwrap();
    assert!(!exists(&client, &slot).await);
    unlock(&client, key).await;
    let slot = create_slot(&client).await;
    Transport::cleanup_slots_at_startup(&dsn, "", Duration::ZERO, Duration::from_secs(5), key)
        .await
        .unwrap();
    assert!(!exists(&client, &slot).await);
    wait_for_free_lock(&client, key).await;
}

#[tokio::test]
#[ignore = "destructive: run explicitly with --ignored --test-threads=1 on disposable PostgreSQL"]
async fn public_cleanup_bounds_connections_to_a_silent_server() {
    use tokio::io::AsyncReadExt;
    for startup in [true, false] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            socket.read_to_end(&mut bytes).await.unwrap();
        });
        let endpoint = format!("host=127.0.0.1 port={port} user=test dbname=test");
        let result = tokio::time::timeout(Duration::from_secs(8), async {
            if startup {
                Transport::cleanup_slots_at_startup(
                    &endpoint,
                    "",
                    Duration::ZERO,
                    Duration::from_millis(100),
                    LOCK_KEY,
                )
                .await
            } else {
                Transport::cleanup_inactive_slots(&endpoint, "", Duration::ZERO, LOCK_KEY).await
            }
        })
        .await
        .expect("cleanup remained stuck on a silent server");
        assert!(result.is_err());
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
#[ignore = "destructive: run explicitly with --ignored --test-threads=1 on disposable PostgreSQL"]
async fn cleanup_is_bounded_under_metadata_contention_and_allows_retry() {
    let (dsn, client) = database().await;
    let slot = create_slot(&client).await;
    client
        .batch_execute("BEGIN; LOCK TABLE pg_catalog.pg_replication_slots IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let _outcome = tokio::time::timeout(
        Duration::from_secs(8),
        Transport::cleanup_inactive_slots(&dsn, "", Duration::ZERO, LOCK_KEY),
    )
    .await
    .expect("cleanup remained stuck on a database lock");
    client.batch_execute("ROLLBACK").await.unwrap();
    // Implementations may read metadata without locking this particular view.
    // Either completion or a bounded error is valid; cleanup must work afterward.
    wait_for_free_lock(&client, LOCK_KEY).await;
    Transport::cleanup_inactive_slots(&dsn, "", Duration::ZERO, LOCK_KEY)
        .await
        .unwrap();
    assert!(!exists(&client, &slot).await);
}

#[tokio::test]
#[ignore = "destructive: run explicitly with --ignored --test-threads=1 on disposable PostgreSQL"]
async fn public_sweeps_retain_active_temporary_physical_and_other_database_slots() {
    let (dsn, client) = database().await;
    let keep = create_slot(&client).await;
    let orphan = create_slot(&client).await;
    let name = || format!("xyne_sync_slot_{}", uuid::Uuid::new_v4().simple());
    let active = name();
    let physical = name();
    let temporary = name();
    let unrelated = format!("unrelated_{}", uuid::Uuid::new_v4().simple());
    client
        .query_one(
            "SELECT * FROM pg_create_logical_replication_slot($1, 'test_decoding')",
            &[&unrelated],
        )
        .await
        .unwrap();
    client
        .query_one(
            "SELECT * FROM pg_create_physical_replication_slot($1)",
            &[&physical],
        )
        .await
        .unwrap();
    client
        .query_one(
            "SELECT * FROM pg_create_logical_replication_slot($1, 'test_decoding', true)",
            &[&temporary],
        )
        .await
        .unwrap();
    let publication = format!("cleanup_{}", uuid::Uuid::new_v4().simple());
    client
        .batch_execute(&format!("CREATE PUBLICATION {publication}"))
        .await
        .unwrap();
    Transport::prepare(&dsn, &active, &publication)
        .await
        .unwrap();
    let transport = Transport::open(&dsn, &active, &publication).await.unwrap();
    let (out, _events) = tokio::sync::mpsc::channel(32);
    let reader = tokio::spawn(async move {
        let mut feed = Feed::new(Arc::new(Catalog::new(vec![])));
        transport
            .stream(Duration::from_millis(200), &mut feed, &[], out)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !client
            .query_one(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
                &[&active],
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let other_database = format!("cleanup_{}", uuid::Uuid::new_v4().simple());
    client
        .batch_execute(&format!("CREATE DATABASE {other_database}"))
        .await
        .unwrap();
    let mut config: tokio_postgres::Config = dsn.parse().unwrap();
    config.dbname(&other_database);
    let (other, driver) = config.connect(tokio_postgres::NoTls).await.unwrap();
    let driver = tokio::spawn(driver);
    let other_slot = create_slot(&other).await;
    let (a, b) = tokio::join!(
        Transport::cleanup_inactive_slots(&dsn, &keep, Duration::ZERO, LOCK_KEY),
        Transport::cleanup_inactive_slots(&dsn, &keep, Duration::ZERO, LOCK_KEY),
    );
    a.unwrap();
    b.unwrap();
    assert!(!exists(&client, &orphan).await);
    for retained in [
        &keep,
        &active,
        &physical,
        &temporary,
        &unrelated,
        &other_slot,
    ] {
        assert!(
            exists(&client, retained).await,
            "unexpectedly deleted {retained}"
        );
    }
    // Repeating the public operation after deletion is harmless.
    Transport::cleanup_inactive_slots(&dsn, &keep, Duration::ZERO, LOCK_KEY)
        .await
        .unwrap();
    other
        .query_one("SELECT pg_drop_replication_slot($1)", &[&other_slot])
        .await
        .unwrap();
    drop(other);
    driver.await.unwrap().unwrap();
    client
        .batch_execute(&format!("DROP DATABASE {other_database}"))
        .await
        .unwrap();
    reader.abort();
    let _ = reader.await;
    for slot in [&keep, &active, &physical, &temporary, &unrelated] {
        remove_slot(&client, slot).await;
    }
    client
        .batch_execute(&format!("DROP PUBLICATION {publication}"))
        .await
        .unwrap();
}

fn settings(dsn: &str, publication: &str, enabled: bool) -> threads::Settings {
    Config::from_lookup(|name| match name {
        "STREAMGRES_PG_DSN" | "STREAMGRES_MASTER_PG_DSN" => Some(dsn.to_owned()),
        "STREAMGRES_QUERY_URL" | "STREAMGRES_MUTATE_URL" => {
            Some("http://localhost/unused".to_owned())
        }
        "STREAMGRES_PUBLICATION" => Some(publication.to_owned()),
        "STREAMGRES_SLOT_CLEANUP_ENABLED" => Some(enabled.to_string()),
        "STREAMGRES_SLOT_CLEANUP_AGE_MS" => Some("0".to_owned()),
        "STREAMGRES_SLOT_SWEEP_INTERVAL_MS" => Some("25".to_owned()),
        "STREAMGRES_SNAPSHOT_ROTATION_MS" => Some("60000".to_owned()),
        _ => None,
    })
    .unwrap()
    .engine_settings()
}

#[tokio::test]
#[ignore = "destructive: run explicitly with --ignored --test-threads=1 on disposable PostgreSQL"]
async fn instance_start_and_stop_control_periodic_cleanup() {
    let (dsn, client) = database().await;
    tokio::task::LocalSet::new()
        .run_until(async {
            for enabled in [true, false] {
                let publication = format!("cleanup_{}", uuid::Uuid::new_v4().simple());
                client
                    .batch_execute(&format!("CREATE PUBLICATION {publication}"))
                    .await
                    .unwrap();
                let config = settings(&dsn, &publication, enabled);
                let (started, service) = threads::start(
                    &config,
                    Arc::new(CatalogHandle::new(Catalog::new(vec![]))),
                    tokio::runtime::Handle::current(),
                    1,
                    Stats::shared(),
                    Shutdown::new(),
                )
                .await
                .unwrap();
                let orphan = create_slot(&client).await;
                if enabled {
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while exists(&client, &orphan).await {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    })
                    .await
                    .expect("started instance did not run periodic cleanup");
                } else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    assert!(exists(&client, &orphan).await);
                }
                assert!(exists(&client, &config.slot).await);
                threads::stop_feed(started.feed).await.unwrap();
                service.abort();
                let _ = service.await;
                let retained = create_slot(&client).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
                assert!(
                    exists(&client, &retained).await,
                    "stopped instance left a cleaner running"
                );
                for slot in [&orphan, &retained, &config.slot] {
                    remove_slot(&client, slot).await;
                }
                drop(started.storage);
                client
                    .batch_execute(&format!("DROP PUBLICATION {publication}"))
                    .await
                    .unwrap();
            }
        })
        .await;
}

#[tokio::test]
#[ignore = "destructive: run explicitly with --ignored --test-threads=1 on disposable PostgreSQL"]
async fn failed_instance_start_does_not_leave_periodic_cleanup_running() {
    let (dsn, client) = database().await;
    tokio::task::LocalSet::new()
        .run_until(async {
            let missing_publication = format!("cleanup_{}", uuid::Uuid::new_v4().simple());
            let config = settings(&dsn, &missing_publication, true);
            let result = threads::start(
                &config,
                Arc::new(CatalogHandle::new(Catalog::new(vec![]))),
                tokio::runtime::Handle::current(),
                1,
                Stats::shared(),
                Shutdown::new(),
            )
            .await;
            assert!(result.is_err());
            let orphan = create_slot(&client).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                exists(&client, &orphan).await,
                "failed startup left a cleaner running"
            );
            remove_slot(&client, &orphan).await;
        })
        .await;
}
