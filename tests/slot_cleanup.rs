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
const TEST_AGE: Duration = Duration::from_millis(1);
const LOCK_KEY: &str = "streamgres.slot_cleanup";

async fn database() -> (String, tokio_postgres::Client) {
    let dsn = std::env::var("STREAMGRES_PG_DSN")
        .expect("set STREAMGRES_PG_DSN to a disposable PostgreSQL server");
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
    // Tests using the minimum positive age should not depend on connection latency.
    tokio::time::sleep(TEST_AGE).await;
    slot
}

#[tokio::test]
#[ignore = "destructive: run explicitly with --ignored --test-threads=1 on disposable PostgreSQL"]
async fn cleanup_uses_supplied_age_and_zero_disables_both_paths() {
    let (dsn, client) = database().await;
    let slot = create_slot(&client).await;
    Transport::cleanup_inactive_slots(&dsn, "", Duration::from_secs(3600), LOCK_KEY)
        .await
        .unwrap();
    assert!(
        exists(&client, &slot).await,
        "fresh slot retained before its age threshold"
    );
    Transport::cleanup_inactive_slots(&dsn, &slot, TEST_AGE, LOCK_KEY)
        .await
        .unwrap();
    assert!(
        exists(&client, &slot).await,
        "own slot excluded even when old enough"
    );
    Transport::cleanup_inactive_slots(&dsn, "", Duration::ZERO, LOCK_KEY)
        .await
        .unwrap();
    Transport::cleanup_slots_at_startup(&dsn, "", Duration::ZERO, Duration::from_secs(1), LOCK_KEY)
        .await
        .unwrap();
    assert!(exists(&client, &slot).await, "zero disables both paths");
    remove_slot(&client, &slot).await;
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
async fn initial_cleanup_frees_capacity_before_own_slot_creation() {
    let (dsn, client) = database().await;
    let capacity: i32 = client
        .query_one("SELECT current_setting('max_replication_slots')::int", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        (1..=256).contains(&capacity),
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
    let mut fixtures = Vec::new();
    for _ in used..i64::from(capacity) {
        fixtures.push(create_slot(&client).await);
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
    tokio::time::sleep(TEST_AGE).await;
    let began = std::time::Instant::now();
    Transport::cleanup_slots_at_startup(&dsn, &own, TEST_AGE, Duration::from_secs(10), LOCK_KEY)
        .await
        .unwrap();
    let elapsed = began.elapsed();
    for slot in &fixtures {
        assert!(!exists(&client, slot).await);
    }
    eprintln!(
        "startup reclaimed {} slots in {elapsed:?} within a 10s budget",
        fixtures.len()
    );
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
            TEST_AGE,
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
        Transport::cleanup_inactive_slots(&dsn, "", TEST_AGE, LOCK_KEY),
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
                wait_for_free_lock(&client, LOCK_KEY).await;
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
        Transport::cleanup_inactive_slots(&dsn, "", TEST_AGE, LOCK_KEY)
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
#[ignore = "destructive: run serially on disposable PostgreSQL 17+"]
async fn idle_cleanup_backend_releases_lock_with_client_still_connected() {
    let (dsn, client) = database().await;
    let (proxy, sweep, pid) = paused_sweep(&dsn, &client).await;
    // Leave both sockets open and withhold replies. Only the server can end its
    // idle session here; the client's startup budget is 30 seconds.
    tokio::time::timeout(Duration::from_secs(22), async {
        while client
            .query_opt("SELECT 1 FROM pg_stat_activity WHERE pid=$1", &[&pid])
            .await
            .unwrap()
            .is_some()
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("PostgreSQL retained the idle cleaner");
    wait_for_free_lock(&client, LOCK_KEY).await;
    assert!(!sweep.is_finished());
    proxy.resume().await;
    assert!(sweep.await.unwrap().is_err());
}

#[tokio::test]
#[ignore = "destructive: run explicitly with --ignored --test-threads=1 on disposable PostgreSQL"]
async fn configured_lock_key_controls_contention_and_startup_budget() {
    let (dsn, client) = database().await;
    let key = "orders:cleanup:掃除'; SELECT 1 --";
    let slot = create_slot(&client).await;
    lock(&client, key).await;
    Transport::cleanup_inactive_slots(&dsn, "", TEST_AGE, key)
        .await
        .unwrap();
    assert!(exists(&client, &slot).await);
    let began = std::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        Transport::cleanup_slots_at_startup(&dsn, "", TEST_AGE, Duration::from_millis(100), key),
    )
    .await
    .expect("startup exceeded its cleanup budget");
    let elapsed = began.elapsed();
    assert!(
        result
            .unwrap_err()
            .0
            .contains("exceeded its 100 ms deadline")
    );
    assert!(
        (Duration::from_millis(100)..Duration::from_millis(750)).contains(&elapsed),
        "100ms cleanup budget took {elapsed:?}"
    );
    assert!(exists(&client, &slot).await);
    // A different configured key does not contend with the external owner.
    Transport::cleanup_inactive_slots(&dsn, "", TEST_AGE, LOCK_KEY)
        .await
        .unwrap();
    assert!(!exists(&client, &slot).await);
    unlock(&client, key).await;
    let slot = create_slot(&client).await;
    Transport::cleanup_slots_at_startup(&dsn, "", TEST_AGE, Duration::from_secs(5), key)
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
                    TEST_AGE,
                    Duration::from_millis(100),
                    LOCK_KEY,
                )
                .await
            } else {
                Transport::cleanup_inactive_slots(&endpoint, "", TEST_AGE, LOCK_KEY).await
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
        Transport::cleanup_inactive_slots(&dsn, "", TEST_AGE, LOCK_KEY),
    )
    .await
    .expect("cleanup remained stuck on a database lock");
    client.batch_execute("ROLLBACK").await.unwrap();
    // Implementations may read metadata without locking this particular view.
    // Either completion or a bounded error is valid; cleanup must work afterward.
    wait_for_free_lock(&client, LOCK_KEY).await;
    Transport::cleanup_inactive_slots(&dsn, "", TEST_AGE, LOCK_KEY)
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
        Transport::cleanup_inactive_slots(&dsn, &keep, TEST_AGE, LOCK_KEY),
        Transport::cleanup_inactive_slots(&dsn, &keep, TEST_AGE, LOCK_KEY),
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
    Transport::cleanup_inactive_slots(&dsn, &keep, TEST_AGE, LOCK_KEY)
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
        "STREAMGRES_SLOT_CLEANUP_AGE_MS" => Some("1".to_owned()),
        "STREAMGRES_SLOT_SWEEP_INTERVAL_MS" => Some("25".to_owned()),
        "STREAMGRES_SNAPSHOT_ROTATION_MS" => Some("60000".to_owned()),
        _ => None,
    })
    .unwrap()
    .engine_settings()
}

async fn start_instance(settings: &threads::Settings) -> (threads::Started, threads::ServiceTask) {
    threads::start(
        settings,
        Arc::new(CatalogHandle::new(Catalog::new(vec![]))),
        tokio::runtime::Handle::current(),
        1,
        Stats::shared(),
        Shutdown::new(),
    )
    .await
    .unwrap()
}

async fn wait_for_feed(client: &Client, slot: &str, previous: Option<i32>) -> i32 {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let pid: Option<i32> = client
                .query_one(
                    "SELECT a.pid FROM pg_replication_slots s
                     LEFT JOIN pg_stat_activity a ON a.pid = s.active_pid AND a.backend_type = 'walsender'
                     WHERE s.slot_name = $1",
                    &[&slot],
                )
                .await
                .unwrap()
                .get(0);
            if let Some(pid) = pid.filter(|pid| Some(*pid) != previous) {
                return pid;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("feed did not acquire its permanent slot")
}

#[tokio::test]
#[ignore = "run on a disposable PostgreSQL 16 or later server"]
async fn feed_opens_and_reconnects_with_cleanup_disabled() {
    let (dsn, client) = database().await;
    tokio::task::LocalSet::new()
        .run_until(async {
            let publication = format!("cleanup_{}", uuid::Uuid::new_v4().simple());
            client
                .batch_execute(&format!("CREATE PUBLICATION {publication}"))
                .await
                .unwrap();
            let version: i32 = client
                .query_one("SELECT current_setting('server_version_num')::int", &[])
                .await
                .unwrap()
                .get(0);
            let orphan = create_slot(&client).await;
            // On PG16 the same feed must also work when a deployment requests age cleanup.
            for enabled in [false, true] {
                if enabled && version >= 170_000 {
                    continue;
                }
                let config = settings(&dsn, &publication, enabled);
                let (started, service) = start_instance(&config).await;
                let pid = wait_for_feed(&client, &config.slot, None).await;
                assert!(
                    Transport::slot_failure(&dsn, &config.slot)
                        .await
                        .unwrap()
                        .is_none()
                );
                client
                    .query_one("SELECT pg_terminate_backend($1)", &[&pid])
                    .await
                    .unwrap();
                wait_for_feed(&client, &config.slot, Some(pid)).await;
                assert!(exists(&client, &orphan).await);
                threads::stop_feed(started.feed).await.unwrap();
                service.abort();
                let _ = service.await;
                remove_slot(&client, &config.slot).await;
            }
            remove_slot(&client, &orphan).await;
            client
                .batch_execute(&format!("DROP PUBLICATION {publication}"))
                .await
                .unwrap();
        })
        .await;
}

// Target just the lifetime owner; the startup guard intentionally uses a separate session.
async fn lifetime_owner_pid(client: &Client, role: &str) -> i32 {
    client
        .query_one(
            "SELECT l.pid FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
         WHERE a.usename=$1 AND l.locktype='advisory' AND l.classid=$2 AND l.granted",
            &[&role, &0x5347_534cu32],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
#[ignore = "destructive: run serially on disposable PostgreSQL 17+"]
async fn peer_sweeps_during_startup_and_reconnect_preserve_owned_slots() {
    let (dsn, client) = database().await;
    tokio::task::LocalSet::new()
        .run_until(async {
            let publication = format!("cleanup_{}", uuid::Uuid::new_v4().simple());
            let role = format!("peer_{}", uuid::Uuid::new_v4().simple());
            client
                .batch_execute(&format!(
                    "CREATE PUBLICATION {publication};
            CREATE ROLE {role} LOGIN REPLICATION PASSWORD 'slot_fixture'"
                ))
                .await
                .unwrap();
            let peer_dsn = if let Ok(mut url) = reqwest::Url::parse(&dsn) {
                url.set_username(&role).unwrap();
                url.set_password(Some("slot_fixture")).unwrap();
                url.to_string()
            } else {
                format!("{dsn} user={role} password=slot_fixture")
            };
            // The read pool connects after slot creation and before the first snapshot.
            let gate = slot_proxy::Proxy::with_gate(&dsn, "paused_peer_read", true).await;
            let mut peer = settings(&peer_dsn, &publication, false);
            peer.read_dsn = gate.dsn.clone();
            let peer_slot = peer.slot.clone();
            let starting = tokio::task::spawn_local(async move { start_instance(&peer).await });
            tokio::time::timeout(Duration::from_secs(10), async {
                while !exists(&client, &peer_slot).await {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("peer did not create its slot");
            // The proxy can acknowledge a pause only after the read-pool connection
            // reaches it. The permanent slot now exists but cannot have a walsender.
            tokio::time::timeout(Duration::from_secs(5), gate.pause())
                .await
                .unwrap();
            let owner_pid = lifetime_owner_pid(&client, &role).await;
            client
                .query_one("SELECT pg_terminate_backend($1, 5000)", &[&owner_pid])
                .await
                .unwrap();
            let orphan = create_slot(&client).await;
            tokio::time::sleep(TEST_AGE).await;
            let cleaner = settings(&dsn, &publication, true);
            let (cleaning, clean_service) = start_instance(&cleaner).await;
            assert!(!exists(&client, &orphan).await, "startup sweep did not run");
            assert!(
                exists(&client, &peer_slot).await,
                "startup sweep dropped the peer"
            );
            gate.resume().await;
            let (running, peer_service) = starting.await.unwrap();
            let pid = wait_for_feed(&client, &peer_slot, None).await;

            // The extra startup session must disappear while the peer is still serving.
            tokio::time::timeout(Duration::from_secs(15), async {
                loop {
                    let guarded: bool = client.query_one(
                        "SELECT EXISTS (SELECT FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
                         WHERE a.usename=$1 AND l.locktype='advisory' AND l.classid=$2 AND l.granted)",
                        &[&role, &0x5347_5354u32],
                    ).await.unwrap().get(0);
                    if !guarded { break; }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }).await.expect("peer retained its startup guard after streaming began");

            // Losing ownership alone must repair the key on a fresh backend
            // without interrupting the active walsender.
            let old_owner = lifetime_owner_pid(&client, &role).await;
            client.query_one("SELECT pg_terminate_backend($1, 5000)", &[&old_owner]).await.unwrap();
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let repaired: bool = client.query_one(
                        "SELECT EXISTS (SELECT FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
                         WHERE a.usename=$1 AND l.locktype='advisory' AND l.classid=$2 AND l.granted AND l.pid <> $3)",
                        &[&role, &0x5347_534cu32, &old_owner],
                    ).await.unwrap().get(0);
                    if repaired { break; }
                    tokio::time::sleep(Duration::from_millis(30)).await;
                }
            }).await.expect("owner was not restored while streaming");
            assert_eq!(wait_for_feed(&client, &peer_slot, None).await, pid);

            assert_eq!(wait_for_feed(&client, &peer_slot, None).await, pid);

            // Give recovery the first turn after loss of BOTH sessions. The next
            // sweep below then tests whether the reopened feed restored ownership.
            lock(&client, LOCK_KEY).await;
            let owner_pid = lifetime_owner_pid(&client, &role).await;
            for backend in [owner_pid, pid] {
                client
                    .query_one("SELECT pg_terminate_backend($1, 5000)", &[&backend])
                    .await
                    .unwrap();
            }
            let pid = wait_for_feed(&client, &peer_slot, Some(pid)).await;
            unlock(&client, LOCK_KEY).await;

            // Refuse new logins for the victim only. Its existing owner session stays
            // alive while its terminated walsender cannot reconnect. A fresh orphan
            // proves the other running instance completed a sweep during that gap.
            client
                .batch_execute(&format!("ALTER ROLE {role} NOLOGIN"))
                .await
                .unwrap();
            client
                .query_one("SELECT pg_terminate_backend($1, 5000)", &[&pid])
                .await
                .unwrap();
            let orphan = create_slot(&client).await;
            tokio::time::timeout(Duration::from_secs(10), async {
                while exists(&client, &orphan).await {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("periodic sweep did not reclaim the witness");
            let active: bool = client
                .query_one(
                    "SELECT active FROM pg_replication_slots WHERE slot_name=$1",
                    &[&peer_slot],
                )
                .await
                .expect("periodic sweep dropped the disconnected peer")
                .get(0);
            assert!(!active);
            client
                .batch_execute(&format!("ALTER ROLE {role} LOGIN"))
                .await
                .unwrap();
            wait_for_feed(&client, &peer_slot, Some(pid)).await;

            threads::stop_feed(running.feed).await.unwrap();
            // stop_feed releases both sessions. No explicit slot drop by the peer.
            tokio::time::timeout(Duration::from_secs(10), async {
                while exists(&client, &peer_slot).await {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("peer retained ownership after stopping");
            threads::stop_feed(cleaning.feed).await.unwrap();
            clean_service.abort();
            peer_service.abort();
            let _ = tokio::join!(clean_service, peer_service);
            drop(running.storage);
            drop(cleaning.storage);
            remove_slot(&client, &peer_slot).await;
            remove_slot(&client, &cleaner.slot).await;
            client
                .batch_execute(&format!("DROP PUBLICATION {publication}; DROP ROLE {role}"))
                .await
                .unwrap();
        })
        .await;
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

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn failed_startup_rolls_back_only_its_new_slot() {
    let (dsn, client) = database().await;
    let publication = format!("cleanup_{}", uuid::Uuid::new_v4().simple());
    client
        .batch_execute(&format!("CREATE PUBLICATION {publication}"))
        .await
        .unwrap();
    tokio::task::LocalSet::new()
        .run_until(async {
            for existing in [false, true] {
                let mut config = settings(&dsn, &publication, false);
                if existing {
                    config.slot = create_slot(&client).await;
                }
                config.read_dsn = "invalid_connection_keyword=value".into();
                let error = threads::start(
                    &config,
                    Arc::new(CatalogHandle::new(Catalog::new(vec![]))),
                    tokio::runtime::Handle::current(),
                    1,
                    Stats::shared(),
                    Shutdown::new(),
                )
                .await
                .err()
                .unwrap();
                assert!(error.contains("connecting the read pool"), "{error}");
                assert_eq!(exists(&client, &config.slot).await, existing);
                if existing {
                    remove_slot(&client, &config.slot).await;
                }
            }
        })
        .await;
    client
        .batch_execute(&format!("DROP PUBLICATION {publication}"))
        .await
        .unwrap();
}

#[tokio::test]
async fn stopping_feed_interrupts_a_stalled_slot_failure_probe() {
    use streamgres::model::Lsn;
    use tokio::io::AsyncReadExt;

    let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dsn = format!(
        "host=127.0.0.1 port={} user=test dbname=test sslmode=disable",
        server.local_addr().unwrap().port()
    );
    let (_events, feed) = threads::spawn_feed(
        settings(&dsn, "unused", false),
        Arc::new(Catalog::new(vec![])),
        Lsn(0),
        Shutdown::new(),
    )
    .unwrap();
    // Reject the owner's connection. Accept the following inspection connection
    // and withhold the startup response so the probe cannot finish on its own.
    let mut probe = tokio::time::timeout(Duration::from_secs(5), async {
        let (mut first, _) = server.accept().await.unwrap();
        let mut packet = [0u8; 4];
        first.read_exact(&mut packet).await.unwrap();
        drop(first);
        let (mut probe, _) = server.accept().await.unwrap();
        probe.read_exact(&mut packet).await.unwrap();
        probe
    })
    .await
    .expect("feed did not reach its failure probe");
    tokio::time::timeout(Duration::from_secs(2), threads::stop_feed(feed))
        .await
        .expect("shutdown waited for the probe timeout")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), probe.read_to_end(&mut Vec::new()))
        .await
        .expect("shutdown left the probe socket open")
        .unwrap();
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn rejected_replication_handshakes_back_off() {
    use streamgres::model::Lsn;
    use tokio::net::{TcpListener, TcpStream};

    let (dsn, client) = database().await;
    let slot = create_slot(&client).await;
    let sql = slot_proxy::Proxy::start(&dsn, "healthy_owner").await;
    let sql_config: tokio_postgres::Config = sql.dsn.parse().unwrap();
    let sql_port = sql_config.get_ports()[0];
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let feed_dsn = sql
        .dsn
        .replacen(&format!("port={sql_port} "), &format!("port={port} "), 1);
    let mut config = settings(&feed_dsn, "unused", false);
    config.slot = slot.clone();
    let (_events, feed) = threads::spawn_feed(
        config,
        Arc::new(Catalog::new(vec![])),
        Lsn(0),
        Shutdown::new(),
    )
    .unwrap();

    // Forward the owner's SQL session to PostgreSQL. Refuse only the
    // subsequent replication connections before they can deliver an event.
    tokio::time::timeout(Duration::from_secs(20), async {
        let (mut owner, _) = listener.accept().await.unwrap();
        let mut upstream = TcpStream::connect(("127.0.0.1", sql_port)).await.unwrap();
        let forward =
            tokio::spawn(
                async move { tokio::io::copy_bidirectional(&mut owner, &mut upstream).await },
            );
        drop(listener.accept().await.unwrap());
        let first = std::time::Instant::now();
        drop(listener.accept().await.unwrap());
        let second = std::time::Instant::now();
        drop(listener.accept().await.unwrap());
        assert!(second.duration_since(first) >= Duration::from_millis(1900));
        assert!(second.elapsed() >= Duration::from_millis(3900));
        threads::stop_feed(feed).await.unwrap();
        // Closing the owner runtime can reset its socket during fixture teardown.
        if let Err(error) = forward.await.unwrap() {
            assert!(
                matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ),
                "{error}"
            );
        }
    })
    .await
    .expect("feed did not retry the failed replication handshakes");
    remove_slot(&client, &slot).await;
}

#[tokio::test]
#[ignore = "destructive: run serially on disposable PostgreSQL 17+"]
async fn public_stream_constructors_keep_managed_slots_owned_after_split() {
    use streamgres::sync::pg::{PgStream, ddl::DdlSource};
    let (dsn, client) = database().await;
    let publication = format!("cleanup_{}", uuid::Uuid::new_v4().simple());
    client
        .batch_execute(&format!("CREATE PUBLICATION {publication}"))
        .await
        .unwrap();
    for with_ddl in [false, true] {
        let slot = threads::slot_name();
        let catalog = Arc::new(Catalog::new(vec![]));
        let mut stream = if with_ddl {
            PgStream::open_with(
                &dsn,
                &slot,
                &publication,
                catalog,
                DdlSource {
                    prefix: "cleanup".into(),
                    schemas: vec!["public".into()],
                },
            )
            .await
            .unwrap()
        } else {
            PgStream::open(&dsn, &slot, &publication, catalog)
                .await
                .unwrap()
        };
        // Poll through an actual event so only lifetime ownership can protect
        // the disconnected slot after split.
        client
            .query_one("SELECT pg_log_standby_snapshot()", &[])
            .await
            .unwrap();
        stream.poll().await.unwrap();
        let (transport, feed) = stream.split();
        drop(feed);
        let pid = wait_for_feed(&client, &slot, None).await;
        client
            .query_one("SELECT pg_terminate_backend($1, 5000)", &[&pid])
            .await
            .unwrap();
        let witness = create_slot(&client).await;
        Transport::cleanup_inactive_slots(&dsn, "", TEST_AGE, LOCK_KEY)
            .await
            .unwrap();
        assert!(!exists(&client, &witness).await);
        assert!(exists(&client, &slot).await);
        drop(transport);
        tokio::time::timeout(Duration::from_secs(5), async {
            while exists(&client, &slot).await {
                Transport::cleanup_inactive_slots(&dsn, "", TEST_AGE, LOCK_KEY)
                    .await
                    .unwrap();
                tokio::time::sleep(TEST_AGE).await;
            }
        })
        .await
        .expect("dropping the transport did not release ownership");
    }
    let slot = threads::slot_name();
    for existing in [false, true] {
        let name = if existing {
            create_slot(&client).await
        } else {
            threads::slot_name()
        };
        let handle = Transport::prepare_owned(&dsn, &name, &publication)
            .await
            .unwrap();
        handle.abort().await.unwrap();
        assert_eq!(exists(&client, &name).await, existing);
        if existing {
            remove_slot(&client, &name).await;
        }
    }
    let prepared = Transport::prepare_owned(&dsn, &slot, &publication)
        .await
        .unwrap();
    tokio::time::sleep(TEST_AGE).await;
    Transport::cleanup_inactive_slots(&dsn, "", TEST_AGE, LOCK_KEY)
        .await
        .unwrap();
    assert!(exists(&client, &slot).await);
    let transport = prepared
        .open(Some(streamgres::model::Lsn(0)))
        .await
        .unwrap();
    wait_for_feed(&client, &slot, None).await;
    drop(transport);
    remove_slot(&client, &slot).await;
    client
        .batch_execute(&format!("DROP PUBLICATION {publication}"))
        .await
        .unwrap();
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "starts the standalone binary on disposable PostgreSQL"]
async fn sigterm_during_snapshot_startup_removes_the_new_slot() {
    let (dsn, client) = database().await;
    let gate = slot_proxy::Proxy::with_gate(&dsn, "stalled_snapshot", true).await;
    let slots = || {
        client.query("SELECT slot_name::text FROM pg_replication_slots WHERE starts_with(slot_name, 'xyne_sync_slot_')", &[])
    };
    let before: Vec<String> = slots().await.unwrap().iter().map(|r| r.get(0)).collect();
    let mut process = tokio::process::Command::new(env!("CARGO_BIN_EXE_server"));
    process
        .kill_on_drop(true)
        .env("STREAMGRES_PG_DSN", &dsn)
        .env("STREAMGRES_MASTER_PG_DSN", &dsn)
        .env("STREAMGRES_READ_PG_DSN", &gate.dsn)
        .env("STREAMGRES_SLOT_CLEANUP_ENABLED", "false")
        .env("STREAMGRES_ADDR", "127.0.0.1:0")
        .env("STREAMGRES_QUERY_URL", "http://localhost/unused")
        .env("STREAMGRES_MUTATE_URL", "http://localhost/unused");
    let mut child = process.spawn().unwrap();
    tokio::time::timeout(Duration::from_secs(30), gate.pause())
        .await
        .expect("startup did not reach the read pool");
    let new: Vec<String> = slots()
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .filter(|s| !before.contains(s))
        .collect();
    assert_eq!(new.len(), 1);
    assert!(
        std::process::Command::new("kill")
            .args(["-TERM", &child.id().unwrap().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("shutdown stalled during snapshot initialization")
        .unwrap();
    assert!(status.success(), "{status}");
    assert!(!exists(&client, &new[0]).await);
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn shutdown_interrupts_a_full_feed_channel() {
    let (dsn, client) = database().await;
    let publication = format!("cleanup_{}", uuid::Uuid::new_v4().simple());
    client
        .batch_execute(&format!("CREATE PUBLICATION {publication}"))
        .await
        .unwrap();
    let slot = threads::slot_name();
    let transport = Transport::prepare_owned(&dsn, &slot, &publication)
        .await
        .unwrap()
        .open(None)
        .await
        .unwrap();
    let (out, receiver) = tokio::sync::mpsc::channel(1);
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let worker = tokio::spawn(async move {
        let mut feed = Feed::new(Arc::new(Catalog::new(vec![])));
        transport
            .stream_with_shutdown(Duration::from_millis(10), &mut feed, &[], out, stopped)
            .await
    });
    tokio::time::timeout(Duration::from_secs(15), async {
        while receiver.is_empty() {
            client
                .query_one(
                    "SELECT pg_logical_emit_message(true, 'test', 'progress')",
                    &[],
                )
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("feed never filled the output channel");
    tokio::time::sleep(Duration::from_millis(100)).await;
    stop.send_replace(true);
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .expect("shutdown blocked on full channel")
        .unwrap()
        .unwrap();
    remove_slot(&client, &slot).await;
    client
        .batch_execute(&format!("DROP PUBLICATION {publication}"))
        .await
        .unwrap();
}
