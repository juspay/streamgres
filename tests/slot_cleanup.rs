//! Destructive live tests: use only a dedicated disposable PostgreSQL 17+ server.
//! Tests intentionally sweep matching slots; do not point this at a shared instance.

use std::time::Duration;
use streamgres::sync::pg::Transport;
const LOCK_KEY: &str = "streamgres.slot_cleanup";

async fn database() -> Option<(String, tokio_postgres::Client)> {
    let dsn = std::env::var("STREAMGRES_SLOT_TEST_DSN").ok()?;
    let (client, driver) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { driver.await.unwrap() });
    Some((dsn, client))
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
async fn cleanup_uses_supplied_age_and_zero_means_immediate() {
    let Some((dsn, client)) = database().await else {
        return;
    };
    let slot = create_slot(&client).await;
    Transport::cleanup_inactive_slots(&dsn, "", Duration::from_secs(3600), LOCK_KEY)
        .await
        .unwrap();
    assert!(
        Transport::slot_exists(&dsn, &slot).await.unwrap(),
        "fresh slot retained before its age threshold"
    );
    Transport::cleanup_inactive_slots(&dsn, &slot, Duration::ZERO, LOCK_KEY)
        .await
        .unwrap();
    assert!(
        Transport::slot_exists(&dsn, &slot).await.unwrap(),
        "own slot excluded even at zero age"
    );
    Transport::cleanup_inactive_slots(&dsn, "", Duration::ZERO, LOCK_KEY)
        .await
        .unwrap();
    assert!(
        !Transport::slot_exists(&dsn, &slot).await.unwrap(),
        "zero immediately deletes an inactive candidate"
    );
    Transport::cleanup_inactive_slots(&dsn, "", Duration::ZERO, LOCK_KEY)
        .await
        .unwrap();
}

#[tokio::test]
async fn cleanup_observes_subsecond_age_and_reports_missing_own_slot() {
    let Some((dsn, client)) = database().await else {
        return;
    };
    let slot = create_slot(&client).await;
    assert!(
        Transport::slot_failure(&dsn, &slot)
            .await
            .unwrap()
            .is_none()
    );
    Transport::cleanup_inactive_slots(&dsn, "", Duration::from_millis(250), LOCK_KEY)
        .await
        .unwrap();
    assert!(Transport::slot_exists(&dsn, &slot).await.unwrap());
    tokio::time::sleep(Duration::from_millis(300)).await;
    Transport::cleanup_inactive_slots(&dsn, "", Duration::from_millis(250), LOCK_KEY)
        .await
        .unwrap();
    assert!(!Transport::slot_exists(&dsn, &slot).await.unwrap());
    assert!(
        Transport::slot_failure(&dsn, &slot)
            .await
            .unwrap()
            .unwrap()
            .contains("disappeared")
    );
}

#[tokio::test]
async fn impossible_age_fails_without_deleting_candidates() {
    let Some((dsn, client)) = database().await else {
        return;
    };
    let slot = create_slot(&client).await;
    assert!(
        Transport::cleanup_inactive_slots(&dsn, "", Duration::MAX, LOCK_KEY)
            .await
            .is_err()
    );
    assert!(Transport::slot_exists(&dsn, &slot).await.unwrap());
    client
        .query_one("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await
        .unwrap();
}

#[tokio::test]
async fn initial_cleanup_frees_capacity_before_own_slot_creation() {
    let Some((dsn, client)) = database().await else {
        return;
    };
    let capacity: i32 = client
        .query_one("SELECT current_setting('max_replication_slots')::int", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        (1..=64).contains(&capacity),
        "use the disposable test runner's small cluster"
    );
    for _ in 0..capacity {
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
    assert!(Transport::slot_exists(&dsn, &own).await.unwrap());
    client
        .query_one("SELECT pg_drop_replication_slot($1)", &[&own])
        .await
        .unwrap();
    client
        .batch_execute(&format!("DROP PUBLICATION {publication}"))
        .await
        .unwrap();
}
