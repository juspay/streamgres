//! Startup policy with Streamgres's own connections.

use std::time::Duration;
use streamgres::cache::{Error, Streamgres};

/// A database that never answers: connections to a reserved address
/// (TEST-NET-1) hang until their own timeout.
const UNREACHABLE: &str = "postgresql://u:p@192.0.2.1:5432/db?connect_timeout=30";

#[tokio::test]
async fn degraded_start_stops_waiting_at_its_deadline() {
    // `start()` is a `Future` itself, so it can be spawned as well as awaited.
    let starting = tokio::spawn(
        Streamgres::connect(UNREACHABLE)
            .start_timeout(Duration::from_millis(20))
            .allow_degraded_start()
            .start(),
    );
    let started = tokio::time::timeout(Duration::from_secs(2), starting).await;
    let lib = started
        .expect("degraded startup returns within its timeout")
        .expect("the start task")
        .expect("degraded startup allows an unready engine");
    lib.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_trigger_is_a_setup_error_even_with_a_degraded_start() {
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        eprintln!("STREAMGRES_PG_DSN not set: setup scenario skipped");
        return;
    };
    let started = Streamgres::connect(dsn)
        .app("review_missing_trigger", 0)
        .start_timeout(Duration::from_secs(3))
        .allow_degraded_start()
        .start()
        .await;
    if let Ok(lib) = &started {
        lib.shutdown().await;
    }
    assert!(
        matches!(started, Err(Error::Setup(_))),
        "{:?}",
        started.err()
    );
}
