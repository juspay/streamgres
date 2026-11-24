//! Startup policy with application-owned checkouts.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use streamgres::cache::{Error, Lease, Streamgres};

#[tokio::test]
async fn degraded_start_stops_waiting_at_its_deadline() {
    let never = || std::future::pending::<Result<Lease, String>>();
    let started = tokio::time::timeout(
        Duration::from_secs(2),
        Streamgres::connect("host=localhost user=postgres")
            .connections(never)
            .start_timeout(Duration::from_millis(20))
            .allow_degraded_start()
            .start(),
    )
    .await;
    let lib = started
        .expect("degraded startup returns within its timeout")
        .expect("degraded startup allows an unready engine");
    lib.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_trigger_after_transient_checkout_failure_is_setup_error() {
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        eprintln!("STREAMGRES_PG_DSN not set: setup scenario skipped");
        return;
    };
    let attempts = Arc::new(AtomicUsize::new(0));
    let builder = Streamgres::connect(dsn.clone())
        .app("review_missing_trigger", 0)
        .start_timeout(Duration::from_secs(3))
        .allow_degraded_start()
        .connections(move || {
            let first = attempts.fetch_add(1, Ordering::Relaxed) == 0;
            let dsn = dsn.clone();
            async move {
                if first {
                    return Err("temporary checkout failure".to_owned());
                }
                let (client, driver) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                    .await
                    .map_err(|e| e.to_string())?;
                tokio::spawn(driver);
                Ok(Lease::new(client))
            }
        });
    let started = builder.start().await;
    if let Ok(lib) = &started {
        lib.shutdown().await;
    }
    assert!(
        matches!(started, Err(Error::Setup(_))),
        "{:?}",
        started.err()
    );
}
