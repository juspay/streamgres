use std::sync::Arc;
use std::time::{Duration, Instant};

use super::*;

#[tokio::test]
async fn fallback_preserves_empty_columns_and_rolls_back_failed_reads() {
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        return;
    };
    let pool = OwnPool::new(&dsn, 1).unwrap();
    let (observer, driver) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);

    let mut lease = pool.get().await.unwrap();
    lease
        .execute("SET statement_timeout = '200ms'; SET idle_in_transaction_session_timeout = '9s'")
        .await
        .unwrap();
    let pid = lease
        .query("SELECT pg_catalog.pg_backend_pid()", &[])
        .await
        .unwrap()
        .int(0, 0)
        .unwrap()
        .unwrap();

    let remaining = Duration::from_secs(5);
    let inside = lease
        .fallback_read(
            "SELECT current_setting('transaction_read_only'), current_setting('statement_timeout'),
                    current_setting('idle_in_transaction_session_timeout')",
            &[],
            remaining,
            true,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(inside.text(0, 0).unwrap().as_deref(), Some("on"));
    assert_eq!(inside.text(0, 1).unwrap().as_deref(), Some("200ms"));
    assert_eq!(inside.text(0, 2).unwrap().as_deref(), Some("5s"));

    let result = lease
        .fallback_read(
            "SELECT 1::bigint AS id, 'sample'::text AS label WHERE false",
            &[],
            remaining,
            true,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(result.rows.is_empty());
    assert_eq!(
        result.columns,
        [
            RawColumn {
                name: "id".into(),
                type_oid: Type::INT8.oid(),
            },
            RawColumn {
                name: "label".into(),
                type_oid: Type::TEXT.oid(),
            },
        ]
    );

    let failure = lease
        .fallback_read(
            "SELECT 1 / $1::integer",
            &[Bind {
                type_oid: Type::INT4.oid(),
                bytes: Some(0i32.to_be_bytes().to_vec()),
            }],
            remaining,
            true,
        )
        .await
        .expect("rollback completed after division by zero");
    assert!(
        matches!(&failure, Err(SessionError::Database { code: Some(code), .. }) if code == "22012"),
        "{failure:?}"
    );

    let outcome = lease
        .fallback_read("SELECT pg_catalog.pg_sleep(2)", &[], remaining, true)
        .await
        .expect("rollback completed after the failed read");
    assert!(
        matches!(&outcome, Err(SessionError::Timeout(message)) if message.contains("statement timeout")),
        "{outcome:?}"
    );
    lease.recycle();

    let state = observer
        .query_one(
            "SELECT state, xact_start IS NULL FROM pg_catalog.pg_stat_activity WHERE pid::bigint = $1",
            &[&pid],
        )
        .await
        .unwrap();
    assert_eq!(state.get::<_, String>(0), "idle");
    assert!(state.get::<_, bool>(1));

    let mut lease = pool.get().await.unwrap();
    let settings = lease
        .query(
            "SELECT pg_catalog.pg_backend_pid(), current_setting('statement_timeout'),
                    current_setting('idle_in_transaction_session_timeout')",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(settings.int(0, 0).unwrap(), Some(pid));
    assert_eq!(settings.text(0, 1).unwrap().as_deref(), Some("200ms"));
    assert_eq!(settings.text(0, 2).unwrap().as_deref(), Some("9s"));
}

#[tokio::test]
async fn permissive_fallback_closes_the_checkout() {
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        return;
    };
    let pool = OwnPool::new(&dsn, 1).unwrap();
    let mut lease = pool.get().await.unwrap();
    let result = lease
        .fallback_read(
            "SELECT pg_catalog.pg_backend_pid()",
            &[],
            Duration::from_secs(5),
            false,
        )
        .await
        .unwrap()
        .unwrap();
    drop(lease);
    let mut next = pool.get().await.unwrap();
    let next_pid = next
        .query("SELECT pg_catalog.pg_backend_pid()", &[])
        .await
        .unwrap();
    assert_ne!(result.int(0, 0).unwrap(), next_pid.int(0, 0).unwrap());
}

/// Measures the built-in pool and transaction path with enough callers
/// to queue for a checkout. The serial path is the original baseline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual benchmark requiring STREAMGRES_PG_DSN"]
async fn concurrent_fallback_benchmark() {
    let dsn = std::env::var("STREAMGRES_PG_DSN").expect("set STREAMGRES_PG_DSN");
    const CALLERS: usize = 16;
    const POOL_SIZE: usize = 8;
    const WINDOW: Duration = Duration::from_secs(3);
    const TIMEOUT: Duration = Duration::from_secs(5);

    for (label, pipelined) in [("serial", false), ("pipelined", true)] {
        let pool = Arc::new(OwnPool::new(&dsn, POOL_SIZE).unwrap());
        let mut warm = Vec::with_capacity(POOL_SIZE);
        for _ in 0..POOL_SIZE {
            warm.push(pool.get().await.unwrap());
        }
        for lease in warm {
            lease.recycle();
        }

        let start = Instant::now();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..CALLERS {
            let pool = pool.clone();
            tasks.spawn(async move {
                let mut samples = Vec::new();
                while start.elapsed() < WINDOW {
                    let request = Instant::now();
                    let mut lease = pool.get().await.unwrap();
                    let wait = request.elapsed();
                    const SQL: &str = "SELECT 1::bigint AS value";
                    if pipelined {
                        lease
                            .fallback_read(SQL, &[], TIMEOUT, true)
                            .await
                            .unwrap()
                            .unwrap();
                    } else {
                        fallback_sequential(&mut lease, SQL, &[], TIMEOUT.as_millis())
                            .await
                            .unwrap();
                    }
                    lease.recycle();
                    samples.push((request.elapsed(), wait));
                }
                samples
            });
        }

        let mut latency = Vec::new();
        let mut checkout = Vec::new();
        while let Some(result) = tasks.join_next().await {
            let samples = result.unwrap();
            assert!(!samples.is_empty());
            for (elapsed, wait) in samples {
                latency.push(elapsed);
                checkout.push(wait);
            }
        }
        println!(
            "{label}, {CALLERS} callers / {POOL_SIZE} connections, {} reads/s",
            (latency.len() as f64 / start.elapsed().as_secs_f64()).round()
        );
        print_percentiles("read latency", &mut latency);
        print_percentiles("pool wait", &mut checkout);
    }
}

fn print_percentiles(label: &str, samples: &mut [Duration]) {
    samples.sort_unstable();
    let percentile = |percent: usize| samples[(samples.len() * percent).div_ceil(100usize) - 1];
    println!(
        "{label}: p50={:?}, p95={:?}, p99={:?}",
        percentile(50),
        percentile(95),
        percentile(99)
    );
}
