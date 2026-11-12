//! A TCP peer that accepts a connection but never speaks Postgres must
//! not prevent cancelling startup or enforcing its deadline.
use std::net::TcpListener;
use std::time::{Duration, Instant};
use streamgres::cache::{Cache, CacheConfig};

#[test]
fn stop_cancels_a_start_waiting_for_the_database() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config = CacheConfig::new(
        format!("postgresql://postgres@{address}/postgres"),
        "test_pub",
    );
    let cache = Cache::start(config);
    let (_peer, _) = listener.accept().unwrap();
    let before = Instant::now();
    drop(cache);
    assert!(before.elapsed() < Duration::from_secs(2));
}

#[test]
fn startup_is_retried_after_its_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let mut config = CacheConfig::new(
        format!("postgresql://postgres@{address}/postgres"),
        "test_pub",
    );
    config.start_timeout = Duration::from_millis(50);
    let cache = Cache::start(config);
    let (_peer, _) = listener.accept().unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while cache.metrics().restarts == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(cache.metrics().restarts > 0);
    cache.stop();
}
