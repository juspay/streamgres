//! A share of reads through the cache, for a staged rollout.
//!
//! `STREAMGRES_CACHE_SHARE` (0.0 to 1.0, default 1.0), or
//! [`set_cache_share`]: each read a pool would serve from the cache goes
//! there with that probability, and to Postgres otherwise, so a rollout
//! can start at a few percent and grow. A query that names its route
//! (`.via(Route::Streamgres)`) is not sampled. Reads sent to Postgres
//! this way are counted ([`bypassed`]) and reported
//! ([`crate::metrics::Metrics::bypassed`]).

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// The share, as the bits of an `f64`, once read.
static SHARE: OnceLock<AtomicU64> = OnceLock::new();

static BYPASSED: AtomicU64 = AtomicU64::new(0);

/// The share in force: `STREAMGRES_CACHE_SHARE` until set.
fn share() -> &'static AtomicU64 {
    SHARE.get_or_init(|| {
        let share = std::env::var("STREAMGRES_CACHE_SHARE")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .map_or(1.0, clamp);
        AtomicU64::new(share.to_bits())
    })
}

/// A share kept between 0 and 1; not a number counts as all.
fn clamp(share: f64) -> f64 {
    if share.is_nan() { 1.0 } else { share.clamp(0.0, 1.0) }
}

/// Sets the share of reads through the cache for this process.
pub fn set_cache_share(share_of_reads: f64) {
    share().store(clamp(share_of_reads).to_bits(), Ordering::Relaxed);
}

/// The share of reads through the cache in force.
pub fn cache_share() -> f64 {
    f64::from_bits(share().load(Ordering::Relaxed))
}

/// How many reads the share sent to Postgres.
pub fn bypassed() -> u64 {
    BYPASSED.load(Ordering::Relaxed)
}

/// Whether this read goes through the cache; one that does not is
/// counted and reported.
pub(crate) fn through_cache() -> bool {
    let share = cache_share();
    if share >= 1.0 || unit() < share {
        return true;
    }
    BYPASSED.fetch_add(1, Ordering::Relaxed);
    crate::metrics::report(|metrics| {
        metrics.bypassed();
        metrics.served(crate::route::Served::Postgres);
    });
    false
}

/// A number in `[0, 1)`, from a process-wide xorshift: enough to sample
/// reads, not for anything secret.
pub(crate) fn unit() -> f64 {
    static STATE: AtomicU64 = AtomicU64::new(0);
    let mut current = STATE.load(Ordering::Relaxed);
    loop {
        let mut x = if current == 0 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0x9E37_79B9_7F4A_7C15, |time| time.as_nanos() as u64)
                | 1
        } else {
            current
        };
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        match STATE.compare_exchange_weak(current, x, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return (x >> 11) as f64 / (1u64 << 53) as f64,
            Err(seen) => current = seen,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Samples spread over `[0, 1)`.
    #[test]
    fn samples_spread_over_the_unit_interval() {
        let samples: Vec<f64> = (0..10_000).map(|_| unit()).collect();
        assert!(samples.iter().all(|x| (0.0..1.0).contains(x)));
        let low = samples.iter().filter(|x| **x < 0.5).count();
        assert!((4_000..6_000).contains(&low), "{low} of 10000 below a half");
    }

    /// A share sends about its fraction through the cache; out-of-range
    /// shares are clamped.
    #[test]
    fn a_share_sends_its_fraction_through_the_cache() {
        set_cache_share(0.25);
        let through = (0..10_000).filter(|_| through_cache()).count();
        assert!((2_000..3_000).contains(&through), "{through} of 10000 through the cache");
        assert!(bypassed() >= 7_000);
        set_cache_share(7.0);
        assert_eq!(cache_share(), 1.0);
        assert!((0..100).all(|_| through_cache()));
        set_cache_share(-1.0);
        assert_eq!(cache_share(), 0.0);
        set_cache_share(1.0);
    }
}
