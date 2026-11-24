//! What the cache keeps and for how long: everything about it that does
//! not depend on which database the engine streams. The database's own
//! settings (where it is, its publication, its schemas) belong to the
//! engine (`crate::sync::embed`).

use std::time::Duration;

/// How long a query is kept after its last read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Ttl {
    /// Freed at the first sweep after its last read.
    Immediate,
    /// Kept this long after its last read.
    After(Duration),
    /// Kept until memory runs short or Streamgres stops.
    Forever,
}

impl Ttl {
    /// The ttl in milliseconds, `u64::MAX` for [`Ttl::Forever`].
    pub(crate) fn millis(self) -> u64 {
        match self {
            Ttl::Immediate => 0,
            Ttl::After(duration) => u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
            Ttl::Forever => u64::MAX,
        }
    }
}

impl std::fmt::Display for Ttl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Ttl::Immediate => f.write_str("immediate"),
            Ttl::After(duration) => write!(f, "{duration:?}"),
            Ttl::Forever => f.write_str("forever"),
        }
    }
}

impl From<Duration> for Ttl {
    fn from(duration: Duration) -> Self {
        if duration.is_zero() {
            Ttl::Immediate
        } else {
            Ttl::After(duration)
        }
    }
}

/// The cache's limits and timings. Only the budgets, the default ttl, the
/// lag bound and the start timeout are set from the builder; the rest are
/// fixed.
#[derive(Debug, Clone)]
pub(crate) struct CacheConfig {
    /// At most this many queries are kept: a bound on the cache's own
    /// bookkeeping, far above what the byte budget usually allows.
    pub max_entries: usize,
    /// What every kept query's rows weigh together, at most.
    pub max_bytes: usize,
    /// What one query's rows weigh, at most; a heavier query is refused.
    pub max_entry_bytes: usize,
    /// How many rows one query may hold, at most; a larger one is refused.
    pub max_rows_per_query: usize,
    /// The ttl of a read that names none.
    pub default_ttl: Ttl,
    /// A query still loading after twice this is dropped as stuck, and
    /// registered again by its next read.
    pub fill_timeout: Duration,
    /// How far behind the database the engine may be and still answer.
    pub max_lag: Duration,
    /// How long a query the engine refused is remembered as refused.
    pub refusal_ttl: Duration,
    /// How long one engine may take to start before it is started again.
    pub start_timeout: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig {
            max_entries: 10_000,
            max_bytes: 256 << 20,
            max_entry_bytes: 32 << 20,
            max_rows_per_query: 100_000,
            default_ttl: Ttl::After(Duration::from_secs(300)),
            fill_timeout: Duration::from_secs(5),
            max_lag: Duration::from_secs(2),
            refusal_ttl: Duration::from_secs(60),
            start_timeout: Duration::from_secs(30),
        }
    }
}

impl CacheConfig {
    /// The ttl a read that named `asked` keeps its query for: what it
    /// named, else the default.
    pub fn ttl_for(&self, asked: Option<Ttl>) -> Ttl {
        asked.unwrap_or(self.default_ttl)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A read's own ttl wins; without one, the default.
    #[test]
    fn ttl_precedence() {
        let config = CacheConfig::default();
        assert_eq!(config.ttl_for(Some(Ttl::Immediate)), Ttl::Immediate);
        assert_eq!(config.ttl_for(None), config.default_ttl);
        assert!(Ttl::Immediate < Ttl::After(Duration::from_secs(1)));
        assert!(Ttl::After(Duration::from_secs(1)) < Ttl::Forever);
    }
}
