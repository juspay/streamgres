//! The result cache: rows by query key, bounded by bytes, kept fresh by the
//! engine's touches.
//!
//! An entry is the rows one read returned, byte for byte. It is filled
//! after the query is subscribed and read, dropped when the engine reports
//! the query touched (the next read refills it from Postgres), and evicted
//! when the cache is over its byte budget or the entry has expired
//! ([`Limits`]), either of which also ends the subscription.
//!
//! Two clocks bound an entry's life. Idle time drops a query nobody has
//! read for a while, with its subscription, so a backend's cache follows
//! what it reads now. Time to live drops an entry on a timer however
//! often it is read: a safety net that makes the next read resubscribe
//! and refill from the engine, off by default since the engine keeps the
//! entry fresh. An engine delta that replaces an entry restarts its
//! clock.
//!
//! Between subscribing and filling there is a window: the read runs, and a
//! touch may arrive for the key before the rows are in. A generation per
//! key closes it. [`Cache::reserve`] takes the generation before the read,
//! [`Cache::fill`] stores the rows only if no touch bumped it since.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use moka::notification::RemovalCause;

use crate::key::QueryKey;
use crate::row::OwnedRow;
use crate::subscriptions::Subscriptions;

/// The rows one read returned.
#[derive(Debug)]
pub struct Entry {
    pub rows: Arc<[OwnedRow]>,
    bytes: usize,
}

impl Entry {
    fn new(rows: Arc<[OwnedRow]>, key: &QueryKey) -> Entry {
        let bytes = key.byte_len()
            + rows.iter().map(OwnedRow::byte_len).sum::<usize>()
            + rows.len() * ROW_OVERHEAD;
        Entry { rows, bytes }
    }
}

/// What a row costs beyond its bytes: its vector of values and the
/// bookkeeping around it. A rough figure; the budget is a ceiling, not an
/// accounting.
const ROW_OVERHEAD: usize = 64;

/// What bounds the cache: bytes, idle time and time to live. From the
/// environment through [`Limits::from_env`]:
///
/// | variable                     | meaning                                    | default   |
/// |------------------------------|--------------------------------------------|-----------|
/// | `STREAMGRES_CACHE_MAX_BYTES` | rows and keys held, in bytes               | 268435456 |
/// | `STREAMGRES_CACHE_IDLE_MS`   | drop an entry unread for this long; 0 never| 1800000   |
/// | `STREAMGRES_CACHE_TTL_MS`    | drop an entry this old however read; 0 never| 0        |
///
/// A value that is missing or not a number leaves the default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    pub max_bytes: u64,
    pub time_to_idle: Option<Duration>,
    pub time_to_live: Option<Duration>,
}

pub const DEFAULT_MAX_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_TIME_TO_IDLE: Duration = Duration::from_secs(30 * 60);

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            max_bytes: DEFAULT_MAX_BYTES,
            time_to_idle: Some(DEFAULT_TIME_TO_IDLE),
            time_to_live: None,
        }
    }
}

impl Limits {
    pub fn from_env() -> Limits {
        let defaults = Limits::default();
        let millis = |name: &str, default: Option<Duration>| match env_number(name) {
            Some(0) => None,
            Some(millis) => Some(Duration::from_millis(millis)),
            None => default,
        };
        Limits {
            max_bytes: env_number("STREAMGRES_CACHE_MAX_BYTES").unwrap_or(defaults.max_bytes),
            time_to_idle: millis("STREAMGRES_CACHE_IDLE_MS", defaults.time_to_idle),
            time_to_live: millis("STREAMGRES_CACHE_TTL_MS", defaults.time_to_live),
        }
    }
}

fn env_number(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.trim().parse::<u64>().ok()
}

/// Counters for dashboards and tests.
#[derive(Debug, Default)]
pub struct Stats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub refused: AtomicU64,
    pub touched: AtomicU64,
    /// Entries dropped for the byte budget.
    pub evicted: AtomicU64,
    /// Entries dropped by the idle or time-to-live clock.
    pub expired: AtomicU64,
    /// Fills dropped because the key was touched during the read.
    pub stale_fills: AtomicU64,
    /// Entries the engine's own rows replaced (rows mode).
    pub replaced: AtomicU64,
    /// Reads sent to Postgres because their connection had just written.
    pub after_write: AtomicU64,
}

impl Stats {
    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

type Generations = Arc<Mutex<HashMap<QueryKey, u64>>>;

pub struct Cache {
    entries: moka::sync::Cache<QueryKey, Arc<Entry>>,
    generations: Generations,
    subscriptions: Arc<dyn Subscriptions>,
    limits: Limits,
    pub stats: Arc<Stats>,
}

impl Cache {
    /// A cache holding at most `max_bytes` of rows and keys, with the
    /// default clocks, subscribing through `subscriptions`.
    pub fn new(max_bytes: u64, subscriptions: Arc<dyn Subscriptions>) -> Cache {
        Self::with_limits(
            Limits {
                max_bytes,
                ..Limits::default()
            },
            subscriptions,
        )
    }

    pub fn with_limits(limits: Limits, subscriptions: Arc<dyn Subscriptions>) -> Cache {
        let stats = Arc::new(Stats::default());
        let generations: Generations = Arc::default();
        let listener_subscriptions = Arc::clone(&subscriptions);
        let listener_stats = Arc::clone(&stats);
        let listener_generations = Arc::clone(&generations);
        let mut builder = moka::sync::Cache::builder()
            .max_capacity(limits.max_bytes)
            .weigher(|_: &QueryKey, entry: &Arc<Entry>| {
                u32::try_from(entry.bytes).unwrap_or(u32::MAX)
            })
            .eviction_listener(move |key: Arc<QueryKey>, _: Arc<Entry>, cause| {
                // A touch drops the entry but keeps the subscription: the
                // next read refills it. The budget and the clocks end it,
                // and bump the generation so a fill in flight for the key
                // is dropped rather than kept without a subscription.
                let ended = match cause {
                    RemovalCause::Size => &listener_stats.evicted,
                    RemovalCause::Expired => &listener_stats.expired,
                    RemovalCause::Explicit | RemovalCause::Replaced => return,
                };
                Stats::bump(ended);
                bump(&listener_generations, &key);
                listener_subscriptions.unsubscribe(&key);
            });
        if let Some(idle) = limits.time_to_idle {
            builder = builder.time_to_idle(idle);
        }
        if let Some(live) = limits.time_to_live {
            builder = builder.time_to_live(live);
        }
        Cache {
            entries: builder.build(),
            generations,
            subscriptions,
            limits,
            stats,
        }
    }

    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// The rows for a key, if the cache has them.
    pub fn get(&self, key: &QueryKey) -> Option<Arc<Entry>> {
        let entry = self.entries.get(key);
        Stats::bump(match entry {
            Some(_) => &self.stats.hits,
            None => &self.stats.misses,
        });
        entry
    }

    /// The engine side the cache subscribes through.
    pub fn subscriptions(&self) -> &dyn Subscriptions {
        &*self.subscriptions
    }

    /// The key's generation, taken before the read that will fill it.
    pub fn reserve(&self, key: &QueryKey) -> u64 {
        self.generation(key)
    }

    /// Stores the rows a read returned, unless the key was touched since
    /// `generation` was taken. Returns whether they were stored.
    pub fn fill(&self, key: QueryKey, generation: u64, rows: Arc<[OwnedRow]>) -> bool {
        if self.generation(&key) != generation {
            Stats::bump(&self.stats.stale_fills);
            return false;
        }
        let entry = Entry::new(rows, &key);
        self.entries.insert(key, Arc::new(entry));
        true
    }

    /// The engine served the rows itself: store them whatever the
    /// generation. Used by rows-mode subscriptions, whose entries are
    /// never filled from a Postgres read.
    pub fn replace(&self, key: &QueryKey, rows: Arc<[OwnedRow]>) {
        Stats::bump(&self.stats.replaced);
        let entry = Entry::new(rows, key);
        self.entries.insert(key.clone(), Arc::new(entry));
    }

    /// The engine reported a change to the query's rows: drop them, so the
    /// next read refills from Postgres, and fail any fill in flight.
    pub fn touched(&self, key: &QueryKey) {
        Stats::bump(&self.stats.touched);
        bump(&self.generations, key);
        self.entries.invalidate(key);
    }

    /// Runs the maintenance the clocks need, so expired entries are
    /// dropped (and unsubscribed) now rather than on the next access.
    /// moka does this on its own as the cache is used; this is for
    /// tests and quiet processes.
    pub fn run_pending_tasks(&self) {
        self.entries.run_pending_tasks();
    }

    /// Drops every entry. Used when the engine connection was lost and the
    /// subscriptions are re-registered from scratch.
    pub fn clear(&self) {
        self.entries.invalidate_all();
        self.generations.lock().expect("generations").clear();
    }

    /// How many entries are in, after pending maintenance.
    pub fn len(&self) -> u64 {
        self.entries.run_pending_tasks();
        self.entries.entry_count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The bytes the entries weigh, after pending maintenance.
    pub fn weighted_size(&self) -> u64 {
        self.entries.run_pending_tasks();
        self.entries.weighted_size()
    }

    fn generation(&self, key: &QueryKey) -> u64 {
        self.generations
            .lock()
            .expect("generations")
            .get(key)
            .copied()
            .unwrap_or(0)
    }
}

fn bump(generations: &Generations, key: &QueryKey) {
    let mut generations = generations.lock().expect("generations");
    match generations.get_mut(key) {
        Some(generation) => *generation += 1,
        None => {
            generations.insert(key.clone(), 1);
        }
    }
}

impl std::fmt::Debug for Cache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache")
            .field("entries", &self.entries.entry_count())
            .field("stats", &self.stats)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_default_to_idle_on_and_ttl_off() {
        let limits = Limits::default();
        assert_eq!(limits.max_bytes, DEFAULT_MAX_BYTES);
        assert_eq!(limits.time_to_idle, Some(DEFAULT_TIME_TO_IDLE));
        assert_eq!(limits.time_to_live, None);
        assert_eq!(env_number("STREAMGRES_CACHE_TEST_UNSET_VARIABLE"), None);
    }
}
