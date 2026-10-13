//! The result cache: rows by query key, bounded by bytes, kept fresh by the
//! engine's touches.
//!
//! An entry is the rows one read returned, byte for byte. It is filled
//! after the query is subscribed and read, dropped when the engine reports
//! the query touched (the next read refills it from Postgres), and evicted
//! when the cache is over its byte budget, which also ends the
//! subscription.
//!
//! Between subscribing and filling there is a window: the read runs, and a
//! touch may arrive for the key before the rows are in. A generation per
//! key closes it. [`Cache::reserve`] takes the generation before the read,
//! [`Cache::fill`] stores the rows only if no touch bumped it since.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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

/// Counters for dashboards and tests.
#[derive(Debug, Default)]
pub struct Stats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub refused: AtomicU64,
    pub touched: AtomicU64,
    pub evicted: AtomicU64,
    /// Fills dropped because the key was touched during the read.
    pub stale_fills: AtomicU64,
}

impl Stats {
    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

pub struct Cache {
    entries: moka::sync::Cache<QueryKey, Arc<Entry>>,
    generations: Mutex<HashMap<QueryKey, u64>>,
    subscriptions: Arc<dyn Subscriptions>,
    pub stats: Arc<Stats>,
}

impl Cache {
    /// A cache holding at most `max_bytes` of rows and keys, subscribing
    /// through `subscriptions`.
    pub fn new(max_bytes: u64, subscriptions: Arc<dyn Subscriptions>) -> Cache {
        let stats = Arc::new(Stats::default());
        let listener_subscriptions = Arc::clone(&subscriptions);
        let listener_stats = Arc::clone(&stats);
        let entries = moka::sync::Cache::builder()
            .max_capacity(max_bytes)
            .weigher(|_: &QueryKey, entry: &Arc<Entry>| {
                u32::try_from(entry.bytes).unwrap_or(u32::MAX)
            })
            .eviction_listener(move |key: Arc<QueryKey>, _: Arc<Entry>, cause| {
                // A touch drops the entry but keeps the subscription: the
                // next read refills it. Only a size eviction ends it.
                if cause == RemovalCause::Size {
                    Stats::bump(&listener_stats.evicted);
                    listener_subscriptions.unsubscribe(&key);
                }
            })
            .build();
        Cache {
            entries,
            generations: Mutex::new(HashMap::new()),
            subscriptions,
            stats,
        }
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

    /// The engine reported a change to the query's rows: drop them, so the
    /// next read refills from Postgres, and fail any fill in flight.
    pub fn touched(&self, key: &QueryKey) {
        Stats::bump(&self.stats.touched);
        {
            let mut generations = self.generations.lock().expect("generations");
            match generations.get_mut(key) {
                Some(generation) => *generation += 1,
                None => {
                    generations.insert(key.clone(), 1);
                }
            }
        }
        self.entries.invalidate(key);
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

impl std::fmt::Debug for Cache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache")
            .field("entries", &self.entries.entry_count())
            .field("stats", &self.stats)
            .finish()
    }
}
