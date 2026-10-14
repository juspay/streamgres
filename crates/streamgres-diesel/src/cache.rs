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
//! [`Cache::fill`] stores the rows only if no touch bumped it since. A
//! generation exists only while a read holds a [`Reservation`] on its
//! key: a touch on a key nobody is filling has nothing to invalidate but
//! the entry, so the map holds the keys in flight, not every key ever
//! seen.
//!
//! The cache also keeps the one clock the read-your-writes window needs:
//! when any connection through it last wrote. A pool's connections
//! share a cache, so a write on one is followed by reads on another, and
//! those must see it too.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use diesel::pg::PgMetadataCache;
use moka::notification::RemovalCause;

use crate::key::{QueryKey, QueryShape};
use crate::row::OwnedRow;
use crate::subscriptions::Subscriptions;

/// The rows one read returned.
#[derive(Debug)]
pub struct Entry {
    pub rows: Arc<[OwnedRow]>,
    bytes: usize,
}

impl Entry {
    /// `extra` is what the entry costs beyond its rows: the engine-side
    /// store a rows-mode subscription keeps for it.
    fn new(rows: Arc<[OwnedRow]>, key: &QueryKey, extra: usize) -> Entry {
        let bytes = key.byte_len() + rows.iter().map(OwnedRow::byte_len).sum::<usize>() + extra;
        Entry { rows, bytes }
    }

    /// What the entry weighs against the byte budget.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

/// What bounds the cache: bytes, idle time, time to live, and how long a
/// refusal is remembered. From the environment through
/// [`Limits::from_env`]:
///
/// | variable                           | meaning                                      | default   |
/// |------------------------------------|----------------------------------------------|-----------|
/// | `STREAMGRES_CACHE_MAX_BYTES`       | rows and keys held, in bytes                 | 268435456 |
/// | `STREAMGRES_CACHE_MAX_ENTRY_BYTES` | the largest result kept; 0 no limit          | 16777216  |
/// | `STREAMGRES_CACHE_IDLE_MS`         | drop an entry unread for this long; 0 never  | 1800000   |
/// | `STREAMGRES_CACHE_TTL_MS`          | drop an entry this old however read; 0 never | 0         |
/// | `STREAMGRES_REFUSED_TTL_MS`        | do not re-ask the engine about a refused query for this long; 0 always ask | 30000 |
///
/// A value that is missing or not a number leaves the default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    pub max_bytes: u64,
    /// A result larger than this is served but not kept, and its
    /// subscription ends: one query must not take the whole budget.
    pub max_entry_bytes: u64,
    pub time_to_idle: Option<Duration>,
    pub time_to_live: Option<Duration>,
    /// A query the engine refused (a shape outside its subset) would
    /// otherwise cost a round trip to the engine on every read; for this
    /// long it goes straight to Postgres instead.
    pub refused_for: Option<Duration>,
}

pub const DEFAULT_MAX_BYTES: u64 = 256 * 1024 * 1024;
pub const DEFAULT_MAX_ENTRY_BYTES: u64 = 16 * 1024 * 1024;
pub const DEFAULT_TIME_TO_IDLE: Duration = Duration::from_secs(30 * 60);
pub const DEFAULT_REFUSED_FOR: Duration = Duration::from_secs(30);

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            max_bytes: DEFAULT_MAX_BYTES,
            max_entry_bytes: DEFAULT_MAX_ENTRY_BYTES,
            time_to_idle: Some(DEFAULT_TIME_TO_IDLE),
            time_to_live: None,
            refused_for: Some(DEFAULT_REFUSED_FOR),
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
            max_entry_bytes: match env_number("STREAMGRES_CACHE_MAX_ENTRY_BYTES") {
                Some(0) => u64::MAX,
                Some(bytes) => bytes,
                None => defaults.max_entry_bytes,
            },
            time_to_idle: millis("STREAMGRES_CACHE_IDLE_MS", defaults.time_to_idle),
            time_to_live: millis("STREAMGRES_CACHE_TTL_MS", defaults.time_to_live),
            refused_for: millis("STREAMGRES_REFUSED_TTL_MS", defaults.refused_for),
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
    /// Reads the engine (or the client's own limits) refused; each one
    /// asked.
    pub refused: AtomicU64,
    /// Reads sent to Postgres without asking because the query was
    /// refused recently.
    pub refused_recently: AtomicU64,
    pub touched: AtomicU64,
    /// Entries dropped for the byte budget.
    pub evicted: AtomicU64,
    /// Entries dropped by the idle or time-to-live clock.
    pub expired: AtomicU64,
    /// Fills dropped because the key was touched during the read.
    pub stale_fills: AtomicU64,
    /// Results served but not kept because they were larger than
    /// [`Limits::max_entry_bytes`].
    pub oversized: AtomicU64,
    /// Entries the engine's own rows replaced (rows mode).
    pub replaced: AtomicU64,
    /// Reads sent to Postgres because their connection had just written.
    pub after_write: AtomicU64,
    /// Reads sent to Postgres because the read's lock (`FOR UPDATE`)
    /// cannot be taken on a cache.
    pub locking: AtomicU64,
}

impl Stats {
    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// A key's generation while reads are in flight for it.
#[derive(Default)]
struct Slot {
    generation: u64,
    inflight: u32,
}

type Generations = Arc<Mutex<HashMap<QueryKey, Slot>>>;

/// A read's claim on a key between reserving it and filling it. Dropping
/// it, with or without a fill, lets the key's generation go once no
/// other read holds one.
pub struct Reservation {
    generations: Generations,
    key: QueryKey,
    generation: u64,
}

impl Reservation {
    pub fn key(&self) -> &QueryKey {
        &self.key
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut generations = self.generations.lock().expect("generations");
        if let Some(slot) = generations.get_mut(&self.key) {
            slot.inflight = slot.inflight.saturating_sub(1);
            if slot.inflight == 0 {
                generations.remove(&self.key);
            }
        }
    }
}

pub struct Cache {
    entries: moka::sync::Cache<QueryKey, Arc<Entry>>,
    generations: Generations,
    subscriptions: Arc<dyn Subscriptions>,
    /// Shapes refused recently, each with the reason, dropped on their
    /// own clock.
    refused: Option<moka::sync::Cache<QueryShape, Arc<str>>>,
    limits: Limits,
    /// When a connection through this cache last wrote, as nanoseconds
    /// since `started` plus one; 0 is never.
    last_write: AtomicU64,
    started: Instant,
    /// The OIDs of user-defined types, resolved once per database for
    /// the async wrapper (the blocking one asks its connection).
    type_metadata: Arc<Mutex<PgMetadataCache>>,
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
        let refused = limits.refused_for.map(|window| {
            moka::sync::Cache::builder()
                .max_capacity(10_000)
                .time_to_live(window)
                .build()
        });
        Cache {
            entries: builder.build(),
            generations,
            subscriptions,
            refused,
            limits,
            last_write: AtomicU64::new(0),
            started: Instant::now(),
            type_metadata: Arc::new(Mutex::new(PgMetadataCache::new())),
            stats,
        }
    }

    /// Counts a refusal of `key`, and when the engine side says the
    /// reason will hold, remembers it so reads of the key's shape skip
    /// the engine for [`Limits::refused_for`].
    pub fn refused(&self, key: &QueryKey, reason: &str) {
        Stats::bump(&self.stats.refused);
        if let Some(refused) = &self.refused
            && self.subscriptions.refusal_is_final(reason)
        {
            refused.insert(key.shape(), Arc::from(reason));
        }
    }

    /// Why `key`'s shape was refused recently, if it was; counts the
    /// skipped ask.
    pub fn refused_recently(&self, key: &QueryKey) -> Option<Arc<str>> {
        let reason = self.refused.as_ref()?.get(&key.shape())?;
        Stats::bump(&self.stats.refused_recently);
        Some(reason)
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

    /// The same, owned, for a subscribe on another thread.
    pub fn shared_subscriptions(&self) -> Arc<dyn Subscriptions> {
        Arc::clone(&self.subscriptions)
    }

    /// The key's generation, taken before the read that will fill it.
    pub fn reserve(&self, key: &QueryKey) -> Reservation {
        let generation = {
            let mut generations = self.generations.lock().expect("generations");
            let slot = generations.entry(key.clone()).or_default();
            slot.inflight += 1;
            slot.generation
        };
        Reservation {
            generations: Arc::clone(&self.generations),
            key: key.clone(),
            generation,
        }
    }

    /// Stores the rows a read returned, unless the key was touched since
    /// the reservation was taken or the result is over the entry limit.
    /// Returns whether they were stored.
    pub fn fill(&self, reservation: Reservation, rows: Arc<[OwnedRow]>) -> bool {
        self.fill_weighing(reservation, rows, 0)
    }

    /// [`Cache::fill`] for rows the engine serves itself, weighing the
    /// store kept for them too.
    ///
    /// The generation is checked before and after the insert: a touch
    /// or a replace between the two would otherwise leave the older
    /// rows in, since nothing holds a lock across the insert (moka runs
    /// the eviction listener on the inserting thread, and the listener
    /// takes the generations lock).
    pub fn fill_weighing(
        &self,
        reservation: Reservation,
        rows: Arc<[OwnedRow]>,
        store_bytes: usize,
    ) -> bool {
        if self.generation(&reservation.key) != reservation.generation {
            Stats::bump(&self.stats.stale_fills);
            return false;
        }
        let entry = Entry::new(rows, &reservation.key, store_bytes);
        if !self.insert(&reservation.key, entry) {
            return false;
        }
        if self.generation(&reservation.key) != reservation.generation {
            // Something newer landed while the rows went in: what it
            // left is dropped too, which costs one refill, never a stale
            // row.
            Stats::bump(&self.stats.stale_fills);
            self.entries.invalidate(&reservation.key);
            return false;
        }
        true
    }

    /// The engine served the rows itself: store them, and fail any fill
    /// in flight, whose rows are older than these. Used by rows-mode
    /// subscriptions; `store_bytes` is what the store kept for them
    /// weighs. The caller reserves the key before it builds the rows,
    /// so a subscription ended meanwhile (which bumps the generation
    /// once more) drops them again: the engine side's own writes are
    /// held to the same rule as a Postgres read's.
    pub fn replace(&self, reservation: Reservation, rows: Arc<[OwnedRow]>, store_bytes: usize) {
        Stats::bump(&self.stats.replaced);
        let key = &reservation.key;
        bump(&self.generations, key);
        let entry = Entry::new(rows, key, store_bytes);
        if self.insert(key, entry) && self.generation(key) != reservation.generation + 1 {
            Stats::bump(&self.stats.stale_fills);
            self.entries.invalidate(key);
        }
    }

    /// Fails any fill or replace in flight for `key`, without touching
    /// the entry. For the engine side to call while it drops the key,
    /// under its own lock, so that no rows built before the drop can
    /// land after it.
    pub fn bump_generation(&self, key: &QueryKey) {
        bump(&self.generations, key);
    }

    fn insert(&self, key: &QueryKey, entry: Entry) -> bool {
        if entry.bytes as u64 > self.limits.max_entry_bytes {
            Stats::bump(&self.stats.oversized);
            self.abandon(key);
            return false;
        }
        self.entries.insert(key.clone(), Arc::new(entry));
        true
    }

    /// The engine reported a change to the query's rows: drop them, so the
    /// next read refills from Postgres, and fail any fill in flight.
    pub fn touched(&self, key: &QueryKey) {
        Stats::bump(&self.stats.touched);
        bump(&self.generations, key);
        self.entries.invalidate(key);
    }

    /// Ends the key's subscription from the cache's side: a read gave up
    /// on it (dropped, or its Postgres read failed) or its result was
    /// too large to keep. The generation is bumped first, so a fill in
    /// flight for the key by another read is dropped rather than kept
    /// with nothing to update it: the sequence the eviction listener
    /// uses, and the only way the wrapper may unsubscribe.
    pub fn abandon(&self, key: &QueryKey) {
        // The subscription first: the engine side bumps the generation
        // as it drops the key, so an engine delta that checked the key
        // before this point and inserts after it is dropped by its own
        // re-check. Then the bump again for a Postgres fill in flight,
        // and the entry.
        self.subscriptions.unsubscribe(key);
        bump(&self.generations, key);
        self.entries.invalidate(key);
    }

    /// A connection through this cache wrote.
    pub fn mark_write(&self) {
        let nanos = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX - 1);
        self.last_write.fetch_max(nanos + 1, Ordering::Release);
    }

    /// Whether a connection through this cache wrote within `window`.
    pub fn wrote_within(&self, window: Duration) -> bool {
        if window.is_zero() {
            return false;
        }
        match self.last_write.load(Ordering::Acquire) {
            0 => false,
            marked => {
                let since = self
                    .started
                    .elapsed()
                    .saturating_sub(Duration::from_nanos(marked - 1));
                since < window
            }
        }
    }

    /// The OIDs of user-defined types resolved so far for this database.
    pub fn type_metadata(&self) -> &Arc<Mutex<PgMetadataCache>> {
        &self.type_metadata
    }

    /// Runs the maintenance the clocks need, so expired entries are
    /// dropped (and unsubscribed) now rather than on the next access.
    /// moka does this on its own as the cache is used; this is for
    /// tests and quiet processes.
    pub fn run_pending_tasks(&self) {
        self.entries.run_pending_tasks();
    }

    /// Drops every entry and fails every fill in flight. Used when the
    /// engine connection was lost and the subscriptions are
    /// re-registered from scratch.
    pub fn clear(&self) {
        self.entries.invalidate_all();
        for slot in self.generations.lock().expect("generations").values_mut() {
            slot.generation += 1;
        }
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

    /// How many keys have a read in flight, for tests.
    pub fn reservations(&self) -> usize {
        self.generations.lock().expect("generations").len()
    }

    fn generation(&self, key: &QueryKey) -> u64 {
        self.generations
            .lock()
            .expect("generations")
            .get(key)
            .map_or(0, |slot| slot.generation)
    }
}

/// Fails every fill in flight for `key`. A key with none has no
/// generation to bump: its entry, if any, is what the caller drops.
fn bump(generations: &Generations, key: &QueryKey) {
    if let Some(slot) = generations.lock().expect("generations").get_mut(key) {
        slot.generation += 1;
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
    use crate::materialized::Snapshot;
    use crate::subscriptions::{Outcome, Unsynced, Verdict};
    use crate::types::TableTypes;

    fn key(sql: &str) -> QueryKey {
        QueryKey {
            sql: sql.to_string(),
            binds: Vec::new(),
        }
    }

    fn no_rows() -> Arc<[OwnedRow]> {
        Arc::from(Vec::new())
    }

    /// Accepts everything and records what is unsubscribed.
    #[derive(Default)]
    struct Recording {
        unsubscribed: Mutex<Vec<QueryKey>>,
    }

    impl Subscriptions for Recording {
        fn subscribe(&self, _: &QueryKey) -> Outcome {
            Outcome::Verdict(Verdict::Accepted)
        }

        fn materialize(
            &self,
            _: &QueryKey,
            _: Snapshot,
            _: HashMap<String, Arc<TableTypes>>,
        ) -> Verdict {
            Verdict::Accepted
        }

        fn unsubscribe(&self, key: &QueryKey) {
            self.unsubscribed.lock().unwrap().push(key.clone());
        }
    }

    #[test]
    fn abandoning_a_key_fails_the_other_readers_fill() {
        // Two readers of one key; the second gives up (its future was
        // dropped, or its read failed) and ends the subscription. The
        // first must not then store an entry nothing will invalidate.
        let engine = Arc::new(Recording::default());
        let cache = Cache::new(1 << 20, engine.clone());
        let k = key("SELECT 1");
        let first = cache.reserve(&k);
        let second = cache.reserve(&k);
        drop(second);
        cache.abandon(&k);
        assert_eq!(engine.unsubscribed.lock().unwrap().len(), 1);
        assert!(
            !cache.fill(first, no_rows()),
            "the first reader's fill is refused"
        );
        assert!(cache.is_empty());
        assert_eq!(cache.reservations(), 0);
    }

    #[test]
    fn engine_rows_beat_a_fill_in_flight() {
        // A rows-mode caller holds the snapshot's rows; a delta replaces
        // the entry before the caller fills. The older rows must lose.
        let cache = Cache::new(1 << 20, Arc::new(Unsynced));
        let k = key("SELECT 1");
        let reservation = cache.reserve(&k);
        let columns: crate::row::Columns = Arc::from(vec!["a".to_string()]);
        let newer: Arc<[OwnedRow]> = Arc::from(vec![OwnedRow::new(columns, vec![None])]);
        let engine_side = cache.reserve(&k);
        cache.replace(engine_side, Arc::clone(&newer), 0);
        assert!(
            !cache.fill(reservation, no_rows()),
            "the snapshot's rows are older than the delta's"
        );
        assert_eq!(
            cache.get(&k).map(|entry| entry.rows.len()),
            Some(1),
            "the delta's rows stay"
        );
        assert_eq!(cache.stats.stale_fills.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_replace_after_the_key_was_abandoned_does_not_stick() {
        // The engine side reserved before building its rows; the key is
        // abandoned (which bumps as the subscription goes) in between.
        let engine = Arc::new(Recording::default());
        let cache = Cache::new(1 << 20, engine.clone());
        let k = key("SELECT 1");
        let engine_side = cache.reserve(&k);
        cache.abandon(&k);
        cache.replace(engine_side, no_rows(), 0);
        assert!(
            cache.is_empty(),
            "rows built for a subscription that ended are not kept"
        );
    }

    #[test]
    fn an_oversized_result_ends_the_subscription_and_the_other_fill() {
        let engine = Arc::new(Recording::default());
        let cache = Cache::with_limits(
            Limits {
                max_entry_bytes: 8,
                ..Limits::default()
            },
            engine.clone(),
        );
        let k = key("SELECT a long enough statement");
        let other = cache.reserve(&k);
        let engine_side = cache.reserve(&k);
        cache.replace(engine_side, no_rows(), 0);
        assert_eq!(engine.unsubscribed.lock().unwrap().len(), 1);
        assert!(
            !cache.fill(other, no_rows()),
            "the other reader's smaller fill cannot keep a key with no subscription"
        );
    }

    #[test]
    fn limits_default_to_idle_on_and_ttl_off() {
        let limits = Limits::default();
        assert_eq!(limits.max_bytes, DEFAULT_MAX_BYTES);
        assert_eq!(limits.max_entry_bytes, DEFAULT_MAX_ENTRY_BYTES);
        assert_eq!(limits.time_to_idle, Some(DEFAULT_TIME_TO_IDLE));
        assert_eq!(limits.time_to_live, None);
        assert_eq!(env_number("STREAMGRES_CACHE_TEST_UNSET_VARIABLE"), None);
    }

    #[test]
    fn generations_live_only_while_a_read_is_in_flight() {
        let cache = Cache::new(1 << 20, Arc::new(Unsynced));
        let k = key("SELECT 1");
        assert_eq!(cache.reservations(), 0);
        let reservation = cache.reserve(&k);
        assert_eq!(cache.reservations(), 1);
        cache.touched(&k);
        assert!(
            !cache.fill(reservation, no_rows()),
            "touched during the read"
        );
        assert_eq!(
            cache.reservations(),
            0,
            "the reservation went with the fill"
        );

        // Touches, evictions and expiries of keys nobody is reading leave
        // nothing behind.
        for i in 0..1000 {
            cache.touched(&key(&format!("SELECT {i}")));
        }
        assert_eq!(cache.reservations(), 0);

        // A reservation dropped without a fill goes too.
        let dropped = cache.reserve(&k);
        drop(dropped);
        assert_eq!(cache.reservations(), 0);
    }

    #[test]
    fn an_older_reservation_cannot_fill_after_a_newer_one_did() {
        let cache = Cache::new(1 << 20, Arc::new(Unsynced));
        let k = key("SELECT 1");
        let older = cache.reserve(&k);
        cache.touched(&k);
        let newer = cache.reserve(&k);
        assert!(
            cache.fill(newer, no_rows()),
            "the newer read saw the change"
        );
        assert!(!cache.fill(older, no_rows()), "the older read did not");
        assert_eq!(cache.reservations(), 0);
    }

    #[test]
    fn a_result_over_the_entry_limit_is_not_kept() {
        let cache = Cache::with_limits(
            Limits {
                max_entry_bytes: 8,
                ..Limits::default()
            },
            Arc::new(Unsynced),
        );
        let k = key("SELECT a long enough statement");
        let reservation = cache.reserve(&k);
        assert!(!cache.fill(reservation, no_rows()));
        assert_eq!(cache.stats.oversized.load(Ordering::Relaxed), 1);
        assert!(cache.is_empty());
        let engine_side = cache.reserve(&k);
        cache.replace(engine_side, no_rows(), 0);
        assert!(
            cache.is_empty(),
            "the engine's rows are bounded the same way"
        );
    }

    #[test]
    fn the_write_clock_is_shared() {
        let cache = Cache::new(1 << 20, Arc::new(Unsynced));
        assert!(!cache.wrote_within(Duration::from_secs(1)));
        cache.mark_write();
        assert!(cache.wrote_within(Duration::from_secs(1)));
        assert!(!cache.wrote_within(Duration::ZERO), "a zero window is off");
    }

    #[test]
    fn a_refusal_covers_every_value_of_the_shape() {
        let cache = Cache::new(1 << 20, Arc::new(Unsynced));
        let mut a = key("SELECT $1");
        a.binds.push(crate::key::Bind {
            type_oid: 25,
            bytes: Some(b"a".to_vec()),
        });
        let mut b = a.clone();
        b.binds[0].bytes = Some(b"b".to_vec());
        cache.refused(&a, "unsupported");
        assert!(cache.refused_recently(&b).is_some());
    }
}
