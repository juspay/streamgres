//! What the cache holds, shared between the engine's thread and the
//! threads that read: the engine's phase, each query kept current with
//! its rows, and where the engine is.
//!
//! Readers take the state's lock shared: a read of a query already kept
//! changes nothing but an atomic. Only the engine's thread, a first read
//! of a query and a read that composes a changed query's rows take it
//! alone.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use super::config::Ttl;
use super::engine::Engine;
use super::metrics::Counters;
use super::rows::Projected;
use super::{CacheConfig, CatalogVersion, Refusal};
use crate::compose::Joined;
use crate::ivm::{Delta, QueryPart};
use crate::model::{
    Catalog, DataFrameKey, DataFrameOperation, DataFrameRow, MultiTableReadQuery, Position, SubId,
};
use crate::parser::{Projection, ResultPlan};
use crate::shutdown::Shutdown;
use crate::sync::{CatalogHandle, Command, Event};

/// Where the engine is in its life.
pub(super) enum Phase {
    Starting,
    Running(Running),
    /// It failed, and a fresh one will be started; why it failed.
    Down(String),
    Stopped,
}

/// A running engine: how to talk to it and the catalog it reads by.
pub(super) struct Running {
    pub commands: mpsc::Sender<Command<MultiTableReadQuery>>,
    /// Subscriptions to end, taken up by the engine's thread: a reader
    /// cannot wait for room in `commands`, and an unsubscribe must not be
    /// lost.
    pub unsubscribe: mpsc::UnboundedSender<SubId>,
    pub catalog: Arc<CatalogHandle>,
    /// What the engine calls this instance (its replication slot).
    pub name: String,
}

/// How far a query's rows are.
pub(super) enum Fill {
    /// Its first rows have not all arrived; since when.
    Pending(Instant),
    Ready,
    /// A change left its rows incomplete until a storage read lands;
    /// since when.
    Settling(Instant),
}

/// One statement shape's projection of a query's ordered rows.
pub(super) struct Kept {
    pub projection: Projection,
    pub plan: ResultPlan,
    pub version: CatalogVersion,
    pub rows: Arc<Projected>,
}
impl Kept {
    fn bytes(&self) -> usize {
        self.rows
            .bytes()
            .saturating_add(super::parse::plan_bytes(&self.plan))
    }
}

/// One query kept current.
pub(super) struct Entry {
    pub query: MultiTableReadQuery,
    /// The statement that first loaded it, for inspection.
    pub sql: Arc<str>,
    pub sub: Option<SubId>,
    pub fill: Fill,
    /// The rows of each part of the query's tree.
    pub parts: HashMap<QueryPart, HashMap<DataFrameKey, Arc<DataFrameRow>>>,
    /// The joined rows in the statement's order, kept until the rows next
    /// change.
    pub ordered: Option<Arc<[Joined]>>,
    pub ordered_by: Option<ResultPlan>,
    /// Each statement shape's projection of `ordered` read so far;
    /// dropped with `ordered`.
    pub projected: Vec<Kept>,
    /// Bumped by every change of the rows, so an ordering built from an
    /// older snapshot is not kept.
    pub generation: u64,
    /// Its last read and longest requested ttl, also updated by local hits.
    pub usage: Arc<Usage>,
    /// What `parts` and `projected` weigh, as [`row_bytes`] counts.
    bytes: usize,
}

impl Entry {
    fn ordered_bytes(&self) -> usize {
        self.ordered
            .as_ref()
            .map_or(0, |rows| crate::compose::ordered_bytes(rows))
            .saturating_add(self.ordered_by.as_ref().map_or(0, super::parse::plan_bytes))
    }
    /// What its rows weigh, as the budget counts them.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// How many rows it keeps, across its tables.
    pub fn rows(&self) -> usize {
        self.parts.values().map(HashMap::len).sum()
    }

    /// Its ttl.
    pub fn ttl(&self) -> Ttl {
        self.usage.ttl()
    }
    pub fn idle(&self, now: u64) -> u64 {
        self.usage.idle(now)
    }
    pub fn touch(&self, now: u64, ttl: Ttl) {
        self.usage.touch(now, ttl);
    }
}

/// Retention shared with thread-local answers. A hit updates the same
/// atomics even when it needs no state lock.
pub(super) struct Usage {
    last_read: AtomicU64,
    ttl: AtomicU64,
}
impl Usage {
    pub fn new(now: u64, ttl: Ttl) -> Self {
        Self {
            last_read: AtomicU64::new(now),
            ttl: AtomicU64::new(ttl.millis()),
        }
    }
    pub fn ttl(&self) -> Ttl {
        match self.ttl.load(Ordering::Relaxed) {
            0 => Ttl::Immediate,
            u64::MAX => Ttl::Forever,
            millis => Ttl::After(Duration::from_millis(millis)),
        }
    }
    pub fn idle(&self, now: u64) -> u64 {
        now.saturating_sub(self.last_read.load(Ordering::Relaxed))
    }
    pub fn touch(&self, now: u64, ttl: Ttl) {
        self.ttl.fetch_max(ttl.millis(), Ordering::Relaxed);
        self.last_read.fetch_max(now, Ordering::Relaxed);
    }
}

/// Everything behind the state's lock.
pub(super) struct State {
    pub phase: Phase,
    pub entries: HashMap<u64, Entry>,
    by_query: HashMap<MultiTableReadQuery, u64>,
    by_sub: HashMap<SubId, u64>,
    next_id: u64,
    /// Every transaction committed at or below this has been applied.
    pub position: Position,
    /// When the engine last told where the feed is.
    heard: Option<Instant>,
    /// How long the last event waited in this process before it was
    /// applied.
    queued: Duration,
    /// How far behind its commit the source sent the last transaction,
    /// and when that was heard.
    behind: Option<(Duration, Instant)>,
    /// Source samples waiting for the engine to apply their WAL position.
    samples: VecDeque<(Position, Instant)>,
    /// When the freshest applied source position was sampled.
    confirmed: Option<Instant>,
    refused: HashMap<MultiTableReadQuery, (Arc<str>, Instant)>,
    refused_bytes: usize,
    /// What every entry weighs together.
    bytes: usize,
    /// Entries a change left weighing more than one query may.
    overweight: Vec<u64>,
}

/// How long the source's lag, as the last transaction told it, is
/// believed without another: after this of only position marks, the
/// stream is quiet and the source is not behind.
const BEHIND_STANDS: Duration = Duration::from_secs(1);

/// How long an answer kept on a thread stands without touching its entry.
const LOCAL_TOUCH: Duration = Duration::from_secs(1);

pub(super) use super::parse::ParseMemo;

/// The state behind its lock, the condition readers wait on for it to
/// change, and what does not need the lock.
pub(super) struct Shared {
    state: RwLock<State>,
    /// This cache's own allocation: what tells its answers kept on a
    /// thread ([`super::local`]) from another cache's.
    pub identity: Arc<()>,
    /// Bumped by every change of the state, before its lock is let go.
    pub revision: AtomicU64,
    /// Counts the state's changes; a reader waits for it to move.
    epoch: AtomicU64,
    wait_gate: Mutex<()>,
    changed: Condvar,
    pub parsed: ParseMemo,
    pub counters: Counters,
    pub config: CacheConfig,
    /// The engine the cache runs and reads the origin of.
    pub engine: Arc<dyn Engine>,
    /// Asked once, when the cache is stopped.
    pub stopping: Shutdown,
    started: Instant,
}

/// The state's lock, held to change it. Letting it go bumps
/// [`Shared::revision`] first, so no reader can see the change and still
/// find an answer kept under the revision before it.
pub(super) struct StateWrite<'a> {
    shared: &'a Shared,
    guard: RwLockWriteGuard<'a, State>,
}
impl std::ops::Deref for StateWrite<'_> {
    type Target = State;
    fn deref(&self) -> &State {
        &self.guard
    }
}
impl std::ops::DerefMut for StateWrite<'_> {
    fn deref_mut(&mut self) -> &mut State {
        &mut self.guard
    }
}
impl Drop for StateWrite<'_> {
    fn drop(&mut self) {
        self.shared.revision.fetch_add(1, Ordering::Release);
    }
}

impl Shared {
    pub fn new(config: CacheConfig, engine: Arc<dyn Engine>) -> Self {
        Shared {
            identity: Arc::new(()),
            revision: AtomicU64::new(0),
            state: RwLock::new(State {
                phase: Phase::Starting,
                entries: HashMap::new(),
                by_query: HashMap::new(),
                by_sub: HashMap::new(),
                next_id: 1,
                position: Position(0),
                heard: None,
                queued: Duration::ZERO,
                behind: None,
                samples: VecDeque::new(),
                confirmed: None,
                refused: HashMap::new(),
                refused_bytes: 0,
                bytes: 0,
                overweight: Vec::new(),
            }),
            epoch: AtomicU64::new(0),
            wait_gate: Mutex::new(()),
            changed: Condvar::new(),
            parsed: ParseMemo::new(),
            counters: Counters::default(),
            config,
            engine,
            stopping: Shutdown::new(),
            started: Instant::now(),
        }
    }

    /// The state, shared with other readers. A poisoned lock is read
    /// through: every change to the state is complete before its lock is
    /// let go.
    pub fn read(&self) -> RwLockReadGuard<'_, State> {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The state, to change it; follow the change with [`Shared::notify`]
    /// once the lock is let go, to wake the readers waiting on it.
    pub fn write(&self) -> StateWrite<'_> {
        StateWrite {
            shared: self,
            guard: self
                .state
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        }
    }

    /// The count of the state's changes so far. Taken before looking at
    /// the state, it lets [`Shared::wait`] tell a change made since.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Waits without losing a notification between the state read and sleep.
    pub fn wait(&self, seen: u64, timeout: Duration) {
        let gate = self
            .wait_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _ = self
            .changed
            .wait_timeout_while(gate, timeout, |_| self.epoch() == seen);
    }

    /// Wakes waiters. Cache hits only read the atomic epoch.
    pub fn notify(&self) {
        let _gate = self
            .wait_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.epoch.fetch_add(1, Ordering::Release);
        self.changed.notify_all();
    }

    /// Milliseconds since the cache started: the clock of
    /// [`Entry::touch`].
    pub fn now(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Marks the cache stopped and asks whatever engine is starting or
    /// running to stop.
    pub fn stop(&self) {
        self.write().phase = Phase::Stopped;
        self.stopping.request();
        self.notify();
    }
}

/// Both copies of the query and the entry/index bookkeeping are retained
/// even when its result is empty, so they belong in the byte budget too.
pub(super) fn entry_metadata_bytes(query: &MultiTableReadQuery) -> usize {
    super::parse::query_bytes(query)
        .saturating_mul(2)
        .saturating_add(std::mem::size_of::<Entry>() + 128)
}

/// What a row weighs in the cache's budget: an estimate of its heap, by
/// the lengths of its values.
pub(super) fn row_bytes(row: &DataFrameRow) -> usize {
    128 + row
        .data
        .iter()
        .map(|(name, value)| 32 + name.as_str().len() + value_bytes(value))
        .sum::<usize>()
}

pub(super) use crate::compose::value_bytes;

impl State {
    /// How stale an answer given now may be: the longer of how long ago
    /// the newest source position the engine has applied was sampled, and
    /// how long the last event waited in this process plus how far behind
    /// the source last said it was.
    pub fn lag(&self) -> Duration {
        let behind = match self.behind {
            Some((behind, at)) if at.elapsed() < BEHIND_STANDS => behind,
            _ => Duration::ZERO,
        };
        self.queued
            .saturating_add(behind)
            .max(self.confirmed.map_or(Duration::ZERO, |at| at.elapsed()))
    }

    /// Whether reads can be answered now: the engine running, a source
    /// position sampled within `max_lag` applied, the engine heard from
    /// within `max_lag`, and its [`State::lag`] within it.
    pub fn readiness(&self, config: &CacheConfig) -> Result<(), Refusal> {
        match &self.phase {
            Phase::Running(_)
                if self
                    .confirmed
                    .is_none_or(|at| at.elapsed() > config.max_lag) =>
            {
                Err(Refusal::NotReady(
                    "no recently sampled source position has been applied".to_owned(),
                ))
            }
            Phase::Running(_) => match self.heard {
                Some(heard) if heard.elapsed() > config.max_lag => Err(Refusal::NotReady(format!(
                    "the engine has not heard the database for {:?}",
                    heard.elapsed()
                ))),
                Some(_) if self.lag() > config.max_lag => Err(Refusal::NotReady(format!(
                    "the engine is {:?} behind the database",
                    self.lag()
                ))),
                Some(_) => Ok(()),
                None => Err(Refusal::NotReady("the engine is catching up".to_owned())),
            },
            Phase::Starting => Err(Refusal::NotReady("the engine is starting".to_owned())),
            Phase::Down(reason) => Err(Refusal::NotReady(format!(
                "the engine is restarting after: {reason}"
            ))),
            Phase::Stopped => Err(Refusal::NotReady("the cache is stopped".to_owned())),
        }
    }

    /// Until when an answer kept on a thread may stand: until the engine's
    /// freshness lapses, or [`LOCAL_TOUCH`] (so its entry is touched again
    /// before it would be swept), whichever is sooner. `None` when reads
    /// cannot be answered now.
    pub fn answer_deadline(&self, config: &CacheConfig) -> Option<Instant> {
        self.readiness(config).ok()?;
        let confirmed = self.confirmed?.checked_add(config.max_lag)?;
        let heard = self.heard?.checked_add(config.max_lag)?;
        Some(
            confirmed
                .min(heard)
                .min(Instant::now().checked_add(LOCAL_TOUCH)?),
        )
    }

    /// Whether the running engine has heard the database yet.
    pub fn heard_once(&self) -> bool {
        self.heard.is_some()
    }

    /// The running engine.
    pub fn running(&self) -> Result<&Running, Refusal> {
        match &self.phase {
            Phase::Running(running) => Ok(running),
            _ => Err(Refusal::NotReady("the engine is not running".to_owned())),
        }
    }

    /// The catalog the running engine reads by.
    pub fn catalog(&self) -> Result<Arc<Catalog>, Refusal> {
        Ok(self.running()?.catalog.load())
    }

    /// The reason `query` was refused, if it was within `ttl`.
    pub fn refused(&self, query: &MultiTableReadQuery, ttl: Duration) -> Option<Arc<str>> {
        match self.refused.get(query) {
            Some((reason, at)) if at.elapsed() < ttl => Some(reason.clone()),
            _ => None,
        }
    }

    /// The entry kept for `query`.
    pub fn entry_of(&self, query: &MultiTableReadQuery) -> Option<u64> {
        self.by_query.get(query).copied()
    }

    /// How many queries are kept, and what they weigh.
    pub fn held(&self) -> (usize, usize) {
        (self.entries.len(), self.bytes)
    }

    /// A new, pending entry for `query`, read at `now`, and the
    /// subscriptions of the entries that gave way to it to keep at most
    /// `max_entries`.
    pub fn add_entry(
        &mut self,
        query: MultiTableReadQuery,
        max_entries: usize,
        now: u64,
        ttl: Ttl,
        sql: Arc<str>,
    ) -> (u64, Vec<SubId>) {
        let mut evicted = Vec::new();
        if self.entries.len() >= max_entries.max(1) {
            let room = (max_entries / 100).max(1);
            evicted.extend(self.evict_oldest(room));
        }
        let id = self.next_id;
        self.next_id += 1;
        let metadata_bytes = entry_metadata_bytes(&query);
        self.bytes = self.bytes.saturating_add(metadata_bytes);
        self.by_query.insert(query.clone(), id);
        self.entries.insert(
            id,
            Entry {
                query,
                sql,
                sub: None,
                fill: Fill::Pending(Instant::now()),
                parts: HashMap::new(),
                ordered: None,
                ordered_by: None,
                projected: Vec::new(),
                generation: 0,
                usage: Arc::new(Usage::new(now, ttl)),
                bytes: metadata_bytes,
            },
        );
        (id, evicted)
    }

    /// Drops the `count` entries read longest ago, those kept
    /// [`Ttl::Forever`] only after every other; their subscriptions.
    fn evict_oldest(&mut self, count: usize) -> Vec<SubId> {
        let mut by_age: Vec<(bool, u64, u64)> = self
            .entries
            .iter()
            .map(|(id, entry)| {
                (
                    entry.usage.ttl.load(Ordering::Relaxed) == u64::MAX,
                    entry.usage.last_read.load(Ordering::Relaxed),
                    *id,
                )
            })
            .collect();
        let count = count.min(by_age.len());
        if count == 0 {
            return Vec::new();
        }
        by_age.select_nth_unstable(count - 1);
        by_age
            .into_iter()
            .take(count)
            .filter_map(|(_, _, id)| self.remove_entry(id))
            .collect()
    }

    /// Drops entry `id`; its subscription, if it has one, to unsubscribe.
    pub fn remove_entry(&mut self, id: u64) -> Option<SubId> {
        let entry = self.entries.remove(&id)?;
        self.bytes = self.bytes.saturating_sub(entry.bytes);
        if self.by_query.get(&entry.query) == Some(&id) {
            self.by_query.remove(&entry.query);
        }
        let sub = entry.sub?;
        self.by_sub.remove(&sub);
        Some(sub)
    }

    /// Keeps `kept` and its ordering (replacing the previous ordering) for entry `id`
    /// if its rows are still those of `generation` and the byte budget has
    /// room for them.
    pub fn keep_projection(
        &mut self,
        id: u64,
        generation: u64,
        ordered: Arc<[Joined]>,
        kept: Kept,
        config: &CacheConfig,
    ) {
        let old_ordered = self.entries.get(&id).map_or(0, Entry::ordered_bytes);
        let ordered_weight = crate::compose::ordered_bytes(&ordered)
            .saturating_add(super::parse::plan_bytes(&kept.plan));
        let bytes = kept.bytes().saturating_add(ordered_weight);
        if self.bytes.saturating_sub(old_ordered).saturating_add(bytes) > config.max_bytes {
            return;
        }
        let Some(entry) = self.entries.get_mut(&id) else {
            return;
        };
        if entry.generation != generation
            || entry.projected.iter().any(|other| {
                other.version == kept.version
                    && other.projection == kept.projection
                    && other.plan == kept.plan
            })
        {
            return;
        }
        if entry
            .bytes
            .saturating_sub(old_ordered)
            .saturating_add(bytes)
            > config.max_entry_bytes
        {
            return;
        }
        entry.ordered = Some(ordered);
        entry.ordered_by = Some(kept.plan.clone());
        let stale: usize = entry
            .projected
            .iter()
            .filter(|other| other.version != kept.version)
            .map(Kept::bytes)
            .sum();
        entry
            .projected
            .retain(|other| other.version == kept.version);
        entry.projected.push(kept);
        entry.bytes = entry
            .bytes
            .saturating_sub(stale)
            .saturating_sub(old_ordered)
            .saturating_add(bytes);
        self.bytes = self
            .bytes
            .saturating_sub(stale)
            .saturating_sub(old_ordered)
            .saturating_add(bytes);
    }

    /// The housekeeping of one tick: entries nobody has read within their
    /// ttl, or filling or settling for over twice `fill_timeout`, are
    /// dropped, and refusals older than `refusal_ttl` are forgotten. The
    /// subscriptions to end.
    pub fn sweep(&mut self, now: u64, config: &CacheConfig) -> Vec<SubId> {
        let stuck = config.fill_timeout * 2;
        let gone: Vec<u64> = self
            .entries
            .iter()
            .filter(|(_, entry)| match &entry.fill {
                // Reads use fallback while filling or settling. Keep a live
                // fill until it completes; drop one that has become stuck.
                Fill::Pending(since) | Fill::Settling(since) => since.elapsed() > stuck,
                Fill::Ready => {
                    now.saturating_sub(entry.usage.last_read.load(Ordering::Relaxed))
                        > entry.usage.ttl.load(Ordering::Relaxed)
                }
            })
            .map(|(id, _)| *id)
            .collect();
        self.refused
            .retain(|_, (_, at)| at.elapsed() < config.refusal_ttl);
        self.refused_bytes = self
            .refused
            .iter()
            .map(|(query, (reason, _))| super::parse::query_bytes(query) + reason.len())
            .sum();
        gone.into_iter()
            .filter_map(|id| self.remove_entry(id))
            .collect()
    }

    /// A fresh engine is running: everything kept for the one before is
    /// gone with it.
    pub fn started(&mut self, running: Running) {
        self.entries.clear();
        self.by_query.clear();
        self.by_sub.clear();
        self.bytes = 0;
        self.position = Position(0);
        self.heard = None;
        self.queued = Duration::ZERO;
        self.behind = None;
        self.samples.clear();
        self.confirmed = None;
        self.refused.clear();
        self.refused_bytes = 0;
        self.overweight.clear();
        self.phase = Phase::Running(running);
    }

    /// Records a WAL watermark sampled over SQL independently of the feed.
    /// Receipt of old transactions or synthetic position marks cannot refresh it.
    pub fn observe_source(&mut self, position: Position, sampled: Instant) {
        self.samples.push_back((position, sampled));
        self.confirm_samples();
        // At five samples a second this retains over 25 seconds of history.
        // Dropping an old unconfirmed sample can only cause extra fallback.
        while self.samples.len() > 128 {
            self.samples.pop_front();
        }
    }

    /// Confirms the samples whose position the engine has applied.
    fn confirm_samples(&mut self) {
        while self
            .samples
            .front()
            .is_some_and(|(position, _)| *position <= self.position)
        {
            if let Some((_, at)) = self.samples.pop_front() {
                self.confirmed = Some(self.confirmed.map_or(at, |old| old.max(at)));
            }
        }
    }

    /// Takes in one event of the engine; the subscriptions to unsubscribe
    /// that it leaves without an entry.
    pub fn absorb(
        &mut self,
        event: Event,
        config: &CacheConfig,
        counters: &Counters,
    ) -> Vec<SubId> {
        let mut orphans = Vec::new();
        match event {
            Event::Registered {
                token,
                sub,
                updates,
                ..
            } => match self.entries.get_mut(&token) {
                Some(entry) => {
                    entry.sub = Some(sub);
                    self.by_sub.insert(sub, token);
                    orphans.extend(self.apply(&updates, config, counters));
                }
                None => orphans.push(sub),
            },
            Event::Landed { updates } => orphans.extend(self.apply(&updates, config, counters)),
            Event::Committed {
                updates,
                position,
                received,
                ..
            } => {
                orphans.extend(self.apply(&updates, config, counters));
                self.position = self.position.max(position);
                self.confirm_samples();
                self.heard = Some(Instant::now());
                self.queued = received.elapsed();
            }
            Event::Hydrated(subs) => {
                for sub in subs {
                    if let Some(entry) = self.entry_mut(sub) {
                        entry.fill = Fill::Ready;
                    }
                }
            }
            Event::Unsettled(subs) => {
                for sub in subs {
                    if let Some(entry) = self.entry_mut(sub)
                        && matches!(entry.fill, Fill::Ready)
                    {
                        entry.fill = Fill::Settling(Instant::now());
                    }
                }
            }
            Event::Refused { sub, reason } => {
                self.refuse(sub, reason.into(), config.max_entries, config.max_bytes / 8);
                orphans.push(sub);
            }
            Event::Capped { sub } => {
                self.refuse(
                    sub,
                    "the query matches more rows than the engine follows".into(),
                    config.max_entries,
                    config.max_bytes / 8,
                );
                orphans.push(sub);
            }
            Event::Heavy { .. } => {}
        }
        orphans.extend(self.keep_within(config, counters));
        orphans
    }

    fn entry_mut(&mut self, sub: SubId) -> Option<&mut Entry> {
        self.by_sub
            .get(&sub)
            .and_then(|id| self.entries.get_mut(id))
    }

    /// Holds the cache to its budget: a query weighing more than one may
    /// is refused, and while everything together weighs more than the
    /// cache may, the queries read longest ago give way. The
    /// subscriptions to end.
    pub fn keep_within(&mut self, config: &CacheConfig, counters: &Counters) -> Vec<SubId> {
        let mut ended = Vec::new();
        let heavy: Vec<SubId> = std::mem::take(&mut self.overweight)
            .into_iter()
            .filter_map(|id| self.entries.get(&id))
            .filter(|entry| entry.bytes > config.max_entry_bytes)
            .filter_map(|entry| entry.sub)
            .collect();
        for sub in heavy {
            self.refuse(
                sub,
                format!(
                    "the query's rows weigh more than the {} bytes one query may hold",
                    config.max_entry_bytes
                )
                .into(),
                config.max_entries,
                config.max_bytes / 8,
            );
            ended.push(sub);
        }
        while self.bytes > config.max_bytes {
            let evicted = self.evict_oldest((self.entries.len() / 16).max(1));
            if self.entries.is_empty() && evicted.is_empty() {
                break;
            }
            counters
                .evicted
                .fetch_add(u64::try_from(evicted.len()).unwrap_or(0), Ordering::Relaxed);
            ended.extend(evicted);
        }
        ended
    }

    /// Remembers that `sub`'s query is refused and empties its entry, so
    /// readers waiting on it see the reason.
    fn refuse(&mut self, sub: SubId, reason: Arc<str>, capacity: usize, byte_budget: usize) {
        let Some(id) = self.by_sub.get(&sub).copied() else {
            return;
        };
        let Some(query) = self.entries.get(&id).map(|entry| entry.query.clone()) else {
            return;
        };
        self.remove_entry(id);
        let bytes = super::parse::query_bytes(&query).saturating_add(reason.len());
        let budget = byte_budget.min(8 << 20);
        if bytes > budget {
            return;
        }
        while self.refused.len() >= capacity.max(1)
            || self.refused_bytes.saturating_add(bytes) > budget
        {
            let gone: Vec<_> = self
                .refused
                .keys()
                .take((capacity / 8).max(1))
                .cloned()
                .collect();
            if gone.is_empty() {
                break;
            }
            for query in gone {
                if let Some((reason, _)) = self.refused.remove(&query) {
                    self.refused_bytes = self
                        .refused_bytes
                        .saturating_sub(super::parse::query_bytes(&query) + reason.len());
                }
            }
        }
        if let Some((old, _)) = self.refused.remove(&query) {
            self.refused_bytes = self
                .refused_bytes
                .saturating_sub(super::parse::query_bytes(&query) + old.len());
        }
        self.refused_bytes = self.refused_bytes.saturating_add(bytes);
        self.refused.insert(query, (reason, Instant::now()));
    }

    /// Applies the engine's row changes to the entries they concern,
    /// noting those left weighing more than `max_entry_bytes`.
    fn apply(
        &mut self,
        updates: &[Delta],
        config: &CacheConfig,
        counters: &Counters,
    ) -> Vec<SubId> {
        let mut ended = Vec::new();
        for delta in updates {
            let added = match &delta.op {
                DataFrameOperation::Add(_, row) => Some((Arc::new(row.clone()), row_bytes(row))),
                DataFrameOperation::Delete(..) => None,
            };
            for target in delta.targets() {
                let Some(id) = self.by_sub.get(&target.sub).copied() else {
                    continue;
                };
                let Some(entry) = self.entries.get_mut(&id) else {
                    continue;
                };
                let before = entry.bytes;
                let replaced = match (&delta.op, &added) {
                    (DataFrameOperation::Add(key, _), Some((row, bytes))) => {
                        entry.bytes = entry.bytes.saturating_add(*bytes);
                        entry
                            .parts
                            .entry(target.part)
                            .or_default()
                            .insert(key.clone(), row.clone())
                    }
                    (DataFrameOperation::Delete(key, _), _) => entry
                        .parts
                        .get_mut(&target.part)
                        .and_then(|rows| rows.remove(key)),
                    (DataFrameOperation::Add(..), None) => continue,
                };
                if let Some(old) = replaced {
                    entry.bytes = entry.bytes.saturating_sub(row_bytes(&old));
                }
                let projected: usize = entry
                    .projected
                    .iter()
                    .map(Kept::bytes)
                    .sum::<usize>()
                    .saturating_add(entry.ordered_bytes());
                entry.bytes = entry.bytes.saturating_sub(projected);
                entry.ordered = None;
                entry.ordered_by = None;
                entry.projected.clear();
                entry.generation += 1;
                if entry.bytes > config.max_entry_bytes {
                    self.overweight.push(id);
                }
                self.bytes = self
                    .bytes
                    .saturating_sub(before)
                    .saturating_add(entry.bytes);
            }
            ended.extend(self.keep_within(config, counters));
        }
        ended
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Value;

    /// A shared state whose engine never starts.
    fn scripted() -> Arc<dyn Engine> {
        Arc::new(crate::cache::engine::tests::Scripted(|_: &Shutdown| {
            Err("no engine".to_owned())
        }))
    }

    fn query(value: i64) -> MultiTableReadQuery {
        use crate::model::{
            ComparisonOperator, Condition, Order, OrderBy, SingleTableReadQuery, Where,
        };
        MultiTableReadQuery::single(SingleTableReadQuery::new(
            "t",
            Where::Condition(Condition::new(
                "id",
                ComparisonOperator::EQ,
                Value::Int(value),
            )),
            vec![OrderBy::new("id", Order::ASC)],
            u32::MAX,
        ))
    }

    #[test]
    fn busy_old_transactions_cannot_refresh_source_freshness() {
        let config = CacheConfig::default();
        let shared = Shared::new(config.clone(), scripted());
        let mut state = shared.write();
        let (commands, _rx) = mpsc::channel(1);
        let (unsubscribe, _unsubscribed) = mpsc::unbounded_channel();
        state.started(Running {
            commands,
            unsubscribe,
            catalog: Arc::new(CatalogHandle::new(Catalog::new(vec![]))),
            name: "test".into(),
        });
        state.heard = Some(Instant::now());
        state.observe_source(Position(100), Instant::now() - Duration::from_secs(10));
        state.position = Position(10);
        state.confirm_samples();
        assert!(
            state.readiness(&config).is_err(),
            "a busy feed behind the source refuses"
        );
        state.position = Position(100);
        state.confirm_samples();
        assert!(
            state.readiness(&config).is_err(),
            "an old watermark alone is still stale"
        );
        state.observe_source(Position(100), Instant::now());
        assert!(state.readiness(&config).is_ok());
    }

    #[test]
    fn refused_queries_have_a_hard_count_bound() {
        let config = CacheConfig::default();
        let shared = Shared::new(config, scripted());
        let mut state = shared.write();
        for n in 0..100 {
            let (id, _) = state.add_entry(query(n), 8, 0, Ttl::Forever, "".into());
            let sub = SubId(id);
            state.entries.get_mut(&id).unwrap().sub = Some(sub);
            state.by_sub.insert(sub, id);
            state.refuse(sub, "unsupported".into(), 8, 4096);
            assert!(state.refused.len() <= 8);
        }
    }

    #[test]
    fn a_hydration_stops_retaining_rows_when_it_crosses_its_budget() {
        use crate::ivm::{Audience, Subs};
        let config = CacheConfig {
            max_entry_bytes: 100,
            ..CacheConfig::default()
        };
        let shared = Shared::new(config.clone(), scripted());
        let mut state = shared.write();
        let (id, _) = state.add_entry(query(1), 8, 0, Ttl::Forever, "".into());
        let sub = SubId(id);
        state.entries.get_mut(&id).unwrap().sub = Some(sub);
        state.by_sub.insert(sub, id);
        let updates: Vec<_> = (0..100)
            .map(|n| Delta {
                table: "t".into(),
                op: DataFrameOperation::Add(
                    DataFrameKey::new(HashMap::from([(
                        crate::model::ColumnName::from("id"),
                        Value::Int(n),
                    )])),
                    DataFrameRow::from(HashMap::from([(
                        "payload".into(),
                        Value::from("x".repeat(1000)),
                    )])),
                ),
                audiences: vec![Audience {
                    part: QueryPart::main(),
                    subs: Subs::One(sub),
                }],
            })
            .collect();
        let ended = state.apply(&updates, &config, &shared.counters);
        assert_eq!(ended, vec![sub]);
        assert_eq!(state.bytes, 0);
        assert!(!state.entries.contains_key(&id));
    }

    #[test]
    fn notification_before_wait_is_not_lost() {
        let shared = Shared::new(CacheConfig::default(), scripted());
        let seen = shared.epoch();
        shared.notify();
        let at = Instant::now();
        shared.wait(seen, Duration::from_secs(1));
        assert!(at.elapsed() < Duration::from_millis(100));
    }
}
