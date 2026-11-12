//! What the cache holds, shared between the engine's thread and the
//! threads that read: the engine's phase, each query kept current with
//! its rows, and where the engine is.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use super::{CacheConfig, Refusal};
use crate::ivm::{Delta, QueryPart};
use crate::model::{
    Catalog, DataFrameKey, DataFrameOperation, DataFrameRow, MultiTableReadQuery, Position, SubId,
};
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

/// A running engine: how to talk to it, the catalog it reads by, and how
/// to stop it.
pub(super) struct Running {
    pub commands: mpsc::Sender<Command<MultiTableReadQuery>>,
    pub catalog: Arc<CatalogHandle>,
    pub shutdown: Shutdown,
    pub slot: String,
}

/// How far a query's rows are.
pub(super) enum Fill {
    Pending,
    Ready,
    Refused(String),
}

/// One query kept current.
pub(super) struct Entry {
    pub query: MultiTableReadQuery,
    pub sub: Option<SubId>,
    pub fill: Fill,
    /// The rows of each part of the query's tree.
    pub parts: HashMap<QueryPart, HashMap<DataFrameKey, Arc<DataFrameRow>>>,
    /// The joined rows in the statement's order, kept until the rows next
    /// change.
    pub ordered: Option<Arc<[super::rows::Joined]>>,
    /// Each statement's projection of `ordered` read so far, by its
    /// projection and the catalog it was resolved against; dropped with
    /// `ordered`.
    pub projected: Vec<(
        crate::parser::Projection,
        super::CatalogVersion,
        Arc<super::rows::Projected>,
    )>,
    /// Bumped by every change of the rows, so an ordering built from an
    /// older snapshot is not kept.
    pub generation: u64,
    pub last_read: Instant,
}

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
    refused: HashMap<MultiTableReadQuery, (String, Instant)>,
}

/// The state behind its lock, and the condition readers wait on for it
/// to change.
/// The hash a parsed statement is filed under.
pub(super) fn parse_hash(
    sql: &str,
    binds: &[crate::parser::Bind],
    version: &super::CatalogVersion,
) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sql.hash(&mut hasher);
    binds.hash(&mut hasher);
    version.hash(&mut hasher);
    hasher.finish()
}

/// What parsing a statement came to: the statement, or why the engine
/// cannot answer it. Both follow from the text, the binds and the
/// catalog alone, so both are kept.
pub(super) type Parsed = Result<Arc<crate::parser::Select>, Arc<str>>;

/// The parsed statements, filed by [`parse_hash`].
pub(super) type ParseMemo = HashMap<u64, Vec<(ParseKey, Parsed)>>;

/// What identifies a parsed statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct ParseKey {
    pub sql: Box<str>,
    pub binds: Vec<crate::parser::Bind>,
    pub version: super::CatalogVersion,
}

/// The most parsed statements kept; past it an eighth of them give way.
pub(super) const PARSED_LIMIT: usize = 8192;

pub(super) struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    /// Statements already parsed, by their text, binds and catalog, and
    /// those refused: a repeated read is not parsed again.
    pub parsed: std::sync::RwLock<ParseMemo>,
    pub config: CacheConfig,
    /// The database the engine streams.
    pub database: String,
}

impl Shared {
    pub fn new(config: CacheConfig) -> Self {
        Shared {
            state: Mutex::new(State {
                phase: Phase::Starting,
                entries: HashMap::new(),
                by_query: HashMap::new(),
                by_sub: HashMap::new(),
                next_id: 1,
                position: Position(0),
                heard: None,
                refused: HashMap::new(),
            }),
            changed: Condvar::new(),
            parsed: std::sync::RwLock::new(HashMap::new()),
            database: config.database(),
            config,
        }
    }

    /// The state, read through a poisoned lock: every change to it is
    /// complete before the lock is let go.
    pub fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Waits at most `timeout` for the state to change.
    pub fn wait<'a>(
        &self,
        guard: MutexGuard<'a, State>,
        timeout: Duration,
    ) -> MutexGuard<'a, State> {
        match self.changed.wait_timeout(guard, timeout) {
            Ok((guard, _)) => guard,
            Err(poisoned) => poisoned.into_inner().0,
        }
    }

    /// Wakes every waiting reader.
    pub fn notify(&self) {
        self.changed.notify_all();
    }

    /// Marks the cache stopped and asks the running engine to stop.
    pub fn stop(&self) {
        let mut state = self.lock();
        if let Phase::Running(running) = &state.phase {
            running.shutdown.request();
        }
        state.phase = Phase::Stopped;
        drop(state);
        self.notify();
    }
}

impl State {
    /// Whether reads can be answered now: the engine running and heard
    /// from within `max_lag`.
    pub fn readiness(&self, config: &CacheConfig) -> Result<(), Refusal> {
        match &self.phase {
            Phase::Running(_) => match self.heard {
                Some(heard) if heard.elapsed() <= config.max_lag => Ok(()),
                Some(heard) => Err(Refusal::NotReady(format!(
                    "the engine has not heard the database for {:?}",
                    heard.elapsed()
                ))),
                None => Err(Refusal::NotReady("the engine is catching up".to_owned())),
            },
            Phase::Starting => Err(Refusal::NotReady("the engine is starting".to_owned())),
            Phase::Down(reason) => Err(Refusal::NotReady(format!(
                "the engine is restarting after: {reason}"
            ))),
            Phase::Stopped => Err(Refusal::NotReady("the cache is stopped".to_owned())),
        }
    }

    /// The catalog the running engine reads by.
    pub fn catalog(&self) -> Result<Arc<Catalog>, Refusal> {
        match &self.phase {
            Phase::Running(running) => Ok(running.catalog.load()),
            _ => Err(Refusal::NotReady("the engine is not running".to_owned())),
        }
    }

    /// The channel to the running engine.
    pub fn commands(&self) -> Result<mpsc::Sender<Command<MultiTableReadQuery>>, Refusal> {
        match &self.phase {
            Phase::Running(running) => Ok(running.commands.clone()),
            _ => Err(Refusal::NotReady("the engine is not running".to_owned())),
        }
    }

    /// The reason `query` was refused, if it was within `ttl`.
    pub fn refused(&mut self, query: &MultiTableReadQuery, ttl: Duration) -> Option<String> {
        match self.refused.get(query) {
            Some((reason, at)) if at.elapsed() < ttl => Some(reason.clone()),
            Some(_) => {
                self.refused.remove(query);
                None
            }
            None => None,
        }
    }

    /// The entry kept for `query`.
    pub fn entry_of(&self, query: &MultiTableReadQuery) -> Option<u64> {
        self.by_query.get(query).copied()
    }

    /// A new, pending entry for `query`, and the subscriptions of the
    /// entries that gave way to it to keep at most `max_entries`.
    pub fn add_entry(
        &mut self,
        query: MultiTableReadQuery,
        max_entries: usize,
    ) -> (u64, Vec<SubId>) {
        let mut evicted = Vec::new();
        if self.entries.len() >= max_entries.max(1) {
            let mut by_age: Vec<(Instant, u64)> = self
                .entries
                .iter()
                .map(|(id, entry)| (entry.last_read, *id))
                .collect();
            let room = (max_entries / 100).clamp(1, by_age.len());
            by_age.select_nth_unstable(room - 1);
            for (_, id) in by_age.into_iter().take(room) {
                evicted.extend(self.remove_entry(id));
            }
        }
        let id = self.next_id;
        self.next_id += 1;
        self.by_query.insert(query.clone(), id);
        self.entries.insert(
            id,
            Entry {
                query,
                sub: None,
                fill: Fill::Pending,
                parts: HashMap::new(),
                ordered: None,
                projected: Vec::new(),
                generation: 0,
                last_read: Instant::now(),
            },
        );
        (id, evicted)
    }

    /// Drops entry `id`; its subscription, if it has one, to unsubscribe.
    pub fn remove_entry(&mut self, id: u64) -> Option<SubId> {
        let entry = self.entries.remove(&id)?;
        if self.by_query.get(&entry.query) == Some(&id) {
            self.by_query.remove(&entry.query);
        }
        let sub = entry.sub?;
        self.by_sub.remove(&sub);
        Some(sub)
    }

    /// The subscriptions of entries nobody has read within `ttl`, their
    /// entries dropped, refused entries with them (their readers have
    /// seen the refusal; the refusal itself is remembered apart).
    pub fn evict_idle(&mut self, ttl: Duration) -> Vec<SubId> {
        let idle: Vec<u64> = self
            .entries
            .iter()
            .filter(|(_, entry)| {
                matches!(entry.fill, Fill::Refused(_)) || entry.last_read.elapsed() > ttl
            })
            .map(|(id, _)| *id)
            .collect();
        idle.into_iter()
            .filter_map(|id| self.remove_entry(id))
            .collect()
    }

    /// A fresh engine is running: everything kept for the one before is
    /// gone with it.
    pub fn running(&mut self, running: Running) {
        self.entries.clear();
        self.by_query.clear();
        self.by_sub.clear();
        self.position = Position(0);
        self.heard = None;
        self.phase = Phase::Running(running);
    }

    /// Takes in one event of the engine; the subscriptions to unsubscribe
    /// that it leaves without an entry.
    pub fn absorb(&mut self, event: Event) -> Vec<SubId> {
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
                    self.apply(&updates);
                }
                None => orphans.push(sub),
            },
            Event::Landed { updates } => self.apply(&updates),
            Event::Committed {
                updates, position, ..
            } => {
                self.apply(&updates);
                self.position = self.position.max(position);
                self.heard = Some(Instant::now());
            }
            Event::Hydrated(subs) => {
                for sub in subs {
                    if let Some(entry) = self
                        .by_sub
                        .get(&sub)
                        .and_then(|id| self.entries.get_mut(id))
                    {
                        entry.fill = Fill::Ready;
                    }
                }
            }
            Event::Unsettled(subs) => {
                for sub in subs {
                    if let Some(entry) = self
                        .by_sub
                        .get(&sub)
                        .and_then(|id| self.entries.get_mut(id))
                        && matches!(entry.fill, Fill::Ready)
                    {
                        entry.fill = Fill::Pending;
                    }
                }
            }
            Event::Refused { sub, reason } => {
                self.refuse(sub, reason);
                orphans.push(sub);
            }
            Event::Capped { sub } => {
                self.refuse(
                    sub,
                    "the query matches more rows than the engine follows".to_owned(),
                );
                orphans.push(sub);
            }
            Event::Heavy { .. } => {}
        }
        orphans
    }

    /// Remembers that `sub`'s query is refused and drops its entry, so
    /// readers waiting on it see the reason.
    fn refuse(&mut self, sub: SubId, reason: String) {
        let Some(id) = self.by_sub.remove(&sub) else {
            return;
        };
        if let Some(entry) = self.entries.get_mut(&id) {
            entry.fill = Fill::Refused(reason.clone());
            entry.sub = None;
            self.by_query.remove(&entry.query);
            self.refused
                .insert(entry.query.clone(), (reason, Instant::now()));
        }
    }

    /// Applies the engine's row changes to the entries they concern.
    fn apply(&mut self, updates: &[Delta]) {
        for delta in updates {
            let added = match &delta.op {
                DataFrameOperation::Add(_, row) => Some(Arc::new(row.clone())),
                DataFrameOperation::Delete(..) => None,
            };
            for target in delta.targets() {
                let Some(entry) = self
                    .by_sub
                    .get(&target.sub)
                    .and_then(|id| self.entries.get_mut(id))
                else {
                    continue;
                };
                match (&delta.op, &added) {
                    (DataFrameOperation::Add(key, _), Some(row)) => {
                        entry
                            .parts
                            .entry(target.part)
                            .or_default()
                            .insert(key.clone(), row.clone());
                    }
                    (DataFrameOperation::Delete(key, _), _) => {
                        if let Some(rows) = entry.parts.get_mut(&target.part) {
                            rows.remove(key);
                        }
                    }
                    (DataFrameOperation::Add(..), None) => continue,
                }
                entry.ordered = None;
                entry.projected.clear();
                entry.generation += 1;
            }
        }
    }
}
