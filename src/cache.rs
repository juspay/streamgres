//! The engine inside an application: reads answered from rows the engine
//! keeps current, by SQL and bind parameters.
//!
//! [`Cache::start`] brings an engine up in the background, on threads of
//! its own, and keeps it up: when one fails it is torn down and a fresh
//! one started, waiting longer each time it fails again soon. A read
//! ([`Cache::read`]) is parsed against the engine's catalog; the first
//! read of a query subscribes it and waits for its rows, and later reads
//! of the same query are answered from the rows the engine has kept
//! current since. A query the engine cannot answer exactly, or a read
//! while the engine is not caught up, is refused ([`Refusal`]), and the
//! caller reads Postgres instead.
//!
//! A cached answer reflects every transaction the engine has applied,
//! which trails a commit by the feed's lag. A caller that must see its
//! own write waits for it first ([`Cache::wait_for`]).

mod config;
mod engine;
mod rows;
mod state;

use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub use config::CacheConfig;
pub use rows::{CachedColumn, CachedRows};

pub use crate::model::Lsn;
use crate::model::MultiTableReadQuery;
pub use crate::parser::Bind;
use crate::parser::parse_select;
use crate::sync::Command;
use state::{Fill, Phase, Shared};

/// Why a read was not answered from the cache. Either way the caller
/// reads Postgres instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// The engine cannot answer this query exactly; asking again will
    /// not change that while the schema stays as it is.
    Unsupported(String),
    /// The engine cannot answer now: it is starting, restarting, behind
    /// the database, or still filling the query.
    NotReady(String),
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::Unsupported(reason) => write!(f, "not served from the cache: {reason}"),
            Refusal::NotReady(reason) => write!(f, "the cache cannot answer now: {reason}"),
        }
    }
}

impl std::error::Error for Refusal {}

/// An engine embedded in this process and the rows it keeps. Cheap to
/// share behind an `Arc`; dropping the last handle stops the engine.
pub struct Cache {
    shared: Arc<Shared>,
    supervisor: Mutex<Option<JoinHandle<()>>>,
}

impl Cache {
    /// Starts the engine in the background and returns at once; reads
    /// are refused as not ready until it is up ([`Cache::wait_ready`]).
    pub fn start(config: CacheConfig) -> Self {
        let shared = Arc::new(Shared::new(config));
        let supervisor = engine::spawn(shared.clone());
        Cache {
            shared,
            supervisor: Mutex::new(supervisor),
        }
    }

    /// Waits until the engine is up and caught up with the database, for
    /// at most `timeout`; the reason it is not otherwise.
    pub fn wait_ready(&self, timeout: Duration) -> Result<(), Refusal> {
        let deadline = Instant::now() + timeout;
        let mut state = self.shared.lock();
        loop {
            match state.readiness(&self.shared.config) {
                Ok(()) => return Ok(()),
                Err(refusal) => {
                    let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                        return Err(refusal);
                    };
                    state = self
                        .shared
                        .wait(state, left.min(Duration::from_millis(100)));
                }
            }
        }
    }

    /// The rows `sql` with `binds` returns, as Postgres would return them
    /// at the position the engine has applied.
    pub fn read(&self, sql: &str, binds: &[Bind]) -> Result<CachedRows, Refusal> {
        let config = &self.shared.config;
        let deadline = Instant::now() + config.fill_timeout;
        let catalog = {
            let state = self.shared.lock();
            state.readiness(config)?;
            state.catalog()?
        };
        let select = parse_select(sql, binds, &catalog)
            .map_err(|error| Refusal::Unsupported(error.to_string()))?;
        let table = catalog.table(select.query.table.as_str()).ok_or_else(|| {
            Refusal::Unsupported(format!("unknown table `{}`", select.query.table))
        })?;
        let query = MultiTableReadQuery::single(select.query.clone());

        let mut state = self.shared.lock();
        if let Some(reason) = state.refused(&query, config.refusal_ttl) {
            return Err(Refusal::Unsupported(reason));
        }
        let id = match state.entry_of(&query) {
            Some(id) => id,
            None => {
                let (id, evicted) = state.add_entry(query.clone(), config.max_entries);
                let commands = state.commands()?;
                for sub in evicted {
                    let _ = commands.try_send(Command::Unregister(sub));
                }
                let register = Command::Register {
                    sink: 0,
                    query,
                    token: id,
                };
                if commands.try_send(register).is_err() {
                    state.remove_entry(id);
                    return Err(Refusal::NotReady("the engine is busy".to_owned()));
                }
                id
            }
        };
        loop {
            state.readiness(config)?;
            let Some(entry) = state.entries.get_mut(&id) else {
                return Err(Refusal::NotReady(
                    "the query was dropped while it filled".to_owned(),
                ));
            };
            match &entry.fill {
                Fill::Ready => {
                    entry.last_read = Instant::now();
                    let count = entry.rows.len();
                    if let Some(ordered) = entry.ordered.clone() {
                        drop(state);
                        return rows::answer(&ordered, count, &select, table);
                    }
                    if select.projection == crate::parser::Projection::CountStar {
                        return rows::answer(&[], count, &select, table);
                    }
                    let generation = entry.generation;
                    let snapshot: Vec<_> = entry.rows.values().cloned().collect();
                    drop(state);
                    let ordered = rows::ordered(snapshot, &select.query.order_by, table);
                    let mut state = self.shared.lock();
                    if let Some(entry) = state.entries.get_mut(&id)
                        && entry.generation == generation
                    {
                        entry.ordered = Some(ordered.clone());
                    }
                    drop(state);
                    return rows::answer(&ordered, count, &select, table);
                }
                Fill::Refused(reason) => return Err(Refusal::Unsupported(reason.clone())),
                Fill::Pending => {}
            }
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                return Err(Refusal::NotReady(format!(
                    "the query's rows did not arrive within {:?}",
                    config.fill_timeout
                )));
            };
            state = self.shared.wait(state, left);
        }
    }

    /// Waits until the engine has applied every transaction committed at
    /// or before `position` (a `pg_current_wal_lsn()` read after a write),
    /// for at most `timeout`; whether it has.
    pub fn wait_for(&self, position: Lsn, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.shared.lock();
        loop {
            if matches!(state.phase, Phase::Running(_)) && state.position >= position {
                return true;
            }
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            state = self.shared.wait(state, left);
        }
    }

    /// The position the engine has applied every transaction up to;
    /// `None` while it is not running.
    pub fn position(&self) -> Option<Lsn> {
        let state = self.shared.lock();
        matches!(state.phase, Phase::Running(_)).then_some(state.position)
    }

    /// The replication slot the running engine streams from.
    pub fn slot(&self) -> Option<String> {
        match &self.shared.lock().phase {
            Phase::Running(running) => Some(running.slot.clone()),
            _ => None,
        }
    }

    /// Stops the engine, drops its slot and waits for its threads.
    pub fn stop(&self) {
        self.shared.stop();
        let supervisor = self
            .supervisor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(supervisor) = supervisor {
            let _ = supervisor.join();
        }
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        self.stop();
    }
}
