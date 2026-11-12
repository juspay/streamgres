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
//! A read is made for a [`Reader`]: the reader's connection, checked by
//! one query ([`Cache::reader_check`]) for the tables it would see
//! otherwise than the engine (another table under its search path, one it
//! may not read). A read of such a table is refused.
//!
//! A cached answer reflects every transaction the engine has applied,
//! which trails a commit by the feed's lag; a read is refused while that
//! lag is past [`CacheConfig::max_lag`]. A caller that must see its own
//! write waits for it first ([`Cache::wait_for`]).
//!
//! What the cache keeps is bounded ([`CacheConfig::max_entries`],
//! [`CacheConfig::max_bytes`]), and what it has done is counted
//! ([`Cache::metrics`]).

mod config;
mod engine;
mod local;
mod metrics;
mod parse;
mod reader;
mod rows;
mod state;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub use config::CacheConfig;
pub use metrics::Metrics;
pub use reader::{CatalogVersion, Reader, ReaderCheck};
pub use rows::{CachedColumn, CachedRows};

/// An opaque point applied by this cache. Obtain one from `position()` or
/// a PostgreSQL WAL location; callers compare and wait, never edit its parts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position(crate::model::Position);
impl From<crate::sync::pg::Lsn> for Position {
    fn from(lsn: crate::sync::pg::Lsn) -> Self {
        Self(lsn.into())
    }
}
impl std::fmt::Display for Position {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
pub use crate::parser::Bind;
use crate::parser::{Projection, Select};
use crate::sync::Command;
use metrics::Counters;
use state::{Fill, Kept, Phase, Shared};

/// Why a read was not answered from the cache. Whichever it is, the
/// caller reads Postgres instead.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Refusal {
    /// The engine cannot answer this query exactly; asking again will
    /// not change that while the schema stays as it is.
    Unsupported(String),
    /// The engine cannot answer now: it is starting, restarting, behind
    /// the database, or the query's rows are not complete.
    NotReady(String),
    /// The query has been read `seen` times, and is kept current only
    /// from its `needed`th read on ([`CacheConfig::admit_after`]).
    NotAdmitted { seen: u32, needed: u32 },
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::Unsupported(reason) => write!(f, "not served from the cache: {reason}"),
            Refusal::NotReady(reason) => write!(f, "the cache cannot answer now: {reason}"),
            Refusal::NotAdmitted { seen, needed } => write!(
                f,
                "the query is not kept yet: read {seen} of the {needed} times it takes"
            ),
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

/// What a look at a query's entry found.
enum Found {
    Answer(CachedRows),
    /// The rows changed since they were last ordered: compose them from
    /// this snapshot.
    Compose {
        id: u64,
        generation: u64,
        ordered: Option<Arc<[rows::Joined]>>,
        snapshot: Option<rows::Snapshot>,
    },
    /// The first rows have not all arrived.
    Filling,
    /// No entry: the query is not kept.
    Absent,
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
        loop {
            let seen = self.shared.epoch();
            let Err(refusal) = self.shared.read().readiness(&self.shared.config) else {
                return Ok(());
            };
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                return Err(refusal);
            };
            self.shared.wait(seen, left.min(Duration::from_millis(100)));
        }
    }

    /// The query that checks a reader's connection against the catalog
    /// the engine serves by now.
    pub fn reader_check(&self) -> Result<ReaderCheck, Refusal> {
        Ok(ReaderCheck::of(&self.shared.read().catalog()?))
    }

    /// The catalog the engine serves by now: a [`Reader`] checked against
    /// another must be checked again.
    pub fn catalog_version(&self) -> Option<CatalogVersion> {
        self.shared
            .read()
            .catalog()
            .ok()
            .map(|catalog| CatalogVersion::of(&catalog))
    }

    /// The rows `sql` with `binds` returns to `reader`, as Postgres would
    /// return them at the position the engine has applied.
    pub fn read(&self, sql: &str, binds: &[Bind], reader: &Reader) -> Result<CachedRows, Refusal> {
        let revision = self
            .shared
            .revision
            .load(std::sync::atomic::Ordering::Acquire);
        let local_key = local::Key {
            identity: &self.shared.identity,
            revision,
            sql,
            binds,
            reader,
        };
        if let Some(rows) = local::get(&local_key) {
            Counters::bump(&self.shared.counters.answered);
            return Ok(rows);
        }
        let counters = &self.shared.counters;
        let answer = self.answer(sql, binds, reader);
        Counters::bump(match &answer {
            Ok(_) => &counters.answered,
            Err(Refusal::Unsupported(_)) => &counters.unsupported,
            Err(Refusal::NotReady(_)) => &counters.not_ready,
            Err(Refusal::NotAdmitted { .. }) => &counters.not_admitted,
        });
        if let Ok(rows) = &answer {
            let expires = self.shared.read().answer_deadline(&self.shared.config);
            if let Some(expires) = expires
                && revision
                    == self
                        .shared
                        .revision
                        .load(std::sync::atomic::Ordering::Acquire)
            {
                local::keep(
                    local_key,
                    expires,
                    rows,
                    self.shared.config.max_entry_bytes / 16,
                );
            }
        }
        answer
    }

    fn answer(&self, sql: &str, binds: &[Bind], reader: &Reader) -> Result<CachedRows, Refusal> {
        let shared = &self.shared;
        let config = &shared.config;
        let bind_bytes = binds.iter().fold(0usize, |bytes, bind| {
            bytes.saturating_add(bind.bytes.as_ref().map_or(0, Vec::len))
        });
        if bind_bytes > (config.max_entry_bytes.min(config.max_bytes) / 16).min(1 << 20) {
            return Err(Refusal::Unsupported(
                "the bind values exceed the cache's evaluation budget".to_owned(),
            ));
        }
        let deadline = Instant::now() + config.fill_timeout;
        let catalog = {
            let state = shared.read();
            state.readiness(config)?;
            if let Some(reason) = reader.elsewhere(&state.running()?.cluster, &shared.database) {
                return Err(Refusal::Unsupported(reason));
            }
            state.catalog()?
        };
        let version = CatalogVersion::of(&catalog);
        if !reader.is_current(&version) {
            return Err(Refusal::NotReady(
                "the reader was checked against an older catalog".to_owned(),
            ));
        }
        let select = shared
            .parsed
            .get_or_parse(sql, binds, &version, &catalog)
            .map_err(|reason| Refusal::Unsupported(reason.to_string()))?;
        let mut tables = Vec::with_capacity(select.nodes.len());
        for node in &select.nodes {
            let table = catalog
                .table(node.table.as_str())
                .ok_or_else(|| Refusal::Unsupported(format!("unknown table `{}`", node.table)))?;
            if let Some(reason) = reader.refuses(&node.table) {
                return Err(Refusal::Unsupported(reason));
            }
            tables.push(table);
        }

        let mut subscribed = false;
        loop {
            let seen = shared.epoch();
            let found = {
                let state = shared.read();
                state.readiness(config)?;
                if let Some(reason) = state.refused(&select.query, config.refusal_ttl) {
                    return Err(Refusal::Unsupported(reason.to_string()));
                }
                self.look(&state, &select, &version)?
            };
            match found {
                Found::Answer(rows) => return Ok(rows),
                Found::Compose {
                    id,
                    generation,
                    ordered,
                    snapshot,
                } => {
                    let ordered = match ordered {
                        Some(ordered) => ordered,
                        None => rows::compose(
                            snapshot.unwrap_or_default(),
                            &select,
                            &tables,
                            config.max_entry_bytes.min(config.max_bytes),
                            config.max_rows_per_query,
                        )?,
                    };
                    let projected = if select.projection == Projection::CountStar {
                        let count = i64::try_from(ordered.len()).map_err(|_| {
                            Refusal::Unsupported("the count is out of range".into())
                        })?;
                        Arc::new(rows::project_count(count))
                    } else {
                        Arc::new(rows::project(
                            &ordered,
                            &select,
                            &tables,
                            config.max_entry_bytes.min(config.max_bytes),
                        )?)
                    };
                    let answer = rows::slice(&projected, &select);
                    shared.write().keep_projection(
                        id,
                        generation,
                        ordered,
                        Kept {
                            projection: select.projection.clone(),
                            version,
                            rows: projected,
                        },
                        config,
                    );
                    return Ok(answer);
                }
                Found::Filling => {}
                Found::Absent if subscribed => {
                    return Err(Refusal::NotReady(
                        "the query was dropped while it filled".to_owned(),
                    ));
                }
                Found::Absent => {
                    self.subscribe(&select)?;
                    subscribed = true;
                    Counters::bump(&shared.counters.filled);
                    continue;
                }
            }
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                return Err(Refusal::NotReady(format!(
                    "the query's rows did not arrive within {:?}",
                    config.fill_timeout
                )));
            };
            shared.wait(seen, left);
        }
    }

    /// What the entry kept for `select`'s query holds for this statement.
    fn look(
        &self,
        state: &state::State,
        select: &Select,
        version: &CatalogVersion,
    ) -> Result<Found, Refusal> {
        let Some(id) = state.entry_of(&select.query) else {
            return Ok(Found::Absent);
        };
        let Some(entry) = state.entries.get(&id) else {
            return Ok(Found::Absent);
        };
        match &entry.fill {
            Fill::Pending(_) => return Ok(Found::Filling),
            Fill::Settling(_) => {
                return Err(Refusal::NotReady(
                    "the query's rows are settling after a change".to_owned(),
                ));
            }
            Fill::Ready => {}
        }
        entry.touch(self.shared.now());
        if select.projection == Projection::CountStar && select.nodes.len() == 1 {
            let count = entry
                .parts
                .get(&crate::ivm::QueryPart::main())
                .map_or(0, HashMap::len);
            return rows::count(count, select).map(Found::Answer);
        }
        if let Some(kept) = entry
            .projected
            .iter()
            .find(|kept| kept.projection == select.projection && kept.version == *version)
        {
            return Ok(Found::Answer(rows::slice(&kept.rows, select)));
        }
        let ordered = entry.ordered.clone();
        let snapshot = ordered.is_none().then(|| {
            select
                .nodes
                .iter()
                .map(|node| {
                    entry
                        .parts
                        .get(&node.part)
                        .map(|rows| rows.values().cloned().collect())
                        .unwrap_or_default()
                })
                .collect()
        });
        Ok(Found::Compose {
            id,
            generation: entry.generation,
            ordered,
            snapshot,
        })
    }

    /// Has the engine keep `select`'s query current, once it has been
    /// read often enough to be admitted.
    fn subscribe(&self, select: &Select) -> Result<(), Refusal> {
        let shared = &self.shared;
        let config = &shared.config;
        let needed = config.admit_after.max(1);
        let hash = state::query_hash(&select.query);
        if needed > 1 {
            let mut sightings = shared
                .sightings
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let seen = sightings.sighted(hash, config.max_entries.saturating_mul(4));
            if seen < needed {
                return Err(Refusal::NotAdmitted { seen, needed });
            }
            sightings.admitted(hash);
        }
        if state::entry_metadata_bytes(&select.query) > config.max_entry_bytes.min(config.max_bytes)
        {
            return Err(Refusal::Unsupported(
                "the query metadata exceeds the cache's byte budget".to_owned(),
            ));
        }
        let mut state = shared.write();
        if state.entry_of(&select.query).is_some() {
            return Ok(());
        }
        let (commands, unsubscribe) = {
            let running = state.running()?;
            (running.commands.clone(), running.unsubscribe.clone())
        };
        let (id, evicted) = state.add_entry(select.query.clone(), config.max_entries, shared.now());
        for sub in evicted
            .into_iter()
            .chain(state.keep_within(config, &shared.counters))
        {
            let _ = unsubscribe.send(sub);
        }
        let register = Command::Register {
            sink: 0,
            query: select.query.clone(),
            token: id,
        };
        if commands.try_send(register).is_err() {
            state.remove_entry(id);
            return Err(Refusal::NotReady("the engine is busy".to_owned()));
        }
        Ok(())
    }

    /// Waits until the engine has applied every transaction committed at
    /// or before `position` (a `pg_current_wal_lsn()` read after a write),
    /// for at most `timeout`; whether it has.
    pub fn wait_for(&self, position: Position, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            let seen = self.shared.epoch();
            {
                let state = self.shared.read();
                if matches!(state.phase, Phase::Running(_)) && state.position >= position.0 {
                    return true;
                }
            }
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            self.shared.wait(seen, left);
        }
    }

    /// The position the engine has applied every transaction up to;
    /// `None` while it is not running.
    pub fn position(&self) -> Option<Position> {
        let state = self.shared.read();
        matches!(state.phase, Phase::Running(_)).then_some(Position(state.position))
    }

    /// The replication slot the running engine streams from.
    pub fn slot(&self) -> Option<String> {
        match &self.shared.read().phase {
            Phase::Running(running) => Some(running.slot.clone()),
            _ => None,
        }
    }

    /// What the cache has done since it started and what it holds now.
    pub fn metrics(&self) -> Metrics {
        let state = self.shared.read();
        let running = matches!(state.phase, Phase::Running(_));
        let lag = (running && state.heard_once()).then(|| state.lag());
        Metrics::of(&self.shared.counters, running, state.held(), lag)
    }

    /// Records a refusal detected by an adapter before it called `read`,
    /// so transactions and untracked connections are visible in metrics too.
    pub fn report_refusal(&self, reason: &Refusal) {
        let counters = &self.shared.counters;
        Counters::bump(match reason {
            Refusal::Unsupported(_) => &counters.unsupported,
            Refusal::NotReady(_) => &counters.not_ready,
            Refusal::NotAdmitted { .. } => &counters.not_admitted,
        });
    }

    /// Counts an answer its caller could not use (an adapter that could
    /// not hand a value to its application), so the fallback shows in
    /// [`Cache::metrics`].
    pub fn report_unusable(&self) {
        Counters::bump(&self.shared.counters.unusable);
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
