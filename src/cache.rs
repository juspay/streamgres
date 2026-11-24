//! The engine inside an application: reads answered from rows the engine
//! keeps current, by SQL and bind parameters.
//!
//! [`Streamgres`] is the library's one handle. [`Streamgres::load_and_sync`]
//! answers a statement from memory when the engine keeps its query, and
//! otherwise registers the query and serves this read from the fallback;
//! from then on the engine keeps the rows current from the change feed, and
//! later reads of the same query are answered from memory, until the query
//! has gone unread for its ttl ([`Ttl`]). A statement the engine cannot
//! answer exactly, or a read while the engine is down or behind, is
//! answered by the fallback ([`Fallback`]): Postgres itself, or an error.
//!
//! The cache talks to the engine through a private trait
//! ([`engine::Engine`]); the Postgres engine behind it is
//! `crate::sync::embed`.
//!
//! A cached answer reflects every transaction the engine has applied,
//! which trails a commit by the feed's lag; the engine answers only while
//! that lag is within the configured bound.

mod api;
mod config;
mod engine;
mod inspect;
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

pub use crate::sync::pg::connections::{
    Bind, Connection, ConnectionFuture, Connections, Lease, RawColumn, RawRows, Session,
    SessionError,
};
pub use api::{
    Builder, Error, Fallback, Inspecting, LoadAndSync, OnUnsupported, Param, Sql, Streamgres,
};
pub(crate) use config::CacheConfig;
pub use config::Ttl;
pub(crate) use engine::{Engine, EngineError, Instance, SendFuture, Serve, TypeOid};
pub use inspect::{Caller, EngineState, Inspect, KeptQuery, QueryId, QueryState, Served, Trace};
pub use metrics::Metrics;
pub(crate) use reader::{CatalogVersion, Reader, ReaderCheck};
pub(crate) use rows::CachedRows;
pub use rows::{Column, Row, Rows};

use crate::parser::{Projection, Select};
use crate::sync::Command;
use metrics::Counters;
use state::{Fill, Kept, Phase, Shared};

/// Why a read was not answered from the cache. Whichever it is, the
/// fallback answers it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The engine can never answer this statement exactly: its SQL, a
    /// table or column, or a type it does not hold exactly. Asking again
    /// will not change that while the schema stays as it is, so it is an
    /// error unless the application asked for these to fall back.
    Unsupported(String),
    /// The engine could answer the statement but not this time: a budget
    /// was exceeded, the engine refused the query while running it, or a
    /// value cannot be encoded yet. The fallback answers.
    Unavailable(String),
    /// The engine cannot answer now: it is starting, restarting, behind
    /// the database, or the query's rows are not complete.
    NotReady(String),
    /// The query's first rows have not all arrived yet: the read that
    /// registered it, and every read until they have, goes to the
    /// fallback.
    Filling,
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::Unsupported(reason) => write!(f, "not supported by Streamgres: {reason}"),
            Refusal::Unavailable(reason) => write!(f, "not served from memory: {reason}"),
            Refusal::NotReady(reason) => write!(f, "the cache cannot answer now: {reason}"),
            Refusal::Filling => write!(f, "the query is loading into memory"),
        }
    }
}

impl std::error::Error for Refusal {}

/// An engine embedded in this process and the rows it keeps. Dropping it
/// stops the engine.
pub(crate) struct Cache {
    shared: Arc<Shared>,
    supervisor: Mutex<Option<JoinHandle<()>>>,
    failures: Mutex<Vec<FailedRead>>,
}

/// A failed result belongs to one statement, catalog and row generation.
/// The bounded memo never prevents a sibling projection from being read.
struct FailedRead {
    sql: String,
    binds: Vec<Bind>,
    version: CatalogVersion,
    stamp: (u64, u64),
    at: Instant,
    reason: String,
    bytes: usize,
}

/// What a look at a query's entry found.
enum Found {
    Answer(CachedRows),
    /// The rows changed since they were last ordered: compose them from
    /// this snapshot.
    Compose {
        id: u64,
        generation: u64,
        ordered: Option<Arc<[crate::compose::Joined]>>,
        snapshot: Option<crate::compose::Snapshot>,
    },
    /// The first rows have not all arrived.
    Filling,
    /// No entry: the query is not kept.
    Absent,
}

impl Cache {
    /// Starts the engine in the background and returns at once; reads
    /// are refused as not ready until it is up ([`Cache::wait_ready`]).
    pub fn start(config: CacheConfig, engine: Arc<dyn Engine>) -> Self {
        let shared = Arc::new(Shared::new(config, engine));
        let supervisor = engine::spawn(shared.clone());
        Cache {
            shared,
            supervisor: Mutex::new(supervisor),
            failures: Mutex::new(Vec::new()),
        }
    }

    /// Waits until the engine is up and caught up with the database, for
    /// the deadline, or until the engine identifies invalid setup.
    pub fn wait_ready(&self, deadline: Instant) -> Result<(), Error> {
        loop {
            let seen = self.shared.epoch();
            let refusal = {
                let state = self.shared.read();
                if let Phase::Down(EngineError::Setup(reason)) = &state.phase {
                    return Err(Error::Setup(reason.clone()));
                }
                match state.readiness(&self.shared.config) {
                    Ok(()) => return Ok(()),
                    Err(refusal) => refusal,
                }
            };
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                return Err(Error::NotReady(refusal.to_string()));
            };
            self.shared.wait(seen, left.min(Duration::from_millis(100)));
        }
    }

    /// The query that checks the origin connection against the catalog
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

    /// The engine the cache runs.
    pub fn engine(&self) -> &Arc<dyn Engine> {
        &self.shared.engine
    }

    /// Counts a read the fallback answered, or failed to.
    pub fn count_fallback(&self, answered: bool) {
        let counters = &self.shared.counters;
        Counters::bump(if answered {
            &counters.fallbacks
        } else {
            &counters.fallback_errors
        });
    }

    /// The rows `sql` with `binds` returns to `reader`, as Postgres would
    /// return them at the position the engine has applied, its query kept
    /// for `ttl` (the configured one when `None`) after this read. A read
    /// of a query the engine does not keep yet registers it, and every
    /// read until its first rows have all arrived is refused as
    /// [`Refusal::Filling`]: it never waits.
    fn read(
        &self,
        sql: &str,
        binds: &[Bind],
        reader: &Reader,
        ttl: Option<Ttl>,
    ) -> Result<CachedRows, Refusal> {
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
        if let Some(rows) = local::get(&local_key, self.shared.now(), ttl) {
            return Ok(rows);
        }
        let answer = self.answer(sql, binds, reader, ttl);
        if let Err(refusal) = &answer {
            self.count_refusal(refusal);
        }
        if let Ok(rows) = &answer {
            let state = self.shared.read();
            if let Some(expires) = state.answer_deadline(&self.shared.config)
                && revision
                    == self
                        .shared
                        .revision
                        .load(std::sync::atomic::Ordering::Acquire)
                && let Ok(catalog) = state.catalog()
                && let Ok(select) = self.shared.parsed.get_or_parse(
                    sql,
                    binds,
                    &CatalogVersion::of(&catalog),
                    &catalog,
                )
                && let Some(entry) = state
                    .entry_of(&select.query)
                    .and_then(|id| state.entries.get(&id))
            {
                let default_ttl = self.shared.config.ttl_for(None);
                local::keep(
                    local_key,
                    expires,
                    rows,
                    self.shared.config.max_entry_bytes / 16,
                    entry.usage.clone(),
                    default_ttl,
                );
            }
        }
        answer
    }

    fn count_refusal(&self, refusal: &Refusal) {
        let counters = &self.shared.counters;
        match refusal {
            Refusal::Unsupported(_) => Counters::bump(&counters.unsupported),
            Refusal::NotReady(_) | Refusal::Unavailable(_) => Counters::bump(&counters.not_ready),
            Refusal::Filling => {}
        }
    }

    /// Admit a memory answer only after its client encoding succeeds.
    fn encoded(
        &self,
        sql: &str,
        binds: &[Bind],
        reader: &Reader,
        ttl: Option<Ttl>,
    ) -> Result<Rows, Refusal> {
        if self.oversized_binds(binds) {
            let admitted = self
                .answer_catalog(reader)
                .and_then(|(catalog, _)| self.admit_oversized(sql, binds, &catalog, Some(reader)));
            let refusal = match admitted {
                Ok(()) => Self::bind_budget_refusal(),
                Err(refusal) => refusal,
            };
            self.count_refusal(&refusal);
            return Err(refusal);
        }
        let identity = {
            let state = self.shared.read();
            state.catalog().ok().and_then(|catalog| {
                let version = CatalogVersion::of(&catalog);
                let select = self
                    .shared
                    .parsed
                    .get_or_parse(sql, binds, &version, &catalog)
                    .ok()?;
                let id = state.entry_of(&select.query)?;
                let entry = state.entries.get(&id)?;
                entry.touch(self.shared.now(), self.shared.config.ttl_for(ttl));
                Some((version, (id, entry.generation)))
            })
        };
        if let Some((version, stamp)) = &identity {
            let mut failures = self.failures.lock().unwrap_or_else(|e| e.into_inner());
            failures.retain(|failure| failure.at.elapsed() < self.shared.config.refusal_ttl);
            if let Some(failure) = failures.iter().find(|failure| {
                failure.sql == sql
                    && failure.binds == binds
                    && failure.version == *version
                    && failure.stamp == *stamp
            }) {
                return Err(Refusal::Unavailable(failure.reason.clone()));
            }
        }
        let result = self
            .read(sql, binds, reader, ttl)
            .and_then(|rows| rows::encode(&rows));
        if result.is_ok() {
            Counters::bump(&self.shared.counters.answered);
        } else if let (Err(Refusal::Unavailable(reason)), Some((version, stamp))) =
            (&result, identity)
        {
            let bytes = sql
                .len()
                .saturating_add(reason.len())
                .saturating_add(256)
                .saturating_add(
                    binds
                        .iter()
                        .map(|b| 32 + b.bytes.as_ref().map_or(0, Vec::len))
                        .sum::<usize>(),
                );
            let budget = (self.shared.config.max_bytes / 64).min(1 << 20);
            if bytes <= budget {
                let mut failures = self.failures.lock().unwrap_or_else(|e| e.into_inner());
                while !failures.is_empty()
                    && (failures.len() >= self.shared.config.max_entries.clamp(1, 256)
                        || failures
                            .iter()
                            .map(|f| f.bytes)
                            .sum::<usize>()
                            .saturating_add(bytes)
                            > budget)
                {
                    failures.remove(0);
                }
                failures.push(FailedRead {
                    sql: sql.to_owned(),
                    binds: binds.to_vec(),
                    version,
                    stamp,
                    at: Instant::now(),
                    reason: reason.clone(),
                    bytes,
                });
            }
        }
        result
    }

    fn oversized_binds(&self, binds: &[Bind]) -> bool {
        let config = &self.shared.config;
        let limit = (config.max_entry_bytes.min(config.max_bytes) / 16).min(1 << 20);
        let mut left = limit;
        for bind in binds {
            let Some(remaining) = left.checked_sub(bind.bytes.as_ref().map_or(0, Vec::len)) else {
                return true;
            };
            left = remaining;
        }
        false
    }

    fn bind_budget_refusal() -> Refusal {
        Refusal::Unavailable("the bind values exceed the cache's evaluation budget".to_owned())
    }

    /// Classifies an oversized read using its shape, bind metadata, and
    /// reader permissions without decoding or hashing the payload.
    fn admit_oversized(
        &self,
        sql: &str,
        binds: &[Bind],
        catalog: &Arc<crate::model::Catalog>,
        reader: Option<&Reader>,
    ) -> Result<(), Refusal> {
        let shape = self
            .shared
            .parsed
            .shape_of(sql, &CatalogVersion::of(catalog), catalog)
            .map_err(|reason| Refusal::Unsupported(reason.to_string()))?;
        shape
            .check_types(binds)
            .map_err(|reason| Refusal::Unsupported(reason.to_string()))?;
        self.admit(shape.unbound(), catalog, reader).map(drop)
    }

    fn validate(
        &self,
        sql: &str,
        binds: &[Bind],
        catalog: &Arc<crate::model::Catalog>,
        reader: &Reader,
    ) -> Result<(), Refusal> {
        if self.oversized_binds(binds) {
            return self.admit_oversized(sql, binds, catalog, Some(reader));
        }
        let version = CatalogVersion::of(catalog);
        let select = self
            .shared
            .parsed
            .get_or_parse(sql, binds, &version, catalog)
            .map_err(|reason| Refusal::Unsupported(reason.to_string()))?;
        self.admit(&select, catalog, Some(reader)).map(drop)
    }

    /// The tables `select` reads, when the engine can answer it for
    /// `reader`: every table known and readable as the engine reads it,
    /// every returned column held exactly.
    fn admit<'c>(
        &self,
        select: &Select,
        catalog: &'c crate::model::Catalog,
        reader: Option<&Reader>,
    ) -> Result<Vec<&'c crate::model::DbTable>, Refusal> {
        let mut tables = Vec::with_capacity(select.nodes.len());
        for node in &select.nodes {
            let table = catalog
                .table(node.table.as_str())
                .ok_or_else(|| Refusal::Unsupported(format!("unknown table `{}`", node.table)))?;
            if let Some(reason) = reader.and_then(|reader| reader.refuses(&node.table)) {
                return Err(Refusal::Unsupported(reason));
            }
            tables.push(table);
        }
        rows::projected_columns(select, &tables)?;
        Ok(tables)
    }

    fn answer_catalog(
        &self,
        reader: &Reader,
    ) -> Result<(Arc<crate::model::Catalog>, CatalogVersion), Refusal> {
        let shared = &self.shared;
        let catalog = {
            let state = shared.read();
            state.readiness(&shared.config)?;
            state.catalog()?
        };
        let version = CatalogVersion::of(&catalog);
        if !reader.is_current(&version) {
            return Err(Refusal::NotReady(
                "the reader was checked against an older catalog".to_owned(),
            ));
        }
        Ok((catalog, version))
    }

    fn answer(
        &self,
        sql: &str,
        binds: &[Bind],
        reader: &Reader,
        ttl: Option<Ttl>,
    ) -> Result<CachedRows, Refusal> {
        let shared = &self.shared;
        let config = &shared.config;
        let (catalog, version) = self.answer_catalog(reader)?;
        let select = shared
            .parsed
            .get_or_parse(sql, binds, &version, &catalog)
            .map_err(|reason| Refusal::Unsupported(reason.to_string()))?;
        // A statement returning a column the engine does not hold exactly
        // is refused before its query is kept for nothing.
        let tables = self.admit(&select, &catalog, Some(reader))?;
        let ttl = config.ttl_for(ttl);

        let mut subscribed = false;
        loop {
            let found = {
                let state = shared.read();
                state.readiness(config)?;
                if let Some(reason) = state.refused(&select.query, config.refusal_ttl) {
                    return Err(Refusal::Unavailable(reason.to_string()));
                }
                self.look(&state, &select, &version, ttl)?
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
                        None => crate::compose::compose(
                            snapshot.unwrap_or_default(),
                            &select,
                            &tables,
                            config.max_entry_bytes.min(config.max_bytes),
                            config.max_rows_per_query,
                        )
                        .map_err(|error| Refusal::Unavailable(error.to_string()))?,
                    };
                    let projected = if select.projection == Projection::CountStar {
                        let count = i64::try_from(ordered.len()).map_err(|_| {
                            Refusal::Unavailable("the count is out of range".into())
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
                            plan: select.result_plan(),
                            version,
                            rows: projected,
                        },
                        config,
                    );
                    return Ok(answer);
                }
                Found::Filling => return Err(Refusal::Filling),
                Found::Absent if subscribed => {
                    return Err(Refusal::NotReady(
                        "the query was dropped while it filled".to_owned(),
                    ));
                }
                Found::Absent => {
                    self.subscribe(&select, ttl, sql)?;
                    subscribed = true;
                    Counters::bump(&shared.counters.filled);
                }
            }
        }
    }

    /// What the entry kept for `select`'s query holds for this statement.
    fn look(
        &self,
        state: &state::State,
        select: &Select,
        version: &CatalogVersion,
        ttl: Ttl,
    ) -> Result<Found, Refusal> {
        let Some(id) = state.entry_of(&select.query) else {
            return Ok(Found::Absent);
        };
        let Some(entry) = state.entries.get(&id) else {
            return Ok(Found::Absent);
        };
        entry.touch(self.shared.now(), ttl);
        match &entry.fill {
            Fill::Pending(_) => return Ok(Found::Filling),
            Fill::Settling(_) => {
                return Err(Refusal::NotReady(
                    "the query's rows are settling after a change".to_owned(),
                ));
            }
            Fill::Ready => {}
        }
        if select.projection == Projection::CountStar && select.nodes.len() == 1 {
            let count = entry
                .parts
                .get(&crate::ivm::QueryPart::main())
                .map_or(0, HashMap::len);
            return rows::count(count, select).map(Found::Answer);
        }
        if let Some(kept) = entry.projected.iter().find(|kept| {
            kept.projection == select.projection
                && kept.version == *version
                && kept.plan.matches(select)
        }) {
            return Ok(Found::Answer(rows::slice(&kept.rows, select)));
        }
        let ordered = entry
            .ordered
            .as_ref()
            .filter(|_| {
                entry
                    .ordered_by
                    .as_ref()
                    .is_some_and(|plan| plan.matches(select))
            })
            .cloned();
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

    /// Has the engine keep `select`'s query current, for `ttl` after its
    /// last read.
    fn subscribe(&self, select: &Select, ttl: Ttl, sql: &str) -> Result<(), Refusal> {
        let shared = &self.shared;
        let config = &shared.config;
        if state::entry_metadata_bytes(&select.query) > config.max_entry_bytes.min(config.max_bytes)
        {
            return Err(Refusal::Unavailable(
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
        let (id, evicted) = state.add_entry(
            select.query.clone(),
            config.max_entries,
            shared.now(),
            ttl,
            sql.into(),
        );
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

    /// Where `sql` with `binds` stands in the cache, read as `reader`
    /// would read it: nothing is registered or touched.
    pub fn describe(&self, sql: &str, binds: &[Bind], reader: Option<&Reader>) -> Described {
        let shared = &self.shared;
        let config = &shared.config;
        let state = shared.read();
        let now = shared.now();
        let not_kept = |state: QueryState| Described {
            state,
            rows: 0,
            bytes: 0,
            ttl: None,
            last_read: None,
            expires_in: None,
        };
        if let Err(refusal) = state.readiness(config) {
            return not_kept(QueryState::NotReady(refusal.to_string()));
        }
        let Ok(catalog) = state.catalog() else {
            return not_kept(QueryState::NotReady("the engine is not running".to_owned()));
        };
        if self.oversized_binds(binds) {
            return not_kept(match self.admit_oversized(sql, binds, &catalog, reader) {
                Ok(()) => QueryState::Refused(Self::bind_budget_refusal().to_string()),
                Err(refusal) => QueryState::Unsupported(refusal.to_string()),
            });
        }
        let version = CatalogVersion::of(&catalog);
        let select = match shared.parsed.get_or_parse(sql, binds, &version, &catalog) {
            Ok(select) => select,
            Err(reason) => return not_kept(QueryState::Unsupported(reason.to_string())),
        };
        if let Err(refusal) = self.admit(&select, &catalog, reader) {
            return not_kept(QueryState::Unsupported(refusal.to_string()));
        }
        if let Some(reason) = state.refused(&select.query, config.refusal_ttl) {
            return not_kept(QueryState::Refused(reason.to_string()));
        }
        let Some(entry) = state
            .entry_of(&select.query)
            .and_then(|id| state.entries.get(&id))
        else {
            return not_kept(QueryState::NotKept);
        };
        let ttl = entry.ttl();
        let idle = Duration::from_millis(entry.idle(now));
        Described {
            state: self
                .failures
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .find(|failure| {
                    failure.sql == sql
                        && failure.binds == binds
                        && failure.version == version
                        && failure.stamp
                            == (
                                state.entry_of(&select.query).unwrap_or(u64::MAX),
                                entry.generation,
                            )
                        && failure.at.elapsed() < config.refusal_ttl
                })
                .map_or_else(
                    || entry_state(entry),
                    |failure| QueryState::Refused(failure.reason.clone()),
                ),
            rows: entry.rows(),
            bytes: entry.bytes(),
            ttl: Some(ttl),
            last_read: Some(idle),
            expires_in: match ttl {
                Ttl::Forever => None,
                ttl => Some(Duration::from_millis(ttl.millis()).saturating_sub(idle)),
            },
        }
    }

    /// Every query kept now, the least recently read last.
    pub fn kept(&self) -> Vec<KeptQuery> {
        let now = self.shared.now();
        let state = self.shared.read();
        let mut kept: Vec<KeptQuery> = state
            .entries
            .values()
            .map(|entry| KeptQuery {
                sql: entry.sql.to_string(),
                state: entry_state(entry),
                rows: entry.rows(),
                bytes: entry.bytes(),
                ttl: entry.ttl(),
                last_read: Duration::from_millis(entry.idle(now)),
            })
            .collect();
        kept.sort_by_key(|query| query.last_read);
        kept
    }

    /// The engine as a whole, its position written as the engine writes it.
    pub fn engine_state(&self) -> EngineState {
        let state = self.shared.read();
        let running = matches!(state.phase, Phase::Running(_));
        let (queries, bytes) = state.held();
        EngineState {
            running,
            instance: match &state.phase {
                Phase::Running(running) => Some(running.name.clone()),
                _ => None,
            },
            position: running.then(|| self.shared.engine.position_text(state.position)),
            lag: (running && state.heard_once()).then(|| state.lag()),
            queries,
            bytes,
        }
    }

    /// What the cache has done since it started and what it holds now.
    pub fn metrics(&self) -> Metrics {
        let state = self.shared.read();
        let running = matches!(state.phase, Phase::Running(_));
        let lag = (running && state.heard_once()).then(|| state.lag());
        Metrics::of(&self.shared.counters, running, state.held(), lag)
    }

    /// Stops the engine, lets it clean up and waits for its threads.
    pub fn stop(&self) {
        self.shared.stop();
        // Keep the completion lock until the join returns. Every caller
        // observes completed cleanup, including callers racing the first.
        let mut supervisor = self
            .supervisor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(thread) = supervisor.take() {
            let _ = thread.join();
        }
    }
}

/// Where a statement's query stands in the cache ([`Cache::describe`]).
pub(crate) struct Described {
    pub state: QueryState,
    pub rows: usize,
    pub bytes: usize,
    pub ttl: Option<Ttl>,
    pub last_read: Option<Duration>,
    pub expires_in: Option<Duration>,
}

/// An entry's fill as inspection reports it.
fn entry_state(entry: &state::Entry) -> QueryState {
    match &entry.fill {
        Fill::Pending(since) => QueryState::Loading(since.elapsed()),
        Fill::Ready => QueryState::Ready,
        Fill::Settling(since) => QueryState::Settling(since.elapsed()),
    }
}

impl Drop for Cache {
    fn drop(&mut self) {
        // The supervisor owns Shared until cleanup finishes. Dropping its
        // JoinHandle detaches the wait, not the work or its resources.
        self.shared.stop();
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn loading_and_settling_reads_extend_retention() {
        use crate::model::{Catalog, DbColumn, DbTable, ValueType};
        let engine = Arc::new(engine::tests::Scripted(|_: &crate::shutdown::Shutdown| {
            Err("unused".to_owned())
        }));
        let shared = Arc::new(Shared::new(CacheConfig::default(), engine));
        let cache = Cache {
            shared,
            supervisor: Mutex::new(None),
            failures: Mutex::new(Vec::new()),
        };
        let catalog = Arc::new(Catalog::new(vec![DbTable::new(
            "t",
            ["id"],
            vec![DbColumn::new("id", ValueType::Int)],
        )]));
        let version = CatalogVersion::of(&catalog);
        let select = cache
            .shared
            .parsed
            .get_or_parse("SELECT id FROM t", &[], &version, &catalog)
            .unwrap();
        let mut state = cache.shared.write();
        let (id, _) = state.add_entry(
            select.query.clone(),
            10,
            0,
            Ttl::Immediate,
            "SELECT id FROM t".into(),
        );
        assert!(matches!(
            cache.look(&state, &select, &version, Ttl::Forever).unwrap(),
            Found::Filling
        ));
        assert_eq!(state.entries[&id].ttl(), Ttl::Forever);
        state.entries.get_mut(&id).unwrap().usage = Arc::new(state::Usage::new(0, Ttl::Immediate));
        state.entries.get_mut(&id).unwrap().fill = Fill::Settling(Instant::now());
        assert!(matches!(
            cache.look(&state, &select, &version, Ttl::Forever),
            Err(Refusal::NotReady(_))
        ));
        assert_eq!(state.entries[&id].ttl(), Ttl::Forever);
    }

    #[test]
    fn concurrent_stop_waits_for_the_same_cleanup() {
        let (started, running) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        let wait = Mutex::new(wait);
        let engine = Arc::new(engine::tests::Scripted(move |_| {
            started.send(()).unwrap();
            wait.lock().unwrap().recv().unwrap();
            Err("test stopped".to_owned())
        }));
        let cache = Arc::new(Cache::start(CacheConfig::default(), engine));
        running.recv_timeout(Duration::from_secs(5)).unwrap();
        let first = cache.clone();
        let first = std::thread::spawn(move || first.stop());
        let deadline = Instant::now() + Duration::from_secs(5);
        while cache.supervisor.try_lock().is_ok() {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        let second = cache.clone();
        let (done, observed) = mpsc::channel();
        let second = std::thread::spawn(move || {
            second.stop();
            done.send(()).unwrap();
        });
        let premature = observed.recv_timeout(Duration::from_millis(100));
        release.send(()).unwrap();
        first.join().unwrap();
        second.join().unwrap();
        assert!(
            premature.is_err(),
            "a concurrent stop returned before cleanup"
        );
    }

    #[test]
    fn dropping_cache_does_not_wait_for_supervisor_cleanup() {
        let (started, running) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        let wait = Mutex::new(wait);
        let engine = Arc::new(engine::tests::Scripted(move |_| {
            started.send(()).unwrap();
            wait.lock().unwrap().recv().unwrap();
            Err("test stopped".to_owned())
        }));
        let cache = Cache::start(CacheConfig::default(), engine);
        running.recv_timeout(Duration::from_secs(5)).unwrap();
        let shared = cache.shared.clone();
        let (dropped, observed) = mpsc::channel();
        let dropper = std::thread::spawn(move || {
            drop(cache);
            dropped.send(()).unwrap();
        });
        let returned = observed.recv_timeout(Duration::from_secs(1));
        // Release even on failure so the regression cannot strand a thread.
        release.send(()).unwrap();
        dropper.join().unwrap();
        assert!(returned.is_ok(), "Drop waited for the blocked supervisor");
        assert!(shared.stopping.requested());
    }
}
