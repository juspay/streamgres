//! What Streamgres can say about a read, for debugging.
//!
//! - [`Trace`]: how one read went: who answered it, why not memory (the
//!   engine refused it, or it was loading), how long it took, where in
//!   the code it was made
//!   ([`super::LoadAndSync::inspect`]).
//! - [`Inspect`]: where one statement's query stands now: kept or not, its
//!   rows, its ttl, when it was last read, and (with
//!   [`super::Builder::debug_history`]) who reads it and how its last
//!   reads went ([`super::Streamgres::inspect`]).
//! - [`KeptQuery`]: every query kept now ([`super::Streamgres::queries`]).
//!
//! None of it changes what a read does. Recording reads for
//! [`Inspect::callers`] and [`Inspect::recent`] is off unless asked for,
//! since it takes a lock per read.

use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::panic::Location;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::config::Ttl;
use crate::parser::Bind;

/// A statement and its bind values, as one number: the same text with
/// the same values is the same query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QueryId(pub u64);

impl QueryId {
    pub(crate) fn of(sql: &str, binds: &[Bind]) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        sql.hash(&mut hasher);
        binds.hash(&mut hasher);
        QueryId(hasher.finish())
    }
}

impl std::fmt::Display for QueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Who answered a read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Served {
    /// Memory: the query was kept and its rows ready.
    Memory,
    /// Postgres, through the fallback: the engine could not answer, or
    /// the query was still loading (the first read of a query always is).
    Postgres,
    /// Nobody: Streamgres can never answer the statement, and it is an
    /// error ([`super::OnUnsupported::Error`]).
    Unsupported,
    /// Nobody: the read failed.
    Failed,
}

impl std::fmt::Display for Served {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Served::Memory => "memory",
            Served::Postgres => "postgres",
            Served::Unsupported => "unsupported",
            Served::Failed => "failed",
        })
    }
}

/// How one read went.
#[derive(Debug, Clone)]
pub struct Trace {
    pub query: QueryId,
    pub sql: String,
    pub served: Served,
    /// Why the engine did not answer, when it did not.
    pub reason: Option<String>,
    /// The ttl the query is kept for after this read, when it is kept.
    pub ttl: Option<Ttl>,
    /// How many rows came back.
    pub rows: usize,
    /// From the call to the rows (or the error).
    pub took: Duration,
    /// Where the engine was in the source's history when it answered,
    /// as the source writes it (`0/1A2B3C` for Postgres).
    pub position: Option<String>,
    /// How stale an answer from memory may be, at the time of the read.
    pub lag: Option<Duration>,
    /// Where in the application the read was made.
    pub at: &'static Location<'static>,
    /// When it was made.
    pub when: Instant,
}

impl std::fmt::Display for Trace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "query {} at {}: {} in {:?}, {} rows",
            self.query, self.at, self.served, self.took, self.rows
        )?;
        if let Some(reason) = &self.reason {
            write!(f, " ({reason})")?;
        }
        Ok(())
    }
}

/// Where a statement's query stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryState {
    /// Not kept: its next read loads it.
    NotKept,
    /// Registered; its first rows have been arriving for this long.
    Loading(Duration),
    /// Kept, and its rows ready.
    Ready,
    /// Kept; a change left its rows incomplete for this long, until a
    /// read of the database lands.
    Settling(Duration),
    /// The engine refused it lately, and reads go to the fallback.
    Refused(String),
    /// The engine cannot answer the statement at all.
    Unsupported(String),
    /// The engine is not ready (starting, restarting, behind).
    NotReady(String),
}

/// The engine as a whole.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineState {
    pub running: bool,
    /// What the engine calls the running instance: for Postgres, the name
    /// of its replication slot.
    pub instance: Option<String>,
    /// Where the engine has applied every transaction up to, as the source
    /// writes it.
    pub position: Option<String>,
    pub lag: Option<Duration>,
    /// Queries kept, and what their rows weigh.
    pub queries: usize,
    pub bytes: usize,
}

/// One place in the application that reads a query, and how its reads
/// went.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    pub at: &'static Location<'static>,
    pub reads: u64,
    pub memory: u64,
    pub postgres: u64,
    pub failed: u64,
    /// How long ago its last read was.
    pub last: Duration,
}

/// Where one statement's query stands, and what is known of its reads.
#[derive(Debug, Clone)]
pub struct Inspect {
    pub query: QueryId,
    pub sql: String,
    pub state: QueryState,
    /// The rows the engine keeps for the query, across its tables.
    pub rows: usize,
    /// What they weigh, as the budget counts them.
    pub bytes: usize,
    /// How long after its last read it is kept.
    pub ttl: Option<Ttl>,
    /// How long ago it was last read from memory.
    pub last_read: Option<Duration>,
    /// When it will be freed if nobody reads it.
    pub expires_in: Option<Duration>,
    pub engine: EngineState,
    /// Each place that read it, with [`super::Builder::debug_history`].
    pub callers: Vec<Caller>,
    /// Its last reads, newest first, with
    /// [`super::Builder::debug_history`].
    pub recent: Vec<Trace>,
}

/// One query kept now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeptQuery {
    /// The statement that first loaded it.
    pub sql: String,
    pub state: QueryState,
    pub rows: usize,
    pub bytes: usize,
    pub ttl: Ttl,
    pub last_read: Duration,
}

/// At most this many places are kept per query.
const CALLERS_PER_QUERY: usize = 16;

/// The reads recorded for [`Inspect::callers`] and [`Inspect::recent`].
pub(super) struct Recorder {
    /// How many recent reads are kept per query; zero records nothing.
    history: usize,
    /// At most this many queries are kept; the least recently read go.
    capacity: usize,
    queries: Mutex<HashMap<QueryId, Recorded>>,
}

struct Recorded {
    callers: Vec<(Caller, Instant)>,
    recent: VecDeque<Trace>,
    last: Instant,
}

impl Recorder {
    pub fn new(history: usize, capacity: usize) -> Self {
        Recorder {
            history,
            capacity: capacity.max(1),
            queries: Mutex::new(HashMap::new()),
        }
    }

    /// Whether reads are recorded.
    pub fn on(&self) -> bool {
        self.history > 0
    }

    /// Records `trace`, when recording is on.
    pub fn record(&self, trace: &Trace) {
        if self.history == 0 {
            return;
        }
        let mut queries = self
            .queries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if queries.len() >= self.capacity && !queries.contains_key(&trace.query) {
            let mut by_age: Vec<(Instant, QueryId)> = queries
                .iter()
                .map(|(id, recorded)| (recorded.last, *id))
                .collect();
            by_age.sort_unstable();
            for (_, id) in by_age.into_iter().take((self.capacity / 10).max(1)) {
                queries.remove(&id);
            }
        }
        let recorded = queries.entry(trace.query).or_insert_with(|| Recorded {
            callers: Vec::new(),
            recent: VecDeque::new(),
            last: trace.when,
        });
        recorded.last = trace.when;
        if recorded.callers.len() < CALLERS_PER_QUERY
            || recorded
                .callers
                .iter()
                .any(|(caller, _)| caller.at == trace.at)
        {
            let caller = match recorded
                .callers
                .iter_mut()
                .position(|(caller, _)| caller.at == trace.at)
            {
                Some(index) => &mut recorded.callers[index],
                None => {
                    recorded.callers.push((
                        Caller {
                            at: trace.at,
                            reads: 0,
                            memory: 0,
                            postgres: 0,
                            failed: 0,
                            last: Duration::ZERO,
                        },
                        trace.when,
                    ));
                    recorded.callers.last_mut().expect("just pushed")
                }
            };
            caller.0.reads += 1;
            *match trace.served {
                Served::Memory => &mut caller.0.memory,
                Served::Postgres => &mut caller.0.postgres,
                Served::Unsupported | Served::Failed => &mut caller.0.failed,
            } += 1;
            caller.1 = trace.when;
        }
        recorded.recent.push_front(trace.clone());
        recorded.recent.truncate(self.history);
    }

    /// The places that read `query` and its last reads, newest first.
    pub fn of(&self, query: QueryId) -> (Vec<Caller>, Vec<Trace>) {
        let queries = self
            .queries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(recorded) = queries.get(&query) else {
            return (Vec::new(), Vec::new());
        };
        let callers = recorded
            .callers
            .iter()
            .map(|(caller, last)| Caller {
                last: last.elapsed(),
                ..caller.clone()
            })
            .collect();
        (callers, recorded.recent.iter().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trace(query: u64, served: Served, at: &'static Location<'static>) -> Trace {
        Trace {
            query: QueryId(query),
            sql: "SELECT 1".to_owned(),
            served,
            reason: None,
            ttl: None,
            rows: 0,
            took: Duration::ZERO,
            position: None,
            lag: None,
            at,
            when: Instant::now(),
        }
    }

    /// Each place is counted apart, the newest reads kept first, and
    /// nothing is kept with recording off.
    #[test]
    fn reads_are_recorded_per_place() {
        let here = Location::caller();
        let there = Location::caller();
        let recorder = Recorder::new(2, 8);
        recorder.record(&trace(1, Served::Postgres, here));
        recorder.record(&trace(1, Served::Memory, here));
        recorder.record(&trace(1, Served::Postgres, there));
        let (callers, recent) = recorder.of(QueryId(1));
        assert_eq!(callers.len(), 2);
        assert_eq!(
            (callers[0].reads, callers[0].memory, callers[0].postgres),
            (2, 1, 1)
        );
        assert_eq!(callers[1].postgres, 1);
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].served, Served::Postgres);

        let off = Recorder::new(0, 8);
        off.record(&trace(1, Served::Memory, here));
        assert!(off.of(QueryId(1)).0.is_empty());
    }

    #[test]
    fn recent_reads_survive_the_caller_limit() {
        let recorder = Recorder::new(32, 8);
        let callers = [
            Location::caller(),
            Location::caller(),
            Location::caller(),
            Location::caller(),
            Location::caller(),
            Location::caller(),
            Location::caller(),
            Location::caller(),
            Location::caller(),
            Location::caller(),
            Location::caller(),
            Location::caller(),
            Location::caller(),
            Location::caller(),
            Location::caller(),
            Location::caller(),
            Location::caller(),
        ];
        for at in callers {
            recorder.record(&trace(1, Served::Memory, at));
        }
        let (places, recent) = recorder.of(QueryId(1));
        assert_eq!(places.len(), 16);
        assert_eq!(recent.len(), 17);
        assert_eq!(recent[0].at, callers[16]);
    }

    /// Past its capacity the least recently read queries are forgotten.
    #[test]
    fn the_recorder_is_bounded() {
        let here = Location::caller();
        let recorder = Recorder::new(1, 4);
        for query in 0..20 {
            recorder.record(&trace(query, Served::Memory, here));
        }
        assert!(recorder.queries.lock().unwrap().len() <= 4);
        assert!(!recorder.of(QueryId(19)).0.is_empty());
    }
}
