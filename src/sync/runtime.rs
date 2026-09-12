//! The runtime: the single owner of an engine, feeding it the write
//! stream and the results of the storage reads it asks for. It is a pure
//! state machine — no I/O, no clock — so every interleaving of reads and
//! writes can be replayed deterministically in a test; the drivers
//! ([`super::Local`], [`super::Service`]) feed it and run the reads it
//! hands out.
//!
//! # Landing a read
//!
//! A read asked for at some point of the stream returns rows true at a
//! snapshot taken later, while further writes arrive; it lands the moment
//! it returns. Two things reconcile it with the writes the engine applied
//! meanwhile:
//!
//! - **The result is brought up to the engine.** Every write delivered
//!   since the read was asked for that lies past the snapshot's location
//!   is applied to the result before it lands: a row it deleted, or moved
//!   out of the read's filter, is dropped; a row it rewrote takes the
//!   newer image. The rows that remain are true at the engine's position,
//!   so landing cannot resurrect what the stream has since removed.
//! - **The engine merges by currency.** Each landed row is stamped with
//!   the read's position; a write the row already reflects is not applied
//!   to it again, a frame row that is newer than the read keeps its image
//!   (see the engine's module docs). A read ahead of the stream therefore
//!   needs no waiting: the writes it already saw are recognized when they
//!   arrive.
//!
//! The buffer of delivered writes is kept only while a read is out, and
//! trimmed as reads land. The stream position the runtime has seen is
//! offered to the storage with every read as `at_least`, so a source
//! whose snapshots are minted ahead of time (the WAL method's alias) never
//! answers with one older than what the engine has already applied.

use std::collections::{HashMap, VecDeque};
use std::fmt;

use crate::ivm::{evaluate, Engine, Fetch, FetchId};
use crate::model::{DataFrameKey, DataFrameRow, Lsn, Snapshot, SubId, TableName, WriteQuery};

/// What one runtime step produced: deltas to deliver, and reads the
/// driver must run and report back through [`Runtime::fetched`] (or
/// [`Runtime::failed`]).
#[derive(Debug)]
pub struct Step<U> {
    pub updates: Vec<U>,
    pub selects: Vec<Fetch>,
}

impl<U> Default for Step<U> {
    /// Nothing to deliver, nothing to run.
    fn default() -> Self {
        Step {
            updates: Vec::new(),
            selects: Vec::new(),
        }
    }
}

/// A read the driver is running: what was asked for, and the stream
/// sequence number it was asked for at.
struct InFlight {
    fetch: Fetch,
    since: u64,
}

/// One write the stream delivered while a read was out, kept to bring
/// the read's result up to the engine when it lands.
struct Delivered {
    seq: u64,
    at: Lsn,
    write: WriteQuery,
}

/// Counters of the runtime's own work.
///
/// - `reads_issued`: reads handed to the driver, retries included.
/// - `reads_landed`: reads whose rows were applied.
/// - `reads_retried`: reads re-issued after the driver reported a failure.
/// - `rows_dropped`: result rows removed before landing because a write
///   delivered since the read was asked for deleted them or moved them
///   out of the read's filter.
/// - `rows_refreshed`: result rows whose image was replaced before
///   landing by such a write.
/// - `writes_buffered`: writes remembered while a read was out.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncStats {
    pub reads_issued: u64,
    pub reads_landed: u64,
    pub reads_retried: u64,
    pub rows_dropped: u64,
    pub rows_refreshed: u64,
    pub writes_buffered: u64,
}

impl fmt::Display for SyncStats {
    /// Multi-line, dot-aligned rendering of every counter.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "reads issued / landed ...... {} / {}", self.reads_issued, self.reads_landed)?;
        writeln!(f, "reads retried .............. {}", self.reads_retried)?;
        writeln!(f, "rows dropped / refreshed ... {} / {}", self.rows_dropped, self.rows_refreshed)?;
        write!(f, "writes buffered ............ {}", self.writes_buffered)
    }
}

/// The single owner of an engine (see the module docs).
///
/// - `engine`: the engine, exclusively owned.
/// - `in_flight`: reads handed to the driver and not yet landed, by id.
/// - `recent`: writes delivered since the oldest in-flight read was asked
///   for, oldest first; trimmed as reads land.
/// - `seq`: the stream sequence number of the last write routed.
/// - `stream`: the furthest WAL location the feed has reported, through
///   writes and progress marks; `None` before the first, and always for
///   the in-process store.
/// - `stats`: the runtime's counters.
pub struct Runtime<E: Engine> {
    engine: E,
    in_flight: HashMap<FetchId, InFlight>,
    recent: VecDeque<Delivered>,
    seq: u64,
    stream: Option<Lsn>,
    stats: SyncStats,
}

impl<E: Engine> Runtime<E> {
    /// A runtime owning `engine`, with the stream at no position yet.
    pub fn new(engine: E) -> Self {
        Runtime {
            engine,
            in_flight: HashMap::new(),
            recent: VecDeque::new(),
            seq: 0,
            stream: None,
            stats: SyncStats::default(),
        }
    }

    /// The engine, for inspection.
    pub fn engine(&self) -> &E {
        &self.engine
    }

    /// The engine, for direct maintenance calls; run [`Runtime::pump`]
    /// afterwards to pick up any reads they asked for.
    pub fn engine_mut(&mut self) -> &mut E {
        &mut self.engine
    }

    /// The runtime's counters.
    pub fn stats(&self) -> &SyncStats {
        &self.stats
    }

    /// The furthest WAL location the feed has reported: what a read
    /// issued now must at least reflect.
    pub fn stream_position(&self) -> Option<Lsn> {
        self.stream
    }

    /// How many reads are out.
    pub fn outstanding(&self) -> usize {
        self.in_flight.len()
    }

    /// Register a subscription: its id, whatever of its snapshot the
    /// engine had at hand, and the reads the rest needs.
    pub fn register(&mut self, query: E::Query) -> (SubId, Step<E::Update>) {
        let (sub, updates) = self.engine.subscribe(query);
        let mut step = Step {
            updates,
            selects: Vec::new(),
        };
        self.collect(&mut step);
        (sub, step)
    }

    /// Remove a subscription; its reads still out land as no-ops.
    pub fn unregister(&mut self, sub: SubId) {
        self.engine.unsubscribe(sub);
    }

    /// Route one write committed at `at`, remembering it for the reads
    /// that are out.
    pub fn write(&mut self, write: &WriteQuery, at: Lsn) -> Step<E::Update> {
        self.seq += 1;
        if !self.in_flight.is_empty() {
            self.stats.writes_buffered += 1;
            self.recent.push_back(Delivered {
                seq: self.seq,
                at,
                write: write.clone(),
            });
        }
        self.advance(Some(at));
        let mut step = Step {
            updates: self.engine.route(write, at),
            selects: Vec::new(),
        };
        self.collect(&mut step);
        step
    }

    /// The feed has delivered everything up to `lsn` without a write to
    /// carry that fact (an idle tick, the end of a batch).
    pub fn progress(&mut self, lsn: Lsn) -> Step<E::Update> {
        self.advance(Some(lsn));
        Step::default()
    }

    /// The driver finished read `id`: bring its result up to the engine
    /// and land it. An id the runtime is not waiting for is ignored.
    pub fn fetched(&mut self, id: FetchId, snapshot: Snapshot) -> Step<E::Update> {
        let mut step = Step::default();
        let Some(flight) = self.in_flight.remove(&id) else {
            return step;
        };
        let rows = self.bring_up(&flight, snapshot.rows, snapshot.at);
        self.stats.reads_landed += 1;
        step.updates = self.engine.land(&flight.fetch, &rows, snapshot.at);
        self.collect(&mut step);
        self.trim();
        step
    }

    /// The driver could not complete read `id`: hand it out again, to be
    /// reconciled from this point of the stream.
    pub fn failed(&mut self, id: FetchId) -> Step<E::Update> {
        let mut step = Step::default();
        if let Some(flight) = self.in_flight.get_mut(&id) {
            flight.since = self.seq;
            self.stats.reads_retried += 1;
            self.stats.reads_issued += 1;
            step.selects.push(flight.fetch.clone());
        }
        step
    }

    /// Pick up reads the engine asked for outside a runtime call (a
    /// maintenance seam used directly).
    pub fn pump(&mut self) -> Step<E::Update> {
        let mut step = Step::default();
        self.collect(&mut step);
        step
    }

    /// Move the stream position forward to `lsn` (never backward).
    fn advance(&mut self, lsn: Option<Lsn>) {
        if let Some(lsn) = lsn
            && self.stream.is_none_or(|current| current < lsn)
        {
            self.stream = Some(lsn);
        }
    }

    /// Take the engine's new read requests into flight, asked for at the
    /// current stream sequence, and hand them to the driver.
    fn collect(&mut self, step: &mut Step<E::Update>) {
        for fetch in self.engine.requests() {
            self.stats.reads_issued += 1;
            self.in_flight.insert(
                fetch.id,
                InFlight {
                    fetch: fetch.clone(),
                    since: self.seq,
                },
            );
            step.selects.push(fetch);
        }
    }

    /// Apply to a read's result every write delivered since the read was
    /// asked for that lies past its snapshot's location: a delete, or a
    /// new image that fails the read's filter, drops the row; a new image
    /// that passes replaces it. Rows the writes never touched stand.
    fn bring_up(
        &mut self,
        flight: &InFlight,
        rows: Vec<(DataFrameKey, DataFrameRow)>,
        at: Lsn,
    ) -> Vec<(DataFrameKey, DataFrameRow)> {
        let table: &TableName = &flight.fetch.query.table;
        let mut index: HashMap<DataFrameKey, usize> = rows
            .iter()
            .enumerate()
            .map(|(position, (key, _))| (key.clone(), position))
            .collect();
        let mut rows: Vec<Option<(DataFrameKey, DataFrameRow)>> = rows.into_iter().map(Some).collect();
        for delivered in &self.recent {
            if delivered.seq <= flight.since || delivered.write.table() != table || delivered.at <= at {
                continue;
            }
            let Some(&position) = index.get(delivered.write.pkey_value()) else {
                continue;
            };
            match delivered.write.new_row_image() {
                Some(image) if evaluate(&flight.fetch.query.filter, &image.data, &mut 0) => {
                    if let Some((_, row)) = rows[position].as_mut() {
                        *row = image.clone();
                        self.stats.rows_refreshed += 1;
                    }
                }
                _ => {
                    if rows[position].take().is_some() {
                        self.stats.rows_dropped += 1;
                    }
                    index.remove(delivered.write.pkey_value());
                }
            }
        }
        rows.into_iter().flatten().collect()
    }

    /// Forget delivered writes no outstanding read can still need.
    fn trim(&mut self) {
        match self.in_flight.values().map(|flight| flight.since).min() {
            None => self.recent.clear(),
            Some(floor) => {
                while self.recent.front().is_some_and(|delivered| delivered.seq <= floor) {
                    self.recent.pop_front();
                }
            }
        }
    }
}
