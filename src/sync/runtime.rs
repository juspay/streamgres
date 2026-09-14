//! The runtime: the single owner of an engine, feeding it the write
//! stream and the results of the storage reads it asks for. It is a pure
//! state machine — no I/O, no clock — so every interleaving of reads and
//! writes can be replayed deterministically in a test; the drivers
//! ([`super::Local`], [`super::Service`]) feed it and run the reads it
//! hands out.
//!
//! # One position
//!
//! The engine sits at one position at every moment: the location of the
//! last write routed into it. A storage read is positioned at or below
//! that (the storage's contract, see [`super::Storage`]), never above, and
//! it lands the moment it returns: every delivered write past the read's
//! snapshot that touches a row of its result is applied to the result
//! first — a delete, or a new image that fails the read's filter, drops
//! the row; a new image that passes replaces it — and what remains is
//! exactly what the engine's position implies. The engine then adopts the
//! rows without comparing anything. Nothing waits: a read behind the
//! stream is caught up, and a read ahead of it cannot exist.
//!
//! The delivered writes are kept in a buffer bounded below by the
//! **floor**: the lowest location a read can still be positioned at,
//! which the driver learns from the storage and sets here, taken together
//! with the floors the reads in flight were issued under. A read the
//! driver could not run (no snapshot available yet) is parked and handed
//! out again the next time the stream moves.

use std::collections::{HashMap, VecDeque};
use std::fmt;

use crate::ivm::{ClientUpdate, Engine, Fetch, FetchId, evaluate};
use crate::model::{
    ClientId, DataFrameKey, DataFrameRow, Lsn, Snapshot, SubId, TableName, WriteQuery,
};

/// What one runtime step produced: deltas to deliver, and reads the
/// driver must run and report back through [`Runtime::fetched`] (or
/// [`Runtime::failed`]).
#[derive(Debug, Default)]
pub struct Step {
    pub updates: Vec<ClientUpdate>,
    pub selects: Vec<Fetch>,
}

/// A read the driver is running: what was asked for, the floor it was
/// issued under, and whether it is parked awaiting the stream.
struct InFlight {
    fetch: Fetch,
    floor: Lsn,
    parked: bool,
}

/// One write the stream delivered, kept while a read may still be
/// positioned below it.
struct Delivered {
    at: Lsn,
    write: WriteQuery,
}

/// Counters of the runtime's own work.
///
/// - `reads_issued`: reads handed to the driver, re-issues included.
/// - `reads_landed`: reads whose rows were applied.
/// - `reads_retried`: reads parked after the driver reported a failure
///   and handed out again.
/// - `rows_dropped`: result rows removed before landing because a
///   delivered write past the read's snapshot deleted them or moved them
///   out of the read's filter.
/// - `rows_refreshed`: result rows whose image was replaced before
///   landing by such a write.
/// - `writes_buffered`: writes remembered for reads to be brought up.
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
        writeln!(
            f,
            "reads issued / landed ...... {} / {}",
            self.reads_issued, self.reads_landed
        )?;
        writeln!(f, "reads retried .............. {}", self.reads_retried)?;
        writeln!(
            f,
            "rows dropped / refreshed ... {} / {}",
            self.rows_dropped, self.rows_refreshed
        )?;
        write!(f, "writes buffered ............ {}", self.writes_buffered)
    }
}

/// The single owner of an engine (see the module docs).
///
/// - `engine`: the engine, exclusively owned.
/// - `in_flight`: reads handed to the driver and not yet landed, by id.
/// - `recent`: delivered writes above the floor, oldest first.
/// - `position`: the location of the last write routed (or progress mark
///   seen): where the engine is.
/// - `floor`: the lowest location a read issued from now on can be at.
/// - `stats`: the runtime's counters.
pub struct Runtime<E: Engine> {
    engine: E,
    in_flight: HashMap<FetchId, InFlight>,
    recent: VecDeque<Delivered>,
    position: Lsn,
    floor: Lsn,
    stats: SyncStats,
}

impl<E: Engine> Runtime<E> {
    /// A runtime owning `engine`, at location zero.
    pub fn new(engine: E) -> Self {
        Runtime {
            engine,
            in_flight: HashMap::new(),
            recent: VecDeque::new(),
            position: Lsn(0),
            floor: Lsn(0),
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

    /// Where the engine is: the location of the last write routed or
    /// progress mark seen.
    pub fn position(&self) -> Lsn {
        self.position
    }

    /// The lowest location a read issued from now on can be positioned
    /// at, as last told by the driver.
    pub fn floor(&self) -> Lsn {
        self.floor
    }

    /// How many delivered writes are kept for bringing reads up.
    pub fn buffered(&self) -> usize {
        self.recent.len()
    }

    /// How many reads are out (parked ones included).
    pub fn outstanding(&self) -> usize {
        self.in_flight.len()
    }

    /// Register a subscription for `client`: its id, whatever of its
    /// snapshot the engine had at hand, and the reads the rest needs.
    pub fn register(&mut self, client: ClientId, query: E::Query) -> (SubId, Step) {
        let (sub, updates) = self.engine.subscribe(client, query);
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

    /// Remove every subscription of `client`.
    pub fn unregister_client(&mut self, client: ClientId) {
        self.engine.unsubscribe_client(client);
    }

    /// Route one write committed at `at`, remembering it for reads
    /// positioned below it. Writes arrive in commit order, so `at` never
    /// goes backwards (two writes of one transaction share a location).
    pub fn write(&mut self, write: &WriteQuery, at: Lsn) -> Step {
        debug_assert!(at >= self.position, "the stream delivers in commit order");
        self.position = self.position.max(at);
        self.stats.writes_buffered += 1;
        self.recent.push_back(Delivered {
            at,
            write: write.clone(),
        });
        let mut step = Step {
            updates: self.engine.route(write),
            selects: Vec::new(),
        };
        self.collect(&mut step);
        self.wake(&mut step);
        self.trim();
        step
    }

    /// The feed has delivered everything up to `lsn` without a write to
    /// carry that fact (an idle tick, the end of a batch).
    pub fn progress(&mut self, lsn: Lsn) -> Step {
        self.position = self.position.max(lsn);
        let mut step = Step::default();
        self.wake(&mut step);
        step
    }

    /// The driver learned the storage's floor: no read issued from now on
    /// is positioned below `floor`. Never moves backwards.
    pub fn set_floor(&mut self, floor: Lsn) {
        if floor > self.floor {
            self.floor = floor;
            self.trim();
        }
    }

    /// The driver finished read `id`: bring its result up to the engine
    /// and land it. An id the runtime is not waiting for is ignored.
    pub fn fetched(&mut self, id: FetchId, snapshot: Snapshot) -> Step {
        let mut step = Step::default();
        let Some(flight) = self.in_flight.remove(&id) else {
            return step;
        };
        debug_assert!(
            snapshot.at <= self.position,
            "a read is never positioned ahead of the engine ({} > {})",
            snapshot.at,
            self.position
        );
        let rows = self.bring_up(&flight.fetch, snapshot.rows, snapshot.at);
        self.stats.reads_landed += 1;
        step.updates = self.engine.land(&flight.fetch, &rows);
        self.collect(&mut step);
        self.trim();
        step
    }

    /// The driver could not run read `id` (no snapshot available yet, a
    /// broken connection): park it; it is handed out again the next time
    /// the stream moves.
    pub fn failed(&mut self, id: FetchId) -> Step {
        if let Some(flight) = self.in_flight.get_mut(&id) {
            flight.parked = true;
        }
        Step::default()
    }

    /// Pick up reads the engine asked for outside a runtime call (a
    /// maintenance seam used directly).
    pub fn pump(&mut self) -> Step {
        let mut step = Step::default();
        self.collect(&mut step);
        step
    }

    /// Take the engine's new read requests into flight, issued under the
    /// current floor, and hand them to the driver.
    fn collect(&mut self, step: &mut Step) {
        for fetch in self.engine.requests() {
            self.stats.reads_issued += 1;
            self.in_flight.insert(
                fetch.id,
                InFlight {
                    fetch: fetch.clone(),
                    floor: self.floor,
                    parked: false,
                },
            );
            step.selects.push(fetch);
        }
    }

    /// Hand every parked read out again, under the current floor.
    fn wake(&mut self, step: &mut Step) {
        for flight in self.in_flight.values_mut() {
            if flight.parked {
                flight.parked = false;
                flight.floor = self.floor;
                self.stats.reads_retried += 1;
                self.stats.reads_issued += 1;
                step.selects.push(flight.fetch.clone());
            }
        }
    }

    /// Apply to a read's result every delivered write past its snapshot's
    /// location: a delete, or a new image that fails the read's filter,
    /// drops the row; a new image that passes replaces it. Rows the writes
    /// never touched stand.
    fn bring_up(
        &mut self,
        fetch: &Fetch,
        rows: Vec<(DataFrameKey, DataFrameRow)>,
        at: Lsn,
    ) -> Vec<(DataFrameKey, DataFrameRow)> {
        let table: &TableName = &fetch.query.table;
        let mut index: HashMap<DataFrameKey, usize> = rows
            .iter()
            .enumerate()
            .map(|(position, (key, _))| (key.clone(), position))
            .collect();
        let mut rows: Vec<Option<(DataFrameKey, DataFrameRow)>> =
            rows.into_iter().map(Some).collect();
        for delivered in &self.recent {
            if delivered.at <= at || delivered.write.table() != table {
                continue;
            }
            let Some(&position) = index.get(delivered.write.pkey_value()) else {
                continue;
            };
            match delivered.write.new_row_image() {
                Some(image) if evaluate(&fetch.query.filter, &image.data, &mut 0) => {
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

    /// Forget delivered writes no read can still be positioned below:
    /// those at or below the floor and every in-flight read's floor.
    fn trim(&mut self) {
        let keep_above = self
            .in_flight
            .values()
            .map(|flight| flight.floor)
            .fold(self.floor, Lsn::min);
        while self
            .recent
            .front()
            .is_some_and(|delivered| delivered.at <= keep_above)
        {
            self.recent.pop_front();
        }
    }
}
