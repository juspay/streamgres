//! Per-table counts of what flows through the server: the changes the
//! change feed brings in for each table, what the engine does with them,
//! the reads it runs on each table, the rows each table puts into pokes,
//! and how long a table's changes take from arriving to reaching a
//! client's socket.
//!
//! Every thread that counts keeps its own handle per table
//! ([`LocalFlows`]), so a count on the engine, group or writer path is one
//! relaxed atomic add; the shared registry ([`Flows`]) is locked only the
//! first time a thread meets a table. The tables are those the catalog
//! publishes, so the series are bounded by the schema.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::ivm::{Delta, FetchKind};
use crate::model::{IdMap, TableName, WriteQuery};

use super::Histogram;

/// The reasons the engine reads a table, in the order of the `kind` label.
pub const READ_KINDS: [&str; 5] = ["snapshot", "narrowed", "refill", "lookup", "row"];

/// Everything counted for one table.
///
/// - `inserts`, `updates`, `deletes`: the changes the feed brought in.
/// - `matched`: those that reached a subscription or asked for a read.
/// - `route_us`: the engine's compute routing them.
/// - `deltas`, `delta_subscriptions`: the row changes they handed to
///   subscriptions, and the subscriptions those reached.
/// - `reads`, `reads_landed`, `rows_read`, `read_us`: the storage reads
///   the engine issued on the table by kind, those that came back with
///   rows (a refused or failed read is issued and never lands), the rows
///   they returned, and their time from being issued to the rows being
///   back on the engine thread.
/// - `rows_put`, `rows_deleted`: the table's row operations written into
///   pokes, once per client group.
/// - `end_to_end`: from the feed decoding the oldest transaction of a
///   poke to that poke's last frame written, for each table the poke
///   carries changed rows of (rows a query's read brought in are not a
///   change, and are not timed).
pub struct TableFlow {
    pub inserts: AtomicU64,
    pub updates: AtomicU64,
    pub deletes: AtomicU64,
    pub matched: AtomicU64,
    pub route_us: AtomicU64,
    pub deltas: AtomicU64,
    pub delta_subscriptions: AtomicU64,
    pub reads: [AtomicU64; READ_KINDS.len()],
    pub reads_landed: AtomicU64,
    pub rows_read: AtomicU64,
    pub read_us: AtomicU64,
    pub rows_put: AtomicU64,
    pub rows_deleted: AtomicU64,
    pub end_to_end: Histogram,
}

impl Default for TableFlow {
    /// Nothing counted.
    fn default() -> Self {
        TableFlow {
            inserts: AtomicU64::new(0),
            updates: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            matched: AtomicU64::new(0),
            route_us: AtomicU64::new(0),
            deltas: AtomicU64::new(0),
            delta_subscriptions: AtomicU64::new(0),
            reads: std::array::from_fn(|_| AtomicU64::new(0)),
            reads_landed: AtomicU64::new(0),
            rows_read: AtomicU64::new(0),
            read_us: AtomicU64::new(0),
            rows_put: AtomicU64::new(0),
            rows_deleted: AtomicU64::new(0),
            end_to_end: Histogram::new(),
        }
    }
}

impl std::fmt::Debug for TableFlow {
    /// The counts that say most about a table at a glance.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableFlow")
            .field("changes", &self.changes())
            .field("matched", &self.matched.load(Ordering::Relaxed))
            .field("deltas", &self.deltas.load(Ordering::Relaxed))
            .field("rows_read", &self.rows_read.load(Ordering::Relaxed))
            .field("rows_put", &self.rows_put.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl TableFlow {
    /// Count one change of this table routed in `took`: its operation, and
    /// whether it reached anything (`deltas` handed out or `reads` asked).
    pub fn note_change(&self, write: &WriteQuery, took: Duration, deltas: usize, reads: usize) {
        let operation = match write {
            WriteQuery::INSERT(_) => &self.inserts,
            WriteQuery::UPDATE(_) => &self.updates,
            WriteQuery::DELETE(_) => &self.deletes,
        };
        operation.fetch_add(1, Ordering::Relaxed);
        if deltas > 0 || reads > 0 {
            self.matched.fetch_add(1, Ordering::Relaxed);
        }
        self.route_us.fetch_add(micros(took), Ordering::Relaxed);
    }

    /// Count one row change a change handed out, and the subscriptions it
    /// reached.
    pub fn note_delta(&self, delta: &Delta) {
        self.note_deltas(1, reach(delta));
    }

    /// Count `deltas` row changes that reached `subscriptions` in all.
    pub fn note_deltas(&self, deltas: u64, subscriptions: u64) {
        self.deltas.fetch_add(deltas, Ordering::Relaxed);
        self.delta_subscriptions
            .fetch_add(subscriptions, Ordering::Relaxed);
    }

    /// Count one storage read of this table asked for `kind`.
    pub fn note_read_issued(&self, kind: FetchKind) {
        self.reads[read_kind_index(kind)].fetch_add(1, Ordering::Relaxed);
    }

    /// Count one storage read of this table that came back with `rows`
    /// rows after `took`.
    pub fn note_read_landed(&self, rows: u64, took: Duration) {
        self.reads_landed.fetch_add(1, Ordering::Relaxed);
        self.rows_read.fetch_add(rows, Ordering::Relaxed);
        self.read_us.fetch_add(micros(took), Ordering::Relaxed);
    }

    /// Count `puts` and `dels` of this table written into one poke.
    pub fn note_sent(&self, puts: u64, dels: u64) {
        self.rows_put.fetch_add(puts, Ordering::Relaxed);
        self.rows_deleted.fetch_add(dels, Ordering::Relaxed);
    }

    /// The changes counted, of every operation.
    pub fn changes(&self) -> u64 {
        self.inserts.load(Ordering::Relaxed)
            + self.updates.load(Ordering::Relaxed)
            + self.deletes.load(Ordering::Relaxed)
    }
}

/// The subscriptions `delta` reaches, over all its audiences.
pub fn reach(delta: &Delta) -> u64 {
    delta
        .audiences
        .iter()
        .map(|audience| audience.subs.len() as u64)
        .sum()
}

/// The position of `kind` in [`READ_KINDS`].
fn read_kind_index(kind: FetchKind) -> usize {
    match kind {
        FetchKind::Snapshot => 0,
        FetchKind::Narrowed => 1,
        FetchKind::Refill => 2,
        FetchKind::Lookup => 3,
        FetchKind::Row => 4,
    }
}

/// `took` in whole microseconds, saturating.
fn micros(took: Duration) -> u64 {
    u64::try_from(took.as_micros()).unwrap_or(u64::MAX)
}

/// Every table's counts, shared by the threads that take them.
#[derive(Default)]
pub struct Flows {
    tables: RwLock<IdMap<TableName, Arc<TableFlow>>>,
}

impl Flows {
    /// The counts of `table`, made the first time it is asked for.
    pub fn table(&self, table: &TableName) -> Arc<TableFlow> {
        if let Ok(tables) = self.tables.read()
            && let Some(flow) = tables.get(table)
        {
            return flow.clone();
        }
        match self.tables.write() {
            Ok(mut tables) => tables.entry(table.clone()).or_default().clone(),
            Err(_) => Arc::new(TableFlow::default()),
        }
    }

    /// Every table counted so far, by name.
    pub fn snapshot(&self) -> Vec<(TableName, Arc<TableFlow>)> {
        let mut tables: Vec<_> = self
            .tables
            .read()
            .map(|tables| {
                tables
                    .iter()
                    .map(|(name, flow)| (name.clone(), flow.clone()))
                    .collect()
            })
            .unwrap_or_default();
        tables.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
        tables
    }
}

/// One thread's handles on the tables it has counted, so that only its
/// first count of a table touches the shared registry.
#[derive(Default)]
pub struct LocalFlows {
    tables: IdMap<TableName, Arc<TableFlow>>,
}

impl LocalFlows {
    /// The counts of `table`, from this thread's handles or else from
    /// `flows`.
    pub fn get(&mut self, flows: &Flows, table: &TableName) -> &TableFlow {
        if !self.tables.contains_key(table) {
            self.tables.insert(table.clone(), flows.table(table));
        }
        &self.tables[table]
    }

    /// The shared handle on `table`'s counts, for a thread that hands it
    /// on.
    pub fn handle(&mut self, flows: &Flows, table: &TableName) -> Arc<TableFlow> {
        if let Some(flow) = self.tables.get(table) {
            return flow.clone();
        }
        let flow = flows.table(table);
        self.tables.insert(table.clone(), flow.clone());
        flow
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::model::{
        ColumnName, DataFrameKey, DataFrameRow, DeleteQuery, InsertQuery, UpdateQuery, Value,
    };

    /// The key of row `id`.
    fn key(id: i64) -> DataFrameKey {
        DataFrameKey::new(HashMap::from([(ColumnName::from("id"), Value::Int(id))]))
    }

    /// A row of `id` alone.
    fn row(id: i64) -> DataFrameRow {
        DataFrameRow::from(HashMap::from([(ColumnName::from("id"), Value::Int(id))]))
    }

    /// Two threads counting one table add to one entry; tables come back
    /// sorted by name.
    #[test]
    fn every_thread_shares_one_count_per_table() {
        let flows = Flows::default();
        let mut engine = LocalFlows::default();
        let mut groups = LocalFlows::default();
        let messages = TableName::from("messages");
        engine.get(&flows, &messages).note_sent(2, 0);
        groups.get(&flows, &messages).note_sent(3, 1);
        engine
            .get(&flows, &TableName::from("channels"))
            .note_sent(1, 0);
        let snapshot = flows.snapshot();
        let names: Vec<&str> = snapshot.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(
            names,
            vec!["channels", "messages"],
            "sorted by name, one entry each"
        );
        assert_eq!(snapshot[1].1.rows_put.load(Ordering::Relaxed), 5);
        assert_eq!(snapshot[1].1.rows_deleted.load(Ordering::Relaxed), 1);
    }

    /// A change counts under its operation, its routing time adds up, and
    /// only a change that handed out a delta or asked for a read counts as
    /// matched.
    #[test]
    fn a_change_is_counted_by_operation_and_by_whether_it_reached_anything() {
        let flow = TableFlow::default();
        let table = TableName::from("tickets");
        let insert = WriteQuery::INSERT(InsertQuery {
            table: table.clone(),
            pkey_value: key(1),
            record: row(1),
        });
        let update = WriteQuery::UPDATE(UpdateQuery {
            table: table.clone(),
            pkey_value: key(1),
            record: row(1),
        });
        let delete = WriteQuery::DELETE(DeleteQuery {
            table,
            pkey_value: key(1),
        });
        flow.note_change(&insert, Duration::from_micros(30), 2, 0);
        flow.note_change(&update, Duration::from_micros(20), 0, 1);
        flow.note_change(&delete, Duration::from_micros(10), 0, 0);
        assert_eq!(flow.inserts.load(Ordering::Relaxed), 1);
        assert_eq!(flow.updates.load(Ordering::Relaxed), 1);
        assert_eq!(flow.deletes.load(Ordering::Relaxed), 1);
        assert_eq!(flow.changes(), 3);
        assert_eq!(
            flow.matched.load(Ordering::Relaxed),
            2,
            "a delta or a read asked for is a change that landed somewhere"
        );
        assert_eq!(flow.route_us.load(Ordering::Relaxed), 60);
    }

    /// A read counts under its kind when issued, and adds its rows and
    /// time when it lands.
    #[test]
    fn a_read_is_counted_by_kind_and_its_rows_and_time_when_it_lands() {
        let flow = TableFlow::default();
        flow.note_read_issued(FetchKind::Snapshot);
        flow.note_read_issued(FetchKind::Refill);
        flow.note_read_issued(FetchKind::Refill);
        flow.note_read_landed(40, Duration::from_millis(3));
        let by_kind: Vec<u64> = flow
            .reads
            .iter()
            .map(|reads| reads.load(Ordering::Relaxed))
            .collect();
        assert_eq!(by_kind, vec![1, 0, 2, 0, 0]);
        assert_eq!(flow.reads_landed.load(Ordering::Relaxed), 1);
        assert_eq!(flow.rows_read.load(Ordering::Relaxed), 40);
        assert_eq!(flow.read_us.load(Ordering::Relaxed), 3_000);
    }
}
