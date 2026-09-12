//! Deterministic interleavings of asynchronous storage reads with the
//! write stream, driven through the runtime state machine directly: the
//! test decides when each read's snapshot is taken, which writes stream
//! past before it lands, and how every write and snapshot is positioned,
//! so the runtime's merge (bring the result up to the engine, land at
//! once) and the engine's per-row currency (a write a landed row already
//! reflects is not news) are checked without any clock or I/O.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use jus_sync::ivm::{Engine, Fetch, MultiTableIVM, QueryPart, SingleTableIVM, SubId};
use jus_sync::model::*;
use jus_sync::sync::{Lsn, MemoryStorage, Runtime, Snapshot, Step};

/// `tickets(id, status, assigned_to, points)`.
fn tickets_table() -> DbTable {
    DbTable::new(
        "tickets",
        ["id"],
        vec![
            DbColumn::new("id", ValueType::Int),
            DbColumn::new("status", ValueType::String),
            DbColumn::new("assigned_to", ValueType::Int),
            DbColumn::new("points", ValueType::Int),
        ],
    )
}

/// `users(id, name)`.
fn users_table() -> DbTable {
    DbTable::new(
        "users",
        ["id"],
        vec![
            DbColumn::new("id", ValueType::Int),
            DbColumn::new("name", ValueType::String),
        ],
    )
}

fn pkey(id: i64) -> HashMap<String, Value> {
    HashMap::from([("id".to_owned(), Value::Int(id))])
}

/// Collects `(column, value)` pairs plus the `id` primary key into a full
/// row image.
fn full_row(id: i64, pairs: &[(&str, Value)]) -> DataFrameRow {
    let mut data: HashMap<String, Value> = pairs
        .iter()
        .map(|(column, value)| ((*column).to_owned(), value.clone()))
        .collect();
    data.insert("id".to_owned(), Value::Int(id));
    DataFrameRow { data }
}

fn insert(table: &str, id: i64, pairs: &[(&str, Value)]) -> WriteQuery {
    WriteQuery::INSERT(InsertQuery {
        table: TableName::from(table),
        pkey_value: DataFrameKey::new(pkey(id)),
        record: full_row(id, pairs),
    })
}

fn update(table: &str, id: i64, pairs: &[(&str, Value)]) -> WriteQuery {
    WriteQuery::UPDATE(UpdateQuery {
        table: TableName::from(table),
        pkey_value: DataFrameKey::new(pkey(id)),
        record: full_row(id, pairs),
    })
}

fn delete(table: &str, id: i64) -> WriteQuery {
    WriteQuery::DELETE(DeleteQuery {
        table: TableName::from(table),
        pkey_value: DataFrameKey::new(pkey(id)),
    })
}

/// A ticket row: `status`, `assigned_to`, `points`.
fn ticket(id: i64, status: &str, user: i64, points: i64) -> WriteQuery {
    insert(
        "tickets",
        id,
        &[
            ("status", status.into()),
            ("assigned_to", Value::Int(user)),
            ("points", Value::Int(points)),
        ],
    )
}

/// A ticket update carrying a full row image.
fn ticket_update(id: i64, status: &str, user: i64, points: i64) -> WriteQuery {
    update(
        "tickets",
        id,
        &[
            ("status", status.into()),
            ("assigned_to", Value::Int(user)),
            ("points", Value::Int(points)),
        ],
    )
}

fn user(id: i64, name: &str) -> WriteQuery {
    insert("users", id, &[("name", name.into())])
}

/// An unbounded single-table query.
fn query(table: &DbTable, filter: Where) -> SingleTableReadQuery {
    SingleTableReadQuery::new(
        table.name.clone(),
        filter,
        OrderBy::new("id", Order::ASC),
        u32::MAX,
    )
}

/// `OPEN` tickets.
fn open_tickets() -> SingleTableReadQuery {
    query(
        &tickets_table(),
        Where::condition("status", ComparisonOperator::EQ, "OPEN"),
    )
}

/// The database of the test: the store the reads are answered from, and a
/// WAL clock. Every commit lands in the store and takes the next WAL
/// location; a snapshot is the store's rows right now, positioned at the
/// last commit.
struct Db {
    storage: MemoryStorage,
    lsn: u64,
}

impl Db {
    /// An empty database whose WAL is at `lsn`.
    fn at(lsn: u64) -> Self {
        Db {
            storage: MemoryStorage::new(),
            lsn,
        }
    }

    /// Commit a write: apply it to the store and position it at the next
    /// WAL location.
    fn commit(&mut self, write: &WriteQuery) -> Lsn {
        self.storage.apply(write);
        self.lsn += 1;
        Lsn(self.lsn)
    }

    /// Seed rows before any subscription exists (committed, positioned,
    /// never streamed to the runtime).
    fn seed(&mut self, writes: &[WriteQuery]) {
        for write in writes {
            self.commit(write);
        }
    }

    /// Run a read against the store as it is now, positioned at the last
    /// commit.
    fn snapshot(&self, fetch: &Fetch) -> Snapshot {
        Snapshot {
            rows: self.storage.rows(&fetch.query),
            at: Lsn(self.lsn),
        }
    }

    /// The current WAL location.
    fn head(&self) -> Lsn {
        Lsn(self.lsn)
    }
}

/// The ids of a subscription's rows, sorted.
fn ids(rows: Option<HashMap<DataFrameKey, DataFrameRow>>) -> BTreeSet<i64> {
    rows.unwrap_or_default()
        .keys()
        .map(|key| match key.pkey_value["id"] {
            Value::Int(id) => id,
            _ => panic!("integer ids"),
        })
        .collect()
}

/// The ids of a subscription's rows with one column's value, sorted by id.
fn column_of(rows: Option<HashMap<DataFrameKey, DataFrameRow>>, column: &str) -> BTreeMap<i64, Value> {
    rows.unwrap_or_default()
        .into_iter()
        .map(|(key, row)| match key.pkey_value["id"] {
            Value::Int(id) => (id, row.data[column].clone()),
            _ => panic!("integer ids"),
        })
        .collect()
}

/// Run every read a step handed out against the database as it is right
/// now and land it, repeating until nothing is out; the updates of every
/// step, in order.
fn settle<E: Engine>(runtime: &mut Runtime<E>, db: &Db, step: Step<E::Update>) -> Vec<E::Update> {
    let mut updates = step.updates;
    let mut queue = step.selects;
    while !queue.is_empty() {
        let fetch = queue.remove(0);
        let landed = runtime.fetched(fetch.id, db.snapshot(&fetch));
        updates.extend(landed.updates);
        queue.extend(landed.selects);
    }
    updates
}

/// Exactly one read was handed out.
fn only(step: &Step<impl std::fmt::Debug>) -> Fetch {
    assert_eq!(step.selects.len(), 1, "one read expected, got {:?}", step.selects);
    step.selects[0].clone()
}

/// A registration's snapshot is taken at some point, and writes stream
/// past before it lands: the result is brought up to the engine first,
/// so rows a later write moved out or deleted are dropped and a row it
/// rewrote takes the newer image, which the frame already holds from the
/// write's own routing; a row inserted after the snapshot was routed
/// natively and is held.
#[test]
fn registration_result_is_brought_up_to_the_engine() {
    let mut db = Db::at(0);
    db.seed(&[
        ticket(1, "OPEN", 7, 1),
        ticket(2, "OPEN", 7, 2),
        ticket(3, "OPEN", 7, 3),
    ]);
    let mut runtime = Runtime::new(SingleTableIVM::new());
    runtime.progress(db.head());

    let (sub, step) = runtime.register(open_tickets());
    assert!(step.updates.is_empty(), "nothing is at hand until the read lands");
    let read = only(&step);
    let snapshot = db.snapshot(&read);
    assert_eq!(snapshot.rows.len(), 3);

    let moved_out = ticket_update(1, "DONE", 7, 1);
    let mut native = runtime.write(&moved_out, db.commit(&moved_out)).updates;
    let deleted = delete("tickets", 2);
    native.extend(runtime.write(&deleted, db.commit(&deleted)).updates);
    let rewritten = ticket_update(3, "OPEN", 7, 33);
    native.extend(runtime.write(&rewritten, db.commit(&rewritten)).updates);
    let inserted = ticket(4, "OPEN", 7, 4);
    native.extend(runtime.write(&inserted, db.commit(&inserted)).updates);
    assert_eq!(
        native.len(),
        2,
        "the rewrite and the insert route natively as adds, got {native:?}"
    );
    runtime.progress(db.head());

    let landed = runtime.fetched(read.id, snapshot);
    assert!(landed.updates.is_empty(), "every snapshot row was overtaken, got {:?}", landed.updates);
    assert_eq!(runtime.stats().rows_dropped, 2, "1 moved out, 2 deleted");
    assert_eq!(runtime.stats().rows_refreshed, 1, "3 rewritten");
    assert_eq!(ids(runtime.engine().rows_for(sub)), BTreeSet::from([3, 4]));
    assert_eq!(
        column_of(runtime.engine().rows_for(sub), "points")[&3],
        Value::Int(33),
        "the natively routed image, not the snapshot's"
    );
    assert_eq!(runtime.outstanding(), 0);
}

/// Writes the snapshot already reflects (committed before it, delivered
/// after the read was asked for) are adopted, and since their rows were
/// also routed natively the landing emits nothing for them again.
#[test]
fn covered_writes_are_adopted_without_duplicate_operations() {
    let mut db = Db::at(0);
    db.seed(&[ticket(1, "OPEN", 7, 1), ticket(2, "OPEN", 7, 2)]);
    let mut runtime = Runtime::new(SingleTableIVM::new());
    runtime.progress(db.head());

    let (sub, step) = runtime.register(open_tickets());
    let read = only(&step);

    let touched = ticket_update(1, "OPEN", 7, 99);
    let arrived = ticket(3, "OPEN", 7, 3);
    let touched_at = db.commit(&touched);
    let arrived_at = db.commit(&arrived);
    let snapshot = db.snapshot(&read);
    assert_eq!(snapshot.rows.len(), 3, "taken after both commits");

    runtime.write(&touched, touched_at);
    runtime.write(&arrived, arrived_at);
    runtime.progress(db.head());
    let landed = runtime.fetched(read.id, snapshot);
    let landed_ids: BTreeSet<i64> = landed
        .updates
        .iter()
        .map(|update| match &update.op {
            DataFrameOperation::Add(key, _) => match key.pkey_value["id"] {
                Value::Int(id) => id,
                _ => panic!(),
            },
            other => panic!("landing only adds, got {other:?}"),
        })
        .collect();
    assert_eq!(landed_ids, BTreeSet::from([2]), "1 and 3 were already held identically");
    assert_eq!(runtime.stats().rows_dropped, 0);
    assert_eq!(ids(runtime.engine().rows_for(sub)), BTreeSet::from([1, 2, 3]));
    assert_eq!(column_of(runtime.engine().rows_for(sub), "points")[&1], Value::Int(99));
}

/// A snapshot ahead of the stream (the database committed writes the
/// feed has not delivered yet) lands at once; when those writes arrive,
/// the landed rows already reflect them and nothing is emitted again.
#[test]
fn snapshot_ahead_of_the_stream_lands_at_once() {
    let mut db = Db::at(0);
    db.seed(&[ticket(1, "OPEN", 7, 1)]);
    let mut runtime = Runtime::new(SingleTableIVM::new());
    runtime.progress(db.head());

    let (sub, step) = runtime.register(open_tickets());
    let read = only(&step);
    let late = ticket(2, "OPEN", 7, 2);
    let late_at = db.commit(&late);
    let snapshot = db.snapshot(&read);
    assert_eq!(snapshot.rows.len(), 2, "the snapshot already contains the undelivered insert");

    let landed = runtime.fetched(read.id, snapshot);
    assert_eq!(landed.updates.len(), 2, "both rows land immediately, got {:?}", landed.updates);
    assert_eq!(runtime.stats().reads_landed, 1);
    assert_eq!(ids(runtime.engine().rows_for(sub)), BTreeSet::from([1, 2]));

    let step = runtime.write(&late, late_at);
    assert!(step.updates.is_empty(), "the landed row already reflects the write, got {:?}", step.updates);
    assert_eq!(ids(runtime.engine().rows_for(sub)), BTreeSet::from([1, 2]));

    let rewrite = ticket_update(2, "OPEN", 7, 22);
    let step = runtime.write(&rewrite, db.commit(&rewrite));
    assert_eq!(step.updates.len(), 2, "a write after the snapshot is news: the replace pair, got {:?}", step.updates);
    assert_eq!(column_of(runtime.engine().rows_for(sub), "points")[&2], Value::Int(22));

    let (other, step) = runtime.register(query(
        &tickets_table(),
        Where::condition("points", ComparisonOperator::GTE, 0),
    ));
    let read = only(&step);
    let quiet = ticket(3, "DONE", 7, 3);
    db.commit(&quiet);
    let landed = runtime.fetched(read.id, db.snapshot(&read));
    assert_eq!(landed.updates.len(), 3, "landed at once, ahead of the feed, got {:?}", landed.updates);
    let step = runtime.progress(db.head());
    assert!(step.updates.is_empty());
    assert_eq!(ids(runtime.engine().rows_for(other)), BTreeSet::from([1, 2, 3]));
}

/// The storage's one obligation: a read's location must not be past a
/// write the snapshot did not see. A read positioned exactly right treats
/// the unseen write as news and drops the row it moved out; a read
/// positioned one write too far adopts the stale row for good (the row
/// already held keeps its newer image, since the frame is newer than the
/// read). This is what the WAL method's consistent point and the XID
/// method's ledger conversion guarantee.
#[test]
fn a_read_positioned_past_an_unseen_write_adopts_a_stale_row() {
    let mut db = Db::at(0);
    db.seed(&[ticket(1, "OPEN", 7, 1), ticket(2, "OPEN", 7, 2)]);
    let mut runtime = Runtime::new(SingleTableIVM::new());
    runtime.progress(db.head());

    let (sub, step) = runtime.register(open_tickets());
    let read = only(&step);
    let snapshot = db.snapshot(&read);
    let moved_out = ticket_update(2, "DONE", 7, 2);
    let moved_at = db.commit(&moved_out);
    let rewritten = ticket_update(1, "OPEN", 7, 11);
    runtime.write(&moved_out, moved_at);
    runtime.write(&rewritten, db.commit(&rewritten));
    let landed = runtime.fetched(read.id, snapshot);
    assert!(landed.updates.is_empty(), "2 is dropped, 1 is already held newer, got {:?}", landed.updates);
    assert_eq!(runtime.stats().rows_dropped, 1);
    assert_eq!(ids(runtime.engine().rows_for(sub)), BTreeSet::from([1]));
    assert_eq!(column_of(runtime.engine().rows_for(sub), "points")[&1], Value::Int(11));

    let mut too_far = Runtime::new(SingleTableIVM::new());
    too_far.progress(Lsn(2));
    let (sub, step) = too_far.register(open_tickets());
    let read = only(&step);
    let mut stale = db.storage.rows(&read.query);
    stale.push((
        DataFrameKey::new(pkey(2)),
        full_row(2, &[("status", "OPEN".into()), ("assigned_to", Value::Int(7)), ("points", Value::Int(2))]),
    ));
    too_far.write(&moved_out, Lsn(3));
    too_far.write(&rewritten, Lsn(4));
    too_far.fetched(
        read.id,
        Snapshot {
            rows: stale,
            at: Lsn(3),
        },
    );
    assert_eq!(
        ids(too_far.engine().rows_for(sub)),
        BTreeSet::from([1, 2]),
        "positioned as if it had seen the write at 3, the read keeps the row that write moved out"
    );
    assert_eq!(
        column_of(too_far.engine().rows_for(sub), "points")[&1],
        Value::Int(11),
        "a row already held keeps the newer image the stream wrote"
    );
}

/// A join crossing's narrowed read is out while the driven table is
/// written: a row the read returns that a later write rewrote lands with
/// the newer image the frame already holds, and a row the write removed
/// stays gone.
#[test]
fn narrowed_join_fetch_defers_to_later_writes() {
    let mut db = Db::at(0);
    db.seed(&[user(7, "meera"), user(8, "old"), user(9, "gone"), ticket(1, "OPEN", 7, 1)]);
    let mut runtime = Runtime::new(MultiTableIVM::new());
    runtime.progress(db.head());
    let spec = MultiTableReadQuery {
        main_table: open_tickets(),
        left_joins: vec![Join::new(
            MultiTableReadQuery::single(query(&users_table(), Where::AND(vec![]))),
            "assigned_to",
            "id",
        )],
        right_joins: Vec::new(),
    };
    let (sub, step) = runtime.register(spec);
    settle(&mut runtime, &db, step);
    assert_eq!(ids(runtime.engine().rows_for(sub, QueryPart::join(0))), BTreeSet::from([7]));

    let refer = ticket(2, "OPEN", 8, 2);
    let step = runtime.write(&refer, db.commit(&refer));
    runtime.progress(db.head());
    let read = only(&step);
    assert_eq!(read.query.table, "users");
    let snapshot = db.snapshot(&read);
    assert_eq!(snapshot.rows.len(), 1);

    let renamed = user(8, "new");
    let native = runtime.write(&renamed, db.commit(&renamed)).updates;
    assert_eq!(native.len(), 1, "the newly referenced user routes natively, got {native:?}");
    runtime.progress(db.head());
    let landed = runtime.fetched(read.id, snapshot);
    assert!(landed.updates.is_empty(), "the frame already holds the newer image, got {:?}", landed.updates);
    assert_eq!(runtime.stats().rows_refreshed, 1);
    assert_eq!(
        column_of(runtime.engine().rows_for(sub, QueryPart::join(0)), "name"),
        BTreeMap::from([(7, "meera".into()), (8, "new".into())])
    );

    let refer = ticket(3, "OPEN", 9, 3);
    let step = runtime.write(&refer, db.commit(&refer));
    runtime.progress(db.head());
    let read = only(&step);
    let snapshot = db.snapshot(&read);
    let removed = delete("users", 9);
    runtime.write(&removed, db.commit(&removed));
    runtime.progress(db.head());
    let landed = runtime.fetched(read.id, snapshot);
    assert!(landed.updates.is_empty());
    assert_eq!(ids(runtime.engine().rows_for(sub, QueryPart::join(0))), BTreeSet::from([7, 8]));
}

/// A refill is out while the window's table is written: the boundary is
/// open meanwhile, so rows the refill will not return are admitted (and
/// evicted past capacity), and landing re-derives the frontier so the
/// window ends up holding exactly the best rows and rejects worse ones
/// again.
#[test]
fn refill_in_flight_keeps_the_window_exact() {
    let mut db = Db::at(0);
    db.seed(
        &[10, 20, 30, 40, 50, 60]
            .iter()
            .map(|points| ticket(*points, "OPEN", 7, *points))
            .collect::<Vec<_>>(),
    );
    let mut runtime = Runtime::new(SingleTableIVM::new());
    runtime.progress(db.head());
    let windowed = SingleTableReadQuery::new(
        tickets_table().name.clone(),
        Where::AND(vec![]),
        OrderBy::new("points", Order::ASC),
        2,
    );
    let (sub, step) = runtime.register(windowed);
    settle(&mut runtime, &db, step);
    assert_eq!(ids(runtime.engine().rows_for(sub)), BTreeSet::from([10, 20, 30, 40]));

    let drop_10 = delete("tickets", 10);
    runtime.write(&drop_10, db.commit(&drop_10));
    let drop_20 = delete("tickets", 20);
    let step = runtime.write(&drop_20, db.commit(&drop_20));
    runtime.progress(db.head());
    let refill = only(&step);
    assert_eq!(refill.query.limit, 3, "two missing plus the held row at the frontier");
    let snapshot = db.snapshot(&refill);
    assert_eq!(snapshot.rows.len(), 3, "40, 50, 60");

    let during = ticket(45, "OPEN", 7, 45);
    let admitted = runtime.write(&during, db.commit(&during)).updates;
    assert_eq!(admitted.len(), 1, "boundary open while the refill is out");
    let far = ticket(100, "OPEN", 7, 100);
    let admitted = runtime.write(&far, db.commit(&far)).updates;
    assert_eq!(admitted.len(), 1, "even a row far beyond the old frontier is admitted for now");
    runtime.progress(db.head());

    let landed = runtime.fetched(refill.id, snapshot);
    assert_eq!(ids(runtime.engine().rows_for(sub)), BTreeSet::from([30, 40, 45, 50]));
    let deletes = landed
        .updates
        .iter()
        .filter(|update| matches!(update.op, DataFrameOperation::Delete(..)))
        .count();
    assert_eq!(deletes, 2, "60 and 100 evicted past capacity, got {:?}", landed.updates);

    let worse = ticket(70, "OPEN", 7, 70);
    assert!(
        runtime.write(&worse, db.commit(&worse)).updates.is_empty(),
        "the boundary is back: worse than the frontier is rejected"
    );
    let better = ticket(35, "OPEN", 7, 35);
    let step = runtime.write(&better, db.commit(&better));
    runtime.progress(db.head());
    assert_eq!(step.updates.len(), 2, "admitted, worst evicted");
    assert_eq!(ids(runtime.engine().rows_for(sub)), BTreeSet::from([30, 35, 40, 45]));
    assert_eq!(runtime.outstanding(), 0);
}

/// Registration walks the tree as reads land: a RIGHT child's read comes
/// first, and only once it has landed does the parent register, with the
/// child's join values already in its set, so the parent costs one read;
/// a LEFT child registers after its parent landed, with the parent's
/// values in its set, again one read.
#[test]
fn post_order_registration_follows_landings() {
    let mut db = Db::at(0);
    db.seed(&[
        user(7, "meera"),
        user(8, "arjun"),
        ticket(1, "OPEN", 7, 1),
        ticket(2, "OPEN", 8, 2),
        ticket(3, "OPEN", 9, 3),
    ]);
    let mut runtime = Runtime::new(MultiTableIVM::new());
    runtime.progress(db.head());
    let right = MultiTableReadQuery {
        main_table: open_tickets(),
        left_joins: Vec::new(),
        right_joins: vec![Join::new(
            MultiTableReadQuery::single(query(&users_table(), Where::AND(vec![]))),
            "assigned_to",
            "id",
        )],
    };
    let (sub, step) = runtime.register(right);
    let first = only(&step);
    assert_eq!(first.query.table, "users", "the driving child reads first");
    let step = runtime.fetched(first.id, db.snapshot(&first));
    let second = only(&step);
    assert_eq!(second.query.table, "tickets", "the parent registers once the child landed");
    let step = runtime.fetched(second.id, db.snapshot(&second));
    assert!(step.selects.is_empty(), "the parent's set was complete: no narrowed reads");
    assert_eq!(ids(runtime.engine().rows_for(sub, QueryPart::main())), BTreeSet::from([1, 2]));
    assert_eq!(runtime.engine().stats().storage_reads, 2);

    let left = MultiTableReadQuery {
        main_table: query(
            &tickets_table(),
            Where::condition("status", ComparisonOperator::EQ, "OPEN"),
        ),
        left_joins: vec![Join::new(
            MultiTableReadQuery::single(query(&users_table(), Where::AND(vec![]))),
            "assigned_to",
            "id",
        )],
        right_joins: Vec::new(),
    };
    let before = runtime.engine().stats().storage_reads;
    let (sub, step) = runtime.register(left);
    let main = only(&step);
    assert_eq!(main.query.table, "tickets");
    let step = runtime.fetched(main.id, db.snapshot(&main));
    let child = only(&step);
    assert_eq!(child.query.table, "users", "the LEFT child registers after the parent landed");
    let step = runtime.fetched(child.id, db.snapshot(&child));
    assert!(step.selects.is_empty());
    assert_eq!(ids(runtime.engine().rows_for(sub, QueryPart::join(0))), BTreeSet::from([7, 8]));
    assert_eq!(runtime.engine().stats().storage_reads - before, 2, "one read per part");
}

/// A failed read is handed out again and reconciled from the point of the
/// stream it is re-issued at; an unregistered subscription's read lands
/// as a no-op; a second identical registration while the first's read is
/// out reads for itself rather than copying an incomplete twin.
#[test]
fn retries_unregistration_and_pending_twins() {
    let mut db = Db::at(0);
    db.seed(&[ticket(1, "OPEN", 7, 1)]);
    let mut runtime = Runtime::new(SingleTableIVM::new());
    runtime.progress(db.head());

    let (sub, step) = runtime.register(open_tickets());
    let read = only(&step);
    let retry = runtime.failed(read.id);
    assert_eq!(only(&retry).id, read.id, "the same read, handed out again");
    assert_eq!(runtime.stats().reads_retried, 1);
    runtime.fetched(read.id, db.snapshot(&read));
    assert_eq!(ids(runtime.engine().rows_for(sub)), BTreeSet::from([1]));

    let (doomed, step) = runtime.register(query(
        &tickets_table(),
        Where::condition("points", ComparisonOperator::GTE, 0),
    ));
    let read = only(&step);
    runtime.unregister(doomed);
    let landed = runtime.fetched(read.id, db.snapshot(&read));
    assert!(landed.updates.is_empty());
    assert_eq!(runtime.outstanding(), 0);
    assert!(runtime.engine().rows_for(doomed).is_none());

    let (first, step) = runtime.register(query(
        &tickets_table(),
        Where::condition("assigned_to", ComparisonOperator::EQ, 7),
    ));
    let first_read = only(&step);
    let (second, step) = runtime.register(query(
        &tickets_table(),
        Where::condition("assigned_to", ComparisonOperator::EQ, 7),
    ));
    let second_read = only(&step);
    assert_ne!(first_read.id, second_read.id);
    assert_eq!(runtime.engine().stats().snapshots_shared, 0, "an incomplete twin donates nothing");
    runtime.fetched(first_read.id, db.snapshot(&first_read));
    runtime.fetched(second_read.id, db.snapshot(&second_read));
    assert_eq!(ids(runtime.engine().rows_for(first)), BTreeSet::from([1]));
    assert_eq!(ids(runtime.engine().rows_for(second)), BTreeSet::from([1]));

    let (third, step) = runtime.register(query(
        &tickets_table(),
        Where::condition("assigned_to", ComparisonOperator::EQ, 7),
    ));
    assert!(step.selects.is_empty(), "with both landed, the twin path serves it");
    assert_eq!(step.updates.len(), 1);
    assert_eq!(runtime.engine().stats().snapshots_shared, 1);
    assert_eq!(ids(runtime.engine().rows_for(third)), BTreeSet::from([1]));
}

/// A later identical join registration while the shared tree is still
/// landing is served what is there and receives the rest as it lands,
/// like every other subscriber.
#[test]
fn twin_joining_a_landing_tree_receives_the_rest() {
    let mut db = Db::at(0);
    db.seed(&[user(7, "meera"), ticket(1, "OPEN", 7, 1)]);
    let mut runtime = Runtime::new(MultiTableIVM::new());
    runtime.progress(db.head());
    let spec = || MultiTableReadQuery {
        main_table: open_tickets(),
        left_joins: vec![Join::new(
            MultiTableReadQuery::single(query(&users_table(), Where::AND(vec![]))),
            "assigned_to",
            "id",
        )],
        right_joins: Vec::new(),
    };
    let (first, step) = runtime.register(spec());
    let main = only(&step);
    let (second, step) = runtime.register(spec());
    assert!(step.selects.is_empty() && step.updates.is_empty(), "shares the tree, nothing landed yet");

    let step = runtime.fetched(main.id, db.snapshot(&main));
    let for_second: Vec<&SubId> = step.updates.iter().map(|update| &update.query).filter(|sub| **sub == second).collect();
    assert_eq!(for_second.len(), 1, "the landed main row reaches the twin too");
    let child = only(&step);
    let step = runtime.fetched(child.id, db.snapshot(&child));
    assert_eq!(step.updates.len(), 2, "the user row, once per subscriber");
    for sub in [first, second] {
        assert_eq!(ids(runtime.engine().rows_for(sub, QueryPart::main())), BTreeSet::from([1]));
        assert_eq!(ids(runtime.engine().rows_for(sub, QueryPart::join(0))), BTreeSet::from([7]));
    }
}

/// A registration read returns a row the frame already holds for another
/// subscription, with a newer image (a write committed before the
/// snapshot, not yet delivered): the reader gets the read's image and is
/// ahead of the frame on that row, the other holder keeps the frame's
/// image and hears about the write when it arrives, the reader does not;
/// a later write reaches both, the reader's `Delete` carrying its own
/// image.
#[test]
fn reader_ahead_of_the_frame_gets_the_read_image() {
    let mut db = Db::at(0);
    db.seed(&[ticket(1, "OPEN", 7, 1)]);
    let mut runtime = Runtime::new(SingleTableIVM::new());
    runtime.progress(db.head());
    let (b, step) = runtime.register(query(
        &tickets_table(),
        Where::condition("points", ComparisonOperator::GTE, 0),
    ));
    settle(&mut runtime, &db, step);
    assert_eq!(column_of(runtime.engine().rows_for(b), "points")[&1], Value::Int(1));

    let (a, step) = runtime.register(open_tickets());
    let read = only(&step);
    let bump = ticket_update(1, "OPEN", 7, 5);
    let bump_at = db.commit(&bump);
    let snapshot = db.snapshot(&read);

    let landed = runtime.fetched(read.id, snapshot);
    assert_eq!(landed.updates.len(), 1, "only the reader is served, got {:?}", landed.updates);
    assert_eq!(landed.updates[0].query, a);
    assert!(matches!(&landed.updates[0].op, DataFrameOperation::Add(_, row) if row.data["points"] == Value::Int(5)));
    assert_eq!(column_of(runtime.engine().rows_for(a), "points")[&1], Value::Int(5), "the reader's view");
    assert_eq!(column_of(runtime.engine().rows_for(b), "points")[&1], Value::Int(1), "the other holder's view");
    assert_eq!(
        runtime.engine().holders_of(&TableName::from("tickets"), &DataFrameKey::new(pkey(1))),
        vec![b, a]
    );

    let step = runtime.write(&bump, bump_at);
    assert_eq!(step.updates.len(), 2, "the replace pair for the holder behind, got {:?}", step.updates);
    assert!(step.updates.iter().all(|update| update.query == b));
    assert_eq!(column_of(runtime.engine().rows_for(b), "points")[&1], Value::Int(5));

    let again = ticket_update(1, "OPEN", 7, 7);
    let step = runtime.write(&again, db.commit(&again));
    assert_eq!(step.updates.len(), 4, "both holders now, got {:?}", step.updates);
    let reader_delete = step
        .updates
        .iter()
        .find(|update| update.query == a && matches!(update.op, DataFrameOperation::Delete(..)))
        .expect("the reader's delete");
    assert!(matches!(&reader_delete.op, DataFrameOperation::Delete(_, row) if row.data["points"] == Value::Int(5)));
    assert_eq!(column_of(runtime.engine().rows_for(a), "points")[&1], Value::Int(7));
    assert_eq!(column_of(runtime.engine().rows_for(b), "points")[&1], Value::Int(7));
}

/// A twin registered while its donor is ahead of the frame is served the
/// donor's view and goes ahead with it: neither hears about the write the
/// view already reflects.
#[test]
fn twin_of_an_ahead_reader_shares_its_view() {
    let mut db = Db::at(0);
    db.seed(&[ticket(1, "OPEN", 7, 1)]);
    let mut runtime = Runtime::new(SingleTableIVM::new());
    runtime.progress(db.head());
    let (b, step) = runtime.register(query(
        &tickets_table(),
        Where::condition("points", ComparisonOperator::GTE, 0),
    ));
    settle(&mut runtime, &db, step);
    let (a, step) = runtime.register(open_tickets());
    let read = only(&step);
    let bump = ticket_update(1, "OPEN", 7, 5);
    let bump_at = db.commit(&bump);
    runtime.fetched(read.id, db.snapshot(&read));

    let (twin, step) = runtime.register(open_tickets());
    assert!(step.selects.is_empty(), "served from the donor");
    assert_eq!(step.updates.len(), 1);
    assert!(matches!(&step.updates[0].op, DataFrameOperation::Add(_, row) if row.data["points"] == Value::Int(5)));
    assert_eq!(column_of(runtime.engine().rows_for(twin), "points")[&1], Value::Int(5));

    let step = runtime.write(&bump, bump_at);
    assert!(step.updates.iter().all(|update| update.query == b), "reader and twin already have it, got {:?}", step.updates);
    assert_eq!(step.updates.len(), 2);
    for sub in [a, twin, b] {
        assert_eq!(column_of(runtime.engine().rows_for(sub), "points")[&1], Value::Int(5));
    }
}

/// The join layer's counts follow each part's own view: a main part
/// ahead of the frame references the join value of the image it holds,
/// is not disturbed when the write it already reflects arrives, and diffs
/// from its own image when a later write moves the row again.
#[test]
fn reader_ahead_keeps_join_counts_consistent() {
    let mut db = Db::at(0);
    db.seed(&[user(7, "meera"), user(8, "arjun"), user(9, "kai"), ticket(1, "OPEN", 7, 1)]);
    let mut runtime = Runtime::new(MultiTableIVM::new());
    runtime.progress(db.head());
    let users_join = || Join::new(
        MultiTableReadQuery::single(query(&users_table(), Where::AND(vec![]))),
        "assigned_to",
        "id",
    );
    let (b, step) = runtime.register(MultiTableReadQuery {
        main_table: query(&tickets_table(), Where::condition("points", ComparisonOperator::GTE, 0)),
        left_joins: vec![users_join()],
        right_joins: Vec::new(),
    });
    settle(&mut runtime, &db, step);
    assert_eq!(ids(runtime.engine().rows_for(b, QueryPart::join(0))), BTreeSet::from([7]));

    let (a, step) = runtime.register(MultiTableReadQuery {
        main_table: open_tickets(),
        left_joins: vec![users_join()],
        right_joins: Vec::new(),
    });
    let main = only(&step);
    let move8 = ticket_update(1, "OPEN", 8, 1);
    let move8_at = db.commit(&move8);
    let step = runtime.fetched(main.id, db.snapshot(&main));
    settle(&mut runtime, &db, step);
    assert_eq!(ids(runtime.engine().rows_for(a, QueryPart::join(0))), BTreeSet::from([8]), "the reader's own image drives its edge");
    assert_eq!(ids(runtime.engine().rows_for(b, QueryPart::join(0))), BTreeSet::from([7]));

    let step = runtime.write(&move8, move8_at);
    settle(&mut runtime, &db, step);
    assert_eq!(ids(runtime.engine().rows_for(a, QueryPart::join(0))), BTreeSet::from([8]), "untouched: it already reflected the write");
    assert_eq!(ids(runtime.engine().rows_for(b, QueryPart::join(0))), BTreeSet::from([8]), "the holder behind moved with the write");

    let move9 = ticket_update(1, "OPEN", 9, 1);
    let step = runtime.write(&move9, db.commit(&move9));
    settle(&mut runtime, &db, step);
    assert_eq!(ids(runtime.engine().rows_for(a, QueryPart::join(0))), BTreeSet::from([9]), "released 8 from its own image, not 7 from the frame's");
    assert_eq!(ids(runtime.engine().rows_for(b, QueryPart::join(0))), BTreeSet::from([9]));
}
