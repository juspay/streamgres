//! Deterministic interleavings of storage reads with the write stream,
//! driven through the runtime state machine directly: the test decides
//! where each read's snapshot is positioned (never ahead of what the
//! runtime has applied, the storage's contract), which writes stream past
//! before it lands, and checks the runtime's one rule: a read's result is
//! brought up to the engine's position before the engine sees it, whether
//! the writes it missed were delivered before or after the read was
//! issued.

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

/// The one client every subscription here belongs to.
const CLIENT: ClientId = ClientId(1);

fn pkey(id: i64) -> HashMap<ColumnName, Value> {
    HashMap::from([("id".into(), Value::Int(id))])
}

/// Collects `(column, value)` pairs plus the `id` primary key into a full
/// row image.
fn full_row(id: i64, pairs: &[(&str, Value)]) -> DataFrameRow {
    let mut data: HashMap<ColumnName, Value> = pairs
        .iter()
        .map(|(column, value)| ((*column).into(), value.clone()))
        .collect();
    data.insert("id".into(), Value::Int(id));
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
fn column_of(
    rows: Option<HashMap<DataFrameKey, DataFrameRow>>,
    column: &str,
) -> BTreeMap<i64, Value> {
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
fn settle<E: Engine>(
    runtime: &mut Runtime<E>,
    db: &Db,
    step: Step,
) -> Vec<jus_sync::ivm::ClientUpdate> {
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
fn only(step: &Step) -> Fetch {
    assert_eq!(
        step.selects.len(),
        1,
        "one read expected, got {:?}",
        step.selects
    );
    step.selects[0].clone()
}

/// Route `write` after committing it, and move the floor up to the
/// runtime's position the way a driver over an in-process store does.
fn stream<E: Engine>(runtime: &mut Runtime<E>, db: &mut Db, write: &WriteQuery) -> Step {
    let at = db.commit(write);
    runtime.write(write, at)
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

    let (sub, step) = runtime.register(CLIENT, open_tickets());
    assert!(
        step.updates.is_empty(),
        "nothing is at hand until the read lands"
    );
    let read = only(&step);
    let snapshot = db.snapshot(&read);
    assert_eq!(snapshot.rows.len(), 3);

    let mut native = stream(&mut runtime, &mut db, &ticket_update(1, "DONE", 7, 1)).updates;
    native.extend(stream(&mut runtime, &mut db, &delete("tickets", 2)).updates);
    native.extend(stream(&mut runtime, &mut db, &ticket_update(3, "OPEN", 7, 33)).updates);
    native.extend(stream(&mut runtime, &mut db, &ticket(4, "OPEN", 7, 4)).updates);
    assert_eq!(
        native.len(),
        2,
        "the rewrite and the insert route natively as adds, got {native:?}"
    );

    let landed = runtime.fetched(read.id, snapshot);
    assert!(
        landed.updates.is_empty(),
        "every snapshot row was overtaken, got {:?}",
        landed.updates
    );
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
/// after the read was asked for) are not applied again: the landing adds
/// only the rows nothing routed yet.
#[test]
fn covered_writes_are_adopted_without_duplicate_operations() {
    let mut db = Db::at(0);
    db.seed(&[ticket(1, "OPEN", 7, 1), ticket(2, "OPEN", 7, 2)]);
    let mut runtime = Runtime::new(SingleTableIVM::new());
    runtime.progress(db.head());

    let (sub, step) = runtime.register(CLIENT, open_tickets());
    let read = only(&step);

    let touched = ticket_update(1, "OPEN", 7, 99);
    let arrived = ticket(3, "OPEN", 7, 3);
    let touched_at = db.commit(&touched);
    let arrived_at = db.commit(&arrived);
    let snapshot = db.snapshot(&read);
    assert_eq!(snapshot.rows.len(), 3, "taken after both commits");

    runtime.write(&touched, touched_at);
    runtime.write(&arrived, arrived_at);
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
    assert_eq!(
        landed_ids,
        BTreeSet::from([2]),
        "1 and 3 were already held identically"
    );
    assert_eq!(runtime.stats().rows_dropped, 0);
    assert_eq!(runtime.stats().rows_refreshed, 0);
    assert_eq!(
        ids(runtime.engine().rows_for(sub)),
        BTreeSet::from([1, 2, 3])
    );
    assert_eq!(
        column_of(runtime.engine().rows_for(sub), "points")[&1],
        Value::Int(99)
    );
}

/// A read positioned behind writes that were delivered *before* it was
/// issued (the storage's snapshot lags the stream, as a rotating alias
/// does): the buffer of delivered writes reaches back to the floor, so
/// the result is still brought up; once the floor passes them the buffer
/// is trimmed.
#[test]
fn a_read_behind_earlier_writes_is_brought_up_from_the_floor() {
    let mut db = Db::at(0);
    db.seed(&[
        ticket(1, "OPEN", 7, 1),
        ticket(2, "OPEN", 7, 2),
        ticket(3, "OPEN", 7, 3),
    ]);
    let mut runtime = Runtime::new(SingleTableIVM::new());
    runtime.progress(db.head());
    let stale_rows = db.storage.rows(&open_tickets());
    let stale_at = db.head();

    stream(&mut runtime, &mut db, &ticket_update(1, "DONE", 7, 1));
    stream(&mut runtime, &mut db, &delete("tickets", 2));
    assert_eq!(runtime.buffered(), 2, "kept: the floor is still at zero");

    let (sub, step) = runtime.register(CLIENT, open_tickets());
    let read = only(&step);
    let landed = runtime.fetched(
        read.id,
        Snapshot {
            rows: stale_rows,
            at: stale_at,
        },
    );
    assert_eq!(
        landed.updates.len(),
        1,
        "only 3 survives, got {:?}",
        landed.updates
    );
    assert_eq!(runtime.stats().rows_dropped, 2);
    assert_eq!(ids(runtime.engine().rows_for(sub)), BTreeSet::from([3]));

    runtime.set_floor(db.head());
    assert_eq!(
        runtime.buffered(),
        0,
        "nothing can be positioned below the floor anymore"
    );
    stream(&mut runtime, &mut db, &ticket(4, "OPEN", 7, 4));
    assert_eq!(
        runtime.buffered(),
        1,
        "a write above the floor is kept until the floor passes it"
    );
    runtime.set_floor(db.head());
    assert_eq!(runtime.buffered(), 0);
}

/// A join crossing's narrowed read is out while the driven table is
/// written: a row the read returns that a later write rewrote lands with
/// the newer image the frame already holds, and a row the write removed
/// stays gone.
#[test]
fn narrowed_join_fetch_defers_to_later_writes() {
    let mut db = Db::at(0);
    db.seed(&[
        user(7, "meera"),
        user(8, "old"),
        user(9, "gone"),
        ticket(1, "OPEN", 7, 1),
    ]);
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
        inner_joins: Vec::new(),
    };
    let (sub, step) = runtime.register(CLIENT, spec);
    settle(&mut runtime, &db, step);
    assert_eq!(
        ids(runtime.engine().rows_for(sub, QueryPart::join(0))),
        BTreeSet::from([7])
    );

    let step = stream(&mut runtime, &mut db, &ticket(2, "OPEN", 8, 2));
    let read = only(&step);
    assert_eq!(read.query.table, "users");
    let snapshot = db.snapshot(&read);
    assert_eq!(snapshot.rows.len(), 1);

    let native = stream(&mut runtime, &mut db, &user(8, "new")).updates;
    assert_eq!(
        native.len(),
        1,
        "the newly referenced user routes natively, got {native:?}"
    );
    let landed = runtime.fetched(read.id, snapshot);
    assert!(
        landed.updates.is_empty(),
        "the frame already holds the newer image, got {:?}",
        landed.updates
    );
    assert_eq!(runtime.stats().rows_refreshed, 1);
    assert_eq!(
        column_of(runtime.engine().rows_for(sub, QueryPart::join(0)), "name"),
        BTreeMap::from([(7, "meera".into()), (8, "new".into())])
    );

    let step = stream(&mut runtime, &mut db, &ticket(3, "OPEN", 9, 3));
    let read = only(&step);
    let snapshot = db.snapshot(&read);
    stream(&mut runtime, &mut db, &delete("users", 9));
    let landed = runtime.fetched(read.id, snapshot);
    assert!(landed.updates.is_empty());
    assert_eq!(
        ids(runtime.engine().rows_for(sub, QueryPart::join(0))),
        BTreeSet::from([7, 8])
    );
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
    let (sub, step) = runtime.register(CLIENT, windowed);
    settle(&mut runtime, &db, step);
    assert_eq!(
        ids(runtime.engine().rows_for(sub)),
        BTreeSet::from([10, 20, 30, 40])
    );

    stream(&mut runtime, &mut db, &delete("tickets", 10));
    let step = stream(&mut runtime, &mut db, &delete("tickets", 20));
    let refill = only(&step);
    assert_eq!(
        refill.query.limit, 3,
        "two missing plus the held row at the frontier"
    );
    let snapshot = db.snapshot(&refill);
    assert_eq!(snapshot.rows.len(), 3, "40, 50, 60");

    let admitted = stream(&mut runtime, &mut db, &ticket(45, "OPEN", 7, 45)).updates;
    assert_eq!(admitted.len(), 1, "boundary open while the refill is out");
    let admitted = stream(&mut runtime, &mut db, &ticket(100, "OPEN", 7, 100)).updates;
    assert_eq!(
        admitted.len(),
        1,
        "even a row far beyond the old frontier is admitted for now"
    );

    let landed = runtime.fetched(refill.id, snapshot);
    assert_eq!(
        ids(runtime.engine().rows_for(sub)),
        BTreeSet::from([30, 40, 45, 50])
    );
    let deletes = landed
        .updates
        .iter()
        .filter(|update| matches!(update.op, DataFrameOperation::Delete(..)))
        .count();
    assert_eq!(
        deletes, 2,
        "60 and 100 evicted past capacity, got {:?}",
        landed.updates
    );

    assert!(
        stream(&mut runtime, &mut db, &ticket(70, "OPEN", 7, 70))
            .updates
            .is_empty(),
        "the boundary is back: worse than the frontier is rejected"
    );
    let step = stream(&mut runtime, &mut db, &ticket(35, "OPEN", 7, 35));
    assert_eq!(step.updates.len(), 2, "admitted, worst evicted");
    assert_eq!(
        ids(runtime.engine().rows_for(sub)),
        BTreeSet::from([30, 35, 40, 45])
    );
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
        inner_joins: Vec::new(),
    };
    let (sub, step) = runtime.register(CLIENT, right);
    let first = only(&step);
    assert_eq!(first.query.table, "users", "the driving child reads first");
    let step = runtime.fetched(first.id, db.snapshot(&first));
    let second = only(&step);
    assert_eq!(
        second.query.table, "tickets",
        "the parent registers once the child landed"
    );
    let step = runtime.fetched(second.id, db.snapshot(&second));
    assert!(
        step.selects.is_empty(),
        "the parent's set was complete: no narrowed reads"
    );
    assert_eq!(
        ids(runtime.engine().rows_for(sub, QueryPart::main())),
        BTreeSet::from([1, 2])
    );
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
        inner_joins: Vec::new(),
    };
    let before = runtime.engine().stats().storage_reads;
    let (sub, step) = runtime.register(CLIENT, left);
    let main = only(&step);
    assert_eq!(main.query.table, "tickets");
    let step = runtime.fetched(main.id, db.snapshot(&main));
    let child = only(&step);
    assert_eq!(
        child.query.table, "users",
        "the LEFT child registers after the parent landed"
    );
    let step = runtime.fetched(child.id, db.snapshot(&child));
    assert!(step.selects.is_empty());
    assert_eq!(
        ids(runtime.engine().rows_for(sub, QueryPart::join(0))),
        BTreeSet::from([7, 8])
    );
    assert_eq!(
        runtime.engine().stats().storage_reads - before,
        2,
        "one read per part"
    );
}

/// A read the driver could not run is parked and handed out again when
/// the stream moves; an unregistered subscription's read lands as a
/// no-op; a second identical registration while the first's read is out
/// reads for itself rather than copying an incomplete twin.
#[test]
fn parked_reads_unregistration_and_pending_twins() {
    let mut db = Db::at(0);
    db.seed(&[ticket(1, "OPEN", 7, 1)]);
    let mut runtime = Runtime::new(SingleTableIVM::new());
    runtime.progress(db.head());

    let (sub, step) = runtime.register(CLIENT, open_tickets());
    let read = only(&step);
    let parked = runtime.failed(read.id);
    assert!(parked.selects.is_empty(), "parked, not re-issued at once");
    assert_eq!(runtime.outstanding(), 1);
    let woken = stream(&mut runtime, &mut db, &ticket(2, "DONE", 7, 2));
    assert_eq!(only(&woken).id, read.id, "the same read, handed out again");
    assert_eq!(runtime.stats().reads_retried, 1);
    runtime.fetched(read.id, db.snapshot(&read));
    assert_eq!(ids(runtime.engine().rows_for(sub)), BTreeSet::from([1]));

    let (doomed, step) = runtime.register(
        CLIENT,
        query(
            &tickets_table(),
            Where::condition("points", ComparisonOperator::GTE, 0),
        ),
    );
    let read = only(&step);
    runtime.unregister(doomed);
    let landed = runtime.fetched(read.id, db.snapshot(&read));
    assert!(landed.updates.is_empty());
    assert_eq!(runtime.outstanding(), 0);
    assert!(runtime.engine().rows_for(doomed).is_none());

    let (first, step) = runtime.register(
        CLIENT,
        query(
            &tickets_table(),
            Where::condition("assigned_to", ComparisonOperator::EQ, 7),
        ),
    );
    let first_read = only(&step);
    let (second, step) = runtime.register(
        CLIENT,
        query(
            &tickets_table(),
            Where::condition("assigned_to", ComparisonOperator::EQ, 7),
        ),
    );
    let second_read = only(&step);
    assert_ne!(first_read.id, second_read.id);
    assert_eq!(
        runtime.engine().stats().snapshots_shared,
        0,
        "an incomplete twin donates nothing"
    );
    runtime.fetched(first_read.id, db.snapshot(&first_read));
    runtime.fetched(second_read.id, db.snapshot(&second_read));
    assert_eq!(
        ids(runtime.engine().rows_for(first)),
        BTreeSet::from([1, 2])
    );
    assert_eq!(
        ids(runtime.engine().rows_for(second)),
        BTreeSet::from([1, 2])
    );

    let (third, step) = runtime.register(
        CLIENT,
        query(
            &tickets_table(),
            Where::condition("assigned_to", ComparisonOperator::EQ, 7),
        ),
    );
    assert!(
        step.selects.is_empty(),
        "with both landed, the twin path serves it"
    );
    assert_eq!(step.updates.len(), 2, "one delta per shared row");
    assert_eq!(runtime.engine().stats().snapshots_shared, 1);
    assert_eq!(
        ids(runtime.engine().rows_for(third)),
        BTreeSet::from([1, 2])
    );
}

/// A later identical join registration while the shared tree is still
/// landing is served what is there and receives the rest as it lands,
/// like every other subscriber; two subscribers of one client receive one
/// delta naming both.
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
        inner_joins: Vec::new(),
    };
    let (first, step) = runtime.register(CLIENT, spec());
    let main = only(&step);
    let (second, step) = runtime.register(CLIENT, spec());
    assert!(
        step.selects.is_empty() && step.updates.is_empty(),
        "shares the tree, nothing landed yet"
    );

    let step = runtime.fetched(main.id, db.snapshot(&main));
    assert_eq!(step.updates.len(), 1, "one delta for the one client");
    let subs: BTreeSet<SubId> = step.updates[0]
        .targets
        .iter()
        .map(|target| target.sub)
        .collect();
    assert_eq!(
        subs,
        BTreeSet::from([first, second]),
        "naming both subscribers"
    );
    let child = only(&step);
    let step = runtime.fetched(child.id, db.snapshot(&child));
    assert_eq!(step.updates.len(), 1);
    assert_eq!(step.updates[0].targets.len(), 2);
    for sub in [first, second] {
        assert_eq!(
            ids(runtime.engine().rows_for(sub, QueryPart::main())),
            BTreeSet::from([1])
        );
        assert_eq!(
            ids(runtime.engine().rows_for(sub, QueryPart::join(0))),
            BTreeSet::from([7])
        );
    }
}
