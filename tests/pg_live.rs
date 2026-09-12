//! Live Postgres scenarios: the runtime over `PgStorage` and the
//! `test_decoding` change feed, with writes committed deliberately behind
//! an open snapshot, in both positioning modes; and the asynchronous
//! service end to end. They run only when `JUS_SYNC_PG_DSN` names a
//! database with `wal_level = logical` (and a free replication slot), and
//! report themselves skipped otherwise; each scenario uses its own tables
//! and slot, so they can run in parallel.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;
use std::time::{Duration, Instant};

use jus_sync::ivm::{Fetch, MultiTableIVM, MultiTableUpdate, QueryPart};
use jus_sync::model::*;
use jus_sync::sync::pg::{PgStorage, PgStream, SnapshotMode, XidLedger};
use jus_sync::sync::{Command, Runtime, Service, Storage, SubId};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{spawn_local, LocalSet};
use tokio_postgres::{Client, NoTls};

/// The database under test, if any.
fn dsn() -> Option<String> {
    let dsn = std::env::var("JUS_SYNC_PG_DSN").ok();
    if dsn.is_none() {
        eprintln!("JUS_SYNC_PG_DSN not set: live Postgres scenario skipped");
    }
    dsn
}

/// Run `body` on a current-thread runtime with a local task set (the
/// engine and its storage are single-threaded).
fn block_on<F: std::future::Future>(body: F) -> F::Output {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    LocalSet::new().block_on(&runtime, body)
}

/// One scenario's names: its tables and its slot.
struct Names {
    tickets: String,
    users: String,
    slot: String,
}

impl Names {
    /// Names suffixed with `tag`.
    fn new(tag: &str) -> Self {
        Names {
            tickets: format!("live_{tag}_tickets"),
            users: format!("live_{tag}_users"),
            slot: format!("jus_sync_live_{tag}"),
        }
    }

    /// The catalog of the two tables.
    fn catalog(&self) -> Catalog {
        Catalog::new(vec![
            DbTable::new(
                self.tickets.as_str(),
                ["id"],
                vec![
                    DbColumn::new("id", ValueType::Int),
                    DbColumn::new("status", ValueType::String),
                    DbColumn::new("assigned_to", ValueType::Int),
                    DbColumn::new("points", ValueType::Int),
                ],
            ),
            DbTable::new(
                self.users.as_str(),
                ["id"],
                vec![
                    DbColumn::new("id", ValueType::Int),
                    DbColumn::new("name", ValueType::String),
                ],
            ),
        ])
    }

    /// `OPEN` tickets LEFT JOIN users on `assigned_to = users.id`.
    fn spec(&self) -> MultiTableReadQuery {
        MultiTableReadQuery {
            main_table: SingleTableReadQuery::new(
                self.tickets.as_str(),
                Where::condition("status", ComparisonOperator::EQ, "OPEN"),
                OrderBy::new("id", Order::ASC),
                u32::MAX,
            ),
            left_joins: vec![Join::new(
                MultiTableReadQuery::single(SingleTableReadQuery::new(
                    self.users.as_str(),
                    Where::AND(vec![]),
                    OrderBy::new("id", Order::ASC),
                    u32::MAX,
                )),
                "assigned_to",
                "id",
            )],
            right_joins: Vec::new(),
        }
    }
}

/// An administrative connection.
async fn admin(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    spawn_local(async move {
        let _ = connection.await;
    });
    client
}

/// Fresh tables (three OPEN tickets on users 7, 7, 8; users 7 and 8) and
/// no leftover slot.
async fn prepare(dsn: &str, names: &Names) -> Client {
    let client = admin(dsn).await;
    PgStream::drop_slot(dsn, &names.slot).await.expect("drop slot");
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {t}; DROP TABLE IF EXISTS {u};
             CREATE TABLE {t} (id int8 PRIMARY KEY, status text, assigned_to int8, points int8);
             CREATE TABLE {u} (id int8 PRIMARY KEY, name text);
             INSERT INTO {u} VALUES (7, 'meera'), (8, 'arjun');
             INSERT INTO {t} VALUES (1, 'OPEN', 7, 1), (2, 'OPEN', 7, 2), (3, 'OPEN', 8, 3);",
            t = names.tickets,
            u = names.users
        ))
        .await
        .expect("prepare");
    client
}

/// Drop the scenario's tables and slot.
async fn cleanup(dsn: &str, client: &Client, names: &Names) {
    let _ = client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {}; DROP TABLE IF EXISTS {};",
            names.tickets, names.users
        ))
        .await;
    let _ = PgStream::drop_slot(dsn, &names.slot).await;
}

/// The ids `sql` returns.
async fn truth(client: &Client, sql: &str) -> BTreeSet<i64> {
    client
        .query(sql, &[])
        .await
        .expect("truth")
        .iter()
        .map(|row| row.get::<_, i64>(0))
        .collect()
}

/// The ids of one part's rows.
fn ids(rows: Option<HashMap<DataFrameKey, DataFrameRow>>) -> BTreeSet<i64> {
    rows.unwrap_or_default()
        .keys()
        .map(|key| match key.pkey_value["id"] {
            Value::Int(id) => id,
            _ => panic!("integer ids"),
        })
        .collect()
}

/// Run every read out, poll the feed to move the stream, and repeat until
/// nothing is out; the deltas, in order.
async fn drain(
    runtime: &mut Runtime<MultiTableIVM>,
    storage: &PgStorage,
    stream: &mut PgStream,
    mut pending: Vec<Fetch>,
) -> Vec<MultiTableUpdate> {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut updates = Vec::new();
    loop {
        let mut next = Vec::new();
        for fetch in pending.drain(..) {
            let snapshot = storage
                .select(&fetch.query, runtime.stream_position())
                .await
                .expect("select");
            let step = runtime.fetched(fetch.id, snapshot);
            updates.extend(step.updates);
            next.extend(step.selects);
        }
        pending = next;
        if pending.is_empty() && runtime.outstanding() == 0 {
            return updates;
        }
        let batch = stream.poll().await.expect("poll");
        for (write, at) in batch.writes {
            let step = runtime.write(&write, at);
            updates.extend(step.updates);
            pending.extend(step.selects);
        }
        let step = runtime.progress(batch.progress);
        updates.extend(step.updates);
        pending.extend(step.selects);
        assert!(Instant::now() < deadline, "the runtime did not settle: {} reads out", runtime.outstanding());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The registration scenario in one positioning mode: the main part's
/// snapshot is held open while writes commit behind it, including one
/// from a transaction that was already open when the snapshot was taken;
/// the frames must end equal to what Postgres holds, without adopting
/// any stale image.
async fn registration_behind_open_snapshot(dsn: &str, mode: SnapshotMode, tag: &str) {
    let names = Names::new(tag);
    let client = prepare(dsn, &names).await;
    let catalog = Rc::new(names.catalog());
    let ledger = match &mode {
        SnapshotMode::Xid(ledger) => Some(ledger.clone()),
        SnapshotMode::Wal => None,
    };
    let mut stream = PgStream::open(dsn, &names.slot, catalog.clone(), ledger)
        .await
        .expect("open stream");
    let slow = PgStorage::connect(dsn, catalog.clone(), mode.clone())
        .await
        .expect("connect")
        .with_read_delay(Duration::from_millis(1500));
    let fast = PgStorage::connect(dsn, catalog.clone(), mode).await.expect("connect");
    let mut runtime = Runtime::new(MultiTableIVM::new());

    let open_before = admin(dsn).await;
    open_before
        .batch_execute(&format!(
            "BEGIN; UPDATE {} SET status = 'DONE' WHERE id = 3;",
            names.tickets
        ))
        .await
        .expect("open transaction");

    let (sub, step) = runtime.register(names.spec());
    assert_eq!(step.selects.len(), 1);
    let main = step.selects[0].clone();
    let query = main.query.clone();
    let floor = runtime.stream_position();
    let select = spawn_local(async move { slow.select(&query, floor).await });
    tokio::time::sleep(Duration::from_millis(400)).await;
    client
        .batch_execute(&format!(
            "UPDATE {t} SET status = 'DONE' WHERE id = 1;
             DELETE FROM {t} WHERE id = 2;
             INSERT INTO {t} VALUES (4, 'OPEN', 8, 4);
             UPDATE {t} SET points = 99 WHERE id = 4;",
            t = names.tickets
        ))
        .await
        .expect("writes behind the snapshot");
    open_before.batch_execute("COMMIT;").await.expect("commit the open transaction");

    let behind = stream.poll().await.expect("poll");
    assert_eq!(behind.writes.len(), 5, "every write committed behind the snapshot is delivered before it lands");
    let mut pending = Vec::new();
    for (write, at) in behind.writes {
        pending.extend(runtime.write(&write, at).selects);
    }
    runtime.progress(behind.progress);

    let snapshot = select.await.expect("join").expect("select");
    assert_eq!(snapshot.rows.len(), 3, "the snapshot predates every write");
    let step = runtime.fetched(main.id, snapshot);
    assert!(step.updates.is_empty(), "every snapshot row was overtaken, got {:?}", step.updates);
    pending.extend(step.selects);

    drain(&mut runtime, &fast, &mut stream, pending).await;

    let open = truth(&client, &format!("SELECT id FROM {} WHERE status = 'OPEN'", names.tickets)).await;
    assert_eq!(open, BTreeSet::from([4]));
    assert_eq!(ids(runtime.engine().rows_for(sub, QueryPart::main())), open);
    assert_eq!(ids(runtime.engine().rows_for(sub, QueryPart::join(0))), BTreeSet::from([8]));
    let main_rows = runtime.engine().rows_for(sub, QueryPart::main()).unwrap();
    let points: BTreeMap<i64, Value> = main_rows
        .iter()
        .map(|(key, row)| match key.pkey_value["id"] {
            Value::Int(id) => (id, row.data["points"].clone()),
            _ => panic!(),
        })
        .collect();
    assert_eq!(points[&4], Value::Int(99), "the stream's image, not an older one");
    assert_eq!(
        runtime.stats().rows_dropped,
        3,
        "every snapshot row was overtaken: 1 moved out, 2 deleted, 3 moved out by the transaction open at snapshot time"
    );
    cleanup(dsn, &client, &names).await;
}

/// The WAL method (exported snapshot of a temporary slot): writes
/// committed behind an open snapshot.
#[test]
fn wal_registration_behind_open_snapshot() {
    let Some(dsn) = dsn() else { return };
    block_on(registration_behind_open_snapshot(&dsn, SnapshotMode::Wal, "wal"));
}

/// Transaction-id positioning: the same, with the in-progress transaction
/// listed in the snapshot's `xip`.
#[test]
fn xid_registration_behind_open_snapshot() {
    let Some(dsn) = dsn() else { return };
    block_on(registration_behind_open_snapshot(&dsn, SnapshotMode::Xid(XidLedger::shared()), "xid"));
}

/// The asynchronous service end to end: the poller feeds commands, the
/// service runs reads as tasks and pushes deltas; a client frame built
/// from the deltas alone converges to what Postgres holds.
#[test]
fn service_streams_end_to_end() {
    let Some(dsn) = dsn() else { return };
    block_on(async {
        let names = Names::new("service");
        let client = prepare(&dsn, &names).await;
        let catalog = Rc::new(names.catalog());
        let ledger = XidLedger::shared();
        let stream = PgStream::open(&dsn, &names.slot, catalog.clone(), Some(ledger.clone()))
            .await
            .expect("open stream");
        let storage = PgStorage::connect(&dsn, catalog.clone(), SnapshotMode::Xid(ledger))
            .await
            .expect("connect");
        let (updates_tx, mut updates) = mpsc::unbounded_channel();
        let (service, commands) = Service::new(MultiTableIVM::new(), Rc::new(storage), updates_tx);
        let service = spawn_local(service.run());
        let poller = spawn_local(stream.run(Duration::from_millis(50), commands.clone()));

        let (reply, sub) = oneshot::channel();
        commands
            .send(Command::Register { query: names.spec(), reply })
            .await
            .expect("send");
        let sub: SubId = sub.await.expect("registered");

        client
            .batch_execute(&format!(
                "INSERT INTO {t} VALUES (10, 'OPEN', 9, 10), (11, 'OPEN', 8, 11);
                 INSERT INTO {u} VALUES (9, 'late');
                 UPDATE {t} SET status = 'DONE' WHERE id = 1;
                 DELETE FROM {t} WHERE id = 2;",
                t = names.tickets,
                u = names.users
            ))
            .await
            .expect("writes");

        let expected_main = truth(&client, &format!("SELECT id FROM {} WHERE status = 'OPEN'", names.tickets)).await;
        let expected_users = truth(
            &client,
            &format!(
                "SELECT DISTINCT u.id FROM {u} u JOIN {t} t ON t.assigned_to = u.id WHERE t.status = 'OPEN'",
                t = names.tickets,
                u = names.users
            ),
        )
        .await;

        let mut frames: HashMap<QueryPart, HashMap<DataFrameKey, DataFrameRow>> = HashMap::new();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match tokio::time::timeout(Duration::from_millis(200), updates.recv()).await {
                Ok(Some(batch)) => {
                    for update in batch {
                        assert_eq!(update.query, sub);
                        let frame = frames.entry(update.part).or_default();
                        match update.op {
                            DataFrameOperation::Add(key, row) => {
                                frame.insert(key, row);
                            }
                            DataFrameOperation::Delete(key, _) => {
                                frame.remove(&key);
                            }
                        }
                    }
                }
                Ok(None) => panic!("service ended"),
                Err(_) => {}
            }
            let main = ids(frames.get(&QueryPart::main()).cloned());
            let users = ids(frames.get(&QueryPart::join(0)).cloned());
            if main == expected_main && users == expected_users {
                tokio::time::sleep(Duration::from_millis(300)).await;
                break;
            }
            assert!(
                Instant::now() < deadline,
                "client frames did not converge: main {main:?} vs {expected_main:?}, users {users:?} vs {expected_users:?}"
            );
        }
        poller.abort();
        drop(commands);
        let runtime = service.await.expect("service");
        assert_eq!(runtime.outstanding(), 0);
        assert!(runtime.stats().reads_landed >= 2);
        cleanup(&dsn, &client, &names).await;
    });
}
