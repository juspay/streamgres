//! What the asynchronous driver promises its consumer, over an in-memory
//! store: a subscription is named before anything is said about it, a
//! registration served from a tree the engine already holds needs no
//! storage read and still reports itself complete, and one committed
//! transaction reaches the consumer as one batch of deltas whatever else
//! is happening. These are the properties the client side builds its
//! pokes on; each was a live defect before it was pinned here.

use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::{LocalSet, spawn_local};
use xyne_sync::ivm::MultiTableIVM;
use xyne_sync::model::*;
use xyne_sync::sync::{Command, Event, Lsn, MemoryStorage, Service, SubId};

/// The one client every subscription here belongs to.
const CLIENT: ClientId = ClientId(1);

/// Run `body` on a current-thread runtime with a local task set (the
/// driver and its storage are single-threaded).
fn block_on<F: std::future::Future>(body: F) -> F::Output {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    LocalSet::new().block_on(&runtime, body)
}

/// The primary key of `id`.
fn pkey(id: i64) -> HashMap<ColumnName, Value> {
    HashMap::from([("id".into(), Value::Int(id))])
}

/// One full ticket row: every column the table declares.
fn ticket(id: i64, status: &str) -> DataFrameRow {
    DataFrameRow {
        data: HashMap::from([
            ("id".into(), Value::Int(id)),
            ("status".into(), Value::String(status.to_owned())),
        ]),
    }
}

/// An insert of `ticket(id, status)`.
fn insert(id: i64, status: &str) -> WriteQuery {
    WriteQuery::INSERT(InsertQuery {
        table: TableName::from("tickets"),
        pkey_value: DataFrameKey::new(pkey(id)),
        record: ticket(id, status),
    })
}

/// The one query these scenarios subscribe to: the open tickets.
fn open_tickets() -> MultiTableReadQuery {
    MultiTableReadQuery::single(SingleTableReadQuery::new(
        "tickets",
        Where::condition("status", ComparisonOperator::EQ, "OPEN"),
        OrderBy::new("id", Order::ASC),
        u32::MAX,
    ))
}

/// A driver over `storage`, with the events it sends and the commands it
/// takes; the driver runs as a task of the caller's local set.
fn start(
    storage: Rc<MemoryStorage>,
) -> (
    mpsc::Sender<Command<MultiTableReadQuery>>,
    mpsc::UnboundedReceiver<Event>,
) {
    let (events_tx, events) = mpsc::unbounded_channel();
    let (service, commands) = Service::new(MultiTableIVM::new(), storage, events_tx);
    spawn_local(service.run());
    (commands, events)
}

/// Every event the driver sends within a short window, in order.
async fn drain(events: &mut mpsc::UnboundedReceiver<Event>) -> Vec<Event> {
    let mut seen = Vec::new();
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(200), events.recv()).await
    {
        seen.push(event);
    }
    seen
}

/// The names of the events, for readable assertions.
fn shapes(events: &[Event]) -> Vec<&'static str> {
    events
        .iter()
        .map(|event| match event {
            Event::Registered { .. } => "registered",
            Event::Updates(_) => "updates",
            Event::Hydrated(_) => "hydrated",
            Event::Landed => "landed",
            Event::Moved { .. } => "moved",
        })
        .collect()
}

/// The rows of every `Updates` event, as ids.
fn ids(events: &[Event]) -> Vec<i64> {
    let mut out = Vec::new();
    for event in events {
        if let Event::Updates(updates) = event {
            for update in updates {
                if let DataFrameOperation::Add(key, _) = &update.op
                    && let Some(Value::Int(id)) = key.pkey_value.get(&ColumnName::from("id"))
                {
                    out.push(*id);
                }
            }
        }
    }
    out.sort_unstable();
    out
}

/// The subscription a registration became, and the token it answers.
fn registered(events: &[Event]) -> Option<(u64, SubId)> {
    events.iter().find_map(|event| match event {
        Event::Registered { token, sub } => Some((*token, *sub)),
        _ => None,
    })
}

/// A registration is announced before anything is said about it: the
/// first event is the subscription's id, carrying the token the command
/// was sent with, and only then do its rows and its completion follow.
/// Without that order a consumer meets a `SubId` it cannot place and
/// drops the rows and the completion on the floor.
#[test]
fn a_registration_is_named_before_its_rows() {
    block_on(async {
        let storage = Rc::new(MemoryStorage::new());
        storage.apply(&insert(1, "OPEN"));
        storage.apply(&insert(2, "DONE"));
        let (commands, mut events) = start(storage);

        commands
            .send(Command::Register {
                client: CLIENT,
                query: open_tickets(),
                token: 7,
            })
            .await
            .expect("send");
        let seen = drain(&mut events).await;

        assert_eq!(
            shapes(&seen).first(),
            Some(&"registered"),
            "the subscription is named first, got {:?}",
            shapes(&seen)
        );
        let (token, sub) = registered(&seen).expect("the registration is answered");
        assert_eq!(token, 7, "the token comes back unchanged");
        assert_eq!(ids(&seen), vec![1], "only the open ticket is delivered");
        assert!(
            seen.iter().any(|event| matches!(
                event,
                Event::Hydrated(subs) if subs.contains(&sub)
            )),
            "the subscription reports itself complete, got {:?}",
            shapes(&seen)
        );
    });
}

/// A second subscription to a query the engine already holds is served
/// from that tree: it is named, given its rows and reported complete
/// without a storage read, so a consumer that waits for a landing before
/// telling the client would wait for something that never comes.
#[test]
fn a_twin_registration_completes_without_a_read() {
    block_on(async {
        let storage = Rc::new(MemoryStorage::new());
        storage.apply(&insert(1, "OPEN"));
        let (commands, mut events) = start(storage);

        commands
            .send(Command::Register {
                client: CLIENT,
                query: open_tickets(),
                token: 1,
            })
            .await
            .expect("send");
        let first = drain(&mut events).await;
        assert!(
            first.iter().any(|event| matches!(event, Event::Landed)),
            "the first registration reads storage, got {:?}",
            shapes(&first)
        );

        commands
            .send(Command::Register {
                client: ClientId(2),
                query: open_tickets(),
                token: 2,
            })
            .await
            .expect("send");
        let twin = drain(&mut events).await;

        let (token, sub) = registered(&twin).expect("the twin is answered");
        assert_eq!(token, 2, "the twin's own token comes back");
        assert_eq!(ids(&twin), vec![1], "the twin is given the held row");
        assert!(
            twin.iter().any(|event| matches!(
                event,
                Event::Hydrated(subs) if subs.contains(&sub)
            )),
            "the twin reports itself complete, got {:?}",
            shapes(&twin)
        );
        assert!(
            !twin.iter().any(|event| matches!(event, Event::Landed)),
            "the twin needs no storage read, got {:?}",
            shapes(&twin)
        );
    });
}

/// One committed transaction reaches the consumer as one batch of
/// deltas: the driver routes every write of a commit in a single
/// synchronous step, so nothing a client is told can be cut in the middle
/// of a commit, and the rows of that commit travel together.
#[test]
fn a_commit_arrives_as_one_batch() {
    block_on(async {
        let storage = Rc::new(MemoryStorage::new());
        let (commands, mut events) = start(storage);

        commands
            .send(Command::Register {
                client: CLIENT,
                query: open_tickets(),
                token: 1,
            })
            .await
            .expect("send");
        drain(&mut events).await;

        commands
            .send(Command::Commit {
                writes: vec![insert(10, "OPEN"), insert(11, "OPEN"), insert(12, "DONE")],
                at: Lsn(100),
            })
            .await
            .expect("send");
        let seen = drain(&mut events).await;

        assert_eq!(
            shapes(&seen)
                .iter()
                .filter(|shape| **shape == "updates")
                .count(),
            1,
            "the commit's rows travel as one batch, got {:?}",
            shapes(&seen)
        );
        assert_eq!(
            ids(&seen),
            vec![10, 11],
            "both open tickets of the commit are delivered together"
        );
    });
}

/// A progress mark reports where the engine now is, which is what a
/// consumer batching per transaction flushes on.
#[test]
fn progress_reports_the_position() {
    block_on(async {
        let storage = Rc::new(MemoryStorage::new());
        let (commands, mut events) = start(storage);

        commands
            .send(Command::Commit {
                writes: vec![insert(1, "OPEN")],
                at: Lsn(42),
            })
            .await
            .expect("send");
        commands
            .send(Command::Progress(Lsn(42)))
            .await
            .expect("send");
        let seen = drain(&mut events).await;

        let moved = seen.iter().find_map(|event| match event {
            Event::Moved { position, .. } => Some(*position),
            _ => None,
        });
        assert_eq!(
            moved,
            Some(Lsn(42)),
            "the progress mark is reported, got {:?}",
            shapes(&seen)
        );
    });
}
