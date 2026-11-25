//! Synchronous Diesel code reading through Streamgres with no runtime of
//! its own: `diesel::PgConnection` for writes, the same `load_and_sync`
//! ended with `.wait()` for reads, on a plain test thread.

use std::future::{Future, IntoFuture};
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::Thread;
use std::time::{Duration, Instant};

use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use streamgres_diesel::{LoadAndSync, Served, Streamgres, Ttl};

diesel::table! {
    desk_sync.tickets (id) {
        id -> Int8,
        team_id -> Int8,
        title -> Text,
    }
}

/// A model that is not `Send`: decoded on the calling thread by `.wait()`.
#[derive(Debug, Queryable)]
struct LocalTitle {
    #[diesel(deserialize_as = String)]
    title: std::rc::Rc<str>,
}

#[derive(Debug, Clone, PartialEq, Queryable, Selectable)]
#[diesel(table_name = tickets)]
struct Ticket {
    id: i64,
    team_id: i64,
    title: String,
}

/// Polls `future` on this thread, parking between polls: an executor that
/// is not Tokio.
fn block_on<F: Future>(future: F) -> F::Output {
    struct Unpark(Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}

#[test]
fn sync_diesel_reads_need_no_runtime() {
    let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
        eprintln!("STREAMGRES_PG_DSN not set: synchronous Diesel scenario skipped");
        return;
    };
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "the test thread has no Tokio runtime"
    );
    let mut conn = PgConnection::establish(&dsn).expect("connect");
    conn.batch_execute(
        "DROP PUBLICATION IF EXISTS desk_sync_pub;
         DROP SCHEMA IF EXISTS desk_sync CASCADE;
         CREATE SCHEMA desk_sync;
         CREATE TABLE desk_sync.tickets (id int8 PRIMARY KEY, team_id int8 NOT NULL, title text NOT NULL);
         INSERT INTO desk_sync.tickets VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c');
         CREATE PUBLICATION desk_sync_pub FOR TABLES IN SCHEMA desk_sync;",
    )
    .expect("prepare");
    let builder = Streamgres::connect(&dsn)
        .publication("desk_sync_pub")
        .schemas(["desk_sync"])
        .app("desksync", 0)
        .max_lag(Duration::from_secs(5));
    conn.batch_execute(&builder.setup_sql())
        .expect("trigger stack");
    let lib = builder.start().wait().expect("start");

    let team = || {
        tickets::table
            .filter(tickets::team_id.eq(1))
            .order(tickets::id)
            .select(Ticket::as_select())
    };
    let direct: Vec<Ticket> = team().load(&mut conn).expect("Diesel's own read");

    // The first read registers the query and Postgres answers it.
    let (first, trace) = team().load_and_sync::<Ticket>(&lib).inspect().wait();
    assert_eq!(first.expect("first read"), direct);
    assert_ne!(trace.expect("a trace").served, Served::Memory);

    // Then memory does, on this thread.
    let until_memory = |want: &dyn Fn(&[Ticket]) -> bool| {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let (rows, trace) = team()
                .load_and_sync::<Ticket>(&lib)
                .ttl(Ttl::Forever)
                .inspect()
                .wait();
            let rows = rows.expect("read");
            if trace.expect("a trace").served == Served::Memory && want(&rows) {
                return rows;
            }
            assert!(Instant::now() < deadline, "memory never answered: {rows:?}");
            std::thread::sleep(Duration::from_millis(20));
        }
    };
    assert_eq!(until_memory(&|_| true), direct);
    let plain: Vec<Ticket> = team().load_and_sync(&lib).wait().expect("plain read");
    assert_eq!(plain, direct);
    let kept: Vec<Ticket> = team()
        .load_and_sync(&lib)
        .ttl(Duration::from_secs(60))
        .wait()
        .expect("read with a ttl");
    assert_eq!(kept, direct);

    // A write through Diesel reaches memory through the change feed.
    diesel::update(tickets::table.find(2))
        .set(tickets::title.eq("b2"))
        .execute(&mut conn)
        .expect("write");
    let changed = until_memory(&|rows| rows.iter().any(|ticket| ticket.title == "b2"));
    assert_eq!(
        changed,
        team().load::<Ticket>(&mut conn).expect("Diesel's own read")
    );

    // An unsupported statement is an error, not a fallback.
    let (refused, trace) = tickets::table
        .filter(tickets::title.ilike("%b%"))
        .select(Ticket::as_select())
        .load_and_sync::<Ticket>(&lib)
        .inspect()
        .wait();
    assert!(refused.is_err());
    assert_eq!(trace.expect("a trace").served, Served::Unsupported);

    // A model that is not `Send` reads through `.wait()` too.
    let titles: Vec<LocalTitle> = tickets::table
        .filter(tickets::team_id.eq(1))
        .order(tickets::id)
        .select((tickets::title,))
        .load_and_sync(&lib)
        .wait()
        .expect("a thread-local model");
    assert_eq!(titles.len(), changed.len());
    assert_eq!(&*titles[1].title, "b2");

    // The async read is awaitable on an executor that is not Tokio.
    let awaited: Vec<Ticket> =
        block_on(team().load_and_sync(&lib).into_future()).expect("awaited read");
    assert_eq!(awaited, changed);
    let other_team = tickets::table
        .filter(tickets::team_id.eq(2))
        .select(Ticket::as_select());
    let fallback: Vec<Ticket> =
        block_on(other_team.load_and_sync(&lib).into_future()).expect("awaited fallback read");
    assert_eq!(fallback.len(), 1);

    lib.shutdown().wait();
    drop(lib);
    conn.batch_execute(
        "DROP PUBLICATION desk_sync_pub;
         DROP SCHEMA desk_sync CASCADE;
         DROP SCHEMA desksync_0 CASCADE;",
    )
    .expect("clean up");
}
