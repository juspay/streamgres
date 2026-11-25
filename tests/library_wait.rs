//! How a read waits, in every way a caller can wait for it: `.wait()` on a
//! plain thread, on a current-thread runtime and on a multi-thread worker,
//! and `.await` on Tokio and on an executor that is not Tokio.
//!
//! Each read is wrapped in a future that counts its polls. A memory hit is
//! ready on the first poll: no waker kept, no hop to Streamgres's runtime.
//! A slow miss (a fallback read that sleeps in Postgres) must take its
//! time yet be polled only a few times: the caller slept until
//! Streamgres's runtime woke it, instead of polling in a loop.
//!
//! Set `STREAMGRES_PG_DSN` (as for `library_live`); without it every test
//! here is skipped.

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use streamgres::cache::{OnUnsupported, Rows, Served, Sql, Streamgres, Trace, block_on};

/// How long the slow miss sleeps in Postgres.
const SLEEP: Duration = Duration::from_millis(500);

/// The most polls a slow miss may take: the first one, the one after its
/// wake, and room for one spurious wake.
const MAX_MISS_POLLS: usize = 3;

fn dsn() -> Option<String> {
    let dsn = std::env::var("STREAMGRES_PG_DSN").ok();
    if dsn.is_none() {
        eprintln!("STREAMGRES_PG_DSN not set: read-waiting scenarios skipped");
    }
    dsn
}

/// Setups and cleanups one at a time, as in `library_live`: each one's DDL
/// fires every installed DDL trigger.
static SETUP: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Runs `sql` as the test's administrator, on a runtime of its own so a
/// test without one can call it.
fn admin(dsn: &str, sql: &str) {
    let _setup = SETUP.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let (dsn, sql) = (dsn.to_owned(), sql.to_owned());
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("admin runtime")
            .block_on(async {
                let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                    .await
                    .expect("connect");
                tokio::spawn(connection);
                client.batch_execute(&sql).await.expect(&sql);
            });
    })
    .join()
    .expect("admin thread");
}

/// A fresh schema with one row, its publication and `app`'s trigger stack.
fn prepare(dsn: &str, schema: &str, app: &str) -> streamgres::cache::Builder {
    let builder = Streamgres::connect(dsn)
        .publication(format!("{schema}_pub"))
        .schemas([schema])
        .app(app, 0)
        // The slow miss is a shape the engine does not answer.
        .on_unsupported(OnUnsupported::Fallback);
    admin(
        dsn,
        &format!(
            "DROP PUBLICATION IF EXISTS {schema}_pub;
             DROP SCHEMA IF EXISTS {schema} CASCADE;
             CREATE SCHEMA {schema};
             CREATE TABLE {schema}.t (id bigint PRIMARY KEY);
             INSERT INTO {schema}.t VALUES (1);
             CREATE PUBLICATION {schema}_pub FOR TABLES IN SCHEMA {schema};
             {}",
            builder.setup_sql()
        ),
    );
    builder
}

fn clean(dsn: &str, schema: &str, app: &str) {
    admin(
        dsn,
        &format!(
            "DROP PUBLICATION IF EXISTS {schema}_pub;
             DROP SCHEMA IF EXISTS {schema} CASCADE;
             DROP SCHEMA IF EXISTS {app}_0 CASCADE;"
        ),
    );
}

/// A miss that Postgres takes [`SLEEP`] to answer.
fn slow() -> Sql {
    Sql::new(format!(
        "SELECT 1::int8 AS one FROM pg_sleep({})",
        SLEEP.as_secs_f64()
    ))
}

fn kept(schema: &str) -> Sql {
    Sql::new(format!("SELECT id FROM {schema}.t"))
}

/// A future that counts how often it is polled.
struct Counted<F> {
    inner: Pin<Box<F>>,
    polls: Arc<AtomicUsize>,
}

impl<F: Future> Future for Counted<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        self.inner.as_mut().poll(cx)
    }
}

/// `work`, counted: the future and its poll counter.
fn counted<F: IntoFuture>(work: F) -> (Counted<F::IntoFuture>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let future = Counted {
        inner: Box::pin(work.into_future()),
        polls: polls.clone(),
    };
    (future, polls)
}

/// One read's outcome: its trace, polls and time taken.
struct Waited {
    trace: Trace,
    rows: Rows,
    polls: usize,
    took: Duration,
}

/// Checks a slow miss: Postgres answered, it took its time, and the
/// caller was polled only on wakes.
fn assert_slept(how: &str, waited: &Waited) {
    assert_eq!(waited.trace.served, Served::Postgres, "{how}: {}", waited.trace);
    assert_eq!(waited.rows.len(), 1, "{how}");
    assert!(waited.took >= SLEEP, "{how}: took {:?}", waited.took);
    assert!(
        waited.polls <= MAX_MISS_POLLS,
        "{how}: polled {} times while waiting {:?}",
        waited.polls,
        waited.took
    );
}

/// Checks a hit: memory answered on the first poll.
fn assert_hit(how: &str, waited: &Waited) {
    assert_eq!(waited.trace.served, Served::Memory, "{how}: {}", waited.trace);
    assert_eq!(waited.polls, 1, "{how}: a hit polled {} times", waited.polls);
}

/// How a blocking caller runs a read to completion.
#[derive(Clone, Copy)]
enum Drive {
    /// Streamgres's own `block_on`: what `.wait()` does.
    Wait,
    /// An executor that is not Tokio ([`own_executor`]).
    Own,
}

/// Waits for one read of `sql`, driven as `drive` says, counting polls.
fn waited(lib: &Streamgres, sql: Sql, drive: Drive) -> Waited {
    let (future, polls) = counted(lib.load_and_sync(sql).inspect());
    let started = Instant::now();
    let (rows, trace) = match drive {
        Drive::Wait => block_on(future),
        Drive::Own => own_executor(future),
    };
    Waited {
        rows: rows.expect("read"),
        trace,
        polls: polls.load(Ordering::Relaxed),
        took: started.elapsed(),
    }
}

/// Reads `sql` as `drive` says until memory answers it, then checks that
/// a hit takes one poll. A hit right after the reader's authorization
/// check expires (every five seconds) goes to Streamgres's runtime once,
/// so a few attempts are allowed.
fn assert_hits(how: &str, lib: &Streamgres, sql: &Sql, drive: Drive) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while waited(lib, sql.clone(), drive).trace.served != Served::Memory {
        assert!(Instant::now() < deadline, "{how}: never answered from memory");
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut last = None;
    for _ in 0..3 {
        let hit = waited(lib, sql.clone(), drive);
        if hit.trace.served == Served::Memory && hit.polls == 1 {
            return assert_hit(how, &hit);
        }
        last = Some(hit);
    }
    assert_hit(how, &last.expect("an attempt"));
}

/// An executor that is not Tokio, written here: poll, park until woken.
fn own_executor<F: Future>(future: F) -> F::Output {
    struct Unpark(std::thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        std::thread::park();
    }
}

/// `.wait()` (Streamgres's `block_on`) and `.await` on an executor that is
/// not Tokio, from a plain thread with no runtime at all.
#[test]
fn a_plain_thread_sleeps_until_woken() {
    let Some(dsn) = dsn() else { return };
    let (schema, app) = ("lib_wait_plain", "libwaitplain");
    let builder = prepare(&dsn, schema, app);
    assert!(tokio::runtime::Handle::try_current().is_err(), "no runtime here");
    let lib = builder.start().wait().expect("start");

    assert_slept(".wait() on a plain thread", &waited(&lib, slow(), Drive::Wait));
    assert_slept("own executor on a plain thread", &waited(&lib, slow(), Drive::Own));
    assert_hits(".wait() on a plain thread", &lib, &kept(schema), Drive::Wait);
    assert_hits("own executor on a plain thread", &lib, &kept(schema), Drive::Own);

    // The public `.wait()` is the same `block_on` over the same future.
    let started = Instant::now();
    let rows = lib.load_and_sync(slow()).wait().expect("waited read");
    assert_eq!(rows.len(), 1);
    assert!(started.elapsed() >= SLEEP);

    lib.shutdown().wait();
    clean(&dsn, schema, app);
}

/// `.wait()` on the only thread of a current-thread runtime: it neither
/// deadlocks nor polls in a loop. The thread is blocked meanwhile, so the
/// runtime's other tasks wait: that is the cost of `.wait()` there.
#[tokio::test(flavor = "current_thread")]
async fn wait_on_a_current_thread_runtime_sleeps_and_blocks_only_it() {
    let Some(dsn) = dsn() else { return };
    let (schema, app) = ("lib_wait_current", "libwaitcurrent");
    let builder = prepare(&dsn, schema, app);
    let lib = builder.start().wait().expect("start");

    let ticks = ticker();
    tokio::task::yield_now().await;
    let before = ticks.load(Ordering::Relaxed);
    let miss = waited(&lib, slow(), Drive::Wait);
    assert_slept(".wait() on a current-thread runtime", &miss);
    assert_eq!(
        ticks.load(Ordering::Relaxed),
        before,
        "the runtime's only thread was blocked, so its other tasks waited"
    );
    assert_hits(".wait() on a current-thread runtime", &lib, &kept(schema), Drive::Wait);

    lib.shutdown().wait();
    clean(&dsn, schema, app);
}

/// `.wait()` on the only worker of a multi-thread runtime: the worker's
/// other tasks move to another thread while it waits, so they keep running.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn wait_on_a_multi_thread_worker_lets_other_tasks_run() {
    let Some(dsn) = dsn() else { return };
    let (schema, app) = ("lib_wait_worker", "libwaitworker");
    let builder = prepare(&dsn, schema, app);
    let lib = builder.start().await.expect("start");

    let ticks = ticker();
    let reader = lib.clone();
    let (miss, ticked) = tokio::spawn(async move {
        let before = ticks.load(Ordering::Relaxed);
        let miss = waited(&reader, slow(), Drive::Wait);
        (miss, ticks.load(Ordering::Relaxed) - before)
    })
    .await
    .expect("worker task");
    assert_slept(".wait() on a multi-thread worker", &miss);
    assert!(
        ticked >= 10,
        "other tasks kept running while a worker waited: {ticked} ticks"
    );
    let hit_lib = lib.clone();
    let schema_owned = schema.to_owned();
    tokio::spawn(async move {
        assert_hits(".wait() on a multi-thread worker", &hit_lib, &kept(&schema_owned), Drive::Wait);
    })
    .await
    .expect("hit task");

    lib.shutdown().await;
    clean(&dsn, schema, app);
}

/// `.await` on Tokio: the task is polled on wakes only, and its thread is
/// free for other tasks meanwhile, even with one worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn await_on_tokio_frees_the_thread() {
    let Some(dsn) = dsn() else { return };
    let (schema, app) = ("lib_wait_await", "libwaitawait");
    let builder = prepare(&dsn, schema, app);
    let lib = builder.start().await.expect("start");

    let ticks = ticker();
    let reader = lib.clone();
    let (miss, ticked) = tokio::spawn(async move {
        let before = ticks.load(Ordering::Relaxed);
        let (future, polls) = counted(reader.load_and_sync(slow()).inspect());
        let started = Instant::now();
        let (rows, trace) = future.await;
        let miss = Waited {
            rows: rows.expect("read"),
            trace,
            polls: polls.load(Ordering::Relaxed),
            took: started.elapsed(),
        };
        (miss, ticks.load(Ordering::Relaxed) - before)
    })
    .await
    .expect("awaiting task");
    assert_slept(".await on Tokio", &miss);
    assert!(
        ticked >= 10,
        "the worker ran other tasks while the read waited: {ticked} ticks"
    );

    let sql = kept(schema);
    let deadline = Instant::now() + Duration::from_secs(10);
    while lib.load_and_sync(sql.clone()).inspect().await.1.served != Served::Memory {
        assert!(Instant::now() < deadline, "never answered from memory");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut hit = None;
    for _ in 0..3 {
        let (future, polls) = counted(lib.load_and_sync(sql.clone()).inspect());
        let (rows, trace) = future.await;
        let read = Waited {
            rows: rows.expect("read"),
            trace,
            polls: polls.load(Ordering::Relaxed),
            took: Duration::ZERO,
        };
        let done = read.trace.served == Served::Memory && read.polls == 1;
        hit = Some(read);
        if done {
            break;
        }
    }
    assert_hit(".await on Tokio", &hit.expect("an attempt"));

    lib.shutdown().await;
    clean(&dsn, schema, app);
}

/// A task on the current runtime counting every 10 ms.
fn ticker() -> Arc<AtomicUsize> {
    let ticks = Arc::new(AtomicUsize::new(0));
    let counter = ticks.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            counter.fetch_add(1, Ordering::Relaxed);
        }
    });
    ticks
}

/// A warm hit through `.wait()` inside a `LocalSet` on a multi-thread
/// runtime returns at once: a hit needs no blocking machinery, and a
/// `LocalSet` does not allow `block_in_place`.
#[tokio::test(flavor = "multi_thread")]
async fn a_warm_hit_waits_inside_a_local_set() {
    let Some(dsn) = dsn() else { return };
    let (schema, app) = ("lib_wait_local", "libwaitlocal");
    let builder = prepare(&dsn, schema, app);
    let lib = builder.start().await.expect("start");
    let sql = kept(schema);
    let deadline = Instant::now() + Duration::from_secs(10);
    while lib.load_and_sync(sql.clone()).inspect().await.1.served != Served::Memory {
        assert!(Instant::now() < deadline, "never answered from memory");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let local = tokio::task::LocalSet::new();
    let (rows, trace) = local
        .run_until(async { lib.load_and_sync(sql.clone()).inspect().wait() })
        .await;
    assert_eq!(rows.expect("warm read").len(), 1);
    assert_eq!(trace.served, Served::Memory, "{trace}");
    lib.shutdown().await;
    clean(&dsn, schema, app);
}

/// `.wait()` on a miss from the thread that runs a multi-thread runtime's
/// `block_on` (the body of `#[tokio::main]`), which is not a worker.
#[tokio::test(flavor = "multi_thread")]
async fn wait_on_a_multi_thread_runtime_s_own_thread() {
    let Some(dsn) = dsn() else { return };
    let (schema, app) = ("lib_wait_main", "libwaitmain");
    let builder = prepare(&dsn, schema, app);
    let lib = builder.start().await.expect("start");
    assert_slept(
        ".wait() on the runtime's own thread",
        &waited(&lib, slow(), Drive::Wait),
    );
    lib.shutdown().await;
    clean(&dsn, schema, app);
}
