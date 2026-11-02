//! The async wrapper's instrumentation, which hears of a query only when
//! a read runs it.
//!
//! diesel-async tells the instrumentation a query started when its
//! future is built, and finished when that future completes. A miss
//! builds the futures it may need before it knows which ([`super::asynchronous`]
//! says why): the read of Postgres, the replay position, the catalog
//! lookup. The engine then serves most misses itself, and a future
//! dropped unpolled never finishes, so each such miss would leave queries
//! started that never end: a gauge of queries in flight that only grows,
//! a span that never closes.
//!
//! So the wrapper keeps the application's instrumentation itself, and the
//! inner connection's is a forwarder to it ([`Instrumented`]). Every
//! future the wrapper builds on the inner connection is built through
//! [`Instrumented::hold`], which holds its start back until the future is
//! first polled ([`Held`]): a future dropped unpolled, as one is that a
//! read never needed, or that waited for a fallback permit and gave up,
//! never started as far as the instrumentation knows. One dropped after
//! it started and before it finished, a read cancelled in the middle of
//! its query, is told finished then, with an error saying so. Every other
//! event passes straight through.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use diesel::connection::{Instrumentation, InstrumentationEvent, StrQueryHelper};

thread_local! {
    /// While [`Instrumented::hold`] builds a future on this thread: the
    /// text of the query start it held back, once one is sent.
    static HOLDING: RefCell<Option<Option<String>>> = const { RefCell::new(None) };
}

/// The application's instrumentation, shared by the wrapper and the
/// forwarder it installs on its inner connection.
#[derive(Clone)]
pub(super) struct Instrumented(Arc<Mutex<Option<Box<dyn Instrumentation>>>>);

impl Instrumented {
    /// Forwarding to `instrumentation`, or to nothing.
    pub(super) fn new(instrumentation: Option<Box<dyn Instrumentation>>) -> Self {
        Instrumented(Arc::new(Mutex::new(instrumentation)))
    }

    /// Forwarding to `instrumentation` from here on, for every connection
    /// and future sharing this one.
    pub(super) fn set(&self, instrumentation: Box<dyn Instrumentation>) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = Some(instrumentation);
    }

    /// The future `build` makes, with the start of the query it sends
    /// held back until the future is first polled.
    pub(super) fn hold<F>(&self, build: impl FnOnce() -> F) -> Held<F> {
        HOLDING.with(|holding| *holding.borrow_mut() = Some(None));
        let future = build();
        let start = HOLDING.with(|holding| holding.borrow_mut().take()).flatten();
        Held {
            future,
            start,
            running: None,
            instrumented: self.clone(),
        }
    }

    /// Passes `event` on to the application's instrumentation.
    fn tell(&self, event: InstrumentationEvent<'_>) {
        if let Some(instrumentation) = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            instrumentation.on_connection_event(event);
        }
    }
}

impl Instrumentation for Instrumented {
    /// Holds back a query's start while [`Instrumented::hold`] builds
    /// its future; passes every other event on.
    fn on_connection_event(&mut self, event: InstrumentationEvent<'_>) {
        if let InstrumentationEvent::StartQuery { query, .. } = &event {
            let held = HOLDING.with(|holding| match &mut *holding.borrow_mut() {
                Some(slot @ None) => {
                    *slot = Some(query.to_string());
                    true
                }
                _ => false,
            });
            if held {
                return;
            }
        }
        self.tell(event);
    }
}

/// A future whose query's start is held back until its first poll, and
/// whose finish is told when it is dropped started and unfinished.
/// diesel-async tells the finish of one that completes.
pub(super) struct Held<F> {
    future: F,
    /// The start held back, told at the first poll.
    start: Option<String>,
    /// The query's text from its start until the future completes.
    running: Option<String>,
    instrumented: Instrumented,
}

impl<F: Future + Unpin> Future for Held<F> {
    type Output = F::Output;

    /// Tells the start at the first poll, then polls the future.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = &mut *self;
        if let Some(sql) = this.start.take() {
            this.instrumented
                .tell(InstrumentationEvent::start_query(&StrQueryHelper::new(&sql)));
            this.running = Some(sql);
        }
        let polled = Pin::new(&mut this.future).poll(cx);
        if polled.is_ready() {
            this.running = None;
        }
        polled
    }
}

impl<F> Drop for Held<F> {
    /// Tells a started query finished, with an error, when it is dropped
    /// before it completed.
    fn drop(&mut self) {
        if let Some(sql) = self.running.take() {
            let dropped = diesel::result::Error::QueryBuilderError(
                "the query's future was dropped before it finished".into(),
            );
            self.instrumented.tell(InstrumentationEvent::finish_query(
                &StrQueryHelper::new(&sql),
                Some(&dropped),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// Counts starts and finishes.
    struct Count(Arc<(AtomicUsize, AtomicUsize)>);

    impl Instrumentation for Count {
        fn on_connection_event(&mut self, event: InstrumentationEvent<'_>) {
            match event {
                InstrumentationEvent::StartQuery { .. } => self.0.0.fetch_add(1, Ordering::SeqCst),
                InstrumentationEvent::FinishQuery { .. } => self.0.1.fetch_add(1, Ordering::SeqCst),
                _ => 0,
            };
        }
    }

    /// A start sent while a future is built is held: dropped unpolled,
    /// the instrumentation hears nothing; polled, it hears the start; one
    /// that completes is finished by diesel-async, not here; one dropped
    /// pending after its start is told finished. A start sent outside
    /// `hold` passes straight through, as does a finish.
    #[test]
    fn a_start_is_told_at_the_first_poll_and_a_dropped_one_finished() {
        let counts = Arc::new((AtomicUsize::new(0), AtomicUsize::new(0)));
        let instrumented = Instrumented::new(Some(Box::new(Count(Arc::clone(&counts)))));
        let forwarder = instrumented.clone();
        let heard = || (counts.0.load(Ordering::SeqCst), counts.1.load(Ordering::SeqCst));
        let build = |sql: &'static str| {
            let mut forwarder = forwarder.clone();
            move || {
                forwarder.on_connection_event(InstrumentationEvent::start_query(
                    &StrQueryHelper::new(sql),
                ));
            }
        };

        let unpolled = instrumented.hold(|| {
            build("SELECT 1")();
            std::future::ready(1)
        });
        assert_eq!(heard(), (0, 0), "held");
        drop(unpolled);
        assert_eq!(heard(), (0, 0), "dropped unpolled: never started");

        let completed = instrumented.hold(|| {
            build("SELECT 2")();
            std::future::ready(2)
        });
        assert_eq!(futures_util::FutureExt::now_or_never(completed), Some(2));
        assert_eq!(heard(), (1, 0), "started; its finish is diesel-async's to tell");

        let cancelled = instrumented.hold(|| {
            build("SELECT pg_sleep(1)")();
            std::future::pending::<()>()
        });
        assert_eq!(futures_util::FutureExt::now_or_never(cancelled), None);
        assert_eq!(heard(), (2, 1), "dropped after its start: told finished");

        let mut outside = forwarder.clone();
        outside.on_connection_event(InstrumentationEvent::start_query(&StrQueryHelper::new("SELECT 3")));
        outside.on_connection_event(InstrumentationEvent::finish_query(&StrQueryHelper::new("SELECT 3"), None));
        assert_eq!(heard(), (3, 2), "outside hold: passed on");
    }
}
