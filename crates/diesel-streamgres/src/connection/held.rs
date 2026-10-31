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
//! inner connection's is a forwarder to it ([`Instrumented`]). A future
//! built through [`Instrumented::hold`] has its start held back and
//! passed on only when the read runs it ([`Held::run`]); dropped, the
//! query was never started as far as the instrumentation knows. Every
//! other event passes straight through.

use std::cell::RefCell;
use std::sync::{Arc, Mutex, PoisonError};

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
    /// held back until the read runs it.
    pub(super) fn hold<F>(&self, build: impl FnOnce() -> F) -> Held<F> {
        HOLDING.with(|holding| *holding.borrow_mut() = Some(None));
        let future = build();
        let start = HOLDING.with(|holding| holding.borrow_mut().take()).flatten();
        Held {
            future,
            start,
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

/// A future whose query's start is held back: told when the read runs
/// it, never when it is dropped unrun.
pub(super) struct Held<F> {
    future: F,
    start: Option<String>,
    instrumented: Instrumented,
}

impl<F> Held<F> {
    /// The future, its query's start told now that the read runs it; its
    /// finish follows as diesel-async tells it.
    pub(super) fn run(self) -> F {
        if let Some(sql) = &self.start {
            self.instrumented
                .tell(InstrumentationEvent::start_query(&StrQueryHelper::new(sql)));
        }
        self.future
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

    /// A start sent while a future is built is held: dropped unrun, the
    /// instrumentation hears nothing; run, it hears the start then. A
    /// start sent outside `hold` passes straight through.
    #[test]
    fn a_start_is_told_only_when_the_future_runs() {
        let counts = Arc::new((AtomicUsize::new(0), AtomicUsize::new(0)));
        let instrumented = Instrumented::new(Some(Box::new(Count(Arc::clone(&counts)))));
        let mut forwarder = instrumented.clone();
        let starts = || counts.0.load(Ordering::SeqCst);

        let unrun = instrumented.hold(|| {
            forwarder.on_connection_event(InstrumentationEvent::start_query(&StrQueryHelper::new(
                "SELECT 1",
            )));
        });
        assert_eq!(starts(), 0, "held");
        drop(unrun);
        assert_eq!(starts(), 0, "dropped unrun: never started");

        let run = instrumented.hold(|| {
            forwarder.on_connection_event(InstrumentationEvent::start_query(&StrQueryHelper::new(
                "SELECT 2",
            )));
        });
        assert_eq!(run.start.as_deref(), Some("SELECT 2"));
        run.run();
        assert_eq!(starts(), 1, "run: started");

        forwarder.on_connection_event(InstrumentationEvent::start_query(&StrQueryHelper::new(
            "SELECT 3",
        )));
        assert_eq!(starts(), 2, "outside hold: passed on");
        forwarder.on_connection_event(InstrumentationEvent::finish_query(
            &StrQueryHelper::new("SELECT 3"),
            None,
        ));
        assert_eq!(counts.1.load(Ordering::SeqCst), 1, "a finish passes on");
    }
}
