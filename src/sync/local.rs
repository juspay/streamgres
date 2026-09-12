//! The synchronous driver: for storage that answers at once (the
//! in-process [`super::MemoryStorage`]), every read the runtime hands out
//! is run inline and landed before the call returns, so registration and
//! routing keep the plain call-and-return shape of the engine itself.
//! This is the driver the test suites, the demo and the benchmark use.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use super::runtime::{Runtime, Step};
use super::storage::Storage;
use crate::ivm::Engine;
use crate::model::{Lsn, SubId, WriteQuery};

/// A runtime over an engine and an immediately-answering storage.
///
/// - `clock`: the driver's own write counter, the location every routed
///   write is committed at; reads of an in-process store are positioned
///   at zero, below all of them.
pub struct Local<E: Engine, S: Storage> {
    runtime: Runtime<E>,
    storage: Rc<S>,
    clock: u64,
}

impl<E: Engine, S: Storage> Local<E, S> {
    /// A driver owning `engine` and reading from `storage`.
    pub fn new(engine: E, storage: Rc<S>) -> Self {
        Local {
            runtime: Runtime::new(engine),
            storage,
            clock: 0,
        }
    }

    /// Register a subscription and return its id and its complete initial
    /// snapshot, every read it needed already landed.
    pub fn register_query(&mut self, query: E::Query) -> (SubId, Vec<E::Update>) {
        let (sub, step) = self.runtime.register(query);
        (sub, self.settle(step))
    }

    /// Remove a subscription.
    pub fn unregister_query(&mut self, sub: SubId) {
        self.runtime.unregister(sub);
    }

    /// Route one write at the next tick of the driver's clock and return
    /// every delta it led to, reads included.
    pub fn incremental_update(&mut self, write: &WriteQuery) -> Vec<E::Update> {
        self.clock += 1;
        let step = self.runtime.write(write, Lsn(self.clock));
        self.settle(step)
    }

    /// Run the reads the engine asked for through a direct maintenance
    /// call ([`Local::engine_mut`]) and return the deltas they led to.
    pub fn pump(&mut self) -> Vec<E::Update> {
        let step = self.runtime.pump();
        self.settle(step)
    }

    /// The engine, for inspection.
    pub fn engine(&self) -> &E {
        self.runtime.engine()
    }

    /// The engine, for direct maintenance calls; follow with
    /// [`Local::pump`].
    pub fn engine_mut(&mut self) -> &mut E {
        self.runtime.engine_mut()
    }

    /// The runtime, for its counters.
    pub fn runtime(&self) -> &Runtime<E> {
        &self.runtime
    }

    /// The storage handle.
    pub fn storage(&self) -> &Rc<S> {
        &self.storage
    }

    /// Run every read a step handed out, land it, and keep going until no
    /// read is left, collecting the deltas in order.
    fn settle(&mut self, step: Step<E::Update>) -> Vec<E::Update> {
        let mut updates = step.updates;
        let mut queue: VecDeque<_> = step.selects.into();
        while let Some(fetch) = queue.pop_front() {
            let snapshot = immediate(self.storage.select(&fetch.query, None))
                .expect("Local drives storage that answers at once; use Service for asynchronous storage")
                .expect("Local drives storage that cannot fail");
            let landed = self.runtime.fetched(fetch.id, snapshot);
            updates.extend(landed.updates);
            queue.extend(landed.selects);
        }
        updates
    }
}

/// The output of a future that is ready as soon as it is created; `None`
/// if it would have to wait.
fn immediate<F: Future>(future: F) -> Option<F::Output> {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => Some(output),
        Poll::Pending => None,
    }
}
