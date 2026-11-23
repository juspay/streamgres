//! Where background tasks run: a runtime the owner picks, or the caller's.

use std::future::Future;

use tokio::runtime::Handle;
use tokio::task::JoinHandle;

#[derive(Clone, Debug, Default)]
pub struct Spawner(Option<Handle>);

impl Spawner {
    pub fn on(handle: Handle) -> Self {
        Spawner(Some(handle))
    }

    pub fn spawn<F>(&self, task: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match &self.0 {
            Some(handle) => handle.spawn(task),
            None => tokio::spawn(task),
        }
    }

    /// The chosen runtime, else the current one if any (for spawning from `Drop`).
    pub fn handle(&self) -> Option<Handle> {
        self.0.clone().or_else(|| Handle::try_current().ok())
    }
}
