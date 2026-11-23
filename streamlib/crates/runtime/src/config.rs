/// Runtime settings.
#[derive(Clone, Debug)]
pub struct Config {
    /// Worker threads for the source's background work (stream, connections).
    pub io_threads: usize,
    /// Run the source's one-time `setup` (for example creating a publication) at start.
    pub setup: bool,
    /// Longest wait between attempts to resume a lost stream.
    pub max_backoff: std::time::Duration,
    /// How long cleanup waits when the runtime is dropped without `shutdown`.
    pub drop_grace: std::time::Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            io_threads: 2,
            setup: false,
            max_backoff: std::time::Duration::from_secs(10),
            drop_grace: std::time::Duration::from_secs(10),
        }
    }
}
