use std::fmt;

use streamlib_datasource::SourceError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// The data source failed or refused.
    Source(SourceError),
    /// The runtime is shut down or failed.
    Closed,
    /// A thread or runtime couldn't be started.
    Start(String),
    /// The engine failed internally (for example it panicked).
    Engine(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Source(e) => write!(f, "source: {e}"),
            Error::Closed => f.write_str("runtime closed"),
            Error::Start(m) => write!(f, "start: {m}"),
            Error::Engine(m) => write!(f, "engine: {m}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<SourceError> for Error {
    fn from(e: SourceError) -> Self {
        Error::Source(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
