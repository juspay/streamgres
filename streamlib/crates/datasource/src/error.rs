use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceError {
    /// May succeed if retried (network, timeout).
    Transient(String),
    /// Will never succeed (bad query, missing table, unsupported type).
    Refused(String),
    /// The stream can't continue from the asked position, so the consumer must re-sync.
    CannotResume(String),
    /// The source broke the contract.
    Contract(String),
    /// Source is closed or not opened.
    Closed,
}

impl SourceError {
    pub fn contract(why: impl Into<String>) -> Self {
        SourceError::Contract(why.into())
    }
}

impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SourceError::Transient(m) => write!(f, "transient: {m}"),
            SourceError::Refused(m) => write!(f, "refused: {m}"),
            SourceError::CannotResume(m) => write!(f, "cannot resume: {m}"),
            SourceError::Contract(m) => write!(f, "contract violation: {m}"),
            SourceError::Closed => f.write_str("source closed"),
        }
    }
}

impl std::error::Error for SourceError {}

pub type Result<T> = std::result::Result<T, SourceError>;
