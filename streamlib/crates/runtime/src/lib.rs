//! The streamlib runtime: an in-process runtime that keeps query results cached and current.

mod config;
mod engine;
mod error;
mod runtime;

pub use config::Config;
pub use error::{Error, Result};
pub use runtime::Runtime;
