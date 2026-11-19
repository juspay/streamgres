//! Test kit: a stand-in engine and an in-memory source.

mod memory;
mod mirror;

pub use memory::{Fault, MemorySource};
pub use mirror::{Mirror, Stats};
