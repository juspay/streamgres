//! Where in the source's history a write, a storage read, or a frame row
//! sits: one WAL location ([`Lsn`]) for all of them. The engine compares
//! locations only; how a source arrives at them is the storage layer's
//! business (see `sync::pg` for the two Postgres methods and
//! `sync::MemoryStorage` for the in-process store).

use std::fmt;

use super::frame::{DataFrameKey, DataFrameRow};

/// A WAL location: the 64-bit form of Postgres's `X/Y`. A write carries
/// the location of its transaction's commit; a read carries the location
/// its snapshot reflects every commit up to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

impl Lsn {
    /// Parse Postgres's `X/Y` spelling (both halves hexadecimal).
    pub fn parse(text: &str) -> Option<Lsn> {
        let (high, low) = text.trim().split_once('/')?;
        let high = u64::from_str_radix(high, 16).ok()?;
        let low = u64::from_str_radix(low, 16).ok()?;
        Some(Lsn((high << 32) | low))
    }
}

impl fmt::Display for Lsn {
    /// Postgres's `X/Y` spelling.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

/// The result of one storage read: the rows, and the location the read's
/// snapshot reflects every commit up to (and nothing beyond).
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub rows: Vec<(DataFrameKey, DataFrameRow)>,
    pub at: Lsn,
}

/// How current a frame row's image is: written by the stream at a
/// location (every later write is news for it), or landed from a read
/// (a write the read already saw is not).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowAt {
    Written(Lsn),
    Landed(Lsn),
}

impl RowAt {
    /// The location itself.
    pub fn lsn(&self) -> Lsn {
        match self {
            RowAt::Written(lsn) | RowAt::Landed(lsn) => *lsn,
        }
    }

    /// Whether the row's image already reflects the write committed at
    /// `write`. A written row never does: the stream delivers in commit
    /// order, and two writes of one transaction share a location, so a
    /// write reaching a written row is always news.
    pub fn reflects(&self, write: Lsn) -> bool {
        match self {
            RowAt::Written(_) => false,
            RowAt::Landed(read) => write <= *read,
        }
    }

    /// Whether the row's image is newer than what a read positioned at
    /// `read` returned for it.
    pub fn newer_than(&self, read: Lsn) -> bool {
        self.lsn() > read
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `X/Y` parses hexadecimal halves into one ordered number and prints
    /// back the same way.
    #[test]
    fn lsn_round_trips() {
        let lsn = Lsn::parse("1/A0").unwrap();
        assert_eq!(lsn, Lsn((1 << 32) | 0xA0));
        assert_eq!(lsn.to_string(), "1/A0");
        assert!(Lsn::parse("0/FFFFFFFF").unwrap() < Lsn::parse("1/0").unwrap());
        assert!(Lsn::parse("nonsense").is_none());
    }

    /// A row's currency against writes and against reads.
    #[test]
    fn row_currency() {
        let landed = RowAt::Landed(Lsn(100));
        assert!(landed.reflects(Lsn(90)));
        assert!(landed.reflects(Lsn(100)));
        assert!(!landed.reflects(Lsn(110)));
        assert!(landed.newer_than(Lsn(50)));
        assert!(!landed.newer_than(Lsn(100)));
        let written = RowAt::Written(Lsn(120));
        assert!(!written.reflects(Lsn(10)));
        assert!(!written.reflects(Lsn(120)));
        assert!(written.newer_than(Lsn(100)));
        assert!(!written.newer_than(Lsn(120)));
    }
}
