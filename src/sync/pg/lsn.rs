//! Postgres's position in its history, the WAL location, and how it maps
//! onto the engine's [`Position`].

use std::fmt;

use crate::model::Position;

/// A WAL location: the 64-bit form of Postgres's `X/Y`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

impl Lsn {
    /// Parses Postgres's `X/Y` spelling (both halves hexadecimal).
    pub fn parse(text: &str) -> Option<Lsn> {
        let (high, low) = text.trim().split_once('/')?;
        let high = u64::from_str_radix(high, 16).ok()?;
        let low = u64::from_str_radix(low, 16).ok()?;
        Some(Lsn((high << 32) | low))
    }

    /// The location a position stands for; `None` for one no WAL
    /// location is.
    pub fn of(position: Position) -> Option<Lsn> {
        u64::try_from(position.0).ok().map(Lsn)
    }
}

impl From<Lsn> for Position {
    fn from(lsn: Lsn) -> Self {
        Position(u128::from(lsn.0))
    }
}

impl fmt::Display for Lsn {
    /// Postgres's `X/Y` spelling.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

/// Parses Postgres's `X/Y` spelling straight into a position.
pub fn parse(text: &str) -> Option<Position> {
    Lsn::parse(text).map(Position::from)
}

/// The WAL location of a position this backend made, as a number;
/// positions from elsewhere saturate, which no comparison here meets.
pub fn wal(position: Position) -> u64 {
    u64::try_from(position.0).unwrap_or(u64::MAX)
}

/// A position written as Postgres writes the WAL location it stands for.
pub fn text(position: Position) -> String {
    match Lsn::of(position) {
        Some(lsn) => lsn.to_string(),
        None => position.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `X/Y` parses hexadecimal halves into one ordered number and prints
    /// back the same way, and orders as positions do.
    #[test]
    fn lsn_round_trips() {
        let lsn = Lsn::parse("1/A0").unwrap();
        assert_eq!(lsn, Lsn((1 << 32) | 0xA0));
        assert_eq!(lsn.to_string(), "1/A0");
        assert!(
            Position::from(Lsn::parse("0/FFFFFFFF").unwrap())
                < Position::from(Lsn::parse("1/0").unwrap())
        );
        assert!(Lsn::parse("nonsense").is_none());
        assert_eq!(text(Position::from(lsn)), "1/A0");
    }
}
