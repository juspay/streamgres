//! Postgres's own time representations: microseconds or days since
//! 2000-01-01, with the extreme values standing for the infinities, as
//! Postgres stores and sends them. Ordering the raw numbers is Postgres's
//! ordering, infinities included.

/// Microseconds from the Unix epoch to Postgres's epoch, 2000-01-01.
const UNIX_TO_PG_MICROS: i64 = 946_684_800_000_000;
/// Days from the Unix epoch to Postgres's epoch.
const UNIX_TO_PG_DAYS: i32 = 10_957;
/// Microseconds in a day.
const MICROS_PER_DAY: i64 = 86_400_000_000;

/// A `timestamp` or `timestamptz`: microseconds since 2000-01-01 00:00 UTC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub i64);

impl Timestamp {
    pub const INFINITY: Timestamp = Timestamp(i64::MAX);
    pub const NEG_INFINITY: Timestamp = Timestamp(i64::MIN);

    /// Whether this is `infinity` or `-infinity`.
    pub fn is_infinite(self) -> bool {
        self == Self::INFINITY || self == Self::NEG_INFINITY
    }

    /// The instant `micros` after the Unix epoch.
    pub fn from_unix_micros(micros: i64) -> Option<Self> {
        micros
            .checked_sub(UNIX_TO_PG_MICROS)
            .map(Timestamp)
            .filter(|at| !at.is_infinite())
    }

    /// Microseconds since the Unix epoch; `None` for an infinity.
    pub fn unix_micros(self) -> Option<i64> {
        if self.is_infinite() {
            return None;
        }
        self.0.checked_add(UNIX_TO_PG_MICROS)
    }

    /// Whole milliseconds since the Unix epoch, rounded down; `None` for
    /// an infinity.
    pub fn unix_millis(self) -> Option<i64> {
        self.unix_micros().map(|micros| micros.div_euclid(1000))
    }

    /// The instant `millis` after the Unix epoch.
    pub fn from_unix_millis(millis: i64) -> Option<Self> {
        millis.checked_mul(1000).and_then(Self::from_unix_micros)
    }
}

/// A `date`: days since 2000-01-01.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Day(pub i32);

impl Day {
    pub const INFINITY: Day = Day(i32::MAX);
    pub const NEG_INFINITY: Day = Day(i32::MIN);

    /// Whether this is `infinity` or `-infinity`.
    pub fn is_infinite(self) -> bool {
        self == Self::INFINITY || self == Self::NEG_INFINITY
    }

    /// Midnight UTC of this day; `None` for an infinity or a day past
    /// the range of a timestamp.
    pub fn midnight(self) -> Option<Timestamp> {
        if self.is_infinite() {
            return None;
        }
        i64::from(self.0)
            .checked_mul(MICROS_PER_DAY)
            .map(Timestamp)
            .filter(|at| !at.is_infinite())
    }

    /// Days since the Unix epoch; `None` for an infinity.
    pub fn unix_days(self) -> Option<i64> {
        if self.is_infinite() {
            return None;
        }
        Some(i64::from(self.0) + i64::from(UNIX_TO_PG_DAYS))
    }

    /// The day `millis` (Unix epoch milliseconds) falls in.
    pub fn from_unix_millis(millis: i64) -> Option<Self> {
        let pg_days = millis.div_euclid(86_400_000).checked_sub(i64::from(UNIX_TO_PG_DAYS))?;
        let day = Day(i32::try_from(pg_days).ok()?);
        (!day.is_infinite()).then_some(day)
    }
}

/// A `time`: microseconds since midnight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Time(pub i64);

impl Time {
    /// This time as PostgreSQL writes it: `HH:MM:SS`, then the fraction
    /// without trailing zeros when there is one.
    ///
    /// The hour is never wrapped onto a single day: `24:00:00` is its own
    /// value in PostgreSQL, distinct from `00:00:00` and ordering after it,
    /// so it is written as the hour the value actually carries. Both the
    /// SQL literal and the client's text come from here, so the two cannot
    /// disagree about end of day.
    pub fn text(self) -> String {
        let seconds = self.0.div_euclid(1_000_000);
        let fraction = self.0.rem_euclid(1_000_000);
        let whole = format!(
            "{:02}:{:02}:{:02}",
            seconds.div_euclid(3600),
            seconds.div_euclid(60) % 60,
            seconds % 60
        );
        if fraction == 0 {
            return whole;
        }
        let digits = format!("{fraction:06}");
        format!("{whole}.{}", digits.trim_end_matches('0'))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One formatter serves both the SQL literal and the client's text, so
    /// end of day stays distinct from midnight in both. PostgreSQL keeps
    /// `24:00:00` as its own value, ordering after `00:00:00`.
    #[test]
    fn a_time_keeps_its_own_hour() {
        assert_eq!(Time(0).text(), "00:00:00");
        assert_eq!(Time(86_400_000_000).text(), "24:00:00");
        assert_eq!(Time(86_399_999_999).text(), "23:59:59.999999");
        assert_eq!(Time(1_500_000).text(), "00:00:01.5");
        assert_ne!(Time(0).text(), Time(86_400_000_000).text());
    }

    #[test]
    fn the_epochs_line_up() {
        let pg_epoch = Timestamp(0);
        assert_eq!(pg_epoch.unix_millis(), Some(946_684_800_000));
        assert_eq!(
            Timestamp::from_unix_millis(0),
            Some(Timestamp(-UNIX_TO_PG_MICROS))
        );
        assert_eq!(Day(0).unix_days(), Some(10_957));
        assert_eq!(Day(1).midnight(), Some(Timestamp(MICROS_PER_DAY)));
    }

    #[test]
    fn milliseconds_round_down() {
        let before_unix_epoch = Timestamp::from_unix_micros(-1).unwrap();
        assert_eq!(before_unix_epoch.unix_millis(), Some(-1));
        assert_eq!(
            Timestamp::from_unix_micros(1_999).unwrap().unix_millis(),
            Some(1)
        );
    }

    #[test]
    fn infinities_order_around_everything() {
        assert!(Timestamp::NEG_INFINITY < Timestamp(i64::MIN + 1));
        assert!(Timestamp(i64::MAX - 1) < Timestamp::INFINITY);
        assert_eq!(Timestamp::INFINITY.unix_millis(), None);
        assert_eq!(Day::INFINITY.midnight(), None);
        assert!(Day::NEG_INFINITY < Day(0));
    }
}
