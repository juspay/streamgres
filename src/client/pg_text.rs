//! Postgres's text form of the raw values a Zero client is shown as text:
//! the binary form of `interval`, `timetz`, `inet`, `cidr` and `macaddr`
//! written as Postgres's output functions write it (`IntervalStyle`
//! `postgres`). Other raw types have no text here.

use std::fmt::Write as _;
use std::net::{Ipv4Addr, Ipv6Addr};

use crate::model::Raw;

const INTERVAL: u32 = 1186;
const TIMETZ: u32 = 1266;
const INET: u32 = 869;
const CIDR: u32 = 650;
const MACADDR: u32 = 829;

/// The text Postgres writes for `raw`, when this module knows its type.
pub fn raw_text(raw: &Raw) -> Option<String> {
    let bytes = raw.bytes.as_ref();
    match raw.type_oid {
        INTERVAL => interval(bytes),
        TIMETZ => timetz(bytes),
        INET | CIDR => inet(bytes, raw.type_oid == CIDR),
        MACADDR => macaddr(bytes),
        _ => None,
    }
}

fn i64_at(bytes: &[u8], at: usize) -> Option<i64> {
    Some(i64::from_be_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
}

fn i32_at(bytes: &[u8], at: usize) -> Option<i32> {
    Some(i32::from_be_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
}

/// `HH:MM:SS` of `micros` (hours past 24 allowed), then the fraction
/// without trailing zeros; no sign.
fn clock(micros: u64) -> String {
    let seconds = micros / 1_000_000;
    let fraction = micros % 1_000_000;
    let mut out = format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60
    );
    if fraction != 0 {
        let digits = format!("{fraction:06}");
        out.push('.');
        out.push_str(digits.trim_end_matches('0'));
    }
    out
}

/// An `interval`: microseconds, days and months.
fn interval(bytes: &[u8]) -> Option<String> {
    if bytes.len() != 16 {
        return None;
    }
    let micros = i64_at(bytes, 0)?;
    let days = i32_at(bytes, 8)?;
    let months = i32_at(bytes, 12)?;
    let years = months / 12;
    let months = months % 12;
    let mut parts: Vec<String> = Vec::new();
    for (count, unit) in [(years, "year"), (months, "mon"), (days, "day")] {
        if count != 0 {
            let plural = if count == 1 { "" } else { "s" };
            parts.push(format!("{count} {unit}{plural}"));
        }
    }
    let earlier_negative = years < 0 || months < 0 || days < 0;
    if micros != 0 || parts.is_empty() {
        let sign = if micros < 0 {
            "-"
        } else if earlier_negative {
            "+"
        } else {
            ""
        };
        parts.push(format!("{sign}{}", clock(micros.unsigned_abs())));
    }
    Some(parts.join(" "))
}

/// A `timetz`: microseconds since midnight and the zone's offset in
/// seconds west of UTC.
fn timetz(bytes: &[u8]) -> Option<String> {
    if bytes.len() != 12 {
        return None;
    }
    let micros = u64::try_from(i64_at(bytes, 0)?).ok()?;
    let east = -i64::from(i32_at(bytes, 8)?);
    let sign = if east < 0 { '-' } else { '+' };
    let east = east.unsigned_abs();
    let mut out = clock(micros);
    let _ = write!(out, "{sign}{:02}", east / 3600);
    if east % 3600 != 0 {
        let _ = write!(out, ":{:02}", east / 60 % 60);
    }
    if east % 60 != 0 {
        let _ = write!(out, ":{:02}", east % 60);
    }
    Some(out)
}

/// An `inet` or `cidr`: family, prefix bits, the cidr flag, the address
/// length and the address. An `inet` with a full-length prefix omits it.
fn inet(bytes: &[u8], cidr: bool) -> Option<String> {
    let (&family, rest) = bytes.split_first()?;
    let (&bits, rest) = rest.split_first()?;
    let (_, rest) = rest.split_first()?;
    let (&length, address) = rest.split_first()?;
    if address.len() != usize::from(length) {
        return None;
    }
    let (text, full) = match family {
        2 => {
            let octets: [u8; 4] = address.try_into().ok()?;
            (Ipv4Addr::from(octets).to_string(), 32)
        }
        3 => {
            let octets: [u8; 16] = address.try_into().ok()?;
            (Ipv6Addr::from(octets).to_string(), 128)
        }
        _ => return None,
    };
    if cidr || bits != full {
        Some(format!("{text}/{bits}"))
    } else {
        Some(text)
    }
}

/// A `macaddr`: six bytes as lowercase hex pairs.
fn macaddr(bytes: &[u8]) -> Option<String> {
    if bytes.len() != 6 {
        return None;
    }
    let pairs: Vec<String> = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    Some(pairs.join(":"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn raw(type_oid: u32, bytes: Vec<u8>) -> Raw {
        Raw {
            type_oid,
            bytes: Bytes::from(bytes),
        }
    }

    fn interval_bytes(micros: i64, days: i32, months: i32) -> Vec<u8> {
        [
            &micros.to_be_bytes()[..],
            &days.to_be_bytes(),
            &months.to_be_bytes(),
        ]
        .concat()
    }

    #[test]
    fn intervals_read_as_postgres_writes_them() {
        let text =
            |micros, days, months| raw_text(&raw(INTERVAL, interval_bytes(micros, days, months)));
        assert_eq!(
            text(11_045_000_000, 2, 12).as_deref(),
            Some("1 year 2 days 03:04:05")
        );
        assert_eq!(text(0, 0, 0).as_deref(), Some("00:00:00"));
        assert_eq!(text(0, 1, 14).as_deref(), Some("1 year 2 mons 1 day"));
        assert_eq!(
            text(7_200_000_000, -1, 0).as_deref(),
            Some("-1 days +02:00:00")
        );
        assert_eq!(text(-1_500_000, 0, 0).as_deref(), Some("-00:00:01.5"));
        assert_eq!(text(90_000_000_000, 0, 0).as_deref(), Some("25:00:00"));
    }

    #[test]
    fn times_with_zones_read_as_postgres_writes_them() {
        let bytes =
            |micros: i64, west: i32| [&micros.to_be_bytes()[..], &west.to_be_bytes()].concat();
        assert_eq!(
            raw_text(&raw(TIMETZ, bytes(36_000_000_000, -19_800))).as_deref(),
            Some("10:00:00+05:30")
        );
        assert_eq!(
            raw_text(&raw(TIMETZ, bytes(500_000, 18_000))).as_deref(),
            Some("00:00:00.5-05")
        );
    }

    #[test]
    fn addresses_read_as_postgres_writes_them() {
        assert_eq!(
            raw_text(&raw(INET, vec![2, 32, 0, 4, 10, 0, 0, 1])).as_deref(),
            Some("10.0.0.1")
        );
        assert_eq!(
            raw_text(&raw(INET, vec![2, 8, 0, 4, 10, 0, 0, 1])).as_deref(),
            Some("10.0.0.1/8")
        );
        assert_eq!(
            raw_text(&raw(CIDR, vec![2, 32, 1, 4, 10, 0, 0, 1])).as_deref(),
            Some("10.0.0.1/32")
        );
        let mut v6 = vec![3, 128, 0, 16];
        v6.extend_from_slice(&Ipv6Addr::LOCALHOST.octets());
        assert_eq!(raw_text(&raw(INET, v6)).as_deref(), Some("::1"));
        assert_eq!(
            raw_text(&raw(MACADDR, vec![8, 0, 0x2b, 1, 2, 3])).as_deref(),
            Some("08:00:2b:01:02:03")
        );
    }

    #[test]
    fn malformed_or_unknown_values_have_no_text() {
        assert_eq!(raw_text(&raw(INTERVAL, vec![0; 3])), None);
        assert_eq!(raw_text(&raw(INET, vec![2, 32, 0, 4, 10])), None);
        assert_eq!(raw_text(&raw(790, vec![0; 8])), None);
    }
}
