//! Postgres's text form of the raw values a Zero client is shown as text:
//! the binary form of `interval`, `timetz`, `inet`, `cidr`, `macaddr` and
//! `tsvector` written as Postgres's output functions write it
//! (`IntervalStyle` `postgres`), and `money` as its amount, as
//! `money::numeric` gives it (the currency symbol and grouping of
//! `lc_monetary` left out). Other raw types have no text here.

use std::fmt::Write as _;
use std::net::{Ipv4Addr, Ipv6Addr};

const INTERVAL: u32 = 1186;
const TIMETZ: u32 = 1266;
const INET: u32 = 869;
const CIDR: u32 = 650;
const MACADDR: u32 = 829;
const TSVECTOR: u32 = 3614;
const MONEY: u32 = 790;

/// The text Postgres writes for a raw value of type `type_oid`, when this
/// module knows the type. `money_scale` is the source instance's
/// `lc_monetary` scale.
pub fn raw_text(type_oid: u32, bytes: &[u8], money_scale: u8) -> Option<String> {
    match type_oid {
        INTERVAL => interval(bytes),
        TIMETZ => timetz(bytes),
        INET | CIDR => inet(bytes, type_oid == CIDR),
        MACADDR => macaddr(bytes),
        TSVECTOR => tsvector(bytes),
        MONEY => money(bytes, money_scale),
        // `hstore`'s OID is assigned by the extension and differs per
        // database, so it is recognized by its exact binary shape rather
        // than an OID. The bytes stay authoritative for equality and
        // identity; only the client's text projection comes from here.
        _ => hstore(bytes),
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

/// A `tsvector`: its lexemes in order, each quoted, with its positions
/// and their weights (`A`, `B`, `C`; `D` is written as none).
fn tsvector(bytes: &[u8]) -> Option<String> {    let count = usize::try_from(i32_at(bytes, 0)?).ok()?;
    let mut at = 4;
    let mut lexemes = Vec::with_capacity(count);
    for _ in 0..count {
        let length = bytes.get(at..)?.iter().position(|byte| *byte == 0)?;
        let lexeme = std::str::from_utf8(bytes.get(at..at + length)?).ok()?;
        at += length + 1;
        let positions = usize::from(u16::from_be_bytes(bytes.get(at..at + 2)?.try_into().ok()?));
        at += 2;
        let mut text = format!("'{}'", lexeme.replace('\\', "\\\\").replace('\'', "''"));
        for index in 0..positions {
            let entry = u16::from_be_bytes(bytes.get(at..at + 2)?.try_into().ok()?);
            at += 2;
            text.push(if index == 0 { ':' } else { ',' });
            let _ = write!(text, "{}", entry & 0x3fff);
            match entry >> 14 {
                3 => text.push('A'),
                2 => text.push('B'),
                1 => text.push('C'),
                _ => {}
            }
        }
        lexemes.push(text);
    }
    (at == bytes.len()).then(|| lexemes.join(" "))
}

/// An `hstore` as Postgres's own output writes it: `"key"=>"value"` pairs
/// joined by `, `. Its binary form is a count, then each pair's length-
/// prefixed key and value (`-1` for a NULL value). The OID is assigned by
/// the extension, so the caller matches on the type name, not the OID.
pub(crate) fn hstore(bytes: &[u8]) -> Option<String> {
    let count = usize::try_from(i32_at(bytes, 0)?).ok()?;
    let mut at = 4;
    let mut pairs = Vec::with_capacity(count);
    for _ in 0..count {
        let key_len = i32_at(bytes, at)?;
        at += 4;
        let key = bytes.get(at..at + usize::try_from(key_len).ok()?)?;
        at += usize::try_from(key_len).ok()?;
        let value_len = i32_at(bytes, at)?;
        at += 4;
        let key = std::str::from_utf8(key).ok()?;
        let pair = if value_len < 0 {
            format!("{}=>NULL", hstore_token(key))
        } else {
            let value = bytes.get(at..at + usize::try_from(value_len).ok()?)?;
            at += usize::try_from(value_len).ok()?;
            let value = std::str::from_utf8(value).ok()?;
            format!("{}=>{}", hstore_token(key), hstore_token(value))
        };
        pairs.push(pair);
    }
    (at == bytes.len()).then(|| pairs.join(", "))
}

/// An `hstore` key or value as its output writes it: bare unless it holds
/// whitespace, a comma, a quote or a backslash, in which case it is
/// double-quoted with `"` and `\` escaped.
fn hstore_token(text: &str) -> String {
    let needs_quotes = text.is_empty()
        || text
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, ',' | '"' | '\\' | '='));
    if !needs_quotes {
        return text.to_owned();
    }
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for character in text.chars() {
        if matches!(character, '"' | '\\') {
            out.push('\\');
        }
        out.push(character);
    }
    out.push('"');
    out
}

/// A `money` amount in the currency's units, with as many fraction
/// digits as the database's `lc_monetary` gives it.
fn money(bytes: &[u8], money_scale: u8) -> Option<String> {
    let cents = i64_at(bytes, 0)?;
    if bytes.len() != 8 {
        return None;
    }
    let scale = u32::from(money_scale);
    if scale == 0 {
        return Some(cents.to_string());
    }
    let unit = 10_u64.checked_pow(scale)?;
    let magnitude = cents.unsigned_abs();
    let sign = if cents < 0 { "-" } else { "" };
    let width = usize::try_from(scale).ok()?;
    Some(format!(
        "{sign}{}.{:0width$}",
        magnitude / unit,
        magnitude % unit
    ))
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

    fn raw(type_oid: u32, bytes: Vec<u8>) -> (u32, Vec<u8>) {
        (type_oid, bytes)
    }

    fn interval_bytes(micros: i64, days: i32, months: i32) -> Vec<u8> {
        [
            &micros.to_be_bytes()[..],
            &days.to_be_bytes(),
            &months.to_be_bytes(),
        ]
        .concat()
    }

    /// An `hstore` is recognized by its count-prefixed layout and written
    /// as Postgres writes it, quoting only where output needs it. Bytes
    /// that are not exactly that shape render nothing, so no other raw type
    /// is misread as a map.
    #[test]
    fn an_hstore_reads_as_postgres_writes_it() {
        let pair = |key: &str, value: Option<&str>| {
            let mut out = Vec::new();
            out.extend_from_slice(&(key.len() as i32).to_be_bytes());
            out.extend_from_slice(key.as_bytes());
            match value {
                Some(value) => {
                    out.extend_from_slice(&(value.len() as i32).to_be_bytes());
                    out.extend_from_slice(value.as_bytes());
                }
                None => out.extend_from_slice(&(-1i32).to_be_bytes()),
            }
            out
        };
        let map = |pairs: &[(&str, Option<&str>)]| {
            let mut out = (pairs.len() as i32).to_be_bytes().to_vec();
            for (key, value) in pairs {
                out.extend_from_slice(&pair(key, *value));
            }
            out
        };
        assert_eq!(
            hstore(&map(&[("foo", Some("bar"))])).as_deref(),
            Some("foo=>bar")
        );
        assert_eq!(
            hstore(&map(&[("a", Some("1")), ("c", Some("x y")), ("d", None)])).as_deref(),
            Some("a=>1, c=>\"x y\", d=>NULL")
        );
        // Not that layout: a UUID and a zeroed point must not become maps.
        assert_eq!(hstore(&[0u8; 16]), None);
        assert_eq!(
            hstore(&0x0000_0001_0000_0000_0000_0003_0000_0000u128.to_be_bytes()),
            None
        );
    }

    #[test]
    fn intervals_read_as_postgres_writes_them() {
        let text = |micros, days, months| {
            let (oid, bytes) = raw(INTERVAL, interval_bytes(micros, days, months));
            raw_text(oid, &bytes, 2)
        };
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
            raw_text(TIMETZ, &bytes(36_000_000_000, -19_800), 2).as_deref(),
            Some("10:00:00+05:30")
        );
        assert_eq!(
            raw_text(TIMETZ, &bytes(500_000, 18_000), 2).as_deref(),
            Some("00:00:00.5-05")
        );
    }

    #[test]
    fn addresses_read_as_postgres_writes_them() {
        assert_eq!(
            raw_text(INET, &[2, 32, 0, 4, 10, 0, 0, 1], 2).as_deref(),
            Some("10.0.0.1")
        );
        assert_eq!(
            raw_text(INET, &[2, 8, 0, 4, 10, 0, 0, 1], 2).as_deref(),
            Some("10.0.0.1/8")
        );
        assert_eq!(
            raw_text(CIDR, &[2, 32, 1, 4, 10, 0, 0, 1], 2).as_deref(),
            Some("10.0.0.1/32")
        );
        let mut v6 = vec![3, 128, 0, 16];
        v6.extend_from_slice(&Ipv6Addr::LOCALHOST.octets());
        assert_eq!(raw_text(INET, &v6, 2).as_deref(), Some("::1"));
        assert_eq!(
            raw_text(MACADDR, &[8, 0, 0x2b, 1, 2, 3], 2).as_deref(),
            Some("08:00:2b:01:02:03")
        );
    }

    #[test]
    fn text_searches_and_amounts_read_as_postgres_gives_them() {
        let mut vector = 2i32.to_be_bytes().to_vec();
        vector.extend_from_slice(b"cat\0");
        vector.extend_from_slice(&2u16.to_be_bytes());
        vector.extend_from_slice(&(3u16 | (3 << 14)).to_be_bytes());
        vector.extend_from_slice(&5u16.to_be_bytes());
        vector.extend_from_slice(b"it's\0");
        vector.extend_from_slice(&0u16.to_be_bytes());
        assert_eq!(
            raw_text(TSVECTOR, &vector, 2).as_deref(),
            Some("'cat':3A,5 'it''s'")
        );
        assert_eq!(
            raw_text(MONEY, &123_456i64.to_be_bytes(), 2).as_deref(),
            Some("1234.56")
        );
        assert_eq!(
            raw_text(MONEY, &(-5i64).to_be_bytes(), 2).as_deref(),
            Some("-0.05")
        );
    }

    #[test]
    fn malformed_or_unknown_values_have_no_text() {
        assert_eq!(raw_text(INTERVAL, &[0; 3], 2), None);
        assert_eq!(raw_text(INET, &[2, 32, 0, 4, 10], 2), None);
        assert_eq!(raw_text(600, &[0; 16], 2), None);
    }
}
