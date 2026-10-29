//! Shadow reads: a sample of cached answers checked against Postgres.
//!
//! `STREAMGRES_SHADOW_SAMPLE` (0.0 to 1.0, default 0), or
//! [`set_shadow_sample`]: that share of the reads a blocking connection
//! serves from the cache or the engine's rows is also run on Postgres, and
//! the two answers are compared. The caller still gets the cached answer;
//! the sampled read costs one more Postgres read, and on a difference a
//! short wait (below).
//!
//! A cached answer may lag a commit by the engine's propagation delay, so
//! a difference is judged once the engine has passed the position
//! Postgres had reached when its read ended: the cache is read again then,
//! for up to [`SETTLE`]. The outcomes ([`Outcome`]):
//!
//! - `Agreed`: the same rows.
//! - `Lagged`: different, then the same once the engine caught up.
//! - `Mismatched`: still different with the engine past Postgres's
//!   position. Logged with both answers, and worth an alert; a write to
//!   the same rows in that moment can also cause one, so a lone mismatch
//!   under heavy writes to those rows is a lead, not a verdict.
//! - `Unverified`: no position to judge by (an engine that names none, a
//!   standby), the entry gone meanwhile, or the Postgres read failed.
//!
//! Rows are compared as a multiset (rows tied in `ORDER BY` may come in
//! either order), and `json`/`jsonb` values by their value: the engine
//! writes JSON spaced differently from Postgres.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::Value as Json;

use crate::encode::oid;
use crate::row::OwnedRow;

/// How long a differing shadow read waits for the engine to pass
/// Postgres's position before it counts as unverified.
pub const SETTLE: Duration = Duration::from_secs(2);

/// What a shadow read found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Agreed,
    Lagged,
    Mismatched,
    Unverified,
}

/// How many shadow reads ended each way.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub agreed: u64,
    pub lagged: u64,
    pub mismatched: u64,
    pub unverified: u64,
}

static SAMPLE: OnceLock<AtomicU64> = OnceLock::new();
static AGREED: AtomicU64 = AtomicU64::new(0);
static LAGGED: AtomicU64 = AtomicU64::new(0);
static MISMATCHED: AtomicU64 = AtomicU64::new(0);
static UNVERIFIED: AtomicU64 = AtomicU64::new(0);

/// The sample rate, as the bits of an `f64`: `STREAMGRES_SHADOW_SAMPLE`
/// until set.
fn sample() -> &'static AtomicU64 {
    SAMPLE.get_or_init(|| {
        let rate = std::env::var("STREAMGRES_SHADOW_SAMPLE")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|rate| !rate.is_nan())
            .map_or(0.0, |rate| rate.clamp(0.0, 1.0));
        AtomicU64::new(rate.to_bits())
    })
}

/// Sets the share of cached reads checked against Postgres.
pub fn set_shadow_sample(rate: f64) {
    let rate = if rate.is_nan() { 0.0 } else { rate.clamp(0.0, 1.0) };
    sample().store(rate.to_bits(), Ordering::Relaxed);
}

/// The share of cached reads checked against Postgres in force.
pub fn shadow_sample() -> f64 {
    f64::from_bits(sample().load(Ordering::Relaxed))
}

/// The outcomes so far.
pub fn counts() -> Counts {
    Counts {
        agreed: AGREED.load(Ordering::Relaxed),
        lagged: LAGGED.load(Ordering::Relaxed),
        mismatched: MISMATCHED.load(Ordering::Relaxed),
        unverified: UNVERIFIED.load(Ordering::Relaxed),
    }
}

/// Whether this cached read is checked.
pub(crate) fn sampled() -> bool {
    let rate = shadow_sample();
    rate > 0.0 && (rate >= 1.0 || crate::rollout::unit() < rate)
}

/// Counts `outcome` and reports it; a mismatch is logged with both
/// answers.
pub(crate) fn conclude(outcome: Outcome, sql: &str, cached: &[OwnedRow], live: &[OwnedRow]) {
    let counter = match outcome {
        Outcome::Agreed => &AGREED,
        Outcome::Lagged => &LAGGED,
        Outcome::Mismatched => &MISMATCHED,
        Outcome::Unverified => &UNVERIFIED,
    };
    counter.fetch_add(1, Ordering::Relaxed);
    crate::metrics::report(|metrics| metrics.shadow(outcome));
    if outcome == Outcome::Mismatched {
        crate::metrics::note(
            log::Level::Error,
            "shadow-mismatch",
            format!(
                "a cached answer disagrees with Postgres past the engine's position: {} \
                 (cached {} rows {}, Postgres {} rows {})",
                truncated(sql, 300),
                cached.len(),
                truncated(&format!("{:?}", canonical(cached)), 500),
                live.len(),
                truncated(&format!("{:?}", canonical(live)), 500),
            ),
        );
    }
}

/// Whether two answers hold the same rows, as a multiset, JSON by value.
pub(crate) fn same(cached: &[OwnedRow], live: &[OwnedRow]) -> bool {
    if cached.len() != live.len() {
        return false;
    }
    if let (Some(a), Some(b)) = (cached.first(), live.first())
        && a.columns() != b.columns()
    {
        return false;
    }
    canonical(cached) == canonical(live)
}

/// One value as compared: its bytes, or for JSON its value written out
/// again (keys sorted), so spacing and key order do not count.
type Cell = Option<Vec<u8>>;

/// The rows as sorted lists of comparable cells.
fn canonical(rows: &[OwnedRow]) -> Vec<Vec<Cell>> {
    let mut rows: Vec<Vec<Cell>> = rows
        .iter()
        .map(|row| {
            (0..row.columns().len())
                .map(|index| row.value(index).map(cell))
                .collect()
        })
        .collect();
    rows.sort();
    rows
}

/// A value as compared.
fn cell(value: &crate::row::OwnedValue) -> Vec<u8> {
    let bytes = value.bytes();
    let json = match value.type_oid().get() {
        oid::JSON => serde_json::from_slice::<Json>(bytes).ok(),
        oid::JSONB => bytes
            .get(1..)
            .and_then(|text| serde_json::from_slice::<Json>(text).ok()),
        _ => None,
    };
    match json {
        Some(json) => serde_json::to_vec(&json).unwrap_or_else(|_| bytes.to_vec()),
        None => bytes.to_vec(),
    }
}

/// `text` cut to `max` characters.
fn truncated(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        Some((at, _)) => format!("{}…", &text[..at]),
        None => text.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::OwnedValue;
    use std::num::NonZeroU32;
    use std::sync::Arc;

    /// A row of `(oid, bytes)` cells under columns `a`, `b`, ...
    fn row(cells: &[(u32, &[u8])]) -> OwnedRow {
        let names: Vec<String> = (0..cells.len()).map(|i| ((b'a' + i as u8) as char).to_string()).collect();
        OwnedRow::new(
            Arc::from(names),
            cells
                .iter()
                .map(|(type_oid, bytes)| {
                    Some(OwnedValue::new(NonZeroU32::new(*type_oid).expect("oid"), bytes.to_vec()))
                })
                .collect(),
        )
    }

    /// Order does not count; a value, a row more or less, does.
    #[test]
    fn answers_compare_as_multisets() {
        let one = row(&[(oid::INT4, &1i32.to_be_bytes())]);
        let two = row(&[(oid::INT4, &2i32.to_be_bytes())]);
        assert!(same(&[one.clone(), two.clone()], &[two.clone(), one.clone()]));
        assert!(!same(std::slice::from_ref(&one), std::slice::from_ref(&two)));
        assert!(!same(std::slice::from_ref(&one), &[one.clone(), one.clone()]));
    }

    /// JSON compares by value: spacing and key order do not count.
    #[test]
    fn json_compares_by_value() {
        let engine = row(&[(oid::JSON, br#"{"b":2,"a":[1,2]}"#)]);
        let postgres = row(&[(oid::JSON, br#"{"a": [1, 2], "b": 2}"#)]);
        assert!(same(&[engine], &[postgres]));
        let engine = row(&[(oid::JSONB, &[&[1u8][..], br#"{"a":1}"#].concat())]);
        let postgres = row(&[(oid::JSONB, &[&[1u8][..], br#"{"a": 1}"#].concat())]);
        assert!(same(std::slice::from_ref(&engine), &[postgres]));
        let other = row(&[(oid::JSONB, &[&[1u8][..], br#"{"a": 2}"#].concat())]);
        assert!(!same(&[engine], &[other]));
    }

    /// The rate is clamped; off by default.
    #[test]
    fn the_sample_rate_is_clamped() {
        set_shadow_sample(3.0);
        assert_eq!(shadow_sample(), 1.0);
        assert!(sampled());
        set_shadow_sample(0.0);
        assert!(!sampled());
    }
}
