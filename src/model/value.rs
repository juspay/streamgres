//! Runtime values and their types.
//!
//! [`Value`] is the single dynamic value representation used everywhere in
//! the engine: in query predicates, in write payloads, and in materialized
//! rows. The engine keys hash maps by types that embed `Value` (the reverse
//! index is keyed by `Condition`, the shared table frames by
//! `DataFrameKey`), so `Value` must implement `Eq` and `Hash`. `f64` and `HashMap` do not
//! provide those out of the box, hence the manual implementations in this
//! file:
//!
//! - `Float`: `NaN == NaN` and `0.0 == -0.0`, hashed via canonicalized bits,
//!   so equality and hashing stay consistent.
//! - `Map`: equality is order-independent (as for any `HashMap`), so the
//!   hash combines per-entry hashes with an order-independent fold.

use chrono::{NaiveDate, NaiveDateTime};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// A dynamically-typed database value.
///
/// `Int` is `i64` so Postgres `bigint` / `bigserial` identifiers fit
/// natively (decided ahead of the Diesel connector).
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Date(NaiveDate),
    Datetime(NaiveDateTime),
    List(Vec<Value>),
    Map(std::collections::HashMap<Value, Value>),
}

/// The static type of a [`Value`], used to describe columns in a schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ValueType {
    String,
    Int,
    Float,
    Bool,
    Date,
    Datetime,
    List(Box<ValueType>),
    Map(Box<ValueType>, Box<ValueType>),
}

impl Value {
    /// Whether this value is [`Value::Null`].
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Ordering comparison backing the `GT` / `GTE` / `LT` / `LTE` operators.
    ///
    /// Values compare only within the same variant, except `Int` / `Float`
    /// which coerce to `f64` (lossy above 2⁵³ — mixed-type comparisons of
    /// such magnitudes may tie spuriously; same-variant comparisons are
    /// always exact). Everything else — including anything involving
    /// `Null` — returns `None`, which predicate evaluation treats as
    /// "does not match" (SQL three-valued logic collapsed to `false`).
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        use Value::*;
        match (self, other) {
            (Int(a), Int(b)) => Some(a.cmp(b)),
            (Float(a), Float(b)) => a.partial_cmp(b),
            (Int(a), Float(b)) => (*a as f64).partial_cmp(b),
            (Float(a), Int(b)) => a.partial_cmp(&(*b as f64)),
            (String(a), String(b)) => Some(a.cmp(b)),
            (Bool(a), Bool(b)) => Some(a.cmp(b)),
            (Date(a), Date(b)) => Some(a.cmp(b)),
            (Datetime(a), Datetime(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    /// Equality backing the `EQ` / `NEQ` / `IN` / `NOT_IN` operators:
    /// strict equality plus `Int` / `Float` numeric coercion, so
    /// `Int(5)` equals `Float(5.0)`.
    pub fn loose_eq(&self, other: &Value) -> bool {
        self == other || self.compare(other) == Some(Ordering::Equal)
    }
}

impl PartialEq for Value {
    /// Strict, variant-exact equality (no `Int` / `Float` coercion — that is
    /// [`Value::loose_eq`]). `Float` deviates from raw `f64` semantics by
    /// treating `NaN == NaN` as true, keeping equality a proper equivalence
    /// relation so `Value` can serve as a hash-map key.
    fn eq(&self, other: &Self) -> bool {
        use Value::*;
        match (self, other) {
            (Null, Null) => true,
            (String(a), String(b)) => a == b,
            (Int(a), Int(b)) => a == b,
            (Float(a), Float(b)) => a == b || (a.is_nan() && b.is_nan()),
            (Bool(a), Bool(b)) => a == b,
            (Date(a), Date(b)) => a == b,
            (Datetime(a), Datetime(b)) => a == b,
            (List(a), List(b)) => a == b,
            (Map(a), Map(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    /// Hashes the variant discriminant plus a payload consistent with
    /// [`Value::eq`]: floats go through `canonical_f64_bits`, maps through
    /// `unordered_map_hash`, so equal values always hash identically.
    fn hash<H: Hasher>(&self, state: &mut H) {
        use Value::*;
        std::mem::discriminant(self).hash(state);
        match self {
            Null => {}
            String(s) => s.hash(state),
            Int(i) => i.hash(state),
            Float(f) => canonical_f64_bits(*f).hash(state),
            Bool(b) => b.hash(state),
            Date(d) => d.hash(state),
            Datetime(dt) => dt.hash(state),
            List(items) => items.hash(state),
            Map(entries) => unordered_map_hash(entries).hash(state),
        }
    }
}

/// Bit pattern for hashing an `f64` such that all values that compare equal
/// under [`Value::eq`] hash identically: every NaN collapses to one bit
/// pattern, and `-0.0` collapses to `0.0`.
fn canonical_f64_bits(f: f64) -> u64 {
    if f.is_nan() {
        f64::NAN.to_bits()
    } else if f == 0.0 {
        0
    } else {
        f.to_bits()
    }
}

/// Order-independent hash of a map, matching `HashMap`'s order-independent
/// equality: each entry is hashed on its own and the results are combined
/// with a commutative fold.
pub(crate) fn unordered_map_hash<K: Hash, V: Hash>(map: &HashMap<K, V>) -> u64 {
    map.iter()
        .map(|(k, v)| {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            k.hash(&mut h);
            v.hash(&mut h);
            h.finish()
        })
        .fold(0u64, u64::wrapping_add)
}

impl From<&str> for Value {
    /// Wraps a borrowed string as [`Value::String`], copying it.
    fn from(s: &str) -> Self {
        Value::String(s.to_owned())
    }
}

impl From<String> for Value {
    /// Wraps an owned string as [`Value::String`].
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<i32> for Value {
    /// Widens an `i32` into [`Value::Int`] — a convenience for literals and
    /// narrow ids.
    fn from(i: i32) -> Self {
        Value::Int(i64::from(i))
    }
}

impl From<i64> for Value {
    /// Wraps an `i64` as [`Value::Int`].
    fn from(i: i64) -> Self {
        Value::Int(i)
    }
}

impl From<f64> for Value {
    /// Wraps an `f64` as [`Value::Float`].
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<bool> for Value {
    /// Wraps a `bool` as [`Value::Bool`].
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    /// Hashes a [`Value`] with the std `DefaultHasher`.
    fn hash_of(v: &Value) -> u64 {
        let mut h = DefaultHasher::new();
        v.hash(&mut h);
        h.finish()
    }

    /// The `Float` special cases (`NaN == NaN`, `0.0 == -0.0`) hash
    /// identically, keeping the `Eq` / `Hash` contract intact.
    #[test]
    fn float_nan_and_zero_equality_matches_hash() {
        let nan_a = Value::Float(f64::NAN);
        let nan_b = Value::Float(-f64::NAN);
        assert_eq!(nan_a, nan_b);
        assert_eq!(hash_of(&nan_a), hash_of(&nan_b));

        let pos_zero = Value::Float(0.0);
        let neg_zero = Value::Float(-0.0);
        assert_eq!(pos_zero, neg_zero);
        assert_eq!(hash_of(&pos_zero), hash_of(&neg_zero));
    }

    /// `Int` / `Float` coercion applies only to [`Value::loose_eq`]; strict
    /// equality (and therefore hashing) stays variant-exact.
    #[test]
    fn int_float_coercion_is_loose_only() {
        assert!(Value::Int(5).loose_eq(&Value::Float(5.0)));
        assert_ne!(Value::Int(5), Value::Float(5.0));
    }

    /// [`Value::compare`] returns `None` for anything involving `Null`,
    /// including `Null` vs `Null`.
    #[test]
    fn null_compares_with_nothing() {
        assert_eq!(Value::Null.compare(&Value::Null), None);
        assert_eq!(Value::Null.compare(&Value::Int(1)), None);
    }
}
