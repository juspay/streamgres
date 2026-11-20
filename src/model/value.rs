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
//! - `Numeric`: equal and hashed by the digits Postgres wrote (`1.50` is
//!   not `1.5`), while comparisons order by numeric value.
//! - `Map`: equality is order-independent (as for any `HashMap`), so the
//!   hash combines per-entry hashes with an order-independent fold.

use chrono::{NaiveDate, NaiveDateTime};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

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
    /// An exact Postgres `numeric`, kept as the digits Postgres wrote so no
    /// digit or trailing zero is lost (`1.50` stays `1.50`, a value past 2⁵³
    /// keeps every digit). Equality and hashing are by those digits;
    /// comparisons order by numeric value.
    Numeric(String),
    Bool(bool),
    Date(NaiveDate),
    Datetime(NaiveDateTime),
    List(Vec<Value>),
    Map(std::collections::HashMap<Value, Value>),
    Set(SharedSet),
}

/// The static type of a [`Value`], used to describe columns in a schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ValueType {
    String,
    Int,
    Float,
    /// An exact Postgres `numeric` / `decimal`; the cell is a
    /// [`Value::Numeric`].
    Numeric,
    Bool,
    Date,
    Datetime,
    List(Box<ValueType>),
    Map(Box<ValueType>, Box<ValueType>),
    /// A point in time carried as microseconds since the Unix epoch, the
    /// form protocol clients see for `timestamp`, `timestamptz` and `date`
    /// columns; the cell is a [`Value::Int`].
    Timestamp,
    /// A JSON document carried as its text, the form the engine keeps for
    /// `json` and `jsonb` columns; the cell is a [`Value::String`] and the
    /// transport writes it out as the JSON it holds.
    Json,
}

impl Value {
    /// Whether this value is [`Value::Null`].
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Ordering comparison backing the `GT` / `GTE` / `LT` / `LTE` operators.
    ///
    /// Values compare only within the same variant, except `Int` / `Float`
    /// / `Numeric` which coerce (a `Numeric` is exact against an `Int`, and
    /// lossy to `f64` against a `Float` above 2⁵³ — mixed-type comparisons
    /// of such magnitudes may tie spuriously; same-variant `Numeric`
    /// comparisons are always exact). Everything else — including anything
    /// involving `Null` — returns `None`, which predicate evaluation treats
    /// as "does not match" (SQL three-valued logic collapsed to `false`).
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        use Value::*;
        match (self, other) {
            (Int(a), Int(b)) => Some(a.cmp(b)),
            (Float(a), Float(b)) => a.partial_cmp(b),
            (Int(a), Float(b)) => (*a as f64).partial_cmp(b),
            (Float(a), Int(b)) => a.partial_cmp(&(*b as f64)),
            (Numeric(a), Numeric(b)) => Some(numeric_cmp(a, b)),
            (Numeric(a), Int(b)) => Some(numeric_cmp(a, &b.to_string())),
            (Int(a), Numeric(b)) => Some(numeric_cmp(&a.to_string(), b)),
            (Numeric(a), Float(b)) => a.parse::<f64>().ok()?.partial_cmp(b),
            (Float(a), Numeric(b)) => a.partial_cmp(&b.parse::<f64>().ok()?),
            (String(a), String(b)) => Some(a.cmp(b)),
            (Bool(a), Bool(b)) => Some(a.cmp(b)),
            (Date(a), Date(b)) => Some(a.cmp(b)),
            (Datetime(a), Datetime(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    /// Equality backing the `EQ` / `NEQ` / `IN` / `NOT_IN` operators:
    /// strict equality plus `Int` / `Float` / `Numeric` numeric coercion,
    /// so `Int(5)`, `Float(5.0)` and `Numeric("5.00")` are one value.
    pub fn loose_eq(&self, other: &Value) -> bool {
        match (self, other) {
            (
                Value::Int(_) | Value::Float(_) | Value::Numeric(_),
                Value::Int(_) | Value::Float(_) | Value::Numeric(_),
            ) => self.compare(other) == Some(Ordering::Equal),
            _ => self == other,
        }
    }
}

impl Value {
    /// A total order over values consistent with [`Value::eq`] (equal values
    /// compare `Equal`, different variants never do), used to keep the
    /// conditions of a disjunct in one canonical sequence so that two
    /// filters naming the same conditions in a different order normalize
    /// to the same disjunct. Variants are ranked in declaration order; within
    /// a variant the natural order applies, with every `NaN` treated as one
    /// value and `-0.0` as `0.0`, lists compared lexicographically and maps
    /// by their key-sorted entries.
    pub fn canonical_cmp(&self, other: &Value) -> Ordering {
        use Value::*;
        fn rank(value: &Value) -> u8 {
            match value {
                Null => 0,
                String(_) => 1,
                Int(_) => 2,
                Float(_) => 3,
                Numeric(_) => 4,
                Bool(_) => 5,
                Date(_) => 6,
                Datetime(_) => 7,
                List(_) => 8,
                Map(_) => 9,
                Set(_) => 10,
            }
        }
        fn canonical_f64(f: f64) -> f64 {
            if f.is_nan() {
                f64::NAN
            } else if f == 0.0 {
                0.0
            } else {
                f
            }
        }
        match (self, other) {
            (Null, Null) => Ordering::Equal,
            (String(a), String(b)) => a.cmp(b),
            (Int(a), Int(b)) => a.cmp(b),
            (Float(a), Float(b)) => canonical_f64(*a).total_cmp(&canonical_f64(*b)),
            (Numeric(a), Numeric(b)) => a.cmp(b),
            (Bool(a), Bool(b)) => a.cmp(b),
            (Date(a), Date(b)) => a.cmp(b),
            (Datetime(a), Datetime(b)) => a.cmp(b),
            (List(a), List(b)) => a
                .iter()
                .zip(b)
                .map(|(x, y)| x.canonical_cmp(y))
                .find(|ordering| *ordering != Ordering::Equal)
                .unwrap_or_else(|| a.len().cmp(&b.len())),
            (Set(a), Set(b)) => a.address().cmp(&b.address()),
            (Map(a), Map(b)) => {
                let mut left: Vec<_> = a.iter().collect();
                let mut right: Vec<_> = b.iter().collect();
                left.sort_by(|x, y| x.0.canonical_cmp(y.0));
                right.sort_by(|x, y| x.0.canonical_cmp(y.0));
                left.iter()
                    .zip(&right)
                    .map(|((ka, va), (kb, vb))| {
                        ka.canonical_cmp(kb).then_with(|| va.canonical_cmp(vb))
                    })
                    .find(|ordering| *ordering != Ordering::Equal)
                    .unwrap_or_else(|| left.len().cmp(&right.len()))
            }
            _ => rank(self).cmp(&rank(other)),
        }
    }
}

/// Floats up to this magnitude are exact integers, so an integral float
/// within it equates to the integer under [`Value::equality_key`].
const EXACT_INTEGER_LIMIT: f64 = 9_007_199_254_740_992.0;

/// Compare two exact decimal digit strings by numeric value
/// (`"1.50" == "1.5"`, `"2" > "1.9"`); the strings are plain digits with an
/// optional leading `-` and optional `.` fraction, as Postgres writes a
/// `numeric`.
pub(crate) fn numeric_cmp(a: &str, b: &str) -> Ordering {
    let (a_sign, a_int, a_frac) = split_numeric(a);
    let (b_sign, b_int, b_frac) = split_numeric(b);
    a_sign
        .cmp(&b_sign)
        .then_with(|| a_int.len().cmp(&b_int.len()))
        .then_with(|| a_int.cmp(b_int))
        .then_with(|| {
            let len = a_frac.len().max(b_frac.len());
            for index in 0..len {
                let a_digit = a_frac.as_bytes().get(index).map_or(b'0', |byte| *byte);
                let b_digit = b_frac.as_bytes().get(index).map_or(b'0', |byte| *byte);
                match a_digit.cmp(&b_digit) {
                    Ordering::Equal => {}
                    unequal => return unequal,
                }
            }
            Ordering::Equal
        })
}

/// A numeric text as (sign, integer digits, fraction digits), with the sign
/// `-1` for negative and `1` otherwise, leading zeros of the integer part
/// dropped and every zero (`-0`, `0.0`) collapsed to the same `(1, "0", …)`
/// form so equal values share a key.
fn split_numeric(text: &str) -> (i8, &str, &str) {
    let (sign, rest) = match text.strip_prefix('-') {
        Some(rest) => (-1i8, rest),
        None => (1i8, text.strip_prefix('+').unwrap_or(text)),
    };
    let (int, frac) = match rest.split_once('.') {
        Some((int, frac)) => (int, frac),
        None => (rest, ""),
    };
    let int = int.trim_start_matches('0');
    let int = if int.is_empty() { "0" } else { int };
    let zero = int == "0" && frac.bytes().all(|byte| byte == b'0');
    (if zero { 1 } else { sign }, int, frac)
}

/// Whether `text` is a numeric Postgres can have written: an optional sign,
/// digits with an optional fraction, and nothing else.
pub(crate) fn is_numeric_text(text: &str) -> bool {
    let rest = text.strip_prefix(['-', '+']).unwrap_or(text);
    let (int, frac) = match rest.split_once('.') {
        Some((int, frac)) => (int, frac),
        None => (rest, ""),
    };
    !int.is_empty()
        && int.bytes().all(|byte| byte.is_ascii_digit())
        && (frac.is_empty() || frac.bytes().all(|byte| byte.is_ascii_digit()))
}

/// A shared, mutable set of values: the operand of a set-valued `IN`, the
/// membership list of a join edge, held by the edge and referenced by the
/// driven part's filter. Two `SharedSet`s are equal only when they are the
/// same set (identity), which keeps a leaf's identity stable while its
/// contents change and lets identical subscriptions share one edge.
/// Members are stored by [`Value::equality_key`], so `Int(5)` and
/// `Float(5.0)` are one member. The set is behind a lock so that a value
/// holding one is `Send`: the engine thread alone mutates it, and the
/// threads that render or serialize a query only read it.
#[derive(Clone, Debug, Default)]
pub struct SharedSet(Arc<RwLock<HashSet<Value>>>);

impl SharedSet {
    /// An empty set.
    pub fn new() -> Self {
        SharedSet::default()
    }

    /// Whether `value` is a member (`NULL` never is).
    pub fn contains(&self, value: &Value) -> bool {
        value
            .equality_key_ref()
            .is_some_and(|key| self.read().contains(&*key))
    }

    /// Add `value`; reports whether it was new (`NULL` is never added).
    pub fn insert(&self, value: &Value) -> bool {
        match value.equality_key() {
            Some(key) => self.write().insert(key),
            None => false,
        }
    }

    /// Remove `value`; reports whether it was a member.
    pub fn remove(&self, value: &Value) -> bool {
        match value.equality_key() {
            Some(key) => self.write().remove(&key),
            None => false,
        }
    }

    /// The current members, as their equality keys, in no particular order.
    pub fn members(&self) -> Vec<Value> {
        self.read().iter().cloned().collect()
    }

    /// How many members the set holds.
    pub fn len(&self) -> usize {
        self.read().len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    /// The members for reading; a poisoned lock (a panic while writing)
    /// is read through, since the set is only ever mutated whole.
    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashSet<Value>> {
        self.0
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The members for writing; see [`SharedSet::read`] on poisoning.
    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashSet<Value>> {
        self.0
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The set's identity, for ordering and hashing.
    fn address(&self) -> usize {
        Arc::as_ptr(&self.0) as *const () as usize
    }
}

impl PartialEq for SharedSet {
    /// Identity: the same set, not equal contents.
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for SharedSet {}

impl Hash for SharedSet {
    /// Hashes the identity, never the contents.
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address().hash(state);
    }
}

impl Value {
    /// The key under which a value takes part in equality tests: `None` for
    /// `NULL` (which equals nothing), the integer for an integral float
    /// within exact range (the coercion of [`Value::loose_eq`]), the value
    /// itself otherwise. Two values are `loose_eq` iff their keys are equal,
    /// which is what lets hash maps and sets stand in for equality tests.
    pub fn equality_key(&self) -> Option<Value> {
        self.equality_key_ref().map(std::borrow::Cow::into_owned)
    }

    /// [`Value::equality_key`] without a copy: the value itself borrowed
    /// when it is its own key, an owned integer for an integral float or
    /// numeric within exact range (the coercion of [`Value::loose_eq`]).
    pub fn equality_key_ref(&self) -> Option<std::borrow::Cow<'_, Value>> {
        match self {
            Value::Null => None,
            Value::Float(f) if f.fract() == 0.0 && f.abs() <= EXACT_INTEGER_LIMIT => {
                Some(std::borrow::Cow::Owned(Value::Int(*f as i64)))
            }
            Value::Numeric(text) => match text.parse::<f64>() {
                Ok(f) if f.fract() == 0.0 && f.abs() <= EXACT_INTEGER_LIMIT => {
                    Some(std::borrow::Cow::Owned(Value::Int(f as i64)))
                }
                Ok(f) if f.is_finite() => Some(std::borrow::Cow::Owned(Value::Float(f))),
                _ => Some(std::borrow::Cow::Borrowed(self)),
            },
            other => Some(std::borrow::Cow::Borrowed(other)),
        }
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
            (Numeric(a), Numeric(b)) => a == b,
            (Bool(a), Bool(b)) => a == b,
            (Date(a), Date(b)) => a == b,
            (Datetime(a), Datetime(b)) => a == b,
            (List(a), List(b)) => a == b,
            (Map(a), Map(b)) => a == b,
            (Set(a), Set(b)) => a == b,
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
            Numeric(text) => text.hash(state),
            Bool(b) => b.hash(state),
            Date(d) => d.hash(state),
            Datetime(dt) => dt.hash(state),
            List(items) => items.hash(state),
            Map(entries) => unordered_map_hash(entries).hash(state),
            Set(set) => set.hash(state),
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
    unordered_pairs_hash(map.iter())
}

/// The order-independent hash of `(key, value)` pairs: each pair hashed
/// on its own, the results summed, so the same pairs in any order hash
/// alike (a row key laid out on a schema and one built from a map agree).
pub(crate) fn unordered_pairs_hash<K: Hash, V: Hash>(pairs: impl Iterator<Item = (K, V)>) -> u64 {
    pairs
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

    /// A `numeric` keeps its scale as an identity (`1.50` is not `1.5`)
    /// while still comparing equal by value, and a past-2⁵³ digit string
    /// survives intact.
    #[test]
    fn numerics_keep_scale_and_order_by_value() {
        let padded = Value::Numeric("1.50".into());
        let plain = Value::Numeric("1.5".into());
        assert_ne!(padded, plain);
        assert_ne!(hash_of(&padded), hash_of(&plain));
        assert_eq!(padded.compare(&plain), Some(Ordering::Equal));
        assert!(padded.loose_eq(&plain));
        let huge = Value::Numeric("9007199254740993.00".into());
        assert_eq!(
            huge,
            Value::Numeric("9007199254740993.00".into()),
            "every digit past 2⁵³ is kept"
        );
    }

    /// A value, and so a row, a key, a query and a write, can cross a
    /// thread boundary: the shared set is behind a lock, not an `Rc`.
    #[test]
    fn values_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Value>();
        assert_send_sync::<SharedSet>();
        assert_send_sync::<crate::model::DataFrameRow>();
        assert_send_sync::<crate::model::DataFrameKey>();
        assert_send_sync::<crate::model::WriteQuery>();
        assert_send_sync::<crate::model::MultiTableReadQuery>();
        assert_send_sync::<crate::ivm::Delta>();
    }

    /// [`Value::compare`] returns `None` for anything involving `Null`,
    /// including `Null` vs `Null`.
    #[test]
    fn null_compares_with_nothing() {
        assert_eq!(Value::Null.compare(&Value::Null), None);
        assert_eq!(Value::Null.compare(&Value::Int(1)), None);
    }
}
