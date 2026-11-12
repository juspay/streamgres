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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use chrono::{NaiveDate, NaiveDateTime};

pub use super::numeric::Numeric;
pub use super::time::{Day, Time, Timestamp};

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
    Set(SharedSet),
    /// A JSON document: compared by its standard form, kept with the text
    /// Postgres gave when that differs.
    Json(JsonText),
    /// A Postgres `numeric`, exact.
    Numeric(Numeric),
    /// A Postgres `timestamp` or `timestamptz`.
    Timestamp(Timestamp),
    /// A Postgres `date`.
    Day(Day),
    /// A Postgres `time`.
    Time(Time),
    /// A Postgres `bytea`.
    Bytes(Bytes),
    /// A value of a type the engine does not interpret, as Postgres's
    /// binary form. It can be stored and sent on, never compared.
    Raw(Raw),
}

/// A JSON document. Two documents are the same value when their standard
/// forms are the same text ([`crate::sync::pg::text`]): that is what the
/// engine compares, orders and hashes. The text Postgres stored or wrote
/// is kept beside it when it is spelled otherwise (a `json` column keeps
/// its input as written, a `jsonb` number its trailing zeros), so the
/// document can be handed back exactly.
#[derive(Debug, Clone)]
pub struct JsonText {
    standard: Box<str>,
    text: Option<Box<str>>,
    known: bool,
}

impl JsonText {
    /// A document in `standard` form, given by its source as `text`.
    pub fn new(standard: String, text: &str) -> Self {
        let text = (text != standard).then(|| text.into());
        JsonText {
            standard: standard.into(),
            text,
            known: true,
        }
    }

    /// A document known only in its standard form: a literal compared
    /// with a column, or a value Postgres wrote without saying how it
    /// spells it (a column's default filled into rows already held).
    pub fn standard_only(standard: String) -> Self {
        JsonText {
            standard: standard.into(),
            text: None,
            known: false,
        }
    }

    /// Whether [`JsonText::text`] is the text Postgres has, rather than
    /// only the standard form.
    pub fn is_known(&self) -> bool {
        self.known
    }

    /// The standard form, what the engine compares.
    pub fn standard(&self) -> &str {
        &self.standard
    }

    /// The text the source gave.
    pub fn text(&self) -> &str {
        self.text.as_deref().unwrap_or(&self.standard)
    }
}

impl PartialEq for JsonText {
    fn eq(&self, other: &Self) -> bool {
        self.standard == other.standard
    }
}

impl Eq for JsonText {}

impl Hash for JsonText {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.standard.hash(state);
    }
}

impl PartialOrd for JsonText {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JsonText {
    fn cmp(&self, other: &Self) -> Ordering {
        self.standard.cmp(&other.standard)
    }
}

/// A value of a type the engine does not interpret: its type and its
/// Postgres binary form.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Raw {
    pub type_oid: u32,
    pub bytes: Bytes,
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
    /// A Postgres `timestamp` or `timestamptz`; the cell is a
    /// [`Value::Timestamp`].
    Timestamp,
    /// A Postgres `json` or `jsonb`; the cell is a [`Value::Json`].
    Json,
    /// A Postgres `numeric`; the cell is a [`Value::Numeric`].
    Numeric,
    /// A Postgres `date`; the cell is a [`Value::Day`].
    Day,
    /// A Postgres `time`; the cell is a [`Value::Time`].
    Time,
    /// A Postgres `bytea`; the cell is a [`Value::Bytes`].
    Bytes,
    /// A type the engine does not interpret; the cell is a [`Value::Raw`].
    Raw,
}

impl Value {
    /// Whether this value is [`Value::Null`].
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Ordering comparison backing the `GT` / `GTE` / `LT` / `LTE` operators.
    ///
    /// Values compare only within the same variant, except the numbers:
    /// `Int` / `Float` coerce to `f64` (lossy above 2⁵³ — mixed-type
    /// comparisons of such magnitudes may tie spuriously; same-variant
    /// comparisons are always exact), `Int` / `Numeric` compare exactly,
    /// and `Float` / `Numeric` compare as `f64`, as Postgres compares
    /// `float8` with `numeric`. Floats order as Postgres orders them
    /// ([`float_cmp`]). Everything else — including anything involving
    /// `Null` — returns `None`, which predicate evaluation treats as
    /// "does not match" (SQL three-valued logic collapsed to `false`).
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        use Value::*;
        match (self, other) {
            (Int(a), Int(b)) => Some(a.cmp(b)),
            (Float(a), Float(b)) => Some(float_cmp(*a, *b)),
            (Int(a), Float(b)) => Some(float_cmp(*a as f64, *b)),
            (Float(a), Int(b)) => Some(float_cmp(*a, *b as f64)),
            (String(a), String(b)) => Some(a.cmp(b)),
            (Bool(a), Bool(b)) => Some(a.cmp(b)),
            (Date(a), Date(b)) => Some(a.cmp(b)),
            (Datetime(a), Datetime(b)) => Some(a.cmp(b)),
            (Json(a), Json(b)) => Some(a.cmp(b)),
            (Numeric(a), Numeric(b)) => Some(a.cmp(b)),
            (Int(a), Numeric(b)) => Some(self::Numeric::from_i64(*a).cmp(b)),
            (Numeric(a), Int(b)) => Some(a.cmp(&self::Numeric::from_i64(*b))),
            (Float(a), Numeric(b)) => Some(float_cmp(*a, b.to_f64())),
            (Numeric(a), Float(b)) => Some(float_cmp(a.to_f64(), *b)),
            (Timestamp(a), Timestamp(b)) => Some(a.cmp(b)),
            (Day(a), Day(b)) => Some(a.cmp(b)),
            (Time(a), Time(b)) => Some(a.cmp(b)),
            (Bytes(a), Bytes(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    /// Equality backing the `EQ` / `NEQ` / `IN` / `NOT_IN` operators:
    /// strict equality plus numeric coercion between `Int`, `Float` and
    /// `Numeric`, so `Int(5)` equals `Float(5.0)` and `Numeric(5.00)`, and
    /// a float equals a numeric as Postgres casts the numeric to `float8`.
    pub fn loose_eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Int(_), Value::Float(_))
            | (Value::Float(_), Value::Int(_))
            | (Value::Float(_), Value::Numeric(_))
            | (Value::Numeric(_), Value::Float(_)) => {
                self.compare(other) == Some(Ordering::Equal)
            }
            (Value::Null, _) | (_, Value::Null) => self == other,
            _ => self.equality_key_ref() == other.equality_key_ref(),
        }
    }
}

/// Postgres's order of `float8`: every `NaN` equal to every other and
/// above every number, infinity included; `-0` equal to `0`.
pub fn float_cmp(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
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
                Bool(_) => 4,
                Date(_) => 5,
                Datetime(_) => 6,
                List(_) => 7,
                Map(_) => 8,
                Set(_) => 9,
                Numeric(_) => 10,
                Timestamp(_) => 11,
                Day(_) => 12,
                Time(_) => 13,
                Bytes(_) => 14,
                Raw(_) => 15,
                Json(_) => 16,
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
            (Numeric(a), Numeric(b)) => a.cmp(b),
            (Timestamp(a), Timestamp(b)) => a.cmp(b),
            (Day(a), Day(b)) => a.cmp(b),
            (Time(a), Time(b)) => a.cmp(b),
            (Bytes(a), Bytes(b)) => a.cmp(b),
            (Raw(a), Raw(b)) => a.cmp(b),
            (Json(a), Json(b)) => a.cmp(b),
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
    /// itself otherwise. Two values of one column's type are `loose_eq` iff
    /// their keys are equal, which is what lets hash maps and sets stand in
    /// for equality tests. A float and a non-integral numeric keep keys of
    /// their own, so a condition's value must have its column's type: the
    /// parsers bring every literal and parameter to it.
    pub fn equality_key(&self) -> Option<Value> {
        self.equality_key_ref().map(std::borrow::Cow::into_owned)
    }

    /// [`Value::equality_key`] without a copy: the value itself borrowed
    /// when it is its own key, an owned integer for an integral float.
    pub fn equality_key_ref(&self) -> Option<std::borrow::Cow<'_, Value>> {
        match self {
            Value::Null => None,
            Value::Float(f) if f.fract() == 0.0 && f.abs() <= EXACT_INTEGER_LIMIT => {
                Some(std::borrow::Cow::Owned(Value::Int(*f as i64)))
            }
            Value::Numeric(n) => match n.to_i64() {
                Some(int) => Some(std::borrow::Cow::Owned(Value::Int(int))),
                None => Some(std::borrow::Cow::Borrowed(self)),
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
            (Bool(a), Bool(b)) => a == b,
            (Date(a), Date(b)) => a == b,
            (Datetime(a), Datetime(b)) => a == b,
            (List(a), List(b)) => a == b,
            (Map(a), Map(b)) => a == b,
            (Set(a), Set(b)) => a == b,
            (Numeric(a), Numeric(b)) => a == b,
            (Timestamp(a), Timestamp(b)) => a == b,
            (Day(a), Day(b)) => a == b,
            (Time(a), Time(b)) => a == b,
            (Bytes(a), Bytes(b)) => a == b,
            (Raw(a), Raw(b)) => a == b,
            (Json(a), Json(b)) => a == b,
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
            Set(set) => set.hash(state),
            Numeric(n) => n.hash(state),
            Timestamp(at) => at.hash(state),
            Day(day) => day.hash(state),
            Time(time) => time.hash(state),
            Bytes(bytes) => bytes.hash(state),
            Raw(raw) => raw.hash(state),
            Json(json) => json.hash(state),
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

    /// Numerics compare and equate by value with integers and with each
    /// other, whatever their scale, and hash alike when equal.
    #[test]
    fn numerics_equate_by_value() {
        let n = |text: &str| Value::Numeric(Numeric::parse(text).unwrap());
        assert!(n("5.00").loose_eq(&Value::Int(5)));
        assert!(Value::Int(5).loose_eq(&n("5")));
        assert!(n("1.50").loose_eq(&n("1.5")));
        assert!(!n("1.5").loose_eq(&Value::Int(1)));
        assert_eq!(n("1.50"), n("1.5"));
        assert_eq!(hash_of(&n("1.50")), hash_of(&n("1.5")));
        assert_eq!(n("2").equality_key(), Some(Value::Int(2)));
        assert_eq!(Value::Int(3).compare(&n("2.5")), Some(Ordering::Greater));
        assert!(Value::Float(0.1).loose_eq(&n("0.10")));
    }

    /// Raw values compare with nothing, and equal only the same type and
    /// bytes.
    #[test]
    fn raw_values_never_compare() {
        let raw = |oid: u32, bytes: &'static [u8]| {
            Value::Raw(Raw {
                type_oid: oid,
                bytes: Bytes::from_static(bytes),
            })
        };
        assert_eq!(raw(1186, b"\x01").compare(&raw(1186, b"\x01")), None);
        assert_eq!(raw(1186, b"\x01"), raw(1186, b"\x01"));
        assert_ne!(raw(1186, b"\x01"), raw(869, b"\x01"));
    }

    /// [`Value::compare`] returns `None` for anything involving `Null`,
    /// including `Null` vs `Null`.
    #[test]
    fn null_compares_with_nothing() {
        assert_eq!(Value::Null.compare(&Value::Null), None);
        assert_eq!(Value::Null.compare(&Value::Int(1)), None);
    }
}
