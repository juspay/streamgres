use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// A cell value; sources convert their own types into these.
#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

/// The declared type of a column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ValueType {
    Bool,
    Int,
    Float,
    Text,
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Orders values of different types; null comes first.
    fn rank(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int(_) => 2,
            Value::Float(_) => 3,
            Value::Text(_) => 4,
        }
    }

    /// Compares two values of the same type; nulls and mixed types don't compare.
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => Some(float_cmp(*a, *b)),
            (Value::Text(a), Value::Text(b)) => Some(a.as_bytes().cmp(b.as_bytes())),
            _ => None,
        }
    }
}

/// Float order like Postgres: -0 equals 0, NaN equals NaN and sorts above every number.
fn float_cmp(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        _ => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
    }
}

/// Bits that are equal whenever `float_cmp` says the floats are equal.
fn float_bits(v: f64) -> u64 {
    if v.is_nan() {
        f64::NAN.to_bits()
    } else if v == 0.0 {
        0
    } else {
        v.to_bits()
    }
}

// Sort order for keys and ORDER BY; text sorts by bytes.
impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compare(other)
            .unwrap_or_else(|| self.rank().cmp(&other.rank()))
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.rank().hash(state);
        match self {
            Value::Null => {}
            Value::Bool(v) => v.hash(state),
            Value::Int(v) => v.hash(state),
            Value::Float(v) => float_bits(*v).hash(state),
            Value::Text(v) => v.hash(state),
        }
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_owned())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_sorts_first() {
        assert!(Value::Null < Value::Int(i64::MIN));
    }

    #[test]
    fn text_is_byte_order() {
        assert!(Value::from("B") < Value::from("a"));
    }

    #[test]
    fn compare_is_same_type_only() {
        assert_eq!(Value::Int(1).compare(&Value::Float(1.0)), None);
        assert_eq!(Value::Null.compare(&Value::Null), None);
    }

    #[test]
    fn floats_compare_like_postgres() {
        use std::collections::HashSet;
        assert_eq!(Value::Float(f64::NAN), Value::Float(-f64::NAN));
        assert_eq!(Value::Float(-0.0), Value::Float(0.0));
        assert!(Value::Float(f64::INFINITY) < Value::Float(f64::NAN));
        let set: HashSet<_> = [Value::Float(-0.0), Value::Float(0.0)]
            .into_iter()
            .collect();
        assert_eq!(set.len(), 1);
    }
}
