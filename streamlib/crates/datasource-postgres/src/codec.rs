//! Converts Postgres column types and text values into engine values.

use std::fmt;

use streamlib_datasource::{Result, SourceError, Value, ValueType};

/// A position in the Postgres WAL.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

impl Lsn {
    /// Parse Postgres's `X/Y` hex format.
    pub fn parse(text: &str) -> Result<Lsn> {
        let bad = || SourceError::Transient(format!("bad LSN `{text}`"));
        let (hi, lo) = text.split_once('/').ok_or_else(bad)?;
        let hi = u64::from_str_radix(hi, 16).map_err(|_| bad())?;
        let lo = u64::from_str_radix(lo, 16).map_err(|_| bad())?;
        Ok(Lsn((hi << 32) | lo))
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xFFFF_FFFF)
    }
}

/// A column's Postgres type, as the catalog reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgType {
    /// Type name, like `int8`, `timestamptz`, or `_int4` for an int array.
    pub name: String,
    /// Type kind: `b` base, `e` enum, `d` domain, `c` composite.
    pub kind: char,
}

/// Maps Postgres types to engine values; reads and the stream share it so they always agree.
pub trait PgCodec: Send + Sync + 'static {
    /// The engine type for a column, or `None` to refuse the column.
    fn value_type(&self, ty: &PgType) -> Option<ValueType>;

    /// Decode one non-null value from Postgres's text form.
    fn decode(&self, ty: ValueType, text: &str) -> Result<Value> {
        decode_text(ty, text)
    }
}

/// The default codec: numbers and booleans by type, other common types as text.
pub struct TextCodec;

impl PgCodec for TextCodec {
    fn value_type(&self, ty: &PgType) -> Option<ValueType> {
        if ty.kind == 'e' {
            return Some(ValueType::Text);
        }
        Some(match ty.name.as_str() {
            "bool" => ValueType::Bool,
            "int2" | "int4" | "int8" => ValueType::Int,
            "float4" | "float8" => ValueType::Float,
            "text" | "varchar" | "bpchar" | "name" | "uuid" | "json" | "jsonb" | "numeric"
            | "date" | "time" | "timetz" | "timestamp" | "timestamptz" | "interval" | "inet"
            | "cidr" | "macaddr" => ValueType::Text,
            _ => return None,
        })
    }
}

/// Convert Postgres text into an engine value of the given type.
pub fn decode_text(ty: ValueType, text: &str) -> Result<Value> {
    let bad = || SourceError::Refused(format!("can't read `{text}` as {ty:?}"));
    Ok(match ty {
        ValueType::Bool => match text {
            "t" => Value::Bool(true),
            "f" => Value::Bool(false),
            _ => return Err(bad()),
        },
        ValueType::Int => Value::Int(text.parse().map_err(|_| bad())?),
        ValueType::Float => Value::Float(text.parse().map_err(|_| bad())?),
        ValueType::Text => Value::Text(text.to_owned()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(ty: ValueType, text: &str) -> Result<Value> {
        TextCodec.decode(ty, text)
    }

    #[test]
    fn lsn_round_trips() {
        let lsn = Lsn::parse("16/B374D848").unwrap();
        assert_eq!(lsn.0, 0x16_B374_D848);
        assert_eq!(lsn.to_string(), "16/B374D848");
    }

    #[test]
    fn decodes_postgres_text() {
        assert_eq!(decode(ValueType::Bool, "t").unwrap(), Value::Bool(true));
        assert_eq!(decode(ValueType::Int, "-42").unwrap(), Value::Int(-42));
        assert_eq!(
            decode(ValueType::Float, "Infinity").unwrap(),
            Value::Float(f64::INFINITY)
        );
        assert_eq!(
            decode(ValueType::Float, "NaN").unwrap(),
            Value::Float(f64::NAN)
        );
        assert!(decode(ValueType::Int, "x").is_err());
    }

    #[test]
    fn refuses_unknown_types() {
        let ty = |name: &str, kind| PgType {
            name: name.into(),
            kind,
        };
        assert_eq!(TextCodec.value_type(&ty("int8", 'b')), Some(ValueType::Int));
        assert_eq!(
            TextCodec.value_type(&ty("mood", 'e')),
            Some(ValueType::Text)
        );
        assert_eq!(TextCodec.value_type(&ty("_int4", 'b')), None);
        assert_eq!(TextCodec.value_type(&ty("bytea", 'b')), None);
    }
}
