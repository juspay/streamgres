//! Postgres's binary format, for the types the engine holds exactly.
//!
//! An application's driver sends bind parameters in binary and reads rows
//! back in binary, so a cache in front of it speaks the same format. This
//! covers the types whose engine value is exactly Postgres's: booleans,
//! integers, floats, text and the text-like types, UUIDs, JSON, and enum
//! labels. Everything else (numeric, dates and times, bytea, intervals,
//! arrays) is not encoded here; [`exact`] says which types a cache may
//! answer for.

use std::fmt;

use crate::model::{DbColumn, Value, ValueType};

/// The OIDs of the built-in types the codec knows.
pub mod oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const CHAR: u32 = 18;
    pub const NAME: u32 = 19;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const OID: u32 = 26;
    pub const JSON: u32 = 114;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIME: u32 = 1083;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;
}

/// The version byte `jsonb_send` writes before the text.
const JSONB_VERSION: u8 = 1;

/// Why bytes are not a value of the type they were sent as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecError {
    pub type_oid: u32,
    pub reason: &'static str,
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "bytes are not a value of type {}: {}",
            self.type_oid, self.reason
        )
    }
}

impl std::error::Error for CodecError {}

/// Whether the engine holds a column's values exactly as Postgres does,
/// so that a value read from the engine and encoded here is the bytes
/// Postgres itself would send. A column of OID 0 (declared by hand, or
/// added by a schema change the engine absorbed) is not.
pub fn exact(column: &DbColumn) -> bool {
    match column.r#type {
        ValueType::Bool => column.type_oid == oid::BOOL,
        ValueType::Int => matches!(
            column.type_oid,
            oid::INT2 | oid::INT4 | oid::INT8 | oid::OID
        ),
        ValueType::Float => matches!(column.type_oid, oid::FLOAT4 | oid::FLOAT8),
        ValueType::Json => matches!(column.type_oid, oid::JSON | oid::JSONB),
        ValueType::String => {
            matches!(
                column.type_oid,
                oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::CHAR | oid::UUID
            ) || column.enum_type
        }
        _ => false,
    }
}

/// The element type an array parameter of `type_oid` holds: the built-in
/// array's, or the one its binary form names in its header (an enum's
/// array, a domain's). `None` when neither says.
pub fn bound_element(type_oid: u32, bytes: Option<&[u8]>) -> Option<u32> {
    array_element(type_oid).or_else(|| {
        let header = bytes?.get(8..12)?;
        Some(u32::from_be_bytes(header.try_into().ok()?))
    })
}

/// The element type of a built-in array type; `None` for any other type.
pub fn array_element(type_oid: u32) -> Option<u32> {
    Some(match type_oid {
        1000 => oid::BOOL,
        1002 => oid::CHAR,
        1003 => oid::NAME,
        1005 => oid::INT2,
        1007 => oid::INT4,
        1009 => oid::TEXT,
        1014 => oid::BPCHAR,
        1015 => oid::VARCHAR,
        1016 => oid::INT8,
        1021 => oid::FLOAT4,
        1022 => oid::FLOAT8,
        2951 => oid::UUID,
        1028 => oid::OID,
        _ => return None,
    })
}

/// Whether `type_oid` is a built-in type the codec reads.
fn known(type_oid: u32) -> bool {
    matches!(
        type_oid,
        oid::BOOL
            | oid::INT2
            | oid::INT4
            | oid::INT8
            | oid::OID
            | oid::FLOAT4
            | oid::FLOAT8
            | oid::TEXT
            | oid::VARCHAR
            | oid::BPCHAR
            | oid::NAME
            | oid::CHAR
            | oid::UUID
            | oid::JSON
            | oid::JSONB
    )
}

/// Decodes one non-null parameter compared with a column declared as
/// `declared`, sent as type `type_oid`. A text column's parameter of a
/// type the codec does not know (an enum label) reads as UTF-8, which is
/// what those types send. A list reads Postgres's one-dimensional array
/// format.
pub fn decode(declared: &ValueType, type_oid: u32, bytes: &[u8]) -> Result<Value, CodecError> {
    let fail = |reason| CodecError { type_oid, reason };
    match declared {
        ValueType::List(inner) => decode_array(inner, type_oid, bytes),
        ValueType::String if !known(type_oid) => utf8(bytes)
            .map(|text| Value::String(text.to_owned()))
            .map_err(|_| fail("not UTF-8")),
        _ => decode_scalar(type_oid, bytes),
    }
}

/// Decodes one non-null parameter by its type alone.
pub fn decode_scalar(type_oid: u32, bytes: &[u8]) -> Result<Value, CodecError> {
    let fail = |reason| CodecError { type_oid, reason };
    Ok(match type_oid {
        oid::BOOL => Value::Bool(fixed::<1>(bytes, type_oid)? != [0]),
        oid::INT2 => Value::Int(i64::from(i16::from_be_bytes(fixed::<2>(bytes, type_oid)?))),
        oid::INT4 => Value::Int(i64::from(i32::from_be_bytes(fixed::<4>(bytes, type_oid)?))),
        oid::INT8 => Value::Int(i64::from_be_bytes(fixed::<8>(bytes, type_oid)?)),
        oid::OID => Value::Int(i64::from(u32::from_be_bytes(fixed::<4>(bytes, type_oid)?))),
        oid::FLOAT4 => Value::Float(f64::from(f32::from_be_bytes(fixed::<4>(bytes, type_oid)?))),
        oid::FLOAT8 => Value::Float(f64::from_be_bytes(fixed::<8>(bytes, type_oid)?)),
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::CHAR => {
            Value::String(utf8(bytes).map_err(|_| fail("not UTF-8"))?.to_owned())
        }
        oid::UUID => Value::String(uuid_text(&fixed::<16>(bytes, type_oid)?)),
        _ => return Err(fail("a type the cache does not compare by")),
    })
}

/// Bytes as UTF-8 text.
fn utf8(bytes: &[u8]) -> Result<&str, std::str::Utf8Error> {
    std::str::from_utf8(bytes)
}

/// Bytes that must be exactly `N` long, as an array.
fn fixed<const N: usize>(bytes: &[u8], type_oid: u32) -> Result<[u8; N], CodecError> {
    bytes.try_into().map_err(|_| CodecError {
        type_oid,
        reason: "wrong length",
    })
}

/// A UUID as Postgres writes it: lowercase hex in 8-4-4-4-12 groups.
fn uuid_text(bytes: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(36);
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// A UUID's 16 bytes from its text, in any case, with or without hyphens.
fn uuid_bytes(text: &str) -> Option<[u8; 16]> {
    let hex: Vec<u8> = text.bytes().filter(|byte| *byte != b'-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (slot, pair) in out.iter_mut().zip(hex.chunks(2)) {
        let pair = std::str::from_utf8(pair).ok()?;
        *slot = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(out)
}

/// Reads a big-endian `i32` at `at`, moving `at` past it.
fn read_i32(bytes: &[u8], at: &mut usize, type_oid: u32) -> Result<i32, CodecError> {
    let word = bytes.get(*at..*at + 4).ok_or(CodecError {
        type_oid,
        reason: "truncated array",
    })?;
    *at += 4;
    Ok(i32::from_be_bytes(fixed::<4>(word, type_oid)?))
}

/// Decodes Postgres's array format: a one-dimensional array, lower bound
/// 1, becomes a list of its elements decoded as `inner`.
fn decode_array(inner: &ValueType, type_oid: u32, bytes: &[u8]) -> Result<Value, CodecError> {
    let fail = |reason| CodecError { type_oid, reason };
    let mut at = 0;
    let dimensions = read_i32(bytes, &mut at, type_oid)?;
    let _has_nulls = read_i32(bytes, &mut at, type_oid)?;
    let element_oid = u32::from_be_bytes(fixed::<4>(
        bytes.get(at..at + 4).ok_or(fail("truncated array"))?,
        type_oid,
    )?);
    at += 4;
    match dimensions {
        0 => return Ok(Value::List(Vec::new())),
        1 => {}
        _ => return Err(fail("an array of more than one dimension")),
    }
    let length = usize::try_from(read_i32(bytes, &mut at, type_oid)?)
        .map_err(|_| fail("negative array length"))?;
    if read_i32(bytes, &mut at, type_oid)? != 1 {
        return Err(fail("an array whose lower bound is not 1"));
    }
    let mut items = Vec::with_capacity(length.min(bytes.len() / 4));
    for _ in 0..length {
        let size = read_i32(bytes, &mut at, type_oid)?;
        if size < 0 {
            items.push(Value::Null);
            continue;
        }
        let size = usize::try_from(size).map_err(|_| fail("bad element length"))?;
        let element = bytes
            .get(at..at + size)
            .ok_or(fail("truncated array element"))?;
        at += size;
        items.push(decode(inner, element_oid, element)?);
    }
    if at != bytes.len() {
        return Err(fail("trailing bytes after array"));
    }
    Ok(Value::List(items))
}

/// Appends `value` in the binary format of `type_oid` (an enum's when
/// `enum_type`) to `out`; whether it could be, `out` as it was when not
/// (or for `NULL`, which has no bytes). Only a column [`exact`] says yes
/// for encodes.
pub fn encode_into(value: &Value, type_oid: u32, enum_type: bool, out: &mut Vec<u8>) -> bool {
    let at = out.len();
    let written = match (value, type_oid) {
        (Value::Null, _) => false,
        (Value::Bool(flag), oid::BOOL) => {
            out.push(u8::from(*flag));
            true
        }
        (Value::Int(int), oid::INT2) => i16::try_from(*int)
            .map(|int| out.extend_from_slice(&int.to_be_bytes()))
            .is_ok(),
        (Value::Int(int), oid::INT4) => i32::try_from(*int)
            .map(|int| out.extend_from_slice(&int.to_be_bytes()))
            .is_ok(),
        (Value::Int(int), oid::INT8) => {
            out.extend_from_slice(&int.to_be_bytes());
            true
        }
        (Value::Int(int), oid::OID) => u32::try_from(*int)
            .map(|int| out.extend_from_slice(&int.to_be_bytes()))
            .is_ok(),
        (Value::Float(float), oid::FLOAT8) => {
            out.extend_from_slice(&float.to_be_bytes());
            true
        }
        (Value::Float(float), oid::FLOAT4) => {
            let narrow = *float as f32;
            let exact = f64::from(narrow) == *float || float.is_nan();
            if exact {
                out.extend_from_slice(&narrow.to_be_bytes());
            }
            exact
        }
        (Value::String(text), oid::UUID) => match uuid_bytes(text) {
            Some(bytes) => {
                out.extend_from_slice(&bytes);
                true
            }
            None => false,
        },
        (Value::String(text), oid::JSONB) => {
            out.push(JSONB_VERSION);
            out.extend_from_slice(text.as_bytes());
            true
        }
        (Value::String(text), oid::JSON)
        | (Value::String(text), oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::CHAR) => {
            out.extend_from_slice(text.as_bytes());
            true
        }
        (Value::String(text), _) if enum_type => {
            out.extend_from_slice(text.as_bytes());
            true
        }
        _ => false,
    };
    if !written {
        out.truncate(at);
    }
    written
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(r#type: ValueType, type_oid: u32) -> DbColumn {
        DbColumn::new("c", r#type).with_type_oid(type_oid)
    }

    fn round_trip(r#type: ValueType, type_oid: u32, bytes: &[u8]) {
        let value = decode(&r#type, type_oid, bytes).unwrap();
        let mut out = Vec::new();
        assert!(encode_into(&value, type_oid, false, &mut out));
        assert_eq!(out, bytes, "{value:?}");
    }

    /// Every scalar the codec reads encodes back to the bytes it came from.
    #[test]
    fn scalars_round_trip() {
        round_trip(ValueType::Bool, oid::BOOL, &[1]);
        round_trip(ValueType::Int, oid::INT2, &(-2i16).to_be_bytes());
        round_trip(ValueType::Int, oid::INT4, &7i32.to_be_bytes());
        round_trip(ValueType::Int, oid::INT8, &i64::MIN.to_be_bytes());
        round_trip(ValueType::Float, oid::FLOAT4, &1.5f32.to_be_bytes());
        round_trip(ValueType::Float, oid::FLOAT8, &0.1f64.to_be_bytes());
        round_trip(ValueType::String, oid::TEXT, b"it's");
        round_trip(ValueType::String, oid::BPCHAR, b"ab ");
        round_trip(
            ValueType::String,
            oid::UUID,
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        );
        assert_eq!(
            decode_scalar(
                oid::UUID,
                &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
            ),
            Ok(Value::from("00010203-0405-0607-0809-0a0b0c0d0e0f"))
        );
    }

    /// A one-dimensional array decodes to a list, nulls kept.
    #[test]
    fn arrays_decode() {
        let mut bytes = Vec::new();
        for word in [1i32, 1, oid::INT8 as i32, 2, 1, 8] {
            bytes.extend_from_slice(&word.to_be_bytes());
        }
        bytes.extend_from_slice(&5i64.to_be_bytes());
        bytes.extend_from_slice(&(-1i32).to_be_bytes());
        assert_eq!(
            decode(&ValueType::List(Box::new(ValueType::Int)), 1016, &bytes),
            Ok(Value::List(vec![Value::Int(5), Value::Null]))
        );
        assert_eq!(bound_element(1016, None), Some(oid::INT8));
    }

    /// Only columns whose values are Postgres's own are exact.
    #[test]
    fn exact_types() {
        assert!(exact(&column(ValueType::Int, oid::INT8)));
        assert!(exact(&column(ValueType::String, oid::UUID)));
        assert!(!exact(&column(ValueType::Float, oid::NUMERIC)));
        assert!(!exact(&column(ValueType::Timestamp, oid::TIMESTAMPTZ)));
        assert!(!exact(&column(ValueType::Int, 0)));
        assert!(!exact(&column(ValueType::String, 99_001)));
        assert!(exact(&column(ValueType::String, 99_001).with_enum_type()));
    }
}
