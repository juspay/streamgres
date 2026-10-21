//! JSON cells from the engine, encoded as the Postgres binary Diesel
//! decodes.
//!
//! The engine ships a row as JSON: strings, integers, floats, booleans,
//! JSON documents, times as microseconds since the Unix epoch, arrays.
//! Each cell is encoded for the column's exact Postgres type, so a cached
//! row decodes through the same `FromSql` as a live one. A type whose
//! value cannot be reproduced exactly from what the engine has is
//! refused ([`Unsupported`]), and the query keeps refilling from Postgres
//! instead: a `numeric` (the engine holds a float), ranges, intervals,
//! composites. A time comes back exact at any declared precision, since
//! the engine keeps Postgres's own microseconds; a float's `NaN` and
//! infinities come as the names Postgres gives them.

use std::num::NonZeroU32;

use serde_json::Value as Json;

use crate::row::OwnedValue;
use crate::types::{ColumnType, TypeKind};

/// Why a column cannot be encoded from the engine's cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsupported(pub String);

mod oid {
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
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;
}

const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// Whether every value of this column can be encoded exactly; checked
/// once per query so an unsupported column refuses the whole query up
/// front rather than at the first odd row.
pub fn supports(column: &ColumnType) -> Result<(), Unsupported> {
    match &column.kind {
        TypeKind::Base => supports_scalar(column.oid, column.typmod, &column.name),
        TypeKind::Enum => Ok(()),
        TypeKind::Array {
            element,
            element_kind,
        } => match element_kind.as_ref() {
            TypeKind::Base => supports_scalar(*element, -1, &column.name),
            TypeKind::Enum => Ok(()),
            _ => Err(Unsupported(format!(
                "column `{}` is an array of a type that cannot be encoded",
                column.name
            ))),
        },
        TypeKind::Other => Err(Unsupported(format!(
            "column `{}` has a type that cannot be encoded (oid {})",
            column.name, column.oid
        ))),
    }
}

fn supports_scalar(type_oid: u32, typmod: i32, name: &str) -> Result<(), Unsupported> {
    match type_oid {
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
        | oid::BYTEA
        | oid::DATE
        | oid::TIMESTAMP
        | oid::TIMESTAMPTZ => {
            let _ = typmod;
            Ok(())
        }
        1700 => Err(Unsupported(format!(
            "column `{name}` is a numeric, which the engine holds as a float"
        ))),
        other => Err(Unsupported(format!(
            "column `{name}` has type oid {other}, which cannot be encoded"
        ))),
    }
}

/// One cell for one column; `None` for SQL `NULL`.
pub fn encode(cell: &Json, column: &ColumnType) -> Result<Option<OwnedValue>, Unsupported> {
    if cell.is_null() {
        return Ok(None);
    }
    let type_oid = NonZeroU32::new(column.oid)
        .ok_or_else(|| Unsupported(format!("column `{}` has type oid 0", column.name)))?;
    let bytes = match &column.kind {
        TypeKind::Base => scalar(cell, column.oid, &column.name)?,
        TypeKind::Enum => text(cell, &column.name)?.into_bytes(),
        TypeKind::Array {
            element,
            element_kind,
        } => array(cell, *element, element_kind, &column.name)?,
        TypeKind::Other => return Err(Unsupported(format!("column `{}`", column.name))),
    };
    Ok(Some(OwnedValue::new(type_oid, bytes)))
}

fn scalar(cell: &Json, type_oid: u32, name: &str) -> Result<Vec<u8>, Unsupported> {
    Ok(match type_oid {
        oid::BOOL => vec![u8::from(
            cell.as_bool().ok_or_else(|| mismatch(name, "a boolean"))?,
        )],
        oid::INT2 => i16::try_from(integer(cell, name)?)
            .map_err(|_| mismatch(name, "a 16-bit integer"))?
            .to_be_bytes()
            .to_vec(),
        oid::INT4 => i32::try_from(integer(cell, name)?)
            .map_err(|_| mismatch(name, "a 32-bit integer"))?
            .to_be_bytes()
            .to_vec(),
        oid::OID => u32::try_from(integer(cell, name)?)
            .map_err(|_| mismatch(name, "an oid"))?
            .to_be_bytes()
            .to_vec(),
        oid::INT8 => integer(cell, name)?.to_be_bytes().to_vec(),
        oid::FLOAT4 => (float(cell, name)? as f32).to_be_bytes().to_vec(),
        oid::FLOAT8 => float(cell, name)?.to_be_bytes().to_vec(),
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::CHAR => {
            text(cell, name)?.into_bytes()
        }
        oid::UUID => {
            let text = text(cell, name)?;
            let hex: String = text.chars().filter(|c| *c != '-').collect();
            let bytes = hex_bytes(&hex).ok_or_else(|| mismatch(name, "a UUID"))?;
            if bytes.len() != 16 {
                return Err(mismatch(name, "a UUID"));
            }
            bytes
        }
        // The engine carries `bytea` as Postgres's hex text, `\x` and two
        // digits per byte; the binary format is the bytes themselves.
        oid::BYTEA => {
            let text = text(cell, name)?;
            let hex = text
                .strip_prefix("\\x")
                .ok_or_else(|| mismatch(name, "hex bytea"))?;
            hex_bytes(hex).ok_or_else(|| mismatch(name, "hex bytea"))?
        }
        oid::JSON => serde_json::to_vec(cell).map_err(|_| mismatch(name, "JSON"))?,
        oid::JSONB => {
            let mut bytes = vec![1u8];
            bytes.extend(serde_json::to_vec(cell).map_err(|_| mismatch(name, "JSON"))?);
            bytes
        }
        oid::TIMESTAMP | oid::TIMESTAMPTZ => {
            let micros = integer(cell, name)?;
            (micros - PG_EPOCH_MICROS).to_be_bytes().to_vec()
        }
        oid::DATE => {
            let micros = integer(cell, name)?;
            let days = (micros - PG_EPOCH_MICROS).div_euclid(86_400_000_000);
            i32::try_from(days)
                .map_err(|_| mismatch(name, "a date"))?
                .to_be_bytes()
                .to_vec()
        }
        other => return Err(Unsupported(format!("column `{name}` has type oid {other}"))),
    })
}

/// A one-dimensional array: the header (dimensions, a has-nulls flag,
/// the element type), one dimension (length, lower bound 1), then each
/// element as its length and bytes, `-1` for NULL.
fn array(
    cell: &Json,
    element: u32,
    element_kind: &TypeKind,
    name: &str,
) -> Result<Vec<u8>, Unsupported> {
    let items = cell.as_array().ok_or_else(|| mismatch(name, "an array"))?;
    let mut out = Vec::new();
    let has_nulls = items.iter().any(Json::is_null);
    out.extend_from_slice(&1i32.to_be_bytes());
    out.extend_from_slice(&i32::from(has_nulls).to_be_bytes());
    out.extend_from_slice(&element.to_be_bytes());
    out.extend_from_slice(&(items.len() as i32).to_be_bytes());
    out.extend_from_slice(&1i32.to_be_bytes());
    for item in items {
        if item.is_null() {
            out.extend_from_slice(&(-1i32).to_be_bytes());
            continue;
        }
        let bytes = match element_kind {
            TypeKind::Enum => text(item, name)?.into_bytes(),
            _ => scalar(item, element, name)?,
        };
        out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
        out.extend_from_slice(&bytes);
    }
    Ok(out)
}

fn integer(cell: &Json, name: &str) -> Result<i64, Unsupported> {
    cell.as_i64()
        .or_else(|| cell.as_f64().filter(|f| f.fract() == 0.0).map(|f| f as i64))
        .ok_or_else(|| mismatch(name, "an integer"))
}

/// A float cell: a number, or the name of a value JSON has no number
/// for (`NaN`, `Infinity`, `-Infinity`), as the engine sends them.
fn float(cell: &Json, name: &str) -> Result<f64, Unsupported> {
    match cell {
        Json::String(text) => match text.as_str() {
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            _ => Err(mismatch(name, "a number")),
        },
        other => other.as_f64().ok_or_else(|| mismatch(name, "a number")),
    }
}

/// Decodes hex digits, two per byte. `None` for an odd length or
/// anything that is not an ASCII hex digit; never panics, whatever the
/// text holds.
fn hex_bytes(hex: &str) -> Option<Vec<u8>> {
    let digits = hex.as_bytes();
    if !digits.len().is_multiple_of(2) {
        return None;
    }
    digits
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16)?;
            let low = (pair[1] as char).to_digit(16)?;
            Some((high * 16 + low) as u8)
        })
        .collect()
}

fn text(cell: &Json, name: &str) -> Result<String, Unsupported> {
    match cell {
        Json::String(text) => Ok(text.clone()),
        other => Err(mismatch(name, &format!("a string, found {other}"))),
    }
}

fn mismatch(name: &str, want: &str) -> Unsupported {
    Unsupported(format!("column `{name}`: expected {want}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn base(oid: u32) -> ColumnType {
        ColumnType {
            name: "c".into(),
            oid,
            typmod: -1,
            kind: TypeKind::Base,
        }
    }

    fn bytes(value: Option<OwnedValue>) -> Vec<u8> {
        value.expect("a value").bytes().to_vec()
    }

    #[test]
    fn scalars_take_their_postgres_widths() {
        assert_eq!(
            bytes(encode(&json!(7), &base(oid::INT4)).unwrap()),
            7i32.to_be_bytes()
        );
        assert_eq!(
            bytes(encode(&json!(7), &base(oid::INT8)).unwrap()),
            7i64.to_be_bytes()
        );
        assert_eq!(
            bytes(encode(&json!(-2), &base(oid::INT2)).unwrap()),
            (-2i16).to_be_bytes()
        );
        assert!(encode(&json!(70000), &base(oid::INT2)).is_err());
        assert_eq!(
            bytes(encode(&json!(2.5), &base(oid::FLOAT8)).unwrap()),
            2.5f64.to_be_bytes()
        );
        assert_eq!(
            bytes(encode(&json!("Infinity"), &base(oid::FLOAT8)).unwrap()),
            f64::INFINITY.to_be_bytes()
        );
        let nan = bytes(encode(&json!("NaN"), &base(oid::FLOAT8)).unwrap());
        assert!(f64::from_be_bytes(nan.try_into().unwrap()).is_nan());
        assert!(encode(&json!("lots"), &base(oid::FLOAT8)).is_err());
        assert_eq!(bytes(encode(&json!(true), &base(oid::BOOL)).unwrap()), [1]);
        assert_eq!(
            bytes(encode(&json!("hi"), &base(oid::TEXT)).unwrap()),
            b"hi"
        );
        assert_eq!(encode(&Json::Null, &base(oid::TEXT)).unwrap(), None);
        assert_eq!(
            bytes(
                encode(
                    &json!("12345678-9abc-def0-1122-334455667788"),
                    &base(oid::UUID)
                )
                .unwrap()
            ),
            [
                0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
                0x77, 0x88
            ]
        );
    }

    #[test]
    fn json_and_times() {
        let jsonb = bytes(encode(&json!({"a": [1, null]}), &base(oid::JSONB)).unwrap());
        assert_eq!(jsonb[0], 1);
        assert_eq!(
            serde_json::from_slice::<Json>(&jsonb[1..]).unwrap(),
            json!({"a": [1, null]})
        );
        let micros = PG_EPOCH_MICROS + 1_500_000;
        assert_eq!(
            bytes(encode(&json!(micros), &base(oid::TIMESTAMPTZ)).unwrap()),
            1_500_000i64.to_be_bytes()
        );
        assert_eq!(
            bytes(
                encode(
                    &json!(PG_EPOCH_MICROS + 2 * 86_400_000_000),
                    &base(oid::DATE)
                )
                .unwrap()
            ),
            2i32.to_be_bytes()
        );
    }

    #[test]
    fn malformed_hex_is_refused_not_panicked() {
        // A multi-byte character where a hex digit should be: slicing by
        // byte index would land inside it and panic.
        let uuid = base(oid::UUID);
        assert!(encode(&json!("é1234567-89ab-cdef-0123-456789abcdef"), &uuid).is_err());
        assert!(encode(&json!("ééééééééééééééééééééééééééééééé"), &uuid).is_err());
        assert!(
            encode(&json!("12345678-89ab-cdef-0123-456789abcde"), &uuid).is_err(),
            "too short"
        );
        let bytea = base(oid::BYTEA);
        assert!(encode(&json!("\\xé0"), &bytea).is_err());
        assert!(encode(&json!("\\xzz"), &bytea).is_err());
        assert!(encode(&json!("\\x0"), &bytea).is_err(), "odd length");
        assert_eq!(hex_bytes("00ff"), Some(vec![0, 255]));
    }

    #[test]
    fn bytea_is_decoded_from_hex_text() {
        let column = ColumnType {
            name: "blob".into(),
            oid: oid::BYTEA,
            typmod: -1,
            kind: TypeKind::Base,
        };
        assert!(supports(&column).is_ok());
        let value = encode(&json!("\\x00ff10"), &column).unwrap().unwrap();
        assert_eq!(value.bytes(), &[0x00, 0xff, 0x10]);
        assert!(encode(&json!("00ff"), &column).is_err(), "no prefix");
        assert!(encode(&json!("\\x0"), &column).is_err(), "odd digits");
    }

    #[test]
    fn what_is_refused_up_front() {
        assert!(
            supports(&base(oid::TIMESTAMP)).is_ok(),
            "microseconds, Postgres's own precision"
        );
        let mut ms = base(oid::TIMESTAMP);
        ms.typmod = 3;
        assert!(supports(&ms).is_ok());
        assert!(supports(&base(1186)).is_err(), "interval");
        assert!(supports(&base(1700)).is_err(), "numeric");
        assert!(
            supports(&base(oid::BYTEA)).is_ok(),
            "bytea comes as hex text"
        );
        assert!(supports(&base(3904)).is_err(), "an int4 range");
        let text_array = ColumnType {
            name: "tags".into(),
            oid: 1009,
            typmod: -1,
            kind: TypeKind::Array {
                element: oid::TEXT,
                element_kind: Box::new(TypeKind::Base),
            },
        };
        assert!(supports(&text_array).is_ok());
        let encoded = bytes(encode(&json!(["a", null, "c,d"]), &text_array).unwrap());
        // header: 1 dim, has nulls, text elements, length 3, lower bound 1
        assert_eq!(
            &encoded[..20],
            &[0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 25, 0, 0, 0, 3, 0, 0, 0, 1]
        );
        assert_eq!(
            &encoded[20..],
            &[
                0, 0, 0, 1, b'a', 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 3, b'c', b',', b'd'
            ]
        );
    }
}
