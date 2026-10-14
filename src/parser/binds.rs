//! Bind parameters as a Postgres client sends them, decoded into values.
//!
//! An ORM renders a query with `$n` placeholders and ships each parameter
//! in Postgres's binary format with its type OID. The engine's parser
//! accepts those placeholders ([`super::parse_read_with_binds`]) and reads
//! the parameter here, by OID, into the [`Value`] the literal would have
//! parsed to; the column's declared type then coerces it the way it
//! coerces a literal, so `$1` and `'OPEN'` take the same path.
//!
//! Only the types a predicate compares on are decoded: integers, floats,
//! numerics, booleans, text-like types, UUIDs, JSON, timestamps and dates.
//! Anything else is refused by OID, never guessed at.

use crate::model::Value;
use crate::sync::pg::text::json_as_jsonb;

/// One bind parameter: its type OID and its bytes, `None` for `NULL`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindValue {
    pub type_oid: u32,
    pub bytes: Option<Vec<u8>>,
}

/// Postgres's built-in type OIDs that a predicate can carry.
pub(crate) mod oid {
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
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;

    /// Array types, by element: what `= ANY($1)` binds.
    pub const BOOL_ARRAY: u32 = 1000;
    pub const INT2_ARRAY: u32 = 1005;
    pub const INT4_ARRAY: u32 = 1007;
    pub const INT8_ARRAY: u32 = 1016;
    pub const TEXT_ARRAY: u32 = 1009;
    pub const VARCHAR_ARRAY: u32 = 1015;
    pub const BPCHAR_ARRAY: u32 = 1014;
    pub const FLOAT4_ARRAY: u32 = 1021;
    pub const FLOAT8_ARRAY: u32 = 1022;
    pub const NUMERIC_ARRAY: u32 = 1231;
    pub const DATE_ARRAY: u32 = 1182;
    pub const TIMESTAMP_ARRAY: u32 = 1115;
    pub const TIMESTAMPTZ_ARRAY: u32 = 1185;
    pub const UUID_ARRAY: u32 = 2951;
    pub const JSONB_ARRAY: u32 = 3807;
}

/// Milliseconds from the Unix epoch to Postgres's epoch (2000-01-01).
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// The value a bind stands for, or why it cannot stand for one.
/// Whether an enum bind's bytes are an array rather than one label: an
/// enum type's array OID is as much an enum OID as the scalar's, and the
/// two are told apart by the bytes. A one-dimensional array header is
/// 20 bytes with a dimension count of 0 or 1; a label is text, which
/// never starts with three zero bytes.
pub fn looks_like_array(bytes: &[u8]) -> bool {
    bytes.len() >= 12 && bytes[..3] == [0, 0, 0] && bytes[3] <= 1
}

pub fn decode(bind: &BindValue) -> Result<Value, String> {
    let Some(bytes) = bind.bytes.as_deref() else {
        return Ok(Value::Null);
    };
    let bytes_of = |want: usize| -> Result<&[u8], String> {
        (bytes.len() == want).then_some(bytes).ok_or_else(|| {
            format!(
                "a bind of type oid {} has {} bytes, not {want}",
                bind.type_oid,
                bytes.len()
            )
        })
    };
    Ok(match bind.type_oid {
        oid::BOOL => Value::Bool(bytes_of(1)?[0] != 0),
        oid::INT2 => Value::Int(i16::from_be_bytes(bytes_of(2)?.try_into().unwrap()).into()),
        oid::INT4 | oid::OID => {
            Value::Int(i32::from_be_bytes(bytes_of(4)?.try_into().unwrap()).into())
        }
        oid::INT8 => Value::Int(i64::from_be_bytes(bytes_of(8)?.try_into().unwrap())),
        oid::FLOAT4 => Value::Float(f32::from_be_bytes(bytes_of(4)?.try_into().unwrap()).into()),
        oid::FLOAT8 => Value::Float(f64::from_be_bytes(bytes_of(8)?.try_into().unwrap())),
        oid::NUMERIC => Value::Float(numeric(bytes)?),
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::CHAR => {
            Value::String(utf8(bytes, bind.type_oid)?.to_owned())
        }
        oid::JSON => Value::String(json_as_jsonb(utf8(bytes, bind.type_oid)?)),
        oid::JSONB => {
            // Binary jsonb is a version byte followed by the text.
            let text = bytes
                .split_first()
                .filter(|(version, _)| **version == 1)
                .map(|(_, text)| text)
                .ok_or("a jsonb bind is not version 1")?;
            Value::String(json_as_jsonb(utf8(text, bind.type_oid)?))
        }
        oid::UUID => {
            let bytes = bytes_of(16)?;
            let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
            Value::String(format!(
                "{}-{}-{}-{}-{}",
                &hex[0..8],
                &hex[8..12],
                &hex[12..16],
                &hex[16..20],
                &hex[20..32]
            ))
        }
        oid::TIMESTAMP | oid::TIMESTAMPTZ => {
            // Microseconds since 2000-01-01; the engine keeps microseconds
            // since the Unix epoch. `infinity` is i64::MAX, `-infinity`
            // i64::MIN: neither fits the shift, and neither can be
            // compared on by a cache.
            let micros = i64::from_be_bytes(bytes_of(8)?.try_into().unwrap());
            if micros == i64::MAX || micros == i64::MIN {
                return Err("an infinite time bind".to_string());
            }
            Value::Int(micros.checked_add(PG_EPOCH_MICROS).ok_or("a time bind out of range")?)
        }
        oid::DATE => {
            let days = i32::from_be_bytes(bytes_of(4)?.try_into().unwrap());
            if days == i32::MAX || days == i32::MIN {
                return Err("an infinite date bind".to_string());
            }
            Value::Int(
                i64::from(days)
                    .checked_mul(86_400_000_000)
                    .and_then(|micros| micros.checked_add(PG_EPOCH_MICROS))
                    .ok_or("a date bind out of range")?,
            )
        }
        oid::BYTEA => return Err("a bytea bind cannot be compared on".to_string()),
        oid::BOOL_ARRAY
        | oid::INT2_ARRAY
        | oid::INT4_ARRAY
        | oid::INT8_ARRAY
        | oid::TEXT_ARRAY
        | oid::VARCHAR_ARRAY
        | oid::BPCHAR_ARRAY
        | oid::FLOAT4_ARRAY
        | oid::FLOAT8_ARRAY
        | oid::NUMERIC_ARRAY
        | oid::DATE_ARRAY
        | oid::TIMESTAMP_ARRAY
        | oid::TIMESTAMPTZ_ARRAY
        | oid::UUID_ARRAY
        | oid::JSONB_ARRAY => Value::List(array(bytes, element_of(bind.type_oid))?),
        other => return Err(format!("a bind of type oid {other} is not supported")),
    })
}

/// The element type of an array type. The array's own header names an
/// element type too, but that is what the client declared, which for
/// an enum remapped to text is the enum's OID; the outer type decides.
fn element_of(array_oid: u32) -> u32 {
    match array_oid {
        oid::BOOL_ARRAY => oid::BOOL,
        oid::INT2_ARRAY => oid::INT2,
        oid::INT4_ARRAY => oid::INT4,
        oid::INT8_ARRAY => oid::INT8,
        oid::TEXT_ARRAY => oid::TEXT,
        oid::VARCHAR_ARRAY => oid::VARCHAR,
        oid::BPCHAR_ARRAY => oid::BPCHAR,
        oid::FLOAT4_ARRAY => oid::FLOAT4,
        oid::FLOAT8_ARRAY => oid::FLOAT8,
        oid::NUMERIC_ARRAY => oid::NUMERIC,
        oid::DATE_ARRAY => oid::DATE,
        oid::TIMESTAMP_ARRAY => oid::TIMESTAMP,
        oid::TIMESTAMPTZ_ARRAY => oid::TIMESTAMPTZ,
        oid::UUID_ARRAY => oid::UUID,
        oid::JSONB_ARRAY => oid::JSONB,
        other => other,
    }
}

/// Postgres's binary array: dimensions, a has-nulls flag, the element
/// type, then per dimension a length and lower bound, then each element
/// as its length (`-1` for NULL) and bytes. Only one dimension is a list
/// a predicate can use. Elements are decoded as `element_oid`.
fn array(bytes: &[u8], element_oid: u32) -> Result<Vec<Value>, String> {
    let word = |at: usize| -> Result<i32, String> {
        bytes
            .get(at..at + 4)
            .map(|b| i32::from_be_bytes(b.try_into().unwrap()))
            .ok_or_else(|| "an array bind ends inside its header".to_string())
    };
    let dimensions = word(0)?;
    if dimensions == 0 {
        return Ok(Vec::new());
    }
    if dimensions != 1 {
        return Err(format!("an array bind has {dimensions} dimensions, not one"));
    }
    let length = usize::try_from(word(12)?).map_err(|_| "an array bind has a negative length")?;
    let mut at = 20;
    let mut items = Vec::with_capacity(length);
    for _ in 0..length {
        let size = word(at)?;
        at += 4;
        if size < 0 {
            items.push(Value::Null);
            continue;
        }
        let size = size as usize;
        let element = bytes
            .get(at..at + size)
            .ok_or_else(|| "an array bind ends inside an element".to_string())?;
        items.push(decode(&BindValue {
            type_oid: element_oid,
            bytes: Some(element.to_vec()),
        })?);
        at += size;
    }
    Ok(items)
}

fn utf8(bytes: &[u8], type_oid: u32) -> Result<&str, String> {
    std::str::from_utf8(bytes).map_err(|_| format!("a bind of type oid {type_oid} is not UTF-8"))
}

/// Postgres's binary `numeric`: a header of base-10000 digit count,
/// weight, sign and scale, then the digits.
fn numeric(bytes: &[u8]) -> Result<f64, String> {
    if bytes.len() < 8 {
        return Err("a numeric bind is shorter than its header".to_string());
    }
    let word = |at: usize| u16::from_be_bytes([bytes[at], bytes[at + 1]]);
    let ndigits = usize::from(word(0));
    let weight = i32::from(word(2) as i16);
    let sign = word(4);
    let scale = usize::from(word(6));
    if bytes.len() != 8 + 2 * ndigits {
        return Err("a numeric bind's digits do not match its header".to_string());
    }
    let negative = match sign {
        0x0000 => false,
        0x4000 => true,
        0xC000 => return Ok(f64::NAN),
        _ => return Err("a numeric bind has an unknown sign".to_string()),
    };
    // Render the digits as text and let the float parser do the rounding,
    // which keeps every representable value exact. `weight` is the power
    // of 10000 of the first group: zero or more means that many integer
    // groups follow it, negative means every group is fractional and
    // `-weight - 1` empty groups precede the first.
    let group = |index: usize| word(8 + 2 * index);
    let mut text = String::new();
    if ndigits == 0 {
        text.push('0');
    } else if weight < 0 {
        text.push_str("0.");
        text.push_str(&"0000".repeat((-weight - 1) as usize));
        for index in 0..ndigits {
            text.push_str(&format!("{:04}", group(index)));
        }
    } else {
        let integer_groups = weight as usize + 1;
        for index in 0..ndigits {
            if index == integer_groups {
                text.push('.');
            }
            if index == 0 {
                text.push_str(&group(index).to_string());
            } else {
                text.push_str(&format!("{:04}", group(index)));
            }
        }
        if ndigits < integer_groups {
            text.push_str(&"0000".repeat(integer_groups - ndigits));
        }
    }
    let _ = scale;
    let value: f64 = text
        .parse()
        .map_err(|_| format!("a numeric bind rendered as `{text}` is not a number"))?;
    Ok(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bind(type_oid: u32, bytes: &[u8]) -> BindValue {
        BindValue {
            type_oid,
            bytes: Some(bytes.to_vec()),
        }
    }

    #[test]
    fn integers_floats_and_bools() {
        assert_eq!(decode(&bind(oid::INT2, &(-7i16).to_be_bytes())), Ok(Value::Int(-7)));
        assert_eq!(decode(&bind(oid::INT4, &42i32.to_be_bytes())), Ok(Value::Int(42)));
        assert_eq!(
            decode(&bind(oid::INT8, &(1i64 << 40).to_be_bytes())),
            Ok(Value::Int(1 << 40))
        );
        assert_eq!(decode(&bind(oid::FLOAT8, &2.5f64.to_be_bytes())), Ok(Value::Float(2.5)));
        assert_eq!(decode(&bind(oid::FLOAT4, &0.5f32.to_be_bytes())), Ok(Value::Float(0.5)));
        assert_eq!(decode(&bind(oid::BOOL, &[1])), Ok(Value::Bool(true)));
        assert_eq!(decode(&bind(oid::BOOL, &[0])), Ok(Value::Bool(false)));
    }

    #[test]
    fn text_like_types_and_uuids() {
        assert_eq!(
            decode(&bind(oid::TEXT, b"OPEN")),
            Ok(Value::String("OPEN".into()))
        );
        assert_eq!(
            decode(&bind(oid::VARCHAR, "m\u{e9}nu".as_bytes())),
            Ok(Value::String("m\u{e9}nu".into()))
        );
        let uuid = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88,
        ];
        assert_eq!(
            decode(&bind(oid::UUID, &uuid)),
            Ok(Value::String("12345678-9abc-def0-1122-334455667788".into()))
        );
        assert!(decode(&bind(oid::TEXT, &[0xff])).is_err());
    }

    #[test]
    fn json_is_normalised_like_a_jsonb_cell() {
        assert_eq!(
            decode(&bind(oid::JSON, br#"{"b": 1, "a": [true]}"#)),
            Ok(Value::String(json_as_jsonb(r#"{"b": 1, "a": [true]}"#)))
        );
        let mut jsonb = vec![1u8];
        jsonb.extend_from_slice(br#"{"k":"v"}"#);
        assert_eq!(
            decode(&bind(oid::JSONB, &jsonb)),
            Ok(Value::String(json_as_jsonb(r#"{"k":"v"}"#)))
        );
        assert!(decode(&bind(oid::JSONB, b"{}")).is_err(), "no version byte");
    }

    #[test]
    fn times_become_unix_microseconds() {
        // 2000-01-01 00:00:01.5 as Postgres micros.
        let micros = 1_500_000i64;
        assert_eq!(
            decode(&bind(oid::TIMESTAMPTZ, &micros.to_be_bytes())),
            Ok(Value::Int(PG_EPOCH_MICROS + 1_500_000))
        );
        assert_eq!(
            decode(&bind(oid::TIMESTAMP, &(-1_000i64).to_be_bytes())),
            Ok(Value::Int(PG_EPOCH_MICROS - 1_000))
        );
        assert_eq!(
            decode(&bind(oid::DATE, &2i32.to_be_bytes())),
            Ok(Value::Int(PG_EPOCH_MICROS + 2 * 86_400_000_000))
        );
    }

    #[test]
    fn numerics_render_through_their_digit_groups() {
        // 12345.678: digits [1, 2345, 6780], weight 1, positive, scale 3.
        let mut bytes = Vec::new();
        for word in [3u16, 1, 0, 3, 1, 2345, 6780] {
            bytes.extend_from_slice(&word.to_be_bytes());
        }
        assert_eq!(decode(&bind(oid::NUMERIC, &bytes)), Ok(Value::Float(12345.678)));
        // -0.0042: digits [42], weight -1, negative, scale 4.
        let mut bytes = Vec::new();
        for word in [1u16, 0xffff, 0x4000, 4, 42] {
            bytes.extend_from_slice(&word.to_be_bytes());
        }
        assert_eq!(decode(&bind(oid::NUMERIC, &bytes)), Ok(Value::Float(-0.0042)));
        // 0: no digits.
        let mut bytes = Vec::new();
        for word in [0u16, 0, 0, 0] {
            bytes.extend_from_slice(&word.to_be_bytes());
        }
        assert_eq!(decode(&bind(oid::NUMERIC, &bytes)), Ok(Value::Float(0.0)));
        // 70000: digits [7], weight 1.
        let mut bytes = Vec::new();
        for word in [1u16, 1, 0, 0, 7] {
            bytes.extend_from_slice(&word.to_be_bytes());
        }
        assert_eq!(decode(&bind(oid::NUMERIC, &bytes)), Ok(Value::Float(70000.0)));
    }

    #[test]
    fn arrays_become_lists() {
        // text[] of ["OPEN", NULL, "DONE"]
        let mut bytes = Vec::new();
        for word in [1i32, 1, 25, 3, 1] {
            bytes.extend_from_slice(&word.to_be_bytes());
        }
        bytes.extend_from_slice(&4i32.to_be_bytes());
        bytes.extend_from_slice(b"OPEN");
        bytes.extend_from_slice(&(-1i32).to_be_bytes());
        bytes.extend_from_slice(&4i32.to_be_bytes());
        bytes.extend_from_slice(b"DONE");
        assert_eq!(
            decode(&bind(oid::TEXT_ARRAY, &bytes)),
            Ok(Value::List(vec![
                Value::String("OPEN".into()),
                Value::Null,
                Value::String("DONE".into())
            ]))
        );
        // int8[] of [7]
        let mut bytes = Vec::new();
        for word in [1i32, 0, 20, 1, 1, 8] {
            bytes.extend_from_slice(&word.to_be_bytes());
        }
        bytes.extend_from_slice(&7i64.to_be_bytes());
        assert_eq!(decode(&bind(oid::INT8_ARRAY, &bytes)), Ok(Value::List(vec![Value::Int(7)])));
        // an empty array: zero dimensions
        let mut bytes = Vec::new();
        for word in [0i32, 0, 25] {
            bytes.extend_from_slice(&word.to_be_bytes());
        }
        assert_eq!(decode(&bind(oid::TEXT_ARRAY, &bytes)), Ok(Value::List(vec![])));
        assert!(decode(&bind(oid::TEXT_ARRAY, &[0, 0, 0, 2])).is_err());
    }

    #[test]
    fn nulls_and_unknown_types() {
        assert_eq!(
            decode(&BindValue {
                type_oid: oid::TEXT,
                bytes: None
            }),
            Ok(Value::Null)
        );
        assert!(decode(&bind(600, b"x")).is_err(), "point");
        assert!(decode(&bind(oid::BYTEA, b"x")).is_err());
        assert!(decode(&bind(oid::INT4, &[1, 2])).is_err(), "wrong width");
    }
}
