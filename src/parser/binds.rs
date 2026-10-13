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
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;
}

/// Milliseconds from the Unix epoch to Postgres's epoch (2000-01-01).
const PG_EPOCH_MILLIS: i64 = 946_684_800_000;

/// The value a bind stands for, or why it cannot stand for one.
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
            // Microseconds since 2000-01-01; the engine keeps milliseconds
            // since the Unix epoch.
            let micros = i64::from_be_bytes(bytes_of(8)?.try_into().unwrap());
            Value::Int(micros.div_euclid(1000) + PG_EPOCH_MILLIS)
        }
        oid::DATE => {
            let days = i32::from_be_bytes(bytes_of(4)?.try_into().unwrap());
            Value::Int(i64::from(days) * 86_400_000 + PG_EPOCH_MILLIS)
        }
        oid::BYTEA => return Err("a bytea bind cannot be compared on".to_string()),
        other => return Err(format!("a bind of type oid {other} is not supported")),
    })
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
    fn times_become_unix_milliseconds() {
        // 2000-01-01 00:00:01.5 as Postgres micros.
        let micros = 1_500_000i64;
        assert_eq!(
            decode(&bind(oid::TIMESTAMPTZ, &micros.to_be_bytes())),
            Ok(Value::Int(PG_EPOCH_MILLIS + 1500))
        );
        assert_eq!(
            decode(&bind(oid::TIMESTAMP, &(-1_000i64).to_be_bytes())),
            Ok(Value::Int(PG_EPOCH_MILLIS - 1))
        );
        assert_eq!(
            decode(&bind(oid::DATE, &2i32.to_be_bytes())),
            Ok(Value::Int(PG_EPOCH_MILLIS + 2 * 86_400_000))
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
