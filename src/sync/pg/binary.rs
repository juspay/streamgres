//! Postgres's binary format: the bytes `*_send` writes for a value, as
//! the change feed and binary reads deliver them, turned into exact
//! [`Value`]s, and values written back the same way.
//!
//! A type the engine interprets decodes to its own variant; any other
//! type decodes to [`Value::Raw`], its bytes kept as they came. Encoding
//! a decoded source value with the same type gives back the value Postgres
//! sent. JSON keeps its source spelling alongside its standard comparison
//! form. Arrays with unusual dimensions or lower bounds stay raw so their
//! original shape is preserved.

use std::fmt;

use bytes::Bytes;

use super::text;
use crate::model::{Day, JsonText, Numeric, Raw, Time, Timestamp, Value, ValueType};
use crate::sync::CellDecoder;

/// Type OIDs the engine interprets.
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

/// Postgres's binary format as a [`CellDecoder`]: a cell is its type's
/// OID and the bytes sent for it.
#[derive(Debug, Clone, Copy, Default)]
pub struct PgBinary;

/// One cell in Postgres's binary format.
#[derive(Debug, Clone, Copy)]
pub struct PgCell<'a> {
    pub type_oid: u32,
    pub bytes: &'a [u8],
}

impl CellDecoder for PgBinary {
    type Cell<'a> = PgCell<'a>;
    type Error = CodecError;

    fn decode(&self, declared: &ValueType, cell: PgCell<'_>) -> Result<Value, CodecError> {
        decode(declared, cell.type_oid, cell.bytes, 2)
    }
}

/// The element type an array parameter of `type_oid` holds: the built-in
/// array's, or, for an array type the engine has no table of (an enum's,
/// a domain's), the one its binary form names in its header, which
/// Postgres holds to the array's own. `None` when neither says.
pub fn bound_element(type_oid: u32, bytes: Option<&[u8]>) -> Option<u32> {
    array_element(type_oid).or_else(|| {
        let header = bytes?.get(8..12)?;
        Some(u32::from_be_bytes(header.try_into().ok()?))
    })
}

/// The element type of a built-in array type: what an array column's
/// values are written as when the catalog has not said (a column a schema
/// change added). `None` for any other type.
pub fn array_element(type_oid: u32) -> Option<u32> {
    Some(match type_oid {
        1000 => oid::BOOL,
        1001 => oid::BYTEA,
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
        1115 => oid::TIMESTAMP,
        1182 => oid::DATE,
        1183 => oid::TIME,
        1185 => oid::TIMESTAMPTZ,
        1231 => oid::NUMERIC,
        199 => oid::JSON,
        2951 => oid::UUID,
        3807 => oid::JSONB,
        1028 => oid::OID,
        _ => return None,
    })
}

/// The engine's type for a column of `type_oid`, when the engine
/// interprets it; `None` for a type kept as [`Value::Raw`] (arrays
/// included: their element type comes from the catalog).
pub fn value_type(type_oid: u32) -> Option<ValueType> {
    Some(match type_oid {
        oid::BOOL => ValueType::Bool,
        oid::INT2 | oid::INT4 | oid::INT8 | oid::OID => ValueType::Int,
        oid::FLOAT4 | oid::FLOAT8 => ValueType::Float,
        oid::NUMERIC => ValueType::Numeric,
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::CHAR | oid::UUID => {
            ValueType::String
        }
        oid::JSON | oid::JSONB => ValueType::Json,
        oid::TIMESTAMP | oid::TIMESTAMPTZ => ValueType::Timestamp,
        oid::DATE => ValueType::Day,
        oid::TIME => ValueType::Time,
        oid::BYTEA => ValueType::Bytes,
        _ => return None,
    })
}

/// Decodes one non-null cell of a column the catalog declares as
/// `declared`, sent as type `type_oid`. A column declared as text whose
/// type is not a known text type (an enum, a domain over text) reads its
/// bytes as UTF-8, which is what those types send. A list reads
/// Postgres's array format.
pub fn decode(declared: &ValueType, type_oid: u32, bytes: &[u8], money_scale: u8) -> Result<Value, CodecError> {
    let fail = |reason| CodecError { type_oid, reason };
    match declared {
        ValueType::List(inner) => decode_array(inner, type_oid, bytes, false, money_scale),
        ValueType::Date => match decode_scalar(type_oid, bytes, money_scale)? {
            Value::Day(day) => day
                .midnight()
                .and_then(naive)
                .map(|at| Value::Date(at.date()))
                .ok_or(fail("a date out of range")),
            _ => Err(fail("not a date")),
        },
        ValueType::Datetime => match decode_scalar(type_oid, bytes, money_scale)? {
            Value::Timestamp(at) => naive(at)
                .map(Value::Datetime)
                .ok_or(fail("a time out of range")),
            _ => Err(fail("not a timestamp")),
        },
        ValueType::Raw => Ok(raw(type_oid, bytes, money_scale)),
        ValueType::String if value_type(type_oid).is_none() => utf8(bytes)
            .map(|text| Value::String(text.to_owned()))
            .map_err(|_| fail("not UTF-8")),
        _ => decode_scalar(type_oid, bytes, money_scale),
    }
}

/// Cells from PostgreSQL itself have canonical jsonb text already. Keep
/// this shortcut private to the snapshot and replication decoders: client
/// parameters must go through the validating, canonicalizing decoder.
pub(crate) fn decode_source(
    declared: &ValueType,
    type_oid: u32,
    bytes: &[u8],
    money_scale: u8,
) -> Result<Value, CodecError> {
    if let ValueType::List(inner) = declared {
        return decode_array(inner, type_oid, bytes, true, money_scale);
    }
    if type_oid == oid::JSONB && *declared == ValueType::Json {
        let fail = |reason| CodecError { type_oid, reason };
        let (version, body) = bytes.split_first().ok_or(fail("empty"))?;
        if *version != JSONB_VERSION {
            return Err(fail("unknown jsonb version"));
        }
        let body = utf8(body).map_err(|_| fail("not UTF-8"))?;
        return Ok(Value::Json(JsonText::new(
            text::standard_json(body).into_owned(),
            body,
        )));
    }
    decode(declared, type_oid, bytes, money_scale)
}

/// Decodes one non-null cell by its type alone. JSONB input is validated
/// and canonicalized, since client bind bytes need not be jsonb_send output.
pub fn decode_scalar(type_oid: u32, bytes: &[u8], money_scale: u8) -> Result<Value, CodecError> {
    let fail = |reason| CodecError { type_oid, reason };
    Ok(match type_oid {
        oid::BOOL => Value::Bool(fixed::<1>(bytes, type_oid)? != [0]),
        oid::INT2 => Value::Int(i64::from(i16::from_be_bytes(fixed::<2>(bytes, type_oid)?))),
        oid::INT4 => Value::Int(i64::from(i32::from_be_bytes(fixed::<4>(bytes, type_oid)?))),
        oid::INT8 => Value::Int(i64::from_be_bytes(fixed::<8>(bytes, type_oid)?)),
        oid::OID => Value::Int(i64::from(u32::from_be_bytes(fixed::<4>(bytes, type_oid)?))),
        oid::FLOAT4 => Value::Float(f64::from(f32::from_be_bytes(fixed::<4>(bytes, type_oid)?))),
        oid::FLOAT8 => Value::Float(f64::from_be_bytes(fixed::<8>(bytes, type_oid)?)),
        oid::NUMERIC => Value::Numeric(Numeric::from_binary(bytes).map_err(|error| fail(error.0))?),
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::CHAR => {
            Value::String(utf8(bytes).map_err(|_| fail("not UTF-8"))?.to_owned())
        }
        oid::UUID => Value::String(uuid_text(&fixed::<16>(bytes, type_oid)?)),
        oid::JSON => {
            let stored = utf8(bytes).map_err(|_| fail("not UTF-8"))?;
            Value::Json(JsonText::new(text::json_as_jsonb(stored), stored))
        }
        oid::JSONB => {
            let (version, body) = bytes.split_first().ok_or(fail("empty"))?;
            if *version != JSONB_VERSION {
                return Err(fail("unknown jsonb version"));
            }
            let body = utf8(body).map_err(|_| fail("not UTF-8"))?;
            // Bind bytes are JSON input, not necessarily jsonb_send output.
            // Canonicalize keys, whitespace and exact decimal numbers alike.
            let standard = text::jsonb_input(body).map_err(fail)?;
            Value::Json(JsonText::new(standard, body))
        }
        oid::TIMESTAMP | oid::TIMESTAMPTZ => {
            Value::Timestamp(Timestamp(i64::from_be_bytes(fixed::<8>(bytes, type_oid)?)))
        }
        oid::DATE => Value::Day(Day(i32::from_be_bytes(fixed::<4>(bytes, type_oid)?))),
        oid::TIME => Value::Time(Time(i64::from_be_bytes(fixed::<8>(bytes, type_oid)?))),
        oid::BYTEA => Value::Bytes(Bytes::copy_from_slice(bytes)),
        _ => raw(type_oid, bytes, money_scale),
    })
}

/// A timestamp as a calendar date and time in UTC.
fn naive(at: Timestamp) -> Option<chrono::NaiveDateTime> {
    chrono::DateTime::from_timestamp_micros(at.unix_micros()?).map(|at| at.naive_utc())
}

/// A value kept as its bytes, with the client text pre-rendered when
/// `client_scale` is supplied (the source instance's money scale).
fn raw(type_oid: u32, bytes: &[u8], money_scale: u8) -> Value {
    let bytes = Bytes::copy_from_slice(bytes);
    let text = client_text(type_oid, bytes.as_ref(), money_scale);
    match text {
        Some(text) => Value::Raw(Raw::with_text(type_oid, bytes, text)),
        None => Value::Raw(Raw::new(type_oid, bytes)),
    }
}

/// The client-side text for a raw value, rendered at ingress so the wire
/// never dispatches on type OIDs. `money_scale` is the source instance's
/// `lc_monetary` scale.
fn client_text(type_oid: u32, bytes: &[u8], money_scale: u8) -> Option<String> {
    crate::client::pg_text::raw_text(type_oid, bytes, money_scale)
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
    let (pairs, _) = hex.as_chunks::<2>();
    for (slot, pair) in out.iter_mut().zip(pairs) {
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

/// Decodes Postgres's array format. A one-dimensional array becomes a
/// list of its elements decoded as `inner`; an empty array an empty list;
/// a multidimensional array becomes nested lists (last dimension
/// innermost). Lower bounds are not kept — the client projection shows
/// contents, not index bounds, matching `to_json`.
fn decode_array(
    inner: &ValueType,
    type_oid: u32,
    bytes: &[u8],
    source: bool,
    money_scale: u8,
) -> Result<Value, CodecError> {
    let fail = |reason| CodecError { type_oid, reason };
    let mut at = 0;
    let dimensions = read_i32(bytes, &mut at, type_oid)?;
    let _has_nulls = read_i32(bytes, &mut at, type_oid)?;
    let element_oid = u32::from_be_bytes(fixed::<4>(
        bytes.get(at..at + 4).ok_or(fail("truncated array"))?,
        type_oid,
    )?);
    at += 4;
    if dimensions <= 0 {
        return Ok(Value::List(Vec::new()));
    }
    let mut dims = Vec::with_capacity(dimensions as usize);
    let mut total = 1usize;
    for _ in 0..dimensions {
        let length = usize::try_from(read_i32(bytes, &mut at, type_oid)?)
            .map_err(|_| fail("negative array dimension"))?;
        let _lower_bound = read_i32(bytes, &mut at, type_oid)?;
        total = total.saturating_mul(length);
        dims.push(length);
    }
    let mut items = Vec::with_capacity(total.min(bytes.len() / 4));
    let mut json_bytes = 0usize;
    for _ in 0..total {
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
        let value = if source {
            decode_source(inner, element_oid, element, money_scale)?
        } else {
            decode(inner, element_oid, element, money_scale)?
        };
        if !source {
            json_bytes = json_bytes.saturating_add(parameter_json_bytes(&value));
            if json_bytes > 1 << 20 {
                return Err(fail("JSON array exceeds the evaluation budget"));
            }
        }
        items.push(value);
    }
    if at != bytes.len() {
        return Err(fail("trailing bytes after array"));
    }
    Ok(Value::List(nest(&mut items.into_iter(), &dims)))
}

/// Groups flat `items` into nested lists by `dims` (outermost first). A
/// single dimension stays flat. Postgres writes the last dimension
/// fastest.
fn nest(items: &mut std::vec::IntoIter<Value>, dims: &[usize]) -> Vec<Value> {
    let Some(&count) = dims.first() else {
        return Vec::new();
    };
    if dims.len() == 1 {
        return items.by_ref().take(count).collect();
    }
    (0..count)
        .map(|_| Value::List(nest(items, &dims[1..])))
        .collect()
}

/// Expanded JSON in a client parameter, beyond its compact wire input.
pub(crate) fn parameter_json_bytes(value: &Value) -> usize {
    match value {
        Value::Json(json) => json.standard().len(),
        Value::List(items) => items.iter().fold(0usize, |size, item| {
            size.saturating_add(parameter_json_bytes(item))
        }),
        _ => 0,
    }
}

/// Encodes `value` as Postgres's binary form for `type_oid`; `None` when
/// the value is not one of that type. `Null` has no bytes and is `None`.
pub fn encode(value: &Value, type_oid: u32) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    encode_into(value, type_oid, &mut out).then_some(out)
}

/// Appends `value` in the binary format of `type_oid` to `out`; whether
/// it could be, `out` as it was when not (or for `NULL`, which has no
/// bytes).
pub fn encode_into(value: &Value, type_oid: u32, out: &mut Vec<u8>) -> bool {
    let at = out.len();
    let written = match (value, type_oid) {
        (Value::Null, _) => false,
        (Value::Raw(raw), _) if raw.type_oid == type_oid => {
            out.extend_from_slice(&raw.bytes);
            true
        }
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
        (Value::Numeric(number), oid::NUMERIC) => {
            out.extend_from_slice(&number.to_binary());
            true
        }
        (Value::String(text), oid::UUID) => match uuid_bytes(text) {
            Some(bytes) => {
                out.extend_from_slice(&bytes);
                true
            }
            None => false,
        },
        (Value::Json(json), oid::JSONB) => {
            out.push(JSONB_VERSION);
            out.extend_from_slice(json.text().as_bytes());
            true
        }
        (Value::Json(json), oid::JSON) => {
            out.extend_from_slice(json.text().as_bytes());
            true
        }
        (Value::String(text), oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::CHAR) => {
            out.extend_from_slice(text.as_bytes());
            true
        }
        (Value::String(text), other) if value_type(other).is_none() => {
            out.extend_from_slice(text.as_bytes());
            true
        }
        (Value::Timestamp(at), oid::TIMESTAMP | oid::TIMESTAMPTZ) => {
            out.extend_from_slice(&at.0.to_be_bytes());
            true
        }
        (Value::Day(day), oid::DATE) => {
            out.extend_from_slice(&day.0.to_be_bytes());
            true
        }
        (Value::Time(time), oid::TIME) => {
            out.extend_from_slice(&time.0.to_be_bytes());
            true
        }
        (Value::Bytes(bytes), oid::BYTEA) => {
            out.extend_from_slice(bytes);
            true
        }
        _ => false,
    };
    if !written {
        out.truncate(at);
    }
    written
}

/// Encodes a list as a one-dimensional array of `element_oid` for the
/// array type `type_oid`; `None` when an element is not of that type.
pub fn encode_array(items: &[Value], element_oid: u32) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    encode_array_into(items, element_oid, &mut out).then_some(out)
}

/// Appends a list as a one-dimensional array of `element_oid` to `out`;
/// whether it could be, `out` as it was when not.
pub fn encode_array_into(items: &[Value], element_oid: u32, out: &mut Vec<u8>) -> bool {
    let at = out.len();
    let written = write_array(items, element_oid, out).is_some();
    if !written {
        out.truncate(at);
    }
    written
}

fn write_array(items: &[Value], element_oid: u32, out: &mut Vec<u8>) -> Option<()> {
    let has_nulls = items.iter().any(Value::is_null);
    let dimensions: i32 = if items.is_empty() { 0 } else { 1 };
    out.extend_from_slice(&dimensions.to_be_bytes());
    out.extend_from_slice(&i32::from(has_nulls).to_be_bytes());
    out.extend_from_slice(&element_oid.to_be_bytes());
    if items.is_empty() {
        return Some(());
    }
    out.extend_from_slice(&i32::try_from(items.len()).ok()?.to_be_bytes());
    out.extend_from_slice(&1i32.to_be_bytes());
    for item in items {
        if item.is_null() {
            out.extend_from_slice(&(-1i32).to_be_bytes());
            continue;
        }
        let length_at = out.len();
        out.extend_from_slice(&0i32.to_be_bytes());
        if !encode_into(item, element_oid, out) {
            return None;
        }
        let length = i32::try_from(out.len() - length_at - 4).ok()?;
        out.get_mut(length_at..length_at + 4)?
            .copy_from_slice(&length.to_be_bytes());
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(declared: &ValueType, type_oid: u32, bytes: &[u8]) -> Result<Value, CodecError> {
        super::decode(declared, type_oid, bytes, 2)
    }

    fn decode_scalar(type_oid: u32, bytes: &[u8]) -> Result<Value, CodecError> {
        super::decode_scalar(type_oid, bytes, 2)
    }

    fn round_trip(type_oid: u32, bytes: &[u8]) -> Value {
        let declared = value_type(type_oid).unwrap_or(ValueType::Raw);
        let value = decode(&declared, type_oid, bytes).unwrap();
        assert_eq!(
            encode(&value, type_oid).as_deref(),
            Some(bytes),
            "{value:?}"
        );
        value
    }

    #[test]
    fn scalars_round_trip_byte_for_byte() {
        assert_eq!(round_trip(oid::BOOL, &[1]), Value::Bool(true));
        assert_eq!(
            round_trip(oid::INT2, &(-2i16).to_be_bytes()),
            Value::Int(-2)
        );
        assert_eq!(round_trip(oid::INT4, &7i32.to_be_bytes()), Value::Int(7));
        assert_eq!(
            round_trip(oid::INT8, &i64::MIN.to_be_bytes()),
            Value::Int(i64::MIN)
        );
        assert_eq!(
            round_trip(oid::OID, &u32::MAX.to_be_bytes()),
            Value::Int(4_294_967_295)
        );
        assert_eq!(
            round_trip(oid::FLOAT4, &1.5f32.to_be_bytes()),
            Value::Float(1.5)
        );
        assert_eq!(
            round_trip(oid::FLOAT8, &0.1f64.to_be_bytes()),
            Value::Float(0.1)
        );
        assert_eq!(
            round_trip(oid::TEXT, "héllo".as_bytes()),
            Value::from("héllo")
        );
        assert_eq!(
            round_trip(oid::TIMESTAMPTZ, &i64::MAX.to_be_bytes()),
            Value::Timestamp(Timestamp::INFINITY)
        );
        assert_eq!(
            round_trip(oid::DATE, &(-1i32).to_be_bytes()),
            Value::Day(Day(-1))
        );
        assert_eq!(
            round_trip(oid::TIME, &1_500_000i64.to_be_bytes()),
            Value::Time(Time(1_500_000))
        );
        assert_eq!(
            round_trip(oid::BYTEA, &[0, 255, 7]),
            Value::Bytes(Bytes::from_static(&[0, 255, 7]))
        );
        let numeric = [0, 2, 0, 0, 0, 0, 0, 2, 0, 1, 0x13, 0x88];
        assert!(matches!(
            round_trip(oid::NUMERIC, &numeric),
            Value::Numeric(number) if number.to_string() == "1.50"
        ));
    }

    #[test]
    fn uuids_read_as_postgres_writes_them() {
        let bytes: Vec<u8> = (0u8..16).collect();
        assert_eq!(
            round_trip(oid::UUID, &bytes),
            Value::from("00010203-0405-0607-0809-0a0b0c0d0e0f")
        );
        assert_eq!(
            encode(
                &Value::from("00010203-0405-0607-0809-0A0B0C0D0E0F"),
                oid::UUID
            ),
            Some(bytes)
        );
        assert_eq!(encode(&Value::from("not-a-uuid"), oid::UUID), None);
    }

    #[test]
    fn json_is_kept_in_the_standard_form() {
        let jsonb = [&[JSONB_VERSION][..], b"{\"a\": 1.50}"].concat();
        assert_eq!(
            decode_scalar(oid::JSONB, &jsonb).unwrap(),
            Value::Json(crate::model::JsonText::standard_only("{\"a\": 1.5}".into()))
        );
        assert_eq!(
            decode_scalar(oid::JSON, b"{ \"b\":2, \"a\":1 }").unwrap(),
            Value::Json(crate::model::JsonText::standard_only(
                "{\"a\": 1, \"b\": 2}".into()
            ))
        );
        assert!(decode_scalar(oid::JSONB, &[2, b'1']).is_err());
    }

    #[test]
    fn jsonb_input_is_canonical_without_rounding_decimal_digits() {
        for (input, expected) in [
            (r#"{"aa":1,"b":2}"#, r#"{"b": 2, "aa": 1}"#),
            (
                r#"{"x":1e2,"x":9007199254740993.00}"#,
                r#"{"x": 9007199254740993}"#,
            ),
            (
                r#"[0.100000000000000001, -0.0, 1e-10]"#,
                "[0.100000000000000001, 0, 0.0000000001]",
            ),
        ] {
            let bytes = [&[1][..], input.as_bytes()].concat();
            let value = decode_scalar(oid::JSONB, &bytes).unwrap();
            assert_eq!(
                value,
                Value::Json(JsonText::standard_only(expected.to_owned()))
            );
            assert_eq!(encode(&value, oid::JSONB).unwrap(), bytes);
        }
        assert!(decode_scalar(oid::JSONB, b"\x01{broken").is_err());
        assert!(decode_scalar(oid::JSONB, b"\x01\"\\u0000\"").is_err());
        assert!(decode_scalar(oid::JSONB, b"\x011e1000000").is_err());
    }

    #[test]
    fn compact_json_exponents_cannot_expand_without_a_bound() {
        let text = format!("[{}]", vec!["1e100000"; 20].join(","));
        let bytes = [&[1][..], text.as_bytes()].concat();
        assert!(decode_scalar(oid::JSONB, &bytes).is_err());
        let value = Value::Json(JsonText::new("1".into(), "1e100000"));
        let bytes = encode_array(&vec![value; 20], oid::JSONB).unwrap();
        assert!(decode(&ValueType::List(Box::new(ValueType::Json)), 3807, &bytes).is_err());
    }

    #[test]
    fn unusual_array_bounds_expose_their_contents() {
        let mut bytes = encode_array(&[Value::Int(7), Value::Int(8)], oid::INT4).unwrap();
        for bound in [0i32, -3, 42] {
            bytes[16..20].copy_from_slice(&bound.to_be_bytes());
            let value = decode(&ValueType::List(Box::new(ValueType::Int)), 1007, &bytes).unwrap();
            assert_eq!(value, Value::List(vec![Value::Int(7), Value::Int(8)]));
        }
    }

    #[test]
    fn a_multidimensional_array_becomes_nested_lists() {
        // A 2×2 int4 array: dims (2,2), lower bounds (1,1), elements
        // 1 2 / 3 4 — Postgres writes the last dimension fastest.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2i32.to_be_bytes()); // dimensions
        bytes.extend_from_slice(&0i32.to_be_bytes()); // no nulls
        bytes.extend_from_slice(&oid::INT4.to_be_bytes());
        bytes.extend_from_slice(&2i32.to_be_bytes()); // dim 1
        bytes.extend_from_slice(&1i32.to_be_bytes());
        bytes.extend_from_slice(&2i32.to_be_bytes()); // dim 2
        bytes.extend_from_slice(&1i32.to_be_bytes());
        for n in [1i32, 2, 3, 4] {
            bytes.extend_from_slice(&4i32.to_be_bytes());
            bytes.extend_from_slice(&n.to_be_bytes());
        }
        let value = decode(&ValueType::List(Box::new(ValueType::Int)), 1007, &bytes).unwrap();
        assert_eq!(
            value,
            Value::List(vec![
                Value::List(vec![Value::Int(1), Value::Int(2)]),
                Value::List(vec![Value::Int(3), Value::Int(4)]),
            ])
        );
    }

    #[test]
    fn unknown_types_stay_raw() {
        let interval = [0u8; 16];
        assert_eq!(
            round_trip(1186, &interval),
            Value::Raw(Raw::new(1186, Bytes::copy_from_slice(&interval)))
        );
        assert_eq!(
            decode(&ValueType::String, 99_999, b"open").unwrap(),
            Value::from("open")
        );
    }

    #[test]
    fn arrays_round_trip() {
        let items = vec![Value::Int(1), Value::Null, Value::Int(3)];
        let bytes = encode_array(&items, oid::INT4).unwrap();
        let declared = ValueType::List(Box::new(ValueType::Int));
        assert_eq!(decode(&declared, 1007, &bytes).unwrap(), Value::List(items));
        let empty = encode_array(&[], oid::TEXT).unwrap();
        assert_eq!(
            decode(&ValueType::List(Box::new(ValueType::String)), 1009, &empty).unwrap(),
            Value::List(Vec::new())
        );
    }

    #[test]
    fn decodes_through_the_source_neutral_trait() {
        let cell = PgCell {
            type_oid: oid::INT4,
            bytes: &5i32.to_be_bytes(),
        };
        assert_eq!(
            PgBinary.decode(&ValueType::Int, cell).unwrap(),
            Value::Int(5)
        );
    }

    #[test]
    fn built_in_arrays_name_their_element() {
        assert_eq!(array_element(1007), Some(oid::INT4));
        assert_eq!(array_element(1009), Some(oid::TEXT));
        assert_eq!(array_element(oid::INT4), None);
    }

    #[test]
    fn malformed_bytes_are_errors_not_panics() {
        assert!(decode_scalar(oid::INT4, &[0, 1]).is_err());
        assert!(decode_scalar(oid::TEXT, &[0xff, 0xfe]).is_err());
        assert!(decode_scalar(oid::NUMERIC, &[0, 1]).is_err());
        let declared = ValueType::List(Box::new(ValueType::Int));
        for cut in 0..20 {
            let bytes = encode_array(&[Value::Int(1)], oid::INT4).unwrap();
            if cut < bytes.len() {
                assert!(decode(&declared, 1007, &bytes[..cut]).is_err(), "cut {cut}");
            }
        }
    }

    #[test]
    fn encoding_refuses_values_of_another_type() {
        assert_eq!(encode(&Value::Int(70_000), oid::INT2), None);
        assert_eq!(encode(&Value::Float(0.1), oid::FLOAT4), None);
        assert_eq!(encode(&Value::Bool(true), oid::INT4), None);
        assert_eq!(encode(&Value::Null, oid::INT4), None);
    }
}
