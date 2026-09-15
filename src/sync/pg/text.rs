//! The text forms Postgres uses for the values the engine carries in a
//! form of its own: the epoch-millisecond reading of a time, a JSON array
//! read from `to_json(column)`, and the `{a,b}` array literal the change
//! feed delivers, each turned into the [`Value`] the column's declared
//! type calls for.

use crate::model::{Value, ValueType};

/// Milliseconds since the Unix epoch of a Postgres `timestamp`,
/// `timestamptz` or `date` in its text form (`2026-09-15 10:00:00.123`,
/// with an optional `+05:30` style offset, or `2026-09-15`); a timestamp
/// without a zone is read as UTC, the client's convention. `None` when the text
/// is not a time (`infinity` included).
pub fn epoch_millis(text: &str) -> Option<i64> {
    let text = text.trim();
    if text.len() == 10 {
        let date = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").ok()?;
        return Some(date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_millis());
    }
    let (naive, offset_seconds) = split_offset(text);
    let parsed = chrono::NaiveDateTime::parse_from_str(naive, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(naive, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(naive, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()?;
    Some(parsed.and_utc().timestamp_millis() - offset_seconds * 1000)
}

/// Split a trailing `+HH`, `+HH:MM` or `+HHMM` zone offset off a timestamp
/// text, returning the rest and the offset in seconds east of UTC.
fn split_offset(text: &str) -> (&str, i64) {
    let Some(position) = text.rfind(['+', '-']) else {
        return (text, 0);
    };
    if position < 11 {
        return (text, 0);
    }
    let (rest, zone) = text.split_at(position);
    let sign = if zone.starts_with('-') { -1 } else { 1 };
    let digits: String = zone[1..].chars().filter(char::is_ascii_digit).collect();
    let (hours, minutes) = match digits.len() {
        2 => (digits.parse::<i64>().unwrap_or(0), 0),
        4 => (
            digits[..2].parse::<i64>().unwrap_or(0),
            digits[2..].parse::<i64>().unwrap_or(0),
        ),
        _ => return (text, 0),
    };
    (rest, sign * (hours * 3600 + minutes * 60))
}

/// A JSON array text (what `to_json(column)::text` returns for an array
/// column) as a [`Value::List`] of `inner`-typed items; text that is not
/// a JSON array is kept whole as a string.
pub fn json_list(text: &str, inner: &ValueType) -> Value {
    match serde_json::from_str::<serde_json::Value>(text) {
        Ok(serde_json::Value::Array(items)) => {
            Value::List(items.iter().map(|item| json_scalar(item, inner)).collect())
        }
        _ => Value::String(text.to_owned()),
    }
}

/// One JSON scalar as the [`Value`] the item type calls for.
fn json_scalar(item: &serde_json::Value, inner: &ValueType) -> Value {
    match item {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(flag) => Value::Bool(*flag),
        serde_json::Value::Number(number) => match (number.as_i64(), inner) {
            (Some(int), ValueType::Float) => Value::Float(int as f64),
            (Some(int), _) => Value::Int(int),
            (None, _) => Value::Float(number.as_f64().unwrap_or(f64::NAN)),
        },
        serde_json::Value::String(text) => scalar_text(text, inner),
        other => Value::String(other.to_string()),
    }
}

/// A Postgres array literal (`{a,"b c",NULL}`) as a [`Value::List`] of
/// `inner`-typed items.
pub fn array_literal(text: &str, inner: &ValueType) -> Value {
    let body = text
        .trim()
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'));
    let Some(body) = body else {
        return Value::String(text.to_owned());
    };
    if body.is_empty() {
        return Value::List(Vec::new());
    }
    let mut items = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut was_quoted = false;
    let mut escaped = false;
    let mut depth = 0usize;
    for character in body.chars() {
        if escaped {
            current.push(character);
            escaped = false;
        } else if quoted {
            match character {
                '\\' => escaped = true,
                '"' => quoted = false,
                other => current.push(other),
            }
        } else {
            match character {
                '"' => {
                    quoted = true;
                    was_quoted = true;
                }
                '{' => {
                    depth += 1;
                    current.push(character);
                }
                '}' => {
                    depth = depth.saturating_sub(1);
                    current.push(character);
                }
                ',' if depth == 0 => {
                    items.push(array_item(&current, was_quoted, inner));
                    current.clear();
                    was_quoted = false;
                }
                other => current.push(other),
            }
        }
    }
    items.push(array_item(&current, was_quoted, inner));
    Value::List(items)
}

/// One element of an array literal: an unquoted `NULL` is null, anything
/// else the scalar the item type calls for.
fn array_item(text: &str, was_quoted: bool, inner: &ValueType) -> Value {
    if !was_quoted && text.eq_ignore_ascii_case("NULL") {
        return Value::Null;
    }
    scalar_text(text, inner)
}

/// A scalar in text form as the [`Value`] its type calls for; text that
/// does not parse as the type stays a string.
fn scalar_text(text: &str, inner: &ValueType) -> Value {
    match inner {
        ValueType::Int | ValueType::Timestamp => text
            .parse()
            .map(Value::Int)
            .unwrap_or_else(|_| Value::String(text.to_owned())),
        ValueType::Float => text
            .parse()
            .map(Value::Float)
            .unwrap_or_else(|_| Value::String(text.to_owned())),
        ValueType::Bool => match text {
            "t" | "true" => Value::Bool(true),
            "f" | "false" => Value::Bool(false),
            other => Value::String(other.to_owned()),
        },
        _ => Value::String(text.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Timestamps without a zone read as UTC, with a zone are shifted, and
    /// dates land on UTC midnight.
    #[test]
    fn times_read_as_epoch_milliseconds() {
        assert_eq!(epoch_millis("1970-01-01 00:00:01.5"), Some(1_500));
        assert_eq!(epoch_millis("1970-01-01 01:00:00+01"), Some(0));
        assert_eq!(epoch_millis("1970-01-01 05:30:00.25+05:30"), Some(250));
        assert_eq!(epoch_millis("1970-01-02"), Some(86_400_000));
        assert_eq!(epoch_millis("infinity"), None);
    }

    /// Array literals split on unquoted commas, unquote, and read NULL.
    #[test]
    fn array_literals_become_lists() {
        assert_eq!(
            array_literal("{a,\"b, c\",NULL,\"NULL\"}", &ValueType::String),
            Value::List(vec![
                Value::from("a"),
                Value::from("b, c"),
                Value::Null,
                Value::from("NULL")
            ])
        );
        assert_eq!(
            array_literal("{1,2}", &ValueType::Int),
            Value::List(vec![Value::Int(1), Value::Int(2)])
        );
        assert_eq!(array_literal("{}", &ValueType::String), Value::List(vec![]));
    }

    /// JSON arrays from `to_json` become the same lists.
    #[test]
    fn json_arrays_become_lists() {
        assert_eq!(
            json_list("[\"x\",null]", &ValueType::String),
            Value::List(vec![Value::from("x"), Value::Null])
        );
        assert_eq!(
            json_list("[1.5,2]", &ValueType::Float),
            Value::List(vec![Value::Float(1.5), Value::Float(2.0)])
        );
    }
}
