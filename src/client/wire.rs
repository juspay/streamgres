//! Rows and keys as the wire carries them: JSON objects keyed by column,
//! with a JSON column's text embedded as the JSON it holds, a time as its
//! milliseconds, and only the columns the client's schema declares when
//! that schema is known. The row-patch entries of a poke are written
//! straight into bytes ([`write_put`], [`write_del`]), once per row and
//! shared by every group's frame that carries the row; the JSON-tree
//! forms stay for the small messages and the tests.

use std::collections::HashSet;
use std::io::Write;

use serde_json::{Map, Value as Json};

use crate::model::{DataFrameKey, DataFrameRow, DbTable, Value, ValueType};

/// Append the `rowsPatch` entry putting `row` into `table`:
/// `{"op":"put","tableName":…,"value":{…}}`, with only the `allowed`
/// columns when the client's schema is known.
pub fn write_put(
    out: &mut Vec<u8>,
    table: &DbTable,
    row: &DataFrameRow,
    allowed: Option<&HashSet<String>>,
) {
    out.extend_from_slice(b"{\"op\":\"put\",\"tableName\":");
    write_text(out, table.name.as_str());
    out.extend_from_slice(b",\"value\":{");
    let mut first = true;
    for (column, value) in row.data.iter() {
        if allowed.is_some_and(|allowed| !allowed.contains(column.as_str())) {
            continue;
        }
        if !first {
            out.push(b',');
        }
        first = false;
        write_text(out, column.as_str());
        out.push(b':');
        let declared = table.column(column.as_str()).map(|column| &column.r#type);
        write_value(out, value, declared);
    }
    out.extend_from_slice(b"}}");
}

/// Append the `rowsPatch` entry deleting `key` from `table`:
/// `{"op":"del","tableName":…,"id":{…}}`.
pub fn write_del(out: &mut Vec<u8>, table: &str, key: &DataFrameKey) {
    out.extend_from_slice(b"{\"op\":\"del\",\"tableName\":");
    write_text(out, table);
    out.extend_from_slice(b",\"id\":{");
    let mut first = true;
    for (column, value) in key.pkey_value.iter() {
        if !first {
            out.push(b',');
        }
        first = false;
        write_text(out, column.as_str());
        out.push(b':');
        write_value(out, value, None);
    }
    out.extend_from_slice(b"}}");
}

/// Append one cell as JSON, in the light of its declared type: a JSON
/// column's text is embedded as it is when it parses, a time as its
/// milliseconds, a list item by item.
pub fn write_value(out: &mut Vec<u8>, value: &Value, declared: Option<&ValueType>) {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::String(text) => match declared {
            Some(ValueType::Json)
                if serde_json::from_str::<serde::de::IgnoredAny>(text).is_ok() =>
            {
                out.extend_from_slice(text.as_bytes());
            }
            _ => write_text(out, text),
        },
        Value::Int(int) => {
            let _ = write!(out, "{int}");
        }
        Value::Float(float) => {
            if serde_json::to_writer(&mut *out, float).is_err() {
                out.extend_from_slice(b"null");
            }
        }
        Value::Bool(true) => out.extend_from_slice(b"true"),
        Value::Bool(false) => out.extend_from_slice(b"false"),
        Value::Date(date) => match date.and_hms_opt(0, 0, 0) {
            Some(at) => {
                let _ = write!(out, "{}", at.and_utc().timestamp_millis());
            }
            None => out.extend_from_slice(b"null"),
        },
        Value::Datetime(datetime) => {
            let _ = write!(out, "{}", datetime.and_utc().timestamp_millis());
        }
        Value::List(items) => {
            let inner = match declared {
                Some(ValueType::List(inner)) => Some(inner.as_ref()),
                _ => None,
            };
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_value(out, item, inner);
            }
            out.push(b']');
        }
        Value::Map(_) | Value::Set(_) => {
            if serde_json::to_writer(&mut *out, &value_json(value, declared)).is_err() {
                out.extend_from_slice(b"null");
            }
        }
    }
}

/// Append `text` as a JSON string, escaped.
fn write_text(out: &mut Vec<u8>, text: &str) {
    if serde_json::to_writer(&mut *out, text).is_err() {
        out.extend_from_slice(b"\"\"");
    }
}

/// A row as a JSON object.
pub fn row_json(row: &DataFrameRow, table: &DbTable, allowed: Option<&HashSet<String>>) -> Json {
    let mut object = Map::with_capacity(row.data.len());
    for (column, value) in row.data.iter() {
        if allowed.is_some_and(|allowed| !allowed.contains(column.as_str())) {
            continue;
        }
        let declared = table.column(column.as_str()).map(|column| &column.r#type);
        object.insert(column.as_str().to_owned(), value_json(value, declared));
    }
    Json::Object(object)
}

/// A key as a JSON object of its primary-key columns.
pub fn key_json(key: &DataFrameKey) -> Json {
    let mut object = Map::with_capacity(key.pkey_value.len());
    for (column, value) in key.pkey_value.iter() {
        object.insert(column.as_str().to_owned(), value_json(value, None));
    }
    Json::Object(object)
}

/// One cell as JSON, in the light of its declared type.
pub fn value_json(value: &Value, declared: Option<&ValueType>) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::String(text) => match declared {
            Some(ValueType::Json) => {
                serde_json::from_str(text).unwrap_or_else(|_| Json::String(text.clone()))
            }
            _ => Json::String(text.clone()),
        },
        Value::Int(int) => Json::from(*int),
        Value::Float(float) => {
            serde_json::Number::from_f64(*float).map_or(Json::Null, Json::Number)
        }
        Value::Bool(flag) => Json::Bool(*flag),
        Value::Date(date) => date
            .and_hms_opt(0, 0, 0)
            .map_or(Json::Null, |at| Json::from(at.and_utc().timestamp_millis())),
        Value::Datetime(datetime) => Json::from(datetime.and_utc().timestamp_millis()),
        Value::List(items) => {
            let inner = match declared {
                Some(ValueType::List(inner)) => Some(inner.as_ref()),
                _ => None,
            };
            Json::Array(items.iter().map(|item| value_json(item, inner)).collect())
        }
        Value::Map(entries) => {
            let mut object = Map::with_capacity(entries.len());
            for (key, item) in entries {
                let key = match key {
                    Value::String(text) => text.clone(),
                    other => format!("{other:?}"),
                };
                object.insert(key, value_json(item, None));
            }
            Json::Object(object)
        }
        Value::Set(set) => Json::Array(
            set.members()
                .iter()
                .map(|item| value_json(item, None))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DbColumn, DbTable};
    use std::collections::HashMap;

    /// JSON columns are embedded, times are numbers, and the client's
    /// schema filters the columns.
    #[test]
    fn rows_serialize_for_the_client() {
        let table = DbTable::new(
            "t",
            ["id"],
            vec![
                DbColumn::new("id", ValueType::String),
                DbColumn::new("meta", ValueType::Json),
                DbColumn::new("at", ValueType::Timestamp),
                DbColumn::new("extra", ValueType::Int),
            ],
        );
        let row = DataFrameRow::from(HashMap::from([
            ("id".into(), Value::from("a")),
            ("meta".into(), Value::from(r#"{"k":[1,2]}"#)),
            ("at".into(), Value::Int(1_000)),
            ("extra".into(), Value::Int(7)),
        ]));
        let allowed: HashSet<String> = ["id", "meta", "at"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        let json = row_json(&row, &table, Some(&allowed));
        assert_eq!(json["meta"]["k"][1], 2);
        assert_eq!(json["at"], 1_000);
        assert!(json.get("extra").is_none());

        let mut bytes = Vec::new();
        write_put(&mut bytes, &table, &row, Some(&allowed));
        let written: Json = serde_json::from_slice(&bytes).expect("valid JSON");
        assert_eq!(written["op"], "put");
        assert_eq!(written["tableName"], "t");
        assert_eq!(written["value"], json, "the bytes say what the tree says");
        let mut bytes = Vec::new();
        write_del(
            &mut bytes,
            "t",
            &DataFrameKey::new([("id", Value::from("a\"b"))]),
        );
        let written: Json = serde_json::from_slice(&bytes).expect("valid JSON");
        assert_eq!(written["id"]["id"], "a\"b", "keys are escaped");
    }
}
