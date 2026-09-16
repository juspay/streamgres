//! Rows and keys as the wire carries them: JSON objects keyed by column,
//! with a JSON column's text embedded as the JSON it holds, a time as its
//! milliseconds, and only the columns the client's schema declares when
//! that schema is known.

use std::collections::HashSet;

use serde_json::{Map, Value as Json};

use crate::model::{DataFrameKey, DataFrameRow, DbTable, Value, ValueType};

/// A row as a JSON object.
pub fn row_json(row: &DataFrameRow, table: &DbTable, allowed: Option<&HashSet<String>>) -> Json {
    let mut object = Map::with_capacity(row.data.len());
    for (column, value) in &row.data {
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
    for (column, value) in &key.pkey_value {
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
        let row = DataFrameRow {
            data: HashMap::from([
                ("id".into(), Value::from("a")),
                ("meta".into(), Value::from(r#"{"k":[1,2]}"#)),
                ("at".into(), Value::Int(1_000)),
                ("extra".into(), Value::Int(7)),
            ]),
        };
        let allowed: HashSet<String> = ["id", "meta", "at"]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        let json = row_json(&row, &table, Some(&allowed));
        assert_eq!(json["meta"]["k"][1], 2);
        assert_eq!(json["at"], 1_000);
        assert!(json.get("extra").is_none());
    }
}
