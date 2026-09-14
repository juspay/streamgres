//! The change feed: a poller over a logical replication slot decoded by
//! the `test_decoding` output plugin, turning each committed change into
//! a [`WriteQuery`] positioned at its transaction's commit location, plus
//! a progress mark per poll saying how far the feed has delivered.
//! Decoding emits whole transactions in commit order.

use std::rc::Rc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_postgres::Client;

use super::parse_lsn;
use crate::model::{
    Catalog, DataFrameKey, DataFrameRow, DbTable, DeleteQuery, InsertQuery, Lsn, UpdateQuery,
    Value, ValueType, WriteQuery,
};
use crate::sync::service::Command;
use crate::sync::storage::StorageError;

/// One poll's yield: the writes committed since the last poll, in commit
/// order, each at its commit location; and the location the feed is
/// known to have delivered everything up to.
#[derive(Debug)]
pub struct Batch {
    pub writes: Vec<(WriteQuery, Lsn)>,
    pub progress: Lsn,
}

/// One decoded transaction: its commit location and its writes on
/// catalog tables.
#[derive(Debug, PartialEq)]
pub struct Transaction {
    pub commit: Lsn,
    pub writes: Vec<WriteQuery>,
}

/// A poller over one replication slot.
pub struct PgStream {
    client: Client,
    slot: String,
    catalog: Rc<Catalog>,
}

impl PgStream {
    /// Connect and make sure `slot` exists (created with `test_decoding`
    /// if not); the feed starts at the slot's position, so a fresh slot
    /// delivers only what is committed after this call.
    pub async fn open(dsn: &str, slot: &str, catalog: Rc<Catalog>) -> Result<Self, StorageError> {
        let config: tokio_postgres::Config = dsn.parse()?;
        let client = super::open(&config).await?;
        let exists = client
            .query_opt(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await?
            .is_some();
        if !exists {
            client
                .execute(
                    "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
                    &[&slot],
                )
                .await?;
        }
        Ok(PgStream {
            client,
            slot: slot.to_owned(),
            catalog,
        })
    }

    /// Drop the slot (tests tear down with this).
    pub async fn drop_slot(dsn: &str, slot: &str) -> Result<(), StorageError> {
        let config: tokio_postgres::Config = dsn.parse()?;
        let client = super::open(&config).await?;
        client
            .execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await?;
        Ok(())
    }

    /// Consume everything the slot has decoded since the last poll. The
    /// progress mark is the flush location read *before* consuming: every
    /// transaction committed at or below it is in this or an earlier
    /// batch.
    pub async fn poll(&mut self) -> Result<Batch, StorageError> {
        let flushed = self
            .client
            .query_one("SELECT pg_current_wal_flush_lsn()::text", &[])
            .await?;
        let progress = parse_lsn(flushed.get(0))?;
        let rows = self
            .client
            .query(
                "SELECT lsn::text, xid::text, data FROM pg_logical_slot_get_changes($1, NULL, NULL, 'include-xids', '1', 'skip-empty-xacts', '1')",
                &[&self.slot],
            )
            .await?;
        let lines: Vec<(String, String, String)> = rows
            .iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect();
        let mut writes = Vec::new();
        for transaction in decode_changes(&lines, &self.catalog)? {
            let commit = transaction.commit;
            writes.extend(transaction.writes.into_iter().map(|write| (write, commit)));
        }
        Ok(Batch { writes, progress })
    }

    /// Poll every `interval` and feed the service's command channel until
    /// it closes; a failed poll is reported and retried at the next tick.
    pub async fn run<Q>(mut self, interval: Duration, commands: mpsc::Sender<Command<Q>>) {
        loop {
            match self.poll().await {
                Ok(batch) => {
                    for (write, at) in batch.writes {
                        if commands.send(Command::Write { write, at }).await.is_err() {
                            return;
                        }
                    }
                    if commands
                        .send(Command::Progress(batch.progress))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(error) => eprintln!("change feed poll failed: {error}"),
            }
            tokio::time::sleep(interval).await;
        }
    }
}

/// Turn decoded `(lsn, xid, data)` lines into transactions: changes are
/// collected per transaction and released at its `COMMIT` with the commit
/// location. Tables the catalog does not declare are skipped.
pub fn decode_changes(
    lines: &[(String, String, String)],
    catalog: &Catalog,
) -> Result<Vec<Transaction>, StorageError> {
    let mut out = Vec::new();
    let mut pending: Vec<WriteQuery> = Vec::new();
    for (lsn, _, data) in lines {
        if data.starts_with("BEGIN ") {
            pending.clear();
        } else if data.starts_with("COMMIT ") {
            out.push(Transaction {
                commit: parse_lsn(lsn)?,
                writes: std::mem::take(&mut pending),
            });
        } else if let Some(rest) = data.strip_prefix("table ") {
            pending.extend(decode_change(rest, catalog)?);
        }
    }
    Ok(out)
}

/// Decode one `schema.table: KIND: columns` line into zero, one, or (for a
/// primary-key change) two writes.
fn decode_change(line: &str, catalog: &Catalog) -> Result<Vec<WriteQuery>, StorageError> {
    let (qualified, rest) = line
        .split_once(": ")
        .ok_or_else(|| StorageError(format!("unreadable change `{line}`")))?;
    let name = qualified
        .rsplit('.')
        .next()
        .unwrap_or(qualified)
        .trim_matches('"');
    let Some(table) = catalog.table(name) else {
        return Ok(Vec::new());
    };
    let (kind, columns) = rest
        .split_once(": ")
        .unwrap_or((rest.trim_end_matches(':'), ""));
    match kind {
        "INSERT" => {
            let row = decode_columns(columns, table)?;
            Ok(vec![WriteQuery::INSERT(InsertQuery {
                table: table.name.clone(),
                pkey_value: key_of(&row, table),
                record: row,
            })])
        }
        "UPDATE" => {
            let (old, new) = match columns.strip_prefix("old-key: ") {
                Some(with_key) => {
                    let (old, new) = with_key
                        .split_once(" new-tuple: ")
                        .ok_or_else(|| StorageError(format!("unreadable update `{line}`")))?;
                    (
                        Some(decode_columns(old, table)?),
                        decode_columns(new, table)?,
                    )
                }
                None => (None, decode_columns(columns, table)?),
            };
            let new_key = key_of(&new, table);
            let old_key = old.as_ref().map(|old| key_of(old, table));
            let mut writes = Vec::new();
            if let Some(old_key) = old_key.filter(|old_key| *old_key != new_key) {
                writes.push(WriteQuery::DELETE(DeleteQuery {
                    table: table.name.clone(),
                    pkey_value: old_key,
                }));
                writes.push(WriteQuery::INSERT(InsertQuery {
                    table: table.name.clone(),
                    pkey_value: new_key,
                    record: new,
                }));
            } else {
                writes.push(WriteQuery::UPDATE(UpdateQuery {
                    table: table.name.clone(),
                    pkey_value: new_key,
                    record: new,
                }));
            }
            Ok(writes)
        }
        "DELETE" => {
            let row = decode_columns(columns, table)?;
            Ok(vec![WriteQuery::DELETE(DeleteQuery {
                table: table.name.clone(),
                pkey_value: key_of(&row, table),
            })])
        }
        _ => Ok(Vec::new()),
    }
}

/// The primary key of a decoded row image.
fn key_of(row: &DataFrameRow, table: &DbTable) -> DataFrameKey {
    DataFrameKey::new(
        table
            .pkey
            .iter()
            .map(|column| {
                (
                    column.as_str().to_owned(),
                    row.data
                        .get(column.as_str())
                        .cloned()
                        .unwrap_or(Value::Null),
                )
            })
            .collect(),
    )
}

/// Decode `name[type]:value name[type]:value …` into a row image, each
/// value converted by the column's declared type.
fn decode_columns(text: &str, table: &DbTable) -> Result<DataFrameRow, StorageError> {
    let mut data = std::collections::HashMap::new();
    for (name, raw) in tokenize(text)? {
        let Some(column) = table.column(&name) else {
            continue;
        };
        data.insert(name, convert(raw.as_deref(), &column.r#type)?);
    }
    Ok(DataFrameRow { data })
}

/// Split a column list into `(name, raw value)` pairs; `None` is a SQL
/// `null`. Quoted values are unquoted (doubled quotes collapsed); the
/// bracketed type is skipped.
fn tokenize(text: &str) -> Result<Vec<(String, Option<String>)>, StorageError> {
    let unreadable = || StorageError(format!("unreadable column list `{text}`"));
    let mut out = Vec::new();
    let mut rest = text.trim_start();
    while !rest.is_empty() {
        let bracket = rest.find('[').ok_or_else(unreadable)?;
        let name = rest[..bracket].to_owned();
        let close = rest[bracket..].find("]:").ok_or_else(unreadable)? + bracket;
        rest = &rest[close + 2..];
        let raw = if let Some(quoted) = rest.strip_prefix('\'') {
            let mut value = String::new();
            let mut chars = quoted.char_indices().peekable();
            let mut end = None;
            while let Some((index, ch)) = chars.next() {
                if ch == '\'' {
                    if chars.peek().is_some_and(|(_, next)| *next == '\'') {
                        value.push('\'');
                        chars.next();
                    } else {
                        end = Some(index + 1);
                        break;
                    }
                } else {
                    value.push(ch);
                }
            }
            let end = end.ok_or_else(unreadable)?;
            rest = &quoted[end..];
            Some(value)
        } else {
            let end = rest.find(' ').unwrap_or(rest.len());
            let bare = &rest[..end];
            rest = &rest[end..];
            if bare == "null" {
                None
            } else if bare == "unchanged-toast-datum" {
                return Err(StorageError(format!(
                    "column `{name}` arrived without its value; the table needs REPLICA IDENTITY FULL or smaller values"
                )));
            } else {
                Some(bare.to_owned())
            }
        };
        out.push((name, raw));
        rest = rest.trim_start();
    }
    Ok(out)
}

/// Convert one decoded text value by its declared type.
fn convert(raw: Option<&str>, declared: &ValueType) -> Result<Value, StorageError> {
    let Some(raw) = raw else {
        return Ok(Value::Null);
    };
    let unreadable = || StorageError(format!("`{raw}` is not a {declared:?}"));
    Ok(match declared {
        ValueType::Int => Value::Int(raw.parse().map_err(|_| unreadable())?),
        ValueType::Float => Value::Float(raw.parse().map_err(|_| unreadable())?),
        ValueType::String | ValueType::List(_) | ValueType::Map(_, _) => {
            Value::String(raw.to_owned())
        }
        ValueType::Bool => Value::Bool(match raw {
            "true" | "t" => true,
            "false" | "f" => false,
            _ => return Err(unreadable()),
        }),
        ValueType::Date => Value::Date(
            chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d").map_err(|_| unreadable())?,
        ),
        ValueType::Datetime => Value::Datetime(
            chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S"))
                .map_err(|_| unreadable())?,
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::evaluate;
    use crate::model::{ComparisonOperator, DbColumn, DbTable, Where};

    /// `probe_t`, the table the format was recorded from.
    fn catalog() -> Catalog {
        Catalog::new(vec![DbTable::new(
            "probe_t",
            ["id"],
            vec![
                DbColumn::new("id", ValueType::Int),
                DbColumn::new("name", ValueType::String),
                DbColumn::new("points", ValueType::Int),
                DbColumn::new("flag", ValueType::Bool),
                DbColumn::new("d", ValueType::Date),
                DbColumn::new("ts", ValueType::Datetime),
                DbColumn::new("amount", ValueType::Float),
                DbColumn::new("tag", ValueType::String),
            ],
        )])
    }

    /// A recorded `test_decoding` session: three transactions, quoting,
    /// nulls, every scalar type, an update, a primary-key change (two
    /// writes) and a delete; each transaction at its commit location.
    #[test]
    fn decodes_a_recorded_session() {
        let lines: Vec<(String, String, String)> = [
            ("0/19735D8", "727", "BEGIN 727"),
            ("0/19735D8", "727", "table public.probe_t: INSERT: id[bigint]:1 name[text]:'it''s x' points[bigint]:3 flag[boolean]:true d[date]:'2026-09-11' ts[timestamp without time zone]:'2026-09-11 10:00:00.5' amount[double precision]:1.5 tag[character varying]:'a b'"),
            ("0/19736E8", "727", "table public.probe_t: INSERT: id[bigint]:2 name[text]:null points[bigint]:null flag[boolean]:null d[date]:null ts[timestamp without time zone]:null amount[double precision]:null tag[character varying]:null"),
            ("0/1973798", "727", "COMMIT 727"),
            ("0/1973798", "728", "BEGIN 728"),
            ("0/1973798", "728", "table public.probe_t: UPDATE: id[bigint]:1 name[text]:'renamed' points[bigint]:4 flag[boolean]:true d[date]:'2026-09-11' ts[timestamp without time zone]:'2026-09-11 10:00:00.5' amount[double precision]:1.5 tag[character varying]:'a b'"),
            ("0/1973840", "728", "COMMIT 728"),
            ("0/1973840", "729", "BEGIN 729"),
            ("0/1973840", "729", "table public.probe_t: UPDATE: old-key: id[bigint]:2 new-tuple: id[bigint]:3 name[text]:null points[bigint]:null flag[boolean]:null d[date]:null ts[timestamp without time zone]:null amount[double precision]:null tag[character varying]:null"),
            ("0/19738D8", "729", "table public.probe_t: DELETE: id[bigint]:1"),
            ("0/1973950", "729", "table public.other: INSERT: id[bigint]:9"),
            ("0/1973950", "729", "COMMIT 729"),
        ]
        .into_iter()
        .map(|(lsn, xid, data)| (lsn.to_owned(), xid.to_owned(), data.to_owned()))
        .collect();
        let transactions = decode_changes(&lines, &catalog()).unwrap();
        assert_eq!(transactions.len(), 3, "{transactions:?}");
        assert_eq!(transactions[0].commit, Lsn::parse("0/1973798").unwrap());
        assert_eq!(transactions[0].writes.len(), 2);

        let image = transactions[0].writes[0].new_row_image().unwrap();
        assert_eq!(image.data["name"], Value::String("it's x".into()));
        assert_eq!(image.data["tag"], Value::String("a b".into()));
        assert_eq!(image.data["flag"], Value::Bool(true));
        assert_eq!(image.data["amount"], Value::Float(1.5));
        assert_eq!(
            image.data["d"],
            Value::Date(chrono::NaiveDate::from_ymd_opt(2026, 9, 11).unwrap())
        );
        assert!(matches!(image.data["ts"], Value::Datetime(_)));
        assert!(evaluate(
            &Where::condition("points", ComparisonOperator::GTE, 3),
            &image.data,
            &mut 0
        ));
        assert_eq!(
            transactions[0].writes[1].new_row_image().unwrap().data["name"],
            Value::Null
        );
        assert!(matches!(transactions[1].writes[0], WriteQuery::UPDATE(_)));
        assert_eq!(transactions[1].commit, Lsn::parse("0/1973840").unwrap());
        let last = &transactions[2].writes;
        assert_eq!(
            last.len(),
            3,
            "a key change is a delete and an insert; the undeclared table is skipped"
        );
        assert!(
            matches!(&last[0], WriteQuery::DELETE(d) if d.pkey_value.pkey_value["id"] == Value::Int(2))
        );
        assert!(
            matches!(&last[1], WriteQuery::INSERT(i) if i.pkey_value.pkey_value["id"] == Value::Int(3))
        );
        assert!(
            matches!(&last[2], WriteQuery::DELETE(d) if d.pkey_value.pkey_value["id"] == Value::Int(1))
        );
        assert_eq!(transactions[2].commit, Lsn::parse("0/1973950").unwrap());
    }
}
