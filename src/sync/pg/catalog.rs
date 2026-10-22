//! The catalog read from the database itself: every table of the chosen
//! schemas that has a primary key, its columns mapped onto the engine's
//! value types the way the sync protocol maps them for its clients (times as epoch
//! microseconds, JSON and arrays as JSON, enums and uuids as strings,
//! `bytea` as Postgres's hex text), so a server needs no hand-written
//! schema. It also notes which type OIDs are enums, so a bound parameter
//! of an enum type can be read as the text it is, and which text-carried
//! columns Postgres orders byte by byte, as the engine compares them
//! ([`orders_as_text`]). Tables of
//! the `public` schema keep their bare names; any other table is named
//! `schema.table`, which is also how the change feed matches it.

use std::collections::BTreeMap;

use tokio_postgres::Client;

use super::StorageError;
use crate::model::{Catalog, ColumnName, DbColumn, DbTable, TableName, ValueType};

/// What the load left out and why, one line per table or column.
pub type Notes = Vec<String>;

/// Read the catalog of `schemas` through `client`.
pub async fn load_catalog(
    client: &Client,
    schemas: &[String],
) -> Result<(Catalog, Notes), StorageError> {
    let columns = client
        .query(
            "SELECT c.table_schema, c.table_name, c.column_name, c.data_type, c.udt_name,
                    c.collation_name
             FROM information_schema.columns c
             JOIN information_schema.tables t
               ON t.table_schema = c.table_schema AND t.table_name = c.table_name
             WHERE t.table_type = 'BASE TABLE' AND c.table_schema = ANY($1)
             ORDER BY c.table_schema, c.table_name, c.ordinal_position",
            &[&schemas],
        )
        .await?;
    let keys = client
        .query(
            "SELECT tc.table_schema, tc.table_name, kcu.column_name
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu
               ON kcu.constraint_name = tc.constraint_name
              AND kcu.constraint_schema = tc.constraint_schema
              AND kcu.table_name = tc.table_name
             WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = ANY($1)
             ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position",
            &[&schemas],
        )
        .await?;
    let enums = client
        .query("SELECT oid, typarray FROM pg_type WHERE typtype = 'e'", &[])
        .await?;
    let database = client
        .query_one(
            "SELECT datcollate, to_jsonb(d) ->> 'datlocprovider'
             FROM pg_database d WHERE datname = current_database()",
            &[],
        )
        .await?;
    let default_collation = DefaultCollation {
        name: database.get(0),
        provider: database.get(1),
    };
    let enum_oids: Vec<u32> = enums
        .iter()
        .flat_map(|row| {
            let oid: u32 = row.get(0);
            let array: u32 = row.get(1);
            [oid, array]
        })
        .filter(|oid| *oid != 0)
        .collect();
    let mut pkeys: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for row in &keys {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        let column: String = row.get(2);
        pkeys.entry((schema, table)).or_default().push(column);
    }
    let mut declared: BTreeMap<(String, String), Vec<DbColumn>> = BTreeMap::new();
    let mut ordered: Vec<(TableName, ColumnName)> = Vec::new();
    let mut notes = Vec::new();
    for row in &columns {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        let column: String = row.get(2);
        let data_type: String = row.get(3);
        let udt_name: String = row.get(4);
        let collation: Option<String> = row.get(5);
        if orders_as_text(&data_type, collation.as_deref(), &default_collation) {
            ordered.push((
                TableName::from(table_name(&schema, &table)),
                ColumnName::from(column.as_str()),
            ));
        }
        match value_type(&data_type, &udt_name) {
            Some(r#type) => declared
                .entry((schema, table))
                .or_default()
                .push(DbColumn::new(column, r#type)),
            None => notes.push(format!(
                "{}.{}.{}: {} is not carried",
                schema, table, column, data_type
            )),
        }
    }
    let mut tables = Vec::new();
    for ((schema, table), columns) in declared {
        let Some(pkey) = pkeys.get(&(schema.clone(), table.clone())) else {
            notes.push(format!("{schema}.{table}: no primary key, left out"));
            continue;
        };
        if pkey
            .iter()
            .any(|key| !columns.iter().any(|column| column.name == key.as_str()))
        {
            notes.push(format!(
                "{schema}.{table}: a key column has an uncarried type, left out"
            ));
            continue;
        }
        tables.push(DbTable::new(
            table_name(&schema, &table),
            pkey.iter().map(String::as_str),
            columns,
        ));
    }
    Ok((
        Catalog::new(tables)
            .with_enum_oids(enum_oids)
            .with_ordered_as_text(ordered),
        notes,
    ))
}

/// The database's default collation: its locale name, and its provider
/// (`c` libc, `i` ICU, `b` builtin; absent before Postgres 15, libc).
pub struct DefaultCollation {
    pub name: String,
    pub provider: Option<String>,
}

impl DefaultCollation {
    /// Whether the default orders strings by their bytes: libc's `C` or
    /// `POSIX` locales (and their UTF-8 spellings), or the builtin
    /// provider, whose locales order by code point, which is UTF-8's
    /// byte order. ICU never does.
    fn is_bytewise(&self) -> bool {
        match self.provider.as_deref() {
            Some("i") => false,
            Some("b") => true,
            _ => is_bytewise_collation(&self.name),
        }
    }
}

/// Whether a collation by `name` orders strings by their bytes.
fn is_bytewise_collation(name: &str) -> bool {
    matches!(
        name,
        "C" | "POSIX" | "C.UTF-8" | "C.utf8" | "ucs_basic" | "pg_c_utf8"
    )
}

/// Whether Postgres orders a column of `data_type` (`information_schema`'s
/// name), under `collation` (`None`: the database's default), as its text
/// compares byte by byte. A `uuid`'s lower-case hex and a `bytea`'s hex
/// do, as do `name` (always `C`) and `time`'s zero-padded text; strings
/// do under a byte-order collation; everything else carried as text (an
/// enum, `citext`, an `inet`, an interval, a time with its zone) orders
/// by rules of its own.
pub fn orders_as_text(
    data_type: &str,
    collation: Option<&str>,
    default: &DefaultCollation,
) -> bool {
    match data_type {
        "uuid" | "bytea" | "name" | "time without time zone" => true,
        "text" | "character varying" | "character" => match collation {
            Some(name) => is_bytewise_collation(name),
            None => default.is_bytewise(),
        },
        _ => false,
    }
}

/// The catalog name of a table: bare in `public`, schema-qualified
/// elsewhere.
pub fn table_name(schema: &str, table: &str) -> String {
    if schema == "public" {
        table.to_owned()
    } else {
        format!("{schema}.{table}")
    }
}

/// The engine's type for a Postgres column, from `information_schema`'s
/// `data_type` and `udt_name`; `None` for a type the wire cannot carry.
pub fn value_type(data_type: &str, udt_name: &str) -> Option<ValueType> {
    match data_type {
        "ARRAY" => {
            let element = udt_name.strip_prefix('_').unwrap_or(udt_name);
            Some(ValueType::List(Box::new(
                scalar_type(element).unwrap_or(ValueType::String),
            )))
        }
        "USER-DEFINED" => Some(scalar_type(udt_name).unwrap_or(ValueType::String)),
        other => scalar_type(other).or_else(|| scalar_type(udt_name)),
    }
}

/// The engine's type for a scalar Postgres type name (`information_schema`'s
/// `data_type` or `pg_type`'s name alike); `None` for one the wire cannot
/// carry. A `bytea` is the hex text Postgres renders (`\x` and two digits
/// a byte): what the change feed carries, and what a server-to-server
/// client turns back into bytes.
pub fn scalar_type(name: &str) -> Option<ValueType> {
    let name = name.to_ascii_lowercase();
    let name = name.split('(').next().unwrap_or("").trim();
    Some(match name {
        "smallint" | "integer" | "bigint" | "int2" | "int4" | "int8" | "serial" | "bigserial"
        | "smallserial" | "oid" => ValueType::Int,
        "numeric" | "decimal" | "real" | "double precision" | "float4" | "float8" | "money" => {
            ValueType::Float
        }
        "boolean" | "bool" => ValueType::Bool,
        "json" | "jsonb" => ValueType::Json,
        "timestamp without time zone"
        | "timestamp with time zone"
        | "timestamp"
        | "timestamptz"
        | "date" => ValueType::Timestamp,
        "text"
        | "character varying"
        | "character"
        | "varchar"
        | "bpchar"
        | "char"
        | "uuid"
        | "citext"
        | "name"
        | "time without time zone"
        | "time with time zone"
        | "time"
        | "timetz"
        | "interval"
        | "inet"
        | "cidr"
        | "macaddr"
        | "xml"
        | "tsvector" => ValueType::String,
        "bytea" => ValueType::String,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The mapping follows the protocol's: times are numbers, json and arrays are
    /// json, enums and bytea are strings, ranges are left out.
    #[test]
    fn types_map_like_the_protocol() {
        assert_eq!(
            value_type("timestamp without time zone", "timestamp"),
            Some(ValueType::Timestamp)
        );
        assert_eq!(value_type("date", "date"), Some(ValueType::Timestamp));
        assert_eq!(value_type("jsonb", "jsonb"), Some(ValueType::Json));
        assert_eq!(
            value_type("ARRAY", "_text"),
            Some(ValueType::List(Box::new(ValueType::String)))
        );
        assert_eq!(
            value_type("USER-DEFINED", "TicketStatus"),
            Some(ValueType::String)
        );
        assert_eq!(value_type("bigint", "int8"), Some(ValueType::Int));
        assert_eq!(value_type("numeric", "numeric"), Some(ValueType::Float));
        assert_eq!(value_type("bytea", "bytea"), Some(ValueType::String));
        assert_eq!(value_type("int4range", "int4range"), None);
        assert_eq!(table_name("public", "tickets"), "tickets");
        assert_eq!(table_name("xyne_0", "clients"), "xyne_0.clients");
    }
}
