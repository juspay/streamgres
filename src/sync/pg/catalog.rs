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
//!
//! With a publication named, only the tables it streams are taken: a
//! table of a served schema the publication leaves out would otherwise be
//! read from a snapshot once and never hear of a change, and its rows
//! served stale. For the same reason the publication must send every
//! change of what it streams ([`publication_gaps`]): a snapshot reads
//! whole tables, so a row filter, a column list or an action left out
//! would leave rows the snapshot served that no change ever updates.
//!
//! It also notes which text-carried columns Postgres compares for
//! equality as the engine does, byte by byte ([`compares_as_text`]): a
//! nondeterministic collation, `citext` or `char(n)` makes distinct
//! texts equal, and the engine would match other rows than Postgres.

use std::collections::{BTreeMap, BTreeSet};

use tokio_postgres::Client;

use super::StorageError;
use crate::model::{Catalog, ColumnName, DbColumn, DbTable, TableName, ValueType};

/// What the load left out and why, one line per table or column.
pub type Notes = Vec<String>;

/// Read the catalog of `schemas` through `client`: every table of theirs
/// that `publication` streams, or every table with no publication named.
pub async fn load_catalog(
    client: &Client,
    schemas: &[String],
    publication: Option<&str>,
) -> Result<(Catalog, Notes), StorageError> {
    if let Some(name) = publication {
        let gaps = publication_gaps(client, name).await?;
        if !gaps.is_empty() {
            return Err(StorageError(format!(
                "publication `{name}` does not send every change of what it publishes ({}); the engine snapshots whole tables, so it would serve rows no change updates. Publish the tables whole, with every action (publish = 'insert, update, delete, truncate'), or let the engine create its own publication",
                gaps.join("; ")
            )));
        }
    }
    let published: Option<BTreeSet<(String, String)>> = match publication {
        Some(name) => Some(
            client
                .query(
                    "SELECT schemaname::text, tablename::text FROM pg_publication_tables
                     WHERE pubname = $1 AND schemaname = ANY($2)",
                    &[&name, &schemas],
                )
                .await?
                .iter()
                .map(|row| (row.get(0), row.get(1)))
                .collect(),
        ),
        None => None,
    };
    let columns = client
        .query(
            "SELECT c.table_schema, c.table_name, c.column_name, c.data_type, c.udt_name,
                    c.collation_name,
                    CASE WHEN c.collation_name IS NULL THEN true ELSE
                      (SELECT bool_and(co.collisdeterministic) FROM pg_collation co
                         JOIN pg_namespace cn ON cn.oid = co.collnamespace
                        WHERE cn.nspname = c.collation_schema AND co.collname = c.collation_name)
                    END,
                    COALESCE((SELECT e.typtype = 'e' FROM pg_type t
                                JOIN pg_namespace tn ON tn.oid = t.typnamespace
                                JOIN pg_type e ON e.oid = CASE WHEN c.data_type = 'ARRAY'
                                                               THEN t.typelem ELSE t.oid END
                               WHERE tn.nspname = c.udt_schema AND t.typname = c.udt_name), false)
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
    let secured = client
        .query(
            "SELECT n.nspname, c.relname FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relrowsecurity AND c.relkind IN ('r', 'p') AND n.nspname = ANY($1)",
            &[&schemas],
        )
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
    let mut compared_otherwise: Vec<(TableName, ColumnName)> = Vec::new();
    let mut notes = Vec::new();
    for row in &columns {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        let column: String = row.get(2);
        let data_type: String = row.get(3);
        let udt_name: String = row.get(4);
        let collation: Option<String> = row.get(5);
        let deterministic: Option<bool> = row.get(6);
        let is_enum: bool = row.get(7);
        if orders_as_text(&data_type, collation.as_deref(), &default_collation) {
            ordered.push((
                TableName::from(table_name(&schema, &table)),
                ColumnName::from(column.as_str()),
            ));
        }
        let element = match data_type.as_str() {
            "ARRAY" => udt_name.strip_prefix('_').unwrap_or(&udt_name),
            _ => udt_name.as_str(),
        };
        if compares_as_text(element, is_enum, deterministic) != Some(true) {
            compared_otherwise.push((
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
    let mut unpublished = 0_usize;
    for ((schema, table), columns) in declared {
        if published
            .as_ref()
            .is_some_and(|published| !published.contains(&(schema.clone(), table.clone())))
        {
            unpublished += 1;
            continue;
        }
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
    if let (Some(name), true) = (publication, unpublished > 0) {
        notes.push(format!(
            "{unpublished} tables of the served schemas are not in publication `{name}`, so they are not followed"
        ));
    }
    Ok((
        Catalog::new(tables)
            .with_enum_oids(enum_oids)
            .with_ordered_as_text(ordered)
            .with_compared_otherwise(compared_otherwise)
            .with_bytewise_default(default_collation.is_bytewise())
            .with_row_secured(secured.iter().map(|row| {
                let schema: String = row.get(0);
                let table: String = row.get(1);
                TableName::from(table_name(&schema, &table))
            })),
        notes,
    ))
}

/// What keeps publication `name` from sending every change of the tables
/// it publishes, one line each: an action it leaves out, a table it
/// filters by a `WHERE`, a table it publishes some columns of. Empty when
/// it sends every change, and when it does not exist (the feed's own
/// check names that). While serving, the schema-change message for a
/// change carries it in WAL order, and stops the engine: a row filter, a
/// column list as columns gone, an action turned off as the actions the
/// stack records ([`super::ddl::DdlMessage::missing_actions`]).
pub async fn publication_gaps(client: &Client, name: &str) -> Result<Vec<String>, StorageError> {
    let Some(actions) = client
        .query_opt(
            "SELECT pubinsert, pubupdate, pubdelete, pubtruncate FROM pg_publication WHERE pubname = $1",
            &[&name],
        )
        .await?
    else {
        return Ok(Vec::new());
    };
    let mut gaps: Vec<String> = ["insert", "update", "delete", "truncate"]
        .into_iter()
        .enumerate()
        .filter(|(at, _)| !actions.get::<_, bool>(*at))
        .map(|(_, action)| format!("it does not publish {action}"))
        .collect();
    let partial = client
        .query(
            "SELECT n.nspname::text, c.relname::text,
                    to_jsonb(r) ->> 'prqual' IS NOT NULL, to_jsonb(r) ->> 'prattrs' IS NOT NULL
             FROM pg_publication_rel r
             JOIN pg_publication p ON p.oid = r.prpubid
             JOIN pg_class c ON c.oid = r.prrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE p.pubname = $1
               AND (to_jsonb(r) ->> 'prqual' IS NOT NULL OR to_jsonb(r) ->> 'prattrs' IS NOT NULL)
             ORDER BY 1, 2",
            &[&name],
        )
        .await?;
    for row in &partial {
        let (schema, table): (String, String) = (row.get(0), row.get(1));
        if row.get::<_, bool>(2) {
            gaps.push(format!("it filters {schema}.{table} by a row filter"));
        }
        if row.get::<_, bool>(3) {
            gaps.push(format!("it publishes some columns of {schema}.{table}"));
        }
    }
    Ok(gaps)
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
/// enum, `citext`, `char(n)`, whose trailing spaces do not count, an
/// `inet`, an interval, a time with its zone) orders by rules of its own.
pub fn orders_as_text(
    data_type: &str,
    collation: Option<&str>,
    default: &DefaultCollation,
) -> bool {
    match data_type {
        "uuid" | "bytea" | "name" | "time without time zone" => true,
        "text" | "character varying" => match collation {
            Some(name) => is_bytewise_collation(name),
            None => default.is_bytewise(),
        },
        _ => false,
    }
}

/// [`orders_as_text`] for a column a schema-change message publishes:
/// its type as `pg_type` names it, and its collation's name (`default`
/// for the database's, whose verdict is `bytewise_default`). A string
/// column with no collation named, from a trigger stack older than the
/// name, is not known to order as text.
pub fn published_orders_as_text(
    type_name: &str,
    collation: Option<&str>,
    bytewise_default: bool,
) -> bool {
    match type_name {
        "uuid" | "bytea" | "name" | "time" => true,
        "text" | "varchar" => match collation {
            Some("default") => bytewise_default,
            Some(name) => is_bytewise_collation(name),
            None => false,
        },
        _ => false,
    }
}

/// Whether Postgres decides equality on a column as the engine does, by
/// its text byte for byte: its type as `pg_type` names it (an array's
/// element type for an array), whether that type is an enum, and whether
/// its collation is deterministic (the database's default always is).
/// `None` when that depends on a collation not known. A string under a
/// deterministic collation does: such a collation calls texts equal only
/// when their bytes are. So do an enum's label, a `uuid`, a `bytea` and a
/// `time`, each carried in one spelling, and every type the engine
/// carries exactly as a value of its own (an integer, a `float8`, a
/// boolean, a time, JSON). A string under a nondeterministic collation
/// does not, nor `citext`, nor `char(n)`, whose trailing spaces do not
/// count, nor any other text-carried type (an interval, a network
/// address, an unknown type), where texts spelled differently can be
/// equal. Nor does a `numeric`, a `money` or a `real`, which the engine
/// carries as a `float8`: a `numeric` past 2^53 or of more than fifteen
/// digits loses its last ones, so distinct values compare equal, and a
/// `real` widened is not the `float8` its text reads as. The same holds of
/// their order, which [`crate::model::Catalog::orders_as_engine`] refuses
/// for them too.
pub fn compares_as_text(type_name: &str, is_enum: bool, deterministic: Option<bool>) -> Option<bool> {
    if is_enum {
        return Some(true);
    }
    match scalar_type(type_name) {
        Some(ValueType::String) => match type_name {
            "text" | "varchar" | "name" => deterministic,
            "uuid" | "bytea" | "time" => Some(true),
            _ => Some(false),
        },
        Some(ValueType::Float) => Some(matches!(type_name, "float8" | "double precision")),
        Some(_) => Some(true),
        None => Some(false),
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

    /// Strings compare as their text only under a deterministic
    /// collation; `citext`, `char(n)` and the other text-carried types
    /// with spellings of their own never do; enums, one-spelling types
    /// and values always do. `char(n)` does not order as its text either.
    #[test]
    fn equality_follows_the_collation_and_type() {
        assert_eq!(compares_as_text("varchar", false, Some(true)), Some(true));
        assert_eq!(compares_as_text("text", false, Some(false)), Some(false));
        assert_eq!(compares_as_text("text", false, None), None);
        assert_eq!(compares_as_text("citext", false, Some(true)), Some(false));
        assert_eq!(compares_as_text("bpchar", false, Some(true)), Some(false));
        assert_eq!(compares_as_text("inet", false, None), Some(false));
        assert_eq!(compares_as_text("TicketStatus", true, None), Some(true));
        assert_eq!(compares_as_text("uuid", false, None), Some(true));
        assert_eq!(compares_as_text("int8", false, None), Some(true));
        assert_eq!(compares_as_text("float8", false, None), Some(true));
        for inexact in ["numeric", "money", "float4"] {
            assert_eq!(compares_as_text(inexact, false, None), Some(false), "{inexact}");
        }
        let c = DefaultCollation {
            name: "C".to_owned(),
            provider: Some("c".to_owned()),
        };
        assert!(orders_as_text("text", None, &c));
        assert!(!orders_as_text("character", None, &c), "trailing spaces do not count");
        assert!(!published_orders_as_text("bpchar", Some("C"), true));
    }
}
