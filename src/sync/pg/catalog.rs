//! The catalog read from the database itself: every table that the
//! change feed's publication streams and that lies in one of the chosen
//! schemas, with a primary key, its published columns mapped onto the
//! engine's value types the way the sync protocol maps them for its clients
//! (times as epoch milliseconds, JSON and arrays as JSON, enums and uuids as
//! strings, `bytea` left out), so a server needs no hand-written schema.
//! Tables of the `public` schema keep their bare names; any other table is
//! named `schema.table`, which is also how the change feed matches it.
//!
//! The publication decides what can be served: a table it does not stream
//! (one left out of it, an unlogged table, a partition published through
//! its root) or a column it does not send (a generated column) would be
//! read once and never changed again, so neither is carried. A served
//! table the publication streams only in part (a row filter or a column
//! list) cannot be followed at all, and the load refuses it.

use std::collections::{BTreeMap, BTreeSet};

use tokio_postgres::Client;

use super::StorageError;
use crate::model::{Catalog, DbColumn, DbTable, TableName, ValueType};

/// What the load left out and why, one line per table or column: a
/// warning for what an operator would expect to be served, an
/// information line for what is left out by design.
#[derive(Debug, Default)]
pub struct Notes {
    pub warnings: Vec<String>,
    pub info: Vec<String>,
}

/// Read the catalog through `client`: the tables `publication` streams in
/// `schemas`. An error when the publication does not exist, or streams a
/// table of `schemas` with a row filter or a column list.
pub async fn load_catalog(
    client: &Client,
    publication: &str,
    schemas: &[String],
) -> Result<(Catalog, Notes), StorageError> {
    if client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await?
        .is_none()
    {
        return Err(StorageError(format!(
            "publication `{publication}` does not exist on the database the change feed follows"
        )));
    }
    let columns = client
        .query(
            "SELECT c.table_schema::text, c.table_name::text, c.column_name::text,
                    c.data_type::text, c.udt_name::text,
                    c.column_name::name = ANY (p.attnames) AS published,
                    c.is_generated::text <> 'NEVER' AS generated,
                    p.rowfilter IS NOT NULL AS filtered
             FROM pg_publication_tables p
             JOIN information_schema.columns c
               ON c.table_schema = p.schemaname AND c.table_name = p.tablename
             WHERE p.pubname = $1 AND p.schemaname = ANY ($2)
             ORDER BY c.table_schema, c.table_name, c.ordinal_position",
            &[&publication, &schemas],
        )
        .await?;
    let keys = client
        .query(
            "SELECT tc.table_schema::text, tc.table_name::text, kcu.column_name::text
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu
               ON kcu.constraint_name = tc.constraint_name
              AND kcu.constraint_schema = tc.constraint_schema
              AND kcu.table_name = tc.table_name
             WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = ANY ($1)
             ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position",
            &[&schemas],
        )
        .await?;
    let unpublished = client
        .query(
            "SELECT n.nspname::text, c.relname::text, c.relispartition,
                    c.relpersistence = 'u' AS unlogged
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = ANY ($2) AND c.relkind IN ('r', 'p')
               AND NOT EXISTS (
                 SELECT 1 FROM pg_publication_tables p
                 WHERE p.pubname = $1 AND p.schemaname = n.nspname AND p.tablename = c.relname
               )
             ORDER BY n.nspname, c.relname",
            &[&publication, &schemas],
        )
        .await?;
    let mut pkeys: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for row in &keys {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        let column: String = row.get(2);
        pkeys.entry((schema, table)).or_default().push(column);
    }
    let mut notes = Notes::default();
    for row in &unpublished {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        let partition: bool = row.get(2);
        let unlogged: bool = row.get(3);
        if partition {
            notes.info.push(format!(
                "{schema}.{table}: a partition, served through its root table"
            ));
        } else if unlogged {
            notes.warnings.push(format!(
                "{schema}.{table}: unlogged, so the change feed never carries it; left out"
            ));
        } else {
            notes.warnings.push(format!(
                "{schema}.{table}: not in publication `{publication}`; left out"
            ));
        }
    }
    let mut declared: BTreeMap<(String, String), Vec<DbColumn>> = BTreeMap::new();
    let mut filtered: BTreeSet<String> = BTreeSet::new();
    let mut narrowed: BTreeSet<String> = BTreeSet::new();
    for row in &columns {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        let column: String = row.get(2);
        let data_type: String = row.get(3);
        let udt_name: String = row.get(4);
        let published: bool = row.get(5);
        let generated: bool = row.get(6);
        if row.get::<_, bool>(7) {
            filtered.insert(format!("{schema}.{table}"));
        }
        if !published {
            if generated {
                notes.warnings.push(format!(
                    "{schema}.{table}.{column}: a generated column the publication does not send; left out"
                ));
            } else {
                narrowed.insert(format!("{schema}.{table}"));
            }
            continue;
        }
        let entry = declared.entry((schema.clone(), table.clone())).or_default();
        match value_type(&data_type, &udt_name) {
            Some(r#type) => entry.push(DbColumn::new(column, r#type)),
            None => notes.info.push(format!(
                "{schema}.{table}.{column}: {data_type} is not carried"
            )),
        }
    }
    if !filtered.is_empty() {
        return Err(StorageError(format!(
            "publication `{publication}` streams {} with a row filter: rows outside it would be read once and never updated; remove the filter",
            filtered.into_iter().collect::<Vec<_>>().join(", ")
        )));
    }
    if !narrowed.is_empty() {
        return Err(StorageError(format!(
            "publication `{publication}` streams {} with a column list: clients expect every column; remove the column list",
            narrowed.into_iter().collect::<Vec<_>>().join(", ")
        )));
    }
    let mut tables = Vec::new();
    for ((schema, table), columns) in declared {
        let Some(pkey) = pkeys.get(&(schema.clone(), table.clone())) else {
            notes
                .info
                .push(format!("{schema}.{table}: no primary key, left out"));
            continue;
        };
        if pkey
            .iter()
            .any(|key| !columns.iter().any(|column| column.name == key.as_str()))
        {
            notes.info.push(format!(
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
    Ok((Catalog::new(tables), notes))
}

/// The catalog name of a table ([`TableName::of`]): bare in `public`,
/// schema-qualified elsewhere, quoted when a part holds a dot or quote.
pub fn table_name(schema: &str, table: &str) -> String {
    TableName::of(schema, table).as_str().to_owned()
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
/// carry.
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
        "bytea" => return None,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The mapping follows the protocol's: times are numbers, json and arrays are
    /// json, enums are strings, bytea is left out.
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
        assert_eq!(value_type("bytea", "bytea"), None);
        assert_eq!(table_name("public", "tickets"), "tickets");
        assert_eq!(table_name("xyne_0", "clients"), "xyne_0.clients");
        assert_eq!(table_name("public", "a.b"), "\"public\".\"a.b\"");
    }

    /// The catalog carries what the publication streams and nothing it
    /// does not: no unpublished, unlogged or keyless table, no partition
    /// of a table published through its root, no generated column; and a
    /// publication that streams a table in part is refused.
    #[tokio::test]
    async fn the_catalog_is_what_the_publication_streams() {
        let Ok(dsn) = std::env::var("STREAMGRES_PG_DSN") else {
            eprintln!("STREAMGRES_PG_DSN not set: skipped");
            return;
        };
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        let connection = tokio::spawn(connection);
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let schema = format!("sgcat{tag}");
        let [whole, filtered, narrowed, single] =
            ["whole", "filtered", "narrowed", "single"].map(|kind| format!("sgcat_{kind}_{tag}"));
        client
            .batch_execute(&format!(
                r#"CREATE SCHEMA "{schema}";
                 CREATE TABLE "{schema}".plain (
                   id int PRIMARY KEY, name text,
                   total int GENERATED ALWAYS AS (id * 2) STORED
                 );
                 CREATE TABLE "{schema}".keyless (id int);
                 CREATE UNLOGGED TABLE "{schema}".scratch (id int PRIMARY KEY);
                 CREATE TABLE "{schema}".events (id int, day date, PRIMARY KEY (id, day))
                   PARTITION BY RANGE (day);
                 CREATE TABLE "{schema}".events_jan PARTITION OF "{schema}".events
                   FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
                 CREATE PUBLICATION "{whole}" FOR TABLES IN SCHEMA "{schema}"
                   WITH (publish_via_partition_root = true);
                 CREATE PUBLICATION "{filtered}" FOR TABLE "{schema}".plain WHERE (id > 0);
                 CREATE PUBLICATION "{narrowed}" FOR TABLE "{schema}".plain (id);
                 CREATE PUBLICATION "{single}" FOR TABLE "{schema}".plain;"#
            ))
            .await
            .unwrap();
        let schemas = [schema.clone()];
        let outcome = async {
            let (catalog, notes) = load_catalog(&client, &whole, &schemas).await.unwrap();
            let mut names: Vec<_> = catalog.tables().map(|table| table.name.to_string()).collect();
            names.sort();
            assert_eq!(names, [format!("{schema}.events"), format!("{schema}.plain")]);
            let plain = catalog.table(&format!("{schema}.plain")).unwrap();
            let mut columns: Vec<_> = plain.columns.keys().map(|name| name.to_string()).collect();
            columns.sort();
            assert_eq!(columns, ["id", "name"]);
            let warned = |text: &str| notes.warnings.iter().any(|note| note.contains(text));
            let told = |text: &str| notes.info.iter().any(|note| note.contains(text));
            assert!(warned(".scratch: unlogged"), "{notes:?}");
            assert!(warned(".plain.total: a generated column"), "{notes:?}");
            assert!(told(".events_jan: a partition"), "{notes:?}");
            assert!(told(".keyless: no primary key"), "{notes:?}");

            let (catalog, notes) = load_catalog(&client, &single, &schemas).await.unwrap();
            let names: Vec<_> = catalog.tables().map(|table| table.name.to_string()).collect();
            assert_eq!(names, [format!("{schema}.plain")]);
            assert!(
                notes
                    .warnings
                    .iter()
                    .any(|note| note.contains(".events: not in publication")),
                "{notes:?}"
            );

            let error = load_catalog(&client, &filtered, &schemas).await.unwrap_err();
            assert!(error.0.contains("row filter"), "{error:?}");
            let error = load_catalog(&client, &narrowed, &schemas).await.unwrap_err();
            assert!(error.0.contains("column list"), "{error:?}");
            let error = load_catalog(&client, &format!("sgcat_missing_{tag}"), &schemas)
                .await
                .unwrap_err();
            assert!(error.0.contains("does not exist"), "{error:?}");
        };
        let outcome = futures_util::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(outcome)).await;
        client
            .batch_execute(&format!(
                r#"DROP PUBLICATION "{whole}", "{filtered}", "{narrowed}", "{single}";
                 DROP SCHEMA "{schema}" CASCADE;"#
            ))
            .await
            .unwrap();
        drop(client);
        connection.await.unwrap().unwrap();
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }
}
