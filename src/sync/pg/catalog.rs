//! The catalog read from the database itself: every table of the chosen
//! schemas that has a primary key, each column mapped onto the engine's
//! exact value type for its Postgres type and carrying the type's OID (a
//! domain's base type), so a server needs no hand-written schema. A type
//! the engine does not interpret is carried raw. Tables of the `public`
//! schema keep their bare names; any other table is named
//! `schema.table`, which is also how the change feed matches it.

use std::collections::BTreeMap;

use tokio_postgres::Client;

use super::StorageError;
use crate::model::{Catalog, Collation, DbColumn, DbTable, ValueType};

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
                    COALESCE(NULLIF(ty.typbasetype, 0), ty.oid)::int8 AS type_oid,
                    base.typtype::text AS type_class,
                    coll.collname::text AS collation,
                    COALESCE(coll.collisdeterministic, true) AS deterministic,
                    base.typelem::int8 AS element_oid
             FROM information_schema.columns c
             JOIN information_schema.tables t
               ON t.table_schema = c.table_schema AND t.table_name = c.table_name
             JOIN pg_namespace ns ON ns.nspname = c.table_schema
             JOIN pg_class rel ON rel.relnamespace = ns.oid AND rel.relname = c.table_name
             JOIN pg_attribute att ON att.attrelid = rel.oid AND att.attname = c.column_name
             JOIN pg_type ty ON ty.oid = att.atttypid
             JOIN pg_type base ON base.oid = COALESCE(NULLIF(ty.typbasetype, 0), ty.oid)
             LEFT JOIN pg_collation coll ON coll.oid = att.attcollation
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
    let mut pkeys: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for row in &keys {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        let column: String = row.get(2);
        pkeys.entry((schema, table)).or_default().push(column);
    }
    let default_collation = database_collation(client).await?;
    #[cfg(feature = "server")]
    {
        let money_scale: i32 = client
            .query_one("SELECT scale('0'::money::numeric)::int4", &[])
            .await?
            .get(0);
        if let Ok(scale) = u8::try_from(money_scale) {
            super::binary::set_money_scale(scale);
        }
    }
    let mut declared: BTreeMap<(String, String), Vec<DbColumn>> = BTreeMap::new();
    let mut notes = Vec::new();
    for row in &columns {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        let column: String = row.get(2);
        let data_type: String = row.get(3);
        let udt_name: String = row.get(4);
        let type_oid = u32::try_from(row.get::<_, i64>(5)).unwrap_or(0);
        let class: String = row.get(6);
        let r#type = if class == "e" {
            ValueType::String
        } else {
            value_type(&data_type, &udt_name)
        };
        let element_oid = u32::try_from(row.get::<_, i64>(9)).unwrap_or(0);
        let mut column = DbColumn::new(column, r#type)
            .with_type_oid(type_oid)
            .with_element_oid(element_oid);
        if let Some(name) = row.get::<_, Option<String>>(7) {
            let name = if name == "default" {
                default_collation.clone()
            } else {
                name
            };
            column = column.with_collation(Collation {
                name,
                deterministic: row.get(8),
            });
        }
        declared.entry((schema, table)).or_default().push(column);
    }
    let mut tables = Vec::new();
    for ((schema, table), columns) in declared {
        let Some(pkey) = pkeys.get(&(schema.clone(), table.clone())) else {
            notes.push(format!("{schema}.{table}: no primary key, left out"));
            continue;
        };
        if pkey.iter().any(|key| {
            columns
                .iter()
                .any(|column| column.name == key.as_str() && !keyable(&column.r#type))
        }) {
            notes.push(format!(
                "{schema}.{table}: a key column has a type the engine cannot key by, left out"
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

/// `catalog` cut down to the tables the engine can follow exactly, with a
/// note for each one left out. The database must be PostgreSQL 14 or
/// later (the feed reads values in binary format) and the publication
/// must send inserts, updates and deletes. A table is left out when the
/// publication filters its rows or columns, when it has a generated
/// column (whose values the feed does not carry), a column of a type
/// without a binary send function (which neither a binary read nor the
/// feed can give exactly), or row-level security (which the engine,
/// reading as its own role, cannot apply). A table the engine carried
/// without its changes streamed would never change.
pub async fn restrict_to_publication(
    client: &Client,
    catalog: Catalog,
    publication: &str,
) -> Result<(Catalog, Notes), StorageError> {
    let version: i32 = client
        .query_one("SELECT current_setting('server_version_num')::int4", &[])
        .await?
        .get(0);
    if version < 140_000 {
        return Err(StorageError(format!(
            "PostgreSQL 14 or later is needed, for values in binary format from the feed; the server is {version}"
        )));
    }
    let Some(row) = client
        .query_opt(
            "SELECT pubinsert AND pubupdate AND pubdelete FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await?
    else {
        return Err(StorageError(format!(
            "the publication `{publication}` does not exist"
        )));
    };
    if !row.get::<_, bool>(0) {
        return Err(StorageError(format!(
            "the publication `{publication}` does not publish every insert, update and delete"
        )));
    }
    let whole = if version >= 150_000 {
        "pt.rowfilter IS NULL AND cardinality(pt.attnames) = (
             SELECT count(*) FROM pg_attribute a
             WHERE a.attrelid = format('%I.%I', pt.schemaname, pt.tablename)::regclass
               AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = '')"
    } else {
        "true"
    };
    let rows = client
        .query(
            &format!(
                "SELECT pt.schemaname::text, pt.tablename::text, {whole},
                        EXISTS (SELECT 1 FROM pg_attribute a
                                WHERE a.attrelid = format('%I.%I', pt.schemaname, pt.tablename)::regclass
                                  AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated <> ''),
                        EXISTS (SELECT 1 FROM pg_attribute a
                                JOIN pg_type t ON t.oid = a.atttypid
                                LEFT JOIN pg_type e ON e.oid = t.typelem AND t.typcategory = 'A'
                                WHERE a.attrelid = format('%I.%I', pt.schemaname, pt.tablename)::regclass
                                  AND a.attnum > 0 AND NOT a.attisdropped
                                  AND (t.typsend::oid = 0 OR e.typsend::oid = 0)),
                        c.relrowsecurity
                 FROM pg_publication_tables pt
                 JOIN pg_class c ON c.oid = format('%I.%I', pt.schemaname, pt.tablename)::regclass
                 WHERE pt.pubname = $1"
            ),
            &[&publication],
        )
        .await?;
    let mut published = std::collections::HashSet::new();
    let mut notes = Vec::new();
    for row in rows {
        let name = table_name(&row.get::<_, String>(0), &row.get::<_, String>(1));
        let left_out = if row.get::<_, bool>(3) {
            Some("has a generated column the feed does not carry")
        } else if !row.get::<_, bool>(2) {
            Some("is published with a row filter or a column list")
        } else if row.get::<_, bool>(4) {
            Some("has a column whose type has no binary send function")
        } else if row.get::<_, bool>(5) {
            Some("has row-level security, which the engine cannot apply")
        } else {
            None
        };
        match left_out {
            Some(reason) => notes.push(format!("{name}: {reason}, left out")),
            None => {
                published.insert(name);
            }
        }
    }
    let tables: Vec<DbTable> = catalog
        .tables()
        .filter(|table| {
            let kept = published.contains(table.name.as_str());
            if !kept
                && !notes
                    .iter()
                    .any(|note| note.starts_with(&format!("{}:", table.name)))
            {
                notes.push(format!(
                    "{}: not in the publication `{publication}`, left out",
                    table.name
                ));
            }
            kept
        })
        .cloned()
        .collect();
    Ok((Catalog::new(tables), notes))
}

/// The name of the collation `default` stands for in this database: its
/// libc locale, or for an ICU or builtin default (PostgreSQL 15 and 17)
/// that provider's locale, prefixed `icu:` for ICU.
async fn database_collation(client: &Client) -> Result<String, StorageError> {
    let version: i32 = client
        .query_one("SELECT current_setting('server_version_num')::int4", &[])
        .await?
        .get(0);
    let sql = match version {
        170_000.. => {
            "SELECT datcollate::text, datlocprovider::text, COALESCE(datlocale, '')::text
             FROM pg_database WHERE datname = current_database()"
        }
        150_000.. => {
            "SELECT datcollate::text, datlocprovider::text, COALESCE(daticulocale, '')::text
             FROM pg_database WHERE datname = current_database()"
        }
        _ => {
            "SELECT datcollate::text, 'c'::text, ''::text
             FROM pg_database WHERE datname = current_database()"
        }
    };
    let row = client.query_one(sql, &[]).await?;
    let libc: String = row.get(0);
    let provider: String = row.get(1);
    let locale: String = row.get(2);
    Ok(match provider.as_str() {
        "i" => format!("icu:{locale}"),
        "b" => locale,
        _ => libc,
    })
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

/// Whether rows can be keyed by a column of `declared`: its values must
/// compare by value and be writable as SQL literals.
pub fn keyable(declared: &ValueType) -> bool {
    !matches!(
        declared,
        ValueType::Raw | ValueType::List(_) | ValueType::Map(_, _) | ValueType::Json
    )
}

/// The engine's type for a Postgres column, from `information_schema`'s
/// `data_type` and `udt_name`; a type the engine does not interpret is
/// [`ValueType::Raw`]. An enum is told apart by its type class, which the
/// caller checks first.
pub fn value_type(data_type: &str, udt_name: &str) -> ValueType {
    match data_type {
        "ARRAY" => {
            let element = udt_name.strip_prefix('_').unwrap_or(udt_name);
            ValueType::List(Box::new(scalar_type(element).unwrap_or(ValueType::Raw)))
        }
        "USER-DEFINED" => scalar_type(udt_name).unwrap_or(ValueType::Raw),
        other => scalar_type(other)
            .or_else(|| scalar_type(udt_name))
            .unwrap_or(ValueType::Raw),
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
        "real" | "double precision" | "float4" | "float8" => ValueType::Float,
        "numeric" | "decimal" => ValueType::Numeric,
        "boolean" | "bool" => ValueType::Bool,
        "json" | "jsonb" => ValueType::Json,
        "timestamp without time zone"
        | "timestamp with time zone"
        | "timestamp"
        | "timestamptz" => ValueType::Timestamp,
        "date" => ValueType::Day,
        "time without time zone" | "time" => ValueType::Time,
        "bytea" => ValueType::Bytes,
        "text" | "character varying" | "character" | "varchar" | "bpchar" | "char" | "uuid"
        | "citext" | "name" | "xml" => ValueType::String,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each Postgres type maps to the engine's exact type for it, and any
    /// type the engine does not interpret is carried raw.
    #[test]
    fn types_map_to_exact_values() {
        assert_eq!(
            value_type("timestamp without time zone", "timestamp"),
            ValueType::Timestamp
        );
        assert_eq!(value_type("date", "date"), ValueType::Day);
        assert_eq!(value_type("jsonb", "jsonb"), ValueType::Json);
        assert_eq!(
            value_type("ARRAY", "_text"),
            ValueType::List(Box::new(ValueType::String))
        );
        assert_eq!(value_type("USER-DEFINED", "citext"), ValueType::String);
        assert_eq!(value_type("USER-DEFINED", "hstore"), ValueType::Raw);
        assert_eq!(value_type("bigint", "int8"), ValueType::Int);
        assert_eq!(value_type("numeric", "numeric"), ValueType::Numeric);
        assert_eq!(value_type("money", "money"), ValueType::Raw);
        assert_eq!(value_type("interval", "interval"), ValueType::Raw);
        assert_eq!(value_type("bytea", "bytea"), ValueType::Bytes);
        assert!(!keyable(&ValueType::Raw));
        assert!(keyable(&ValueType::Numeric));
        assert_eq!(table_name("public", "tickets"), "tickets");
        assert_eq!(table_name("xyne_0", "clients"), "xyne_0.clients");
    }
}
