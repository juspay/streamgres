//! The catalog read from the database itself: every table of the chosen
//! schemas that has a primary key, each column mapped onto the engine's
//! exact value type for its Postgres type and carrying the type's OID (a
//! domain's underlying base type), so a server needs no hand-written schema.
//! A type the engine does not interpret is carried raw. Tables of the
//! `public` schema keep their bare names; any other table is named
//! `schema.table`, which is also how the change feed matches it.
//!
//! Columns are described by [`PgType`], the one mapper startup and
//! schema-change messages both use, so a domain cannot be classified one
//! way when the catalog loads and another when a later migration arrives.

use std::collections::BTreeMap;

use tokio_postgres::Client;

use super::StorageError;
use crate::model::{Catalog, DbColumn, DbTable, ValueType};

/// What the load left out and why, one line per table or column.
pub type Notes = Vec<String>;

/// A column's type after every domain has been followed to its final,
/// non-domain base — what PostgreSQL itself reports in a result's
/// description, and what binary decoding dispatches on.
#[derive(Debug, Clone, Copy)]
pub struct PgType<'a> {
    /// `pg_type.typname` of the base (`int8`, `_mood`, `timestamptz`, ...).
    pub base_name: &'a str,
    /// The base's type class: `b` base, `e` enum, and so on.
    pub base_class: &'a str,
    /// Whether the base's binary output is UTF-8 text (`typsend` is
    /// `textsend`): an unknown type like `hstore` or `citext` still reads
    /// as text then, where `tsvector` or `bytea` must stay raw.
    pub base_sends_text: bool,
    /// The base type's OID.
    pub base_oid: u32,
    /// For an array base, its element's `typname`; empty otherwise.
    pub element_name: &'a str,
    /// For an array base, its element's type class; empty otherwise.
    pub element_class: &'a str,
    /// For an array base, whether its element's binary output is text.
    pub element_sends_text: bool,
    /// The array's element OID; 0 for a non-array.
    pub element_oid: u32,
    /// Whether the base is an array, independent of a known element OID.
    pub is_array: bool,
    /// Whether the column's declared type is a domain.
    pub is_domain: bool,
}

impl PgType<'_> {
    /// The engine's type for this column. Domains contribute only their
    /// final base, so `account_id` over `cents` over `bigint` is an `Int`.
    pub fn value_type(&self) -> ValueType {
        let scalar = |name: &str, class: &str, sends_text: bool| {
            if class == "e" {
                ValueType::String
            } else if let Some(known) = scalar_type(name) {
                known
            } else if sends_text {
                // A type the engine does not know by name whose binary form
                // is UTF-8 text reads as a string, as it did before; framing
                // it as `Raw` would project it as `null`.
                ValueType::String
            } else {
                ValueType::Raw
            }
        };
        if self.is_array {
            ValueType::List(Box::new(scalar(
                self.element_name,
                self.element_class,
                self.element_sends_text,
            )))
        } else {
            scalar(self.base_name, self.base_class, self.base_sends_text)
        }
    }

    /// This type as a catalog column, with the OIDs a binary decode and a
    /// result-type check need.
    pub fn to_db_column(&self, name: &str) -> DbColumn {
        DbColumn::new(name, self.value_type())
            .with_type_oid(self.base_oid)
            .with_element_oid(self.element_oid)
            .with_is_domain(self.is_domain)
    }
}

/// Read the catalog of `schemas` through `client`.
pub async fn load_catalog(
    client: &Client,
    schemas: &[String],
) -> Result<(Catalog, Notes), StorageError> {
    let columns = client
        .query(
            "WITH RECURSIVE type_chain(oid, base_oid, typtype, typbasetype, depth) AS (
                 SELECT oid, oid, typtype, typbasetype, 0 FROM pg_type
                 UNION ALL
                 SELECT r.oid, base.oid, base.typtype, base.typbasetype, r.depth + 1
                 FROM type_chain r JOIN pg_type base ON base.oid = r.typbasetype
                 WHERE r.typtype = 'd' AND r.depth < 64
             ), leaf_types AS (
                 SELECT DISTINCT ON (oid) oid, base_oid
                 FROM type_chain WHERE typtype != 'd'
                 ORDER BY oid, depth DESC
             )
             SELECT c.table_schema, c.table_name, c.column_name,
                    base.typname, base.typtype::text AS base_class,
                    base.oid::int8 AS base_oid,
                    COALESCE(elem.typname, '') AS element_name,
                    COALESCE(elem.typtype::text, '') AS element_class,
                    (base.typsend = 'textsend'::regproc) AS sends_text,
                    (elem.typsend = 'textsend'::regproc) AS element_sends_text,
                    base.typelem::int8 AS element_oid,
                    base.typelem <> 0 AND base.typlen = -1 AS is_array,
                    declared.typtype = 'd' AS is_domain
             FROM information_schema.columns c
             JOIN information_schema.tables t
               ON t.table_schema = c.table_schema AND t.table_name = c.table_name
             JOIN pg_namespace ns ON ns.nspname = c.table_schema
             JOIN pg_class rel ON rel.relnamespace = ns.oid AND rel.relname = c.table_name
             JOIN pg_attribute att ON att.attrelid = rel.oid AND att.attname = c.column_name
             JOIN pg_type declared ON declared.oid = att.atttypid
             JOIN leaf_types leaf ON leaf.oid = declared.oid
             JOIN pg_type base ON base.oid = leaf.base_oid
             LEFT JOIN pg_type elem ON elem.oid = base.typelem
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
    let money_scale: i32 = client
        .query_one("SELECT scale('0'::money::numeric)::int4", &[])
        .await?
        .get(0);
    let money_scale = u8::try_from(money_scale).unwrap_or(2);
    let mut pkeys: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
    for row in &keys {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        let column: String = row.get(2);
        pkeys.entry((schema, table)).or_default().push(column);
    }
    let mut declared: BTreeMap<(String, String), Vec<DbColumn>> = BTreeMap::new();
    let mut notes = Vec::new();
    for row in &columns {
        let schema: String = row.get(0);
        let table: String = row.get(1);
        let column: String = row.get(2);
        let base_name: String = row.get(3);
        let base_class: String = row.get(4);
        let base_oid = u32::try_from(row.get::<_, i64>(5)).unwrap_or(0);
        let element_name: String = row.get(6);
        let element_class: String = row.get(7);
        let base_sends_text: bool = row.get(8);
        let element_sends_text: bool = row.get::<_, Option<bool>>(9).unwrap_or(false);
        declared.entry((schema, table)).or_default().push(
            PgType {
                base_name: &base_name,
                base_class: &base_class,
                base_sends_text,
                base_oid,
                element_name: &element_name,
                element_class: &element_class,
                element_sends_text,
                element_oid: u32::try_from(row.get::<_, i64>(10)).unwrap_or(0),
                // `typelem` alone is not array-ness: `name`, `point` and
                // `oidvector` carry a nonzero `typelem` and are scalars.
                // PostgreSQL marks a true array with `typlen = -1`.
                is_array: row.get(11),
                is_domain: row.get(12),
            }
            .to_db_column(&column),
        );
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
    Ok((Catalog::new(tables).with_money_scale(money_scale), notes))
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
    crate::model::supports(crate::model::Capability::Key, declared)
}

/// The engine's type for a Postgres column, from `information_schema`'s
/// `data_type` and `udt_name`; a type the engine does not interpret is
/// [`ValueType::Raw`]. An enum is told apart by its type class, which the
/// caller checks first.
///
/// Schema-change messages written before the trigger reported a column's
/// underlying type are still read this way; new messages go through
/// [`PgType`] instead.
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

    /// The shared mapper classifies by the final base type: a domain and a
    /// domain over a domain both read as their underlying built-in, while
    /// an enum base keeps its labels.
    #[test]
    fn the_mapper_uses_the_final_base_type() {
        let plain = PgType {
            base_name: "int8",
            base_class: "b",
            base_sends_text: false,
            base_oid: 20,
            element_name: "",
            element_class: "",
            element_sends_text: false,
            element_oid: 0,
            is_array: false,
            is_domain: true,
        };
        assert_eq!(plain.value_type(), ValueType::Int);
        assert_eq!(plain.to_db_column("id").type_oid, 20);
        assert!(plain.to_db_column("id").is_domain);

        let enum_base = PgType {
            base_name: "mood",
            base_class: "e",
            base_sends_text: false,
            base_oid: 40_000,
            element_name: "",
            element_class: "",
            element_sends_text: false,
            element_oid: 0,
            is_array: false,
            is_domain: true,
        };
        assert_eq!(enum_base.value_type(), ValueType::String);

        let enum_array = PgType {
            base_name: "_mood",
            base_class: "b",
            base_sends_text: false,
            base_oid: 40_001,
            element_name: "mood",
            element_class: "e",
            element_sends_text: false,
            element_oid: 40_000,
            is_array: true,
            is_domain: true,
        };
        assert_eq!(
            enum_array.value_type(),
            ValueType::List(Box::new(ValueType::String))
        );
    }
}
