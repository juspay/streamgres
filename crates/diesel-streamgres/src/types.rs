//! What Postgres says a table's columns are, for encoding rows the engine
//! ships into the bytes Diesel decodes.
//!
//! The engine knows its own coarse types (string, int, float, time);
//! Diesel decodes Postgres's binary format, where an `int4` is four bytes
//! and an `int8` eight. So the exact column types come from Postgres
//! itself, once per table, through the connection the read came in on.

use diesel::connection::{DefaultLoadingMode, LoadConnection};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{Integer, Nullable, Oid, Text};
use std::collections::HashMap;

/// One column of a table as Postgres declares it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnType {
    pub name: String,
    pub oid: u32,
    /// The type modifier: a `timestamp(3)`'s precision, a `varchar(64)`'s
    /// length plus four, `-1` when none.
    pub typmod: i32,
    pub kind: TypeKind,
}

/// How a type's values are laid out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeKind {
    /// A built-in scalar, by OID.
    Base,
    /// A user enum: its labels travel as text.
    Enum,
    /// A one-dimensional array of `element`, which is `Base` or `Enum`.
    Array {
        element: u32,
        element_kind: Box<TypeKind>,
    },
    /// Composite, range, domain, pseudo: not encoded.
    Other,
}

/// A table's columns in declaration order, which is also the order of
/// `SELECT *`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableTypes {
    pub columns: Vec<ColumnType>,
}

impl TableTypes {
    /// The column named `name`, with its position.
    pub fn column(&self, name: &str) -> Option<(usize, &ColumnType)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == name)
    }
}

/// Where the engine client asks for a table's types: the connection the
/// read is on.
pub trait TypeLookup {
    /// The column types of `table`, in declaration order.
    fn table_types(&mut self, table: &str) -> QueryResult<TableTypes>;
}

/// One column as `pg_attribute` and `pg_type` describe it.
#[derive(QueryableByName)]
struct Attribute {
    #[diesel(sql_type = Text)]
    attname: String,
    #[diesel(sql_type = Oid)]
    atttypid: u32,
    #[diesel(sql_type = Integer)]
    atttypmod: i32,
    #[diesel(sql_type = Text)]
    typtype: String,
    #[diesel(sql_type = Oid)]
    typelem: u32,
    #[diesel(sql_type = Text)]
    typcategory: String,
    #[diesel(sql_type = Nullable<Text>)]
    elemtyptype: Option<String>,
}

impl Attribute {
    /// The column's type. An array is a type of the array category: a
    /// fixed-width type like `name` or `point` also names an element
    /// type, and is not one.
    fn into_column(self) -> ColumnType {
        let kind = match self.typtype.as_str() {
            "b" if self.typcategory == "A" && self.elemtyptype.is_some() => {
                let element_kind = match self.elemtyptype.as_deref() {
                    Some("b") => TypeKind::Base,
                    Some("e") => TypeKind::Enum,
                    _ => TypeKind::Other,
                };
                TypeKind::Array {
                    element: self.typelem,
                    element_kind: Box::new(element_kind),
                }
            }
            "b" => TypeKind::Base,
            "e" => TypeKind::Enum,
            _ => TypeKind::Other,
        };
        ColumnType {
            name: self.attname,
            oid: self.atttypid,
            typmod: self.atttypmod,
            kind,
        }
    }
}

const ATTRIBUTE_COLUMNS: &str = "a.attname, a.atttypid, a.atttypmod, t.typtype::text AS typtype, \
     t.typelem, t.typcategory::text AS typcategory, et.typtype::text AS elemtyptype";

impl<C> TypeLookup for C
where
    C: LoadConnection<DefaultLoadingMode> + Connection<Backend = Pg>,
{
    /// Reads the table's columns from the catalog on this connection.
    fn table_types(&mut self, table: &str) -> QueryResult<TableTypes> {
        let attributes = sql_query(format!(
            "SELECT {ATTRIBUTE_COLUMNS} \
             FROM pg_attribute a \
             JOIN pg_type t ON t.oid = a.atttypid \
             LEFT JOIN pg_type et ON et.oid = t.typelem AND t.typelem <> 0 \
             WHERE a.attrelid = quote_ident($1)::regclass AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum"
        ))
        .bind::<Text, _>(table)
        .load::<Attribute>(self)?;
        Ok(TableTypes {
            columns: attributes.into_iter().map(Attribute::into_column).collect(),
        })
    }
}

/// One column of one table, from [`ALL_TABLES_SQL`]: how a connection
/// that cannot run a query per table (an async one, whose futures are
/// built before the engine names the tables) learns every table's types
/// at once.
#[derive(QueryableByName)]
pub struct TableAttribute {
    #[diesel(sql_type = Text)]
    relname: String,
    #[diesel(embed)]
    attribute: Attribute,
}

/// Every column of every table in the search path, tables in no
/// particular order, columns in declaration order. A table name present
/// in several schemas resolves to the first in the search path, as a
/// bare name does.
pub const ALL_TABLES_SQL: &str = "SELECT c.relname, a.attname, a.atttypid, a.atttypmod, \
     t.typtype::text AS typtype, t.typelem, t.typcategory::text AS typcategory, \
     et.typtype::text AS elemtyptype \
     FROM pg_class c \
     JOIN pg_namespace n ON n.oid = c.relnamespace \
     JOIN pg_attribute a ON a.attrelid = c.oid \
     JOIN pg_type t ON t.oid = a.atttypid \
     LEFT JOIN pg_type et ON et.oid = t.typelem AND t.typelem <> 0 \
     WHERE c.relkind IN ('r', 'p', 'v', 'm') \
       AND n.nspname = ANY(current_schemas(false)) \
       AND a.attnum > 0 AND NOT a.attisdropped \
     ORDER BY array_position(current_schemas(false), n.nspname), c.relname, a.attnum";

/// Groups [`ALL_TABLES_SQL`]'s rows by table. Rows of one table are
/// contiguous; a later run under a name already taken is the same name
/// in a later schema, which a bare name does not mean, so it is skipped
/// whole.
pub fn all_table_types(rows: Vec<TableAttribute>) -> std::collections::HashMap<String, TableTypes> {
    let mut tables: HashMap<String, TableTypes> = HashMap::new();
    let mut current: Option<(String, bool)> = None;
    for row in rows {
        let skip = match &current {
            Some((name, skip)) if *name == row.relname => *skip,
            _ => {
                let skip = tables.contains_key(&row.relname);
                current = Some((row.relname.clone(), skip));
                skip
            }
        };
        if skip {
            continue;
        }
        tables
            .entry(row.relname)
            .or_insert_with(|| TableTypes {
                columns: Vec::new(),
            })
            .columns
            .push(row.attribute.into_column());
    }
    tables
}
