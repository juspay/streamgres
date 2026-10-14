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
    Array { element: u32, element_kind: Box<TypeKind> },
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
    fn table_types(&mut self, table: &str) -> QueryResult<TableTypes>;
}

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
    #[diesel(sql_type = Nullable<Text>)]
    elemtyptype: Option<String>,
}

impl<C> TypeLookup for C
where
    C: LoadConnection<DefaultLoadingMode> + Connection<Backend = Pg>,
{
    fn table_types(&mut self, table: &str) -> QueryResult<TableTypes> {
        let attributes = sql_query(
            "SELECT a.attname, a.atttypid, a.atttypmod, t.typtype::text AS typtype, \
             t.typelem, et.typtype::text AS elemtyptype \
             FROM pg_attribute a \
             JOIN pg_type t ON t.oid = a.atttypid \
             LEFT JOIN pg_type et ON et.oid = t.typelem AND t.typelem <> 0 \
             WHERE a.attrelid = quote_ident($1)::regclass AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
        )
        .bind::<Text, _>(table)
        .load::<Attribute>(self)?;
        let columns = attributes
            .into_iter()
            .map(|attribute| {
                let kind = match attribute.typtype.as_str() {
                    "b" if attribute.typelem != 0 && attribute.elemtyptype.is_some() => {
                        let element_kind = match attribute.elemtyptype.as_deref() {
                            Some("b") => TypeKind::Base,
                            Some("e") => TypeKind::Enum,
                            _ => TypeKind::Other,
                        };
                        TypeKind::Array {
                            element: attribute.typelem,
                            element_kind: Box::new(element_kind),
                        }
                    }
                    "b" => TypeKind::Base,
                    "e" => TypeKind::Enum,
                    _ => TypeKind::Other,
                };
                ColumnType {
                    name: attribute.attname,
                    oid: attribute.atttypid,
                    typmod: attribute.atttypmod,
                    kind,
                }
            })
            .collect();
        Ok(TableTypes { columns })
    }
}
