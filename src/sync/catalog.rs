//! The catalog as the running server holds it: one handle, read by every
//! thread, whose content is replaced whole when a migration adds a table
//! or a column. A reader loads a snapshot for the duration of one
//! operation (one translation, one read, one flush of pokes) and never
//! sees a half-written catalog; the writer builds the next catalog from
//! the current one and swaps it in. The [`Catalog`] itself stays what it
//! is in the model, an immutable set of tables: the next one is one table
//! more or one column wider.

use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::model::{Catalog, DbColumn, DbTable};

/// The catalog every thread reads through, replaced whole by the feed
/// when the schema grows.
pub struct CatalogHandle {
    current: ArcSwap<Catalog>,
}

impl CatalogHandle {
    /// A handle holding `catalog`.
    pub fn new(catalog: Catalog) -> Self {
        CatalogHandle {
            current: ArcSwap::from_pointee(catalog),
        }
    }

    /// A handle holding a catalog already shared.
    pub fn from_arc(catalog: Arc<Catalog>) -> Self {
        CatalogHandle {
            current: ArcSwap::new(catalog),
        }
    }

    /// The catalog as of now, to keep for one operation.
    pub fn load(&self) -> Arc<Catalog> {
        self.current.load_full()
    }

    /// Whether `catalog` is the one held right now (the same allocation).
    pub fn holds(&self, catalog: &Arc<Catalog>) -> bool {
        Arc::ptr_eq(&self.current.load(), catalog)
    }

    /// Invalidate library admission even when DDL leaves the row shape unchanged.
    pub(crate) fn invalidate_admission(&self) {
        self.current.rcu(|catalog| Arc::new((**catalog).clone()));
    }

    /// Make `catalog` the one every reader loads from now on.
    pub fn swap(&self, catalog: Arc<Catalog>) {
        self.current.store(catalog);
    }
}

/// `catalog` with `table` in it, replacing a table of the same name.
pub fn with_table(catalog: &Catalog, table: DbTable) -> Catalog {
    let name = table.name.clone();
    Catalog::new(
        catalog
            .tables()
            .filter(|held| held.name != name)
            .cloned()
            .chain(std::iter::once(table)),
    )
}

/// `table` with `column` added: the same name and key, one column more.
/// The columns it had keep everything known of them (their source types,
/// collations) and their declared order.
pub fn with_column(table: &DbTable, column: DbColumn) -> DbTable {
    let mut columns: Vec<DbColumn> = table
        .declared()
        .iter()
        .filter_map(|name| table.column(name.as_str()).cloned())
        .collect();
    columns.push(DbColumn::new(column.name, column.r#type));
    DbTable::new(table.name.clone(), table.pkey.iter().cloned(), columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ValueType;

    /// A widened table's columns keep their source types and their
    /// declared order, the new one last.
    #[test]
    fn a_widened_table_keeps_what_its_columns_carry() {
        let tickets = DbTable::new(
            "tickets",
            ["id"],
            vec![
                DbColumn::new("id", ValueType::Int).with_type_oid(20),
                DbColumn::new("title", ValueType::String).with_type_oid(25),
                DbColumn::new("state", ValueType::String)
                    .with_type_oid(99_001)
                    .with_enum_type(),
            ],
        );
        let wider = with_column(&tickets, DbColumn::new("owner", ValueType::String));
        assert_eq!(wider.column("title").map(|c| c.type_oid), Some(25));
        assert!(wider.column("state").is_some_and(|c| c.enum_type));
        assert_eq!(wider.column("owner").map(|c| c.type_oid), Some(0));
        assert_eq!(
            wider
                .declared()
                .iter()
                .map(|c| c.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "title", "state", "owner"]
        );
    }

    /// A swap is seen by the next load and not by a snapshot already
    /// taken; a table added or widened leaves the others as they were.
    #[test]
    fn the_handle_swaps_whole_catalogs() {
        let tickets = DbTable::new(
            "tickets",
            ["id"],
            vec![
                DbColumn::new("id", ValueType::Int),
                DbColumn::new("status", ValueType::String),
            ],
        );
        let handle = CatalogHandle::new(Catalog::new(vec![tickets.clone()]));
        let before = handle.load();
        assert!(handle.holds(&before));
        let wider = with_column(&tickets, DbColumn::new("owner", ValueType::String));
        assert_eq!(wider.pkey, tickets.pkey);
        assert!(wider.column("owner").is_some() && wider.column("status").is_some());
        assert_eq!(
            wider
                .row_schema()
                .names()
                .iter()
                .map(|c| c.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "owner", "status"],
            "the key first, then the rest by name"
        );
        let users = DbTable::new("users", ["id"], vec![DbColumn::new("id", ValueType::Int)]);
        let next = with_table(&with_table(&before, wider), users);
        handle.swap(Arc::new(next));
        assert!(!handle.holds(&before));
        let after = handle.load();
        assert!(after.table("users").is_some());
        assert!(after.table("tickets").unwrap().column("owner").is_some());
        assert!(before.table("tickets").unwrap().column("owner").is_none());
        assert_eq!(after.tables().count(), 2);
    }
}
