//! The rules a publication must meet for the change feed to follow the
//! tables it serves, in one place: setup and the catalog load check them
//! before the server starts, and the schema-change follower checks them
//! on every change to the publication while it runs, so a publication is
//! refused or stops the server for the same reasons, with the same words.
//!
//! - It publishes inserts, updates, deletes and truncates: a kind it
//!   leaves out would never reach clients.
//! - It publishes partitioned tables through their root
//!   (`publish_via_partition_root`): the catalog serves a partitioned
//!   table under its root's name, so its changes must arrive under it.
//! - It streams no served table with a row filter: rows outside the
//!   filter would be read once and never updated.
//! - It sends every column of each served table (generated columns
//!   aside, which PostgreSQL sends only when asked): a column list that
//!   leaves one out, from the start or once a column is added to the
//!   table, would keep it from clients.

use tokio_postgres::GenericClient;

use super::sql::quote_ident;
use crate::model::TableName;

/// What the rules look at in one publication: the kinds of change it
/// publishes, whether it publishes partitions through their root, and
/// the served tables it streams with a row filter, by catalog name, with
/// the filter as PostgreSQL prints it, and the served tables whose columns
/// it leaves out, with those columns in table order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationShape {
    pub name: String,
    pub insert: bool,
    pub update: bool,
    pub delete: bool,
    pub truncate: bool,
    pub via_root: bool,
    pub filtered: Vec<(TableName, String)>,
    pub left_out: Vec<(TableName, Vec<String>)>,
}

/// When the rules are checked, which only changes how a broken rule is
/// told: before the server starts the publication *is* broken; while it
/// runs it has *just* been changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum When {
    Startup,
    Live,
}

impl PublicationShape {
    /// The shape of `publication` as `client` sees it, with the filtered
    /// and the narrowed tables of `schemas`; `None` when the publication
    /// does not exist.
    pub async fn read(
        client: &impl GenericClient,
        publication: &str,
        schemas: &[String],
    ) -> Result<Option<PublicationShape>, tokio_postgres::Error> {
        let Some(flags) = client
            .query_opt(
                "SELECT pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot
                 FROM pg_publication WHERE pubname = $1",
                &[&publication],
            )
            .await?
        else {
            return Ok(None);
        };
        let filtered = client
            .query(
                "SELECT schemaname::text, tablename::text, rowfilter
                 FROM pg_publication_tables
                 WHERE pubname = $1 AND schemaname = ANY ($2) AND rowfilter IS NOT NULL
                 ORDER BY schemaname, tablename",
                &[&publication, &schemas],
            )
            .await?
            .into_iter()
            .map(|row| {
                (
                    TableName::of(row.get(0), row.get(1)),
                    row.get::<_, String>(2),
                )
            })
            .collect();
        let left_out = client
            .query(
                "SELECT p.schemaname::text, p.tablename::text,
                        array_agg(a.attname::text ORDER BY a.attnum)
                 FROM pg_publication_tables p
                 JOIN pg_namespace n ON n.nspname = p.schemaname
                 JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = p.tablename
                 JOIN pg_attribute a ON a.attrelid = c.oid
                 WHERE p.pubname = $1 AND p.schemaname = ANY ($2)
                   AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
                   AND NOT (a.attname = ANY (p.attnames))
                 GROUP BY p.schemaname, p.tablename
                 ORDER BY p.schemaname, p.tablename",
                &[&publication, &schemas],
            )
            .await?
            .into_iter()
            .map(|row| (TableName::of(row.get(0), row.get(1)), row.get(2)))
            .collect();
        Ok(Some(PublicationShape {
            name: publication.to_owned(),
            insert: flags.get(0),
            update: flags.get(1),
            delete: flags.get(2),
            truncate: flags.get(3),
            via_root: flags.get(4),
            filtered,
            left_out,
        }))
    }

    /// Why the change feed cannot follow the served tables through this
    /// publication, with what to do about it; `Ok` when it can.
    pub fn check(&self, when: When) -> Result<(), String> {
        let name = &self.name;
        let quoted = quote_ident(name);
        let missing: Vec<&str> = [
            ("insert", self.insert),
            ("update", self.update),
            ("delete", self.delete),
            ("truncate", self.truncate),
        ]
        .into_iter()
        .filter(|(_, on)| !on)
        .map(|(kind, _)| kind)
        .collect();
        let (publishes, has, leaves) = match when {
            When::Startup => ("does not publish", "has", "leaves out"),
            When::Live => ("no longer publishes", "now has", "now leaves out"),
        };
        if !missing.is_empty() {
            return Err(format!(
                "publication `{name}` {publishes} {}, so those changes would never reach clients; run on the primary: ALTER PUBLICATION {quoted} SET (publish = 'insert, update, delete, truncate')",
                missing.join(", ")
            ));
        }
        if !self.via_root {
            return Err(format!(
                "publication `{name}` {publishes} partitioned tables through their root, so their changes would arrive under the partitions' names; run on the primary: ALTER PUBLICATION {quoted} SET (publish_via_partition_root = true)"
            ));
        }
        if !self.filtered.is_empty() {
            let tables: Vec<String> = self
                .filtered
                .iter()
                .map(|(table, filter)| format!("`{table}` {filter}"))
                .collect();
            return Err(format!(
                "publication `{name}` {has} a row filter on {}: rows outside it would be read once and never updated; remove the filter on the primary",
                listed(&tables)
            ));
        }
        if !self.left_out.is_empty() {
            let tables: Vec<String> = self
                .left_out
                .iter()
                .map(|(table, columns)| format!("`{table}` ({})", columns.join(", ")))
                .collect();
            return Err(format!(
                "publication `{name}` {leaves} columns of {}: they would never reach clients; on the primary, drop each such table from the publication and add it back without a column list",
                listed(&tables)
            ));
        }
        Ok(())
    }
}

/// `a`, `a and b`, `a, b and c`.
fn listed(items: &[String]) -> String {
    match items {
        [] => String::new(),
        [only] => only.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A publication that meets every rule.
    fn sound() -> PublicationShape {
        PublicationShape {
            name: "my pub".to_owned(),
            insert: true,
            update: true,
            delete: true,
            truncate: true,
            via_root: true,
            filtered: Vec::new(),
            left_out: Vec::new(),
        }
    }

    #[test]
    fn a_publication_that_sends_everything_through_the_root_passes() {
        assert_eq!(sound().check(When::Startup), Ok(()));
        assert_eq!(sound().check(When::Live), Ok(()));
    }

    /// Each kind of change left out is named, with the command that
    /// publishes every kind again; before the server starts the
    /// publication does not publish it, while it runs it no longer does.
    #[test]
    fn a_kind_of_change_left_out_is_named_with_the_fix() {
        let shape = PublicationShape {
            update: false,
            delete: false,
            ..sound()
        };
        let startup = shape.check(When::Startup).unwrap_err();
        assert_eq!(
            startup,
            "publication `my pub` does not publish update, delete, so those changes would never reach clients; run on the primary: ALTER PUBLICATION \"my pub\" SET (publish = 'insert, update, delete, truncate')"
        );
        let live = shape.check(When::Live).unwrap_err();
        assert!(live.starts_with("publication `my pub` no longer publishes update, delete, so"));
        for kind in ["insert", "truncate"] {
            let shape = PublicationShape {
                insert: kind != "insert",
                truncate: kind != "truncate",
                ..sound()
            };
            let error = shape.check(When::Startup).unwrap_err();
            assert!(
                error.contains(&format!("does not publish {kind},")),
                "{error}"
            );
        }
    }

    #[test]
    fn partitions_under_their_own_names_are_refused() {
        let shape = PublicationShape {
            via_root: false,
            ..sound()
        };
        assert_eq!(
            shape.check(When::Startup).unwrap_err(),
            "publication `my pub` does not publish partitioned tables through their root, so their changes would arrive under the partitions' names; run on the primary: ALTER PUBLICATION \"my pub\" SET (publish_via_partition_root = true)"
        );
        assert!(
            shape
                .check(When::Live)
                .unwrap_err()
                .contains("no longer publishes partitioned tables through their root")
        );
    }

    /// Every filtered served table is named with its filter.
    #[test]
    fn every_filtered_table_is_named_with_its_filter() {
        let shape = PublicationShape {
            filtered: vec![
                (TableName::from("tickets"), "(points > 0)".to_owned()),
                (TableName::from("billing.invoices"), "(paid)".to_owned()),
                (TableName::from("users"), "(id > 0)".to_owned()),
            ],
            ..sound()
        };
        assert_eq!(
            shape.check(When::Startup).unwrap_err(),
            "publication `my pub` has a row filter on `tickets` (points > 0), `billing.invoices` (paid) and `users` (id > 0): rows outside it would be read once and never updated; remove the filter on the primary"
        );
        let one = PublicationShape {
            filtered: vec![(TableName::from("tickets"), "(points > 0)".to_owned())],
            ..sound()
        };
        assert_eq!(
            one.check(When::Live).unwrap_err(),
            "publication `my pub` now has a row filter on `tickets` (points > 0): rows outside it would be read once and never updated; remove the filter on the primary"
        );
    }

    /// Every served table whose columns the publication leaves out is
    /// named with those columns.
    #[test]
    fn every_column_left_out_is_named_with_its_table() {
        let shape = PublicationShape {
            left_out: vec![
                (TableName::from("tickets"), vec!["description".to_owned()]),
                (
                    TableName::from("billing.invoices"),
                    vec!["paid".to_owned(), "due".to_owned()],
                ),
            ],
            ..sound()
        };
        assert_eq!(
            shape.check(When::Startup).unwrap_err(),
            "publication `my pub` leaves out columns of `tickets` (description) and `billing.invoices` (paid, due): they would never reach clients; on the primary, drop each such table from the publication and add it back without a column list"
        );
        assert!(
            shape
                .check(When::Live)
                .unwrap_err()
                .starts_with("publication `my pub` now leaves out columns of `tickets` (description) and")
        );
    }
}
