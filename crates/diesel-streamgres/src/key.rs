//! What identifies a read: its SQL and its bind values.
//!
//! Diesel's own statement cache keys a query by the `TypeId` of its shape,
//! which is the same for `WHERE key = 'a'` and `WHERE key = 'b'`. A result
//! cache needs the values too, so the key is the rendered SQL (with `$n`
//! placeholders) and every bind as Postgres received it: its type OID and
//! its bytes. Two reads with equal keys get equal rows.

use diesel::QueryResult;
use diesel::pg::{Pg, PgMetadataLookup, PgQueryBuilder};
use diesel::query_builder::{QueryBuilder, QueryFragment, bind_collector::RawBytesBindCollector};

use crate::route::Route;

/// One bind parameter as the wire carries it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Bind {
    pub type_oid: u32,
    /// `None` is SQL `NULL`.
    pub bytes: Option<Vec<u8>>,
}

/// The identity of one read.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryKey {
    pub sql: String,
    pub binds: Vec<Bind>,
}

fn starts_with_word(text: &str, word: &str) -> bool {
    text.get(..word.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(word))
        && text[word.len()..]
            .chars()
            .next()
            .is_none_or(|next| !next.is_alphanumeric() && next != '_')
}

/// Whether `word` occurs in `sql` as a whole word, case-insensitively.
fn has_word(sql: &str, word: &str) -> bool {
    sql.split(|c: char| !c.is_alphanumeric() && c != '_')
        .any(|token| token.eq_ignore_ascii_case(word))
}

impl QueryKey {
    /// Renders a query the way the connection is about to send it. The
    /// lookup resolves custom types' OIDs, so it is the connection itself.
    pub fn of<T>(query: &T, lookup: &mut (dyn PgMetadataLookup + 'static)) -> QueryResult<QueryKey>
    where
        T: QueryFragment<Pg>,
    {
        let mut builder = PgQueryBuilder::default();
        query.to_sql(&mut builder, &Pg)?;
        let sql = builder.finish();

        let mut collector = RawBytesBindCollector::<Pg>::new();
        query.collect_binds(&mut collector, lookup, &Pg)?;
        let binds = collector
            .metadata
            .into_iter()
            .zip(collector.binds)
            .map(|(metadata, bytes)| Bind {
                type_oid: metadata.oid().unwrap_or(0),
                bytes,
            })
            .collect();
        Ok(QueryKey { sql, binds })
    }

    /// Splits a leading route marker off the SQL, if the query was flagged
    /// with [`crate::RouteDsl::via`]. The key, and what the engine sees,
    /// is the SQL without it.
    pub fn take_route(&mut self) -> Option<Route> {
        let (route, rest) = Route::strip(&self.sql)?;
        self.sql = rest.to_string();
        Some(route)
    }

    /// Whether the statement reads rather than writes: a `SELECT`, possibly
    /// parenthesised, or a `WITH` whose common table expressions do not
    /// modify data. Diesel sends `INSERT ... RETURNING` through the same
    /// `load` path as a `SELECT`, and those must never be cached.
    pub fn is_select(&self) -> bool {
        let head = self
            .sql
            .trim_start_matches(|c: char| c.is_whitespace() || c == '(');
        if starts_with_word(head, "SELECT") {
            return true;
        }
        starts_with_word(head, "WITH")
            && !["INSERT", "UPDATE", "DELETE", "MERGE"]
                .iter()
                .any(|word| has_word(&self.sql, word))
    }

    /// Whether the read takes row locks (`FOR UPDATE`, `FOR SHARE` and
    /// their variants). The lock is the point of such a read, and a cache
    /// cannot take one, so it always goes to Postgres.
    pub fn is_locking(&self) -> bool {
        let mut words = self
            .sql
            .split_whitespace()
            .map(|word| word.trim_end_matches(';'));
        while let Some(word) = words.next() {
            if word.eq_ignore_ascii_case("FOR")
                && let Some(next) = words.next()
                && (next.eq_ignore_ascii_case("UPDATE")
                    || next.eq_ignore_ascii_case("SHARE")
                    || next.eq_ignore_ascii_case("NO")
                    || next.eq_ignore_ascii_case("KEY"))
            {
                return true;
            }
        }
        false
    }

    /// The bytes the binds hold, for a size-bounded cache.
    pub fn byte_len(&self) -> usize {
        self.sql.len()
            + self
                .binds
                .iter()
                .map(|bind| bind.bytes.as_ref().map_or(0, Vec::len))
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(sql: &str) -> QueryKey {
        QueryKey {
            sql: sql.to_string(),
            binds: Vec::new(),
        }
    }

    #[test]
    fn selects_and_reading_ctes_are_reads() {
        assert!(key("SELECT 1").is_select());
        assert!(key("  (select 1)").is_select());
        assert!(key("WITH recent AS (SELECT * FROM t) SELECT * FROM recent").is_select());
        assert!(
            key("with u as (select updated_at from t) select * from u").is_select(),
            "a column name is not a statement"
        );
        assert!(!key("WITH moved AS (DELETE FROM t RETURNING *) SELECT * FROM moved").is_select());
        assert!(!key("with x as (update t set a = 1 returning *) select 1").is_select());
        assert!(!key("INSERT INTO t VALUES (1) RETURNING id").is_select());
        assert!(!key("SELECTED").is_select());
        assert!(!key("WITHIN").is_select());
    }

    #[test]
    fn locking_reads_are_recognised() {
        assert!(key("SELECT * FROM t WHERE id = $1 FOR UPDATE").is_locking());
        assert!(key("SELECT * FROM t FOR NO KEY UPDATE").is_locking());
        assert!(key("SELECT * FROM t for share skip locked").is_locking());
        assert!(key("SELECT * FROM t FOR KEY SHARE;").is_locking());
        assert!(!key("SELECT * FROM t ORDER BY id").is_locking());
        assert!(!key("SELECT * FROM forecasts").is_locking());
    }
}
