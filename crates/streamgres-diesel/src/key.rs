//! What identifies a read: its SQL and its bind values.
//!
//! Diesel's own statement cache keys a query by the `TypeId` of its shape,
//! which is the same for `WHERE key = 'a'` and `WHERE key = 'b'`. A result
//! cache needs the values too, so the key is the rendered SQL (with `$n`
//! placeholders) and every bind as Postgres received it: its type OID and
//! its bytes. Two reads with equal keys get equal rows.

use diesel::pg::{Pg, PgMetadataLookup, PgQueryBuilder};
use diesel::query_builder::{QueryBuilder, QueryFragment, bind_collector::RawBytesBindCollector};
use diesel::QueryResult;

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

impl QueryKey {
    /// Renders a query the way the connection is about to send it. The
    /// lookup resolves custom types' OIDs, so it is the connection itself.
    pub fn of<T>(
        query: &T,
        lookup: &mut (dyn PgMetadataLookup + 'static),
    ) -> QueryResult<QueryKey>
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

    /// Whether the statement reads rather than writes: a `SELECT`, possibly
    /// parenthesised. Diesel sends `INSERT ... RETURNING` through the same
    /// `load` path as a `SELECT`, and those must never be cached.
    pub fn is_select(&self) -> bool {
        let head = self.sql.trim_start_matches(|c: char| c.is_whitespace() || c == '(');
        head.get(..6)
            .is_some_and(|word| word.eq_ignore_ascii_case("SELECT"))
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
