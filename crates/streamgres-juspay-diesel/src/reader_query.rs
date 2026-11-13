//! The complete reader check, eligible for Diesel's prepared-statement cache.
//!
//! Raw `sql_query` is deliberately not cacheable in Diesel. Here the SQL comes
//! only from the engine, and the three table-name arrays remain ordinary binds.
//! Every execution still resolves names and checks privileges on the connection.
use crate::diesel::connection::Connection;
use crate::diesel::pg::Pg;
use crate::diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use crate::diesel::query_dsl::RunQueryDsl;
use crate::diesel::result::{Error, QueryResult};
use crate::diesel::sql_types::{Array, Text, Untyped};
use streamgres::cache::ReaderCheck;

pub(crate) struct ReaderQuery<'a>(pub &'a ReaderCheck, pub bool);

impl Query for ReaderQuery<'_> {
    type SqlType = Untyped;
}
impl QueryId for ReaderQuery<'_> {
    type QueryId = ();
    // Key by SQL and bind types, so a future engine SQL change cannot reuse
    // a prepared statement for different text under this adapter type.
    const HAS_STATIC_QUERY_ID: bool = false;
}
impl<C: Connection> RunQueryDsl<C> for ReaderQuery<'_> {}
impl QueryFragment<Pg> for ReaderQuery<'_> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        if !self.1 {
            out.unsafe_to_cache_prepared();
        }
        let mut sql = self.0.sql;
        for (marker, values) in [
            ("$1", &self.0.names),
            ("$2", &self.0.as_written),
            ("$3", &self.0.qualified),
        ] {
            let (before, rest) = sql.split_once(marker).ok_or_else(|| {
                Error::QueryBuilderError("reader-check SQL is missing an expected bind".into())
            })?;
            out.push_sql(before);
            out.push_bind_param::<Array<Text>, _>(values)?;
            sql = rest;
        }
        out.push_sql(sql);
        Ok(())
    }
}
