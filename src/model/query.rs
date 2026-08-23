//! The query model: read (subscription) queries, write queries, and the
//! predicate tree (`Where` / `Condition`) shared by both.
//!
//! This is deliberately the engine's own representation rather than raw SQL
//! strings: predicates must be inspectable (to index them) and comparable
//! (to route writes), neither of which text allows. The SQL parser
//! (`crate::parser`) produces these types from query text.

use std::collections::HashMap;

use super::schema::{DbColumn, DbRecord};
use super::value::Value;

/// A subscription query: `SELECT * FROM table WHERE … ORDER BY … LIMIT …`.
///
/// `Eq` + `Hash` let structurally identical queries be compared and deduped
/// (e.g. for a future shared-materialization step); the IVM forward index
/// itself is keyed by subscription uuid.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReadQuery {
    /// Table name, resolved against the [`super::schema::Catalog`].
    pub table: String,
    pub filter: Where,
    pub order_by: OrderBy,
    pub limit: u32,
}

/// A write against a table — exactly one of insert / update / delete.
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq)]
pub enum WriteQuery {
    UPDATE(UpdateQuery),
    DELETE(DeleteQuery),
    INSERT(InsertQuery),
}

/// Update the row identified by `pkey_value`.
///
/// v1 constraint: `record.data` must carry the **complete new row image**,
/// not just the changed columns. Predicates are evaluated against it, so a
/// partial image would silently mis-evaluate conditions on omitted columns.
/// Supporting partial updates needs a read-before-write against storage
/// (see the roadmap in the README).
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateQuery {
    pub table: String,
    pub pkey_value: HashMap<String, Value>,
    pub record: DbRecord,
}

/// Delete the row identified by `pkey_value`. Carries no row data — only
/// identity — so it can never move a row *into* a result set, only out.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteQuery {
    pub table: String,
    pub pkey_value: HashMap<String, Value>,
}

/// Insert a new row (upsert semantics in v1: inserting an existing key
/// replaces the row).
#[derive(Debug, Clone, PartialEq)]
pub struct InsertQuery {
    pub table: String,
    pub pkey_value: HashMap<String, Value>,
    pub record: DbRecord,
}

/// A leaf predicate: `column <op> value`.
///
/// This is the key of the IVM reverse index, hence `Eq` + `Hash`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Condition {
    pub column: String,
    pub comparison_operator: ComparisonOperator,
    pub value: Value,
}

/// A predicate tree of [`Condition`]s combined with `AND` / `OR`.
///
/// `AND(vec![])` is vacuously true (a query with no filter — a full-table
/// subscription); `OR(vec![])` is vacuously false, matching SQL conventions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Where {
    Condition(Condition),
    AND(Vec<Where>),
    OR(Vec<Where>),
}

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ComparisonOperator {
    EQ,
    NEQ,
    GT,
    GTE,
    LT,
    LTE,
    IN,
    NOT_IN,
}

/// `ORDER BY column ASC|DESC`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderBy {
    pub column: DbColumn,
    pub direction: Order,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Order {
    ASC,
    DESC,
}

impl Condition {
    pub fn new(
        column: impl Into<String>,
        comparison_operator: ComparisonOperator,
        value: impl Into<Value>,
    ) -> Self {
        Condition {
            column: column.into(),
            comparison_operator,
            value: value.into(),
        }
    }
}

impl Where {
    /// Convenience for a single-condition filter.
    pub fn condition(
        column: impl Into<String>,
        op: ComparisonOperator,
        value: impl Into<Value>,
    ) -> Self {
        Where::Condition(Condition::new(column, op, value))
    }

    /// All leaf [`Condition`]s of the tree, in depth-first order. These are
    /// what `IVM::register_query` feeds into the reverse index.
    pub fn leaf_conditions(&self) -> Vec<&Condition> {
        let mut out = Vec::new();
        self.collect_leaves(&mut out);
        out
    }

    fn collect_leaves<'a>(&'a self, out: &mut Vec<&'a Condition>) {
        match self {
            Where::Condition(c) => out.push(c),
            Where::AND(children) | Where::OR(children) => {
                for child in children {
                    child.collect_leaves(out);
                }
            }
        }
    }

    /// Can this tree evaluate to `true` with *zero* leaf conditions
    /// matching? True for `AND(vec![])` (no filter at all) and for shapes
    /// like `x OR TRUE`.
    ///
    /// The routing invariant this backs: if a filter evaluates `true` on
    /// some row, then either one of its leaf conditions matched that row
    /// (so the reverse index finds it) or the filter is vacuously
    /// satisfiable (so `IVM` must treat the query as a candidate for every
    /// same-table write).
    pub fn vacuously_satisfiable(&self) -> bool {
        match self {
            Where::Condition(_) => false,
            Where::AND(children) => children.iter().all(Where::vacuously_satisfiable),
            Where::OR(children) => children.iter().any(Where::vacuously_satisfiable),
        }
    }
}

impl OrderBy {
    pub fn new(column: DbColumn, direction: Order) -> Self {
        OrderBy { column, direction }
    }
}

impl ReadQuery {
    pub fn new(table: impl Into<String>, filter: Where, order_by: OrderBy, limit: u32) -> Self {
        ReadQuery {
            table: table.into(),
            filter,
            order_by,
            limit,
        }
    }
}

impl WriteQuery {
    /// The name of the table this write targets.
    pub fn table(&self) -> &str {
        match self {
            WriteQuery::UPDATE(q) => &q.table,
            WriteQuery::DELETE(q) => &q.table,
            WriteQuery::INSERT(q) => &q.table,
        }
    }

    /// The primary-key values identifying the affected row.
    pub fn pkey_value(&self) -> &HashMap<String, Value> {
        match self {
            WriteQuery::UPDATE(q) => &q.pkey_value,
            WriteQuery::DELETE(q) => &q.pkey_value,
            WriteQuery::INSERT(q) => &q.pkey_value,
        }
    }

    /// The complete row image *after* this write: primary-key values merged
    /// over the record payload. `None` for deletes, which carry no row data.
    ///
    /// Predicates are evaluated against this image, so for updates it relies
    /// on the full-row-image constraint documented on [`UpdateQuery`].
    pub fn new_row_image(&self) -> Option<HashMap<String, Value>> {
        let record = match self {
            WriteQuery::UPDATE(q) => &q.record,
            WriteQuery::INSERT(q) => &q.record,
            WriteQuery::DELETE(_) => return None,
        };
        let mut row = record.data.clone();
        for (col, val) in self.pkey_value() {
            row.insert(col.clone(), val.clone());
        }
        Some(row)
    }
}
