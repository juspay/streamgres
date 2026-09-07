//! The query model: read (subscription) queries, write queries, and the
//! predicate tree (`Where` / `Condition`) shared by both.
//!
//! This is deliberately the engine's own representation rather than raw SQL
//! strings: predicates must be inspectable (to index them) and comparable
//! (to route writes), neither of which text allows. The SQL parser
//! (`crate::parser`) produces these types from query text.

use super::frame::{DataFrameKey, DataFrameRow};
use super::schema::{ColumnName, TableName};
use super::value::Value;

/// The client-facing handle of one registered subscription.
///
/// A dedicated type rather than a bare `String`, so a subscription id can
/// never be confused with the other strings the engine passes around
/// (table names, column names). Constructed from any string-ish value;
/// compares, orders, and hashes exactly like the underlying id, and maps
/// keyed by `QueryId` accept a plain `&str` for lookups.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryId(String);

impl QueryId {
    /// The id as a borrowed string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for QueryId {
    /// Wraps a borrowed id.
    fn from(id: &str) -> Self {
        QueryId(id.to_owned())
    }
}

impl From<String> for QueryId {
    /// Wraps an owned id.
    fn from(id: String) -> Self {
        QueryId(id)
    }
}

impl std::borrow::Borrow<str> for QueryId {
    /// Lets maps keyed by [`QueryId`] be queried with a plain `&str`.
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for QueryId {
    /// Renders as the bare id, honoring width/alignment format flags.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.pad(&self.0)
    }
}

/// A subscription query: `SELECT * FROM table WHERE … ORDER BY … LIMIT …`.
///
/// `Eq` + `Hash` let structurally identical queries be compared and deduped
/// — registration uses this to serve an identical already-registered query
/// straight from the shared frame instead of re-running it; the IVM
/// forward index itself is keyed by subscription uuid.
///
/// - `table`: table name, resolved against the [`super::schema::Catalog`].
/// - `filter`: the `WHERE` predicate tree.
/// - `order_by`: the `ORDER BY` clause; the parser defaults it to the
///   first declared pkey column, ascending.
/// - `limit`: the `LIMIT` row cap; `u32::MAX` is the parser's spelling of
///   "no limit".
///
/// `order_by` and `limit` take part in the structural equality that twin
/// sharing matches on, so programmatic queries must reproduce the parser's
/// conventions exactly to share materialization with parsed ones.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SingleTableReadQuery {
    pub table: TableName,
    pub filter: Where,
    pub order_by: OrderBy,
    pub limit: u32,
}

/// A LEFT JOIN edge of a [`MultiTableReadQuery`]: rows of `sub_table`
/// attach to main rows where
/// `sub_table.<sub_table_column> = main.<main_table_column>`.
///
/// `sub_table` carries the sub side's own `WHERE`; its `order_by` /
/// `limit` are unused — a `LIMIT` on a join's sub side has no SQL meaning
/// (it would cap the whole side across every referenced value), so the
/// engine normalizes it away at registration. A `NULL` (or missing) join
/// value never matches, consistent with the engine's NULL semantics —
/// such a main row simply shows an empty sub side.
#[derive(Debug, Clone, PartialEq)]
pub struct LeftJoin {
    pub sub_table: SingleTableReadQuery,
    pub main_table_column: ColumnName,
    pub sub_table_column: ColumnName,
}

/// A multi-table subscription: a main query LEFT JOINed to sub queries.
///
/// Main rows are visible purely by the main `WHERE` — a join value with no
/// matching sub rows renders as an empty sub side, never hides the main
/// row. `order_by` / `limit` (not yet enforced) live on `main_table`.
#[derive(Debug, Clone, PartialEq)]
pub struct MultiTableReadQuery {
    pub main_table: SingleTableReadQuery,
    pub left_joins: Vec<LeftJoin>,
}

/// A write against a table — exactly one of insert / update / delete.
///
/// Writes speak the same vocabulary as the operations the engine emits: the
/// affected row is identified by a [`DataFrameKey`] and its new image
/// carried as a [`DataFrameRow`] — there is no separate record type.
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq)]
pub enum WriteQuery {
    UPDATE(UpdateQuery),
    DELETE(DeleteQuery),
    INSERT(InsertQuery),
}

/// Update the row identified by `pkey_value`.
///
/// v1 constraint: `record` must carry the **complete new row image** —
/// every column, primary-key columns included, never just the changed
/// ones. Predicates are evaluated against it, so a partial image would
/// silently mis-evaluate conditions on omitted columns. Supporting partial
/// updates needs a read-before-write against storage (see the roadmap in
/// the README).
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateQuery {
    pub table: TableName,
    pub pkey_value: DataFrameKey,
    pub record: DataFrameRow,
}

/// Delete the row identified by `pkey_value`. Carries no row data — only
/// identity — so it can never move a row *into* a result set, only out.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteQuery {
    pub table: TableName,
    pub pkey_value: DataFrameKey,
}

/// Insert a new row (upsert semantics in v1: inserting an existing key
/// replaces the row). `record` carries the complete row image — every
/// column, primary-key columns included.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertQuery {
    pub table: TableName,
    pub pkey_value: DataFrameKey,
    pub record: DataFrameRow,
}

/// A leaf predicate: `column <op> value`.
///
/// This is the key of the IVM reverse index, hence `Eq` + `Hash`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Condition {
    pub column: ColumnName,
    pub comparison_operator: ComparisonOperator,
    pub value: Value,
}

/// A predicate tree of [`Condition`]s combined with `AND` / `OR`.
///
/// `AND(vec![])` is vacuously true (a query with no filter — a full-table
/// subscription); `OR(vec![])` is vacuously false, matching SQL conventions.
///
/// This is the *raw* shape, exactly as the user's query states it. The
/// engine routes on the derived DNF shape instead — see [`Where::to_dnf`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Where {
    Condition(Condition),
    AND(Vec<Where>),
    OR(Vec<Where>),
}

/// One conjunctive clause of a DNF-normalized filter: the disjunct matches a
/// row iff **every** condition in it matches. A disjunct with no conditions
/// is vacuously true.
///
/// A full filter in DNF is an OR of disjuncts — represented simply as
/// `Vec<Disjunct>`, where the empty vec is vacuously false (`OR` of
/// nothing). Produced by [`Where::to_dnf`]; the IVM indexes and counts these
/// rather than walking `Where` trees per write.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Disjunct {
    pub conditions: Vec<Condition>,
}

/// The comparison operator of a leaf [`Condition`]: equality, ordering, and
/// set-membership tests. Negation lives here (`NEQ`, `NOT_IN`) — the
/// [`Where`] tree has no `NOT` node.
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
    pub column: ColumnName,
    pub direction: Order,
}

/// Sort direction of an [`OrderBy`] clause: ascending or descending.
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Order {
    ASC,
    DESC,
}

impl Condition {
    /// Builds a `column <op> value` leaf condition.
    pub fn new(
        column: impl Into<ColumnName>,
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
        column: impl Into<ColumnName>,
        op: ComparisonOperator,
        value: impl Into<Value>,
    ) -> Self {
        Where::Condition(Condition::new(column, op, value))
    }

    /// All leaf [`Condition`]s of the tree, in depth-first order — the raw
    /// syntactic leaves, duplicates and dead branches included.
    ///
    /// Note this is *not* what drives routing: the IVM indexes the
    /// [`Where::to_dnf`] disjunct conditions instead, which collapse
    /// duplicates and drop conditions in unsatisfiable branches (for
    /// `a = 1 AND FALSE`, this returns `a = 1` while the DNF is empty and
    /// nothing gets indexed).
    pub fn leaf_conditions(&self) -> Vec<&Condition> {
        let mut out = Vec::new();
        self.collect_leaves(&mut out);
        out
    }

    /// Recursive worker for [`Where::leaf_conditions`]: pushes every leaf of
    /// this subtree onto `out` in depth-first order.
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

    /// Replace every leaf equal to `old` with `new`, in place — the seam
    /// for editing one condition's value list (a join's `IN`) without
    /// rebuilding the tree.
    pub fn replace_condition(&mut self, old: &Condition, new: &Condition) {
        match self {
            Where::Condition(condition) => {
                if condition == old {
                    *condition = new.clone();
                }
            }
            Where::AND(children) | Where::OR(children) => {
                for child in children {
                    child.replace_condition(old, new);
                }
            }
        }
    }

    /// Can this tree evaluate to `true` with *zero* leaf conditions
    /// matching? True for `AND(vec![])` (no filter at all) and for shapes
    /// like `x OR TRUE`. Equivalent to `to_dnf` containing an empty
    /// disjunct, without building the DNF.
    pub fn vacuously_satisfiable(&self) -> bool {
        match self {
            Where::Condition(_) => false,
            Where::AND(children) => children.iter().all(Where::vacuously_satisfiable),
            Where::OR(children) => children.iter().any(Where::vacuously_satisfiable),
        }
    }

    /// Normalize to disjunctive normal form: an OR of [`Disjunct`]s (each an
    /// AND of leaf conditions), logically equivalent to this tree.
    ///
    /// There is no `NOT` node in `Where` (negation lives inside leaf
    /// operators like `NEQ` / `NOT_IN`), so this is plain distribution of
    /// AND over OR. Identities fall out naturally: `AND(vec![])` becomes one
    /// empty disjunct (always true), `OR(vec![])` becomes zero disjuncts
    /// (never true), and a duplicated condition within one disjunct is
    /// collapsed so each disjunct's length is its exact match requirement.
    ///
    /// The result can be exponential in the alternation depth of the tree —
    /// `(a1 OR b1) AND … AND (an OR bn)` yields `2^n` disjuncts. Typical
    /// subscription filters (ANDs with small OR/IN sprinkles) stay tiny;
    /// a size cap with a tree-evaluation fallback is deliberately deferred
    /// until real workloads show the need.
    ///
    /// The `AND` arm folds the cross product starting from TRUE (one empty
    /// disjunct); an always-false child empties the accumulator, making the
    /// whole AND false.
    pub fn to_dnf(&self) -> Vec<Disjunct> {
        match self {
            Where::Condition(condition) => vec![Disjunct {
                conditions: vec![condition.clone()],
            }],
            Where::OR(children) => children.iter().flat_map(Where::to_dnf).collect(),
            Where::AND(children) => {
                let mut accumulated = vec![Disjunct {
                    conditions: Vec::new(),
                }];
                for child in children {
                    let child_dnf = child.to_dnf();
                    let mut next = Vec::with_capacity(accumulated.len() * child_dnf.len());
                    for left in &accumulated {
                        for right in &child_dnf {
                            let mut conditions = left.conditions.clone();
                            for condition in &right.conditions {
                                if !conditions.contains(condition) {
                                    conditions.push(condition.clone());
                                }
                            }
                            next.push(Disjunct { conditions });
                        }
                    }
                    accumulated = next;
                }
                accumulated
            }
        }
    }
}

impl OrderBy {
    /// Builds an `ORDER BY column direction` clause.
    pub fn new(column: impl Into<ColumnName>, direction: Order) -> Self {
        OrderBy {
            column: column.into(),
            direction,
        }
    }
}

impl SingleTableReadQuery {
    /// Builds a subscription query from its four clauses.
    pub fn new(table: impl Into<TableName>, filter: Where, order_by: OrderBy, limit: u32) -> Self {
        SingleTableReadQuery {
            table: table.into(),
            filter,
            order_by,
            limit,
        }
    }
}

impl WriteQuery {
    /// The name of the table this write targets.
    pub fn table(&self) -> &TableName {
        match self {
            WriteQuery::UPDATE(q) => &q.table,
            WriteQuery::DELETE(q) => &q.table,
            WriteQuery::INSERT(q) => &q.table,
        }
    }

    /// The identity of the affected row.
    pub fn pkey_value(&self) -> &DataFrameKey {
        match self {
            WriteQuery::UPDATE(q) => &q.pkey_value,
            WriteQuery::DELETE(q) => &q.pkey_value,
            WriteQuery::INSERT(q) => &q.pkey_value,
        }
    }

    /// The complete row image *after* this write — the record exactly as
    /// carried, which must hold every column, primary-key columns included
    /// (see [`UpdateQuery`]); nothing is merged in here. `None` for
    /// deletes, which carry no row data.
    ///
    /// Predicates are evaluated against this image, so a record missing a
    /// column would silently mis-evaluate conditions on it.
    pub fn new_row_image(&self) -> Option<&DataFrameRow> {
        match self {
            WriteQuery::UPDATE(q) => Some(&q.record),
            WriteQuery::INSERT(q) => Some(&q.record),
            WriteQuery::DELETE(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ComparisonOperator::EQ;

    /// Shorthand for a single `column = value` leaf [`Where`].
    fn c(column: &str, value: i32) -> Where {
        Where::condition(column, EQ, value)
    }

    /// Builds the expected [`Disjunct`] of `column = value` conditions.
    fn disjunct(pairs: &[(&str, i32)]) -> Disjunct {
        Disjunct {
            conditions: pairs
                .iter()
                .map(|(column, value)| Condition::new(*column, EQ, *value))
                .collect(),
        }
    }

    /// A lone leaf condition normalizes to exactly one one-condition disjunct.
    #[test]
    fn single_condition_is_one_singleton_disjunct() {
        assert_eq!(c("a", 1).to_dnf(), vec![disjunct(&[("a", 1)])]);
    }

    /// OR children concatenate their disjuncts, and AND distributes over OR:
    /// `(a OR b) AND (c OR d)` yields the cross product `ac, ad, bc, bd`.
    #[test]
    fn or_concatenates_and_distributes_over_and() {
        let or = Where::OR(vec![c("a", 1), c("b", 2)]);
        assert_eq!(or.to_dnf(), vec![disjunct(&[("a", 1)]), disjunct(&[("b", 2)])]);

        let cross = Where::AND(vec![
            Where::OR(vec![c("a", 1), c("b", 2)]),
            Where::OR(vec![c("c", 3), c("d", 4)]),
        ]);
        assert_eq!(
            cross.to_dnf(),
            vec![
                disjunct(&[("a", 1), ("c", 3)]),
                disjunct(&[("a", 1), ("d", 4)]),
                disjunct(&[("b", 2), ("c", 3)]),
                disjunct(&[("b", 2), ("d", 4)]),
            ]
        );
    }

    /// The boolean identities: TRUE (`AND(vec![])`) is one empty disjunct,
    /// FALSE (`OR(vec![])`) is no disjuncts, `x AND FALSE` annihilates to
    /// FALSE, and `x OR TRUE` keeps its vacuous disjunct.
    #[test]
    fn identities_true_false_and_annihilation() {
        assert_eq!(Where::AND(vec![]).to_dnf(), vec![disjunct(&[])]);
        assert_eq!(Where::OR(vec![]).to_dnf(), Vec::<Disjunct>::new());
        assert_eq!(
            Where::AND(vec![c("x", 1), Where::OR(vec![])]).to_dnf(),
            Vec::<Disjunct>::new()
        );
        assert_eq!(
            Where::OR(vec![c("x", 1), Where::AND(vec![])]).to_dnf(),
            vec![disjunct(&[("x", 1)]), disjunct(&[])]
        );
    }

    /// `a AND a` must require ONE match, not two — the disjunct's length is
    /// the engine's exact firing threshold.
    #[test]
    fn duplicate_condition_in_a_conjunction_collapses() {
        assert_eq!(
            Where::AND(vec![c("a", 1), c("a", 1)]).to_dnf(),
            vec![disjunct(&[("a", 1)])]
        );
    }
}
