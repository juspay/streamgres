//! The query model: read (subscription) queries, write queries, and the
//! predicate tree (`Where` / `Condition`) shared by both.
//!
//! This is deliberately the engine's own representation rather than raw SQL
//! strings: predicates must be inspectable (to index them) and comparable
//! (to route writes), neither of which text allows. The SQL parser
//! (`crate::parser`) produces these types from query text.

use std::collections::HashMap;

use super::schema::{DbColumn, DbRecord, TableName};
use super::value::Value;

/// A subscription query: `SELECT * FROM table WHERE … ORDER BY … LIMIT …`.
///
/// `Eq` + `Hash` let structurally identical queries be compared and deduped
/// (e.g. for a future shared-materialization step); the IVM forward index
/// itself is keyed by subscription uuid.
///
/// - `table`: table name, resolved against the [`super::schema::Catalog`].
/// - `filter`: the `WHERE` predicate tree.
/// - `order_by`: the `ORDER BY` clause.
/// - `limit`: the `LIMIT` row cap.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReadQuery {
    pub table: TableName,
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
    pub table: TableName,
    pub pkey_value: HashMap<String, Value>,
    pub record: DbRecord,
}

/// Delete the row identified by `pkey_value`. Carries no row data — only
/// identity — so it can never move a row *into* a result set, only out.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteQuery {
    pub table: TableName,
    pub pkey_value: HashMap<String, Value>,
}

/// Insert a new row (upsert semantics in v1: inserting an existing key
/// replaces the row).
#[derive(Debug, Clone, PartialEq)]
pub struct InsertQuery {
    pub table: TableName,
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
    pub column: DbColumn,
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
    pub fn new(column: DbColumn, direction: Order) -> Self {
        OrderBy { column, direction }
    }
}

impl ReadQuery {
    /// Builds a subscription query from its four clauses.
    pub fn new(table: impl Into<TableName>, filter: Where, order_by: OrderBy, limit: u32) -> Self {
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
    pub fn table(&self) -> &TableName {
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
