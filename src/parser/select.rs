//! `SELECT` over one table or a tree of joined tables, as an ORM renders
//! it, into the engine's [`MultiTableReadQuery`].
//!
//! The `FROM` is read as a tree. Diesel writes
//! `FROM ("a" INNER JOIN ("b" INNER JOIN "c" ON (...)) ON (...))`: each
//! `JOIN` links the item on its right to one table on its left through an
//! `ON` equality of one column each side, and that becomes an edge from
//! the left table (the parent) to the right item's root (the child).
//! `INNER` (or a bare `JOIN`) is an inner edge, `LEFT [OUTER]` an outer
//! one. Other conditions in an `ON` go to the filter of the one table
//! they name. `WHERE` is split into its top-level conjuncts and each goes
//! to the one table it names. `ORDER BY` and `LIMIT` belong to the root.
//!
//! What is refused, by name: a table appearing twice, `RIGHT`/`FULL`/
//! `CROSS` joins, a condition naming two tables (other than the edge's
//! equality), an `OR` across tables, a `WHERE` condition on a
//! `LEFT`-joined table (it would turn the join inner), an `ORDER BY` on a
//! joined table, an unqualified column in a join (Diesel always
//! qualifies), compound join keys.

use super::{ParseError, Parser, Projected, Projection, Tok, collapse, default_order};
use crate::model::{
    Catalog, ColumnName, ComparisonOperator, Condition, DbTable, Join, MultiTableReadQuery,
    Order, OrderBy, SingleTableReadQuery, Value, Where,
};

/// A parsed `SELECT`: the engine's query tree and what the statement
/// projects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Select {
    pub query: MultiTableReadQuery,
    pub projection: Projection,
    /// Rows the statement skips (`OFFSET`). The tree's limit is the
    /// statement's limit plus this, so the engine keeps enough rows for
    /// the client to skip and cut.
    pub offset: u32,
}

/// One table of the `FROM`, while the statement is read.
struct Node<'c> {
    table: &'c DbTable,
    /// Top-level conjuncts of this table's filter, from `ON` and `WHERE`.
    conjuncts: Vec<Where>,
    /// Edges to child nodes: (child, parent column, child column, inner).
    edges: Vec<(usize, ColumnName, ColumnName, bool)>,
    /// Whether this node is the child of a `LEFT` edge, which forbids a
    /// `WHERE` condition on it.
    left_joined: bool,
}

/// A column resolved to a node of the `FROM`.
struct Ref {
    node: usize,
    column: ColumnName,
}

impl Parser<'_> {
    /// `SELECT` to the end of the statement.
    pub(super) fn select(&mut self, catalog: &Catalog) -> Result<Select, ParseError> {
        self.expect_word("SELECT")?;
        if self.is_word("DISTINCT") {
            return self.err("DISTINCT is not supported");
        }
        // The projection is read once the FROM is known, so its columns
        // can be checked; the engine follows whole rows whatever is
        // selected, so what it names is validated and kept for the client.
        let projection_at = self.pos;
        self.skip_projection()?;
        self.expect_word("FROM")?;
        let mut nodes: Vec<Node<'_>> = Vec::new();
        let root = self.join_item(catalog, &mut nodes)?;
        debug_assert_eq!(root, 0, "the leftmost table is the root");
        let after_from = self.pos;
        self.pos = projection_at;
        let projection = self.projection_of(&nodes)?;
        self.pos = after_from;

        if self.eat_word("WHERE") {
            let scope: Vec<usize> = (0..nodes.len()).collect();
            for (node, filter) in self.scoped_filter(&nodes, &scope)? {
                if nodes[node].left_joined {
                    return self.err(format!(
                        "a WHERE condition on `{}` would turn its LEFT JOIN inner; put it in the ON",
                        nodes[node].table.name
                    ));
                }
                nodes[node].conjuncts.push(filter);
            }
        }

        let order_by = if self.eat_word("ORDER") {
            self.expect_word("BY")?;
            let mut clauses = Vec::new();
            loop {
                let at = self.at();
                let column = self.column_ref(&nodes, &(0..nodes.len()).collect::<Vec<_>>())?;
                if column.node != 0 {
                    return Err(ParseError {
                        message: format!(
                            "ORDER BY `{}` is on a joined table; only the first table's columns order",
                            nodes[column.node].table.name
                        ),
                        position: at,
                    });
                }
                let direction = if self.eat_word("DESC") {
                    Order::DESC
                } else {
                    self.eat_word("ASC");
                    Order::ASC
                };
                clauses.push(OrderBy::new(column.column, direction));
                if !self.eat_sym(",") {
                    break;
                }
            }
            clauses
        } else {
            vec![default_order(nodes[0].table, self.at())?]
        };
        let limit = self.limit()?;
        let offset = self.offset()?;
        // The tree holds `limit + offset` rows; `u32::MAX` means no limit,
        // so a sum that reaches it would register an unbounded query.
        let kept = if limit == u32::MAX {
            limit
        } else {
            match limit.checked_add(offset) {
                Some(kept) if kept < u32::MAX => kept,
                _ => {
                    return Err(ParseError {
                        message: "LIMIT plus OFFSET is more rows than the engine follows".to_string(),
                        position: self.at(),
                    });
                }
            }
        };

        let query = build(&nodes, 0, order_by, kept);
        Ok(Select {
            query,
            projection,
            offset,
        })
    }

    /// `LIMIT n`, `LIMIT $k`, or nothing (`u32::MAX`).
    fn limit(&mut self) -> Result<u32, ParseError> {
        if !self.eat_word("LIMIT") {
            return Ok(u32::MAX);
        }
        self.count("limit")
    }

    /// `OFFSET n`, `OFFSET $k`, or nothing (0).
    fn offset(&mut self) -> Result<u32, ParseError> {
        if !self.eat_word("OFFSET") {
            return Ok(0);
        }
        self.count("offset")
    }

    /// A non-negative integer, literal or bound.
    fn count(&mut self, what: &str) -> Result<u32, ParseError> {
        let at = self.at();
        match self.peek().clone() {
            Tok::Num(number) => {
                self.bump();
                number.parse::<u32>().map_err(|_| ParseError {
                    message: format!("`{number}` is not a valid {what}"),
                    position: at,
                })
            }
            Tok::Bind(_) => match self.value()? {
                Value::Int(limit) if limit >= 0 && limit <= i64::from(u32::MAX) => Ok(limit as u32),
                other => Err(ParseError {
                    message: format!(
                        "a {what} parameter must be a non-negative integer, found {other:?}"
                    ),
                    position: at,
                }),
            },
            _ => self.err(format!("expected a {what}, found {}", self.describe())),
        }
    }

    /// A `FROM` item: a table or a parenthesised item, followed by any
    /// number of `JOIN`s. Returns the index of the item's root node.
    fn join_item<'c>(
        &mut self,
        catalog: &'c Catalog,
        nodes: &mut Vec<Node<'c>>,
    ) -> Result<usize, ParseError> {
        let first = nodes.len();
        let root = self.joined_table(catalog, nodes)?;
        loop {
            let at = self.at();
            let inner = if self.eat_word("INNER") {
                self.expect_word("JOIN")?;
                true
            } else if self.eat_word("LEFT") {
                self.eat_word("OUTER");
                self.expect_word("JOIN")?;
                false
            } else if self.eat_word("JOIN") {
                true
            } else if self.is_word("RIGHT") || self.is_word("FULL") || self.is_word("CROSS") {
                return self.err("only INNER and LEFT joins are supported");
            } else {
                return Ok(root);
            };
            let left: Vec<usize> = (first..nodes.len()).collect();
            let right_root = self.joined_table(catalog, nodes)?;
            let right: Vec<usize> = (right_root..nodes.len()).collect();
            self.expect_word("ON")?;
            let on_at = self.at();
            let mut edge: Option<(usize, ColumnName, ColumnName)> = None;
            let scope: Vec<usize> = (first..nodes.len()).collect();
            for term in self.on_terms(nodes, &scope)? {
                match term {
                    OnTerm::Equality(a, b) => {
                        let (parent, child) = if left.contains(&a.node) && b.node == right_root {
                            (a, b)
                        } else if left.contains(&b.node) && a.node == right_root {
                            (b, a)
                        } else {
                            return Err(ParseError {
                                message: format!(
                                    "a JOIN's ON must link `{}` to a table on its left",
                                    nodes[right_root].table.name
                                ),
                                position: on_at,
                            });
                        };
                        if edge.is_some() {
                            return Err(ParseError {
                                message: "compound join keys are not supported".to_string(),
                                position: on_at,
                            });
                        }
                        edge = Some((parent.node, parent.column, child.column));
                    }
                    OnTerm::Condition(node, filter) => {
                        if !inner && !right.contains(&node) {
                            return Err(ParseError {
                                message: format!(
                                    "a LEFT JOIN's ON may only filter `{}`",
                                    nodes[right_root].table.name
                                ),
                                position: on_at,
                            });
                        }
                        nodes[node].conjuncts.push(filter);
                    }
                }
            }
            let Some((parent, parent_column, child_column)) = edge else {
                return Err(ParseError {
                    message: "a JOIN's ON needs an equality of one column each side".to_string(),
                    position: at,
                });
            };
            nodes[parent]
                .edges
                .push((right_root, parent_column, child_column, inner));
            if !inner {
                nodes[right_root].left_joined = true;
            }
        }
    }

    /// A table, or a parenthesised `FROM` item.
    fn joined_table<'c>(
        &mut self,
        catalog: &'c Catalog,
        nodes: &mut Vec<Node<'c>>,
    ) -> Result<usize, ParseError> {
        if self.eat_sym("(") {
            self.descend()?;
            let root = self.join_item(catalog, nodes)?;
            self.expect_sym(")")?;
            self.depth -= 1;
            return Ok(root);
        }
        let at = self.at();
        let table = self.table(catalog)?;
        if self.is_word("AS") {
            return self.err("table aliases are not supported");
        }
        if nodes.iter().any(|node| node.table.name == table.name) {
            return Err(ParseError {
                message: format!("`{}` appears twice; a table may be joined once", table.name),
                position: at,
            });
        }
        nodes.push(Node {
            table,
            conjuncts: Vec::new(),
            edges: Vec::new(),
            left_joined: false,
        });
        Ok(nodes.len() - 1)
    }

    /// The terms of an `ON`: a parenthesised `AND` of equalities between
    /// tables and conditions on one table.
    fn on_terms(&mut self, nodes: &[Node<'_>], scope: &[usize]) -> Result<Vec<OnTerm>, ParseError> {
        let mut terms = Vec::new();
        self.on_term(nodes, scope, &mut terms)?;
        Ok(terms)
    }

    fn on_term(
        &mut self,
        nodes: &[Node<'_>],
        scope: &[usize],
        terms: &mut Vec<OnTerm>,
    ) -> Result<(), ParseError> {
        if self.eat_sym("(") {
            self.descend()?;
            // Either a nested AND of terms, or one condition in parens.
            let before = self.pos;
            if let Ok(column) = self.column_ref(nodes, scope)
                && self.eat_sym("=")
                && let Ok(other) = self.column_ref(nodes, scope)
                && self.is_sym(")")
                && other.node != column.node
            {
                self.expect_sym(")")?;
                self.depth -= 1;
                terms.push(OnTerm::Equality(column, other));
                return Ok(());
            }
            self.pos = before;
            self.on_term(nodes, scope, terms)?;
            while self.eat_word("AND") {
                self.on_term(nodes, scope, terms)?;
            }
            self.expect_sym(")")?;
            self.depth -= 1;
            return Ok(());
        }
        let before = self.pos;
        if let Ok(column) = self.column_ref(nodes, scope)
            && self.eat_sym("=")
            && let Ok(other) = self.column_ref(nodes, scope)
            && other.node != column.node
        {
            terms.push(OnTerm::Equality(column, other));
            return Ok(());
        }
        self.pos = before;
        let mut filters = self.scoped_filter_primary(nodes, scope)?;
        if filters.len() != 1 {
            return self.err("an ON condition must name one table");
        }
        let (node, filter) = filters.pop().expect("one");
        terms.push(OnTerm::Condition(node, filter));
        Ok(())
    }

    /// A filter whose conditions may name different tables, as a list of
    /// (table, filter) pairs: an `AND` of parts naming different tables
    /// splits, an `OR` must stay within one table.
    fn scoped_filter(
        &mut self,
        nodes: &[Node<'_>],
        scope: &[usize],
    ) -> Result<Vec<(usize, Where)>, ParseError> {
        let at = self.at();
        let mut alternatives = vec![self.scoped_conjunct(nodes, scope)?];
        while self.eat_word("OR") {
            alternatives.push(self.scoped_conjunct(nodes, scope)?);
        }
        if alternatives.len() == 1 {
            return Ok(alternatives.pop().expect("one"));
        }
        // Each alternative must collapse to one table, the same one.
        let mut node = None;
        let mut parts = Vec::new();
        for alternative in alternatives {
            let (n, filter) = one_table(alternative, at)?;
            if node.is_some_and(|seen| seen != n) {
                return Err(ParseError {
                    message: "an OR across tables is not supported".to_string(),
                    position: at,
                });
            }
            node = Some(n);
            parts.push(filter);
        }
        Ok(vec![(node.expect("at least one"), collapse(parts, Where::OR))])
    }

    fn scoped_conjunct(
        &mut self,
        nodes: &[Node<'_>],
        scope: &[usize],
    ) -> Result<Vec<(usize, Where)>, ParseError> {
        let mut parts = self.scoped_filter_primary(nodes, scope)?;
        while self.eat_word("AND") {
            parts.extend(self.scoped_filter_primary(nodes, scope)?);
        }
        Ok(parts)
    }

    /// A parenthesised filter, `TRUE` / `FALSE`, or one condition.
    fn scoped_filter_primary(
        &mut self,
        nodes: &[Node<'_>],
        scope: &[usize],
    ) -> Result<Vec<(usize, Where)>, ParseError> {
        if self.eat_sym("(") {
            self.descend()?;
            let filter = self.scoped_filter(nodes, scope)?;
            self.expect_sym(")")?;
            self.depth -= 1;
            return Ok(filter);
        }
        if self.eat_word("TRUE") {
            return Ok(vec![(scope[0], Where::AND(Vec::new()))]);
        }
        if self.eat_word("FALSE") {
            return Ok(vec![(scope[0], Where::OR(Vec::new()))]);
        }
        let column = self.column_ref(nodes, scope)?;
        let condition = self.comparison(column.column)?;
        Ok(vec![(column.node, condition)])
    }

    /// The operator and value after a column, as the single-table parser
    /// reads them.
    fn comparison(&mut self, column: ColumnName) -> Result<Where, ParseError> {
        if self.eat_word("IS") {
            let comparison_operator = if self.eat_word("NOT") {
                ComparisonOperator::IS_NOT
            } else {
                ComparisonOperator::IS
            };
            if !self.eat_word("NULL") {
                return self.err(format!(
                    "expected NULL after IS [NOT], found {}",
                    self.describe()
                ));
            }
            return Ok(Where::Condition(Condition {
                column,
                comparison_operator,
                value: Value::Null,
            }));
        }
        let comparison_operator = if self.eat_sym("=") {
            if self.eat_word("ANY") {
                return Ok(Where::Condition(Condition {
                    column,
                    comparison_operator: ComparisonOperator::IN,
                    value: self.quantified_list("ANY")?,
                }));
            }
            ComparisonOperator::EQ
        } else if self.eat_sym("!=") {
            if self.eat_word("ALL") {
                return Ok(Where::Condition(Condition {
                    column,
                    comparison_operator: ComparisonOperator::NOT_IN,
                    value: self.quantified_list("ALL")?,
                }));
            }
            ComparisonOperator::NEQ
        } else if self.eat_sym(">=") {
            ComparisonOperator::GTE
        } else if self.eat_sym(">") {
            ComparisonOperator::GT
        } else if self.eat_sym("<=") {
            ComparisonOperator::LTE
        } else if self.eat_sym("<") {
            ComparisonOperator::LT
        } else if self.eat_word("IN") {
            return Ok(Where::Condition(Condition {
                column,
                comparison_operator: ComparisonOperator::IN,
                value: self.value_list()?,
            }));
        } else if self.eat_word("NOT") {
            self.expect_word("IN")?;
            return Ok(Where::Condition(Condition {
                column,
                comparison_operator: ComparisonOperator::NOT_IN,
                value: self.value_list()?,
            }));
        } else {
            return self.err(format!(
                "expected a comparison operator, [NOT] IN or IS [NOT] NULL after a column, found {}",
                self.describe()
            ));
        };
        Ok(Where::Condition(Condition {
            column,
            comparison_operator,
            value: self.value()?,
        }))
    }

    /// A column, qualified by its table when the scope has several.
    fn column_ref(&mut self, nodes: &[Node<'_>], scope: &[usize]) -> Result<Ref, ParseError> {
        let at = self.at();
        let first = self.ident()?;
        let (node, name) = if self.eat_sym(".") {
            let name = self.ident()?;
            let node = scope
                .iter()
                .copied()
                .find(|&n| nodes[n].table.name.as_str() == first)
                .ok_or_else(|| ParseError {
                    message: format!("`{first}` is not a table of this query"),
                    position: at,
                })?;
            (node, name)
        } else if scope.len() == 1 {
            (scope[0], first)
        } else {
            return Err(ParseError {
                message: format!("`{first}` must be qualified by its table in a join"),
                position: at,
            });
        };
        if nodes[node].table.column(&name).is_none() {
            return Err(ParseError {
                message: format!(
                    "unknown column `{name}` on table `{}`",
                    nodes[node].table.name
                ),
                position: at,
            });
        }
        Ok(Ref {
            node,
            column: ColumnName::from(name),
        })
    }

    /// The projection over the whole `FROM`.
    fn projection_of(&mut self, nodes: &[Node<'_>]) -> Result<Projection, ParseError> {
        if self.eat_sym("*") {
            self.expect_from()?;
            return Ok(Projection::All);
        }
        if self.is_word("COUNT") {
            self.bump();
            self.expect_sym("(")?;
            self.expect_sym("*")?;
            self.expect_sym(")")?;
            let name = self.alias()?.unwrap_or_else(|| "count".to_string());
            self.expect_from()?;
            return Ok(Projection::Count { name });
        }
        let scope: Vec<usize> = (0..nodes.len()).collect();
        let mut columns = Vec::new();
        loop {
            let column = self.column_ref(nodes, &scope)?;
            let name = self
                .alias()?
                .unwrap_or_else(|| column.column.as_str().to_string());
            columns.push(Projected {
                table: nodes[column.node].table.name.clone(),
                column: column.column,
                name,
            });
            if !self.eat_sym(",") {
                self.expect_from()?;
                return Ok(Projection::Columns(columns));
            }
        }
    }
}

enum OnTerm {
    Equality(Ref, Ref),
    Condition(usize, Where),
}

/// A list of (table, filter) pairs as one table's filter, or an error
/// when they name several.
fn one_table(mut parts: Vec<(usize, Where)>, at: usize) -> Result<(usize, Where), ParseError> {
    let Some(node) = parts.first().map(|(node, _)| *node) else {
        return Err(ParseError {
            message: "an empty condition".to_string(),
            position: at,
        });
    };
    if parts.iter().any(|(n, _)| *n != node) {
        return Err(ParseError {
            message: "an OR across tables is not supported".to_string(),
            position: at,
        });
    }
    if parts.len() == 1 {
        return Ok(parts.pop().expect("one"));
    }
    Ok((
        node,
        Where::AND(parts.into_iter().map(|(_, filter)| filter).collect()),
    ))
}

/// The engine's tree from the nodes, rooted at `index`.
fn build(nodes: &[Node<'_>], index: usize, order_by: Vec<OrderBy>, limit: u32) -> MultiTableReadQuery {
    let node = &nodes[index];
    let filter = collapse_conjuncts(node.conjuncts.clone());
    let main = SingleTableReadQuery::new(node.table.name.clone(), filter, order_by, limit);
    let joins = node
        .edges
        .iter()
        .map(|(child, parent_column, child_column, inner)| {
            let child_table = nodes[*child].table;
            let child_order = default_order(child_table, 0)
                .map(|order| vec![order])
                .unwrap_or_default();
            let sub = build(nodes, *child, child_order, u32::MAX);
            if *inner {
                Join::inner(sub, parent_column.clone(), child_column.clone())
            } else {
                Join::left(sub, parent_column.clone(), child_column.clone())
            }
        })
        .collect();
    MultiTableReadQuery::new(main, joins)
}

fn collapse_conjuncts(conjuncts: Vec<Where>) -> Where {
    match conjuncts.len() {
        0 => Where::AND(Vec::new()),
        _ => collapse(conjuncts, Where::AND),
    }
}
