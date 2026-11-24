//! SQL SELECTs translated into the existing engine's join tree.
//!
//! The library accepts INNER, LEFT and RIGHT joins, table aliases and
//! self-joins, composite equality keys, ON conjunctions in any order, and
//! USING/NATURAL joins with same-source-type common columns. RIGHT joins
//! are re-rooted as LEFT joins; an INNER join through a nullable node
//! makes the path to that node inner. SQL column order is kept separately.
//!
//! Predicates on a nullable side remain post-join predicates: moving them
//! into ON would change SQL semantics. Their complete candidate relation,
//! and any window ordered by a joined table, is composed before filtering,
//! sorting and LIMIT/OFFSET. Existing cache budgets bound this work.
//!
//! FULL/CROSS joins, non-equality edges, edges referencing several parent
//! nodes, preserved-side ON predicates, and comparisons not represented
//! faithfully by the engine use the fallback. A NATURAL join with no
//! common columns requires a CROSS join and likewise uses the fallback.
//! Aliases for a parenthesized join relation and USING type coercion are
//! not supported.

use super::prepared::{Count, PreparedSelect};
use super::{Bind, ParseError, Parser, Scope, Tagged, Tok, default_order, lex};
use crate::ivm::QueryPart;
use crate::model::{
    Catalog, ColumnName, ComparisonOperator, Condition, DbColumn, DbTable, Join,
    MultiTableReadQuery, Order, OrderBy, PageRead, SingleTableReadQuery, TableName, Value,
    ValueType, Where,
};
use crate::sync::pg::binary::oid;

/// One column a statement returns: which table of the `FROM` it is on (by
/// position, in the order the `FROM` names them) and its name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projected {
    pub node: usize,
    pub column: ColumnName,
}

/// What a statement returns of the rows it reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    /// Every column of every table, in the order the `FROM` names them.
    All,
    /// These columns, in this order.
    Columns(Vec<Projected>),
    /// One row holding the number of rows matched.
    CountStar,
}

/// One table of the `FROM`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub table: TableName,
    /// The part of the engine's tree that holds its rows.
    pub part: QueryPart,
    /// How it joins its parent; `None` for the root.
    pub edge: Option<Edge>,
}

/// How a table joins its parent: `parent.parent_column = column`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edge {
    pub parent: usize,
    pub parent_column: ColumnName,
    pub column: ColumnName,
    /// An INNER edge; a LEFT edge otherwise, after RIGHT normalization.
    pub inner: bool,
}

/// One clause of the statement's `ORDER BY`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ordered {
    pub node: usize,
    pub column: ColumnName,
    pub direction: Order,
}

/// A parsed `SELECT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Select {
    /// The rows the engine follows: the tree of tables, their filters, and
    /// the root's order, with as many root rows as the statement may
    /// return after skipping its offset. A count, post-join filter or
    /// joined-column ordering loads the complete candidate relation.
    pub query: MultiTableReadQuery,
    /// Root of the normalized engine tree; SQL column order remains in `nodes`.
    pub root: usize,
    /// Predicates applied after outer joins, before ordering and paging.
    pub post_filters: Vec<(usize, Where)>,
    /// Tables in SQL FROM order, independent of the normalized root.
    pub nodes: Vec<Node>,
    pub projection: Projection,
    /// The statement's order of its joined rows.
    pub order: Vec<Ordered>,
    /// The statement's own `LIMIT`, `u32::MAX` for none.
    pub limit: u32,
    /// Rows the statement skips.
    pub offset: u32,
}

/// Everything that changes the composed rows independently of the engine
/// subscription. Different aliases, tree roots and post-filters cannot
/// reuse each other's composed result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResultPlan {
    pub(crate) nodes: Vec<Node>,
    root: usize,
    pub(crate) order: Vec<Ordered>,
    pub(crate) filters: Vec<(usize, Where)>,
}
impl ResultPlan {
    pub(crate) fn matches(&self, select: &Select) -> bool {
        self.root == select.root
            && self.nodes == select.nodes
            && self.order == select.order
            && self.filters == select.post_filters
    }
}
impl Select {
    pub(crate) fn result_plan(&self) -> ResultPlan {
        ResultPlan {
            nodes: self.nodes.clone(),
            root: self.root,
            order: self.order.clone(),
            filters: self.post_filters.clone(),
        }
    }
}

/// Parses `sql` with its bind parameters against `catalog`.
pub fn parse_select(sql: &str, binds: &[Bind], catalog: &Catalog) -> Result<Select, ParseError> {
    PreparedSelect::parse(sql, catalog)?.bind(binds, catalog)
}

impl PreparedSelect {
    pub(crate) fn parse(sql: &str, catalog: &Catalog) -> Result<Self, ParseError> {
        let mut parser = Parser {
            tokens: lex(sql)?,
            pos: 0,
            depth: 0,
            binds: Vec::new(),
        };
        let select = parser.select(catalog)?;
        parser.eat_sym(";");
        if !matches!(parser.peek(), Tok::Eof) {
            return parser.err(format!("unexpected trailing input: {}", parser.describe()));
        }
        Ok(select)
    }
}

/// A table of the `FROM` while the statement is read.
struct Building<'c> {
    table: &'c DbTable,
    alias: Option<String>,
    edge: Option<Edge>,
    /// Under a `LEFT` join, itself or an ancestor.
    left_joined: bool,
    conjuncts: Vec<Tagged>,
}

/// The tables of the `FROM` as a scope: a qualified column names its
/// table; a bare one is taken only when one table has it.
struct FromScope<'c> {
    tables: Vec<&'c DbTable>,
    aliases: Vec<Option<String>>,
    output: Vec<Projected>,
}

impl Scope for FromScope<'_> {
    fn resolve<'s>(&'s self, parser: &mut Parser) -> Result<(usize, &'s DbColumn), ParseError> {
        let at = parser.at();
        let mut names = parser.dotted(3)?;
        let name = names.pop().unwrap_or_default();
        if names.iter().any(|name| name.contains('.')) {
            return Err(ParseError {
                message: "a dot inside a table or schema qualifier is not supported".to_owned(),
                position: at,
            });
        }
        let qualifier = match names.as_slice() {
            [] => None,
            [table] => Some(table.clone()),
            [schema, table] if schema == "public" => Some(table.clone()),
            [schema, table] => Some(format!("{schema}.{table}")),
            _ => None,
        };
        let fail = |message: String| ParseError {
            message,
            position: at,
        };
        let candidates: Vec<usize> = (0..self.tables.len())
            .filter(|index| match &qualifier {
                Some(qualifier) => match &self.aliases[*index] {
                    Some(alias) => alias == qualifier,
                    None => super::names_table(self.tables[*index].name.as_str(), qualifier),
                },
                None => self
                    .output
                    .iter()
                    .any(|column| column.node == *index && column.column.as_str() == name),
            })
            .collect();
        match candidates.as_slice() {
            [node] => self.tables[*node]
                .column(&name)
                .map(|column| (*node, column))
                .ok_or_else(|| {
                    fail(format!(
                        "unknown column `{name}` on table `{}`",
                        self.tables[*node].name
                    ))
                }),
            [] => Err(fail(match qualifier {
                Some(qualifier) => format!("`{qualifier}` is not a table this statement reads"),
                None => format!("unknown column `{name}`"),
            })),
            _ => Err(fail(format!(
                "`{name}` is ambiguous: qualify it with its table"
            ))),
        }
    }
}

impl Parser {
    fn select(&mut self, catalog: &Catalog) -> Result<PreparedSelect, ParseError> {
        self.expect_word("SELECT")?;
        if self.is_word("DISTINCT") {
            return self.err("DISTINCT is not supported");
        }
        let projection_at = self.pos;
        self.skip_projection()?;
        self.expect_word("FROM")?;
        let mut building: Vec<Building<'_>> = Vec::new();
        let relation = self.from(catalog, &mut building)?;
        if self.is_sym(",") || self.is_word("CROSS") {
            return self.err("a CROSS join, or a FROM of several tables, is not supported");
        }
        refresh_nullable(&mut building, relation.root);
        let scope = scope_of(&building, relation.output.clone());
        let after_from = self.pos;
        self.pos = projection_at;
        let mut projection = self.projection(&scope)?;
        if projection == Projection::All && relation.merged {
            projection = Projection::Columns(relation.output.clone());
        }
        if !self.is_word("FROM") {
            return self.err("unsupported trailing projection expression");
        }
        self.pos = after_from;

        let mut post_filters = Vec::new();
        if self.eat_word("WHERE") {
            let filter = self.filter_in(&scope)?;
            for conjunct in filter.conjuncts() {
                if conjunct.names_no_table() {
                    building[relation.root].conjuncts.push(conjunct);
                    continue;
                }
                let Some(node) = conjunct.only_node() else {
                    return self.err("a WHERE condition naming several tables is not supported");
                };
                if building[node].left_joined {
                    post_filters.push((node, conjunct));
                } else {
                    building[node].conjuncts.push(conjunct);
                }
            }
        }
        let mut order = Vec::new();
        if self.eat_word("ORDER") {
            self.expect_word("BY")?;
            loop {
                order.push(self.order(&scope)?);
                if !self.eat_sym(",") {
                    break;
                }
            }
        }
        let limit = if self.eat_word("LIMIT") {
            self.count(u32::MAX)?
        } else {
            Count::Literal(u32::MAX)
        };
        let offset = if self.eat_word("OFFSET") {
            self.count(0)?
        } else {
            Count::Literal(0)
        };
        if self.is_word("FOR") {
            return self.err("a locking read (FOR UPDATE, FOR SHARE) is not supported");
        }
        let at = self.at();
        let root_order: Vec<OrderBy> = match order
            .iter()
            .filter(|clause| clause.node == relation.root)
            .count()
        {
            0 => vec![default_order(building[relation.root].table, at)?],
            _ => order
                .iter()
                .filter(|clause| clause.node == relation.root)
                .map(|clause| OrderBy::new(clause.column.clone(), clause.direction))
                .collect(),
        };
        for (index, node) in building.iter().enumerate() {
            let filter = Where::AND(node.conjuncts.iter().map(Tagged::validation).collect());
            let ordered: Vec<OrderBy> = order
                .iter()
                .filter(|clause| clause.node == index)
                .map(|clause| OrderBy::new(clause.column.clone(), clause.direction))
                .collect();
            check_exact(node.table, &filter, &ordered, at)?;
            if let Some(edge) = &node.edge {
                check_edge(building[edge.parent].table, node.table, edge, at)?;
            }
        }

        for (node, filter) in &post_filters {
            check_exact(building[*node].table, &filter.validation(), &[], at)?;
        }
        let nodes = parts(&building, relation.root)?;
        let query = tree(&building, relation.root, root_order, u32::MAX);
        let filters = building
            .iter()
            .zip(&nodes)
            .map(|(node, held)| (held.part, Tagged::And(node.conjuncts.clone())))
            .collect();
        Ok(PreparedSelect {
            base: Select {
                query,
                root: relation.root,
                post_filters: Vec::new(),
                nodes,
                projection,
                order,
                limit: u32::MAX,
                offset: 0,
            },
            filters,
            post_filters,
            limit,
            offset,
            parameters: self
                .tokens
                .iter()
                .filter_map(|token| match token.tok {
                    Tok::Param(number) => Some(number),
                    _ => None,
                })
                .max()
                .unwrap_or(0),
        })
    }

    /// Normalize SQL's join direction into the engine's rooted LEFT/INNER tree.
    fn from<'c>(
        &mut self,
        catalog: &'c Catalog,
        building: &mut Vec<Building<'c>>,
    ) -> Result<Relation, ParseError> {
        let mut left = self.item(catalog, building)?;
        loop {
            let natural = self.eat_word("NATURAL");
            let kind = if self.eat_word("INNER") {
                self.expect_word("JOIN")?;
                JoinKind::Inner
            } else if self.eat_word("LEFT") {
                self.eat_word("OUTER");
                self.expect_word("JOIN")?;
                JoinKind::Left
            } else if self.eat_word("RIGHT") {
                self.eat_word("OUTER");
                self.expect_word("JOIN")?;
                JoinKind::Right
            } else if self.eat_word("JOIN") {
                JoinKind::Inner
            } else if self.is_word("FULL") || self.is_word("CROSS") {
                return self.err("FULL and CROSS joins are not supported by the engine");
            } else if natural {
                return self.err("expected JOIN after NATURAL");
            } else {
                return Ok(left);
            };
            let right = self.item(catalog, building)?;
            let mut output = left.output.clone();
            output.extend(right.output.iter().cloned());
            let scope = scope_of(building, output);
            let names = if natural {
                let mut names = Vec::new();
                for column in &left.output {
                    if right
                        .output
                        .iter()
                        .any(|other| other.column == column.column)
                        && !names.contains(&column.column)
                    {
                        names.push(column.column.clone());
                    }
                }
                Some(names)
            } else if self.eat_word("USING") {
                self.expect_sym("(")?;
                let mut names = vec![ColumnName::from(self.ident()?)];
                while self.eat_sym(",") {
                    names.push(ColumnName::from(self.ident()?));
                }
                self.expect_sym(")")?;
                Some(names)
            } else {
                None
            };
            let (pairs, extra) = if let Some(names) = &names {
                if names
                    .iter()
                    .enumerate()
                    .any(|(index, name)| names[..index].contains(name))
                {
                    return self.err("a USING column is named twice");
                }
                if names.is_empty() {
                    return self.err("a NATURAL join without common columns requires a CROSS join");
                }
                let mut pairs = Vec::new();
                for name in names {
                    let l = output_column(&left.output, name, self.at())?;
                    let r = output_column(&right.output, name, self.at())?;
                    let lt = building[l.node]
                        .table
                        .column(name.as_str())
                        .ok_or(ParseError {
                            message: "unknown USING column".into(),
                            position: self.at(),
                        })?;
                    let rt = building[r.node]
                        .table
                        .column(name.as_str())
                        .ok_or(ParseError {
                            message: "unknown USING column".into(),
                            position: self.at(),
                        })?;
                    if lt.type_oid != rt.type_oid {
                        return self.err(
                            "USING column coercion between different source types is not supported",
                        );
                    }
                    pairs.push((l.node, l.column.clone(), r.node, r.column.clone()));
                }
                (pairs, Vec::new())
            } else {
                self.expect_word("ON")?;
                self.on_terms(&scope)?
            };
            let mut normalized = Vec::new();
            for (a, ac, b, bc) in pairs {
                if left.nodes.contains(&a) && right.nodes.contains(&b) {
                    normalized.push((a, ac, b, bc));
                } else if left.nodes.contains(&b) && right.nodes.contains(&a) {
                    normalized.push((b, bc, a, ac));
                } else {
                    return self.err("a join equality must connect its left and right relations");
                }
            }
            let Some((parent, _, child, _)) = normalized.first() else {
                return self.err("an ON must contain a column equality between its relations");
            };
            let (parent, child) = (*parent, *child);
            if normalized
                .iter()
                .any(|(p, _, c, _)| *p != parent || *c != child)
            {
                return self.err(
                    "join keys referencing multiple parent nodes are not supported by the engine",
                );
            }
            let parent_column =
                ColumnName::composite(normalized.iter().map(|(_, c, _, _)| c.as_str()));
            let column = ColumnName::composite(normalized.iter().map(|(_, _, _, c)| c.as_str()));
            if kind == JoinKind::Right {
                reroot(building, parent, left.root)?;
                building[parent].edge = Some(Edge {
                    parent: child,
                    parent_column: column,
                    column: parent_column,
                    inner: false,
                });
            } else {
                // Only matching rows of the right relation can join. A
                // nullable key inside that relation becomes its inner root.
                reroot(building, child, right.root)?;
                if kind == JoinKind::Inner {
                    promote_path(building, parent, left.root)?;
                }
                building[child].edge = Some(Edge {
                    parent,
                    parent_column,
                    column,
                    inner: kind == JoinKind::Inner,
                });
            }
            for condition in extra.into_iter().flat_map(Tagged::conjuncts) {
                let node = if condition.names_no_table() {
                    if kind == JoinKind::Right {
                        parent
                    } else {
                        child
                    }
                } else if let Some(node) = condition.only_node() {
                    node
                } else {
                    return self.err("an ON filter must name one table");
                };
                let safe = match kind {
                    JoinKind::Inner => left.nodes.contains(&node) || right.nodes.contains(&node),
                    JoinKind::Left => right.nodes.contains(&node),
                    JoinKind::Right => left.nodes.contains(&node),
                };
                // A filter inside a nullable subtree cannot be pushed below
                // that subtree's outer join without changing its meaning.
                let relation = if left.nodes.contains(&node) {
                    &left
                } else {
                    &right
                };
                let filter_root = match kind {
                    JoinKind::Right if left.nodes.contains(&node) => parent,
                    JoinKind::Inner | JoinKind::Left if right.nodes.contains(&node) => child,
                    _ => relation.root,
                };
                if !safe || nullable(building, node, filter_root) {
                    return self.err("an ON filter on a preserved or nullable parent requires an engine edge predicate");
                }
                building[node].conjuncts.push(condition);
            }
            let mut combined = Vec::new();
            if let Some(names) = &names {
                for name in names {
                    let source = if kind == JoinKind::Right {
                        &right.output
                    } else {
                        &left.output
                    };
                    combined.push(output_column(source, name, self.at())?.clone());
                }
                combined.extend(
                    left.output
                        .iter()
                        .filter(|column| !names.contains(&column.column))
                        .cloned(),
                );
                combined.extend(
                    right
                        .output
                        .iter()
                        .filter(|column| !names.contains(&column.column))
                        .cloned(),
                );
            } else {
                combined.extend(left.output.iter().chain(&right.output).cloned());
            }
            let root = if kind == JoinKind::Right {
                right.root
            } else {
                left.root
            };
            let merged = names.is_some() || left.merged || right.merged;
            left.nodes.extend(right.nodes);
            left.root = root;
            left.output = combined;
            left.merged = merged;
        }
    }

    fn item<'c>(
        &mut self,
        catalog: &'c Catalog,
        building: &mut Vec<Building<'c>>,
    ) -> Result<Relation, ParseError> {
        if self.eat_sym("(") {
            self.descend()?;
            let relation = self.from(catalog, building)?;
            self.expect_sym(")")?;
            self.depth -= 1;
            return Ok(relation);
        }
        let table = self.table(catalog)?;
        let explicit_alias = self.eat_word("AS");
        let alias = if explicit_alias
            || matches!(self.peek(), Tok::Quoted(_))
            || matches!(self.peek(), Tok::Word(word) if !["INNER","LEFT","RIGHT","FULL","OUTER","CROSS","JOIN","NATURAL","ON","USING","WHERE","ORDER","LIMIT","OFFSET","FOR","GROUP","HAVING","UNION"].iter().any(|keyword| word.eq_ignore_ascii_case(keyword)))
        {
            Some(self.ident()?)
        } else {
            None
        };
        if building.iter().any(|node| match (&node.alias, &alias) {
            (Some(a), Some(b)) => a == b,
            (None, None) => node.table.name == table.name,
            (Some(a), None) => super::names_table(table.name.as_str(), a),
            (None, Some(b)) => super::names_table(node.table.name.as_str(), b),
        }) {
            return self
                .err("a table reference is ambiguous; give each occurrence a distinct alias");
        }
        let index = building.len();
        let output = table
            .declared()
            .iter()
            .map(|column| Projected {
                node: index,
                column: column.clone(),
            })
            .collect();
        building.push(Building {
            table,
            alias,
            edge: None,
            left_joined: false,
            conjuncts: Vec::new(),
        });
        Ok(Relation {
            root: index,
            nodes: vec![index],
            output,
            merged: false,
        })
    }

    /// AND may contain key equalities and single-table filters in any order.
    fn on_terms(&mut self, scope: &FromScope<'_>) -> Result<OnTerms, ParseError> {
        let mut pairs = Vec::new();
        let mut filters = Vec::new();
        loop {
            let pos = self.pos;
            let depth = self.depth;
            let equality = (|| {
                let (a, ac) = scope.resolve(self)?;
                self.expect_sym("=")?;
                let (b, bc) = scope.resolve(self)?;
                if a == b {
                    return self.err("an edge must connect different table occurrences");
                }
                Ok((a, ac.name.clone(), b, bc.name.clone()))
            })();
            if let Ok(pair) = equality {
                pairs.push(pair);
            } else {
                self.pos = pos;
                self.depth = depth;
                match self.primary(scope) {
                    Ok(filter) => filters.push(filter),
                    Err(error) => {
                        self.pos = pos;
                        self.depth = depth;
                        if !self.eat_sym("(") {
                            return Err(error);
                        }
                        self.descend()?;
                        let (nested, extra) = self.on_terms(scope)?;
                        self.expect_sym(")")?;
                        self.depth -= 1;
                        pairs.extend(nested);
                        filters.extend(extra);
                    }
                }
            }
            if !self.eat_word("AND") {
                break;
            }
        }
        Ok((pairs, filters))
    }

    /// Steps over the projection to the `FROM`, which it must come before.
    fn skip_projection(&mut self) -> Result<(), ParseError> {
        let mut depth = 0usize;
        loop {
            match self.peek() {
                Tok::Eof => return self.err("expected FROM"),
                Tok::Word(word) if depth == 0 && word.eq_ignore_ascii_case("FROM") => {
                    return Ok(());
                }
                Tok::Sym("(") => depth += 1,
                Tok::Sym(")") => depth = depth.saturating_sub(1),
                _ => {}
            }
            self.bump();
        }
    }

    /// The projection, its columns resolved against the `FROM`.
    fn projection(&mut self, scope: &FromScope<'_>) -> Result<Projection, ParseError> {
        if self.eat_sym("*") {
            return Ok(Projection::All);
        }
        if self.is_word("COUNT") {
            self.bump();
            self.expect_sym("(")?;
            self.expect_sym("*")?;
            self.expect_sym(")")?;
            return Ok(Projection::CountStar);
        }
        let mut columns = Vec::new();
        let mut stars = 0usize;
        loop {
            let at = self.pos;
            if let Some(node) = self.qualified_star(scope)? {
                stars += 1;
                for column in scope.tables[node].declared() {
                    columns.push(Projected {
                        node,
                        column: column.clone(),
                    });
                }
            } else {
                self.pos = at;
                if self
                    .tokens
                    .get(self.pos + 1)
                    .is_some_and(|next| next.tok == Tok::Sym("("))
                {
                    return self.err(format!(
                        "a projection is columns of the tables, not {}(...)",
                        self.describe()
                    ));
                }
                let (node, column) = scope.resolve(self)?;
                columns.push(Projected {
                    node,
                    column: column.name.clone(),
                });
                if matches!(self.peek(), Tok::Quoted(_) | Tok::Word(_)) && !self.is_word("FROM") {
                    return self.err("a projected column cannot be renamed or computed");
                }
            }
            if !self.eat_sym(",") {
                break;
            }
        }
        if !self.is_word("FROM") {
            return self.err(format!(
                "a projection is columns of the tables, found {}",
                self.describe()
            ));
        }
        if stars == 1
            && scope.tables.len() == 1
            && columns.len() == scope.tables[0].declared().len()
        {
            return Ok(Projection::All);
        }
        Ok(Projection::Columns(columns))
    }

    /// Consumes `table.*` (or `schema.table.*`) when it is next; the table.
    fn qualified_star(&mut self, scope: &FromScope<'_>) -> Result<Option<usize>, ParseError> {
        let mut names = Vec::new();
        while matches!(self.peek(), Tok::Word(_) | Tok::Quoted(_)) {
            names.push(self.ident()?);
            if !self.eat_sym(".") {
                return Ok(None);
            }
            if self.eat_sym("*") {
                let named = match names.as_slice() {
                    [schema, name] if schema == "public" => name.clone(),
                    [schema, name] => format!("{schema}.{name}"),
                    _ => names.concat(),
                };
                return match scope
                    .tables
                    .iter()
                    .enumerate()
                    .find(|(node, table)| match &scope.aliases[*node] {
                        Some(alias) => alias == &named,
                        None => super::names_table(table.name.as_str(), &named),
                    })
                    .map(|(node, _)| node)
                {
                    Some(node) => Ok(Some(node)),
                    None => self.err(format!("`{named}` is not a table this statement reads")),
                };
            }
        }
        Ok(None)
    }

    /// One `ORDER BY` clause. Postgres puts `NULL`s last ascending and first
    /// descending, as the engine does; the other way round is refused.
    fn order(&mut self, scope: &FromScope<'_>) -> Result<Ordered, ParseError> {
        let (node, column) = scope.resolve(self)?;
        let direction = if self.eat_word("DESC") {
            Order::DESC
        } else {
            self.eat_word("ASC");
            Order::ASC
        };
        if self.eat_word("NULLS") {
            let first = if self.eat_word("FIRST") {
                true
            } else {
                self.expect_word("LAST")?;
                false
            };
            if first != (direction == Order::DESC) {
                return self.err("NULLS FIRST ascending or NULLS LAST descending is not supported");
            }
        }
        Ok(Ordered {
            node,
            column: column.name.clone(),
            direction,
        })
    }

    /// A row count: a non-negative integer, or a parameter bound to one;
    /// `null` for a parameter bound to `NULL` (no limit, no offset).
    fn count(&mut self, null: u32) -> Result<Count, ParseError> {
        let at = self.at();
        match self.peek().clone() {
            Tok::Num(number) => {
                self.bump();
                number
                    .parse::<u32>()
                    .map(Count::Literal)
                    .map_err(|_| ParseError {
                        message: "a LIMIT or OFFSET must be a row count".to_owned(),
                        position: at,
                    })
            }
            Tok::Param(number) => {
                self.bump();
                Ok(Count::Parameter { number, null, at })
            }
            _ => self.err("a LIMIT or OFFSET must be a row count"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinKind {
    Inner,
    Left,
    Right,
}
struct Relation {
    root: usize,
    nodes: Vec<usize>,
    output: Vec<Projected>,
    merged: bool,
}
type OnPair = (usize, ColumnName, usize, ColumnName);
type OnTerms = (Vec<OnPair>, Vec<Tagged>);
fn scope_of<'c>(building: &[Building<'c>], output: Vec<Projected>) -> FromScope<'c> {
    FromScope {
        tables: building.iter().map(|node| node.table).collect(),
        aliases: building.iter().map(|node| node.alias.clone()).collect(),
        output,
    }
}
fn output_column<'a>(
    output: &'a [Projected],
    name: &ColumnName,
    at: usize,
) -> Result<&'a Projected, ParseError> {
    let mut found = output.iter().filter(|column| column.column == *name);
    match (found.next(), found.next()) {
        (Some(column), None) => Ok(column),
        _ => Err(ParseError {
            message: format!("USING column `{name}` is missing or ambiguous"),
            position: at,
        }),
    }
}
fn nullable(building: &[Building<'_>], mut node: usize, root: usize) -> bool {
    while node != root {
        let Some(edge) = &building[node].edge else {
            return false;
        };
        if !edge.inner {
            return true;
        }
        node = edge.parent;
    }
    false
}
fn refresh_nullable(building: &mut [Building<'_>], root: usize) {
    for node in 0..building.len() {
        building[node].left_joined = nullable(building, node, root);
    }
}
fn promote_path(
    building: &mut [Building<'_>],
    mut node: usize,
    root: usize,
) -> Result<(), ParseError> {
    while node != root {
        let Some(edge) = building[node].edge.as_mut() else {
            return Err(ParseError {
                message: "disconnected join relation".into(),
                position: 0,
            });
        };
        edge.inner = true;
        node = edge.parent;
    }
    Ok(())
}
/// Re-root just the matched portion of a relation. An old nullable edge
/// becomes inner when traversed backward: its rows existed only with a
/// matching old parent. The new outer edge supplies null extension.
fn reroot(building: &mut [Building<'_>], node: usize, root: usize) -> Result<(), ParseError> {
    let mut path = Vec::new();
    let mut current = node;
    while current != root {
        let edge = building[current].edge.clone().ok_or(ParseError {
            message: "disconnected join relation".into(),
            position: 0,
        })?;
        let parent = edge.parent;
        path.push((current, edge));
        current = parent;
    }
    building[node].edge = None;
    for (child, edge) in path {
        building[edge.parent].edge = Some(Edge {
            parent: child,
            parent_column: edge.column,
            column: edge.parent_column,
            inner: true,
        });
    }
    Ok(())
}

/// Each table's part of the engine's tree: the root's joins are numbered
/// in the order the `FROM` names them, and so on down.
fn parts(building: &[Building<'_>], root: usize) -> Result<Vec<Node>, ParseError> {
    let mut nodes: Vec<Node> = building
        .iter()
        .map(|node| Node {
            table: node.table.name.clone(),
            part: QueryPart::main(),
            edge: node.edge.clone(),
        })
        .collect();
    fn assign(
        building: &[Building<'_>],
        nodes: &mut [Node],
        node: usize,
        path: &mut Vec<usize>,
    ) -> Result<(), ParseError> {
        let children: Vec<usize> = (0..building.len())
            .filter(|index| {
                building[*index]
                    .edge
                    .as_ref()
                    .is_some_and(|edge| edge.parent == node)
            })
            .collect();
        for (position, child) in children.into_iter().enumerate() {
            path.push(position);
            nodes[child].part = QueryPart::try_new(path).ok_or(ParseError {
                message: "join tree exceeds the engine's depth or width limit".into(),
                position: 0,
            })?;
            assign(building, nodes, child, path)?;
            path.pop();
        }
        Ok(())
    }
    assign(building, &mut nodes, root, &mut Vec::new())?;
    Ok(nodes)
}

/// The engine's query for the tree under `node`.
fn tree(
    building: &[Building<'_>],
    node: usize,
    order: Vec<OrderBy>,
    limit: u32,
) -> MultiTableReadQuery {
    let table = building[node].table;
    let joins: Vec<Join> = (0..building.len())
        .filter_map(|index| {
            let edge = building[index].edge.as_ref()?;
            (edge.parent == node).then(|| {
                let key: Vec<OrderBy> = building[index]
                    .table
                    .pkey
                    .iter()
                    .map(|column| OrderBy::new(column.clone(), Order::ASC))
                    .collect();
                let sub = tree(building, index, key, u32::MAX);
                if edge.inner {
                    Join::inner_from_main(sub, edge.parent_column.clone(), edge.column.clone())
                } else {
                    Join::left(sub, edge.parent_column.clone(), edge.column.clone())
                }
            })
        })
        .collect();
    let main = SingleTableReadQuery::new(
        table.name.clone(),
        Where::AND(
            building[node]
                .conjuncts
                .iter()
                .map(Tagged::validation)
                .collect(),
        ),
        order,
        limit,
    );
    let mut query = MultiTableReadQuery::new(main, joins);
    query.page = PageRead::Batched;
    query
}

/// Refuses an edge the engine would join otherwise than Postgres: columns
/// whose equality differs from Postgres's, or of different types.
fn check_edge(parent: &DbTable, child: &DbTable, edge: &Edge, at: usize) -> Result<(), ParseError> {
    for (left_name, right_name) in edge.parent_column.parts().zip(edge.column.parts()) {
        let (Some(left), Some(right)) = (parent.column(left_name), child.column(right_name)) else {
            return Err(ParseError {
                message: "a join column is unknown".into(),
                position: at,
            });
        };
        if comparable(left) == Comparable::Not || comparable(right) == Comparable::Not {
            return Err(ParseError {
                message: format!(
                    "`{}` = `{}` is compared by rules of Postgres's own the engine does not follow",
                    left.name, right.name
                ),
                position: at,
            });
        }
        if left.r#type == ValueType::Timestamp && left.type_oid != right.type_oid {
            return Err(ParseError {
                message: "timestamp and timestamptz joins depend on the session time zone"
                    .to_owned(),
                position: at,
            });
        }
        if !super::comparison_types(left.type_oid, right.type_oid)
            || !super::comparison_types(right.type_oid, left.type_oid)
        {
            return Err(ParseError {
                message: "the join types do not have a supported Postgres comparison".to_owned(),
                position: at,
            });
        }
        let same = left.r#type == right.r#type;
        if !same {
            return Err(ParseError {
                message: format!(
                    "`{}` and `{}` are of different types",
                    left.name, right.name
                ),
                position: at,
            });
        }
    }
    Ok(())
}

/// How the engine compares a column's values against Postgres.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Comparable {
    /// Equal and ordered as Postgres has them.
    Fully,
    /// Equal as Postgres has them; ordered otherwise.
    Equality,
    /// Neither: Postgres's equality differs (case, padding, its own rules).
    Not,
}

/// How a column of `column`'s type compares in the engine, against how
/// Postgres compares it.
fn comparable(column: &DbColumn) -> Comparable {
    match &column.r#type {
        ValueType::Int | ValueType::Bool => Comparable::Fully,
        // Postgres orders NaN above every number; the engine leaves it
        // unordered, so a float compares only for equality.
        ValueType::Float => Comparable::Equality,
        // Times are held to the millisecond and JSON as text: neither
        // compares as Postgres compares it.
        ValueType::Timestamp
        | ValueType::Date
        | ValueType::Datetime
        | ValueType::Json
        | ValueType::List(_)
        | ValueType::Map(_, _) => Comparable::Not,
        ValueType::String => match column.type_oid {
            oid::UUID => Comparable::Fully,
            oid::BPCHAR => Comparable::Not,
            oid::TEXT | oid::VARCHAR | oid::NAME | oid::CHAR | 0 => match &column.collation {
                Some(collation) if !collation.deterministic => Comparable::Not,
                Some(collation) if collation.orders_bytewise() => Comparable::Fully,
                _ => Comparable::Equality,
            },
            _ if column.collation.is_some() => Comparable::Not,
            // An enum: equal by its label, ordered by its declaration.
            _ => Comparable::Equality,
        },
    }
}

/// Refuses a filter or order the engine would answer otherwise than
/// Postgres: a comparison on a column whose equality differs from
/// Postgres's (`citext`, `char(n)`, a nondeterministic collation, a type
/// the engine does not interpret), a range or an `ORDER BY` on a column
/// it orders otherwise (text under a collation that does not order by
/// bytes, an enum, JSON), and a value of another type than the column's.
pub(super) fn check_exact(
    table: &DbTable,
    filter: &Where,
    order: &[OrderBy],
    at: usize,
) -> Result<(), ParseError> {
    let refuse = |message: String| {
        Err(ParseError {
            message,
            position: at,
        })
    };
    let column_of = |name: &ColumnName| {
        table.column(name.as_str()).ok_or(ParseError {
            message: format!("unknown column `{name}`"),
            position: at,
        })
    };
    let mut conditions = Vec::new();
    collect_conditions(filter, &mut conditions);
    for condition in conditions {
        use ComparisonOperator::*;
        let column = column_of(&condition.column)?;
        let ranged = matches!(condition.comparison_operator, GT | GTE | LT | LTE);
        match (condition.comparison_operator, comparable(column)) {
            (IS | IS_NOT, _) if condition.value.is_null() => continue,
            (_, Comparable::Not) => {
                return refuse(format!(
                    "`{}` is compared by rules of Postgres's own the engine does not follow",
                    column.name
                ));
            }
            (_, Comparable::Equality) if ranged => {
                return refuse(format!(
                    "`{}` is ordered by Postgres in a way the engine does not follow",
                    column.name
                ));
            }
            _ => {}
        }
        let values: Vec<&Value> = match &condition.value {
            Value::List(items) => items.iter().collect(),
            other => vec![other],
        };
        for value in values {
            if !fits(value, &column.r#type) {
                return refuse(format!(
                    "`{}` is compared with a value of another type: {value:?}",
                    column.name
                ));
            }
        }
    }
    for clause in order {
        let column = column_of(&clause.column)?;
        if comparable(column) != Comparable::Fully {
            return refuse(format!(
                "ORDER BY `{}`: Postgres orders it in a way the engine does not follow",
                column.name
            ));
        }
    }
    Ok(())
}

/// Every condition of a filter tree.
fn collect_conditions<'a>(filter: &'a Where, out: &mut Vec<&'a Condition>) {
    match filter {
        Where::Condition(condition) => out.push(condition),
        Where::AND(parts) | Where::OR(parts) => {
            for part in parts {
                collect_conditions(part, out);
            }
        }
    }
}

/// Whether `value` is of the type a column of `declared` holds, so that
/// the engine compares the two as Postgres does.
fn fits(value: &Value, declared: &ValueType) -> bool {
    matches!(
        (value, declared),
        (Value::Null, _)
            | (Value::Int(_), ValueType::Int)
            | (Value::Float(_), ValueType::Float)
            | (Value::Bool(_), ValueType::Bool)
            | (Value::String(_), ValueType::String)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Collation;

    /// No projection kind may silently discard expressions or aliases.
    #[test]
    fn trailing_projection_expressions_are_refused() {
        for sql in [
            "SELECT *, 42 AS extra FROM orders",
            "SELECT COUNT(*), 42 AS extra FROM orders",
            "SELECT COUNT(*) AS total FROM orders",
            "SELECT COUNT(*) + 1 FROM orders",
        ] {
            assert!(parse_select(sql, &[], &catalog()).is_err(), "{sql}");
        }
        assert!(parse_select("SELECT * FROM orders", &[], &catalog()).is_ok());
        assert!(parse_select("SELECT COUNT(*) FROM orders", &[], &catalog()).is_ok());
    }

    #[test]
    fn aliases_self_joins_composite_keys_and_post_filters_are_planned() {
        let sql = "SELECT a.id, b.id FROM orders a LEFT JOIN orders b ON b.id=a.id AND a.code=b.code WHERE b.id IS NULL ORDER BY b.id LIMIT 2 OFFSET 1";
        let select = parse(sql, &[]).unwrap();
        assert_eq!(select.nodes.len(), 2);
        assert!(select.nodes[1].edge.as_ref().unwrap().column.is_composite());
        assert_eq!(select.post_filters.len(), 1);
        assert_eq!(select.query.main_table.limit, u32::MAX);
        let select = parse(
            "SELECT * FROM orders a RIGHT JOIN users b ON a.id=b.order_id",
            &[],
        )
        .unwrap();
        assert_eq!(select.root, 1);
        assert!(select.nodes[1].part.is_main());
        assert_eq!(select.query.main_table.table.as_str(), "users");
        let select = parse("SELECT orders.id FROM orders LEFT JOIN users ON orders.id=users.order_id INNER JOIN billing.invoices ON users.id=billing.invoices.id", &[]).unwrap();
        assert!(select.nodes[1].edge.as_ref().unwrap().inner);
        for sql in [
            "SELECT id FROM orders a JOIN orders a ON a.id=a.id",
            "SELECT orders.id FROM orders a JOIN users b ON a.id=b.order_id",
            "SELECT * FROM orders JOIN users USING(id,id)",
        ] {
            assert!(parse(sql, &[]).is_err(), "{sql}");
        }
    }

    #[test]
    fn typed_null_counts_and_extra_parameters_are_refused() {
        let catalog = catalog();
        let null_text = Bind {
            type_oid: oid::TEXT,
            bytes: None,
        };
        for sql in [
            "SELECT id FROM orders ORDER BY id LIMIT $1",
            "SELECT id FROM orders ORDER BY id OFFSET $1",
            "SELECT id FROM orders WHERE created_at = $1",
        ] {
            assert!(
                parse_select(sql, std::slice::from_ref(&null_text), &catalog).is_err(),
                "{sql}"
            );
        }
        assert!(parse_select("SELECT id FROM orders", &[null_text], &catalog).is_err());
        for sql in [
            "SELECT id FROM orders WHERE created_at = '2026-01-01 00:00:00'",
            "SELECT id FROM orders WHERE created_at = 0",
        ] {
            assert!(parse_select(sql, &[], &catalog).is_err(), "{sql}");
        }
        let null_int = Bind {
            type_oid: oid::INT8,
            bytes: None,
        };
        assert!(
            parse_select(
                "SELECT id FROM orders ORDER BY id LIMIT $1",
                &[null_int],
                &catalog
            )
            .is_ok()
        );
    }

    fn catalog() -> Catalog {
        let c = Collation {
            name: "C".to_owned(),
            deterministic: true,
        };
        let english = Collation {
            name: "en_US.UTF-8".to_owned(),
            deterministic: true,
        };
        Catalog::new(vec![
            DbTable::new(
                "orders",
                ["id"],
                vec![
                    DbColumn::new("id", ValueType::Int).with_type_oid(oid::INT8),
                    DbColumn::new("merchant_id", ValueType::String)
                        .with_type_oid(oid::TEXT)
                        .with_collation(english),
                    DbColumn::new("code", ValueType::String)
                        .with_type_oid(oid::TEXT)
                        .with_collation(c.clone()),
                    DbColumn::new("amount", ValueType::Float).with_type_oid(oid::FLOAT8),
                    DbColumn::new("created_at", ValueType::Timestamp)
                        .with_type_oid(oid::TIMESTAMPTZ),
                    DbColumn::new("status", ValueType::String).with_type_oid(99_001),
                    DbColumn::new("email", ValueType::String)
                        .with_type_oid(99_002)
                        .with_collation(c.clone()),
                    DbColumn::new("country", ValueType::String)
                        .with_type_oid(oid::BPCHAR)
                        .with_collation(c),
                    DbColumn::new("span", ValueType::Json).with_type_oid(oid::JSON),
                ],
            ),
            DbTable::new(
                "users",
                ["id"],
                vec![
                    DbColumn::new("id", ValueType::Int).with_type_oid(oid::INT8),
                    DbColumn::new("order_id", ValueType::Int).with_type_oid(oid::INT8),
                    DbColumn::new("name", ValueType::String).with_type_oid(oid::TEXT),
                ],
            ),
            DbTable::new(
                "billing.invoices",
                ["id"],
                vec![DbColumn::new("id", ValueType::Int).with_type_oid(oid::INT8)],
            ),
        ])
    }

    fn text(value: &str) -> Bind {
        Bind {
            type_oid: oid::TEXT,
            bytes: Some(value.as_bytes().to_vec()),
        }
    }

    fn int8(value: i64) -> Bind {
        Bind {
            type_oid: oid::INT8,
            bytes: Some(value.to_be_bytes().to_vec()),
        }
    }

    fn parse(sql: &str, binds: &[Bind]) -> Result<Select, ParseError> {
        parse_select(sql, binds, &catalog())
    }

    #[test]
    fn reads_a_diesel_select_with_binds() {
        let select = parse(
            r#"SELECT "orders"."id", "orders"."amount" FROM "orders" WHERE (("orders"."merchant_id" = $1) AND ("orders"."id" > $2)) ORDER BY "orders"."id" DESC LIMIT $3 OFFSET $4"#,
            &[text("m1"), int8(10), int8(20), int8(5)],
        )
        .unwrap();
        assert_eq!(
            select.projection,
            Projection::Columns(vec![
                Projected {
                    node: 0,
                    column: ColumnName::from("id")
                },
                Projected {
                    node: 0,
                    column: ColumnName::from("amount")
                }
            ])
        );
        assert_eq!(select.limit, 20);
        assert_eq!(select.offset, 5);
        assert_eq!(select.query.main_table.limit, 25);
        assert_eq!(
            select.query.main_table.order_by,
            vec![OrderBy::new("id", Order::DESC)]
        );
        let unbounded = parse(
            r#"SELECT "orders"."id" FROM "orders" LIMIT $1 OFFSET $2"#,
            &[
                Bind {
                    type_oid: oid::INT8,
                    bytes: None,
                },
                Bind {
                    type_oid: oid::INT8,
                    bytes: None,
                },
            ],
        )
        .unwrap();
        assert_eq!(
            (unbounded.limit, unbounded.offset),
            (u32::MAX, 0),
            "NULL is no limit, no offset"
        );
        assert_eq!(
            select.query.main_table.filter,
            Where::AND(vec![
                Where::condition("merchant_id", ComparisonOperator::EQ, "m1"),
                Where::condition("id", ComparisonOperator::GT, 10),
            ])
        );
    }

    /// Postgres's binary form of a one-dimensional `int8[]`.
    fn int8_array(items: &[i64]) -> Bind {
        let mut bytes = Vec::new();
        for word in [1i32, 0, oid::INT8 as i32, items.len() as i32, 1] {
            bytes.extend_from_slice(&word.to_be_bytes());
        }
        for item in items {
            bytes.extend_from_slice(&8i32.to_be_bytes());
            bytes.extend_from_slice(&item.to_be_bytes());
        }
        Bind {
            type_oid: 1016,
            bytes: Some(bytes),
        }
    }

    #[test]
    fn reads_any_all_between_and_counts() {
        let ids = int8_array(&[1, 2]);
        let select = parse(
            r#"SELECT COUNT(*) FROM "orders" WHERE ("orders"."id" = ANY($1)) AND "orders"."id" != ALL($2) AND "orders"."id" BETWEEN $3 AND 100"#,
            &[ids.clone(), ids, int8(1)],
        )
        .unwrap();
        assert_eq!(select.projection, Projection::CountStar);
        assert_eq!(select.query.main_table.limit, u32::MAX);
        let list = Value::List(vec![Value::Int(1), Value::Int(2)]);
        assert_eq!(
            select.query.main_table.filter,
            Where::AND(vec![
                Where::condition("id", ComparisonOperator::IN, list.clone()),
                Where::condition("id", ComparisonOperator::NOT_IN, list),
                Where::condition("id", ComparisonOperator::GTE, Value::Int(1)),
                Where::condition("id", ComparisonOperator::LTE, Value::Int(100)),
            ])
        );
    }

    #[test]
    fn reads_qualified_names_and_whole_rows() {
        let select = parse(
            r#"SELECT "billing"."invoices".* FROM "billing"."invoices""#,
            &[],
        )
        .unwrap();
        assert_eq!(select.projection, Projection::All);
        assert_eq!(select.query.main_table.table.as_str(), "billing.invoices");
        assert!(
            parse(
                r#"SELECT * FROM "public"."orders" WHERE "orders"."id" = 1"#,
                &[]
            )
            .is_ok()
        );
        assert!(parse(r#"SELECT "other"."id" FROM "orders""#, &[]).is_err());
    }

    #[test]
    fn reads_a_join_tree_as_diesel_writes_it() {
        let select = parse(
            r#"SELECT "orders"."id", "users"."name" FROM ("orders" INNER JOIN "users" ON ("orders"."id" = "users"."order_id")) WHERE ("users"."name" = $1) ORDER BY "orders"."id" LIMIT $2"#,
            &[text("meera"), int8(3)],
        )
        .unwrap();
        assert_eq!(select.nodes.len(), 2);
        let edge = select.nodes[1].edge.as_ref().unwrap();
        assert_eq!(
            (
                edge.parent,
                edge.parent_column.as_str(),
                edge.column.as_str(),
                edge.inner
            ),
            (0, "id", "order_id", true)
        );
        assert_eq!(select.nodes[1].part, QueryPart::join(0));
        assert_eq!(select.query.main_table.limit, 3);
        let join = &select.query.joins[0];
        assert!(join.is_inner && join.main_drives());
        assert_eq!(
            join.sub.main_table.filter,
            Where::AND(vec![Where::condition(
                "name",
                ComparisonOperator::EQ,
                "meera"
            )])
        );
        assert_eq!(
            select.projection,
            Projection::Columns(vec![
                Projected {
                    node: 0,
                    column: ColumnName::from("id")
                },
                Projected {
                    node: 1,
                    column: ColumnName::from("name")
                },
            ])
        );
    }

    #[test]
    fn a_constant_condition_is_the_roots() {
        let select = parse(
            r#"SELECT "orders"."id" FROM ("orders" INNER JOIN "users" ON ("orders"."id" = "users"."order_id")) WHERE (FALSE AND "users"."name" = 'x')"#,
            &[],
        )
        .unwrap();
        assert_eq!(
            select.query.main_table.filter,
            Where::AND(vec![Where::OR(Vec::new())])
        );
    }

    #[test]
    fn a_left_join_takes_its_on_conditions_on_the_joined_table() {
        let select = parse(
            r#"SELECT * FROM ("orders" LEFT OUTER JOIN "users" ON (("orders"."id" = "users"."order_id") AND ("users"."name" = 'meera')))"#,
            &[],
        )
        .unwrap();
        let join = &select.query.joins[0];
        assert!(!join.is_inner);
        assert_eq!(
            join.sub.main_table.filter,
            Where::AND(vec![Where::condition(
                "name",
                ComparisonOperator::EQ,
                "meera"
            )])
        );
        assert_eq!(select.projection, Projection::All);
    }

    #[test]
    fn binds_take_the_column_type() {
        let select = parse(
            r#"SELECT * FROM "orders" WHERE "orders"."id" >= $1 AND "orders"."status" = $2"#,
            &[
                int8(7),
                Bind {
                    type_oid: 99_001,
                    bytes: Some(b"paid".to_vec()),
                },
            ],
        )
        .unwrap();
        assert_eq!(
            select.query.main_table.filter,
            Where::AND(vec![
                Where::condition("id", ComparisonOperator::GTE, 7),
                Where::condition("status", ComparisonOperator::EQ, "paid"),
            ])
        );
        let at = Bind {
            type_oid: oid::TIMESTAMPTZ,
            bytes: Some(1_000_000i64.to_be_bytes().to_vec()),
        };
        assert!(
            parse(
                r#"SELECT * FROM "orders" WHERE "orders"."created_at" >= $1"#,
                &[at]
            )
            .is_err(),
            "a time compares as the engine does not hold it"
        );
    }

    #[test]
    fn refuses_what_postgres_would_answer_otherwise() {
        for (sql, why) in [
            (
                r#"SELECT * FROM "orders" WHERE "orders"."merchant_id" > 'm'"#,
                "ordered",
            ),
            (
                r#"SELECT * FROM "orders" ORDER BY "orders"."merchant_id""#,
                "ORDER BY",
            ),
            (
                r#"SELECT * FROM "orders" ORDER BY "orders"."status""#,
                "ORDER BY",
            ),
            (
                r#"SELECT * FROM "orders" WHERE "orders"."email" = 'a@b'"#,
                "rules",
            ),
            (
                r#"SELECT * FROM "orders" WHERE "orders"."country" = 'IN'"#,
                "rules",
            ),
            (
                r#"SELECT * FROM "orders" WHERE "orders"."span" = 'x'"#,
                "rules",
            ),
            (
                r#"SELECT * FROM "orders" WHERE "orders"."id" = 'one'"#,
                "another type",
            ),
            (
                r#"SELECT * FROM "orders" ORDER BY "orders"."id" NULLS FIRST"#,
                "NULLS",
            ),
            (r#"SELECT DISTINCT "orders"."id" FROM "orders""#, "DISTINCT"),
            (
                r#"SELECT lower("orders"."code") FROM "orders""#,
                "projection",
            ),
            (r#"SELECT * FROM "orders" FOR UPDATE"#, "locking"),
            (
                r#"SELECT * FROM "orders" INNER JOIN "billing"."invoices" ON TRUE"#,
                "column",
            ),
            (
                r#"SELECT * FROM "orders" FULL JOIN "users" ON ("orders"."id" = "users"."id")"#,
                "FULL and CROSS",
            ),
            (
                r#"SELECT * FROM ("orders" INNER JOIN "orders" ON ("orders"."id" = "orders"."id"))"#,
                "ambiguous",
            ),
            (
                r#"SELECT * FROM ("orders" INNER JOIN "users" ON ("orders"."id" = "users"."order_id")) WHERE ("orders"."id" = 1 OR "users"."name" = 'x')"#,
                "several tables",
            ),
            (
                r#"SELECT "orders"."id" FROM ("orders" INNER JOIN "users" ON ("orders"."id" = "users"."order_id")) ORDER BY "users"."name" LIMIT 5"#,
                "ORDER BY",
            ),
            (
                r#"SELECT * FROM "orders" WHERE "orders"."id" = $1"#,
                "no value",
            ),
        ] {
            let error = parse(sql, &[]).expect_err(sql);
            assert!(error.message.contains(why), "{sql}: {}", error.message);
        }
        assert!(
            parse(
                r#"SELECT * FROM "orders" WHERE "orders"."code" > 'a' ORDER BY "orders"."code""#,
                &[]
            )
            .is_ok()
        );
        assert!(
            parse(
                r#"SELECT * FROM "orders" WHERE "orders"."merchant_id" IN ('a', 'b')"#,
                &[]
            )
            .is_ok()
        );
        assert!(
            parse(
                r#"SELECT * FROM "orders" WHERE "orders"."email" IS NULL"#,
                &[]
            )
            .is_ok()
        );
    }
}
