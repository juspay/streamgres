//! A `SELECT` as an ORM renders it, over one table or a tree of joined
//! tables, with its bind parameters, into the engine's query and what the
//! statement returns.
//!
//! ```text
//! select     := SELECT projection FROM from [WHERE filter]
//!               [ORDER BY order (, order)*] [LIMIT count] [OFFSET count]
//! from       := item ((INNER | LEFT [OUTER]) JOIN item ON on)*
//! item       := table | '(' from ')'
//! on         := ['('] column '=' column [AND filter] [')']
//! projection := '*' | table '.' '*' | COUNT '(' '*' ')' | column (, column)*
//! order      := column [ASC | DESC] [NULLS FIRST | NULLS LAST]
//! count      := integer | $n
//! ```
//!
//! Names may be quoted and qualified (`"schema"."table"."column"`), as
//! Diesel writes them, and the filter takes `$n` parameters wherever it
//! takes a literal, `= ANY($n)` and `!= ALL($n)` over an array parameter,
//! and `BETWEEN`.
//!
//! The `FROM` is read as a tree, as Diesel writes it: each `JOIN` links
//! the item on its right to one table on its left through an `ON`
//! equality of one column each side, an edge from the left table (the
//! parent) to the right item's first table (the child). Other conditions
//! of an `ON` belong to the child. Each top-level conjunct of the `WHERE`
//! goes to the one table it names. `ORDER BY` and `LIMIT` belong to the
//! joined rows; the engine follows the root's rows in that order, so a
//! `LIMIT` is taken only with an order on the root alone.
//!
//! The engine must answer exactly what Postgres would, so a statement is
//! refused, with the reason, when it compares or orders a column the
//! engine cannot compare the way Postgres does ([`check_exact`]), or
//! joins in a way the engine's tree does not express: `RIGHT`, `FULL` or
//! `CROSS` joins, a table joined twice, a condition naming two tables
//! other than an edge's equality, a `WHERE` condition on a table under a
//! `LEFT` join (it would turn the join inner), an `INNER JOIN` onto a
//! table under a `LEFT` join written after it rather than inside it, and
//! an `ON` condition on the parent of a `LEFT` join.

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
    /// An `INNER` join; a `LEFT` one otherwise.
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
    /// return after skipping its offset (`limit + offset`; no limit for a
    /// count).
    pub query: MultiTableReadQuery,
    /// The tables of the `FROM`, in the order it names them; the root
    /// first.
    pub nodes: Vec<Node>,
    pub projection: Projection,
    /// The statement's order of its joined rows.
    pub order: Vec<Ordered>,
    /// The statement's own `LIMIT`, `u32::MAX` for none.
    pub limit: u32,
    /// Rows the statement skips.
    pub offset: u32,
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
    edge: Option<Edge>,
    /// Under a `LEFT` join, itself or an ancestor.
    left_joined: bool,
    conjuncts: Vec<Tagged>,
}

/// The tables of the `FROM` as a scope: a qualified column names its
/// table; a bare one is taken only when one table has it.
struct FromScope<'c> {
    tables: Vec<&'c DbTable>,
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
                Some(qualifier) => self.tables[*index].name.as_str() == qualifier.as_str(),
                None => self.tables[*index].column(&name).is_some(),
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
        self.from(catalog, &mut building)?;
        if self.is_sym(",") || self.is_word("CROSS") {
            return self.err("a CROSS join, or a FROM of several tables, is not supported");
        }
        let scope = FromScope {
            tables: building.iter().map(|node| node.table).collect(),
        };
        let after_from = self.pos;
        self.pos = projection_at;
        let projection = self.projection(&scope)?;
        self.pos = after_from;

        if self.eat_word("WHERE") {
            let filter = self.filter_in(&scope)?;
            for conjunct in filter.conjuncts() {
                if conjunct.names_no_table() {
                    building[0].conjuncts.push(conjunct);
                    continue;
                }
                let Some(node) = conjunct.only_node() else {
                    return self.err("a WHERE condition naming several tables is not supported");
                };
                if building[node].left_joined {
                    return self.err(format!(
                        "a WHERE condition on `{}` would turn its LEFT JOIN inner; put it in the ON",
                        building[node].table.name
                    ));
                }
                building[node].conjuncts.push(conjunct);
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
        let statically_windowed = matches!(limit, Count::Literal(value) if value != u32::MAX)
            || matches!(offset, Count::Literal(value) if value != 0);
        if statically_windowed
            && projection != Projection::CountStar
            && order.iter().any(|clause| clause.node != 0)
        {
            return self
                .err("a LIMIT or OFFSET with an ORDER BY on a joined table is not supported");
        }
        let at = self.at();
        let root_order: Vec<OrderBy> = match order.iter().filter(|clause| clause.node == 0).count()
        {
            0 => vec![default_order(building[0].table, at)?],
            _ => order
                .iter()
                .filter(|clause| clause.node == 0)
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

        let nodes = parts(&building);
        let query = tree(&building, 0, root_order, u32::MAX);
        let filters = building
            .iter()
            .map(|node| (node.table.name.clone(), Tagged::And(node.conjuncts.clone())))
            .collect();
        Ok(PreparedSelect {
            base: Select {
                query,
                nodes,
                projection,
                order,
                limit: u32::MAX,
                offset: 0,
            },
            filters,
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

    /// The `FROM`: an item, then each join onto it.
    fn from<'c>(
        &mut self,
        catalog: &'c Catalog,
        building: &mut Vec<Building<'c>>,
    ) -> Result<usize, ParseError> {
        let root = self.item(catalog, building)?;
        loop {
            let inner = if self.eat_word("INNER") {
                self.expect_word("JOIN")?;
                true
            } else if self.eat_word("JOIN") {
                true
            } else if self.eat_word("LEFT") {
                self.eat_word("OUTER");
                self.expect_word("JOIN")?;
                false
            } else if self.is_word("RIGHT") || self.is_word("FULL") || self.is_word("CROSS") {
                return self.err("only INNER and LEFT joins are supported");
            } else {
                return Ok(root);
            };
            let child = self.item(catalog, building)?;
            self.expect_word("ON")?;
            self.on(building, child, inner)?;
        }
    }

    /// A table, or a parenthesised `FROM`; its first table.
    fn item<'c>(
        &mut self,
        catalog: &'c Catalog,
        building: &mut Vec<Building<'c>>,
    ) -> Result<usize, ParseError> {
        if self.eat_sym("(") {
            self.descend()?;
            let root = self.from(catalog, building)?;
            self.expect_sym(")")?;
            self.depth -= 1;
            return Ok(root);
        }
        let at = self.at();
        let table = self.table(catalog)?;
        if building.iter().any(|node| node.table.name == table.name) {
            return Err(ParseError {
                message: format!("`{}` is joined twice, which is not supported", table.name),
                position: at,
            });
        }
        building.push(Building {
            table,
            edge: None,
            left_joined: false,
            conjuncts: Vec::new(),
        });
        Ok(building.len() - 1)
    }

    /// The `ON` of the join onto `child`: the edge's equality, then any
    /// conditions on the child.
    fn on(
        &mut self,
        building: &mut [Building<'_>],
        child: usize,
        inner: bool,
    ) -> Result<(), ParseError> {
        let scope = FromScope {
            tables: building.iter().map(|node| node.table).collect(),
        };
        let mut open = 0usize;
        while self.eat_sym("(") {
            open += 1;
        }
        let at = self.at();
        let (first, first_column) = scope.resolve(self)?;
        self.expect_sym("=")?;
        let (second, second_column) = scope.resolve(self)?;
        let (parent, parent_column, child_column) = if second == child && first < child {
            (first, first_column, second_column)
        } else if first == child && second < child {
            (second, second_column, first_column)
        } else {
            return Err(ParseError {
                message:
                    "an ON must equate a column of the joined table with one of a table before it"
                        .to_owned(),
                position: at,
            });
        };
        let mut extra = Vec::new();
        loop {
            if open > 0 && self.eat_sym(")") {
                open -= 1;
            } else if self.eat_word("AND") {
                extra.push(self.primary(&scope)?);
            } else {
                break;
            }
        }
        if open > 0 {
            return self.err(format!("expected `)`, found {}", self.describe()));
        }
        if inner && building[parent].left_joined {
            return Err(ParseError {
                message: format!(
                    "an INNER JOIN onto `{}`, which is under a LEFT JOIN, drops the rows that join keeps; join it inside the LEFT JOIN's parentheses",
                    building[parent].table.name
                ),
                position: at,
            });
        }
        let edge = Edge {
            parent,
            parent_column: parent_column.name.clone(),
            column: child_column.name.clone(),
            inner,
        };
        let below_left = !inner || building[parent].left_joined;
        building[child].edge = Some(edge);
        if below_left {
            mark_left(building, child);
        }
        for condition in extra.into_iter().flat_map(Tagged::conjuncts) {
            match condition.only_node() {
                Some(node) if node == child => building[child].conjuncts.push(condition),
                Some(node) if inner && !building[node].left_joined => {
                    building[node].conjuncts.push(condition)
                }
                _ => {
                    return Err(ParseError {
                        message: "an ON condition must name only the joined table".to_owned(),
                        position: at,
                    });
                }
            }
        }
        Ok(())
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
                    .position(|table| table.name.as_str() == named)
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

/// Marks `node` and every table below it as under a `LEFT` join.
fn mark_left(building: &mut [Building<'_>], node: usize) {
    let mut marked = vec![node];
    while let Some(next) = marked.pop() {
        building[next].left_joined = true;
        for (index, other) in building.iter().enumerate() {
            if other.edge.as_ref().is_some_and(|edge| edge.parent == next) && !other.left_joined {
                marked.push(index);
            }
        }
    }
}

/// Each table's part of the engine's tree: the root's joins are numbered
/// in the order the `FROM` names them, and so on down.
fn parts(building: &[Building<'_>]) -> Vec<Node> {
    let mut nodes: Vec<Node> = building
        .iter()
        .map(|node| Node {
            table: node.table.name.clone(),
            part: QueryPart::main(),
            edge: node.edge.clone(),
        })
        .collect();
    fn assign(building: &[Building<'_>], nodes: &mut [Node], node: usize, path: &mut Vec<usize>) {
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
            if let Some(part) = QueryPart::try_new(path) {
                nodes[child].part = part;
            }
            assign(building, nodes, child, path);
            path.pop();
        }
    }
    assign(building, &mut nodes, 0, &mut Vec::new());
    nodes
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
    let (Some(left), Some(right)) = (
        parent.column(edge.parent_column.as_str()),
        child.column(edge.column.as_str()),
    ) else {
        return Err(ParseError {
            message: "a join column is unknown".to_owned(),
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
            message: "timestamp and timestamptz joins depend on the session time zone".to_owned(),
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
    let same = left.r#type == right.r#type
        || matches!(
            (&left.r#type, &right.r#type),
            (ValueType::Int, ValueType::Numeric) | (ValueType::Numeric, ValueType::Int)
        );
    if !same {
        return Err(ParseError {
            message: format!(
                "`{}` and `{}` are of different types",
                left.name, right.name
            ),
            position: at,
        });
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
        ValueType::Int
        | ValueType::Float
        | ValueType::Bool
        | ValueType::Numeric
        | ValueType::Timestamp
        | ValueType::Day
        | ValueType::Time
        | ValueType::Bytes
        | ValueType::Date
        | ValueType::Datetime => Comparable::Fully,
        ValueType::Json if column.type_oid == oid::JSON => Comparable::Not,
        ValueType::Json => Comparable::Equality,
        ValueType::String => match column.type_oid {
            oid::UUID => Comparable::Fully,
            oid::BPCHAR => Comparable::Not,
            oid::TEXT | oid::VARCHAR | oid::NAME | oid::CHAR | 0 => match &column.collation {
                Some(collation) if !collation.deterministic => Comparable::Not,
                Some(collation) if collation.orders_bytewise() => Comparable::Fully,
                _ => Comparable::Equality,
            },
            _ if column.collation.is_some() => Comparable::Not,
            _ => Comparable::Equality,
        },
        ValueType::List(_) | ValueType::Map(_, _) | ValueType::Raw => Comparable::Not,
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
            | (
                Value::Int(_) | Value::Numeric(_),
                ValueType::Int | ValueType::Numeric
            )
            | (Value::Float(_), ValueType::Float)
            | (Value::Bool(_), ValueType::Bool)
            | (Value::String(_), ValueType::String)
            | (Value::Json(_), ValueType::Json)
            | (Value::Timestamp(_), ValueType::Timestamp)
            | (Value::Day(_), ValueType::Day)
            | (Value::Time(_), ValueType::Time)
            | (Value::Bytes(_), ValueType::Bytes)
            | (Value::Date(_), ValueType::Date)
            | (Value::Datetime(_), ValueType::Datetime)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Collation, Numeric, Timestamp};

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

    #[test]
    fn numeric_float_comparisons_refuse_overflow_and_underflow() {
        let catalog = Catalog::new(vec![DbTable::new(
            "floats",
            ["id"],
            vec![
                DbColumn::new("id", ValueType::Int).with_type_oid(oid::INT8),
                DbColumn::new("f", ValueType::Float).with_type_oid(oid::FLOAT8),
            ],
        )]);
        for number in ["1e1000", "1e-1000"] {
            assert!(
                parse_select(
                    &format!("SELECT id FROM floats WHERE f = {number}"),
                    &[],
                    &catalog
                )
                .is_err()
            );
            let bind = Bind {
                type_oid: oid::NUMERIC,
                bytes: Some(Numeric::parse(number).unwrap().to_binary()),
            };
            assert!(parse_select("SELECT id FROM floats WHERE f = $1", &[bind], &catalog).is_err());
        }
        assert!(parse_select("SELECT id FROM floats WHERE f = 0.1", &[], &catalog).is_ok());
        let infinity = Bind {
            type_oid: oid::NUMERIC,
            bytes: Some(Numeric::Infinity.to_binary()),
        };
        assert!(parse_select("SELECT id FROM floats WHERE f = $1", &[infinity], &catalog).is_ok());
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
                    DbColumn::new("amount", ValueType::Numeric).with_type_oid(oid::NUMERIC),
                    DbColumn::new("created_at", ValueType::Timestamp)
                        .with_type_oid(oid::TIMESTAMPTZ),
                    DbColumn::new("status", ValueType::String).with_type_oid(99_001),
                    DbColumn::new("email", ValueType::String)
                        .with_type_oid(99_002)
                        .with_collation(c.clone()),
                    DbColumn::new("country", ValueType::String)
                        .with_type_oid(oid::BPCHAR)
                        .with_collation(c),
                    DbColumn::new("span", ValueType::Raw).with_type_oid(1186),
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

    #[test]
    fn reads_any_all_between_and_counts() {
        let ids = Bind {
            type_oid: 1016,
            bytes: Some(
                crate::sync::pg::binary::encode_array(&[Value::Int(1), Value::Int(2)], oid::INT8)
                    .unwrap(),
            ),
        };
        let select = parse(
            r#"SELECT COUNT(*) FROM "orders" WHERE ("orders"."id" = ANY($1)) AND "orders"."id" != ALL($2) AND "orders"."amount" BETWEEN $3 AND 100.50"#,
            &[
                ids.clone(),
                ids,
                Bind {
                    type_oid: oid::NUMERIC,
                    bytes: Some(Numeric::parse("1.5").unwrap().to_binary()),
                },
            ],
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
                Where::condition(
                    "amount",
                    ComparisonOperator::GTE,
                    Value::Numeric(Numeric::parse("1.5").unwrap())
                ),
                Where::condition(
                    "amount",
                    ComparisonOperator::LTE,
                    Value::Numeric(Numeric::parse("100.50").unwrap())
                ),
            ])
        );
        let Where::AND(parts) = &select.query.main_table.filter else {
            panic!("a conjunction");
        };
        assert!(
            matches!(&parts[3], Where::Condition(condition)
                if matches!(&condition.value, Value::Numeric(n) if n.to_string() == "100.50")),
            "a numeric literal keeps its digits and scale"
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
        let at = Timestamp::from_unix_millis(1_000).unwrap();
        let select = parse(
            r#"SELECT * FROM "orders" WHERE "orders"."created_at" >= $1 AND "orders"."status" = $2"#,
            &[
                Bind {
                    type_oid: oid::TIMESTAMPTZ,
                    bytes: Some(at.0.to_be_bytes().to_vec()),
                },
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
                Where::condition("created_at", ComparisonOperator::GTE, Value::Timestamp(at)),
                Where::condition("status", ComparisonOperator::EQ, "paid"),
            ])
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
                r#"SELECT * FROM "orders" RIGHT JOIN "users" ON ("orders"."id" = "users"."id")"#,
                "INNER and LEFT",
            ),
            (
                r#"SELECT * FROM ("orders" INNER JOIN "orders" ON ("orders"."id" = "orders"."id"))"#,
                "twice",
            ),
            (
                r#"SELECT * FROM ("orders" LEFT JOIN "users" ON ("orders"."id" = "users"."order_id")) WHERE "users"."name" = 'x'"#,
                "inner",
            ),
            (
                r#"SELECT * FROM ("orders" INNER JOIN "users" ON ("orders"."id" = "users"."order_id")) WHERE ("orders"."id" = 1 OR "users"."name" = 'x')"#,
                "several tables",
            ),
            (
                r#"SELECT "orders"."id" FROM ("orders" INNER JOIN "users" ON ("orders"."id" = "users"."order_id")) ORDER BY "users"."name" LIMIT 5"#,
                "joined table",
            ),
            (
                r#"SELECT * FROM (("orders" LEFT JOIN "users" ON ("orders"."id" = "users"."order_id")) INNER JOIN "billing"."invoices" ON ("users"."id" = "billing"."invoices"."id"))"#,
                "under a LEFT JOIN",
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
