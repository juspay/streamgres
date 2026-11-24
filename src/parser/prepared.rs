//! A statement's shape: its SQL parsed and resolved against the catalog
//! once, with its parameters left open, and bound to each read's values
//! without parsing again ([`PreparedSelect::bind`]). The engine's query is
//! built only from bound values.
use super::Select;
use super::{Bind, ParseError, Tagged, decode_bind, select};
use crate::ivm::QueryPart;
use crate::model::{
    Catalog, ColumnName, ComparisonOperator, Condition, DbColumn, MultiTableReadQuery, Value,
};

/// What a condition compares its column with.
#[derive(Debug, Clone)]
pub(super) enum Operand {
    /// A value written in the SQL.
    Literal(Value),
    /// Parameter `$number`, read as a value of `declared`'s type, or as a
    /// list of them for `ANY` / `ALL`; `at` is where it is in the SQL.
    Parameter {
        number: usize,
        declared: DbColumn,
        list: bool,
        at: usize,
    },
    /// An `IN` list.
    List(Vec<Operand>),
}
impl From<Value> for Operand {
    fn from(value: Value) -> Self {
        Self::Literal(value)
    }
}
impl Operand {
    fn bind(&self, binds: &[Bind]) -> Result<Value, ParseError> {
        match self {
            Self::Literal(value) => Ok(value.clone()),
            Self::List(items) => items
                .iter()
                .map(|item| item.bind(binds))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::List),
            Self::Parameter {
                number,
                declared,
                list,
                at,
            } => {
                let value = decode_bind(binds, *number, declared, *list, *at)?;
                // A NULL array is unknown under ANY/ALL and their negations.
                Ok(if *list && value.is_null() {
                    Value::List(vec![Value::Null])
                } else {
                    value
                })
            }
        }
    }
    /// The operand with every parameter `NULL`: what the shape's checks
    /// that do not depend on values see.
    pub(super) fn validation(&self) -> Value {
        match self {
            Self::Literal(value) => value.clone(),
            Self::Parameter { .. } => Value::Null,
            Self::List(items) => Value::List(items.iter().map(Self::validation).collect()),
        }
    }
}

/// A condition whose operand may still be a parameter.
#[derive(Debug, Clone)]
pub(super) struct Predicate {
    pub column: ColumnName,
    pub comparison_operator: ComparisonOperator,
    pub value: Operand,
}
impl Predicate {
    pub(super) fn new(
        column: impl Into<ColumnName>,
        comparison_operator: ComparisonOperator,
        value: impl Into<Operand>,
    ) -> Self {
        Self {
            column: column.into(),
            comparison_operator,
            value: value.into(),
        }
    }
    pub(super) fn bind(&self, binds: &[Bind]) -> Result<Condition, ParseError> {
        Ok(Condition {
            column: self.column.clone(),
            comparison_operator: self.comparison_operator,
            value: self.value.bind(binds)?,
        })
    }
}

/// A `LIMIT` or `OFFSET`: a number, or a parameter (`null` standing in
/// for a `NULL` one).
#[derive(Debug, Clone)]
pub(super) enum Count {
    Literal(u32),
    Parameter { number: usize, null: u32, at: usize },
}
impl Count {
    fn bind(&self, binds: &[Bind]) -> Result<u32, ParseError> {
        match self {
            Self::Literal(count) => Ok(*count),
            Self::Parameter { number, null, at } => {
                let error = || ParseError {
                    message: format!("${number}: a LIMIT or OFFSET must be a bound row count"),
                    position: *at,
                };
                let bind = number
                    .checked_sub(1)
                    .and_then(|index| binds.get(index))
                    .ok_or_else(error)?;
                use crate::sync::pg::binary::oid;
                if !matches!(bind.type_oid, oid::INT8 | oid::INT2 | oid::INT4) {
                    return Err(error());
                }
                match &bind.bytes {
                    None => Ok(*null),
                    Some(bytes) => {
                        match crate::sync::pg::binary::decode_scalar(bind.type_oid, bytes) {
                            Ok(Value::Int(count)) => u32::try_from(count).map_err(|_| error()),
                            _ => Err(error()),
                        }
                    }
                }
            }
        }
    }
}

/// A statement's shape: the parsed statement, each table occurrence's
/// filter (by engine part, including self-joins), post-join filters and
/// counts with parameters open, and how many parameters it takes.
#[derive(Debug, Clone)]
pub(crate) struct PreparedSelect {
    pub(super) base: Select,
    pub(super) filters: Vec<(QueryPart, Tagged)>,
    pub(super) post_filters: Vec<(usize, Tagged)>,
    pub(super) limit: Count,
    pub(super) offset: Count,
    pub(super) parameters: usize,
}
impl PreparedSelect {
    /// The statement with `binds` substituted, each checked against its
    /// column's type, without lexing or resolving names again.
    pub(crate) fn bind(&self, binds: &[Bind], catalog: &Catalog) -> Result<Select, ParseError> {
        if binds.len() != self.parameters {
            return Err(ParseError {
                message: if binds.len() < self.parameters {
                    format!(
                        "no value is bound for all {} parameters (received {})",
                        self.parameters,
                        binds.len()
                    )
                } else {
                    format!(
                        "expected {} bind parameters, received {}",
                        self.parameters,
                        binds.len()
                    )
                },
                position: 0,
            });
        }
        let mut select = self.base.clone();
        select.limit = self.limit.bind(binds)?;
        select.offset = self.offset.bind(binds)?;
        self.bind_tree(&mut select.query, binds, catalog, &mut Vec::new())?;
        for (node, filter) in &self.post_filters {
            let filter = filter.bind(binds)?;
            let table = catalog
                .table(select.nodes[*node].table.as_str())
                .ok_or(ParseError {
                    message: "post-filter table is not in the catalog".into(),
                    position: 0,
                })?;
            select::check_exact(table, &filter, &[], 0)?;
            select.post_filters.push((*node, filter));
        }
        // Global sorting or a post-join predicate needs the complete
        // candidate relation. Window only after composition; the cache's
        // existing row/byte budgets bound that work.
        select.query.main_table.limit = if select.projection == super::Projection::CountStar
            || !select.post_filters.is_empty()
            || select.order.iter().any(|order| order.node != select.root)
        {
            u32::MAX
        } else {
            select.limit.saturating_add(select.offset)
        };
        Ok(select)
    }
    fn bind_tree(
        &self,
        query: &mut MultiTableReadQuery,
        binds: &[Bind],
        catalog: &Catalog,
        path: &mut Vec<usize>,
    ) -> Result<(), ParseError> {
        let name = &query.main_table.table;
        let part = QueryPart::try_new(path).ok_or(ParseError {
            message: "join tree exceeds the engine's depth or width limit".into(),
            position: 0,
        })?;
        let (_, filter) = self
            .filters
            .iter()
            .find(|(held, _)| *held == part)
            .ok_or_else(|| ParseError {
                message: "prepared query has no filter for its table".into(),
                position: 0,
            })?;
        query.main_table.filter = filter.bind(binds)?;
        let table = catalog.table(name.as_str()).ok_or_else(|| ParseError {
            message: format!("unknown table `{name}`"),
            position: 0,
        })?;
        // Schema checks were done on compilation; values still need their type check.
        select::check_exact(table, &query.main_table.filter, &[], 0)?;
        for (index, edge) in query.joins.iter_mut().enumerate() {
            path.push(index);
            self.bind_tree(&mut edge.sub, binds, catalog, path)?;
            path.pop();
        }
        Ok(())
    }
}
