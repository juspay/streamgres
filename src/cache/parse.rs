//! What a statement parses to, kept so that reads do not parse again.
//!
//! Two bounded memos, each sharded by hash: one keeps each statement's
//! shape (its SQL resolved against the catalog, binds still open), the
//! other the shape with one set of binds substituted. A statement read
//! with binds not seen before binds its kept shape, without lexing or
//! resolving names again; one read again with the same binds is found
//! whole.

use super::{CatalogVersion, state::value_bytes};
use crate::model::{Catalog, MultiTableReadQuery, Where};
use crate::parser::{Bind, PreparedSelect, Select};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

/// How many shards each memo has.
const SHARDS: usize = 16;
/// What one shard may hold, by [`Memo::insert`]'s estimate.
const BYTES_PER_SHARD: usize = 2 << 20;
/// How many statements one shard may hold.
const ENTRIES_PER_SHARD: usize = 512;
/// The longest statement the cache parses; a longer one is refused.
const MAX_SQL_BYTES: usize = 64 << 10;

/// A statement's shape, or why it is refused whatever its binds.
type Shape = Result<Arc<PreparedSelect>, Arc<str>>;
/// A statement with its binds, or why it is refused. Both follow from the
/// SQL, the binds and the catalog alone, so both are kept.
pub(super) type Parsed = Result<Arc<Select>, Arc<str>>;

/// What a shape is kept under.
#[derive(Clone)]
struct ShapeKey {
    sql: Box<str>,
    version: CatalogVersion,
}
/// What a statement with its binds is kept under.
struct BoundKey {
    sql: Box<str>,
    binds: Vec<Bind>,
    version: CatalogVersion,
}
/// One shard: entries by hash, with what they weigh. When full, an
/// eighth of its hashes give way, whichever they are.
struct Memo<K, V> {
    buckets: HashMap<u64, Vec<(K, V, usize)>>,
    bytes: usize,
}
impl<K, V> Default for Memo<K, V> {
    fn default() -> Self {
        Self {
            buckets: HashMap::new(),
            bytes: 0,
        }
    }
}
impl<K, V: Clone> Memo<K, V> {
    fn get(&self, hash: u64, matches: impl Fn(&K) -> bool) -> Option<V> {
        self.buckets
            .get(&hash)?
            .iter()
            .find(|(key, _, _)| matches(key))
            .map(|(_, value, _)| value.clone())
    }
    fn insert(&mut self, hash: u64, key: K, value: V, bytes: usize) {
        if bytes > BYTES_PER_SHARD {
            return;
        }
        while self.bytes.saturating_add(bytes) > BYTES_PER_SHARD
            || self.buckets.len() >= ENTRIES_PER_SHARD
        {
            let gone: Vec<_> = self
                .buckets
                .keys()
                .take((ENTRIES_PER_SHARD / 8).max(1))
                .copied()
                .collect();
            if gone.is_empty() {
                break;
            }
            for hash in gone {
                if let Some(bucket) = self.buckets.remove(&hash) {
                    for (_, _, bytes) in bucket {
                        self.bytes = self.bytes.saturating_sub(bytes);
                    }
                }
            }
        }
        self.bytes = self.bytes.saturating_add(bytes);
        self.buckets
            .entry(hash)
            .or_default()
            .push((key, value, bytes));
    }
}

/// Both memos.
pub(super) struct ParseMemo {
    shapes: [RwLock<Memo<ShapeKey, Shape>>; SHARDS],
    answers: [RwLock<Memo<BoundKey, Parsed>>; SHARDS],
}
impl ParseMemo {
    pub fn new() -> Self {
        Self {
            shapes: std::array::from_fn(|_| RwLock::default()),
            answers: std::array::from_fn(|_| RwLock::default()),
        }
    }
    /// What `sql` with `binds` parses to against `catalog`, from the
    /// memos when it can be.
    pub fn get_or_parse(
        &self,
        sql: &str,
        binds: &[Bind],
        version: &CatalogVersion,
        catalog: &Catalog,
    ) -> Parsed {
        if sql.len() > MAX_SQL_BYTES {
            return Err("the SQL exceeds the cache's 64 KiB statement limit".into());
        }
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        sql.hash(&mut hash);
        version.hash(&mut hash);
        let shape_hash = hash.finish();
        binds.hash(&mut hash);
        let bound_hash = hash.finish();
        let answer_shard = &self.answers[usize::try_from(bound_hash >> 60).unwrap_or(0) % SHARDS];
        let same_bound =
            |key: &BoundKey| &*key.sql == sql && key.binds == binds && key.version == *version;
        if let Some(answer) = answer_shard
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(bound_hash, same_bound)
        {
            return answer;
        }
        let shape = self.shape(sql, version, catalog, shape_hash);
        let answer = match shape {
            Ok(shape) => shape
                .bind(binds, catalog)
                .map(Arc::new)
                .map_err(|error| Arc::from(error.to_string())),
            Err(reason) => Err(reason),
        };
        let bytes = sql
            .len()
            .saturating_add(
                binds
                    .iter()
                    .map(|bind| 32 + bind.bytes.as_ref().map_or(0, Vec::len))
                    .sum::<usize>(),
            )
            .saturating_add(match &answer {
                Ok(select) => select_bytes(select),
                Err(reason) => reason.len(),
            });
        if bytes <= BYTES_PER_SHARD {
            let mut shard = answer_shard.write().unwrap_or_else(|p| p.into_inner());
            if shard.get(bound_hash, same_bound).is_none() {
                shard.insert(
                    bound_hash,
                    BoundKey {
                        sql: sql.into(),
                        binds: binds.to_vec(),
                        version: version.clone(),
                    },
                    answer.clone(),
                    bytes,
                );
            }
        }
        answer
    }
    fn shape(&self, sql: &str, version: &CatalogVersion, catalog: &Catalog, hash: u64) -> Shape {
        let shard = &self.shapes[usize::try_from(hash >> 60).unwrap_or(0) % SHARDS];
        let same = |key: &ShapeKey| &*key.sql == sql && key.version == *version;
        if let Some(shape) = shard
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(hash, same)
        {
            return shape;
        }
        // Only a genuinely new SQL shape takes this write lock. One compiler
        // wins if several threads encounter the shape at once.
        let mut shard = shard.write().unwrap_or_else(|p| p.into_inner());
        if let Some(shape) = shard.get(hash, same) {
            return shape;
        }
        let shape = PreparedSelect::parse(sql, catalog)
            .map(Arc::new)
            .map_err(|error| Arc::from(error.to_string()));
        // Column metadata and the syntax tree are bounded by the SQL length;
        // this deliberately conservative estimate includes their allocations.
        let bytes = sql.len().saturating_mul(64).saturating_add(1024);
        shard.insert(
            hash,
            ShapeKey {
                sql: sql.into(),
                version: version.clone(),
            },
            shape.clone(),
            bytes,
        );
        shape
    }
}
/// The layout and predicates retained with a composed result. Post-filters
/// can contain large bound values and belong in the same cache budget.
pub(super) fn plan_bytes(plan: &crate::parser::ResultPlan) -> usize {
    128usize
        .saturating_add(
            plan.nodes
                .iter()
                .map(|node| {
                    128 + node.table.as_str().len()
                        + node.edge.as_ref().map_or(0, |edge| {
                            edge.parent_column.as_str().len() + edge.column.as_str().len()
                        })
                })
                .sum::<usize>(),
        )
        .saturating_add(
            plan.order
                .iter()
                .map(|order| 64 + order.column.as_str().len())
                .sum::<usize>(),
        )
        .saturating_add(
            plan.filters
                .iter()
                .map(|(_, filter)| filter_bytes(filter))
                .sum::<usize>(),
        )
}

fn select_bytes(select: &Select) -> usize {
    256 + query_bytes(&select.query)
        + select.nodes.len() * 128
        + select.order.len() * 64
        + select
            .post_filters
            .iter()
            .map(|(_, filter)| filter_bytes(filter))
            .sum::<usize>()
        + match &select.projection {
            crate::parser::Projection::Columns(columns) => columns.len() * 64,
            _ => 0,
        }
}
pub(super) fn query_bytes(query: &MultiTableReadQuery) -> usize {
    256 + query.main_table.table.as_str().len()
        + filter_bytes(&query.main_table.filter)
        + query
            .joins
            .iter()
            .map(|join| query_bytes(&join.sub) + 128)
            .sum::<usize>()
}
fn filter_bytes(filter: &Where) -> usize {
    match filter {
        Where::Condition(condition) => {
            64 + condition.column.as_str().len() + value_bytes(&condition.value)
        }
        Where::AND(parts) | Where::OR(parts) => 32 + parts.iter().map(filter_bytes).sum::<usize>(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DbColumn, DbTable, ValueType};
    #[test]
    fn distinct_values_share_one_compiled_shape_and_keep_distinct_answers() {
        let catalog = Arc::new(Catalog::new(vec![DbTable::new(
            "t",
            ["id"],
            vec![DbColumn::new("id", ValueType::Int).with_type_oid(20)],
        )]));
        let version = CatalogVersion::of(&catalog);
        let memo = ParseMemo::new();
        for id in 0i64..100 {
            let binds = [Bind {
                type_oid: 20,
                bytes: Some(id.to_be_bytes().to_vec()),
            }];
            let select = memo
                .get_or_parse("SELECT id FROM t WHERE id = $1", &binds, &version, &catalog)
                .unwrap();
            assert!(
                format!("{:?}", select.query.main_table.filter).contains(&format!("Int({id})"))
            );
        }
        let shapes: usize = memo
            .shapes
            .iter()
            .map(|shard| shard.read().unwrap().buckets.len())
            .sum();
        assert_eq!(shapes, 1);
    }
    #[test]
    fn memo_budgets_are_enforced_before_retaining_an_entry() {
        let mut memo: Memo<u64, u64> = Memo::default();
        for id in 0..1000 {
            memo.insert(id, id, id, BYTES_PER_SHARD / 10);
            assert!(memo.bytes <= BYTES_PER_SHARD);
        }
        memo.insert(2000, 2000, 2000, BYTES_PER_SHARD + 1);
        assert!(memo.get(2000, |_| true).is_none());
    }
}
