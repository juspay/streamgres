//! Multi-table subscriptions: a tree of single-table parts joined by LEFT
//! and RIGHT edges, maintained over the single-table engine.
//!
//! A [`MultiTableReadQuery`] is a tree: every node is a single-table query,
//! every edge a join, and a child is itself a full multi-table query. Each
//! node registers as one inner single-table subscription — a *part*,
//! addressed by its [`QueryPart`] path — and the client receives every
//! operation tagged with its part, keeps one frame per part, and composes
//! the join itself.
//!
//! # Driver and driven
//!
//! Every edge has a **driver** side, whose rows decide which join values
//! are *referenced*, and a **driven** side, whose part carries the
//! restriction `driven_column IN (referenced values)` inside its filter.
//! A LEFT edge preserves the parent, so the parent drives and the child is
//! driven; a RIGHT edge preserves the child, so the child drives and the
//! parent is driven. Because the restriction lives inside the driven
//! part's filter, writes on the driven table route natively through the
//! single engine: a row matching the filter (restriction included) fires
//! an `Add`, a held row moving out fires a `Delete` via membership, and an
//! unreferenced row never fires at all. The join layer never inspects
//! driven-side writes; it forwards them and keeps its counts.
//!
//! A node can be driven by several edges on one column — a LEFT parent
//! above it and a RIGHT child below it, both on `id` — and its filter then
//! carries one `IN` leaf per driven column holding the **intersection** of
//! the driving edges' referenced values, recomputed on every change.
//!
//! # Counts, crossings, cascades
//!
//! Per edge and per join value the layer keeps `left` (driver rows
//! carrying the value), `right` (driven rows held for it; observational),
//! and the referenced list in first-referenced order. Only zero crossings
//! of `left` act, and only when they change the driven part's `IN` leaf:
//! the leaf is edited in place ([`SingleTableIVM::replace_condition`]) and
//! the value's driven rows are fetched (one narrowed storage query) or
//! pruned from current data. Rows a fetch brings in are **arrivals** at
//! the driven node and rows a prune removes are **departures**, and the
//! driven node may itself drive further edges, so the same handling
//! cascades down the tree — the recursion that makes nesting work with
//! one code path.
//!
//! # Order
//!
//! Registration is a post-order walk: a node's RIGHT children register
//! first (their rows supply its `IN` values), then the node, then its LEFT
//! children (restricted by the node's rows); during that walk crossings
//! only record counts, since every part is registered with its full list.
//! Within one write, each part's native operations are forwarded with
//! every **driven part before its driver** — LEFT children before their
//! parent, a RIGHT child after its parent — because handling a driver's
//! operation may fetch into or prune the driven frame, and a stale driven
//! operation forwarded after that prune would resurrect a row on the
//! client. An in-place replacement arrives as an adjacent `Delete(old)` +
//! `Add(new)` pair and is diffed per edge, so a rewrite that keeps a join
//! value never swings its count through zero.
//!
//! Part ids are `{uuid}_r` for the root and `{uuid}_r_{i}_{j}…` down the
//! path; they are looked up in a map, never parsed.

use std::collections::HashMap;
use std::rc::Rc;

use super::stats::IvmStats;
use super::storage::Storage;
use super::{QueryId, SingleTableIVM, SingleTableUpdate};
use crate::model::{
    ColumnName, ComparisonOperator, Condition, DataFrameKey, DataFrameOperation, DataFrameRow,
    MultiTableReadQuery, SingleTableReadQuery, TableName, Value, Where, WriteQuery,
};

/// Which node of a subscription's join tree a part is: the path of join
/// indices from the root, a node's left joins numbered first and its right
/// joins after them. The root is the empty path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct QueryPart(pub Vec<usize>);

impl QueryPart {
    /// The root part.
    pub fn main() -> Self {
        QueryPart(Vec::new())
    }

    /// The root's `index`-th join.
    pub fn join(index: usize) -> Self {
        QueryPart(vec![index])
    }

    /// Whether this is the root.
    pub fn is_main(&self) -> bool {
        self.0.is_empty()
    }

    /// The join index of a first-level part; `None` for the root and for
    /// nested parts.
    pub fn join_index(&self) -> Option<usize> {
        match self.0.as_slice() {
            [index] => Some(*index),
            _ => None,
        }
    }

    /// The `index`-th child of this part.
    fn child(&self, index: usize) -> Self {
        let mut path = self.0.clone();
        path.push(index);
        QueryPart(path)
    }
}

/// One operation for one part of one multi-table subscription — the unit
/// the transport pushes to the subscribed client.
///
/// - `query`: which subscription (the external id the client registered).
/// - `table`: the table the operation lands on.
/// - `part`: which node of the tree produced it — kept beside the table
///   because a self-join makes the table alone ambiguous.
/// - `op`: the delta itself.
#[derive(Debug, Clone, PartialEq)]
pub struct MultiTableUpdate {
    pub query: QueryId,
    pub table: TableName,
    pub part: QueryPart,
    pub op: DataFrameOperation,
}

/// Which side of an edge is preserved: LEFT keeps the parent (the parent
/// drives), RIGHT keeps the child (the child drives).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinKind {
    Left,
    Right,
}

/// Per-edge reference state.
///
/// - `left`: how many driver rows carry each join value.
/// - `right`: how many driven rows the driven part holds per value;
///   observational, kept for the `left == 0 ⇒ right == 0` invariant.
/// - `referenced`: the values with `left > 0`, in first-referenced order.
///
/// Entries leave `left` / `right` when they reach zero, so the maps only
/// hold live values.
#[derive(Default)]
struct JoinKeyCounts {
    left: HashMap<Value, u64>,
    right: HashMap<Value, u64>,
    referenced: Vec<Value>,
}

/// One join edge of a registered tree, with its direction made explicit
/// through [`Edge::driven`].
struct Edge {
    kind: JoinKind,
    parent: QueryPart,
    child: QueryPart,
    parent_column: ColumnName,
    child_column: ColumnName,
    counts: JoinKeyCounts,
}

impl Edge {
    /// The part whose filter carries this edge's `IN` leaf.
    fn driven(&self) -> &QueryPart {
        match self.kind {
            JoinKind::Left => &self.child,
            JoinKind::Right => &self.parent,
        }
    }

    /// The column the `IN` leaf is on.
    fn driven_column(&self) -> &ColumnName {
        match self.kind {
            JoinKind::Left => &self.child_column,
            JoinKind::Right => &self.parent_column,
        }
    }

    /// The join column of `part`, which must be one end of the edge.
    fn column_of(&self, part: &QueryPart) -> &ColumnName {
        if *part == self.parent {
            &self.parent_column
        } else {
            &self.child_column
        }
    }
}

/// One node of a registered tree: its inner part and its place among the
/// edges.
struct Node {
    part_id: QueryId,
    query: SingleTableReadQuery,
    parent: Option<usize>,
    children: Vec<usize>,
    registered: bool,
}

/// One registered multi-table subscription.
///
/// - `spec`: the tree as registered.
/// - `nodes`: every part by path.
/// - `edges`: every join edge; nodes refer to them by index.
/// - `rank`: forwarding order of the parts, every driven part before its
///   driver (see the module docs).
struct Tree {
    spec: Rc<MultiTableReadQuery>,
    nodes: HashMap<QueryPart, Node>,
    edges: Vec<Edge>,
    rank: HashMap<QueryPart, usize>,
}

/// The join layer. Owns the inner [`SingleTableIVM`] exclusively, so its
/// part ids cannot collide with anything registered from outside; storage
/// lives inside the inner engine.
///
/// - `single`: the inner engine holding every part's routing and the
///   shared per-table frames.
/// - `trees`: external id → the registered tree.
/// - `parts`: inner part id → (external id, part), the reverse map every
///   routed operation goes through.
pub struct MultiTableIVM {
    single: SingleTableIVM,
    trees: HashMap<QueryId, Tree>,
    parts: HashMap<QueryId, (QueryId, QueryPart)>,
}

/// The inner id of part `part` of subscription `uuid`: the external id, a
/// `_r` marker, then one `_{index}` per step of the path. Distinct
/// (subscription, part) pairs never collide, whatever the external ids
/// contain, because the path segments are digits only.
fn part_id(uuid: &QueryId, part: &QueryPart) -> QueryId {
    let mut id = format!("{uuid}_r");
    for index in &part.0 {
        id.push('_');
        id.push_str(&index.to_string());
    }
    QueryId::from(id)
}

/// The row's value in `column`; a missing column joins like `NULL` (never).
fn join_value(row: &DataFrameRow, column: &ColumnName) -> Value {
    row.data.get(column.as_str()).cloned().unwrap_or(Value::Null)
}

/// The `IN` leaf restricting a driven part to the referenced values; an
/// empty list matches nothing.
fn in_condition(column: &ColumnName, values: &[Value]) -> Condition {
    Condition::new(
        column.clone(),
        ComparisonOperator::IN,
        Value::List(values.to_vec()),
    )
}

/// Build the node and edge tables of a spec.
fn build_tree(uuid: &QueryId, spec: &MultiTableReadQuery) -> Tree {
    let mut tree = Tree {
        spec: Rc::new(spec.clone()),
        nodes: HashMap::new(),
        edges: Vec::new(),
        rank: HashMap::new(),
    };
    add_node(&mut tree, uuid, spec, QueryPart::main(), None);
    let mut order = Vec::new();
    forwarding_order(&tree, &QueryPart::main(), &mut order);
    tree.rank = order
        .into_iter()
        .enumerate()
        .map(|(rank, part)| (part, rank))
        .collect();
    tree
}

/// Add `spec`'s node at `part` and, recursively, its children.
fn add_node(
    tree: &mut Tree,
    uuid: &QueryId,
    spec: &MultiTableReadQuery,
    part: QueryPart,
    parent: Option<usize>,
) {
    let mut node = Node {
        part_id: part_id(uuid, &part),
        query: spec.main_table.clone(),
        parent,
        children: Vec::new(),
        registered: false,
    };
    let joins = spec
        .left_joins
        .iter()
        .map(|join| (JoinKind::Left, join))
        .chain(spec.right_joins.iter().map(|join| (JoinKind::Right, join)));
    for (index, (kind, join)) in joins.enumerate() {
        let child = part.child(index);
        let edge = tree.edges.len();
        tree.edges.push(Edge {
            kind,
            parent: part.clone(),
            child: child.clone(),
            parent_column: join.main_table_column.clone(),
            child_column: join.sub_table_column.clone(),
            counts: JoinKeyCounts::default(),
        });
        node.children.push(edge);
        add_node(tree, uuid, &join.sub, child, Some(edge));
    }
    tree.nodes.insert(part, node);
}

/// Append the subtree at `part` in forwarding order: LEFT subtrees (driven
/// by this node) first, the node, then RIGHT subtrees (which drive it).
fn forwarding_order(tree: &Tree, part: &QueryPart, out: &mut Vec<QueryPart>) {
    let node = &tree.nodes[part];
    for &edge in &node.children {
        if tree.edges[edge].kind == JoinKind::Left {
            forwarding_order(tree, &tree.edges[edge].child, out);
        }
    }
    out.push(part.clone());
    for &edge in &node.children {
        if tree.edges[edge].kind == JoinKind::Right {
            forwarding_order(tree, &tree.edges[edge].child, out);
        }
    }
}

/// The edges driving `part` on `column`, in edge order.
fn driving_edges(tree: &Tree, part: &QueryPart, column: &ColumnName) -> Vec<usize> {
    tree.edges
        .iter()
        .enumerate()
        .filter(|(_, edge)| edge.driven() == part && edge.driven_column() == column)
        .map(|(index, _)| index)
        .collect()
}

/// The values of `part`'s `IN` leaf on `column`: the intersection of the
/// referenced lists of every edge driving it there, in the first edge's
/// order.
fn leaf_values(tree: &Tree, part: &QueryPart, column: &ColumnName) -> Vec<Value> {
    let driving = driving_edges(tree, part, column);
    let Some((first, rest)) = driving.split_first() else {
        return Vec::new();
    };
    tree.edges[*first]
        .counts
        .referenced
        .iter()
        .filter(|value| {
            rest.iter()
                .all(|edge| tree.edges[*edge].counts.left.contains_key(*value))
        })
        .cloned()
        .collect()
}

/// The distinct columns on which `part` is driven, in edge order.
fn driven_columns(tree: &Tree, part: &QueryPart) -> Vec<ColumnName> {
    let mut columns: Vec<ColumnName> = Vec::new();
    for edge in &tree.edges {
        if edge.driven() == part && !columns.contains(edge.driven_column()) {
            columns.push(edge.driven_column().clone());
        }
    }
    columns
}

/// What `part` does on each edge it touches: `(edge, drives, column)`
/// where `drives` says whether the part is the edge's driver and `column`
/// is the part's own join column on that edge.
fn edge_steps(tree: &Tree, part: &QueryPart) -> Vec<(usize, bool, ColumnName)> {
    let node = &tree.nodes[part];
    node.children
        .iter()
        .chain(node.parent.iter())
        .map(|&edge| {
            let e = &tree.edges[edge];
            (edge, e.driven() != part, e.column_of(part).clone())
        })
        .collect()
}

impl MultiTableIVM {
    /// An empty join layer; the inner engine reads initial data and
    /// fetches from `storage`.
    pub fn new(storage: Rc<dyn Storage>) -> Self {
        MultiTableIVM {
            single: SingleTableIVM::new(storage),
            trees: HashMap::new(),
            parts: HashMap::new(),
        }
    }

    /// Register a multi-table subscription under `query_uuid`, returning
    /// its initial snapshot as operations, parts in registration order
    /// (post-order over the tree; see the module docs). Re-registering the
    /// identical spec is a no-op returning no operations; a changed spec
    /// replaces the subscription.
    pub fn register_query(
        &mut self,
        query_uuid: impl Into<QueryId>,
        query: MultiTableReadQuery,
    ) -> Vec<MultiTableUpdate> {
        let query_uuid = query_uuid.into();
        if self
            .trees
            .get(&query_uuid)
            .is_some_and(|tree| *tree.spec == query)
        {
            return Vec::new();
        }
        if self.trees.contains_key(&query_uuid) {
            self.unregister_query(query_uuid.as_str());
        }
        let tree = build_tree(&query_uuid, &query);
        for (part, node) in &tree.nodes {
            self.parts
                .insert(node.part_id.clone(), (query_uuid.clone(), part.clone()));
        }
        self.trees.insert(query_uuid.clone(), tree);
        let mut out = Vec::new();
        self.register_part(&query_uuid, QueryPart::main(), &mut out);
        out
    }

    /// Register the subtree at `part` in post-order: RIGHT children, the
    /// node itself with its full `IN` restrictions, then LEFT children.
    fn register_part(&mut self, uuid: &QueryId, part: QueryPart, out: &mut Vec<MultiTableUpdate>) {
        let (right_children, left_children, own, part_id) = {
            let tree = &self.trees[uuid];
            let node = &tree.nodes[&part];
            let children = |kind: JoinKind| -> Vec<QueryPart> {
                node.children
                    .iter()
                    .filter(|&&edge| tree.edges[edge].kind == kind)
                    .map(|&edge| tree.edges[edge].child.clone())
                    .collect()
            };
            (
                children(JoinKind::Right),
                children(JoinKind::Left),
                node.query.clone(),
                node.part_id.clone(),
            )
        };
        for child in right_children {
            self.register_part(uuid, child, out);
        }
        let limit = if part.is_main() { own.limit } else { u32::MAX };
        let query = SingleTableReadQuery {
            filter: self.restricted_filter(uuid, &part),
            limit,
            ..own.clone()
        };
        let ops = self.single.register_query(part_id, query, None);
        if let Some(node) = self
            .trees
            .get_mut(uuid)
            .and_then(|tree| tree.nodes.get_mut(&part))
        {
            node.registered = true;
        }
        for op in ops {
            out.push(MultiTableUpdate {
                query: uuid.clone(),
                table: own.table.clone(),
                part: part.clone(),
                op: op.clone(),
            });
            if let DataFrameOperation::Add(_, row) = &op {
                self.arrived(uuid, &part, row, out);
            }
        }
        for child in left_children {
            self.register_part(uuid, child, out);
        }
    }

    /// A part's registered filter: its own `WHERE` plus one `IN` leaf per
    /// column it is driven on.
    fn restricted_filter(&self, uuid: &QueryId, part: &QueryPart) -> Where {
        let tree = &self.trees[uuid];
        let mut parts = vec![tree.nodes[part].query.filter.clone()];
        for column in driven_columns(tree, part) {
            let values = leaf_values(tree, part, &column);
            parts.push(Where::Condition(in_condition(&column, &values)));
        }
        Where::AND(parts)
    }

    /// Remove a multi-table subscription: every inner part and its join
    /// state. Unknown ids are a no-op.
    pub fn unregister_query(&mut self, query_uuid: &str) {
        let Some(tree) = self.trees.remove(query_uuid) else {
            return;
        };
        for node in tree.nodes.values() {
            self.single.unregister_query(node.part_id.as_str());
            self.parts.remove(&node.part_id);
        }
    }

    /// Route one write through the inner engine and forward the resulting
    /// per-part operations, maintaining the join state on the way (see the
    /// module docs for the order and the replace-pair diffing).
    pub fn incremental_update(&mut self, write: &WriteQuery) -> Vec<MultiTableUpdate> {
        let applied = self.single.incremental_update(write);
        let mut tagged: Vec<(QueryId, usize, QueryPart, SingleTableUpdate)> = Vec::new();
        for update in applied {
            let Some((uuid, part)) = self.parts.get(&update.query).cloned() else {
                continue;
            };
            let Some(rank) = self
                .trees
                .get(&uuid)
                .and_then(|tree| tree.rank.get(&part).copied())
            else {
                continue;
            };
            tagged.push((uuid, rank, part, update));
        }
        tagged.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

        let mut out = Vec::new();
        let mut updates = tagged.into_iter().peekable();
        while let Some((uuid, _, part, update)) = updates.next() {
            let paired_add = match (&update.op, updates.peek()) {
                (DataFrameOperation::Delete(key, _), Some((_, _, _, next)))
                    if next.query == update.query
                        && matches!(&next.op, DataFrameOperation::Add(next_key, _) if next_key == key) =>
                {
                    Some(updates.next().expect("peeked just above").3.op)
                }
                _ => None,
            };
            let forward = |op: DataFrameOperation| MultiTableUpdate {
                query: uuid.clone(),
                table: update.table.clone(),
                part: part.clone(),
                op,
            };
            match (update.op.clone(), paired_add) {
                (DataFrameOperation::Delete(key, old), Some(add)) => {
                    let new = add.row().clone();
                    out.push(forward(DataFrameOperation::Delete(key, old.clone())));
                    out.push(forward(add));
                    self.replaced(&uuid, &part, &old, &new, &mut out);
                }
                (op @ DataFrameOperation::Add(_, _), _) => {
                    let row = op.row().clone();
                    out.push(forward(op));
                    self.arrived(&uuid, &part, &row, &mut out);
                }
                (op @ DataFrameOperation::Delete(_, _), None) => {
                    let row = op.row().clone();
                    out.push(forward(op));
                    self.departed(&uuid, &part, &row, &mut out);
                }
            }
        }
        out
    }

    /// The rows currently held for one part of a subscription — key →
    /// image, an inspection view for tests and debugging. `None` for
    /// unknown ids or parts.
    pub fn rows_for(
        &self,
        query_uuid: &str,
        part: QueryPart,
    ) -> Option<HashMap<DataFrameKey, DataFrameRow>> {
        let node = self.trees.get(query_uuid)?.nodes.get(&part)?;
        self.single.rows_for(node.part_id.as_str())
    }

    /// The inner engine's routing counters.
    pub fn stats(&self) -> &IvmStats {
        self.single.stats()
    }

    /// A row now held by `part`: reference its join value on every edge
    /// the part drives, count it on every edge the part is driven by.
    fn arrived(&mut self, uuid: &QueryId, part: &QueryPart, row: &DataFrameRow, out: &mut Vec<MultiTableUpdate>) {
        for (edge, drives, column) in edge_steps(&self.trees[uuid], part) {
            let value = join_value(row, &column);
            if drives {
                self.reference(uuid, edge, value, out);
            } else {
                self.right_bump(uuid, edge, value);
            }
        }
    }

    /// A row no longer held by `part`: the mirror of [`Self::arrived`].
    fn departed(&mut self, uuid: &QueryId, part: &QueryPart, row: &DataFrameRow, out: &mut Vec<MultiTableUpdate>) {
        for (edge, drives, column) in edge_steps(&self.trees[uuid], part) {
            let value = join_value(row, &column);
            if drives {
                self.release(uuid, edge, &value, out);
            } else {
                self.right_drop(uuid, edge, &value);
            }
        }
    }

    /// A row of `part` replaced in place: move references only on edges
    /// whose join value actually changed — the new value referenced first,
    /// the old released after — so a kept value never crosses zero.
    fn replaced(
        &mut self,
        uuid: &QueryId,
        part: &QueryPart,
        old: &DataFrameRow,
        new: &DataFrameRow,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        for (edge, drives, column) in edge_steps(&self.trees[uuid], part) {
            let old_value = join_value(old, &column);
            let new_value = join_value(new, &column);
            if old_value == new_value {
                continue;
            }
            if drives {
                self.reference(uuid, edge, new_value, out);
                self.release(uuid, edge, &old_value, out);
            } else {
                self.right_drop(uuid, edge, &old_value);
                self.right_bump(uuid, edge, new_value);
            }
        }
    }

    /// A driver row now carries `value` on `edge`: bump `left`, and when
    /// that is the 0 → 1 crossing and it changes the driven part's `IN` leaf, widen the leaf in place,
    /// fetch the value's driven rows (one narrowed storage query), forward
    /// them, and let them arrive at the driven node. During registration
    /// the driven part may not exist yet; only the counts are kept then.
    fn reference(&mut self, uuid: &QueryId, edge: usize, value: Value, out: &mut Vec<MultiTableUpdate>) {
        if self.left_count(uuid, edge, &value) > 0 {
            self.left_bump(uuid, edge, value);
            return;
        }
        let (driven, column) = self.driven_end(uuid, edge);
        let before = leaf_values(&self.trees[uuid], &driven, &column);
        self.left_bump(uuid, edge, value.clone());
        self.push_reference(uuid, edge, value.clone());
        let after = leaf_values(&self.trees[uuid], &driven, &column);
        let Some((part_id, table)) = self.registered_part(uuid, &driven) else {
            return;
        };
        if before == after {
            return;
        }
        self.single.replace_condition(
            part_id.as_str(),
            &in_condition(&column, &before),
            in_condition(&column, &after),
        );
        let adds = self
            .single
            .fetch(part_id.as_str(), column.as_str(), std::slice::from_ref(&value));
        self.single.mark_reconciled(part_id.as_str());
        for op in adds {
            out.push(MultiTableUpdate {
                query: uuid.clone(),
                table: table.clone(),
                part: driven.clone(),
                op: op.clone(),
            });
            if let DataFrameOperation::Add(_, row) = &op {
                self.arrived(uuid, &driven, row, out);
            }
        }
    }

    /// A driver row no longer carries `value` on `edge`: drop `left`, and
    /// when that changes the driven part's `IN` leaf, narrow the leaf in
    /// place, prune the value's held driven rows from current data (no
    /// storage round-trip), forward the `Delete`s, and let them depart
    /// from the driven node.
    fn release(&mut self, uuid: &QueryId, edge: usize, value: &Value, out: &mut Vec<MultiTableUpdate>) {
        if self.left_count(uuid, edge, value) != 1 {
            self.left_drop(uuid, edge, value);
            return;
        }
        let (driven, column) = self.driven_end(uuid, edge);
        let before = leaf_values(&self.trees[uuid], &driven, &column);
        self.left_drop(uuid, edge, value);
        self.pull_reference(uuid, edge, value);
        let after = leaf_values(&self.trees[uuid], &driven, &column);
        let Some((part_id, table)) = self.registered_part(uuid, &driven) else {
            return;
        };
        if before == after {
            return;
        }
        self.single.replace_condition(
            part_id.as_str(),
            &in_condition(&column, &before),
            in_condition(&column, &after),
        );
        let deletes = self
            .single
            .delete_rows(part_id.as_str(), column.as_str(), std::slice::from_ref(value));
        self.single.mark_reconciled(part_id.as_str());
        for op in deletes {
            out.push(MultiTableUpdate {
                query: uuid.clone(),
                table: table.clone(),
                part: driven.clone(),
                op: op.clone(),
            });
            if let DataFrameOperation::Delete(_, row) = &op {
                self.departed(uuid, &driven, row, out);
            }
        }
    }

    /// The driven part of `edge` and the column its `IN` leaf is on.
    fn driven_end(&self, uuid: &QueryId, edge: usize) -> (QueryPart, ColumnName) {
        let edge = &self.trees[uuid].edges[edge];
        (edge.driven().clone(), edge.driven_column().clone())
    }

    /// The inner id and table of `part` once it is registered; `None`
    /// while registration has not reached it yet.
    fn registered_part(&self, uuid: &QueryId, part: &QueryPart) -> Option<(QueryId, TableName)> {
        let node = self.trees.get(uuid)?.nodes.get(part)?;
        node.registered
            .then(|| (node.part_id.clone(), node.query.table.clone()))
    }

    /// `value`'s current `left` count on `edge` (zero when absent) — read
    /// before a bump or drop to know whether it will cross zero, so the
    /// leaf is only recomputed when it can change.
    fn left_count(&self, uuid: &QueryId, edge: usize, value: &Value) -> u64 {
        self.trees
            .get(uuid)
            .and_then(|tree| tree.edges.get(edge))
            .and_then(|edge| edge.counts.left.get(value).copied())
            .unwrap_or(0)
    }

    /// Mutable access to one edge's counts.
    fn counts_mut(&mut self, uuid: &QueryId, edge: usize) -> Option<&mut JoinKeyCounts> {
        self.trees
            .get_mut(uuid)
            .and_then(|tree| tree.edges.get_mut(edge))
            .map(|edge| &mut edge.counts)
    }

    /// Append a newly referenced value to `edge`'s list.
    fn push_reference(&mut self, uuid: &QueryId, edge: usize, value: Value) {
        if let Some(counts) = self.counts_mut(uuid, edge)
            && !counts.referenced.contains(&value)
        {
            counts.referenced.push(value);
            debug_assert!(
                counts.referenced.len() == counts.left.len(),
                "the referenced list must mirror exactly the values with left > 0"
            );
        }
    }

    /// Remove a no-longer-referenced value from `edge`'s list.
    fn pull_reference(&mut self, uuid: &QueryId, edge: usize, value: &Value) {
        if let Some(counts) = self.counts_mut(uuid, edge) {
            counts.referenced.retain(|candidate| candidate != value);
            debug_assert!(
                counts.referenced.len() == counts.left.len(),
                "the referenced list must mirror exactly the values with left > 0"
            );
        }
    }

    /// Increment `value`'s `left` count on `edge`.
    fn left_bump(&mut self, uuid: &QueryId, edge: usize, value: Value) {
        if let Some(counts) = self.counts_mut(uuid, edge) {
            *counts.left.entry(value).or_insert(0) += 1;
        }
    }

    /// Decrement `value`'s `left` count on `edge`, removing the entry at
    /// zero.
    fn left_drop(&mut self, uuid: &QueryId, edge: usize, value: &Value) {
        Self::drop_in(self.counts_mut(uuid, edge).map(|counts| &mut counts.left), value);
    }

    /// Increment `value`'s `right` count on `edge`.
    fn right_bump(&mut self, uuid: &QueryId, edge: usize, value: Value) {
        if let Some(counts) = self.counts_mut(uuid, edge) {
            *counts.right.entry(value).or_insert(0) += 1;
        }
    }

    /// Decrement `value`'s `right` count on `edge`, removing the entry at
    /// zero.
    fn right_drop(&mut self, uuid: &QueryId, edge: usize, value: &Value) {
        Self::drop_in(self.counts_mut(uuid, edge).map(|counts| &mut counts.right), value);
    }

    /// Decrement `value` in one count map, removing the entry at zero;
    /// absent values are left alone.
    fn drop_in(counts: Option<&mut HashMap<Value, u64>>, value: &Value) {
        let Some(counts) = counts else {
            return;
        };
        let Some(count) = counts.get_mut(value) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            counts.remove(value);
        }
    }
}
