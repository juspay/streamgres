//! Multi-table subscriptions: a tree of single-table parts joined by LEFT
//! and RIGHT edges, maintained over the single-table engine, with one tree
//! shared by every subscription that registers the same spec.
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
//! restriction `driven_column IN <set>` inside its filter. A LEFT edge
//! preserves the parent, so the parent drives and the child is driven; a
//! RIGHT edge preserves the child, so the child drives and the parent is
//! driven. Because the restriction lives inside the driven part's filter,
//! writes on the driven table route natively through the single engine: a
//! row matching the filter (restriction included) fires an `Add`, a held
//! row moving out fires a `Delete` via membership, and an unreferenced row
//! never fires at all. The join layer never inspects driven-side writes;
//! it forwards them and keeps its counts.
//!
//! # Set-valued leaves
//!
//! The restriction's operand is a [`SharedSet`], one per (driven part,
//! column), owned by the tree and referenced by the part's filter. A zero
//! crossing therefore adds or removes **one member**
//! ([`SingleTableIVM::set_insert`] / [`SingleTableIVM::set_remove`]): the
//! index files the leaf under that one value, the filter is untouched
//! (it holds the same set), and nothing proportional to the set's size is
//! rebuilt. A node driven by several edges on one column — a LEFT parent
//! above it and a RIGHT child below it, both on `id` — has one leaf whose
//! members are the **intersection** of the driving edges' referenced
//! values, maintained one crossing at a time.
//!
//! # Sharing
//!
//! Subscriptions with an identical spec share one tree: one inner part
//! per node, one set of edges and counts, one crossing per event. A later
//! identical registration is served its snapshot from the shared parts'
//! rows, and every operation a part produces is emitted once per
//! subscriber of its tree. The tree is dropped with its last subscriber.
//!
//! # Counts, crossings, cascades
//!
//! Per edge and per join value the layer keeps `left` (driver rows
//! carrying the value) and `right` (driven rows held for it;
//! observational). Only zero crossings of `left` act, and only when they
//! change the driven leaf: the value's driven rows are fetched (one
//! narrowed storage query) or pruned from current data. Rows a fetch
//! brings in are **arrivals** at the driven node and rows a prune removes
//! are **departures**, and the driven node may itself drive further edges,
//! so the same handling cascades down the tree — the recursion that makes
//! nesting work with one code path.
//!
//! # Order
//!
//! Registration is a post-order walk: a node's RIGHT children register
//! first (their rows fill the sets it is restricted by), then the node,
//! then its LEFT children (restricted by the node's rows); during that
//! walk crossings only fill sets, since every part is registered with its
//! full set. Within one write, each part's native operations are
//! forwarded with every **driven part before its driver** — LEFT children
//! before their parent, a RIGHT child after its parent — because handling
//! a driver's operation may fetch into or prune the driven frame, and a
//! stale driven operation forwarded after that prune would resurrect a
//! row on the client. An in-place replacement arrives as an adjacent
//! `Delete(old)` + `Add(new)` pair and is diffed per edge, so a rewrite
//! that keeps a join value never swings its count through zero.
//!
//! Inner part ids are `t{tree}_r` for a tree's root and `t{tree}_r_{i}_{j}…`
//! down the path; they are looked up in a map, never parsed.

use std::collections::HashMap;
use std::rc::Rc;

use super::stats::IvmStats;
use super::storage::Storage;
use super::{QueryId, SingleTableIVM, SingleTableUpdate};
use crate::model::{
    ColumnName, ComparisonOperator, Condition, DataFrameKey, DataFrameOperation, DataFrameRow,
    MultiTableReadQuery, SharedSet, SingleTableReadQuery, TableName, Value, Where, WriteQuery,
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

/// Per-edge reference counts.
///
/// - `left`: how many driver rows carry each join value.
/// - `right`: how many driven rows the driven part holds per value;
///   observational, kept for the `left == 0 ⇒ right == 0` invariant.
///
/// Entries leave the maps when they reach zero, so they only hold live
/// values.
#[derive(Default)]
struct JoinKeyCounts {
    left: HashMap<Value, u64>,
    right: HashMap<Value, u64>,
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
    /// The part whose filter carries this edge's leaf.
    fn driven(&self) -> &QueryPart {
        match self.kind {
            JoinKind::Left => &self.child,
            JoinKind::Right => &self.parent,
        }
    }

    /// The column the leaf is on.
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

/// One registered spec and every subscription sharing it.
///
/// - `spec`: the tree as registered.
/// - `subscribers`: the external ids sharing it, in registration order.
/// - `nodes`: every part by path.
/// - `edges`: every join edge; nodes refer to them by index.
/// - `leaves`: the shared set behind each (driven part, column) leaf.
/// - `rank`: forwarding order of the parts, every driven part before its
///   driver (see the module docs).
/// - `post_order`: registration order of the parts, used to serve a later
///   subscriber's snapshot.
struct Tree {
    spec: Rc<MultiTableReadQuery>,
    subscribers: Vec<QueryId>,
    nodes: HashMap<QueryPart, Node>,
    edges: Vec<Edge>,
    leaves: HashMap<(QueryPart, ColumnName), SharedSet>,
    rank: HashMap<QueryPart, usize>,
    post_order: Vec<QueryPart>,
}

/// The join layer. Owns the inner [`SingleTableIVM`] exclusively, so its
/// part ids cannot collide with anything registered from outside; storage
/// lives inside the inner engine.
///
/// - `single`: the inner engine holding every part's routing and the
///   shared per-table frames.
/// - `trees`: tree id → the shared tree.
/// - `by_uuid`: external id → the tree it subscribes to.
/// - `parts`: inner part id → (tree id, part), the reverse map every
///   routed operation goes through.
pub struct MultiTableIVM {
    single: SingleTableIVM,
    trees: HashMap<usize, Tree>,
    next_tree: usize,
    by_uuid: HashMap<QueryId, usize>,
    parts: HashMap<QueryId, (usize, QueryPart)>,
}

/// The inner id of part `part` of tree `tree`: a tree marker, an `_r`
/// marker, then one `_{index}` per step of the path.
fn part_id(tree: usize, part: &QueryPart) -> QueryId {
    let mut id = format!("t{tree}_r");
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

/// The leaf restricting a driven part to a shared set's members.
fn leaf_condition(column: &ColumnName, set: &SharedSet) -> Condition {
    Condition::new(column.clone(), ComparisonOperator::IN, Value::Set(set.clone()))
}

/// Build the node, edge and leaf tables of a spec.
fn build_tree(tree_id: usize, spec: &MultiTableReadQuery) -> Tree {
    let mut tree = Tree {
        spec: Rc::new(spec.clone()),
        subscribers: Vec::new(),
        nodes: HashMap::new(),
        edges: Vec::new(),
        leaves: HashMap::new(),
        rank: HashMap::new(),
        post_order: Vec::new(),
    };
    add_node(&mut tree, tree_id, spec, QueryPart::main(), None);
    for edge in &tree.edges {
        tree.leaves
            .entry((edge.driven().clone(), edge.driven_column().clone()))
            .or_default();
    }
    let mut forward = Vec::new();
    forwarding_order(&tree, &QueryPart::main(), &mut forward);
    tree.rank = forward
        .into_iter()
        .enumerate()
        .map(|(rank, part)| (part, rank))
        .collect();
    let mut post = Vec::new();
    registration_order(&tree, &QueryPart::main(), &mut post);
    tree.post_order = post;
    tree
}

/// Add `spec`'s node at `part` and, recursively, its children.
fn add_node(
    tree: &mut Tree,
    tree_id: usize,
    spec: &MultiTableReadQuery,
    part: QueryPart,
    parent: Option<usize>,
) {
    let mut node = Node {
        part_id: part_id(tree_id, &part),
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
        add_node(tree, tree_id, &join.sub, child, Some(edge));
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

/// Append the subtree at `part` in registration order: RIGHT subtrees
/// (which fill this node's sets) first, the node, then LEFT subtrees.
fn registration_order(tree: &Tree, part: &QueryPart, out: &mut Vec<QueryPart>) {
    let node = &tree.nodes[part];
    for &edge in &node.children {
        if tree.edges[edge].kind == JoinKind::Right {
            registration_order(tree, &tree.edges[edge].child, out);
        }
    }
    out.push(part.clone());
    for &edge in &node.children {
        if tree.edges[edge].kind == JoinKind::Left {
            registration_order(tree, &tree.edges[edge].child, out);
        }
    }
}

/// Whether every edge driving `part` on `column` currently references
/// `value` — the membership test of the leaf's intersection.
fn referenced_by_all(tree: &Tree, part: &QueryPart, column: &ColumnName, value: &Value) -> bool {
    tree.edges
        .iter()
        .filter(|edge| edge.driven() == part && edge.driven_column() == column)
        .all(|edge| edge.counts.left.contains_key(value))
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
            next_tree: 0,
            by_uuid: HashMap::new(),
            parts: HashMap::new(),
        }
    }

    /// Register a multi-table subscription under `query_uuid`, returning
    /// its initial snapshot as operations. A spec already registered by
    /// another subscription is shared: the new subscriber joins that tree
    /// and is served the shared parts' current rows, touching no storage.
    /// Re-registering the identical spec under the same id is a no-op
    /// returning no operations; a changed spec replaces the subscription.
    pub fn register_query(
        &mut self,
        query_uuid: impl Into<QueryId>,
        query: MultiTableReadQuery,
    ) -> Vec<MultiTableUpdate> {
        let query_uuid = query_uuid.into();
        if let Some(&tree) = self.by_uuid.get(&query_uuid) {
            if *self.trees[&tree].spec == query {
                return Vec::new();
            }
            self.unregister_query(query_uuid.as_str());
        }
        let mut out = Vec::new();
        if let Some((&tree_id, _)) = self.trees.iter().find(|(_, tree)| *tree.spec == query) {
            self.by_uuid.insert(query_uuid.clone(), tree_id);
            let tree = self.trees.get_mut(&tree_id).expect("found just above");
            tree.subscribers.push(query_uuid.clone());
            for part in &tree.post_order {
                let node = &tree.nodes[part];
                let Some(rows) = self.single.rows_for(node.part_id.as_str()) else {
                    continue;
                };
                for (key, row) in rows {
                    out.push(MultiTableUpdate {
                        query: query_uuid.clone(),
                        table: node.query.table.clone(),
                        part: part.clone(),
                        op: DataFrameOperation::Add(key, row),
                    });
                }
            }
            let parts = tree.nodes.len() as u64;
            self.single.note_shared_snapshots(parts);
            return out;
        }
        let tree_id = self.next_tree;
        self.next_tree += 1;
        let mut tree = build_tree(tree_id, &query);
        tree.subscribers.push(query_uuid.clone());
        for (part, node) in &tree.nodes {
            self.parts
                .insert(node.part_id.clone(), (tree_id, part.clone()));
        }
        self.trees.insert(tree_id, tree);
        self.by_uuid.insert(query_uuid, tree_id);
        self.register_part(tree_id, QueryPart::main(), &mut out);
        out
    }

    /// Register the subtree at `part` in post-order: RIGHT children, the
    /// node itself with its full set restrictions, then LEFT children.
    fn register_part(&mut self, tree_id: usize, part: QueryPart, out: &mut Vec<MultiTableUpdate>) {
        let (right_children, left_children, own, part_id) = {
            let tree = &self.trees[&tree_id];
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
            self.register_part(tree_id, child, out);
        }
        let limit = if part.is_main() { own.limit } else { u32::MAX };
        let query = SingleTableReadQuery {
            filter: self.restricted_filter(tree_id, &part),
            limit,
            ..own.clone()
        };
        let ops = self.single.register_query(part_id, query, None);
        if let Some(node) = self
            .trees
            .get_mut(&tree_id)
            .and_then(|tree| tree.nodes.get_mut(&part))
        {
            node.registered = true;
        }
        for op in ops {
            self.emit(tree_id, &part, &own.table, op.clone(), out);
            if let DataFrameOperation::Add(_, row) = &op {
                self.arrived(tree_id, &part, row, out);
            }
        }
        for child in left_children {
            self.register_part(tree_id, child, out);
        }
    }

    /// A part's registered filter: its own `WHERE` plus one set-valued
    /// `IN` leaf per column it is driven on.
    fn restricted_filter(&self, tree_id: usize, part: &QueryPart) -> Where {
        let tree = &self.trees[&tree_id];
        let mut parts = vec![tree.nodes[part].query.filter.clone()];
        for column in driven_columns(tree, part) {
            let set = &tree.leaves[&(part.clone(), column.clone())];
            parts.push(Where::Condition(leaf_condition(&column, set)));
        }
        Where::AND(parts)
    }

    /// Remove a subscription. Its tree lives on while other subscriptions
    /// share it; with the last one gone, every inner part and the join
    /// state go too. Unknown ids are a no-op.
    pub fn unregister_query(&mut self, query_uuid: &str) {
        let Some(tree_id) = self.by_uuid.remove(query_uuid) else {
            return;
        };
        let Some(tree) = self.trees.get_mut(&tree_id) else {
            return;
        };
        tree.subscribers
            .retain(|subscriber| subscriber.as_str() != query_uuid);
        if !tree.subscribers.is_empty() {
            return;
        }
        let Some(tree) = self.trees.remove(&tree_id) else {
            return;
        };
        for node in tree.nodes.values() {
            self.single.unregister_query(node.part_id.as_str());
            self.parts.remove(&node.part_id);
        }
    }

    /// Route one write through the inner engine and forward the resulting
    /// per-part operations to every subscriber of their tree, maintaining
    /// the join state on the way (see the module docs for the order and
    /// the replace-pair diffing).
    pub fn incremental_update(&mut self, write: &WriteQuery) -> Vec<MultiTableUpdate> {
        let applied = self.single.incremental_update(write);
        let mut tagged: Vec<(usize, usize, QueryPart, SingleTableUpdate)> = Vec::new();
        for update in applied {
            let Some((tree_id, part)) = self.parts.get(&update.query).cloned() else {
                continue;
            };
            let Some(rank) = self
                .trees
                .get(&tree_id)
                .and_then(|tree| tree.rank.get(&part).copied())
            else {
                continue;
            };
            tagged.push((tree_id, rank, part, update));
        }
        tagged.sort_by_key(|(tree_id, rank, _, _)| (*tree_id, *rank));

        let mut out = Vec::new();
        let mut updates = tagged.into_iter().peekable();
        while let Some((tree_id, _, part, update)) = updates.next() {
            let paired_add = match (&update.op, updates.peek()) {
                (DataFrameOperation::Delete(key, _), Some((_, _, _, next)))
                    if next.query == update.query
                        && matches!(&next.op, DataFrameOperation::Add(next_key, _) if next_key == key) =>
                {
                    Some(updates.next().expect("peeked just above").3.op)
                }
                _ => None,
            };
            let table = update.table.clone();
            match (update.op, paired_add) {
                (DataFrameOperation::Delete(key, old), Some(add)) => {
                    let new = add.row().clone();
                    self.emit(tree_id, &part, &table, DataFrameOperation::Delete(key, old.clone()), &mut out);
                    self.emit(tree_id, &part, &table, add, &mut out);
                    self.replaced(tree_id, &part, &old, &new, &mut out);
                }
                (op @ DataFrameOperation::Add(_, _), _) => {
                    let row = op.row().clone();
                    self.emit(tree_id, &part, &table, op, &mut out);
                    self.arrived(tree_id, &part, &row, &mut out);
                }
                (op @ DataFrameOperation::Delete(_, _), None) => {
                    let row = op.row().clone();
                    self.emit(tree_id, &part, &table, op, &mut out);
                    self.departed(tree_id, &part, &row, &mut out);
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
        let tree = self.trees.get(self.by_uuid.get(query_uuid)?)?;
        let node = tree.nodes.get(&part)?;
        self.single.rows_for(node.part_id.as_str())
    }

    /// The inner engine's routing counters.
    pub fn stats(&self) -> &IvmStats {
        self.single.stats()
    }

    /// Forward one part operation to every subscriber of its tree.
    fn emit(
        &self,
        tree_id: usize,
        part: &QueryPart,
        table: &TableName,
        op: DataFrameOperation,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let Some(tree) = self.trees.get(&tree_id) else {
            return;
        };
        for subscriber in &tree.subscribers {
            out.push(MultiTableUpdate {
                query: subscriber.clone(),
                table: table.clone(),
                part: part.clone(),
                op: op.clone(),
            });
        }
    }

    /// A row now held by `part`: reference its join value on every edge
    /// the part drives, count it on every edge the part is driven by.
    fn arrived(&mut self, tree_id: usize, part: &QueryPart, row: &DataFrameRow, out: &mut Vec<MultiTableUpdate>) {
        for (edge, drives, column) in edge_steps(&self.trees[&tree_id], part) {
            let value = join_value(row, &column);
            if drives {
                self.reference(tree_id, edge, value, out);
            } else {
                self.right_bump(tree_id, edge, value);
            }
        }
    }

    /// A row no longer held by `part`: the mirror of [`Self::arrived`].
    fn departed(&mut self, tree_id: usize, part: &QueryPart, row: &DataFrameRow, out: &mut Vec<MultiTableUpdate>) {
        for (edge, drives, column) in edge_steps(&self.trees[&tree_id], part) {
            let value = join_value(row, &column);
            if drives {
                self.release(tree_id, edge, &value, out);
            } else {
                self.right_drop(tree_id, edge, &value);
            }
        }
    }

    /// A row of `part` replaced in place: move references only on edges
    /// whose join value actually changed — the new value referenced first,
    /// the old released after — so a kept value never crosses zero.
    fn replaced(
        &mut self,
        tree_id: usize,
        part: &QueryPart,
        old: &DataFrameRow,
        new: &DataFrameRow,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        for (edge, drives, column) in edge_steps(&self.trees[&tree_id], part) {
            let old_value = join_value(old, &column);
            let new_value = join_value(new, &column);
            if old_value == new_value {
                continue;
            }
            if drives {
                self.reference(tree_id, edge, new_value, out);
                self.release(tree_id, edge, &old_value, out);
            } else {
                self.right_drop(tree_id, edge, &old_value);
                self.right_bump(tree_id, edge, new_value);
            }
        }
    }

    /// A driver row now carries `value` on `edge`: bump `left`, and on the
    /// 0 → 1 crossing, if every edge driving the same leaf now references
    /// the value, add it to the leaf's set (one index filing), fetch the
    /// value's driven rows (one narrowed storage query), forward them, and
    /// let them arrive at the driven node. Before the driven part is
    /// registered the set is filled directly; registration files it whole.
    fn reference(&mut self, tree_id: usize, edge: usize, value: Value, out: &mut Vec<MultiTableUpdate>) {
        let crossing = self.left_count(tree_id, edge, &value) == 0;
        self.left_bump(tree_id, edge, value.clone());
        if !crossing {
            return;
        }
        let (driven, column) = self.driven_end(tree_id, edge);
        if !referenced_by_all(&self.trees[&tree_id], &driven, &column, &value) {
            return;
        }
        let set = self.trees[&tree_id].leaves[&(driven.clone(), column.clone())].clone();
        let Some((part_id, table)) = self.registered_part(tree_id, &driven) else {
            set.insert(&value);
            return;
        };
        if !self
            .single
            .set_insert(part_id.as_str(), &leaf_condition(&column, &set), &value)
        {
            return;
        }
        let adds = self
            .single
            .fetch(part_id.as_str(), column.as_str(), std::slice::from_ref(&value));
        self.single.mark_reconciled(part_id.as_str());
        for op in adds {
            self.emit(tree_id, &driven, &table, op.clone(), out);
            if let DataFrameOperation::Add(_, row) = &op {
                self.arrived(tree_id, &driven, row, out);
            }
        }
    }

    /// A driver row no longer carries `value` on `edge`: drop `left`, and
    /// on the crossing to 0, if the value was in the leaf's set, remove it
    /// (one index unfiling), prune the value's held driven rows from
    /// current data (no storage round-trip), forward the `Delete`s, and
    /// let them depart from the driven node.
    fn release(&mut self, tree_id: usize, edge: usize, value: &Value, out: &mut Vec<MultiTableUpdate>) {
        let crossing = self.left_count(tree_id, edge, value) == 1;
        self.left_drop(tree_id, edge, value);
        if !crossing {
            return;
        }
        let (driven, column) = self.driven_end(tree_id, edge);
        let set = self.trees[&tree_id].leaves[&(driven.clone(), column.clone())].clone();
        if !set.contains(value) {
            return;
        }
        let Some((part_id, table)) = self.registered_part(tree_id, &driven) else {
            set.remove(value);
            return;
        };
        if !self
            .single
            .set_remove(part_id.as_str(), &leaf_condition(&column, &set), value)
        {
            return;
        }
        let deletes = self
            .single
            .delete_rows(part_id.as_str(), column.as_str(), std::slice::from_ref(value));
        self.single.mark_reconciled(part_id.as_str());
        for op in deletes {
            self.emit(tree_id, &driven, &table, op.clone(), out);
            if let DataFrameOperation::Delete(_, row) = &op {
                self.departed(tree_id, &driven, row, out);
            }
        }
    }

    /// The driven part of `edge` and the column its leaf is on.
    fn driven_end(&self, tree_id: usize, edge: usize) -> (QueryPart, ColumnName) {
        let edge = &self.trees[&tree_id].edges[edge];
        (edge.driven().clone(), edge.driven_column().clone())
    }

    /// The inner id and table of `part` once it is registered; `None`
    /// while registration has not reached it yet.
    fn registered_part(&self, tree_id: usize, part: &QueryPart) -> Option<(QueryId, TableName)> {
        let node = self.trees.get(&tree_id)?.nodes.get(part)?;
        node.registered
            .then(|| (node.part_id.clone(), node.query.table.clone()))
    }

    /// `value`'s current `left` count on `edge` (zero when absent).
    fn left_count(&self, tree_id: usize, edge: usize, value: &Value) -> u64 {
        self.trees
            .get(&tree_id)
            .and_then(|tree| tree.edges.get(edge))
            .and_then(|edge| edge.counts.left.get(value).copied())
            .unwrap_or(0)
    }

    /// Mutable access to one edge's counts.
    fn counts_mut(&mut self, tree_id: usize, edge: usize) -> Option<&mut JoinKeyCounts> {
        self.trees
            .get_mut(&tree_id)
            .and_then(|tree| tree.edges.get_mut(edge))
            .map(|edge| &mut edge.counts)
    }

    /// Increment `value`'s `left` count on `edge`.
    fn left_bump(&mut self, tree_id: usize, edge: usize, value: Value) {
        if let Some(counts) = self.counts_mut(tree_id, edge) {
            *counts.left.entry(value).or_insert(0) += 1;
        }
    }

    /// Decrement `value`'s `left` count on `edge`, removing the entry at
    /// zero.
    fn left_drop(&mut self, tree_id: usize, edge: usize, value: &Value) {
        Self::drop_in(self.counts_mut(tree_id, edge).map(|counts| &mut counts.left), value);
    }

    /// Increment `value`'s `right` count on `edge`.
    fn right_bump(&mut self, tree_id: usize, edge: usize, value: Value) {
        if let Some(counts) = self.counts_mut(tree_id, edge) {
            *counts.right.entry(value).or_insert(0) += 1;
        }
    }

    /// Decrement `value`'s `right` count on `edge`, removing the entry at
    /// zero.
    fn right_drop(&mut self, tree_id: usize, edge: usize, value: &Value) {
        Self::drop_in(self.counts_mut(tree_id, edge).map(|counts| &mut counts.right), value);
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
