//! Multi-table subscriptions: a tree of single-table parts joined by LEFT,
//! RIGHT and INNER edges, maintained over the single-table engine, with
//! one tree shared by every subscription that registers the same spec.
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
//! driven; an INNER edge is evaluated from the child like a RIGHT edge,
//! and adds the gate below. Because the restriction lives inside the
//! driven part's filter, writes on the driven table route natively through
//! the single engine: a row matching the filter (restriction included)
//! fires an `Add`, a held row moving out fires a `Delete` via membership,
//! and an unreferenced row never fires at all. The join layer never
//! inspects driven-side writes; it forwards them and keeps its counts.
//!
//! # Set-valued leaves, placed
//!
//! The restriction's operand is a [`SharedSet`] owned by the tree and
//! referenced by the part's filter. A zero crossing therefore adds or
//! removes **one member** ([`SingleTableIVM::set_insert`] /
//! [`SingleTableIVM::set_remove`]): the index files the leaf under that one
//! value, the filter is untouched (it holds the same set), and nothing
//! proportional to the set's size is rebuilt. Where the leaf sits is the
//! author's choice: an `EXISTS` leaf in the node's own `WHERE`
//! ([`ComparisonOperator::EXISTS`], naming one of the node's inner joins)
//! is bound in place, so `visibility = 'PUBLIC' OR EXISTS(...)` is one
//! filter with the restriction inside its `OR`; an edge no leaf names is
//! conjoined at the top. Edges bound by a leaf have a set of their own;
//! unnamed edges driving one part on one column — a LEFT parent above it
//! and a RIGHT child below it, both on `id` — share one set holding the
//! **intersection** of their referenced values. When a value leaves a set
//! the value's held rows are re-evaluated against the filter, not deleted
//! outright, so a row another branch still admits stays.
//!
//! # The gate
//!
//! A row is *shown* (delivered to clients) only under a shown parent row:
//! under a LEFT or INNER edge a child row is shown while at least one
//! shown parent row carries its join value, under a RIGHT edge always (the
//! child is preserved), and the root always. Per edge and per value the
//! layer counts the shown parent rows; a crossing of that count admits
//! (one `Add` each) or retracts (one `Delete` each) the child rows for the
//! value, and each of those rows in turn is counted on the edges below it,
//! so a chain of INNER edges shows exactly the rows that reach the root.
//! Held rows that are not shown still drive: an INNER child's rows are
//! evaluated first and fill the parent's set whether or not the parent row
//! that makes them visible has arrived.
//!
//! # Sharing
//!
//! Subscriptions with an identical spec share one tree: one inner part
//! per node, one set of edges and counts, one crossing per event. A later
//! identical registration is served its snapshot from the shared parts'
//! shown rows, and every operation a part produces is emitted once per
//! subscriber of its tree. The tree is dropped with its last subscriber.
//!
//! # Counts, crossings, cascades
//!
//! Per edge and per join value the layer keeps `left` (driver rows
//! carrying the value) and `shown` (shown parent rows carrying it). Only
//! zero crossings of `left` act on the set: the value's driven rows are
//! fetched (one narrowed storage query) or pruned from current data. Rows
//! a fetch brings in are **arrivals** at the driven node and rows a prune
//! removes are **departures**, and the driven node may itself drive
//! further edges, so the same handling cascades down the tree — the
//! recursion that makes nesting work with one code path.
//!
//! # Order
//!
//! Registration is a post-order walk: a node's RIGHT and INNER children
//! register first (their rows fill the sets it is restricted by), then the
//! node, then its LEFT children (restricted by the node's rows); during
//! that walk crossings only fill sets, since every part is registered with
//! its full set. A part's rows arrive when its storage read **lands**,
//! some time after it is asked for, so the walk is driven by landings: a
//! node registers once every RIGHT and INNER child below it is *live*
//! (its own read landed and nothing further out for it), and its LEFT
//! children register once it is live itself. A part served from a twin is
//! live at once. Within one write, each part's native operations are
//! forwarded with every **driven part before its driver** — LEFT children
//! before their parent, a RIGHT or INNER child after its parent — because
//! handling a driver's operation may prune the driven frame, and a stale
//! driven operation forwarded after that prune would resurrect a row on
//! the client. An in-place replacement arrives as an adjacent
//! `Delete(old)` + `Add(new)` pair and is diffed per edge, so a rewrite
//! that keeps a join value never swings its count through zero.
//!
//! Inner parts are ordinary subscriptions of the inner engine, addressed by
//! the ids it hands out and looked up in a map; the reads they ask for
//! surface through the inner engine's request list, and land back through
//! [`MultiTableIVM::land_fetch`], which cascades the landed rows' arrivals
//! exactly as it cascades a write's.

use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

use super::stats::IvmStats;
use super::update::{Raw, Target, group};
use super::{ClientUpdate, Engine, Fetch, QueryPart, SingleTableIVM, SingleTableUpdate};
use crate::model::{
    ClientId, ColumnName, ComparisonOperator, Condition, DataFrameKey, DataFrameOperation,
    DataFrameRow, MultiTableReadQuery, SharedSet, SingleTableReadQuery, SubId, TableName, Value,
    Where, WriteQuery,
};

/// One operation for one part of one multi-table subscription — the
/// join layer's own unit, grouped per client before it leaves the engine
/// ([`ClientUpdate`]).
///
/// - `query`: which subscription (the engine id `register_query` returned).
/// - `table`: the table the operation lands on.
/// - `part`: which node of the tree produced it — kept beside the table
///   because a self-join makes the table alone ambiguous.
/// - `op`: the delta itself.
#[derive(Debug, Clone, PartialEq)]
pub struct MultiTableUpdate {
    pub query: SubId,
    pub table: TableName,
    pub part: QueryPart,
    pub op: DataFrameOperation,
}

/// Which side of an edge is preserved: LEFT keeps the parent (the parent
/// drives), RIGHT keeps the child (the child drives), INNER keeps neither
/// (the child drives, and its rows are shown only under a shown parent
/// row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinKind {
    Left,
    Right,
    Inner,
}

/// Which shared set an edge restricts its driven part through: an edge
/// bound by an `EXISTS` leaf has a set of its own; unnamed edges driving
/// one part on one column share a set holding the intersection of their
/// referenced values.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum LeafKey {
    Shared(QueryPart, ColumnName),
    Own(usize),
}

/// Per-edge counts by join value.
///
/// - `left`: how many driver rows carry the value; its zero crossings
///   move the value in and out of the driven part's set.
/// - `shown`: how many shown parent rows carry the value; its zero
///   crossings admit and retract the child rows under a LEFT or INNER
///   edge.
///
/// Entries leave the maps when they reach zero, so they only hold live
/// values.
#[derive(Default)]
struct JoinKeyCounts {
    left: HashMap<Value, u64>,
    shown: HashMap<Value, u64>,
}

/// One join edge of a registered tree, with its direction made explicit
/// through [`Edge::driven`].
///
/// - `leaf`: the set the edge's restriction reads.
/// - `named`: the `EXISTS` leaf in the driven node's own filter the edge
///   binds, if the filter names it; otherwise the restriction is
///   conjoined at the top.
struct Edge {
    kind: JoinKind,
    parent: QueryPart,
    child: QueryPart,
    parent_column: ColumnName,
    child_column: ColumnName,
    leaf: LeafKey,
    named: Option<Condition>,
    counts: JoinKeyCounts,
}

impl Edge {
    /// The part whose filter carries this edge's leaf.
    fn driven(&self) -> &QueryPart {
        match self.kind {
            JoinKind::Left => &self.child,
            JoinKind::Right | JoinKind::Inner => &self.parent,
        }
    }

    /// The column the leaf is on.
    fn driven_column(&self) -> &ColumnName {
        match self.kind {
            JoinKind::Left => &self.child_column,
            JoinKind::Right | JoinKind::Inner => &self.parent_column,
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

    /// Whether the child's rows are shown only under a shown parent row.
    fn gates_child(&self) -> bool {
        self.kind != JoinKind::Right
    }
}

/// One node of a registered tree: its inner part (once registered),
/// whether that part's rows have all arrived (`live`), and its place among
/// the edges.
struct Node {
    part: Option<SubId>,
    live: bool,
    query: SingleTableReadQuery,
    parent: Option<usize>,
    children: Vec<usize>,
}

/// One registered spec and every subscription sharing it.
///
/// - `spec`: the tree as registered.
/// - `subscribers`: the subscription ids sharing it, in registration order.
/// - `nodes`: every part by path.
/// - `edges`: every join edge; nodes refer to them by index.
/// - `leaves`: the shared set behind each leaf, by [`LeafKey`].
/// - `rank`: forwarding order of the parts, every driven part before its
///   driver (see the module docs).
/// - `post_order`: registration order of the parts, used to serve a later
///   subscriber's snapshot.
struct Tree {
    spec: Rc<MultiTableReadQuery>,
    subscribers: Vec<SubId>,
    nodes: HashMap<QueryPart, Node>,
    edges: Vec<Edge>,
    leaves: HashMap<LeafKey, SharedSet>,
    rank: HashMap<QueryPart, usize>,
    post_order: Vec<QueryPart>,
}

/// The join layer's handle for one shared tree; unique for the life of
/// the layer, never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct TreeId(u64);

/// The join layer. Owns the inner [`SingleTableIVM`] exclusively, so its
/// part ids cannot collide with anything registered from outside.
///
/// - `single`: the inner engine holding every part's routing and the
///   shared per-table frames.
/// - `trees`: tree id → the shared tree.
/// - `by_sub`: subscription id → the tree it subscribes to.
/// - `next_sub`: the next subscription id to hand out; never reused.
/// - `parts`: inner part id → (tree id, part), the reverse map every
///   routed operation goes through.
/// - `clients`: subscription id → the client it belongs to, and
///   `by_client` the reverse, for addressing deltas and for a client's
///   disconnect.
pub struct MultiTableIVM {
    single: SingleTableIVM,
    trees: HashMap<TreeId, Tree>,
    next_tree: u64,
    by_sub: HashMap<SubId, TreeId>,
    next_sub: u64,
    parts: HashMap<SubId, (TreeId, QueryPart)>,
    clients: HashMap<SubId, ClientId>,
    by_client: HashMap<ClientId, BTreeSet<SubId>>,
}

/// Whether a held row of `part` is shown to clients: the root always, a
/// child under a RIGHT edge always, a child under a LEFT or INNER edge
/// only while a shown parent row carries its join value.
fn shown_in(tree: &Tree, part: &QueryPart, row: &DataFrameRow) -> bool {
    let Some(edge) = tree.nodes[part].parent.map(|edge| &tree.edges[edge]) else {
        return true;
    };
    if !edge.gates_child() {
        return true;
    }
    let value = join_value(row, &edge.child_column);
    edge.counts
        .shown
        .get(&value)
        .is_some_and(|count| *count > 0)
}

/// The row's value in `column`; a missing column joins like `NULL` (never).
fn join_value(row: &DataFrameRow, column: &ColumnName) -> Value {
    row.data
        .get(column.as_str())
        .cloned()
        .unwrap_or(Value::Null)
}

/// The leaf restricting a driven part to a shared set's members.
fn leaf_condition(column: &ColumnName, set: &SharedSet) -> Condition {
    Condition::new(
        column.clone(),
        ComparisonOperator::IN,
        Value::Set(set.clone()),
    )
}

/// Build the node, edge and leaf tables of a spec.
fn build_tree(spec: &MultiTableReadQuery) -> Tree {
    let mut tree = Tree {
        spec: Rc::new(spec.clone()),
        subscribers: Vec::new(),
        nodes: HashMap::new(),
        edges: Vec::new(),
        leaves: HashMap::new(),
        rank: HashMap::new(),
        post_order: Vec::new(),
    };
    add_node(&mut tree, spec, QueryPart::main(), None);
    for edge in &tree.edges {
        tree.leaves.entry(edge.leaf.clone()).or_default();
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

/// Add `spec`'s node at `part` and, recursively, its children: left joins
/// numbered first, then right, then inner. An inner join the node's own
/// filter names through an `EXISTS` leaf gets a set of its own, bound in
/// place at registration.
fn add_node(tree: &mut Tree, spec: &MultiTableReadQuery, part: QueryPart, parent: Option<usize>) {
    let mut node = Node {
        part: None,
        live: false,
        query: spec.main_table.clone(),
        parent,
        children: Vec::new(),
    };
    let joins = spec
        .left_joins
        .iter()
        .map(|join| (JoinKind::Left, join, None))
        .chain(
            spec.right_joins
                .iter()
                .map(|join| (JoinKind::Right, join, None)),
        )
        .chain(
            spec.inner_joins
                .iter()
                .enumerate()
                .map(|(index, join)| (JoinKind::Inner, join, Some(index))),
        );
    for (index, (kind, join, inner_index)) in joins.enumerate() {
        let child = part.child(index);
        let edge = tree.edges.len();
        let named = inner_index
            .map(|inner_index| {
                Condition::new(
                    join.main_table_column.clone(),
                    ComparisonOperator::EXISTS,
                    Value::Int(inner_index as i64),
                )
            })
            .filter(|leaf| spec.main_table.filter.contains(leaf));
        let leaf = match (&named, kind) {
            (Some(_), _) => LeafKey::Own(edge),
            (None, JoinKind::Left) => LeafKey::Shared(child.clone(), join.sub_table_column.clone()),
            (None, JoinKind::Right | JoinKind::Inner) => {
                LeafKey::Shared(part.clone(), join.main_table_column.clone())
            }
        };
        tree.edges.push(Edge {
            kind,
            parent: part.clone(),
            child: child.clone(),
            parent_column: join.main_table_column.clone(),
            child_column: join.sub_table_column.clone(),
            leaf,
            named,
            counts: JoinKeyCounts::default(),
        });
        node.children.push(edge);
        add_node(tree, &join.sub, child, Some(edge));
    }
    tree.nodes.insert(part, node);
}

/// Append the subtree at `part` in forwarding order: LEFT subtrees (driven
/// by this node) first, the node, then RIGHT and INNER subtrees (which
/// drive it).
fn forwarding_order(tree: &Tree, part: &QueryPart, out: &mut Vec<QueryPart>) {
    let node = &tree.nodes[part];
    for &edge in &node.children {
        if tree.edges[edge].kind == JoinKind::Left {
            forwarding_order(tree, &tree.edges[edge].child, out);
        }
    }
    out.push(part.clone());
    for &edge in &node.children {
        if tree.edges[edge].kind != JoinKind::Left {
            forwarding_order(tree, &tree.edges[edge].child, out);
        }
    }
}

/// Append the subtree at `part` in registration order: RIGHT and INNER
/// subtrees (which fill this node's sets) first, the node, then LEFT
/// subtrees.
fn registration_order(tree: &Tree, part: &QueryPart, out: &mut Vec<QueryPart>) {
    let node = &tree.nodes[part];
    for &edge in &node.children {
        if tree.edges[edge].kind != JoinKind::Left {
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

/// Whether every edge reading `edge`'s set currently references `value`:
/// the membership test of a shared leaf's intersection, trivially true for
/// a set of the edge's own.
fn referenced_by_all(tree: &Tree, edge: usize, value: &Value) -> bool {
    match &tree.edges[edge].leaf {
        LeafKey::Own(_) => true,
        key => tree
            .edges
            .iter()
            .filter(|other| other.leaf == *key)
            .all(|other| other.counts.left.contains_key(value)),
    }
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

impl Default for MultiTableIVM {
    /// [`MultiTableIVM::new`].
    fn default() -> Self {
        Self::new()
    }
}

impl MultiTableIVM {
    /// An empty join layer.
    pub fn new() -> Self {
        MultiTableIVM {
            single: SingleTableIVM::new(),
            trees: HashMap::new(),
            next_tree: 0,
            by_sub: HashMap::new(),
            next_sub: 0,
            parts: HashMap::new(),
            clients: HashMap::new(),
            by_client: HashMap::new(),
        }
    }

    /// Register a multi-table subscription, returning its engine id and
    /// whatever of its initial snapshot is available at once, as
    /// operations. Ids are handed out by the join layer and never reused;
    /// the layer above maps a client's own ids to them. A spec already
    /// registered by another subscription is shared: the new subscriber
    /// joins that tree and is served the shared parts' current rows,
    /// touching no storage (rows still on their way reach it as they land,
    /// like every other subscriber's). A new spec registers its parts in
    /// post-order as their reads land (see the module docs), so its
    /// snapshot arrives through [`MultiTableIVM::land_fetch`].
    pub fn register_query(&mut self, query: MultiTableReadQuery) -> (SubId, Vec<MultiTableUpdate>) {
        let sub = SubId(self.next_sub);
        self.next_sub += 1;
        let mut out = Vec::new();
        if let Some((&tree_id, _)) = self.trees.iter().find(|(_, tree)| *tree.spec == query) {
            self.by_sub.insert(sub, tree_id);
            let tree = self.trees.get_mut(&tree_id).expect("found just above");
            tree.subscribers.push(sub);
            for part in &tree.post_order {
                let node = &tree.nodes[part];
                let Some(rows) = node.part.and_then(|inner| self.single.rows_for(inner)) else {
                    continue;
                };
                for (key, row) in rows {
                    if !shown_in(tree, part, &row) {
                        continue;
                    }
                    out.push(MultiTableUpdate {
                        query: sub,
                        table: node.query.table.clone(),
                        part: part.clone(),
                        op: DataFrameOperation::Add(key, row),
                    });
                }
            }
            let parts = tree.nodes.len() as u64;
            self.single.note_shared_snapshots(parts);
            return (sub, out);
        }
        let tree_id = TreeId(self.next_tree);
        self.next_tree += 1;
        let mut tree = build_tree(&query);
        tree.subscribers.push(sub);
        self.trees.insert(tree_id, tree);
        self.by_sub.insert(sub, tree_id);
        self.register_part(tree_id, QueryPart::main(), &mut out);
        (sub, out)
    }

    /// Register the subtree at `part` in post-order. RIGHT and INNER
    /// children not yet live are registered first and the node waits for
    /// them: the last of them to go live comes back here through
    /// [`Self::landed`]. With every such child live the node registers
    /// itself with its full set restrictions; if its rows are all at hand (a twin's) it is live at
    /// once, otherwise it goes live when its read lands. LEFT children
    /// follow from [`Self::landed`].
    fn register_part(&mut self, tree_id: TreeId, part: QueryPart, out: &mut Vec<MultiTableUpdate>) {
        let (waiting, own) = {
            let tree = &self.trees[&tree_id];
            let node = &tree.nodes[&part];
            if node.part.is_some() {
                return;
            }
            let waiting: Vec<QueryPart> = node
                .children
                .iter()
                .map(|&edge| &tree.edges[edge])
                .filter(|edge| edge.kind != JoinKind::Left && !tree.nodes[&edge.child].live)
                .map(|edge| edge.child.clone())
                .collect();
            (waiting, node.query.clone())
        };
        if !waiting.is_empty() {
            for child in waiting {
                self.register_part(tree_id, child, out);
            }
            return;
        }
        let limit = if part.is_main() { own.limit } else { u32::MAX };
        let query = SingleTableReadQuery {
            filter: self.restricted_filter(tree_id, &part),
            limit,
            ..own.clone()
        };
        let (inner, ops) = self.single.register_query(query);
        if let Some(node) = self
            .trees
            .get_mut(&tree_id)
            .and_then(|tree| tree.nodes.get_mut(&part))
        {
            node.part = Some(inner);
        }
        self.parts.insert(inner, (tree_id, part.clone()));
        for op in ops {
            self.emit(tree_id, &part, &own.table, op.clone(), out);
            if let DataFrameOperation::Add(_, row) = &op {
                self.arrived(tree_id, &part, row, out);
            }
        }
        if !self.single.is_pending(inner) {
            self.landed(tree_id, &part, out);
        }
    }

    /// `part`'s rows have all arrived: mark it live, register its LEFT
    /// children (their sets are now filled by its rows), and, if it is a
    /// RIGHT or INNER child whose parent is still waiting, let the parent
    /// try to register.
    fn landed(&mut self, tree_id: TreeId, part: &QueryPart, out: &mut Vec<MultiTableUpdate>) {
        let (left_children, waiting_parent) = {
            let Some(tree) = self.trees.get_mut(&tree_id) else {
                return;
            };
            let Some(node) = tree.nodes.get_mut(part) else {
                return;
            };
            if node.live {
                return;
            }
            node.live = true;
            let node = &tree.nodes[part];
            let left_children: Vec<QueryPart> = node
                .children
                .iter()
                .map(|&edge| &tree.edges[edge])
                .filter(|edge| edge.kind == JoinKind::Left)
                .map(|edge| edge.child.clone())
                .collect();
            let waiting_parent = node
                .parent
                .map(|edge| &tree.edges[edge])
                .filter(|edge| {
                    edge.kind != JoinKind::Left && tree.nodes[&edge.parent].part.is_none()
                })
                .map(|edge| edge.parent.clone());
            (left_children, waiting_parent)
        };
        for child in left_children {
            self.register_part(tree_id, child, out);
        }
        if let Some(parent) = waiting_parent {
            self.register_part(tree_id, parent, out);
        }
    }

    /// Land the rows a part's storage read returned (brought up to the
    /// engine's position by the runtime): merge them through the inner
    /// engine, forward the resulting operations like a write's, letting
    /// arrivals cascade down the tree (each may reference further join
    /// values and ask for further reads), and, for each part the read
    /// served that is not yet live and has no read left out, continue the
    /// registration walk from it. A read for parts that are gone lands as
    /// nothing.
    pub fn land_fetch(
        &mut self,
        fetch: &Fetch,
        rows: &[(DataFrameKey, DataFrameRow)],
    ) -> Vec<MultiTableUpdate> {
        let readers = self.single.readers_of(fetch);
        let applied = self.single.land_fetch(fetch, rows);
        let mut out = self.forward(applied);
        for sub in readers {
            let Some((tree_id, part)) = self.parts.get(&sub).cloned() else {
                continue;
            };
            let live = self
                .trees
                .get(&tree_id)
                .and_then(|tree| tree.nodes.get(&part))
                .is_some_and(|node| node.live);
            if !live && !self.single.is_pending(sub) {
                self.landed(tree_id, &part, &mut out);
            }
        }
        out
    }

    /// Take the storage reads the inner parts asked for since the last
    /// call.
    pub fn take_requests(&mut self) -> Vec<Fetch> {
        self.single.take_requests()
    }

    /// A part's registered filter: its own `WHERE` with every `EXISTS`
    /// leaf bound in place to its edge's set, and one set-valued `IN` leaf
    /// conjoined per set the unnamed edges driving it read.
    fn restricted_filter(&self, tree_id: TreeId, part: &QueryPart) -> Where {
        let tree = &self.trees[&tree_id];
        let mut filter = tree.nodes[part].query.filter.clone();
        let mut conjoined: Vec<LeafKey> = Vec::new();
        let mut parts = Vec::new();
        for edge in tree.edges.iter().filter(|edge| edge.driven() == part) {
            let bound = leaf_condition(edge.driven_column(), &tree.leaves[&edge.leaf]);
            match &edge.named {
                Some(unbound) => filter.replace_condition(unbound, &bound),
                None if conjoined.contains(&edge.leaf) => {}
                None => {
                    conjoined.push(edge.leaf.clone());
                    parts.push(Where::Condition(bound));
                }
            }
        }
        debug_assert!(
            !filter
                .leaf_conditions()
                .iter()
                .any(|leaf| leaf.comparison_operator == ComparisonOperator::EXISTS),
            "an EXISTS leaf names no inner join of its node"
        );
        parts.insert(0, filter);
        Where::AND(parts)
    }

    /// Remove a subscription. Its tree lives on while other subscriptions
    /// share it; with the last one gone, every inner part and the join
    /// state go too. Unknown ids are a no-op.
    pub fn unregister_query(&mut self, sub: SubId) {
        let Some(tree_id) = self.by_sub.remove(&sub) else {
            return;
        };
        let Some(tree) = self.trees.get_mut(&tree_id) else {
            return;
        };
        tree.subscribers.retain(|subscriber| *subscriber != sub);
        if !tree.subscribers.is_empty() {
            return;
        }
        let Some(tree) = self.trees.remove(&tree_id) else {
            return;
        };
        for node in tree.nodes.values() {
            if let Some(inner) = node.part {
                self.single.unregister_query(inner);
                self.parts.remove(&inner);
            }
        }
    }

    /// Route one write through the inner engine and forward the resulting
    /// per-part operations to every subscriber of their tree, maintaining
    /// the join state on the way (see the module docs for the order and
    /// the replace-pair diffing).
    pub fn incremental_update(&mut self, write: &WriteQuery) -> Vec<MultiTableUpdate> {
        let applied = self.single.incremental_update(write);
        self.forward(applied)
    }

    /// Forward the inner engine's operations to the subscribers of their
    /// trees, every driven part before its driver, diffing replace pairs
    /// and cascading arrivals and departures.
    fn forward(&mut self, applied: Vec<SingleTableUpdate>) -> Vec<MultiTableUpdate> {
        let mut tagged: Vec<(TreeId, usize, QueryPart, SingleTableUpdate)> = Vec::new();
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
                    self.emit(
                        tree_id,
                        &part,
                        &table,
                        DataFrameOperation::Delete(key, old.clone()),
                        &mut out,
                    );
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

    /// The rows currently shown for one part of a subscription — key →
    /// image, the client's view of the part; an inspection seam for tests
    /// and debugging. `None` for unknown ids or parts.
    pub fn rows_for(
        &self,
        sub: SubId,
        part: QueryPart,
    ) -> Option<HashMap<DataFrameKey, DataFrameRow>> {
        let tree = self.trees.get(self.by_sub.get(&sub)?)?;
        let node = tree.nodes.get(&part)?;
        let rows = self.single.rows_for(node.part?)?;
        Some(
            rows.into_iter()
                .filter(|(_, row)| shown_in(tree, &part, row))
                .collect(),
        )
    }

    /// The inner engine's routing counters.
    pub fn stats(&self) -> &IvmStats {
        self.single.stats()
    }

    /// Forward one part operation to every subscriber of its tree, unless
    /// the row is not shown (a child with no shown parent row).
    fn emit(
        &self,
        tree_id: TreeId,
        part: &QueryPart,
        table: &TableName,
        op: DataFrameOperation,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let Some(tree) = self.trees.get(&tree_id) else {
            return;
        };
        if !shown_in(tree, part, op.row()) {
            return;
        }
        for subscriber in &tree.subscribers {
            out.push(MultiTableUpdate {
                query: *subscriber,
                table: table.clone(),
                part: part.clone(),
                op: op.clone(),
            });
        }
    }

    /// A row now held by `part`: reference its join value on every edge
    /// the part drives (a new reference asks for a read; nothing is
    /// emitted for it here) and, if the row is shown, count it on the
    /// edges below it, admitting the children it uncovers.
    fn arrived(
        &mut self,
        tree_id: TreeId,
        part: &QueryPart,
        row: &DataFrameRow,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let shown = shown_in(&self.trees[&tree_id], part, row);
        for (edge, drives, column) in edge_steps(&self.trees[&tree_id], part) {
            if drives {
                self.reference(tree_id, edge, join_value(row, &column));
            }
        }
        if shown {
            self.appear(tree_id, part, row, out);
        }
    }

    /// A row no longer held by `part`: the mirror of [`Self::arrived`].
    fn departed(
        &mut self,
        tree_id: TreeId,
        part: &QueryPart,
        row: &DataFrameRow,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        if shown_in(&self.trees[&tree_id], part, row) {
            self.vanish(tree_id, part, row, out);
        }
        for (edge, drives, column) in edge_steps(&self.trees[&tree_id], part) {
            if drives {
                self.release(tree_id, edge, &join_value(row, &column), out);
            }
        }
    }

    /// A row of `part` replaced in place: move references only on edges
    /// whose join value actually changed — the new value referenced first,
    /// the old released after — so a kept value never crosses zero; the
    /// shown counts below it move the same way.
    fn replaced(
        &mut self,
        tree_id: TreeId,
        part: &QueryPart,
        old: &DataFrameRow,
        new: &DataFrameRow,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let (was, is) = {
            let tree = &self.trees[&tree_id];
            (shown_in(tree, part, old), shown_in(tree, part, new))
        };
        for (edge, drives, column) in edge_steps(&self.trees[&tree_id], part) {
            let old_value = join_value(old, &column);
            let new_value = join_value(new, &column);
            if drives && old_value != new_value {
                self.reference(tree_id, edge, new_value);
                self.release(tree_id, edge, &old_value, out);
            }
        }
        for edge in self.trees[&tree_id].nodes[part].children.clone() {
            let column = self.trees[&tree_id].edges[edge].parent_column.clone();
            let old_value = join_value(old, &column);
            let new_value = join_value(new, &column);
            if was && is && old_value == new_value {
                continue;
            }
            if is {
                self.show(tree_id, edge, new_value, out);
            }
            if was {
                self.hide(tree_id, edge, &old_value, out);
            }
        }
    }

    /// A shown row of `part` is in place: count it on every edge below,
    /// admitting the children its value uncovers.
    fn appear(
        &mut self,
        tree_id: TreeId,
        part: &QueryPart,
        row: &DataFrameRow,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        for edge in self.trees[&tree_id].nodes[part].children.clone() {
            let column = self.trees[&tree_id].edges[edge].parent_column.clone();
            self.show(tree_id, edge, join_value(row, &column), out);
        }
    }

    /// A shown row of `part` is going: the mirror of [`Self::appear`].
    fn vanish(
        &mut self,
        tree_id: TreeId,
        part: &QueryPart,
        row: &DataFrameRow,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        for edge in self.trees[&tree_id].nodes[part].children.clone() {
            let column = self.trees[&tree_id].edges[edge].parent_column.clone();
            self.hide(tree_id, edge, &join_value(row, &column), out);
        }
    }

    /// One more shown parent row carries `value` on `edge`; on the 0 → 1
    /// crossing of a gating edge, admit the child rows held for the value.
    fn show(
        &mut self,
        tree_id: TreeId,
        edge: usize,
        value: Value,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let Some(counts) = self.counts_mut(tree_id, edge) else {
            return;
        };
        let count = counts.shown.entry(value.clone()).or_insert(0);
        *count += 1;
        if *count != 1 || !self.trees[&tree_id].edges[edge].gates_child() {
            return;
        }
        let (child, column) = {
            let edge = &self.trees[&tree_id].edges[edge];
            (edge.child.clone(), edge.child_column.clone())
        };
        let Some((inner, table)) = self.registered_part(tree_id, &child) else {
            return;
        };
        for (key, row) in self.single.rows_matching(inner, column.as_str(), &value) {
            self.emit(
                tree_id,
                &child,
                &table,
                DataFrameOperation::Add(key, row.clone()),
                out,
            );
            self.appear(tree_id, &child, &row, out);
        }
    }

    /// One shown parent row fewer carries `value` on `edge`; on the 1 → 0
    /// crossing of a gating edge, retract the child rows held for the
    /// value before the count drops, so their `Delete`s still pass the
    /// gate.
    fn hide(
        &mut self,
        tree_id: TreeId,
        edge: usize,
        value: &Value,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let crossing = self
            .trees
            .get(&tree_id)
            .and_then(|tree| tree.edges.get(edge))
            .is_some_and(|e| e.gates_child() && e.counts.shown.get(value).copied() == Some(1));
        if crossing {
            let (child, column) = {
                let edge = &self.trees[&tree_id].edges[edge];
                (edge.child.clone(), edge.child_column.clone())
            };
            if let Some((inner, table)) = self.registered_part(tree_id, &child) {
                for (key, row) in self.single.rows_matching(inner, column.as_str(), value) {
                    self.emit(
                        tree_id,
                        &child,
                        &table,
                        DataFrameOperation::Delete(key, row.clone()),
                        out,
                    );
                    self.vanish(tree_id, &child, &row, out);
                }
            }
        }
        Self::drop_in(
            self.counts_mut(tree_id, edge)
                .map(|counts| &mut counts.shown),
            value,
        );
    }

    /// A driver row now carries `value` on `edge`: bump `left`, and on the
    /// 0 → 1 crossing, if every edge reading the same set now references
    /// the value, add it to the set (one index filing) and ask for the
    /// value's driven rows (one narrowed storage read); when it lands
    /// ([`Self::land_fetch`]) they are forwarded and arrive at the driven
    /// node. Before the driven part is registered the set is filled
    /// directly; registration files it whole.
    fn reference(&mut self, tree_id: TreeId, edge: usize, value: Value) {
        let crossing = self.left_count(tree_id, edge, &value) == 0;
        self.left_bump(tree_id, edge, value.clone());
        if !crossing {
            return;
        }
        if !referenced_by_all(&self.trees[&tree_id], edge, &value) {
            return;
        }
        let (driven, column, key) = self.leaf_of(tree_id, edge);
        let set = self.trees[&tree_id].leaves[&key].clone();
        let Some((inner, _)) = self.registered_part(tree_id, &driven) else {
            set.insert(&value);
            return;
        };
        if !self
            .single
            .set_insert(inner, &leaf_condition(&column, &set), &value)
        {
            return;
        }
        self.single
            .fetch(inner, column.as_str(), std::slice::from_ref(&value));
    }

    /// A driver row no longer carries `value` on `edge`: drop `left`, and
    /// on the crossing to 0, if the value was in the set, remove it (one
    /// index unfiling), prune the value's held driven rows the filter no
    /// longer admits (no storage round-trip), forward the `Delete`s, and
    /// let them depart from the driven node.
    fn release(
        &mut self,
        tree_id: TreeId,
        edge: usize,
        value: &Value,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let crossing = self.left_count(tree_id, edge, value) == 1;
        self.left_drop(tree_id, edge, value);
        if !crossing {
            return;
        }
        let (driven, column, key) = self.leaf_of(tree_id, edge);
        let set = self.trees[&tree_id].leaves[&key].clone();
        if !set.contains(value) {
            return;
        }
        let Some((inner, table)) = self.registered_part(tree_id, &driven) else {
            set.remove(value);
            return;
        };
        if !self
            .single
            .set_remove(inner, &leaf_condition(&column, &set), value)
        {
            return;
        }
        let deletes = self
            .single
            .prune_rows(inner, column.as_str(), std::slice::from_ref(value));
        for op in deletes {
            self.emit(tree_id, &driven, &table, op.clone(), out);
            if let DataFrameOperation::Delete(_, row) = &op {
                self.departed(tree_id, &driven, row, out);
            }
        }
    }

    /// The driven part of `edge`, the column its leaf is on, and the set
    /// the leaf reads.
    fn leaf_of(&self, tree_id: TreeId, edge: usize) -> (QueryPart, ColumnName, LeafKey) {
        let edge = &self.trees[&tree_id].edges[edge];
        (
            edge.driven().clone(),
            edge.driven_column().clone(),
            edge.leaf.clone(),
        )
    }

    /// The inner id and table of `part` once it is registered; `None`
    /// while registration has not reached it yet.
    fn registered_part(&self, tree_id: TreeId, part: &QueryPart) -> Option<(SubId, TableName)> {
        let node = self.trees.get(&tree_id)?.nodes.get(part)?;
        node.part.map(|inner| (inner, node.query.table.clone()))
    }

    /// `value`'s current `left` count on `edge` (zero when absent).
    fn left_count(&self, tree_id: TreeId, edge: usize, value: &Value) -> u64 {
        self.trees
            .get(&tree_id)
            .and_then(|tree| tree.edges.get(edge))
            .and_then(|edge| edge.counts.left.get(value).copied())
            .unwrap_or(0)
    }

    /// Mutable access to one edge's counts.
    fn counts_mut(&mut self, tree_id: TreeId, edge: usize) -> Option<&mut JoinKeyCounts> {
        self.trees
            .get_mut(&tree_id)
            .and_then(|tree| tree.edges.get_mut(edge))
            .map(|edge| &mut edge.counts)
    }

    /// Increment `value`'s `left` count on `edge`.
    fn left_bump(&mut self, tree_id: TreeId, edge: usize, value: Value) {
        if let Some(counts) = self.counts_mut(tree_id, edge) {
            *counts.left.entry(value).or_insert(0) += 1;
        }
    }

    /// Decrement `value`'s `left` count on `edge`, removing the entry at
    /// zero.
    fn left_drop(&mut self, tree_id: TreeId, edge: usize, value: &Value) {
        Self::drop_in(
            self.counts_mut(tree_id, edge)
                .map(|counts| &mut counts.left),
            value,
        );
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

impl MultiTableIVM {
    /// Address per-subscription operations to their clients and group
    /// them per client and row.
    fn addressed(&self, updates: Vec<MultiTableUpdate>) -> Vec<ClientUpdate> {
        group(
            updates
                .into_iter()
                .filter_map(|update| {
                    let client = *self.clients.get(&update.query)?;
                    Some(Raw {
                        client,
                        table: update.table,
                        target: Target {
                            sub: update.query,
                            part: update.part,
                        },
                        op: update.op,
                    })
                })
                .collect(),
        )
    }
}

impl Engine for MultiTableIVM {
    type Query = MultiTableReadQuery;

    /// [`MultiTableIVM::register_query`], its snapshot addressed to
    /// `client`.
    fn subscribe(
        &mut self,
        client: ClientId,
        query: MultiTableReadQuery,
    ) -> (SubId, Vec<ClientUpdate>) {
        let (sub, updates) = self.register_query(query);
        self.clients.insert(sub, client);
        self.by_client.entry(client).or_default().insert(sub);
        (sub, self.addressed(updates))
    }

    /// [`MultiTableIVM::unregister_query`].
    fn unsubscribe(&mut self, sub: SubId) {
        if let Some(client) = self.clients.remove(&sub)
            && let Some(subs) = self.by_client.get_mut(&client)
        {
            subs.remove(&sub);
            if subs.is_empty() {
                self.by_client.remove(&client);
            }
        }
        self.unregister_query(sub);
    }

    /// Every subscription of `client`, unregistered.
    fn unsubscribe_client(&mut self, client: ClientId) {
        for sub in self.by_client.remove(&client).unwrap_or_default() {
            self.clients.remove(&sub);
            self.unregister_query(sub);
        }
    }

    /// [`MultiTableIVM::incremental_update`], grouped per client.
    fn route(&mut self, write: &WriteQuery) -> Vec<ClientUpdate> {
        let updates = self.incremental_update(write);
        self.addressed(updates)
    }

    /// [`MultiTableIVM::land_fetch`], grouped per client.
    fn land(&mut self, fetch: &Fetch, rows: &[(DataFrameKey, DataFrameRow)]) -> Vec<ClientUpdate> {
        let updates = self.land_fetch(fetch, rows);
        self.addressed(updates)
    }

    /// [`MultiTableIVM::take_requests`].
    fn requests(&mut self) -> Vec<Fetch> {
        self.take_requests()
    }
}
