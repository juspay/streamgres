//! Multi-table subscriptions: a tree of single-table parts joined by
//! edges, maintained over the single-table engine, with one tree shared by
//! every subscription that registers the same spec.
//!
//! A [`MultiTableReadQuery`] is a tree: every node is a single-table query,
//! every edge a [`Join`], and a child is itself a full multi-table query.
//! Each node registers as one inner single-table subscription — a *part*,
//! addressed by its [`QueryPart`] path — and the client receives every
//! operation tagged with its part, keeps one frame per part, and composes
//! the join itself.
//!
//! # Driver and driven
//!
//! Every edge says who drives it ([`Join::driver`]): the driver's held
//! rows decide which join values are *referenced*, and the driven side's
//! part carries the restriction `driven_column IN <set>` inside its
//! filter. An edge the main drives (LEFT, and the inner edge evaluated from
//! the main) restricts the sub; an edge the sub drives (RIGHT, and the
//! inner edge evaluated from the sub) restricts the main. Because the
//! restriction lives inside the driven part's filter, writes on the driven
//! table route natively through the single engine: a row matching the
//! filter (restriction included) fires an `Add`, a held row moving out
//! fires a `Delete` via membership, and an unreferenced row never fires at
//! all. The join layer never inspects driven-side writes; it forwards them
//! and keeps its counts. Everything the layer does follows from the edge's
//! driver, its `is_inner`, and the sub node's limit; no planning happens
//! here.
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
//! ([`ComparisonOperator::EXISTS`], naming one of the node's inner edges)
//! is bound in place when the sub drives that edge, so `visibility =
//! 'PUBLIC' OR EXISTS(...)` is one filter with the restriction inside its
//! `OR`; an edge no leaf names is conjoined at the top. Edges bound by a
//! leaf have a set of their own; unnamed edges driving one part on one
//! column — a LEFT parent above it and a RIGHT child below it, both on
//! `id` — share one set holding the **intersection** of their referenced
//! values. When a value leaves a set the value's held rows are
//! re-evaluated against the filter, not deleted outright, so a row another
//! branch still admits stays.
//!
//! # The gates
//!
//! A row is *shown* (delivered to clients) only under a shown parent row:
//! under every edge but a RIGHT one a child row is shown while at least
//! one shown parent row carries its join value, under a RIGHT edge always
//! (the child is preserved), and the root always. Per edge and per value
//! the layer counts the shown parent rows; a crossing of that count admits
//! (one `Add` each) or retracts (one `Delete` each) the child rows for the
//! value, and each of those rows in turn is counted on the edges below it,
//! so a chain of inner edges shows exactly the rows that reach the root.
//! Held rows that are not shown still drive: a sub-driven inner child's
//! rows are evaluated first and fill the parent's set whether or not the
//! parent row that makes them visible has arrived.
//!
//! An inner edge the **main drives** cannot restrict the main's rows (the
//! sub's values are not known before the main is read; the sub is narrowed
//! to the main's values), so the main part registers its `WHERE` with the
//! edge's `EXISTS` leaf taken as true and the leaf becomes a gate: a main
//! row is shown while its `WHERE` holds with every `EXISTS` leaf read as
//! "a held sub row carries this row's join value" (`matched`, counted per
//! value as the sub's rows arrive and depart; unnamed, the test is
//! conjoined). A crossing of that count re-evaluates the main rows
//! carrying the value and admits or retracts them, cascading below as any
//! other visibility change.
//!
//! **What a row counts for above it.** A held row of a gated node acts on
//! the edge above its node only while its own gate is open: it is a
//! *match* for the parent rows it gates, or, when its node drives that
//! edge, a *reference* filling the parent's set. A user without a profile
//! opens no ticket's gate and drives no ticket in, however long it is
//! held; a private channel the reader is no participant of drives no
//! conversation. The node records which of its rows have *risen*
//! ([`Node::risen`]), so a row leaves exactly the counts it entered,
//! whatever the counts read by the time it goes. Downwards a row
//! references its join values as soon as it is held, gate open or not:
//! those references are what fetches the sub rows its gate is decided by.
//! Existence therefore flows leaves to root through open gates and
//! visibility root to leaves through shown rows; a gate reads risen rows,
//! never shown ones, so there is no cycle. A node that drives its parent
//! is waited for until its gates are decided (its own rows and those of
//! every node gating it have arrived), so the parent registers once,
//! with the whole set.
//!
//! # A page under a gate
//!
//! A node with a `LIMIT` that drives an inner edge is a page of the rows
//! *the edge admits*: `messages WHERE id = ? LIMIT 1` under the access
//! rule's `EXISTS`, the latest conversation of each channel whose first
//! message the reader may see. The single engine's window knows nothing
//! of gates, so the layer tells it: once none of the tree's reads is out
//! (until then a closed gate may be a sub row not yet fetched), the rows
//! the page shows whose gate is closed are reported as rejected
//! ([`SingleTableIVM::reject_rows`]); the window counts without them and
//! shows further rows, which arrive here like any others and ask for
//! their own sub rows, and the next round follows when those reads have
//! landed ([`MultiTableIVM::settle_pages`]). A gate opening later takes
//! the row off the rejected list and the page draws back. A client is
//! only ever sent the rows whose gate is open, and the subscription is
//! not hydrated while a round is under way.
//!
//! # Driven windows
//!
//! A driven sub node with an `ORDER BY` / `LIMIT` of its own means, as it
//! does in the client's `related`, the best *n* rows **per parent row**: the
//! latest three messages of every conversation, not three messages in
//! all. Such a node registers no single part. It is *fanned*: one inner
//! part per referenced join value, `child WHERE own_filter AND column =
//! value ORDER BY … LIMIT n`, so every value has a window of its own
//! maintained by the single engine like any other, refills included. A
//! value entering the driver's set registers that value's part (its read
//! lands like a registration's); a value leaving unregisters it, its held
//! rows departing first. Every per-value part is addressed by the same
//! [`QueryPart`], so the client sees one part whose rows happen to be
//! windowed per parent. A sub node that drives its edge is read whole, so
//! its limit is an ordinary window on that one part.
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
//! carrying the value), `shown` (shown parent rows carrying it) and, for
//! an inner edge the main drives, `matched` (held sub rows carrying it).
//! Only zero crossings of `left` act on the set: the value's driven rows
//! are fetched (one narrowed storage query) or pruned from current data.
//! Rows a fetch brings in are **arrivals** at the driven node and rows a
//! prune removes are **departures**, and the driven node may itself drive
//! further edges, so the same handling cascades down the tree — the
//! recursion that makes nesting work with one code path. The rows of one
//! join value are found through the value index the single engine keeps
//! on every join column ([`SingleTableIVM::index_column`]), so a crossing
//! costs the matches, not the part.
//!
//! # Order
//!
//! Registration is a post-order walk: the children that drive a node
//! register first (their rows fill the sets it is restricted by), then the
//! node, then the children it drives (restricted by its rows); during that
//! walk crossings only fill sets, since every part is registered with its
//! full set. A part's rows arrive when its storage read **lands**, some
//! time after it is asked for, so the walk is driven by landings: a node
//! registers once every child that drives it is *live* (its own read
//! landed and nothing further out for it), and the children it drives
//! register once it is live itself. A part served from a twin is live at
//! once. Within one write, each part's native operations are forwarded
//! with every **driven part before its driver** — because handling a
//! driver's operation may prune the driven frame, and a stale driven
//! operation forwarded after that prune would resurrect a row on the
//! client. An in-place replacement arrives as an adjacent `Delete(old)` +
//! `Add(new)` pair and is diffed per edge, so a rewrite that keeps a join
//! value never swings its count through zero.
//!
//! Inner parts are ordinary subscriptions of the inner engine, addressed by
//! the ids it hands out and looked up in a map; the reads they ask for
//! surface through the inner engine's request list, and land back through
//! [`MultiTableIVM::land_fetch`], which cascades the landed rows' arrivals
//! exactly as it cascades a write's.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::rc::Rc;

use super::engine::Footprint;
use super::predicate::evaluate_with;
use super::stats::IvmStats;
use super::update::{Raw, Target, group};
use super::{ClientUpdate, Engine, Fetch, QueryPart, SingleTableIVM, SingleTableUpdate};
use crate::model::frame::SharedRow;
use crate::model::{
    ClientId, ColumnName, ComparisonOperator, Condition, DataFrameKey, DataFrameOperation,
    DataFrameRow, Driver, IdMap, Join, MultiTableReadQuery, SharedSet, SingleTableReadQuery, SubId,
    TableName, Value, Where, WriteQuery,
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

/// Which shared set an edge restricts its driven part through: an edge
/// bound by an `EXISTS` leaf, or fanning its child, has a set of its own;
/// unnamed edges driving one part on one column share a set holding the
/// intersection of their referenced values.
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
///   crossings admit and retract the child rows under a gating edge.
/// - `matched`: how many held child rows carry the value, kept for an
///   inner edge the main drives; its zero crossings admit and retract the
///   parent rows the edge gates.
///
/// Entries leave the maps when they reach zero, so they only hold live
/// values.
#[derive(Default)]
struct JoinKeyCounts {
    left: HashMap<Value, u64>,
    shown: HashMap<Value, u64>,
    matched: HashMap<Value, u64>,
}

/// One join edge of a registered tree, its direction made explicit
/// through [`Edge::driven`].
///
/// - `leaf`: the set the edge's restriction reads.
/// - `bound`: the `EXISTS` leaf in the parent's own filter this edge
///   binds in place to its set, when the sub drives and the filter names
///   it; otherwise the restriction is conjoined at the top.
/// - `gate`: the `EXISTS` leaf in the parent's own filter this edge
///   answers per row from its `matched` count, when the main drives an
///   inner edge and the filter names it; unnamed, the test is conjoined.
struct Edge {
    driver: Driver,
    is_inner: bool,
    parent: QueryPart,
    child: QueryPart,
    parent_column: ColumnName,
    child_column: ColumnName,
    leaf: LeafKey,
    bound: Option<Condition>,
    gate: Option<Condition>,
    counts: JoinKeyCounts,
}

impl Edge {
    /// The part whose filter carries this edge's leaf.
    fn driven(&self) -> &QueryPart {
        match self.driver {
            Driver::Main => &self.child,
            Driver::Sub => &self.parent,
        }
    }

    /// The column the leaf is on.
    fn driven_column(&self) -> &ColumnName {
        match self.driver {
            Driver::Main => &self.child_column,
            Driver::Sub => &self.parent_column,
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

    /// Whether the child's rows are shown only under a shown parent row
    /// (every edge but a RIGHT one).
    fn gates_child(&self) -> bool {
        self.driver != Driver::Sub || self.is_inner
    }

    /// Whether the parent's rows are shown only while a child row matches
    /// them (an inner edge the main drives).
    fn gates_parent(&self) -> bool {
        self.driver == Driver::Main && self.is_inner
    }

    /// How many held child rows carry `value`: the driver count when the
    /// child drives, the matched count when the main does. What an
    /// `EXISTS` leaf reads.
    fn child_count(&self, value: &Value) -> u64 {
        let counts = match self.driver {
            Driver::Sub => &self.counts.left,
            Driver::Main => &self.counts.matched,
        };
        counts.get(value).copied().unwrap_or(0)
    }
}

/// The inner parts behind one node: none until registration reaches it,
/// one for an ordinary node, one per referenced join value for a driven
/// window (see the module docs).
enum Parts {
    Unregistered,
    One(SubId),
    Fan(HashMap<Value, SubId>),
}

/// One node of a registered tree: its inner parts (once registered),
/// whether it is a driven window (`fanned`), whether its rows have all
/// arrived (`live`), its place among the edges, the child edges that
/// gate its own rows (`gates`, the inner edges it drives), and, for a
/// gated node that acts on the edge above it, the keys of its rows
/// currently counted there (`risen`: held, with the gate open).
struct Node {
    parts: Parts,
    fanned: bool,
    live: bool,
    query: SingleTableReadQuery,
    parent: Option<usize>,
    children: Vec<usize>,
    gates: Vec<usize>,
    risen: HashSet<DataFrameKey>,
}

impl Node {
    /// Whether registration has reached this node.
    fn is_registered(&self) -> bool {
        !matches!(self.parts, Parts::Unregistered)
    }

    /// Every inner part currently behind the node.
    fn inner_parts(&self) -> Vec<SubId> {
        match &self.parts {
            Parts::Unregistered => Vec::new(),
            Parts::One(inner) => vec![*inner],
            Parts::Fan(fan) => fan.values().copied().collect(),
        }
    }
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
/// - `pages`: the parts that are pages under a gate (a finite limit and
///   inner edges the node drives), whose windows are told which rows the
///   gate rejects once the tree's reads have landed.
struct Tree {
    spec: Rc<MultiTableReadQuery>,
    subscribers: Vec<SubId>,
    nodes: IdMap<QueryPart, Node>,
    edges: Vec<Edge>,
    leaves: HashMap<LeafKey, SharedSet>,
    rank: IdMap<QueryPart, usize>,
    post_order: Vec<QueryPart>,
    pages: Vec<QueryPart>,
}

/// How many rounds of rejecting rows and showing more one step may take
/// for one tree before the rest waits for the next step.
const SETTLE_ROUNDS: usize = 64;

/// The join layer's handle for one shared tree; unique for the life of
/// the layer, never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct TreeId(u64);

/// The join layer. Owns the inner [`SingleTableIVM`] exclusively, so its
/// part ids cannot collide with anything registered from outside.
///
/// - `single`: the inner engine holding every part's routing and the
///   shared per-table frames.
/// - `trees`: tree id → the shared tree, and `by_spec` the tree of each
///   registered spec (one lookup per registration).
/// - `by_sub`: subscription id → the tree it subscribes to.
/// - `next_sub`: the next subscription id to hand out; never reused.
/// - `parts`: inner part id → (tree id, part), the reverse map every
///   routed operation goes through.
/// - `clients`: subscription id → the client it belongs to, and
///   `by_client` the reverse, for addressing deltas and for a client's
///   disconnect.
/// - `dirty`: the trees with a page under a gate whose rows moved or
///   whose reads landed in this step, settled before the step returns.
/// - `in_flight`: the rows the inner engine holds whose `Add` this layer
///   has not reached yet within the step being forwarded.
pub struct MultiTableIVM {
    single: SingleTableIVM,
    trees: IdMap<TreeId, Tree>,
    by_spec: HashMap<Rc<MultiTableReadQuery>, TreeId>,
    next_tree: u64,
    by_sub: IdMap<SubId, TreeId>,
    next_sub: u64,
    parts: IdMap<SubId, (TreeId, QueryPart)>,
    clients: IdMap<SubId, ClientId>,
    by_client: IdMap<ClientId, BTreeSet<SubId>>,
    dirty: Vec<TreeId>,
    in_flight: Vec<(SubId, DataFrameKey)>,
}

/// Whether a held row of `part` is shown to clients: the root always, a
/// child under a RIGHT edge always, a child under any other edge only
/// while a shown parent row carries its join value; and, for a node that
/// gates its own rows, only while its `WHERE` holds with the `EXISTS`
/// leaves answered from the edges' counts.
fn shown_in(tree: &Tree, part: &QueryPart, row: &DataFrameRow) -> bool {
    let node = &tree.nodes[part];
    if let Some(edge) = node.parent.map(|edge| &tree.edges[edge])
        && edge.gates_child()
    {
        let value = join_value(row, &edge.child_column);
        if edge
            .counts
            .shown
            .get(&value)
            .is_none_or(|count| *count == 0)
        {
            return false;
        }
    }
    gate_open(tree, node, row)
}

/// Whether the gates of `node` let `row` through: true for a node with
/// none; otherwise the node's own `WHERE` evaluated with every `EXISTS`
/// leaf read as "a held child row carries this row's join value on that
/// edge", and the unnamed gating edges conjoined.
fn gate_open(tree: &Tree, node: &Node, row: &DataFrameRow) -> bool {
    if node.gates.is_empty() {
        return true;
    }
    let inner_edges: Vec<&Edge> = node
        .children
        .iter()
        .map(|edge| &tree.edges[*edge])
        .filter(|edge| edge.is_inner)
        .collect();
    let exists = |leaf: &Condition| -> bool {
        let Value::Int(index) = leaf.value else {
            return false;
        };
        let Some(edge) = usize::try_from(index)
            .ok()
            .and_then(|index| inner_edges.get(index))
        else {
            return false;
        };
        edge.child_count(&join_value(row, &edge.parent_column)) > 0
    };
    if !evaluate_with(&node.query.filter, &row.data, &mut 0, &exists) {
        return false;
    }
    node.gates.iter().all(|edge| {
        let edge = &tree.edges[*edge];
        edge.gate.is_some() || edge.child_count(&join_value(row, &edge.parent_column)) > 0
    })
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
fn build_tree(spec: Rc<MultiTableReadQuery>) -> Tree {
    let mut tree = Tree {
        spec: spec.clone(),
        subscribers: Vec::new(),
        nodes: IdMap::default(),
        edges: Vec::new(),
        leaves: HashMap::new(),
        rank: IdMap::default(),
        post_order: Vec::new(),
        pages: Vec::new(),
    };
    add_node(&mut tree, &spec, QueryPart::main(), None);

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
    tree.pages = post
        .iter()
        .filter(|part| {
            let node = &tree.nodes[*part];
            !node.gates.is_empty() && windowed_limit(node.query.limit)
        })
        .copied()
        .collect();
    tree.post_order = post;
    tree
}

/// Add `spec`'s node at `part` and, recursively, its children, one edge
/// per join in order. An inner edge the node's own filter names through
/// an `EXISTS` leaf (the `i`-th inner edge) is bound in place when the
/// sub drives it and gates the node's rows when the main does.
fn add_node(tree: &mut Tree, spec: &MultiTableReadQuery, part: QueryPart, parent: Option<usize>) {
    let mut node = Node {
        parts: Parts::Unregistered,
        fanned: parent.is_some_and(|edge| {
            tree.edges[edge].driver == Driver::Main && spec.main_table.limit != u32::MAX
        }),
        live: false,
        query: spec.main_table.clone(),
        parent,
        children: Vec::new(),
        gates: Vec::new(),
        risen: HashSet::new(),
    };
    let mut inner_index = 0usize;
    for (index, join) in spec.joins.iter().enumerate() {
        let child = part.child(index);
        let edge = tree.edges.len();
        let named = if join.is_inner {
            let leaf = Condition::new(
                join.main_table_column.clone(),
                ComparisonOperator::EXISTS,
                Value::Int(inner_index as i64),
            );
            inner_index += 1;
            spec.main_table.filter.contains(&leaf).then_some(leaf)
        } else {
            None
        };
        let (bound, gate) = match join.driver {
            Driver::Sub => (named, None),
            Driver::Main => (None, named),
        };
        let leaf = match (&bound, join.driver) {
            (Some(_), _) => LeafKey::Own(edge),
            (None, Driver::Main) if fanned_child(join) => LeafKey::Own(edge),
            (None, Driver::Main) => LeafKey::Shared(child, join.sub_table_column.clone()),
            (None, Driver::Sub) => LeafKey::Shared(part, join.main_table_column.clone()),
        };
        tree.edges.push(Edge {
            driver: join.driver,
            is_inner: join.is_inner,
            parent: part,
            child,
            parent_column: join.main_table_column.clone(),
            child_column: join.sub_table_column.clone(),
            leaf,
            bound,
            gate,
            counts: JoinKeyCounts::default(),
        });
        node.children.push(edge);
        if join.driver == Driver::Main && join.is_inner {
            node.gates.push(edge);
        }
        add_node(tree, &join.sub, child, Some(edge));
    }
    tree.nodes.insert(part, node);
}

/// Whether `limit` is a page: finite and positive.
fn windowed_limit(limit: u32) -> bool {
    limit > 0 && limit < u32::MAX
}

/// Whether `join`'s sub node is a driven window: the main drives it and
/// it has a finite limit.
fn fanned_child(join: &Join) -> bool {
    join.driver == Driver::Main && join.sub.main_table.limit != u32::MAX
}

/// Append the subtree at `part` in forwarding order: the subtrees this
/// node drives first, the node, then the subtrees that drive it.
fn forwarding_order(tree: &Tree, part: &QueryPart, out: &mut Vec<QueryPart>) {
    let node = &tree.nodes[part];
    for &edge in &node.children {
        if tree.edges[edge].driver == Driver::Main {
            forwarding_order(tree, &tree.edges[edge].child, out);
        }
    }
    out.push(*part);
    for &edge in &node.children {
        if tree.edges[edge].driver == Driver::Sub {
            forwarding_order(tree, &tree.edges[edge].child, out);
        }
    }
}

/// Append the subtree at `part` in registration order: the subtrees that
/// drive this node (which fill its sets) first, the node, then the
/// subtrees it drives.
fn registration_order(tree: &Tree, part: &QueryPart, out: &mut Vec<QueryPart>) {
    let node = &tree.nodes[part];
    for &edge in &node.children {
        if tree.edges[edge].driver == Driver::Sub {
            registration_order(tree, &tree.edges[edge].child, out);
        }
    }
    out.push(*part);
    for &edge in &node.children {
        if tree.edges[edge].driver == Driver::Main {
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

/// Whether `part` has everything that decides which of its rows count
/// above it: its own rows have arrived and so have those of every node
/// gating it, all the way down. A part that drives its parent is waited
/// for until then, so the parent registers once with the whole set
/// instead of fetching it value by value as the gates open.
fn settled(tree: &Tree, part: &QueryPart) -> bool {
    let node = &tree.nodes[part];
    node.live
        && node
            .gates
            .iter()
            .all(|edge| settled(tree, &tree.edges[*edge].child))
}

/// The node still waiting to register that `part` going live may
/// release: walking up through the edges `part`'s subtree gates, the
/// first unregistered parent a settled node drives.
fn waiting_above(tree: &Tree, part: &QueryPart) -> Option<QueryPart> {
    let mut current = *part;
    loop {
        let edge = &tree.edges[tree.nodes[&current].parent?];
        if edge.driver == Driver::Sub {
            return (!tree.nodes[&edge.parent].is_registered() && settled(tree, &current))
                .then_some(edge.parent);
        }
        if !edge.is_inner {
            return None;
        }
        current = edge.parent;
    }
}

/// The edges `part` drives below it, with its own join column on each.
fn downward_edges(tree: &Tree, part: &QueryPart) -> Vec<(usize, ColumnName)> {
    tree.nodes[part]
        .children
        .iter()
        .map(|&edge| (edge, &tree.edges[edge]))
        .filter(|(_, edge)| edge.driver == Driver::Main)
        .map(|(index, edge)| (index, edge.parent_column.clone()))
        .collect()
}

/// The edge above `part` when the part's rows act on it, with the part's
/// join column and whether the part drives the edge (a sub-driven edge,
/// whose set its values fill) or gates the parent (an inner edge the main
/// drives, whose `matched` count they feed). A LEFT edge the main drives
/// is acted on from above only.
fn upward_edge(tree: &Tree, part: &QueryPart) -> Option<(usize, ColumnName, bool)> {
    let index = tree.nodes[part].parent?;
    let edge = &tree.edges[index];
    if edge.driver == Driver::Sub {
        Some((index, edge.child_column.clone(), true))
    } else if edge.gates_parent() {
        Some((index, edge.child_column.clone(), false))
    } else {
        None
    }
}

/// The columns `part` joins on, each once: what the single engine indexes
/// by value for the part's table.
fn join_columns(tree: &Tree, part: &QueryPart) -> Vec<ColumnName> {
    let mut columns: Vec<ColumnName> = Vec::new();
    for (_, _, column) in edge_steps(tree, part) {
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
    columns
}

/// `filter` with every `EXISTS` leaf that names nothing this node can
/// honor (a leaf naming a non-inner edge, or no edge) made false.
fn unbound_exists_false(filter: Where) -> Where {
    match filter {
        Where::Condition(condition)
            if condition.comparison_operator == ComparisonOperator::EXISTS =>
        {
            Where::OR(Vec::new())
        }
        Where::Condition(condition) => Where::Condition(condition),
        Where::AND(children) => {
            Where::AND(children.into_iter().map(unbound_exists_false).collect())
        }
        Where::OR(children) => Where::OR(children.into_iter().map(unbound_exists_false).collect()),
    }
}

/// Bring together, within one part of one tree, a row's `Delete` and the
/// `Add` that follows it from another inner part (a row moving between
/// the per-value parts of a fanned node): the `Add` is moved right behind
/// the `Delete`, so the two are handled as one replacement.
fn pair_moves(tagged: &mut Vec<(TreeId, usize, QueryPart, SingleTableUpdate)>) {
    let mut index = 0;
    while index < tagged.len() {
        let (tree_id, _, part, update) = &tagged[index];
        if let DataFrameOperation::Delete(key, _) = &update.op {
            let partner =
                tagged[index + 1..]
                    .iter()
                    .position(|(other_tree, _, other_part, other)| {
                        other_tree == tree_id && other_part == part && other.op.key() == key
                    });
            if let Some(offset) = partner
                && offset > 0
                && matches!(
                    tagged[index + 1 + offset].3.op,
                    DataFrameOperation::Add(_, _)
                )
            {
                let add = tagged.remove(index + 1 + offset);
                tagged.insert(index + 1, add);
            }
        }
        index += 1;
    }
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
            trees: IdMap::default(),
            by_spec: HashMap::new(),
            next_tree: 0,
            by_sub: IdMap::default(),
            next_sub: 0,
            parts: IdMap::default(),
            clients: IdMap::default(),
            by_client: IdMap::default(),
            dirty: Vec::new(),
            in_flight: Vec::new(),
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
        if let Some(&tree_id) = self.by_spec.get(&query) {
            self.by_sub.insert(sub, tree_id);
            let tree = self.trees.get_mut(&tree_id).expect("indexed by spec");
            tree.subscribers.push(sub);
            for part in &tree.post_order {
                let node = &tree.nodes[part];
                for inner in node.inner_parts() {
                    let Some(rows) = self.single.rows_for(inner) else {
                        continue;
                    };
                    for (key, row) in rows {
                        if !shown_in(tree, part, &row) {
                            continue;
                        }
                        out.push(MultiTableUpdate {
                            query: sub,
                            table: node.query.table.clone(),
                            part: *part,
                            op: DataFrameOperation::Add(key, row),
                        });
                    }
                }
            }
            let parts = tree.nodes.len() as u64;
            self.single.note_shared_snapshots(parts);
            return (sub, out);
        }
        let tree_id = TreeId(self.next_tree);
        self.next_tree += 1;
        let spec = Rc::new(query);
        let mut tree = build_tree(spec.clone());
        tree.subscribers.push(sub);
        self.trees.insert(tree_id, tree);
        self.by_spec.insert(spec, tree_id);
        self.by_sub.insert(sub, tree_id);
        self.register_part(tree_id, QueryPart::main(), &mut out);
        self.mark_dirty(tree_id);
        self.settle_pages(&mut out);
        (sub, out)
    }

    /// Register the subtree at `part` in post-order. Children that drive
    /// the node and are not yet live are registered first and the node
    /// waits for them: the last of them to go live comes back here through
    /// [`Self::landed`]. With every such child live the node registers
    /// itself with its full set restrictions; if its rows are all at hand
    /// (a twin's) it is live at once, otherwise it goes live when its read
    /// lands. The children it drives follow from [`Self::landed`].
    fn register_part(&mut self, tree_id: TreeId, part: QueryPart, out: &mut Vec<MultiTableUpdate>) {
        let (waiting, own, fanned, columns) = {
            let tree = &self.trees[&tree_id];
            let node = &tree.nodes[&part];
            if node.is_registered() {
                return;
            }
            let waiting: Vec<QueryPart> = node
                .children
                .iter()
                .map(|&edge| &tree.edges[edge])
                .filter(|edge| edge.driver == Driver::Sub && !settled(tree, &edge.child))
                .map(|edge| edge.child)
                .collect();
            (
                waiting,
                node.query.clone(),
                node.fanned,
                join_columns(tree, &part),
            )
        };
        if !waiting.is_empty() {
            for child in waiting {
                self.register_part(tree_id, child, out);
            }
            return;
        }
        for column in &columns {
            self.single.index_column(&own.table, column);
        }
        if fanned {
            let values = {
                let tree = &self.trees[&tree_id];
                let edge = tree.nodes[&part]
                    .parent
                    .expect("a fanned node has a parent");
                tree.leaves[&tree.edges[edge].leaf].members()
            };
            if let Some(node) = self
                .trees
                .get_mut(&tree_id)
                .and_then(|tree| tree.nodes.get_mut(&part))
            {
                node.parts = Parts::Fan(HashMap::new());
            }
            for value in values {
                self.register_value(tree_id, &part, value, out);
            }
        } else {
            let query = SingleTableReadQuery {
                filter: self.restricted_filter(tree_id, &part, None),
                ..own.clone()
            };
            let (inner, ops) = self.single.register_query(query);
            if let Some(node) = self
                .trees
                .get_mut(&tree_id)
                .and_then(|tree| tree.nodes.get_mut(&part))
            {
                node.parts = Parts::One(inner);
            }
            self.parts.insert(inner, (tree_id, part));
            for op in ops {
                self.emit(tree_id, &part, &own.table, op.clone(), out);
                if let DataFrameOperation::Add(key, row) = &op {
                    self.arrived(tree_id, &part, key, row, out);
                }
            }
        }
        if !self.part_pending(tree_id, &part) {
            self.landed(tree_id, &part, out);
        }
    }

    /// Register the per-value part of a fanned node for `value`: the
    /// node's own filter narrowed to `column = value`, with the node's
    /// window, read like any registration; rows a twin holds arrive at
    /// once.
    fn register_value(
        &mut self,
        tree_id: TreeId,
        part: &QueryPart,
        value: Value,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let (query, table) = {
            let tree = &self.trees[&tree_id];
            let node = &tree.nodes[part];
            let edge = node.parent.expect("a fanned node has a parent");
            let column = tree.edges[edge].child_column.clone();
            let narrowed = Where::AND(vec![
                self.restricted_filter(tree_id, part, Some(edge)),
                Where::condition(column, ComparisonOperator::EQ, value.clone()),
            ]);
            (
                SingleTableReadQuery {
                    filter: narrowed,
                    ..node.query.clone()
                },
                node.query.table.clone(),
            )
        };
        let (inner, ops) = self.single.register_query(query);
        if let Some(Parts::Fan(fan)) = self
            .trees
            .get_mut(&tree_id)
            .and_then(|tree| tree.nodes.get_mut(part))
            .map(|node| &mut node.parts)
        {
            fan.insert(value, inner);
        }
        self.parts.insert(inner, (tree_id, *part));
        for op in ops {
            self.emit(tree_id, part, &table, op.clone(), out);
            if let DataFrameOperation::Add(key, row) = &op {
                self.arrived(tree_id, part, key, row, out);
            }
        }
    }

    /// Unregister the per-value part of a fanned node for `value`,
    /// letting its held rows depart first (their `Delete`s went out when
    /// the value's last shown parent row left). If the part's read was
    /// the last one the node was waiting for, the node goes live here:
    /// the read will land into nothing, and the registration walk below
    /// the node would otherwise never continue.
    fn unregister_value(
        &mut self,
        tree_id: TreeId,
        part: &QueryPart,
        value: &Value,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let Some(inner) = self
            .trees
            .get_mut(&tree_id)
            .and_then(|tree| tree.nodes.get_mut(part))
            .and_then(|node| match &mut node.parts {
                Parts::Fan(fan) => fan.remove(value),
                _ => None,
            })
        else {
            return;
        };
        let rows = self.single.rows_for(inner).unwrap_or_default();
        self.single.unregister_query(inner);
        self.parts.remove(&inner);
        for (key, row) in rows {
            self.departed(tree_id, part, &key, &row, out);
        }
        let waiting = self
            .trees
            .get(&tree_id)
            .and_then(|tree| tree.nodes.get(part))
            .is_some_and(|node| !node.live);
        if waiting && !self.part_pending(tree_id, part) {
            self.landed(tree_id, part, out);
        }
    }

    /// Whether any inner part of `part` still has a read out.
    fn part_pending(&self, tree_id: TreeId, part: &QueryPart) -> bool {
        self.trees
            .get(&tree_id)
            .and_then(|tree| tree.nodes.get(part))
            .is_some_and(|node| {
                node.inner_parts()
                    .into_iter()
                    .any(|inner| self.single.is_pending(inner))
            })
    }

    /// `part`'s rows have all arrived: mark it live, register the
    /// children it drives (their sets are now filled by its rows), and, if
    /// it drives its parent and the parent is still waiting, let the parent
    /// try to register.
    fn landed(&mut self, tree_id: TreeId, part: &QueryPart, out: &mut Vec<MultiTableUpdate>) {
        let (driven_children, waiting_parent) = {
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
            let driven_children: Vec<QueryPart> = node
                .children
                .iter()
                .map(|&edge| &tree.edges[edge])
                .filter(|edge| edge.driver == Driver::Main)
                .map(|edge| edge.child)
                .collect();
            let waiting_parent = waiting_above(tree, part);
            (driven_children, waiting_parent)
        };
        for child in driven_children {
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
        let worst = super::frames::worst_of_full(fetch, rows);
        self.land_read(fetch, rows, worst.as_ref())
    }

    /// [`MultiTableIVM::land_fetch`] with the read's own coverage (see
    /// [`SingleTableIVM::land_read`]).
    pub fn land_read(
        &mut self,
        fetch: &Fetch,
        rows: &[(DataFrameKey, DataFrameRow)],
        worst_read: Option<&DataFrameRow>,
    ) -> Vec<MultiTableUpdate> {
        let readers = self.single.readers_of(fetch);
        let applied = self.single.land_read(fetch, rows, worst_read);
        let mut out = self.forward(applied);
        for sub in &readers {
            if let Some((tree_id, _)) = self.parts.get(sub).cloned() {
                self.mark_dirty(tree_id);
            }
        }
        for sub in readers {
            let Some((tree_id, part)) = self.parts.get(&sub).cloned() else {
                continue;
            };
            let live = self
                .trees
                .get(&tree_id)
                .and_then(|tree| tree.nodes.get(&part))
                .is_some_and(|node| node.live);
            if !live && !self.part_pending(tree_id, &part) {
                self.landed(tree_id, &part, &mut out);
            }
        }
        self.settle_pages(&mut out);
        out
    }

    /// The trees with a part reading `fetch`, each once.
    fn trees_reading(&self, fetch: &Fetch) -> Vec<TreeId> {
        let mut trees: Vec<TreeId> = Vec::new();
        for inner in self.single.readers_of(fetch) {
            if let Some((tree_id, _)) = self.parts.get(&inner)
                && !trees.contains(tree_id)
            {
                trees.push(*tree_id);
            }
        }
        trees
    }

    /// Take the storage reads the inner parts asked for since the last
    /// call.
    pub fn take_requests(&mut self) -> Vec<Fetch> {
        self.single.take_requests()
    }

    /// A part's registered filter: its own `WHERE` with every gate leaf
    /// taken as true (its truth is a matter of visibility, decided per
    /// row), every bound leaf replaced in place by its edge's set, one
    /// set-valued `IN` leaf conjoined per set the unnamed edges driving it
    /// read, and any `EXISTS` leaf left standing made false; `skip` leaves
    /// one edge out (a fanned node's own, whose restriction is the
    /// per-value equality instead).
    fn restricted_filter(&self, tree_id: TreeId, part: &QueryPart, skip: Option<usize>) -> Where {
        let tree = &self.trees[&tree_id];
        let node = &tree.nodes[part];
        let mut filter = node.query.filter.clone();
        let gate_leaves: Vec<&Condition> = node
            .gates
            .iter()
            .filter_map(|edge| tree.edges[*edge].gate.as_ref())
            .collect();
        if !gate_leaves.is_empty() {
            filter = filter.assuming_true(&|leaf| gate_leaves.contains(&leaf));
        }
        let mut conjoined: Vec<LeafKey> = Vec::new();
        let mut parts = Vec::new();
        for edge in tree
            .edges
            .iter()
            .enumerate()
            .filter(|(index, edge)| edge.driven() == part && Some(*index) != skip)
            .map(|(_, edge)| edge)
        {
            let bound = leaf_condition(edge.driven_column(), &tree.leaves[&edge.leaf]);
            match &edge.bound {
                Some(unbound) => filter.replace_condition(unbound, &bound),
                None if conjoined.contains(&edge.leaf) => {}
                None => {
                    conjoined.push(edge.leaf.clone());
                    parts.push(Where::Condition(bound));
                }
            }
        }
        parts.insert(0, unbound_exists_false(filter));
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
        self.by_spec.remove(&tree.spec);
        for node in tree.nodes.values() {
            for inner in node.inner_parts() {
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
        let mut out = self.forward(applied);
        self.settle_pages(&mut out);
        out
    }

    /// Forward the inner engine's operations to the subscribers of their
    /// trees, every driven part before its driver, diffing replace pairs
    /// and cascading arrivals and departures. The inner engine has applied
    /// the whole step before the first of its operations is handled here,
    /// so until a row's `Add` is reached the row is *in flight*: held
    /// below, not yet arrived here, and left out of what the counts'
    /// crossings re-evaluate ([`Self::rows_of_value`]). A row moving
    /// between two per-value parts of one node (its `Delete` from one, its
    /// `Add` into the other) is one replacement, like a row rewritten in
    /// place.
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
        pair_moves(&mut tagged);
        let arriving: Vec<(SubId, DataFrameKey)> = tagged
            .iter()
            .filter(|(_, _, _, update)| matches!(update.op, DataFrameOperation::Add(_, _)))
            .map(|(_, _, _, update)| (update.query, update.op.key().clone()))
            .collect();
        self.in_flight.extend(arriving);

        let mut out = Vec::new();
        let mut updates = tagged.into_iter().peekable();
        while let Some((tree_id, _, part, update)) = updates.next() {
            let paired_add = match (&update.op, updates.peek()) {
                (DataFrameOperation::Delete(key, _), Some((next_tree, _, next_part, next)))
                    if *next_tree == tree_id
                        && *next_part == part
                        && matches!(&next.op, DataFrameOperation::Add(next_key, _) if next_key == key) =>
                {
                    let (_, _, _, next) = updates.next().expect("peeked just above");
                    self.landed_in_flight(next.query, next.op.key());
                    Some(next.op)
                }
                _ => None,
            };
            if let DataFrameOperation::Add(key, _) = &update.op {
                self.landed_in_flight(update.query, key);
            }
            let table = update.table.clone();
            match (update.op, paired_add) {
                (DataFrameOperation::Delete(key, old), Some(add)) => {
                    let new = add.row().clone();
                    self.emit(
                        tree_id,
                        &part,
                        &table,
                        DataFrameOperation::Delete(key.clone(), old.clone()),
                        &mut out,
                    );
                    self.emit(tree_id, &part, &table, add, &mut out);
                    self.replaced(tree_id, &part, &key, &old, &new, &mut out);
                }
                (op @ DataFrameOperation::Add(_, _), _) => {
                    let (key, row) = (op.key().clone(), op.row().clone());
                    self.emit(tree_id, &part, &table, op, &mut out);
                    self.arrived(tree_id, &part, &key, &row, &mut out);
                }
                (op @ DataFrameOperation::Delete(_, _), None) => {
                    let (key, row) = (op.key().clone(), op.row().clone());
                    self.emit(tree_id, &part, &table, op, &mut out);
                    self.departed(tree_id, &part, &key, &row, &mut out);
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
        if !node.is_registered() {
            return None;
        }
        let mut shown = HashMap::new();
        for inner in node.inner_parts() {
            for (key, row) in self.single.rows_for(inner).unwrap_or_default() {
                if shown_in(tree, &part, &row) {
                    shown.insert(key, row);
                }
            }
        }
        Some(shown)
    }

    /// The inner engine's routing counters.
    pub fn stats(&self) -> &IvmStats {
        self.single.stats()
    }

    /// A subscription's tree in words, one line per part: what is behind
    /// it (its inner parts, the rows each shows, whether a read is out)
    /// and the counts on the edge above it. A test and debugging seam.
    pub fn describe(&self, sub: SubId) -> Vec<String> {
        let Some(tree) = self.by_sub.get(&sub).and_then(|id| self.trees.get(id)) else {
            return Vec::new();
        };
        tree.post_order
            .iter()
            .map(|part| {
                let node = &tree.nodes[part];
                let parts: Vec<String> = match &node.parts {
                    Parts::Unregistered => vec!["unregistered".to_owned()],
                    Parts::One(inner) => vec![self.describe_inner(*inner, None)],
                    Parts::Fan(fan) => fan
                        .iter()
                        .map(|(value, inner)| self.describe_inner(*inner, Some(value)))
                        .collect(),
                };
                let above = node.parent.map(|edge| {
                    let counts = &tree.edges[edge].counts;
                    format!(
                        " left {:?} matched {:?} shown {:?}",
                        counts.left, counts.matched, counts.shown
                    )
                });
                format!(
                    "{part:?} {} live={} [{}]{}",
                    node.query.table,
                    node.live,
                    parts.join("; "),
                    above.unwrap_or_default()
                )
            })
            .collect()
    }

    /// One inner part in words, for [`Self::describe`].
    fn describe_inner(&self, inner: SubId, value: Option<&Value>) -> String {
        let rows = self.single.rows_for(inner).map_or(0, |rows| rows.len());
        let page = self
            .single
            .page_state(inner)
            .map(|(rejected, state)| format!(", {} rejected, {state}", rejected.len()))
            .unwrap_or_default();
        format!(
            "{inner:?}{} shows {rows}, read out {}{page}",
            value
                .map(|value| format!(" for {value:?}"))
                .unwrap_or_default(),
            self.single.is_pending(inner)
        )
    }

    /// Check every tree's per-edge counts against the rows its parts
    /// hold, returning one line per disagreement (none when the state is
    /// consistent): `left` is the driver's rows per join value (for a
    /// driving child, the rows that have risen), `matched` the risen child
    /// rows of an inner edge the main drives, `shown` the shown parent
    /// rows, `risen` exactly the held rows of a gated node whose gate is
    /// open, and, once none of a tree's reads is out, each page's rejected
    /// rows exactly the rows it shows that its gate closes on. A test
    /// seam; nothing in the engine reads it.
    pub fn audit(&self) -> Vec<String> {
        let mut problems = Vec::new();
        for (tree_id, tree) in self.trees.iter() {
            let held = |part: &QueryPart| -> Vec<(DataFrameKey, DataFrameRow)> {
                tree.nodes[part]
                    .inner_parts()
                    .into_iter()
                    .flat_map(|inner| self.single.rows_for(inner).unwrap_or_default())
                    .collect()
            };
            for (part, node) in tree.nodes.iter() {
                if node.gates.is_empty() || upward_edge(tree, part).is_none() {
                    continue;
                }
                let open: HashSet<DataFrameKey> = held(part)
                    .into_iter()
                    .filter(|(_, row)| gate_open(tree, node, row))
                    .map(|(key, _)| key)
                    .collect();
                if open != node.risen {
                    problems.push(format!(
                        "tree {tree_id:?} part {part:?}: risen {:?}, open {:?}",
                        node.risen, open
                    ));
                }
            }
            let reading = tree.nodes.values().any(|node| {
                !node.is_registered()
                    || node
                        .inner_parts()
                        .into_iter()
                        .any(|inner| self.single.is_pending(inner))
            });
            for part in tree.pages.iter().filter(|_| !reading) {
                let node = &tree.nodes[part];
                for inner in node.inner_parts() {
                    let closed: HashSet<DataFrameKey> = self
                        .single
                        .rows_for(inner)
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|(_, row)| !gate_open(tree, node, row))
                        .map(|(key, _)| key)
                        .collect();
                    if let Some((rejected, state)) = self.single.page_state(inner)
                        && rejected != closed
                    {
                        problems.push(format!(
                            "tree {tree_id:?} part {part:?} {inner:?}: rejected {rejected:?}, the gate closes on {closed:?} ({state})"
                        ));
                    }
                }
            }
            for (index, edge) in tree.edges.iter().enumerate() {
                let tally = |rows: Vec<(DataFrameKey, DataFrameRow)>, column: &ColumnName| {
                    let mut counts: HashMap<Value, u64> = HashMap::new();
                    for (_, row) in rows {
                        *counts.entry(join_value(&row, column)).or_insert(0) += 1;
                    }
                    counts
                };
                let risen_rows = |part: &QueryPart| -> Vec<(DataFrameKey, DataFrameRow)> {
                    let node = &tree.nodes[part];
                    held(part)
                        .into_iter()
                        .filter(|(key, _)| node.gates.is_empty() || node.risen.contains(key))
                        .collect()
                };
                let left = match edge.driver {
                    Driver::Main => tally(held(&edge.parent), &edge.parent_column),
                    Driver::Sub => tally(risen_rows(&edge.child), &edge.child_column),
                };
                if left != edge.counts.left {
                    problems.push(format!(
                        "tree {tree_id:?} edge {index}: left {:?}, rows give {:?}",
                        edge.counts.left, left
                    ));
                }
                if edge.gates_parent() {
                    let matched = tally(risen_rows(&edge.child), &edge.child_column);
                    if matched != edge.counts.matched {
                        problems.push(format!(
                            "tree {tree_id:?} edge {index}: matched {:?}, rows give {:?}",
                            edge.counts.matched, matched
                        ));
                    }
                }
                let shown_rows = held(&edge.parent)
                    .into_iter()
                    .filter(|(_, row)| shown_in(tree, &edge.parent, row))
                    .collect();
                let shown = tally(shown_rows, &edge.parent_column);
                if shown != edge.counts.shown {
                    problems.push(format!(
                        "tree {tree_id:?} edge {index}: shown {:?}, rows give {:?}",
                        edge.counts.shown, shown
                    ));
                }
            }
        }
        problems
    }

    /// Forward one part operation to every subscriber of its tree, unless
    /// the row is not shown (a child with no shown parent row, a gated row
    /// with no match).
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
        self.emit_raw(tree_id, part, table, op, out);
    }

    /// Forward one part operation to every subscriber of its tree, the
    /// caller having decided it is shown (or was, for a retraction).
    fn emit_raw(
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
        for subscriber in &tree.subscribers {
            out.push(MultiTableUpdate {
                query: *subscriber,
                table: table.clone(),
                part: *part,
                op: op.clone(),
            });
        }
    }

    /// A row now held by `part`: reference its join value on every edge
    /// the part drives below it (a new reference asks for a read; nothing
    /// is emitted for it here), let it act on the edge above it if its own
    /// gate is open ([`Self::rise`]) and, if the row is shown, count it on
    /// the edges below it, admitting the children it uncovers.
    fn arrived(
        &mut self,
        tree_id: TreeId,
        part: &QueryPart,
        key: &DataFrameKey,
        row: &DataFrameRow,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let (shown, open) = {
            let tree = &self.trees[&tree_id];
            (
                shown_in(tree, part, row),
                gate_open(tree, &tree.nodes[part], row),
            )
        };
        self.touch_page(tree_id, part);
        if open {
            self.rise(tree_id, part, key, row, out);
        }
        for (edge, column) in downward_edges(&self.trees[&tree_id], part) {
            self.reference(tree_id, edge, join_value(row, &column), out);
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
        key: &DataFrameKey,
        row: &DataFrameRow,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        self.touch_page(tree_id, part);
        if shown_in(&self.trees[&tree_id], part, row) {
            self.vanish(tree_id, part, row, out);
        }
        self.sink(tree_id, part, key, row, out);
        for (edge, column) in downward_edges(&self.trees[&tree_id], part) {
            self.release(tree_id, edge, &join_value(row, &column), out);
        }
    }

    /// A row of `part` replaced in place: move references only on edges
    /// whose join value actually changed — the new value referenced first,
    /// the old released after — so a kept value never crosses zero; what
    /// the row counts for on the edge above it moves the same way, and
    /// comes or goes with its gate; the shown counts below it follow.
    fn replaced(
        &mut self,
        tree_id: TreeId,
        part: &QueryPart,
        key: &DataFrameKey,
        old: &DataFrameRow,
        new: &DataFrameRow,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let (was, is) = {
            let tree = &self.trees[&tree_id];
            (shown_in(tree, part, old), shown_in(tree, part, new))
        };
        self.touch_page(tree_id, part);
        for (edge, column) in downward_edges(&self.trees[&tree_id], part) {
            let old_value = join_value(old, &column);
            let new_value = join_value(new, &column);
            if old_value != new_value {
                self.reference(tree_id, edge, new_value, out);
                self.release(tree_id, edge, &old_value, out);
            }
        }
        if let Some((_, column, _)) = upward_edge(&self.trees[&tree_id], part) {
            let had = self.has_risen(tree_id, part, key);
            let open = {
                let tree = &self.trees[&tree_id];
                gate_open(tree, &tree.nodes[part], new)
            };
            let moved = join_value(old, &column) != join_value(new, &column);
            match (had, open) {
                (true, true) if moved => {
                    self.rise_value(tree_id, part, new, out);
                    self.sink_value(tree_id, part, old, out);
                }
                (true, false) => self.sink(tree_id, part, key, old, out),
                (false, true) => self.rise(tree_id, part, key, new, out),
                _ => {}
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

    /// The `Add` of `key` into `inner` is being handled: the row is no
    /// longer in flight.
    fn landed_in_flight(&mut self, inner: SubId, key: &DataFrameKey) {
        if let Some(position) = self
            .in_flight
            .iter()
            .position(|(sub, other)| *sub == inner && other == key)
        {
            self.in_flight.swap_remove(position);
        }
    }

    /// Note that rows of `part` moved, so the tree's pages are settled
    /// before the step returns; nothing for a tree without one.
    fn touch_page(&mut self, tree_id: TreeId, _part: &QueryPart) {
        self.mark_dirty(tree_id);
    }

    /// Queue `tree_id` for [`Self::settle_pages`] if it has a page under a
    /// gate.
    fn mark_dirty(&mut self, tree_id: TreeId) {
        let paged = self
            .trees
            .get(&tree_id)
            .is_some_and(|tree| !tree.pages.is_empty());
        if paged && !self.dirty.contains(&tree_id) {
            self.dirty.push(tree_id);
        }
    }

    /// Settle the pages of every tree queued in this step: see
    /// [`Self::settle_tree`]. Forwarding what a settled page shows or
    /// withdraws may queue the tree again, so each tree is settled in
    /// rounds, up to [`SETTLE_ROUNDS`] of them.
    fn settle_pages(&mut self, out: &mut Vec<MultiTableUpdate>) {
        while let Some(tree_id) = self.dirty.pop() {
            for _ in 0..SETTLE_ROUNDS {
                if !self.settle_tree(tree_id, out) {
                    break;
                }
            }
            self.dirty.retain(|queued| *queued != tree_id);
        }
    }

    /// One round for the pages of one tree, once none of its reads is out
    /// (until then a closed gate may only be a sub row not yet fetched):
    /// each page's window is told which of the rows it shows the gate
    /// rejects, and what the window shows or withdraws in answer (the
    /// page reaching further, or drawing back when a gate opened) is
    /// forwarded like any other operation, the rows newly shown asking
    /// for their sub rows in turn. Reports whether anything moved, in
    /// which case another round follows once those reads have landed.
    fn settle_tree(&mut self, tree_id: TreeId, out: &mut Vec<MultiTableUpdate>) -> bool {
        let Some(tree) = self.trees.get(&tree_id) else {
            return false;
        };
        let reading = tree.nodes.values().any(|node| {
            !node.is_registered()
                || node
                    .inner_parts()
                    .into_iter()
                    .any(|inner| self.single.is_pending(inner))
        });
        if reading {
            return false;
        }
        let mut verdicts: Vec<(SubId, HashSet<DataFrameKey>)> = Vec::new();
        for part in &tree.pages {
            let node = &tree.nodes[part];
            for inner in node.inner_parts() {
                let rejected = self
                    .single
                    .rows_for(inner)
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|(_, row)| !gate_open(tree, node, row))
                    .map(|(key, _)| key)
                    .collect();
                verdicts.push((inner, rejected));
            }
        }
        let mut moved = false;
        for (inner, rejected) in verdicts {
            let ops = self.single.reject_rows(inner, rejected);
            if ops.is_empty() {
                continue;
            }
            moved = true;
            let forwarded = self.forward(ops);
            out.extend(forwarded);
        }
        moved
    }

    /// Whether `key` of `part` is counted on the edge above it: every held
    /// row of a node without gates, the recorded ones of a gated node.
    fn has_risen(&self, tree_id: TreeId, part: &QueryPart, key: &DataFrameKey) -> bool {
        let node = &self.trees[&tree_id].nodes[part];
        node.gates.is_empty() || node.risen.contains(key)
    }

    /// A held row of `part` whose gate is open starts acting on the edge
    /// above it: a match for the parent rows it gates, or a reference when
    /// the part drives that edge. A gated node records the key, so the
    /// row leaves exactly what it entered.
    fn rise(
        &mut self,
        tree_id: TreeId,
        part: &QueryPart,
        key: &DataFrameKey,
        row: &DataFrameRow,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        if upward_edge(&self.trees[&tree_id], part).is_none() {
            return;
        }
        let recorded = self
            .trees
            .get_mut(&tree_id)
            .and_then(|tree| tree.nodes.get_mut(part))
            .is_some_and(|node| node.gates.is_empty() || node.risen.insert(key.clone()));
        if recorded {
            self.rise_value(tree_id, part, row, out);
        }
    }

    /// The mirror of [`Self::rise`]: a row that was counted above stops
    /// being.
    fn sink(
        &mut self,
        tree_id: TreeId,
        part: &QueryPart,
        key: &DataFrameKey,
        row: &DataFrameRow,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        if upward_edge(&self.trees[&tree_id], part).is_none() {
            return;
        }
        let recorded = self
            .trees
            .get_mut(&tree_id)
            .and_then(|tree| tree.nodes.get_mut(part))
            .is_some_and(|node| node.gates.is_empty() || node.risen.remove(key));
        if recorded {
            self.sink_value(tree_id, part, row, out);
        }
    }

    /// Count `row`'s join value on the edge above `part`.
    fn rise_value(
        &mut self,
        tree_id: TreeId,
        part: &QueryPart,
        row: &DataFrameRow,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let Some((edge, column, drives)) = upward_edge(&self.trees[&tree_id], part) else {
            return;
        };
        let value = join_value(row, &column);
        if drives {
            self.reference(tree_id, edge, value, out);
        } else {
            self.matched_in(tree_id, edge, value, out);
        }
    }

    /// Stop counting `row`'s join value on the edge above `part`.
    fn sink_value(
        &mut self,
        tree_id: TreeId,
        part: &QueryPart,
        row: &DataFrameRow,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let Some((edge, column, drives)) = upward_edge(&self.trees[&tree_id], part) else {
            return;
        };
        let value = join_value(row, &column);
        if drives {
            self.release(tree_id, edge, &value, out);
        } else {
            self.matched_out(tree_id, edge, &value, out);
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
            (edge.child, edge.child_column.clone())
        };
        let table = self.trees[&tree_id].nodes[&child].query.table.clone();
        for (key, row) in self.rows_of_value(tree_id, &child, &column, &value) {
            if !gate_open(
                &self.trees[&tree_id],
                &self.trees[&tree_id].nodes[&child],
                &row,
            ) {
                continue;
            }
            self.emit_raw(
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
                (edge.child, edge.child_column.clone())
            };
            let table = self.trees[&tree_id].nodes[&child].query.table.clone();
            for (key, row) in self.rows_of_value(tree_id, &child, &column, value) {
                if !gate_open(
                    &self.trees[&tree_id],
                    &self.trees[&tree_id].nodes[&child],
                    &row,
                ) {
                    continue;
                }
                self.emit_raw(
                    tree_id,
                    &child,
                    &table,
                    DataFrameOperation::Delete(key, row.clone()),
                    out,
                );
                self.vanish(tree_id, &child, &row, out);
            }
        }
        Self::drop_in(
            self.counts_mut(tree_id, edge)
                .map(|counts| &mut counts.shown),
            value,
        );
    }

    /// One more held child row carries `value` on `edge`, an inner edge
    /// the main drives: bump `matched`, and on the 0 → 1 crossing
    /// re-evaluate the parent rows carrying the value, admitting the ones
    /// the gate now lets through.
    fn matched_in(
        &mut self,
        tree_id: TreeId,
        edge: usize,
        value: Value,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let crossing = self
            .trees
            .get(&tree_id)
            .and_then(|tree| tree.edges.get(edge))
            .is_none_or(|e| !e.counts.matched.contains_key(&value));
        let bump = |this: &mut Self| {
            if let Some(counts) = this.counts_mut(tree_id, edge) {
                *counts.matched.entry(value.clone()).or_insert(0) += 1;
            }
        };
        if crossing {
            let (parent, column) = {
                let e = &self.trees[&tree_id].edges[edge];
                (e.parent, e.parent_column.clone())
            };
            self.regate(tree_id, &parent, &column, &value, out, bump);
        } else {
            bump(self);
        }
    }

    /// One held child row fewer carries `value` on `edge`: the mirror of
    /// [`Self::matched_in`], retracting the parent rows the gate now
    /// closes on.
    fn matched_out(
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
            .is_some_and(|e| e.counts.matched.get(value).copied() == Some(1));
        let drop = |this: &mut Self| {
            Self::drop_in(
                this.counts_mut(tree_id, edge)
                    .map(|counts| &mut counts.matched),
                value,
            );
        };
        if crossing {
            let (parent, column) = {
                let e = &self.trees[&tree_id].edges[edge];
                (e.parent, e.parent_column.clone())
            };
            self.regate(tree_id, &parent, &column, value, out, drop);
        } else {
            drop(self);
        }
    }

    /// Apply `change` (a count moving across zero) and re-evaluate the
    /// rows of `part` whose `column` is `value` around it, in two passes.
    /// First what the client sees: a row the gate opens on under a shown
    /// parent is admitted (its `Add`, then its children), a row it closes
    /// on is retracted (its children, then its `Delete`). Then what the
    /// rows count for above: a row whose gate opened rises, one whose gate
    /// closed sinks, which may open or close the gates further up (and a
    /// parent row revealed that way admits this part's rows itself, which
    /// is why the first pass comes first and reads the counts as they are
    /// before anything above has moved).
    fn regate(
        &mut self,
        tree_id: TreeId,
        part: &QueryPart,
        column: &ColumnName,
        value: &Value,
        out: &mut Vec<MultiTableUpdate>,
        change: impl FnOnce(&mut Self),
    ) {
        let rows = self.rows_of_value(tree_id, part, column, value);
        let was: Vec<bool> = rows
            .iter()
            .map(|(_, row)| shown_in(&self.trees[&tree_id], part, row))
            .collect();
        change(self);
        if rows.is_empty() {
            return;
        }
        self.touch_page(tree_id, part);
        let table = self.trees[&tree_id].nodes[part].query.table.clone();
        for ((key, row), was) in rows.iter().zip(was) {
            let is = shown_in(&self.trees[&tree_id], part, row);
            if !was && is {
                self.emit_raw(
                    tree_id,
                    part,
                    &table,
                    DataFrameOperation::Add(key.clone(), row.clone()),
                    out,
                );
                self.appear(tree_id, part, row, out);
            } else if was && !is {
                self.emit_raw(
                    tree_id,
                    part,
                    &table,
                    DataFrameOperation::Delete(key.clone(), row.clone()),
                    out,
                );
                self.vanish(tree_id, part, row, out);
            }
        }
        if upward_edge(&self.trees[&tree_id], part).is_none() {
            return;
        }
        for (key, row) in &rows {
            let open = {
                let tree = &self.trees[&tree_id];
                gate_open(tree, &tree.nodes[part], row)
            };
            if open {
                self.rise(tree_id, part, key, row, out);
            } else {
                self.sink(tree_id, part, key, row, out);
            }
        }
    }

    /// A driver row now carries `value` on `edge`: bump `left`, and on the
    /// 0 → 1 crossing, if every edge reading the same set now references
    /// the value, add it to the set (one index filing) and ask for the
    /// value's driven rows (one narrowed storage read); when it lands
    /// ([`Self::land_fetch`]) they are forwarded and arrive at the driven
    /// node. Before the driven part is registered the set is filled
    /// directly; registration files it whole. A fanned node gains a part
    /// for the value when the edge is the one that fans it, and otherwise
    /// has the value filed once (its parts share the set) and fetched for
    /// each of its parts. When the driven part is a gated parent (the
    /// edge's test sits beside a main-driven one in its `WHERE`), the
    /// crossing also re-evaluates the parent rows already held for the
    /// value.
    fn reference(
        &mut self,
        tree_id: TreeId,
        edge: usize,
        value: Value,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let crossing = self.left_count(tree_id, edge, &value) == 0;
        let bump = |this: &mut Self| this.left_bump(tree_id, edge, value.clone());
        match self.gated_driven_parent(tree_id, edge).filter(|_| crossing) {
            Some((parent, column)) => self.regate(tree_id, &parent, &column, &value, out, bump),
            None => bump(self),
        }
        if !crossing {
            return;
        }
        if !referenced_by_all(&self.trees[&tree_id], edge, &value) {
            return;
        }
        let (driven, column, key) = self.leaf_of(tree_id, edge);
        let set = self.trees[&tree_id].leaves[&key].clone();
        let node = &self.trees[&tree_id].nodes[&driven];
        if !node.is_registered() {
            set.insert(&value);
            return;
        }
        if node.fanned && node.parent == Some(edge) {
            if set.insert(&value) {
                self.register_value(tree_id, &driven, value, out);
            }
            return;
        }
        let inners = node.inner_parts();
        let Some(first) = inners.first().copied() else {
            set.insert(&value);
            return;
        };
        if !self
            .single
            .set_insert(first, &leaf_condition(&column, &set), &value)
        {
            return;
        }
        for inner in inners {
            self.single
                .fetch(inner, column.as_str(), std::slice::from_ref(&value));
        }
    }

    /// A driver row no longer carries `value` on `edge`: drop `left`, and
    /// on the crossing to 0, if the value was in the set, remove it (one
    /// index unfiling), prune the value's held driven rows the filter no
    /// longer admits (no storage round-trip) and forward what that
    /// produces like any other operation: the `Delete`s, whose rows depart
    /// from the driven node, and, when the driven part is a page, the
    /// `Add`s of the rows that move up into it, which arrive there (and
    /// ask for their own sub rows). A gated parent's held rows are
    /// re-evaluated as in [`Self::reference`].
    fn release(
        &mut self,
        tree_id: TreeId,
        edge: usize,
        value: &Value,
        out: &mut Vec<MultiTableUpdate>,
    ) {
        let crossing = self.left_count(tree_id, edge, value) == 1;
        let drop = |this: &mut Self| this.left_drop(tree_id, edge, value);
        match self.gated_driven_parent(tree_id, edge).filter(|_| crossing) {
            Some((parent, column)) => self.regate(tree_id, &parent, &column, value, out, drop),
            None => drop(self),
        }
        if !crossing {
            return;
        }
        let (driven, column, key) = self.leaf_of(tree_id, edge);
        let set = self.trees[&tree_id].leaves[&key].clone();
        if !set.contains(value) {
            return;
        }
        let node = &self.trees[&tree_id].nodes[&driven];
        if !node.is_registered() {
            set.remove(value);
            return;
        }
        if node.fanned && node.parent == Some(edge) {
            set.remove(value);
            self.unregister_value(tree_id, &driven, value, out);
            return;
        }
        let inners = node.inner_parts();
        let table = node.query.table.clone();
        let Some(first) = inners.first().copied() else {
            set.remove(value);
            return;
        };
        if !self
            .single
            .set_remove(first, &leaf_condition(&column, &set), value)
        {
            return;
        }
        for inner in inners {
            let pruned: Vec<SingleTableUpdate> = self
                .single
                .prune_rows(inner, column.as_str(), std::slice::from_ref(value))
                .into_iter()
                .map(|op| SingleTableUpdate {
                    query: inner,
                    table: table.clone(),
                    op,
                })
                .collect();
            let forwarded = self.forward(pruned);
            out.extend(forwarded);
        }
    }

    /// When `edge` is an inner edge the sub drives into a parent that
    /// gates its own rows, that parent and its join column: the rows to
    /// re-evaluate when the edge's count crosses zero.
    fn gated_driven_parent(&self, tree_id: TreeId, edge: usize) -> Option<(QueryPart, ColumnName)> {
        let tree = &self.trees[&tree_id];
        let e = &tree.edges[edge];
        (e.driver == Driver::Sub && e.is_inner && !tree.nodes[&e.parent].gates.is_empty())
            .then(|| (e.parent, e.parent_column.clone()))
    }

    /// The driven part of `edge`, the column its leaf is on, and the set
    /// the leaf reads.
    fn leaf_of(&self, tree_id: TreeId, edge: usize) -> (QueryPart, ColumnName, LeafKey) {
        let edge = &self.trees[&tree_id].edges[edge];
        (
            *edge.driven(),
            edge.driven_column().clone(),
            edge.leaf.clone(),
        )
    }

    /// The rows `part` holds whose `column` equals `value`: the matching
    /// rows of its one part; for a fanned node, every row of its per-value
    /// part when `column` is the fanning column and the matching rows of
    /// every part otherwise; nothing while registration has not reached
    /// it.
    fn rows_of_value(
        &self,
        tree_id: TreeId,
        part: &QueryPart,
        column: &ColumnName,
        value: &Value,
    ) -> Vec<(DataFrameKey, DataFrameRow)> {
        let Some(tree) = self.trees.get(&tree_id) else {
            return Vec::new();
        };
        let Some(node) = tree.nodes.get(part) else {
            return Vec::new();
        };
        let arrived = |inner: SubId| -> Vec<(DataFrameKey, DataFrameRow)> {
            let mut rows = self
                .single
                .visible_rows_matching(inner, column.as_str(), value);
            if !self.in_flight.is_empty() {
                rows.retain(|(key, _)| {
                    !self
                        .in_flight
                        .iter()
                        .any(|(sub, other)| *sub == inner && other == key)
                });
            }
            rows
        };
        match &node.parts {
            Parts::Unregistered => Vec::new(),
            Parts::One(inner) => arrived(*inner),
            Parts::Fan(fan) => {
                let fanning = node
                    .parent
                    .map(|edge| &tree.edges[edge].child_column)
                    .is_some_and(|fanning| fanning == column);
                if fanning {
                    fan.get(value)
                        .map(|inner| arrived(*inner))
                        .unwrap_or_default()
                } else {
                    fan.values().flat_map(|inner| arrived(*inner)).collect()
                }
            }
        }
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

    /// What the engine holds: its subscriptions, its trees and the rows
    /// in its frames per table.
    fn footprint(&self) -> Footprint {
        let mut footprint = self.single.footprint();
        footprint.trees = self.trees.len() as u64;
        footprint
    }

    /// [`SingleTableIVM::take_dead`] of the inner engine.
    fn take_dead(&mut self) -> Vec<SharedRow> {
        self.single.take_dead()
    }

    /// Every subscription of every tree with a part reading the fetch.
    fn waiting_on(&self, fetch: &Fetch) -> Vec<SubId> {
        let mut subs = Vec::new();
        for tree_id in self.trees_reading(fetch) {
            if let Some(tree) = self.trees.get(&tree_id) {
                subs.extend(tree.subscribers.iter().copied());
            }
        }
        subs
    }

    /// Every subscription of every tree with a part reading the refused
    /// fetch, unsubscribed.
    fn refuse(&mut self, fetch: &Fetch) -> Vec<(SubId, ClientId)> {
        let trees = self.trees_reading(fetch);
        let mut gone = Vec::new();
        for tree_id in trees {
            let subs = self
                .trees
                .get(&tree_id)
                .map(|tree| tree.subscribers.clone())
                .unwrap_or_default();
            for sub in subs {
                if let Some(client) = self.clients.get(&sub).copied() {
                    gone.push((sub, client));
                }
                self.unsubscribe(sub);
            }
        }
        gone
    }

    /// [`MultiTableIVM::incremental_update`], grouped per client.
    fn route(&mut self, write: &WriteQuery) -> Vec<ClientUpdate> {
        let updates = self.incremental_update(write);
        self.addressed(updates)
    }

    /// [`MultiTableIVM::land_read`], grouped per client.
    fn land(
        &mut self,
        fetch: &Fetch,
        rows: &[(DataFrameKey, DataFrameRow)],
        worst_read: Option<&DataFrameRow>,
    ) -> Vec<ClientUpdate> {
        let updates = self.land_read(fetch, rows, worst_read);
        self.addressed(updates)
    }

    /// [`MultiTableIVM::take_requests`].
    fn requests(&mut self) -> Vec<Fetch> {
        self.take_requests()
    }

    /// Every part of the subscription's tree is live, with no read out.
    fn hydrated(&self, sub: SubId) -> bool {
        let Some(tree) = self
            .by_sub
            .get(&sub)
            .and_then(|tree_id| self.trees.get(tree_id))
        else {
            return false;
        };
        tree.nodes.values().all(|node| {
            node.live
                && node.is_registered()
                && node
                    .inner_parts()
                    .into_iter()
                    .all(|inner| !self.single.is_pending(inner))
        })
    }

    /// The inner engine's routing counters.
    fn stats(&self) -> &IvmStats {
        self.single.stats()
    }
}
