//! Which side of every inner edge drives it, decided before a query
//! registers, and the cache that makes deciding the exception.
//!
//! A node nothing drives is read **whole**: every row it matches sits in
//! memory. A driven node holds only the rows matching its driver's
//! values, and a node with a page (`LIMIT`) holds its window. The planner
//! counts every node it may have to decide, all in one batch and no
//! further than the configured limit plus one, twice: **alone** — its own
//! filter with its `EXISTS` leaves taken as true, what it holds when it
//! drives — and **narrowed** — the join result at the node, its `EXISTS`
//! leaves answered by its subs all the way down, what it holds when its
//! subs drive it (a node without inner subs is the same either way and is
//! counted once). The tree is then sized from the leaves up and decided
//! from the root down by one rule: **a node's subs drive it when they cut
//! it down by more than they cost**. Every node is given the rows its
//! subtree holds at best, its *hold*: read whole, its alone count; driven
//! by its subs, its narrowed count plus the hold of every sub that drives
//! it — the smaller of the two is its plan and its hold. Only a bounded
//! sub (one with a hold) can drive. A sub whose `EXISTS` sits under an
//! `OR` must drive for the narrowing to mean anything (the filter is only
//! cut down once every branch is), so with one of those unbounded the
//! node is read whole; a sub the filter requires (an `AND` conjunct, or
//! an edge no leaf names) drives when it costs no more than what the node
//! keeps with the drivers before it, smallest first, or when nothing else
//! drives; and under a page every bounded sub drives, so the window is
//! restricted exactly and gates nothing. A tie goes to the preferred
//! side, except that a page never wins one. A node driven from above — a
//! LEFT child, the parent of a RIGHT child, the driven side of an inner
//! edge — is narrowed already and drives every inner sub below it without
//! a comparison: its rows are the few its driver's values reach, whatever
//! the table holds, so nothing below it is counted. A node with no
//! bounded plan refuses the query, naming the tables, unless it has a
//! page: the page drives, reading its window and keeping it to the rows
//! its subs admit (see the `window` module). The root never changes: the
//! decision is a `driver` per edge ([`Join::driver`]), so part paths,
//! hidden parts and `EXISTS` leaves all stay where the translation put
//! them, and an inner edge at any depth is planned the same way as one at
//! the root. Two `EXISTS` on one to-one relationship were made one node
//! before any of this (`ast::merge_exists`), so they are counted and
//! compared as one.
//!
//! So the canvases a reader may see — `createdBy = me OR EXISTS
//! participants(userId = me OR EXISTS group(EXISTS member = me) OR EXISTS
//! channel(EXISTS member = me))`, twenty thousand canvases, a page of
//! twenty — are read from the leaves: the reader's few memberships drive
//! their groups and channels, those and the reader's own participations
//! drive the few hundred participations, and those drive the page,
//! `canvases WHERE createdBy = me OR id IN (…) ORDER BY … LIMIT 20`, one
//! read; a page counted alone against a participants table counted whole
//! would have walked the table. A page of one canvas by id still drives
//! everything from the row outwards: alone it is one row, and no sub
//! costs less than that.
//!
//! **A main with a page** is counted like any other node, on its own
//! filter with its cursor and its `LIMIT` ignored, so a new cursor is a
//! new plan and a page turn costs its counts on the root. Driven by its
//! subs it holds its window at most, so its narrowed count is taken no
//! higher than its `LIMIT`, and a page whose counts are both over the
//! limit is still served: driven by its bounded subs, else driving them
//! all, reading its window in batches and keeping it to the rows they
//! admit.
//!
//! **How a page that drives is read** is the planner's last word
//! ([`MultiTableReadQuery::page`]): a page whose alone count is within
//! the whole-page limit ([`Policy::whole`]) is read **whole** — every row
//! of its filter in one read, the `LIMIT` applied in memory, the rows its
//! edge rejects kept, so the edge's restriction stays exact — and any
//! other page in **batches** that double from one round to the next, the
//! rows its edge rejects dropped and the driven side routing on its own
//! filter (see the `multi` module).
//!
//! The planner does no I/O: it hands out the counts it wants and takes
//! the answers back, so the caller runs them wherever it can and the
//! decision stays a pure function that tests drive with numbers. [`plan`]
//! is that caller for the server: the cache first (keyed by the tree as
//! translated, the same identity the engine's twin sharing uses), else
//! the counts on the storage, concurrently, then the decision, remembered
//! for a while so the next client of the same tree pays nothing.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use futures_util::future::join_all;

use super::ast::Translated;
use crate::log::{Level, log_event};
use crate::model::{
    ColumnName, ComparisonOperator, Condition, Driver, MultiTableReadQuery, PageRead,
    SingleTableReadQuery, Value,
};
use crate::sync::Storage;

/// Which side of an inner edge drives when reading a node whole and
/// having its subs drive it hold the same.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Child,
    Parent,
}

/// The planner's settings: the most rows a whole node may hold (the same
/// number a storage read may return), the side that breaks a tie, and
/// the most rows a page that drives an inner edge is read whole for
/// (past it the page is read in batches).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    pub limit: u64,
    pub preferred: Side,
    pub whole: u64,
}

/// One node of the tree being planned.
///
/// - `path`: the join positions from the root to the node.
/// - `query`: the node's own query.
/// - `driven`: whether an outer edge drives the node (a LEFT child, a
///   RIGHT parent): narrowed by construction, it drives everything below
///   it and is never counted or compared.
/// - `alone`: its count on its own filter, the `EXISTS` leaves taken as
///   true, once answered; `None` when not asked.
/// - `narrowed`: its count with its subs answering its `EXISTS` leaves,
///   once answered; `None` when not asked (a node without inner subs).
struct PlanNode {
    path: Vec<usize>,
    query: SingleTableReadQuery,
    driven: bool,
    alone: Option<u64>,
    narrowed: Option<u64>,
}

/// One edge of the tree being planned: its ends as node indices, the
/// position of its join under the parent, the parent's join column, its
/// driver as translated, whether it is inner and, when it is, its index
/// among the parent's inner edges (what an `EXISTS` leaf names it by).
struct PlanEdge {
    parent: usize,
    child: usize,
    position: usize,
    column: ColumnName,
    driver: Driver,
    is_inner: bool,
    inner_index: Option<usize>,
}

/// One count the planner wants: of the node `node`, `narrowed` by its
/// subs (the node's subtree, its `EXISTS` leaves answered by the subs)
/// or alone (the node by itself, its `EXISTS` leaves taken as true), no
/// further than `cap`.
pub struct Count {
    pub node: usize,
    pub narrowed: bool,
    pub query: MultiTableReadQuery,
    pub cap: u64,
}

/// A node's plan from its counts: the inner edges whose subs drive it
/// (none when it drives them all) and the rows its subtree holds at
/// best, `None` when no plan of it is bounded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Plan {
    drivers: Vec<usize>,
    hold: Option<u64>,
}

/// How a node is decided: compared with its inner subs, or narrowed from
/// above already and driving them all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Compare,
    Driven,
}

/// The planner for one translated query.
pub struct Planner {
    query: MultiTableReadQuery,
    policy: Policy,
    nodes: Vec<PlanNode>,
    edges: Vec<PlanEdge>,
    counted: Vec<usize>,
}

impl Planner {
    /// A planner for `query` under `policy`; [`Planner::counts`] says
    /// what it wants to know.
    pub fn new(query: MultiTableReadQuery, policy: Policy) -> Self {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        flatten(&query, Vec::new(), None, &mut nodes, &mut edges);
        let mut planner = Planner {
            query,
            policy,
            nodes,
            edges,
            counted: Vec::new(),
        };
        if planner.query.has_joins() {
            let mut counted = Vec::new();
            planner.count_below(0, Mode::Compare, &mut counted);
            planner.counted = counted;
        }
        planner
    }

    /// The edges under the node `index`, in join order.
    fn edges_of(&self, index: usize) -> impl Iterator<Item = (usize, &PlanEdge)> + '_ {
        self.edges
            .iter()
            .enumerate()
            .filter(move |(_, edge)| edge.parent == index)
    }

    /// The inner edges under the node `index`, in join order.
    fn inner_of(&self, index: usize) -> Vec<usize> {
        self.edges_of(index)
            .filter(|(_, edge)| edge.is_inner)
            .map(|(edge, _)| edge)
            .collect()
    }

    /// The mode the node `index` is decided in, given the mode its parent
    /// hands down: a node an outer edge drives is narrowed whatever the
    /// parent says.
    fn mode_of(&self, index: usize, handed: Mode) -> Mode {
        if self.nodes[index].driven {
            Mode::Driven
        } else {
            handed
        }
    }

    /// The mode the child of `edge` is decided in when `drivers` are the
    /// edges whose subs drive the parent: a driving sub, and a RIGHT
    /// child (read whole, driving its parent), are compared with their
    /// own subs; every other child is driven.
    fn mode_below(&self, edge: usize, drivers: &[usize]) -> Mode {
        let e = &self.edges[edge];
        if drivers.contains(&edge) || (!e.is_inner && e.driver == Driver::Sub) {
            Mode::Compare
        } else {
            Mode::Driven
        }
    }

    /// Collect into `out` the nodes to count under `index`, decided in
    /// `mode`: a compared node and every node its comparison may reach
    /// through inner edges; nothing under a driven node but the RIGHT
    /// children that drive it.
    fn count_below(&self, index: usize, mode: Mode, out: &mut Vec<usize>) {
        let mode = self.mode_of(index, mode);
        if mode == Mode::Compare {
            out.push(index);
        }
        let children: Vec<usize> = self.edges_of(index).map(|(edge, _)| edge).collect();
        for edge in children {
            let below = match mode {
                Mode::Compare if self.edges[edge].is_inner => Mode::Compare,
                _ => self.mode_below(edge, &[]),
            };
            self.count_below(self.edges[edge].child, below, out);
        }
    }

    /// The counts the planner wants, all at once: for each node it may
    /// compare, the node alone (its own filter, cursor included, with its
    /// `EXISTS` leaves taken as true and no `LIMIT`) and, when it has
    /// inner subs, the node narrowed (its subtree, the `LIMIT` off the
    /// node), each capped at the limit plus one.
    pub fn counts(&self) -> Vec<Count> {
        let cap = self.policy.limit + 1;
        let mut out = Vec::new();
        for &index in &self.counted {
            let node = &self.nodes[index];
            let main = SingleTableReadQuery {
                limit: u32::MAX,
                ..node.query.clone()
            };
            out.push(Count {
                node: index,
                narrowed: false,
                query: MultiTableReadQuery::single(SingleTableReadQuery {
                    filter: main.filter.assuming_true(&|leaf| {
                        leaf.comparison_operator == ComparisonOperator::EXISTS
                    }),
                    ..main.clone()
                }),
                cap,
            });
            if let Some(subtree) = self.query.node_at(&node.path)
                && subtree.joins.iter().any(|join| join.is_inner)
            {
                out.push(Count {
                    node: index,
                    narrowed: true,
                    query: MultiTableReadQuery::new(main, subtree.joins.clone()),
                    cap,
                });
            }
        }
        out
    }

    /// The table of the node `index`, for messages and logs.
    pub fn table_of(&self, index: usize) -> &str {
        self.nodes[index].query.table.as_str()
    }

    /// The answer to one of the counts: `rows` of the node `index`,
    /// `narrowed` or alone.
    pub fn answer(&mut self, index: usize, narrowed: bool, rows: u64) {
        if let Some(node) = self.nodes.get_mut(index) {
            if narrowed {
                node.narrowed = Some(rows);
            } else {
                node.alone = Some(rows);
            }
        }
    }

    /// The counts as answered, one entry per counted node, `table
    /// alone/narrowed` (`alone` only for a node counted once), for the
    /// log.
    pub fn report(&self) -> String {
        let limit = self.policy.limit;
        let shown = |count: Option<u64>| match count {
            Some(count) if count > limit => format!(">{limit}"),
            Some(count) => count.to_string(),
            None => "?".to_owned(),
        };
        self.counted
            .iter()
            .map(|&index| {
                let node = &self.nodes[index];
                match node.narrowed {
                    Some(_) => format!(
                        "{} {}/{}",
                        node.query.table,
                        shown(node.alone),
                        shown(node.narrowed)
                    ),
                    None => format!("{} {}", node.query.table, shown(node.alone)),
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Decide every inner edge's driver from the counts in, or refuse:
    /// every node's plan from the leaves up ([`Planner::plans`]), then
    /// the tree from the root down, a compared node by its plan and a
    /// node narrowed from above driving everything below it. Every page
    /// left driving an inner edge is then marked read whole when its
    /// alone count is within the whole-page limit, in batches otherwise.
    pub fn decide(mut self) -> Result<MultiTableReadQuery, String> {
        if self.counted.is_empty() {
            return Ok(self.query);
        }
        let plans = self.plans();
        let mut drivers: Vec<Option<Driver>> = vec![None; self.edges.len()];
        let mut over: Vec<usize> = Vec::new();
        self.decide_node(0, Mode::Compare, &plans, &mut drivers, &mut over);
        if !over.is_empty() {
            return Err(self.refusal(&over));
        }
        let decisions: Vec<(Vec<usize>, usize, Driver)> = self
            .edges
            .iter()
            .enumerate()
            .filter_map(|(index, edge)| {
                drivers[index]
                    .map(|driver| (self.nodes[edge.parent].path.clone(), edge.position, driver))
            })
            .collect();
        for (path, position, driver) in decisions {
            if let Some(join) = join_at(&mut self.query, &path, position) {
                join.driver = driver;
            }
        }
        let whole = self.policy.whole;
        let pages: Vec<(Vec<usize>, PageRead)> =
            self.nodes
                .iter()
                .enumerate()
                .filter(|(index, node)| {
                    node.query.limit != u32::MAX
                        && self.edges.iter().enumerate().any(|(edge, e)| {
                            e.parent == *index && drivers[edge] == Some(Driver::Main)
                        })
                })
                .map(|(_, node)| {
                    let read = match node.alone {
                        Some(count) if count <= whole => PageRead::Whole,
                        _ => PageRead::Batched,
                    };
                    (node.path.clone(), read)
                })
                .collect();
        for (path, read) in pages {
            if let Some(node) = self.query.node_at_mut(&path) {
                node.page = read;
            }
        }
        Ok(self.query)
    }

    /// Every counted node's plan, subs before their parents (a sub's
    /// index is always past its parent's), by [`Planner::plan_of`]; a
    /// node not counted has the empty plan.
    fn plans(&self) -> Vec<Plan> {
        let mut plans = vec![Plan::default(); self.nodes.len()];
        for index in (0..self.nodes.len()).rev() {
            if self.counted.contains(&index) {
                plans[index] = self.plan_of(index, &plans);
            }
        }
        plans
    }

    /// The plan of the node `index` from its counts and its subs' plans
    /// (`plans`, decided already): read whole it holds its alone count;
    /// driven by its subs it holds its narrowed count (a page, its window
    /// at most) plus the hold of each driver, the drivers being every
    /// bounded sub under an `OR` (with one of those unbounded there is no
    /// such plan), every bounded sub under a page, and otherwise each
    /// required bounded sub, smallest first, that costs no more than what
    /// the node keeps with the drivers before it — or the smallest of
    /// them when none does. The smaller hold is the plan; a tie goes to
    /// the subs under a page or when the child is preferred.
    fn plan_of(&self, index: usize, plans: &[Plan]) -> Plan {
        let node = &self.nodes[index];
        let limit = self.policy.limit;
        let paged = node.query.limit != u32::MAX;
        let alone = node.alone.filter(|&count| count <= limit);
        let whole = Plan {
            drivers: Vec::new(),
            hold: alone,
        };
        let inner = self.inner_of(index);
        if inner.is_empty() {
            return whole;
        }
        let Some(mut hold) = (if paged {
            Some(
                node.narrowed
                    .unwrap_or(u64::MAX)
                    .min(u64::from(node.query.limit)),
            )
        } else {
            node.narrowed.filter(|&count| count <= limit)
        }) else {
            return whole;
        };
        let mut drivers = Vec::new();
        let mut required: Vec<(u64, usize)> = Vec::new();
        for &edge in &inner {
            let bounded = plans[self.edges[edge].child].hold;
            match (self.requires(edge), bounded) {
                (false, None) => return whole,
                (false, Some(cost)) | (true, Some(cost)) if paged => {
                    drivers.push(edge);
                    hold += cost;
                }
                (false, Some(cost)) => {
                    drivers.push(edge);
                    hold += cost;
                }
                (true, Some(cost)) => required.push((cost, edge)),
                (true, None) => {}
            }
        }
        required.sort();
        for &(cost, edge) in &required {
            if cost <= hold {
                drivers.push(edge);
                hold += cost;
            }
        }
        if drivers.is_empty()
            && let Some(&(cost, edge)) = required.first()
        {
            drivers.push(edge);
            hold += cost;
        }
        if drivers.is_empty() {
            return whole;
        }
        let narrowed = Plan {
            drivers,
            hold: Some(hold),
        };
        match alone {
            None => narrowed,
            Some(alone) => match hold.cmp(&alone) {
                std::cmp::Ordering::Less => narrowed,
                std::cmp::Ordering::Greater => whole,
                std::cmp::Ordering::Equal if paged || self.policy.preferred == Side::Child => {
                    narrowed
                }
                std::cmp::Ordering::Equal => whole,
            },
        }
    }

    /// Whether the parent's filter requires the inner edge `edge`: its
    /// `EXISTS` leaf stands on a path of `AND`s from the root, or no leaf
    /// names it (an unnamed inner edge is conjoined). An edge only named
    /// under an `OR` is not required: the filter is cut down by it only
    /// when its other branches are too.
    fn requires(&self, edge: usize) -> bool {
        let e = &self.edges[edge];
        let filter = &self.nodes[e.parent].query.filter;
        let Some(index) = e.inner_index else {
            return true;
        };
        let leaf = Condition::new(
            e.column.clone(),
            ComparisonOperator::EXISTS,
            Value::Int(index as i64),
        );
        !filter.contains(&leaf) || filter.requires(&leaf)
    }

    /// Decide the node `index` in `mode` and every node below it: the
    /// driver of each inner edge under it into `drivers`, the nodes no
    /// plan bounds (and no page saves) into `over`.
    fn decide_node(
        &self,
        index: usize,
        mode: Mode,
        plans: &[Plan],
        drivers: &mut [Option<Driver>],
        over: &mut Vec<usize>,
    ) {
        let mode = self.mode_of(index, mode);
        let inner = self.inner_of(index);
        let chosen: &[usize] = match mode {
            Mode::Driven => &[],
            Mode::Compare => &plans[index].drivers,
        };
        if mode == Mode::Compare
            && plans[index].hold.is_none()
            && self.nodes[index].query.limit == u32::MAX
        {
            over.push(index);
            over.extend(
                inner
                    .iter()
                    .map(|&edge| self.edges[edge].child)
                    .filter(|&child| plans[child].hold.is_none()),
            );
        }
        for &edge in &inner {
            drivers[edge] = Some(if chosen.contains(&edge) {
                Driver::Sub
            } else {
                Driver::Main
            });
        }
        let children: Vec<usize> = self.edges_of(index).map(|(edge, _)| edge).collect();
        for edge in children {
            let below = self.mode_below(edge, chosen);
            self.decide_node(self.edges[edge].child, below, plans, drivers, over);
        }
    }

    /// The reason for refusing: every node that came out over the limit.
    fn refusal(&self, over: &[usize]) -> String {
        let limit = self.policy.limit;
        let named: Vec<String> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(index, node)| {
                over.contains(index) && node.alone.is_some_and(|count| count > limit)
            })
            .map(|(_, node)| format!("{} holds more than {limit}", node.query.table))
            .collect();
        format!(
            "the query would read more than {limit} rows into memory ({})",
            named.join(", ")
        )
    }
}

/// Whether some node of `query` with a page drives an inner edge: the
/// plan whose page the engine keeps to the rows the edge admits.
pub fn page_drives(query: &MultiTableReadQuery) -> bool {
    let paged = query.main_table.limit != u32::MAX;
    query.joins.iter().any(|join| {
        (paged && join.is_inner && join.driver == Driver::Main) || page_drives(&join.sub)
    })
}

/// Flatten `query`'s subtree at `path` into `nodes` and `edges`; `above`
/// is the edge the node hangs from, which says whether an outer edge
/// drives it.
fn flatten(
    query: &MultiTableReadQuery,
    path: Vec<usize>,
    above: Option<(Driver, bool)>,
    nodes: &mut Vec<PlanNode>,
    edges: &mut Vec<PlanEdge>,
) -> usize {
    let index = nodes.len();
    nodes.push(PlanNode {
        path: path.clone(),
        query: query.main_table.clone(),
        driven: matches!(above, Some((Driver::Main, false))),
        alone: None,
        narrowed: None,
    });
    let mut inner_seen = 0;
    for (position, join) in query.joins.iter().enumerate() {
        let mut child_path = path.clone();
        child_path.push(position);
        let child = flatten(
            &join.sub,
            child_path,
            Some((join.driver, join.is_inner)),
            nodes,
            edges,
        );
        if join.driver == Driver::Sub && !join.is_inner {
            nodes[index].driven = true;
        }
        let inner_index = join.is_inner.then_some(inner_seen);
        if join.is_inner {
            inner_seen += 1;
        }
        edges.push(PlanEdge {
            parent: index,
            child,
            position,
            column: join.main_table_column.clone(),
            driver: join.driver,
            is_inner: join.is_inner,
            inner_index,
        });
    }
    index
}

/// The join at `position` under the node at `path`.
fn join_at<'a>(
    query: &'a mut MultiTableReadQuery,
    path: &[usize],
    position: usize,
) -> Option<&'a mut crate::model::Join> {
    let mut node = query;
    for &step in path {
        node = &mut node.joins.get_mut(step)?.sub;
    }
    node.joins.get_mut(position)
}

/// One remembered decision.
struct Entry {
    outcome: Result<MultiTableReadQuery, String>,
    at: Instant,
    tick: u64,
}

/// The cache's state: the decisions by the tree they were made for, and
/// the least recently used order.
#[derive(Default)]
struct CacheState {
    entries: HashMap<MultiTableReadQuery, Entry>,
    order: BTreeMap<u64, MultiTableReadQuery>,
    tick: u64,
}

/// Remembered plans, keyed by the tree as translated: a hit skips the
/// counts. An entry expires after `ttl` (a table that grew past the limit
/// is re-counted eventually), and the least recently used entry leaves
/// when `capacity` is reached (every literal in a filter is its own key).
pub struct PlanCache {
    state: Mutex<CacheState>,
    ttl: Duration,
    capacity: usize,
}

impl PlanCache {
    /// A cache keeping each decision for `ttl`, at most `capacity` of
    /// them.
    pub fn new(ttl: Duration, capacity: usize) -> Self {
        PlanCache {
            state: Mutex::new(CacheState::default()),
            ttl,
            capacity: capacity.max(1),
        }
    }

    /// The remembered decision for `query`, if it has one that has not
    /// expired.
    pub fn get(&self, query: &MultiTableReadQuery) -> Option<Result<MultiTableReadQuery, String>> {
        let mut state = self.lock();
        let tick = state.tick + 1;
        state.tick = tick;
        let (outcome, old_tick) = {
            let entry = state.entries.get_mut(query)?;
            if entry.at.elapsed() > self.ttl {
                None
            } else {
                let old = entry.tick;
                entry.tick = tick;
                Some((entry.outcome.clone(), old))
            }?
        };
        state.order.remove(&old_tick);
        state.order.insert(tick, query.clone());
        Some(outcome)
    }

    /// Remember `outcome` for `query`.
    pub fn put(&self, query: MultiTableReadQuery, outcome: Result<MultiTableReadQuery, String>) {
        let mut state = self.lock();
        let tick = state.tick + 1;
        state.tick = tick;
        if let Some(previous) = state.entries.remove(&query) {
            state.order.remove(&previous.tick);
        }
        while state.entries.len() >= self.capacity {
            let Some((&oldest, _)) = state.order.iter().next() else {
                break;
            };
            if let Some(evicted) = state.order.remove(&oldest) {
                state.entries.remove(&evicted);
            }
        }
        state.order.insert(tick, query.clone());
        state.entries.insert(
            query,
            Entry {
                outcome,
                at: Instant::now(),
                tick,
            },
        );
    }

    /// How many decisions are remembered.
    pub fn len(&self) -> usize {
        self.lock().entries.len()
    }

    /// Whether nothing is remembered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The state, read through a poisoned lock.
    fn lock(&self) -> std::sync::MutexGuard<'_, CacheState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Plan `translated` under `policy`: the cache's decision when it has
/// one, otherwise the counts on `storage` (concurrently) and the
/// planner's decision, remembered in the cache. A count that fails (the
/// database was unreachable, the count ran past the read timeout) refuses
/// the query, saying so, and is not remembered: it says nothing about the
/// query, and the next asking counts again. The counts are logged at
/// debug level, `table alone/narrowed` per node.
pub async fn plan<S: Storage + ?Sized>(
    translated: Translated,
    policy: Policy,
    cache: &PlanCache,
    storage: &S,
) -> Result<Translated, String> {
    let Translated { query, hidden } = translated;
    if let Some(outcome) = cache.get(&query) {
        return outcome.map(|query| Translated { query, hidden });
    }
    let mut planner = Planner::new(query.clone(), policy);
    let wanted = planner.counts();
    let answers = join_all(wanted.iter().map(|count| async move {
        let started = Instant::now();
        let answer = storage.count(&count.query, count.cap).await;
        if let Some(stats) = crate::stats::Stats::global() {
            stats.count_io.record(started.elapsed());
        }
        answer
    }))
    .await;
    let mut failed = None;
    for (count, answer) in wanted.iter().zip(answers) {
        match answer {
            Ok(rows) => planner.answer(count.node, count.narrowed, rows),
            Err(error) => {
                failed = Some(match error.refusal() {
                    Some(reason) => reason.to_owned(),
                    None => format!(
                        "counting the rows of {} failed: {error}",
                        planner.table_of(count.node)
                    ),
                });
                break;
            }
        }
    }
    if !wanted.is_empty() {
        log_event!(
            Level::Debug,
            "query counted",
            table = query.main_table.table,
            counts = planner.report()
        );
    }
    let outcome = match failed {
        Some(reason) => return Err(reason),
        None => planner.decide(),
    };
    cache.put(query, outcome.clone());
    outcome.map(|query| Translated { query, hidden })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::ivm::QueryPart;
    use crate::model::{Join, Order, OrderBy, Where};

    /// `table WHERE filter`, pkey-ordered, with `limit`.
    fn node(table: &str, filter: Where, limit: u32) -> SingleTableReadQuery {
        SingleTableReadQuery::new(table, filter, OrderBy::new("id", Order::ASC), limit)
    }

    /// `messages WHERE EXISTS(conversations WHERE channelId = 'c1')`, the
    /// EXISTS named in the filter, with the conversations part hidden.
    fn messages_in_channel(limit: u32) -> Translated {
        let conversations = MultiTableReadQuery::single(node(
            "conversations",
            Where::condition("channelId", ComparisonOperator::EQ, "c1"),
            u32::MAX,
        ));
        let query = MultiTableReadQuery::new(
            node("messages", Where::exists("conversationId", 0), limit),
            vec![Join::inner(
                conversations,
                "conversationId",
                "conversationId",
            )],
        );
        Translated {
            query,
            hidden: HashSet::from([QueryPart::join(0)]),
        }
    }

    /// Drive `planner` with the counts by table name — `alone` for every
    /// node, `narrowed` for the nodes with inner subs (a table missing
    /// there is narrowed to its alone count: its subs cut nothing) —
    /// returning the tables it counted alone (in the order it asked) and
    /// its decision.
    fn drive(
        mut planner: Planner,
        alone: &[(&str, u64)],
        narrowed: &[(&str, u64)],
    ) -> (Vec<String>, Result<MultiTableReadQuery, String>) {
        let wanted = planner.counts();
        let asked: Vec<String> = wanted
            .iter()
            .filter(|count| !count.narrowed)
            .map(|count| count.query.main_table.table.to_string())
            .collect();
        let of = |answers: &[(&str, u64)], table: &str| {
            answers
                .iter()
                .find(|(named, _)| *named == table)
                .map(|(_, rows)| *rows)
        };
        for count in &wanted {
            assert_eq!(
                count.cap,
                planner.policy.limit + 1,
                "the cap is the limit plus one"
            );
            assert_eq!(count.query.main_table.limit, u32::MAX, "no LIMIT counted");
            let table = count.query.main_table.table.as_str();
            let rows = match count.narrowed {
                true => of(narrowed, table).or_else(|| of(alone, table)),
                false => of(alone, table),
            }
            .unwrap_or_else(|| panic!("no answer for {table}"));
            planner.answer(count.node, count.narrowed, rows);
        }
        (asked, planner.decide())
    }

    /// The policy with `limit`, preferring `preferred`, reading no page
    /// whole.
    fn policy(limit: u64, preferred: Side) -> Policy {
        Policy {
            limit,
            preferred,
            whole: 0,
        }
    }

    /// A node with inner subs is counted twice, alone (its `EXISTS`
    /// leaves taken as true, no joins) and narrowed (its subtree, the
    /// filter as it stands); a leaf once; a `LIMIT` never.
    #[test]
    fn a_node_is_counted_alone_and_narrowed() {
        let filter = Where::AND(vec![
            Where::condition("status", ComparisonOperator::EQ, "OPEN"),
            Where::OR(vec![
                Where::condition("visibility", ComparisonOperator::EQ, "PUBLIC"),
                Where::exists("id", 0),
            ]),
        ]);
        let users = MultiTableReadQuery::single(node("users", Where::AND(Vec::new()), u32::MAX));
        let query = MultiTableReadQuery::new(
            node("tickets", filter.clone(), 50),
            vec![Join::inner(users, "id", "ticketId")],
        );
        let planner = Planner::new(query.clone(), policy(100, Side::Child));
        let counts = planner.counts();
        assert_eq!(counts.len(), 3);
        assert_eq!((counts[0].node, counts[0].narrowed), (0, false));
        assert_eq!(
            counts[0].query.main_table.filter,
            Where::AND(vec![Where::condition(
                "status",
                ComparisonOperator::EQ,
                "OPEN"
            )])
        );
        assert!(counts[0].query.joins.is_empty());
        assert_eq!((counts[1].node, counts[1].narrowed), (0, true));
        assert_eq!(counts[1].query.main_table.filter, filter);
        assert_eq!(counts[1].query.main_table.limit, u32::MAX);
        assert_eq!(counts[1].query.joins, query.joins);
        assert_eq!((counts[2].node, counts[2].narrowed), (1, false));
        assert!(counts.iter().all(|count| count.cap == 101));
    }

    /// A page that drives is read whole when its alone count is within
    /// the whole-page limit and in batches otherwise; a page a sub
    /// drives, and a node without a page, are left as translated.
    #[test]
    fn a_driving_page_is_read_whole_within_the_whole_page_limit() {
        let whole = Policy {
            limit: 100,
            preferred: Side::Parent,
            whole: 10,
        };
        for (messages, expected) in [(10, PageRead::Whole), (11, PageRead::Batched)] {
            let (_, outcome) = drive(
                Planner::new(messages_in_channel(50).query, whole),
                &[("messages", messages), ("conversations", 101)],
                &[],
            );
            let planned = outcome.expect("planned");
            assert_eq!(planned.joins[0].driver, Driver::Main, "the page drives");
            assert_eq!(planned.page, expected, "a page of {messages} rows");
        }
        let (_, outcome) = drive(
            Planner::new(messages_in_channel(50).query, whole),
            &[("messages", 50), ("conversations", 3)],
            &[("messages", 5)],
        );
        let planned = outcome.expect("planned");
        assert_eq!(planned.joins[0].driver, Driver::Sub, "the sub drives");
        assert_eq!(planned.page, PageRead::Batched, "left as translated");
        let (_, outcome) = drive(
            Planner::new(messages_in_channel(u32::MAX).query, whole),
            &[("messages", 5), ("conversations", 101)],
            &[],
        );
        assert_eq!(
            outcome.expect("planned").page,
            PageRead::Batched,
            "no page, nothing to read whole"
        );
    }

    /// A query without joins is registered as it is with nothing counted.
    #[test]
    fn nothing_to_plan_without_joins() {
        let plain = MultiTableReadQuery::single(node("tickets", Where::AND(Vec::new()), u32::MAX));
        let (asked, outcome) = drive(
            Planner::new(plain.clone(), policy(10, Side::Child)),
            &[],
            &[],
        );
        assert!(asked.is_empty());
        assert_eq!(outcome.expect("kept"), plain);
    }

    /// Both sides are counted in one batch; a child that cuts the parent
    /// down keeps the tree as translated (the sub drives, as
    /// `whereExists` does).
    #[test]
    fn a_small_child_keeps_the_tree() {
        let translated = messages_in_channel(u32::MAX);
        let (asked, outcome) = drive(
            Planner::new(translated.query.clone(), policy(100, Side::Child)),
            &[("messages", 40), ("conversations", 7)],
            &[("messages", 10)],
        );
        assert_eq!(asked, vec!["messages", "conversations"]);
        let kept = outcome.expect("kept");
        assert_eq!(kept, translated.query);
        assert_eq!(kept.joins[0].driver, Driver::Sub);
    }

    /// The child is too big and the parent fits: the edge is flipped to
    /// the parent, which is read whole and drives the child; the root,
    /// the filter and the hidden part stay where they were.
    #[test]
    fn a_big_child_and_a_small_parent_flip_the_edge() {
        let translated = messages_in_channel(u32::MAX);
        let (asked, outcome) = drive(
            Planner::new(translated.query.clone(), policy(100, Side::Child)),
            &[("messages", 40), ("conversations", 101)],
            &[],
        );
        assert_eq!(asked, vec!["messages", "conversations"]);
        let planned = outcome.expect("planned");
        assert_eq!(planned.main_table, translated.query.main_table);
        assert_eq!(planned.joins[0].driver, Driver::Main);
        assert!(planned.joins[0].is_inner);
        assert_eq!(planned.joins[0].sub, translated.query.joins[0].sub);
    }

    /// Both sides over the limit, the child cutting the parent down to
    /// nothing under it: refused, naming both. A parent narrowed within
    /// the limit by a child over it is refused too: the child cannot be
    /// read whole to narrow it.
    #[test]
    fn two_big_sides_are_refused() {
        let (_, outcome) = drive(
            Planner::new(
                messages_in_channel(u32::MAX).query,
                policy(100, Side::Child),
            ),
            &[("messages", 101), ("conversations", 101)],
            &[],
        );
        let reason = outcome.expect_err("refused");
        assert!(
            reason.contains("conversations holds more than 100"),
            "{reason}"
        );
        assert!(reason.contains("messages holds more than 100"), "{reason}");
        let (_, outcome) = drive(
            Planner::new(
                messages_in_channel(u32::MAX).query,
                policy(100, Side::Child),
            ),
            &[("messages", 101), ("conversations", 101)],
            &[("messages", 5)],
        );
        assert!(outcome.is_err(), "no bounded side to read whole");
    }

    /// A root with a page is counted like any node, on its filter without
    /// the page; a child too big to restrict it is driven by the page
    /// (the engine keeps the page to the rows the child admits), whether
    /// the page's own count fits or not.
    #[test]
    fn a_paged_root_drives_a_big_child() {
        for messages in [40, 101] {
            let (asked, outcome) = drive(
                Planner::new(messages_in_channel(50).query, policy(100, Side::Child)),
                &[("messages", messages), ("conversations", 101)],
                &[],
            );
            assert_eq!(asked, vec!["messages", "conversations"]);
            assert_eq!(
                outcome.expect("planned").joins[0].driver,
                Driver::Main,
                "{messages} messages"
            );
        }
    }

    /// `messages WHERE <own> LIMIT limit` under `EXISTS conversations
    /// (EXISTS channels)`: the access rule of a message.
    fn message_under_the_access_rule(limit: u32) -> MultiTableReadQuery {
        let channels = MultiTableReadQuery::single(node(
            "channels",
            Where::condition("workspaceId", ComparisonOperator::EQ, "w"),
            u32::MAX,
        ));
        let conversations = MultiTableReadQuery::new(
            node("conversations", Where::exists("channelId", 0), u32::MAX),
            vec![Join::inner(channels, "channelId", "id")],
        );
        MultiTableReadQuery::new(
            node(
                "messages",
                Where::AND(vec![
                    Where::condition("id", ComparisonOperator::EQ, "m1"),
                    Where::exists("conversationId", 0),
                ]),
                limit,
            ),
            vec![Join::inner(
                conversations,
                "conversationId",
                "conversationId",
            )],
        )
    }

    /// `messages WHERE id = ? LIMIT 1` under the access rule, the
    /// conversations huge (sixty of them under the channels), the
    /// channels few. The page, one row alone, drives the conversations —
    /// no sub costs less than one row — and the conversations, driven
    /// from above, drive the channels in turn: every link is read
    /// narrowed from the row outwards. The channels never drive the
    /// conversations (every conversation of every channel) into the page.
    #[test]
    fn a_page_of_one_row_drives_the_rule_from_the_row_outwards() {
        let (asked, outcome) = drive(
            Planner::new(message_under_the_access_rule(1), policy(100, Side::Parent)),
            &[("messages", 1), ("conversations", 101), ("channels", 20)],
            &[("conversations", 60)],
        );
        assert_eq!(asked, vec!["messages", "conversations", "channels"]);
        let planned = outcome.expect("planned");
        assert_eq!(planned.joins[0].driver, Driver::Main, "the page drives");
        assert_eq!(
            planned.joins[0].sub.joins[0].driver,
            Driver::Main,
            "the conversation it reaches drives the channels"
        );
    }

    /// The same rule under an unpaged root of five rows: nothing cuts
    /// five rows down for less than five, so the root drives, and the
    /// node driven from above drives the edges below it however small
    /// the sub's own count is (a conversation's messages reach one
    /// conversation, whatever the table holds).
    #[test]
    fn a_node_driven_from_above_drives_below() {
        for preferred in [Side::Parent, Side::Child] {
            let (_, outcome) = drive(
                Planner::new(
                    message_under_the_access_rule(u32::MAX),
                    policy(100, preferred),
                ),
                &[("messages", 5), ("conversations", 90), ("channels", 3)],
                &[],
            );
            let planned = outcome.expect("planned");
            assert_eq!(planned.joins[0].driver, Driver::Main, "{preferred:?}");
            assert_eq!(
                planned.joins[0].sub.joins[0].driver,
                Driver::Main,
                "{preferred:?}"
            );
        }
    }

    /// With nothing small at the root the rule's own side is all there
    /// is: the three channels cut the conversations to thirty, and those
    /// cut the messages to eighty, so the channels drive the
    /// conversations and those the messages, the holds adding up along
    /// the chain (thirty-three, then a hundred and thirteen) — a chain's
    /// total may exceed the limit, each node's rows stay within it.
    #[test]
    fn a_big_root_is_driven_from_the_small_end_of_the_chain() {
        let (_, outcome) = drive(
            Planner::new(
                message_under_the_access_rule(u32::MAX),
                policy(100, Side::Parent),
            ),
            &[("messages", 101), ("conversations", 101), ("channels", 3)],
            &[("messages", 80), ("conversations", 30)],
        );
        let planned = outcome.expect("planned");
        assert_eq!(planned.joins[0].driver, Driver::Sub);
        assert_eq!(planned.joins[0].sub.joins[0].driver, Driver::Sub);
    }

    /// A paged root whose alone count is over the limit is driven by a
    /// child that fits, whichever side is preferred, whether the child
    /// narrows it within the limit or not: the page holds its window at
    /// most, so the child restricts it inside its own filter.
    #[test]
    fn a_paged_root_is_driven_by_a_small_child() {
        for preferred in [Side::Parent, Side::Child] {
            for narrowed in [40, 101] {
                let (_, outcome) = drive(
                    Planner::new(messages_in_channel(50).query, policy(100, preferred)),
                    &[("messages", 101), ("conversations", 5)],
                    &[("messages", narrowed)],
                );
                assert_eq!(
                    outcome.expect("planned").joins[0].driver,
                    Driver::Sub,
                    "the page is over the limit, so the child drives ({preferred:?}, narrowed to {narrowed})"
                );
            }
        }
    }

    /// A page and a fitting child compare by what each plan holds: a
    /// page of three rows drives a child of five (the child would hold
    /// eight to show three); a page of fifty cut to ten by that child is
    /// driven by it (fifteen against fifty); and a page gets no
    /// preferred-side allowance, so a tie goes to the child.
    #[test]
    fn a_page_and_a_fitting_child_compare_strictly() {
        for (messages, narrowed, conversations, expected) in [
            (3, 3, 5, Driver::Main),
            (50, 10, 5, Driver::Sub),
            (10, 5, 5, Driver::Sub),
        ] {
            for preferred in [Side::Parent, Side::Child] {
                let (_, outcome) = drive(
                    Planner::new(messages_in_channel(50).query, policy(100, preferred)),
                    &[("messages", messages), ("conversations", conversations)],
                    &[("messages", narrowed)],
                );
                assert_eq!(
                    outcome.expect("planned").joins[0].driver,
                    expected,
                    "a page of {messages} narrowed to {narrowed} against {conversations}, preferring {preferred:?}"
                );
            }
        }
    }

    /// The canvas rule as xyne-spaces states it: `canvases WHERE <own>
    /// AND (createdBy = me OR EXISTS participants OR visibility = PUBLIC)
    /// AND EXISTS users LIMIT limit`, the participants `userId = me OR
    /// EXISTS group(EXISTS member = me) OR EXISTS channel(EXISTS member =
    /// me)`.
    fn canvases_under_the_rule(own: Where, limit: u32) -> MultiTableReadQuery {
        let memberships = MultiTableReadQuery::single(node(
            "channel_participants",
            Where::condition("userId", ComparisonOperator::EQ, "me"),
            u32::MAX,
        ));
        let channels = MultiTableReadQuery::new(
            node("channels", Where::exists("id", 0), u32::MAX),
            vec![Join::inner(memberships, "id", "channelId")],
        );
        let mappings = MultiTableReadQuery::single(node(
            "user_group_mappings",
            Where::condition("userId", ComparisonOperator::EQ, "me"),
            u32::MAX,
        ));
        let groups = MultiTableReadQuery::new(
            node("user_groups", Where::exists("id", 0), u32::MAX),
            vec![Join::inner(mappings, "id", "groupId")],
        );
        let participants = MultiTableReadQuery::new(
            node(
                "canvas_participants",
                Where::OR(vec![
                    Where::condition("userId", ComparisonOperator::EQ, "me"),
                    Where::exists("groupId", 0),
                    Where::exists("channelId", 1),
                ]),
                u32::MAX,
            ),
            vec![
                Join::inner(groups, "groupId", "id"),
                Join::inner(channels, "channelId", "id"),
            ],
        );
        let users = MultiTableReadQuery::single(node("users", Where::AND(Vec::new()), u32::MAX));
        MultiTableReadQuery::new(
            node(
                "canvases",
                Where::AND(vec![
                    own,
                    Where::OR(vec![
                        Where::condition("createdBy", ComparisonOperator::EQ, "me"),
                        Where::exists("id", 0),
                        Where::condition("visibility", ComparisonOperator::EQ, "PUBLIC"),
                    ]),
                    Where::exists("createdBy", 1),
                ]),
                limit,
            ),
            vec![
                Join::inner(participants, "id", "canvasId"),
                Join::inner(users, "createdBy", "id"),
            ],
        )
    }

    /// `canvases WHERE id = ? LIMIT 1` under the canvas rule as the rig
    /// holds it: the participants (an `EXISTS` inside an `OR`, so alone
    /// they are the whole table, 25 100) reach user groups and channels
    /// (44 066) and the channels their memberships; a second `EXISTS` on
    /// users. With the page counted at one row, every inner edge is
    /// driven from the page outwards, however small the leaves: nothing
    /// costs less than the one row.
    #[test]
    fn a_one_row_page_drives_the_canvas_rule_from_the_row_outwards() {
        let query =
            canvases_under_the_rule(Where::condition("id", ComparisonOperator::EQ, "cv1"), 1);
        let (asked, outcome) = drive(
            Planner::new(query, policy(20_000, Side::Parent)),
            &[
                ("canvases", 1),
                ("canvas_participants", 20_001),
                ("user_groups", 42),
                ("user_group_mappings", 5),
                ("channels", 20_001),
                ("channel_participants", 52),
                ("users", 2_000),
            ],
            &[
                ("canvases", 1),
                ("canvas_participants", 700),
                ("user_groups", 5),
                ("channels", 52),
            ],
        );
        assert_eq!(
            asked,
            vec![
                "canvases",
                "canvas_participants",
                "user_groups",
                "user_group_mappings",
                "channels",
                "channel_participants",
                "users"
            ]
        );
        let planned = outcome.expect("served");
        assert_eq!(
            planned.joins[0].driver,
            Driver::Main,
            "the page drives the participants"
        );
        assert_eq!(planned.joins[1].driver, Driver::Main, "and the users");
        let participants = &planned.joins[0].sub;
        assert_eq!(
            participants.joins[0].driver,
            Driver::Main,
            "the participants drive the groups"
        );
        assert_eq!(
            participants.joins[1].driver,
            Driver::Main,
            "and the channels"
        );
        assert_eq!(
            participants.joins[1].sub.joins[0].driver,
            Driver::Main,
            "and the channels their memberships"
        );
        assert!(page_drives(&planned));
    }

    /// The reader's canvases, a page of twenty over twenty thousand, as
    /// the sandbox holds them: alone, every node but the leaves is its
    /// whole table; narrowed, the reader's five group memberships cut
    /// the groups to five, their fifty channel memberships the channels
    /// to fifty, those the participations to seven hundred and those the
    /// canvases to eight hundred. So the leaves drive all the way up,
    /// the users too under the page (every bounded sub drives a page, so
    /// its window is exact), and the page drives nothing: one read of
    /// twenty canvases in place of a walk of the table.
    #[test]
    fn a_page_is_driven_from_the_leaves_that_cut_it_down() {
        let query = canvases_under_the_rule(Where::AND(Vec::new()), 20);
        let sandbox = Policy {
            limit: 100_000,
            preferred: Side::Parent,
            whole: 5_000,
        };
        let (_, outcome) = drive(
            Planner::new(query, sandbox),
            &[
                ("canvases", 20_000),
                ("canvas_participants", 50_000),
                ("user_groups", 300),
                ("user_group_mappings", 5),
                ("channels", 44_000),
                ("channel_participants", 50),
                ("users", 3_694),
            ],
            &[
                ("canvases", 800),
                ("canvas_participants", 700),
                ("user_groups", 5),
                ("channels", 50),
            ],
        );
        let planned = outcome.expect("served");
        assert_eq!(
            planned.joins[0].driver,
            Driver::Sub,
            "the participants drive the page"
        );
        assert_eq!(planned.joins[1].driver, Driver::Sub, "the users too");
        let participants = &planned.joins[0].sub;
        assert_eq!(
            participants.joins[0].driver,
            Driver::Sub,
            "the groups drive the participants"
        );
        assert_eq!(
            participants.joins[1].driver,
            Driver::Sub,
            "the channels too"
        );
        assert_eq!(
            participants.joins[0].sub.joins[0].driver,
            Driver::Sub,
            "the group memberships drive the groups"
        );
        assert_eq!(
            participants.joins[1].sub.joins[0].driver,
            Driver::Sub,
            "the channel memberships drive the channels"
        );
        assert!(!page_drives(&planned), "the page drives nothing");
        assert_eq!(planned.page, PageRead::Batched, "left as translated");
    }

    /// The DM list: a page of twenty channel stats over forty thousand
    /// under `EXISTS channel(type IN (DM, GROUP_DM) AND EXISTS member =
    /// me)`. Alone the channels are forty-four thousand; the reader's
    /// fifty memberships cut them to fifty, and those the stats to fifty:
    /// the memberships drive the channels and the channels the page.
    #[test]
    fn the_dm_list_is_driven_by_the_readers_channels() {
        let memberships = MultiTableReadQuery::single(node(
            "channel_participants",
            Where::condition("userId", ComparisonOperator::EQ, "me"),
            u32::MAX,
        ));
        let channels = MultiTableReadQuery::new(
            node(
                "channels",
                Where::AND(vec![
                    Where::condition(
                        "scopeType",
                        ComparisonOperator::IN,
                        Value::List(vec!["DM".into(), "GROUP_DM".into()]),
                    ),
                    Where::exists("id", 0),
                ]),
                u32::MAX,
            ),
            vec![Join::inner(memberships, "id", "channelId")],
        );
        let query = MultiTableReadQuery::new(
            node("channel_stats", Where::exists("channelId", 0), 20),
            vec![Join::inner(channels, "channelId", "id")],
        );
        let (_, outcome) = drive(
            Planner::new(query, policy(100_000, Side::Parent)),
            &[
                ("channel_stats", 40_000),
                ("channels", 44_000),
                ("channel_participants", 50),
            ],
            &[("channel_stats", 50), ("channels", 50)],
        );
        let planned = outcome.expect("served");
        assert_eq!(planned.joins[0].driver, Driver::Sub);
        assert_eq!(planned.joins[0].sub.joins[0].driver, Driver::Sub);
        assert!(!page_drives(&planned));
    }

    /// The preferred side gets no allowance: the plan that holds less
    /// wins however slightly, and the preferred side decides a tie only.
    #[test]
    fn the_preferred_side_breaks_a_tie_only() {
        for (messages, narrowed, conversations, preferred, expected) in [
            (40, 25, 5, Side::Parent, Driver::Sub),
            (30, 25, 5, Side::Parent, Driver::Main),
            (30, 25, 5, Side::Child, Driver::Sub),
            (0, 0, 0, Side::Parent, Driver::Main),
            (0, 0, 0, Side::Child, Driver::Sub),
        ] {
            let (_, outcome) = drive(
                Planner::new(messages_in_channel(u32::MAX).query, policy(100, preferred)),
                &[("messages", messages), ("conversations", conversations)],
                &[("messages", narrowed)],
            );
            assert_eq!(
                outcome.expect("planned").joins[0].driver,
                expected,
                "{messages} messages narrowed to {narrowed}, {conversations} conversations, preferring {preferred:?}"
            );
        }
    }

    /// `messages WHERE EXISTS channels AND EXISTS users`, a thousand
    /// messages cut to a hundred by the two: a required sub drives when
    /// it costs no more than what the node keeps with the drivers before
    /// it (fifty channels against a hundred; then eight hundred users
    /// against a hundred and fifty, gated; a hundred users, driving);
    /// with the channels over the limit the users drive alone, being all
    /// there is; and subs that cut nothing down are driven by the node.
    #[test]
    fn a_required_sub_drives_when_it_costs_no_more_than_the_node_keeps() {
        let channels =
            MultiTableReadQuery::single(node("channels", Where::AND(Vec::new()), u32::MAX));
        let users = MultiTableReadQuery::single(node("users", Where::AND(Vec::new()), u32::MAX));
        let query = MultiTableReadQuery::new(
            node(
                "messages",
                Where::AND(vec![
                    Where::exists("channelId", 0),
                    Where::exists("senderId", 1),
                ]),
                u32::MAX,
            ),
            vec![
                Join::inner(channels, "channelId", "id"),
                Join::inner(users, "senderId", "id"),
            ],
        );
        for (narrowed, channels, users, expected) in [
            (100, 50, 800, [Driver::Sub, Driver::Main]),
            (100, 50, 100, [Driver::Sub, Driver::Sub]),
            (100, 1_001, 800, [Driver::Main, Driver::Sub]),
            (1_000, 50, 100, [Driver::Main, Driver::Main]),
        ] {
            let (_, outcome) = drive(
                Planner::new(query.clone(), policy(1_000, Side::Parent)),
                &[
                    ("messages", 1_000),
                    ("channels", channels),
                    ("users", users),
                ],
                &[("messages", narrowed)],
            );
            let planned = outcome.expect("planned");
            assert_eq!(
                [planned.joins[0].driver, planned.joins[1].driver],
                expected,
                "1000 messages narrowed to {narrowed}, {channels} channels, {users} users"
            );
        }
    }

    /// `messages WHERE status = ? OR EXISTS channels`: a sub under an
    /// `OR` cuts the node down only if it drives, so with the channels
    /// over the limit the node is read whole (five hundred messages),
    /// and with the channels within it they drive (a hundred and ten
    /// against five hundred); a page over the limit under such a sub is
    /// still served, the page driving.
    #[test]
    fn a_sub_under_an_or_too_big_to_drive_leaves_the_node_whole() {
        let query = |limit: u32| {
            MultiTableReadQuery::new(
                node(
                    "messages",
                    Where::OR(vec![
                        Where::condition("status", ComparisonOperator::EQ, "pinned"),
                        Where::exists("channelId", 0),
                    ]),
                    limit,
                ),
                vec![Join::inner(
                    MultiTableReadQuery::single(node("channels", Where::AND(Vec::new()), u32::MAX)),
                    "channelId",
                    "id",
                )],
            )
        };
        for (channels, expected) in [(1_001, Driver::Main), (100, Driver::Sub)] {
            let (_, outcome) = drive(
                Planner::new(query(u32::MAX), policy(1_000, Side::Child)),
                &[("messages", 500), ("channels", channels)],
                &[("messages", 10)],
            );
            assert_eq!(
                outcome.expect("planned").joins[0].driver,
                expected,
                "{channels} channels"
            );
        }
        let (_, outcome) = drive(
            Planner::new(query(20), policy(1_000, Side::Child)),
            &[("messages", 1_001), ("channels", 1_001)],
            &[("messages", 10)],
        );
        let planned = outcome.expect("served");
        assert_eq!(planned.joins[0].driver, Driver::Main);
        assert!(page_drives(&planned));
    }

    /// A planned page that drives is reported as such, at any depth.
    #[test]
    fn a_driving_page_is_reported() {
        let (_, outcome) = drive(
            Planner::new(messages_in_channel(50).query, policy(100, Side::Child)),
            &[("messages", 101), ("conversations", 101)],
            &[],
        );
        assert!(page_drives(&outcome.expect("planned")));
        let (_, outcome) = drive(
            Planner::new(messages_in_channel(50).query, policy(100, Side::Child)),
            &[("messages", 101), ("conversations", 5)],
            &[("messages", 30)],
        );
        assert!(!page_drives(&outcome.expect("planned")));
    }

    /// Preferring the parent when it is too big falls back to the child.
    #[test]
    fn a_preferred_parent_too_big_falls_back_to_the_child() {
        let (_, outcome) = drive(
            Planner::new(
                messages_in_channel(u32::MAX).query,
                policy(100, Side::Parent),
            ),
            &[("messages", 101), ("conversations", 5)],
            &[("messages", 50)],
        );
        assert_eq!(outcome.expect("planned").joins[0].driver, Driver::Sub);
    }

    /// A LEFT join's root is read whole: over the limit it is refused,
    /// and the LEFT child, driven by construction, is never counted.
    #[test]
    fn a_big_left_root_is_refused() {
        let users = MultiTableReadQuery::single(node("users", Where::AND(Vec::new()), u32::MAX));
        let query = MultiTableReadQuery::new(
            node("tickets", Where::AND(Vec::new()), u32::MAX),
            vec![Join::left(users, "assigned_to", "id")],
        );
        let (asked, outcome) = drive(
            Planner::new(query, policy(100, Side::Child)),
            &[("tickets", 101)],
            &[],
        );
        assert_eq!(asked, vec!["tickets"]);
        let reason = outcome.expect_err("refused");
        assert!(reason.contains("tickets holds more than 100"), "{reason}");
    }

    /// A paged root with LEFT joins is counted but holds its window: over
    /// the limit it is still served.
    #[test]
    fn a_paged_left_root_is_bounded_by_its_window() {
        let users = MultiTableReadQuery::single(node("users", Where::AND(Vec::new()), u32::MAX));
        let query = MultiTableReadQuery::new(
            node("tickets", Where::AND(Vec::new()), 50),
            vec![Join::left(users, "assigned_to", "id")],
        );
        let (asked, outcome) = drive(
            Planner::new(query, policy(100, Side::Child)),
            &[("tickets", 101)],
            &[],
        );
        assert_eq!(asked, vec!["tickets"]);
        assert!(outcome.is_ok());
    }

    /// A node is decided with all its subs at once: the sub that cuts it
    /// down drives it, the sub over the limit is driven by it, the LEFT
    /// edge and the leaves stay put.
    #[test]
    fn a_node_is_decided_with_all_its_subs_at_once() {
        let channels =
            MultiTableReadQuery::single(node("channels", Where::AND(Vec::new()), u32::MAX));
        let users = MultiTableReadQuery::single(node("users", Where::AND(Vec::new()), u32::MAX));
        let attachments =
            MultiTableReadQuery::single(node("attachments", Where::AND(Vec::new()), u32::MAX));
        let filter = Where::AND(vec![
            Where::exists("channelId", 0),
            Where::exists("senderId", 1),
        ]);
        let query = MultiTableReadQuery::new(
            node("messages", filter.clone(), u32::MAX),
            vec![
                Join::left(attachments, "id", "messageId"),
                Join::inner(channels, "channelId", "id"),
                Join::inner(users, "senderId", "id"),
            ],
        );
        let (asked, outcome) = drive(
            Planner::new(query, policy(100, Side::Child)),
            &[("messages", 40), ("channels", 101), ("users", 5)],
            &[("messages", 20)],
        );
        assert_eq!(asked, vec!["messages", "channels", "users"]);
        let planned = outcome.expect("planned");
        assert_eq!(planned.main_table.filter, filter, "the leaves stay");
        assert_eq!(planned.joins[0].driver, Driver::Main);
        assert!(!planned.joins[0].is_inner, "the LEFT edge is untouched");
        assert_eq!(
            planned.joins[1].driver,
            Driver::Main,
            "channels is too big to drive"
        );
        assert_eq!(
            planned.joins[2].driver,
            Driver::Sub,
            "users drives, cutting the messages to twenty"
        );
    }

    /// Under a LEFT child nothing is compared or counted: the child is
    /// driven by its parent, so it drives its own inner sub, whatever the
    /// sub holds.
    #[test]
    fn a_nested_inner_edge_is_planned_too() {
        let members =
            MultiTableReadQuery::single(node("members", Where::AND(Vec::new()), u32::MAX));
        let project = MultiTableReadQuery::new(
            node("projects", Where::exists("id", 0), u32::MAX),
            vec![Join::inner(members, "id", "projectId")],
        );
        let query = MultiTableReadQuery::new(
            node("tickets", Where::AND(Vec::new()), u32::MAX),
            vec![Join::left(project, "projectId", "id")],
        );
        let (asked, outcome) = drive(
            Planner::new(query, policy(100, Side::Child)),
            &[("tickets", 30), ("members", 101)],
            &[],
        );
        assert_eq!(
            asked,
            vec!["tickets"],
            "the LEFT child is driven, so neither it nor anything below it is counted"
        );
        let planned = outcome.expect("planned");
        assert_eq!(planned.joins[0].sub.joins[0].driver, Driver::Main);
    }

    /// The counts are reported per node, `table alone/narrowed`, a count
    /// past the limit as `>limit`.
    #[test]
    fn the_counts_are_reported() {
        let mut planner = Planner::new(messages_in_channel(50).query, policy(100, Side::Child));
        planner.answer(0, false, 101);
        planner.answer(0, true, 30);
        planner.answer(1, false, 5);
        assert_eq!(planner.report(), "messages >100/30, conversations 5");
    }

    /// The cache answers a repeat, expires, and evicts the least recently
    /// used decision at capacity.
    #[test]
    fn the_cache_remembers_decisions() {
        let cache = PlanCache::new(Duration::from_secs(60), 2);
        let first = messages_in_channel(u32::MAX).query;
        let second = messages_in_channel(50).query;
        let third = messages_in_channel(7).query;
        assert!(cache.get(&first).is_none());
        cache.put(first.clone(), Ok(first.clone()));
        cache.put(second.clone(), Err("no".to_owned()));
        assert_eq!(cache.get(&first), Some(Ok(first.clone())));
        assert_eq!(cache.get(&second), Some(Err("no".to_owned())));
        assert!(cache.get(&first).is_some(), "touch the first again");
        cache.put(third.clone(), Ok(third.clone()));
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&first).is_some(), "recently used, kept");
        assert!(cache.get(&second).is_none(), "least recently used, evicted");

        let brief = PlanCache::new(Duration::from_millis(0), 10);
        brief.put(first.clone(), Ok(first.clone()));
        std::thread::sleep(Duration::from_millis(2));
        assert!(brief.get(&first).is_none(), "expired");
    }
}
