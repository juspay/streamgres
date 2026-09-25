//! Which side of every inner edge drives it, decided before a query
//! registers, and the cache that makes deciding the exception.
//!
//! A node nothing drives is read **whole**: every row it matches sits in
//! memory. A driven node holds only the rows matching its driver's
//! values, and a node with a page (`LIMIT`) holds its window. The planner
//! counts the nodes it may have to compare, no further than the
//! configured limit plus one and all in one batch, and then decides the
//! tree from the root down, one node at a time, by one rule: **the
//! smallest of a node and its inner subs drives**. A side is *measured*
//! when its count is within the limit, and among measured sides the
//! smallest count wins (the preferred side breaks a tie, except that a
//! page never wins one: restricted by its sub, its window stays exact).
//! With no measured side, a side with a page drives, the node's own page
//! before any sub's, reading its window and keeping it to the rows its
//! subs admit (see the `window` module). With none of those, a sub that
//! is itself *narrowed* by one of its own subs drives: an access rule is
//! read from its small end (the reader's participations narrow the
//! channels, the channels the conversations, those the messages). When a
//! sub drives, the node is narrowed by it and drives its other inner
//! subs; when the node drives, it drives them all. A node something
//! drives from above — a LEFT child, the parent of a RIGHT child, the
//! driven side of an inner edge — is narrowed already and drives every
//! inner sub below it without a comparison: its rows are the few its
//! driver's values reach, whatever the table holds, so nothing below it
//! is counted. A node with no side to drive it (over the limit, no page,
//! no narrowed sub) refuses the query, naming the tables. The root never
//! changes: the decision is a `driver` per edge ([`Join::driver`]), so
//! part paths, hidden parts and `EXISTS` leaves all stay where the
//! translation put them, and an inner edge at any depth is planned the
//! same way as one at the root. Two `EXISTS` on one to-one relationship
//! were made one node before any of this (`ast::merge_exists`), so they
//! are counted and compared as one.
//!
//! **A main with a page** is counted like any other node, on its own
//! filter with its cursor, its `EXISTS` leaves taken as true and its
//! `LIMIT` ignored, so a new cursor is a new plan and a page turn costs
//! one capped count on the root. Against a sub that fits too, the smaller
//! of the two drives, with no allowance for the preferred side: a page of
//! one canvas drives the rule's twenty-five thousand participants from the
//! row outwards, and a page over forty thousand channel stats is driven
//! by the few hundred channels it can show. A page whose count is over
//! the limit is still bounded by its window: a sub that fits drives it,
//! restricting the page inside its own filter, and any other sub is
//! driven *by* the page, the page reading its window and the engine
//! keeping it to the rows the sub admits, reaching past the ones it
//! rejects (see the `window` module). A node bounded only because a
//! bounded node drives it is not small in any measured sense (the
//! fan-out is unknown), so it never drives a page.
//!
//! **How a page that drives is read** is the planner's last word
//! ([`MultiTableReadQuery::page`]): a page whose count is within the
//! whole-page limit ([`Policy::whole`]) is read **whole** — every row of
//! its filter in one read, the `LIMIT` applied in memory, the rows its
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
use crate::model::{
    ComparisonOperator, Driver, MultiTableReadQuery, PageRead, SingleTableReadQuery,
};
use crate::sync::Storage;

/// Which side of an inner edge drives when a node and one of its subs
/// are measured equal.
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
/// - `count`: its whole count, once answered; `None` when not asked.
struct PlanNode {
    path: Vec<usize>,
    query: SingleTableReadQuery,
    driven: bool,
    count: Option<u64>,
}

/// One edge of the tree being planned: its ends as node indices, the
/// position of its join under the parent, its driver as translated and
/// whether it is inner.
struct PlanEdge {
    parent: usize,
    child: usize,
    position: usize,
    driver: Driver,
    is_inner: bool,
}

/// How small a side is, smallest first: measured within the limit (by
/// its count), bounded by a page, narrowed by a sub of its own, or none
/// of those.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Rank {
    Measured(u64),
    Paged,
    Narrowed,
    Unbounded,
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

    /// The mode the child of `edge` is decided in when `chosen` is the
    /// edge whose sub drives the parent: the driving sub, and a RIGHT
    /// child (read whole, driving its parent), are compared with their
    /// own subs; every other child is driven.
    fn mode_below(&self, edge: usize, chosen: Option<usize>) -> Mode {
        let e = &self.edges[edge];
        if Some(edge) == chosen || (!e.is_inner && e.driver == Driver::Sub) {
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
                _ => self.mode_below(edge, None),
            };
            self.count_below(self.edges[edge].child, below, out);
        }
    }

    /// The counts the planner wants, all at once: for each node it may
    /// compare, its index, the query to count (the node's own filter,
    /// cursor included, with its `EXISTS` leaves taken as true and no
    /// `LIMIT`) and the cap.
    pub fn counts(&self) -> Vec<(usize, SingleTableReadQuery, u64)> {
        self.counted
            .iter()
            .map(|&index| {
                let node = &self.nodes[index];
                let query = SingleTableReadQuery {
                    filter: node.query.filter.assuming_true(&|leaf| {
                        leaf.comparison_operator == ComparisonOperator::EXISTS
                    }),
                    limit: u32::MAX,
                    ..node.query.clone()
                };
                (index, query, self.policy.limit + 1)
            })
            .collect()
    }

    /// The table of the node `index`, for messages and logs.
    pub fn table_of(&self, index: usize) -> &str {
        self.nodes[index].query.table.as_str()
    }

    /// The answer to one of the counts.
    pub fn answer(&mut self, index: usize, count: u64) {
        if let Some(node) = self.nodes.get_mut(index) {
            node.count = Some(count);
        }
    }

    /// Decide every inner edge's driver from the counts in, or refuse:
    /// the tree from the root down, each node by the rule in the module
    /// docs (the smallest of the node and its inner subs drives, the rest
    /// are driven; a node narrowed from above drives them all). Every
    /// page left driving an inner edge is then marked read whole when its
    /// count is within the whole-page limit, in batches otherwise.
    pub fn decide(mut self) -> Result<MultiTableReadQuery, String> {
        if self.counted.is_empty() {
            return Ok(self.query);
        }
        let ranks = self.ranks();
        let mut drivers: Vec<Option<Driver>> = vec![None; self.edges.len()];
        let mut over: Vec<usize> = Vec::new();
        self.decide_node(0, Mode::Compare, &ranks, &mut drivers, &mut over);
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
                    let read = match node.count {
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

    /// How small every node is, subs before their parents: measured when
    /// its count is within the limit, bounded by its page when it has
    /// one, narrowed when one of its inner subs, or a RIGHT child, is
    /// itself bounded, unbounded otherwise.
    fn ranks(&self) -> Vec<Rank> {
        let limit = self.policy.limit;
        let mut ranks = vec![Rank::Unbounded; self.nodes.len()];
        for index in (0..self.nodes.len()).rev() {
            let node = &self.nodes[index];
            let narrowed = self.edges_of(index).any(|(_, edge)| {
                (edge.is_inner || edge.driver == Driver::Sub)
                    && ranks[edge.child] != Rank::Unbounded
            });
            ranks[index] = match node.count {
                Some(count) if count <= limit => Rank::Measured(count),
                _ if node.query.limit != u32::MAX => Rank::Paged,
                _ if narrowed => Rank::Narrowed,
                _ => Rank::Unbounded,
            };
        }
        ranks
    }

    /// Decide the node `index` in `mode` and every node below it: the
    /// driver of each inner edge under it into `drivers`, the nodes no
    /// side can drive into `over`.
    fn decide_node(
        &self,
        index: usize,
        mode: Mode,
        ranks: &[Rank],
        drivers: &mut [Option<Driver>],
        over: &mut Vec<usize>,
    ) {
        let mode = self.mode_of(index, mode);
        let inner: Vec<usize> = self
            .edges_of(index)
            .filter(|(_, edge)| edge.is_inner)
            .map(|(edge, _)| edge)
            .collect();
        let chosen = match mode {
            Mode::Driven => None,
            Mode::Compare => self.smallest(index, &inner, ranks, over),
        };
        for &edge in &inner {
            drivers[edge] = Some(if Some(edge) == chosen {
                Driver::Sub
            } else {
                Driver::Main
            });
        }
        let children: Vec<usize> = self.edges_of(index).map(|(edge, _)| edge).collect();
        for edge in children {
            let below = self.mode_below(edge, chosen);
            self.decide_node(self.edges[edge].child, below, ranks, drivers, over);
        }
    }

    /// Among the node `index` and its inner subs (`inner`, as edges),
    /// the side that drives: `None` for the node itself, `Some(edge)`
    /// for a sub. The smallest rank wins; a tie between the node and a
    /// sub goes to the sub when the node is a page (restricted, its
    /// window stays exact) and to the preferred side otherwise, and a
    /// tie between subs to the first. With no side bounded the node and
    /// its unbounded subs are recorded in `over` and the node is left
    /// driving.
    fn smallest(
        &self,
        index: usize,
        inner: &[usize],
        ranks: &[Rank],
        over: &mut Vec<usize>,
    ) -> Option<usize> {
        let own = match ranks[index] {
            rank @ (Rank::Measured(_) | Rank::Paged) => Some(rank),
            Rank::Narrowed | Rank::Unbounded => None,
        };
        let best = inner
            .iter()
            .copied()
            .filter(|&edge| ranks[self.edges[edge].child] != Rank::Unbounded)
            .min_by_key(|&edge| ranks[self.edges[edge].child]);
        match (own, best) {
            (None, None) => {
                over.push(index);
                over.extend(
                    inner
                        .iter()
                        .map(|&edge| self.edges[edge].child)
                        .filter(|&child| ranks[child] == Rank::Unbounded),
                );
                None
            }
            (Some(_), None) => None,
            (None, Some(edge)) => Some(edge),
            (Some(own), Some(edge)) => {
                let sub = ranks[self.edges[edge].child];
                let paged = self.nodes[index].query.limit != u32::MAX;
                let sub_wins = match sub.cmp(&own) {
                    std::cmp::Ordering::Less => true,
                    std::cmp::Ordering::Greater => false,
                    std::cmp::Ordering::Equal => match own {
                        Rank::Measured(_) => paged || self.policy.preferred == Side::Child,
                        _ => false,
                    },
                };
                sub_wins.then_some(edge)
            }
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
                over.contains(index) && node.count.is_some_and(|count| count > limit)
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
        count: None,
    });
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
        edges.push(PlanEdge {
            parent: index,
            child,
            position,
            driver: join.driver,
            is_inner: join.is_inner,
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
/// query, and the next asking counts again.
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
    let answers = join_all(wanted.iter().map(|(_, count_query, cap)| async move {
        let started = Instant::now();
        let answer = storage.count(count_query, *cap).await;
        if let Some(stats) = crate::stats::Stats::global() {
            stats.count_io.record(started.elapsed());
        }
        answer
    }))
    .await;
    let mut failed = None;
    for ((index, _, _), answer) in wanted.iter().zip(answers) {
        match answer {
            Ok(count) => planner.answer(*index, count),
            Err(error) => {
                failed = Some(match error.refusal() {
                    Some(reason) => reason.to_owned(),
                    None => format!(
                        "counting the rows of {} failed: {error}",
                        planner.table_of(*index)
                    ),
                });
                break;
            }
        }
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

    /// Drive `planner` with `answers` by table name, returning the tables
    /// it counted (in the order it asked) and its decision.
    fn drive(
        mut planner: Planner,
        answers: &[(&str, u64)],
    ) -> (Vec<String>, Result<MultiTableReadQuery, String>) {
        let wanted = planner.counts();
        let asked: Vec<String> = wanted
            .iter()
            .map(|(_, query, _)| query.table.to_string())
            .collect();
        for (index, query, cap) in &wanted {
            assert_eq!(
                *cap,
                planner.policy.limit + 1,
                "the cap is the limit plus one"
            );
            let count = answers
                .iter()
                .find(|(table, _)| *table == query.table.as_str())
                .map(|(_, count)| *count)
                .unwrap_or_else(|| panic!("no answer for {}", query.table));
            planner.answer(*index, count);
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

    /// A page that drives is read whole when its count is within the
    /// whole-page limit and in batches otherwise; a page a sub drives, and
    /// a node without a page, are left as translated.
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
            );
            let planned = outcome.expect("planned");
            assert_eq!(planned.joins[0].driver, Driver::Main, "the page drives");
            assert_eq!(planned.page, expected, "a page of {messages} rows");
        }
        let (_, outcome) = drive(
            Planner::new(messages_in_channel(50).query, whole),
            &[("messages", 5), ("conversations", 3)],
        );
        let planned = outcome.expect("planned");
        assert_eq!(planned.joins[0].driver, Driver::Sub, "the sub drives");
        assert_eq!(planned.page, PageRead::Batched, "left as translated");
        let (_, outcome) = drive(
            Planner::new(messages_in_channel(u32::MAX).query, whole),
            &[("messages", 5), ("conversations", 101)],
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
        let (asked, outcome) = drive(Planner::new(plain.clone(), policy(10, Side::Child)), &[]);
        assert!(asked.is_empty());
        assert_eq!(outcome.expect("kept"), plain);
    }

    /// Both sides are counted in one batch; under the limit, the tree
    /// stays as translated (the sub drives, as `whereExists` does).
    #[test]
    fn a_small_child_keeps_the_tree() {
        let translated = messages_in_channel(u32::MAX);
        let (asked, outcome) = drive(
            Planner::new(translated.query.clone(), policy(100, Side::Child)),
            &[("messages", 40), ("conversations", 7)],
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
        );
        assert_eq!(asked, vec!["messages", "conversations"]);
        let planned = outcome.expect("planned");
        assert_eq!(planned.main_table, translated.query.main_table);
        assert_eq!(planned.joins[0].driver, Driver::Main);
        assert!(planned.joins[0].is_inner);
        assert_eq!(planned.joins[0].sub, translated.query.joins[0].sub);
    }

    /// Both sides over the limit: refused, naming both.
    #[test]
    fn two_big_sides_are_refused() {
        let (_, outcome) = drive(
            Planner::new(
                messages_in_channel(u32::MAX).query,
                policy(100, Side::Child),
            ),
            &[("messages", 101), ("conversations", 101)],
        );
        let reason = outcome.expect_err("refused");
        assert!(
            reason.contains("conversations holds more than 100"),
            "{reason}"
        );
        assert!(reason.contains("messages holds more than 100"), "{reason}");
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
    /// conversations unfiltered and huge, the channels few. The page
    /// drives the conversations, and the conversations, driven from above,
    /// drive the channels in turn: every link is read narrowed from the
    /// row outwards. The channels never drive the conversations (every
    /// conversation of every channel) into the page.
    #[test]
    fn a_page_is_not_driven_through_an_unmeasured_fan_out() {
        let (asked, outcome) = drive(
            Planner::new(message_under_the_access_rule(1), policy(100, Side::Parent)),
            &[("messages", 1), ("conversations", 101), ("channels", 20)],
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

    /// The same rule under an unpaged root that fits: a node driven from
    /// above drives the edges below it, however much smaller the sub's
    /// own count is than the node's (a conversation's messages reach one
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

    /// With nothing small at the root the rule's own side is all there is:
    /// the channels drive the conversations and those the messages (and
    /// the read limit is what stops it if the fan-out is large).
    #[test]
    fn a_big_root_is_driven_from_the_small_end_of_the_chain() {
        let (_, outcome) = drive(
            Planner::new(
                message_under_the_access_rule(u32::MAX),
                policy(100, Side::Parent),
            ),
            &[("messages", 101), ("conversations", 101), ("channels", 3)],
        );
        let planned = outcome.expect("planned");
        assert_eq!(planned.joins[0].driver, Driver::Sub);
        assert_eq!(planned.joins[0].sub.joins[0].driver, Driver::Sub);
    }

    /// A paged root whose count is over the limit is driven by a child
    /// that fits, whichever side is preferred: the child restricts the
    /// page inside its own filter.
    #[test]
    fn a_paged_root_is_driven_by_a_small_child() {
        for preferred in [Side::Parent, Side::Child] {
            let (_, outcome) = drive(
                Planner::new(messages_in_channel(50).query, policy(100, preferred)),
                &[("messages", 101), ("conversations", 5)],
            );
            assert_eq!(
                outcome.expect("planned").joins[0].driver,
                Driver::Sub,
                "the page is over the limit, so the child drives ({preferred:?})"
            );
        }
    }

    /// With both a page and its child counted within the limit, the
    /// smaller drives, strictly: a page gets no preferred-side allowance,
    /// so a page of three rows drives a child of five even when the child
    /// is preferred, and a page of fifty is driven by that child.
    #[test]
    fn a_page_and_a_fitting_child_compare_strictly() {
        for (messages, conversations, expected) in [
            (3, 5, Driver::Main),
            (50, 5, Driver::Sub),
            (5, 5, Driver::Sub),
        ] {
            for preferred in [Side::Parent, Side::Child] {
                let (_, outcome) = drive(
                    Planner::new(messages_in_channel(50).query, policy(100, preferred)),
                    &[("messages", messages), ("conversations", conversations)],
                );
                assert_eq!(
                    outcome.expect("planned").joins[0].driver,
                    expected,
                    "a page of {messages} against {conversations}, preferring {preferred:?}"
                );
            }
        }
    }

    /// `canvases WHERE id = ? LIMIT 1` under the canvas rule as the rig
    /// holds it: the participants (an `EXISTS` inside an `OR`, so their
    /// count is the whole table, 25 100) reach user groups and channels
    /// (44 066) and the channels their memberships; a second `EXISTS` on
    /// users. With the page counted at one row, every inner edge is
    /// driven from the page outwards: nothing large is ever read whole.
    #[test]
    fn a_one_row_page_drives_the_canvas_rule_from_the_row_outwards() {
        let memberships = MultiTableReadQuery::single(node(
            "channel_participants",
            Where::condition("userId", ComparisonOperator::EQ, "me"),
            u32::MAX,
        ));
        let channels = MultiTableReadQuery::new(
            node("channels", Where::exists("id", 0), u32::MAX),
            vec![Join::inner(memberships, "id", "channelId")],
        );
        let groups = MultiTableReadQuery::single(node(
            "user_groups",
            Where::condition("userId", ComparisonOperator::EQ, "me"),
            u32::MAX,
        ));
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
        let query = MultiTableReadQuery::new(
            node(
                "canvases",
                Where::AND(vec![
                    Where::condition("id", ComparisonOperator::EQ, "cv1"),
                    Where::OR(vec![
                        Where::condition("createdBy", ComparisonOperator::EQ, "me"),
                        Where::exists("id", 0),
                        Where::condition("visibility", ComparisonOperator::EQ, "PUBLIC"),
                    ]),
                    Where::exists("createdBy", 1),
                ]),
                1,
            ),
            vec![
                Join::inner(participants, "id", "canvasId"),
                Join::inner(users, "createdBy", "id"),
            ],
        );
        let (asked, outcome) = drive(
            Planner::new(query, policy(20_000, Side::Parent)),
            &[
                ("canvases", 1),
                ("canvas_participants", 20_001),
                ("user_groups", 42),
                ("channels", 20_001),
                ("channel_participants", 52),
                ("users", 2_000),
            ],
        );
        assert_eq!(
            asked,
            vec![
                "canvases",
                "canvas_participants",
                "user_groups",
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
    }

    /// The preferred side gets no allowance: the smaller side drives
    /// however slightly smaller it is, and the preferred side decides a
    /// tie only.
    #[test]
    fn the_preferred_side_breaks_a_tie_only() {
        for (messages, conversations, preferred, expected) in [
            (40, 30, Side::Parent, Driver::Sub),
            (30, 30, Side::Parent, Driver::Main),
            (30, 30, Side::Child, Driver::Sub),
        ] {
            let (_, outcome) = drive(
                Planner::new(messages_in_channel(u32::MAX).query, policy(100, preferred)),
                &[("messages", messages), ("conversations", conversations)],
            );
            assert_eq!(
                outcome.expect("planned").joins[0].driver,
                expected,
                "{messages} messages, {conversations} conversations, preferring {preferred:?}"
            );
        }
    }

    /// A node and all its inner subs are compared at once: the smallest
    /// of them drives, and every other sub is driven by the node, however
    /// small it is by itself.
    #[test]
    fn the_smallest_of_a_node_and_its_subs_drives_and_the_rest_are_driven() {
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
        for (channels, users, expected) in [
            (10, 5, [Driver::Main, Driver::Sub]),
            (10, 30, [Driver::Sub, Driver::Main]),
            (50, 60, [Driver::Main, Driver::Main]),
        ] {
            let (_, outcome) = drive(
                Planner::new(query.clone(), policy(100, Side::Parent)),
                &[("messages", 40), ("channels", channels), ("users", users)],
            );
            let planned = outcome.expect("planned");
            assert_eq!(
                [planned.joins[0].driver, planned.joins[1].driver],
                expected,
                "40 messages, {channels} channels, {users} users"
            );
        }
    }

    /// A planned page that drives is reported as such, at any depth.
    #[test]
    fn a_driving_page_is_reported() {
        let (_, outcome) = drive(
            Planner::new(messages_in_channel(50).query, policy(100, Side::Child)),
            &[("messages", 101), ("conversations", 101)],
        );
        assert!(page_drives(&outcome.expect("planned")));
        let (_, outcome) = drive(
            Planner::new(messages_in_channel(50).query, policy(100, Side::Child)),
            &[("messages", 101), ("conversations", 5)],
        );
        assert!(!page_drives(&outcome.expect("planned")));
    }

    /// With both sides measured the smaller one drives, whichever is
    /// preferred: a project's few boards drive the workspace's stages, a
    /// conversation's few messages drive the channels of the access rule;
    /// equal counts go to the preferred side.
    #[test]
    fn the_smaller_side_drives() {
        for (messages, conversations, preferred, expected) in [
            (90, 5, Side::Parent, Driver::Sub),
            (90, 46, Side::Parent, Driver::Sub),
            (5, 90, Side::Parent, Driver::Main),
            (5, 90, Side::Child, Driver::Main),
            (46, 90, Side::Child, Driver::Main),
            (90, 5, Side::Child, Driver::Sub),
            (0, 0, Side::Parent, Driver::Main),
            (0, 0, Side::Child, Driver::Sub),
        ] {
            let (_, outcome) = drive(
                Planner::new(messages_in_channel(u32::MAX).query, policy(100, preferred)),
                &[("messages", messages), ("conversations", conversations)],
            );
            assert_eq!(
                outcome.expect("planned").joins[0].driver,
                expected,
                "{messages} messages, {conversations} conversations, preferring {preferred:?}"
            );
        }
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
        );
        assert_eq!(asked, vec!["tickets"]);
        assert!(outcome.is_ok());
    }

    /// A node is decided with all its subs at once: the smallest sub
    /// drives it, the big sub is driven by it, the LEFT edge and the
    /// leaves stay put.
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
            "users drives, as preferred"
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
        );
        assert_eq!(
            asked,
            vec!["tickets"],
            "the LEFT child is driven, so neither it nor anything below it is counted"
        );
        let planned = outcome.expect("planned");
        assert_eq!(planned.joins[0].sub.joins[0].driver, Driver::Main);
    }

    /// The count query takes every `EXISTS` leaf as true: `OR(x, EXISTS)`
    /// drops out, a bare AND of leaves keeps the others.
    #[test]
    fn counts_take_exists_as_true() {
        let filter = Where::AND(vec![
            Where::condition("status", ComparisonOperator::EQ, "OPEN"),
            Where::OR(vec![
                Where::condition("visibility", ComparisonOperator::EQ, "PUBLIC"),
                Where::exists("id", 0),
            ]),
        ]);
        let users = MultiTableReadQuery::single(node("users", Where::AND(Vec::new()), u32::MAX));
        let query = MultiTableReadQuery::new(
            node("tickets", filter, u32::MAX),
            vec![Join::inner(users, "id", "ticketId")],
        );
        let planner = Planner::new(query, policy(100, Side::Child));
        let counts = planner.counts();
        assert_eq!(
            counts[0].1.filter,
            Where::AND(vec![Where::condition(
                "status",
                ComparisonOperator::EQ,
                "OPEN"
            )])
        );
        assert_eq!(counts[1].1.filter, Where::AND(Vec::new()));
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
