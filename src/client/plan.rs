//! Which side of every inner edge drives it, decided before a query
//! registers, and the cache that makes deciding the exception.
//!
//! A node nothing drives is read **whole**: every row it matches sits in
//! memory. A driven node holds only the rows matching its driver's
//! values, and a node with a page (`LIMIT`) holds its window. The planner
//! counts each node that would be read whole, no further than the
//! configured limit plus one and all in one batch, and then settles the
//! inner edges by a fixed point: a node is *bounded* when its count is
//! within the limit, when it has a page, or when it is the driven side of
//! an edge whose driver is bounded. An inner edge whose sides both fit by
//! their own counts is driven from the **smaller** one (the preferred
//! side winning unless the other is at most half its size: a project's
//! few boards drive the workspace's stages, not the reverse); an edge
//! with one bounded side is driven from it; an edge left with none
//! refuses the query. The root never changes: the decision is a `driver`
//! per edge ([`Join::driver`]), so part paths, hidden parts and `EXISTS`
//! leaves all stay where the translation put them, and an inner edge at
//! any depth is planned the same way as one at the root.
//!
//! **A main with a page.** A sub that fits by its own count drives it:
//! its values restrict the page inside the page's own filter, so the
//! window is exact and reads only rows the sub admits (a channel's
//! conversations driving the page of its attachments). Any other sub is
//! driven *by* the page: the page reads its window, the sub is narrowed
//! to the window's values, and the engine keeps the page to the rows the
//! sub admits, reaching past the ones it rejects (see the `window`
//! module). That is the plan for `WHERE id = ? … LIMIT 1` under an access
//! rule that walks `conversations`: four small reads from the row
//! outwards, where driving from the rule's side reads every conversation
//! of every channel the user may see. A node bounded only because a
//! bounded node drives it is not small in any measured sense (the
//! fan-out is unknown), so it never drives a page.
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
use crate::model::{ComparisonOperator, Driver, MultiTableReadQuery, SingleTableReadQuery};
use crate::sync::Storage;

/// Which side of an inner edge to prefer when both are small enough to
/// read whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Child,
    Parent,
}

/// The planner's settings: the most rows a whole node may hold (zero
/// turns planning off), and the side to prefer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    pub limit: u64,
    pub preferred: Side,
}

/// One node of the tree being planned.
///
/// - `path`: the join positions from the root to the node.
/// - `query`: the node's own query.
/// - `driven`: whether an outer edge drives the node (a LEFT child, a
///   RIGHT parent), which bounds it by its driver and spares it a count.
/// - `count`: its whole count, once answered; `None` when not asked.
struct PlanNode {
    path: Vec<usize>,
    query: SingleTableReadQuery,
    driven: bool,
    count: Option<u64>,
}

/// One edge of the tree being planned: its ends as node indices, the
/// position of its join under the parent, and, for an inner edge, the
/// driver decided so far.
struct PlanEdge {
    parent: usize,
    child: usize,
    position: usize,
    driver: Driver,
    is_inner: bool,
    decided: bool,
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
        let counted = if policy.limit == 0 || !query.has_joins() {
            Vec::new()
        } else {
            nodes
                .iter()
                .enumerate()
                .filter(|(_, node)| !node.driven && node.query.limit == u32::MAX)
                .map(|(index, _)| index)
                .collect()
        };
        Planner {
            query,
            policy,
            nodes,
            edges,
            counted,
        }
    }

    /// The counts the planner wants, all at once: for each node that
    /// would be read whole, its index, the query to count (the node's own
    /// filter with its `EXISTS` leaves taken as true) and the cap.
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

    /// Decide every inner edge's driver from the counts in, or refuse.
    pub fn decide(mut self) -> Result<MultiTableReadQuery, String> {
        if self.counted.is_empty() {
            return Ok(self.query);
        }
        let limit = self.policy.limit;
        let paged: Vec<bool> = self
            .nodes
            .iter()
            .map(|node| node.query.limit != u32::MAX)
            .collect();
        let counts: Vec<Option<u64>> = self
            .nodes
            .iter()
            .map(|node| node.count.filter(|count| *count <= limit))
            .collect();
        let mut bounded: Vec<bool> = (0..self.nodes.len())
            .map(|index| paged[index] || counts[index].is_some())
            .collect();
        loop {
            let mut changed = false;
            for edge in self.edges.iter_mut() {
                if !edge.is_inner || edge.decided {
                    let (from, to) = match edge.driver {
                        Driver::Main => (edge.parent, edge.child),
                        Driver::Sub => (edge.child, edge.parent),
                    };
                    if bounded[from] && !bounded[to] {
                        bounded[to] = true;
                        changed = true;
                    }
                    continue;
                }
                let (parent, child) = (edge.parent, edge.child);
                let chosen = if paged[parent] {
                    Some(if counts[child].is_some() || paged[child] {
                        Driver::Sub
                    } else {
                        Driver::Main
                    })
                } else {
                    match (bounded[parent], bounded[child]) {
                        (true, false) => Some(Driver::Main),
                        (false, true) => Some(Driver::Sub),
                        (true, true) => Some(match (counts[parent], counts[child]) {
                            (Some(main), Some(sub)) => smaller(main, sub, self.policy.preferred),
                            (Some(_), None) if !paged[child] => Driver::Main,
                            _ => match self.policy.preferred {
                                Side::Parent => Driver::Main,
                                Side::Child => Driver::Sub,
                            },
                        }),
                        (false, false) => None,
                    }
                };
                if let Some(driver) = chosen {
                    edge.driver = driver;
                    edge.decided = true;
                    let to = match driver {
                        Driver::Main => child,
                        Driver::Sub => parent,
                    };
                    if !bounded[to] {
                        bounded[to] = true;
                    }
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        let undecided: Vec<&PlanEdge> = self
            .edges
            .iter()
            .filter(|edge| edge.is_inner && !edge.decided)
            .collect();
        let over: Vec<usize> = (0..self.nodes.len())
            .filter(|&index| !bounded[index])
            .collect();
        if !undecided.is_empty() || !over.is_empty() {
            return Err(self.refusal(&over));
        }
        let decisions: Vec<(Vec<usize>, usize, Driver)> = self
            .edges
            .iter()
            .filter(|edge| edge.is_inner)
            .map(|edge| {
                (
                    self.nodes[edge.parent].path.clone(),
                    edge.position,
                    edge.driver,
                )
            })
            .collect();
        for (path, position, driver) in decisions {
            if let Some(join) = join_at(&mut self.query, &path, position) {
                join.driver = driver;
            }
        }
        Ok(self.query)
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

/// Which side of an inner edge drives it when both fit by their own
/// counts: the smaller one, the preferred side winning unless the other
/// is at most half its size.
fn smaller(main: u64, sub: u64, preferred: Side) -> Driver {
    match preferred {
        Side::Parent if sub < main && sub.saturating_mul(2) <= main => Driver::Sub,
        Side::Parent => Driver::Main,
        Side::Child if main < sub && main.saturating_mul(2) <= sub => Driver::Main,
        Side::Child => Driver::Sub,
    }
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
            decided: !join.is_inner,
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
/// planner's decision, remembered in the cache. A count that fails
/// refuses the query, saying so.
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
                failed = Some(format!(
                    "counting the rows of {} failed: {error}",
                    planner.table_of(*index)
                ));
                break;
            }
        }
    }
    let outcome = match failed {
        Some(reason) => Err(reason),
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

    /// The policy with `limit`, preferring `preferred`.
    fn policy(limit: u64, preferred: Side) -> Policy {
        Policy { limit, preferred }
    }

    /// A query without joins, or a limit of zero, is registered as it is
    /// with nothing counted.
    #[test]
    fn nothing_to_plan_without_joins_or_a_limit() {
        let plain = MultiTableReadQuery::single(node("tickets", Where::AND(Vec::new()), u32::MAX));
        let (asked, outcome) = drive(Planner::new(plain.clone(), policy(10, Side::Child)), &[]);
        assert!(asked.is_empty());
        assert_eq!(outcome.expect("kept"), plain);

        let (asked, outcome) = drive(
            Planner::new(messages_in_channel(u32::MAX).query, policy(0, Side::Child)),
            &[],
        );
        assert!(asked.is_empty(), "a limit of zero turns planning off");
        assert!(outcome.is_ok());
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

    /// A root with a page is never counted; a child too big to restrict
    /// it is driven by the page (the engine keeps the page to the rows the
    /// child admits).
    #[test]
    fn a_paged_root_drives_a_big_child() {
        let (asked, outcome) = drive(
            Planner::new(messages_in_channel(50).query, policy(100, Side::Child)),
            &[("conversations", 101)],
        );
        assert_eq!(asked, vec!["conversations"]);
        assert_eq!(outcome.expect("planned").joins[0].driver, Driver::Main);
    }

    /// The access rule of a message: `messages WHERE id = ? LIMIT 1` under
    /// `EXISTS conversations (EXISTS channels)`, the conversations
    /// unfiltered and huge, the channels few. The channels may drive the
    /// conversations, but conversations bounded only that way (every
    /// conversation of every channel) never drive the page: the page
    /// drives them.
    #[test]
    fn a_page_is_not_driven_through_an_unmeasured_fan_out() {
        let channels = MultiTableReadQuery::single(node(
            "channels",
            Where::condition("workspaceId", ComparisonOperator::EQ, "w"),
            u32::MAX,
        ));
        let conversations = MultiTableReadQuery::new(
            node("conversations", Where::exists("channelId", 0), u32::MAX),
            vec![Join::inner(channels, "channelId", "id")],
        );
        let query = MultiTableReadQuery::new(
            node(
                "messages",
                Where::AND(vec![
                    Where::condition("id", ComparisonOperator::EQ, "m1"),
                    Where::exists("conversationId", 0),
                ]),
                1,
            ),
            vec![Join::inner(
                conversations,
                "conversationId",
                "conversationId",
            )],
        );
        let (asked, outcome) = drive(
            Planner::new(query, policy(100, Side::Parent)),
            &[("conversations", 101), ("channels", 20)],
        );
        assert_eq!(asked, vec!["conversations", "channels"]);
        let planned = outcome.expect("planned");
        assert_eq!(planned.joins[0].driver, Driver::Main, "the page drives");
        assert_eq!(
            planned.joins[0].sub.joins[0].driver,
            Driver::Sub,
            "the few channels still narrow the conversations"
        );
    }

    /// A paged root with a small child keeps the child driving.
    #[test]
    fn a_paged_root_is_driven_by_a_small_child() {
        let (_, outcome) = drive(
            Planner::new(messages_in_channel(50).query, policy(100, Side::Parent)),
            &[("conversations", 5)],
        );
        assert_eq!(
            outcome.expect("planned").joins[0].driver,
            Driver::Sub,
            "the preferred parent has a page, so the child drives"
        );
    }

    /// Preferring the parent flips the edge when both sides fit and the
    /// child is not much the smaller.
    #[test]
    fn a_preferred_parent_that_fits_drives() {
        let (_, outcome) = drive(
            Planner::new(
                messages_in_channel(u32::MAX).query,
                policy(100, Side::Parent),
            ),
            &[("messages", 40), ("conversations", 30)],
        );
        assert_eq!(outcome.expect("planned").joins[0].driver, Driver::Main);
    }

    /// A planned page that drives is reported as such, at any depth.
    #[test]
    fn a_driving_page_is_reported() {
        let (_, outcome) = drive(
            Planner::new(messages_in_channel(50).query, policy(100, Side::Child)),
            &[("conversations", 101)],
        );
        assert!(page_drives(&outcome.expect("planned")));
        let (_, outcome) = drive(
            Planner::new(messages_in_channel(50).query, policy(100, Side::Child)),
            &[("conversations", 5)],
        );
        assert!(!page_drives(&outcome.expect("planned")));
    }

    /// With both sides measured the smaller one drives, whichever is
    /// preferred: a project's few boards drive the workspace's stages, a
    /// conversation's few messages drive the channels of the access rule.
    #[test]
    fn the_smaller_side_drives() {
        for (messages, conversations, preferred, expected) in [
            (90, 5, Side::Parent, Driver::Sub),
            (90, 46, Side::Parent, Driver::Main),
            (5, 90, Side::Parent, Driver::Main),
            (5, 90, Side::Child, Driver::Main),
            (46, 90, Side::Child, Driver::Sub),
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

    /// A paged root with LEFT joins holds its window and is never
    /// counted; nothing is asked.
    #[test]
    fn a_paged_left_root_is_bounded_by_its_window() {
        let users = MultiTableReadQuery::single(node("users", Where::AND(Vec::new()), u32::MAX));
        let query = MultiTableReadQuery::new(
            node("tickets", Where::AND(Vec::new()), 50),
            vec![Join::left(users, "assigned_to", "id")],
        );
        let (asked, outcome) = drive(Planner::new(query, policy(100, Side::Child)), &[]);
        assert!(asked.is_empty());
        assert!(outcome.is_ok());
    }

    /// Each inner edge is decided on its own: the big child's edge flips
    /// to the parent, the small child's keeps the child, the LEFT edge and
    /// the leaves stay put.
    #[test]
    fn edges_are_decided_one_by_one_in_place() {
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

    /// An inner edge below the root is planned like one at the root: the
    /// nested big child's edge flips to its (driven, so bounded) parent.
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
            vec!["tickets", "members"],
            "the LEFT child is driven and not counted; its inner child is"
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
