//! Which side of a join is read whole, decided before a query registers.
//!
//! Every tree node that nothing drives is read **whole**: the root when no
//! RIGHT or INNER child restricts it, and a RIGHT or INNER child with no
//! such children of its own. A whole node's rows all sit in memory, so
//! its size is the query's cost; a driven node holds only what its driver
//! references, and a node with a page (`LIMIT`) holds its window. The
//! planner counts each whole node without a page, no further than the
//! configured limit plus one, and refuses the query when one of them is
//! over the limit — unless an INNER edge at the root can be **turned
//! around**: the child becomes the root and the parent its INNER child,
//! so the parent is read whole and the child narrowed to the parent's
//! keys, the same rows shown either way. Which side to try first is the
//! preferred side; a root with a page cannot be turned (its window is
//! its own), so such a query is driven from its children or refused.
//!
//! The planner does no I/O: it hands out one count at a time and takes
//! the answer back, so the caller runs the counts wherever it can (the
//! engine side answers them on its snapshot) and the decision stays a
//! pure function that tests can drive with numbers.

use std::collections::HashSet;

use super::ast::Translated;
use crate::ivm::QueryPart;
use crate::model::{
    ComparisonOperator, Condition, Join, MultiTableReadQuery, SingleTableReadQuery, Value, Where,
};

/// Which side of an INNER edge to count first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Child,
    Parent,
}

/// The planner's settings: the most rows a whole node may hold (zero
/// turns planning off), and the side to try first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    pub limit: u64,
    pub preferred: Side,
}

/// What the planner needs next: a count, or its decision.
#[derive(Debug)]
pub enum Step {
    Count(SingleTableReadQuery, u64),
    Done(Result<Translated, String>),
}

/// One node counted as if read whole.
///
/// - `query`: the count query, the node's own filter with its `EXISTS`
///   leaves taken as true.
/// - `root_inner`: the root's INNER join this node is the child of, when
///   it is one (the edges that can be turned).
#[derive(Debug, Clone)]
struct Probe {
    query: SingleTableReadQuery,
    root_inner: Option<usize>,
    count: Option<u64>,
}

/// Where the planner is between counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Counting the whole nodes of the tree as translated; an over-limit
    /// root INNER child turns the plan towards the parent.
    Children,
    /// Counting the root as if it drove an INNER edge.
    Root,
    /// Counting whatever whole nodes are still uncounted, to verify the
    /// turned shape.
    Rest,
    Finished,
}

/// The planner for one translated query.
#[derive(Debug)]
pub struct Planner {
    translated: Translated,
    policy: Policy,
    wholes: Vec<Probe>,
    root: Option<Probe>,
    turn: Option<usize>,
    phase: Phase,
    outcome: Option<Result<Translated, String>>,
}

impl Planner {
    /// A planner for `translated` under `policy`; the first call to
    /// [`Planner::step`] says what it needs.
    pub fn new(translated: Translated, policy: Policy) -> Self {
        let query = &translated.query;
        let has_joins = !(query.left_joins.is_empty()
            && query.right_joins.is_empty()
            && query.inner_joins.is_empty());
        let mut wholes = Vec::new();
        let mut root = None;
        if policy.limit > 0 && has_joins {
            collect_wholes(query, true, &mut wholes);
            if !query.inner_joins.is_empty() {
                root = Some(Probe {
                    query: SingleTableReadQuery {
                        filter: assume_exists_true(&query.main_table.filter),
                        limit: u32::MAX,
                        ..query.main_table.clone()
                    },
                    root_inner: None,
                    count: None,
                });
            }
        }
        let mut planner = Planner {
            translated,
            policy,
            wholes,
            root,
            turn: None,
            phase: Phase::Children,
            outcome: None,
        };
        if policy.limit == 0 || !has_joins {
            planner.finish(Ok(()));
        } else if policy.preferred == Side::Parent && planner.can_turn() {
            planner.phase = Phase::Root;
        }
        planner
    }

    /// What the planner needs next; after [`Step::Done`] the planner is
    /// spent.
    pub fn step(&mut self) -> Step {
        loop {
            if let Some(outcome) = self.outcome.take() {
                self.phase = Phase::Finished;
                return Step::Done(outcome);
            }
            let cap = self.policy.limit + 1;
            match self.phase {
                Phase::Children | Phase::Rest => {
                    match self.wholes.iter().find(|probe| probe.count.is_none()) {
                        Some(probe) => return Step::Count(probe.query.clone(), cap),
                        None => self.settle(),
                    }
                }
                Phase::Root => match self.root.as_ref().filter(|probe| probe.count.is_none()) {
                    Some(probe) => return Step::Count(probe.query.clone(), cap),
                    None => self.settle(),
                },
                Phase::Finished => {
                    return Step::Done(Err("the planner was already done".to_owned()));
                }
            }
        }
    }

    /// The answer to the count [`Planner::step`] last asked for.
    pub fn answer(&mut self, count: u64) {
        let limit = self.policy.limit;
        match self.phase {
            Phase::Children => {
                let Some(index) = self.wholes.iter().position(|probe| probe.count.is_none()) else {
                    return;
                };
                self.wholes[index].count = Some(count);
                if count <= limit {
                    return;
                }
                match self.wholes[index].root_inner {
                    Some(edge) if self.can_turn() && self.policy.preferred == Side::Child => {
                        self.turn = Some(edge);
                        self.phase = Phase::Root;
                    }
                    _ => self.reject(),
                }
            }
            Phase::Root => {
                if let Some(root) = &mut self.root {
                    root.count = Some(count);
                }
                if count <= limit {
                    self.phase = Phase::Rest;
                } else if self.policy.preferred == Side::Parent && self.turn.is_none() {
                    self.phase = Phase::Children;
                } else {
                    self.reject();
                }
            }
            Phase::Rest => {
                if let Some(probe) = self.wholes.iter_mut().find(|probe| probe.count.is_none()) {
                    probe.count = Some(count);
                }
            }
            Phase::Finished => {}
        }
    }

    /// Whether an INNER edge at the root can be turned around: the root
    /// has one, and no page of its own.
    fn can_turn(&self) -> bool {
        self.root.is_some() && self.translated.query.main_table.limit == u32::MAX
    }

    /// Every count the phase needs is in: decide.
    fn settle(&mut self) {
        let limit = self.policy.limit;
        match self.phase {
            Phase::Children => {
                if self
                    .wholes
                    .iter()
                    .all(|probe| probe.count.is_none_or(|count| count <= limit))
                {
                    self.finish(Ok(()));
                } else {
                    self.reject();
                }
            }
            Phase::Root => self.phase = Phase::Rest,
            Phase::Rest => {
                let edge = self.turn.or_else(|| self.largest_root_inner());
                let others_fit = self.wholes.iter().all(|probe| {
                    probe.count.is_none_or(|count| count <= limit)
                        || (probe.root_inner.is_some() && probe.root_inner == edge)
                });
                match edge {
                    Some(edge) if others_fit => {
                        let turned = turn(&self.translated, edge);
                        self.translated = turned;
                        self.finish(Ok(()));
                    }
                    Some(_) => self.reject(),
                    None => {
                        if others_fit {
                            self.finish(Ok(()));
                        } else {
                            self.reject();
                        }
                    }
                }
            }
            Phase::Finished => {}
        }
    }

    /// Among the root's INNER children, the one with the highest count:
    /// the one most worth turning when the parent is preferred.
    fn largest_root_inner(&self) -> Option<usize> {
        self.wholes
            .iter()
            .filter(|probe| probe.root_inner.is_some())
            .max_by_key(|probe| probe.count.unwrap_or(0))
            .and_then(|probe| probe.root_inner)
    }

    /// Refuse the query, naming every side that came out over the limit.
    fn reject(&mut self) {
        let limit = self.policy.limit;
        let over: Vec<String> = self
            .wholes
            .iter()
            .chain(self.root.iter())
            .filter(|probe| probe.count.is_some_and(|count| count > limit))
            .map(|probe| format!("{} holds more than {limit}", probe.query.table))
            .collect();
        let paged = self.translated.query.main_table.limit != u32::MAX;
        let child_over = self.wholes.iter().any(|probe| {
            probe.root_inner.is_some() && probe.count.is_some_and(|count| count > limit)
        });
        let message = if paged
            && child_over
            && self.root.as_ref().is_none_or(|root| root.count.is_none())
        {
            format!(
                "the query's page can only be driven from {}'s own side, and the other side is too large to read whole ({})",
                self.translated.query.main_table.table,
                over.join(", ")
            )
        } else {
            format!(
                "the query would read more than {limit} rows into memory ({})",
                over.join(", ")
            )
        };
        self.finish(Err(message));
    }

    /// End the plan with `outcome` (the translated query on success).
    fn finish(&mut self, outcome: Result<(), String>) {
        self.outcome = Some(outcome.map(|()| self.translated.clone()));
        self.phase = Phase::Finished;
    }
}

/// Append every node of `query`'s subtree that is read whole and has no
/// page, with its count query; `root` says whether this node is the
/// tree's root. LEFT children are driven, so the walk never enters them;
/// a node with RIGHT or INNER children is driven by them.
fn collect_wholes(query: &MultiTableReadQuery, root: bool, out: &mut Vec<Probe>) {
    let driven_from_below = !query.right_joins.is_empty() || !query.inner_joins.is_empty();
    if !driven_from_below && query.main_table.limit == u32::MAX {
        out.push(Probe {
            query: SingleTableReadQuery {
                filter: assume_exists_true(&query.main_table.filter),
                ..query.main_table.clone()
            },
            root_inner: None,
            count: None,
        });
    }
    for join in &query.right_joins {
        collect_wholes(&join.sub, false, out);
    }
    for (index, join) in query.inner_joins.iter().enumerate() {
        let before = out.len();
        collect_wholes(&join.sub, false, out);
        let child_is_whole = join.sub.right_joins.is_empty()
            && join.sub.inner_joins.is_empty()
            && join.sub.main_table.limit == u32::MAX;
        if root && child_is_whole && out.len() > before {
            out[before].root_inner = Some(index);
        }
    }
}

/// `filter` with every `EXISTS` leaf taken as true: what a node's own
/// rows are, before its INNER edges narrow them. An `OR` holding such a
/// leaf becomes true, a clause that became true drops out of its `AND`,
/// and a filter that is nothing but true is the empty `AND`.
fn assume_exists_true(filter: &Where) -> Where {
    assume(filter).unwrap_or_else(|| Where::AND(Vec::new()))
}

/// [`assume_exists_true`]'s recursion; `None` is "true".
fn assume(filter: &Where) -> Option<Where> {
    match filter {
        Where::Condition(condition) => (condition.comparison_operator
            != ComparisonOperator::EXISTS)
            .then(|| Where::Condition(condition.clone())),
        Where::AND(children) => {
            let kept: Vec<Where> = children.iter().filter_map(assume).collect();
            (!kept.is_empty()).then_some(Where::AND(kept))
        }
        Where::OR(children) => {
            let mut kept = Vec::with_capacity(children.len());
            for child in children {
                kept.push(assume(child)?);
            }
            Some(Where::OR(kept))
        }
    }
}

/// `filter` with the `EXISTS` leaf naming inner join `removed` taken as
/// true and every leaf naming a later inner join renumbered down by one.
fn drop_exists(filter: &Where, removed: usize) -> Where {
    walk_exists(filter, removed).unwrap_or_else(|| Where::AND(Vec::new()))
}

/// [`drop_exists`]'s recursion; `None` is "true".
fn walk_exists(filter: &Where, removed: usize) -> Option<Where> {
    match filter {
        Where::Condition(condition) => {
            if condition.comparison_operator != ComparisonOperator::EXISTS {
                return Some(Where::Condition(condition.clone()));
            }
            let Value::Int(index) = condition.value else {
                return Some(Where::Condition(condition.clone()));
            };
            let index = index as usize;
            if index == removed {
                return None;
            }
            let renumbered = if index > removed { index - 1 } else { index };
            Some(Where::Condition(Condition::new(
                condition.column.clone(),
                ComparisonOperator::EXISTS,
                Value::Int(renumbered as i64),
            )))
        }
        Where::AND(children) => {
            let kept: Vec<Where> = children
                .iter()
                .filter_map(|child| walk_exists(child, removed))
                .collect();
            (!kept.is_empty()).then_some(Where::AND(kept))
        }
        Where::OR(children) => {
            let mut kept = Vec::with_capacity(children.len());
            for child in children {
                kept.push(walk_exists(child, removed)?);
            }
            Some(Where::OR(kept))
        }
    }
}

/// Turn the root's INNER edge `edge` around: the edge's child becomes the
/// root, the old root its last INNER child (so the old root is read whole
/// and drives it), the old root's other children stay under the old
/// root, and the hidden parts follow their nodes.
pub fn turn(translated: &Translated, edge: usize) -> Translated {
    let old_root = &translated.query;
    let left_count = old_root.left_joins.len();
    let right_count = old_root.right_joins.len();
    let old_child_index = left_count + right_count + edge;
    let join = &old_root.inner_joins[edge];
    let child = &join.sub;
    let new_index = child.left_joins.len() + child.right_joins.len() + child.inner_joins.len();

    let mut demoted = MultiTableReadQuery {
        main_table: SingleTableReadQuery {
            filter: drop_exists(&old_root.main_table.filter, edge),
            ..old_root.main_table.clone()
        },
        left_joins: old_root.left_joins.clone(),
        right_joins: old_root.right_joins.clone(),
        inner_joins: old_root.inner_joins.clone(),
    };
    demoted.inner_joins.remove(edge);

    let mut new_root = child.clone();
    new_root.inner_joins.push(Join::new(
        demoted,
        join.sub_table_column.clone(),
        join.main_table_column.clone(),
    ));

    let remap = |part: &QueryPart| -> QueryPart {
        let path = &part.0;
        if path.first() == Some(&old_child_index) {
            return QueryPart(path[1..].to_vec());
        }
        let mut moved = vec![new_index];
        if let Some(&first) = path.first() {
            moved.push(if first > old_child_index {
                first - 1
            } else {
                first
            });
            moved.extend_from_slice(&path[1..]);
        }
        QueryPart(moved)
    };
    let hidden: HashSet<QueryPart> = translated.hidden.iter().map(remap).collect();
    Translated {
        query: new_root,
        hidden,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Order, OrderBy};

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
        let query = MultiTableReadQuery {
            main_table: node("messages", Where::exists("conversationId", 0), limit),
            left_joins: Vec::new(),
            right_joins: Vec::new(),
            inner_joins: vec![Join::new(conversations, "conversationId", "conversationId")],
        };
        Translated {
            query,
            hidden: HashSet::from([QueryPart::join(0)]),
        }
    }

    /// Drive `planner` with `answers` in the order it asks, returning the
    /// tables it counted and its decision.
    fn drive(mut planner: Planner, answers: &[u64]) -> (Vec<String>, Result<Translated, String>) {
        let mut asked = Vec::new();
        let mut answers = answers.iter();
        loop {
            match planner.step() {
                Step::Count(query, _) => {
                    asked.push(query.table.to_string());
                    planner.answer(*answers.next().expect("an answer for every count"));
                }
                Step::Done(outcome) => return (asked, outcome),
            }
        }
    }

    /// A query without joins, or a limit of zero, is registered as it is
    /// with nothing counted.
    #[test]
    fn nothing_to_plan_without_joins_or_a_limit() {
        let plain = Translated {
            query: MultiTableReadQuery::single(node("tickets", Where::AND(Vec::new()), u32::MAX)),
            hidden: HashSet::new(),
        };
        let (asked, outcome) = drive(
            Planner::new(
                plain.clone(),
                Policy {
                    limit: 10,
                    preferred: Side::Child,
                },
            ),
            &[],
        );
        assert!(asked.is_empty());
        assert_eq!(outcome.expect("kept").query, plain.query);

        let (asked, outcome) = drive(
            Planner::new(
                messages_in_channel(u32::MAX),
                Policy {
                    limit: 0,
                    preferred: Side::Child,
                },
            ),
            &[],
        );
        assert!(asked.is_empty(), "a limit of zero turns planning off");
        assert!(outcome.is_ok());
    }

    /// The child is counted first; under the limit, the tree stays as
    /// translated (the child drives, as it does today).
    #[test]
    fn a_small_child_keeps_the_tree() {
        let (asked, outcome) = drive(
            Planner::new(
                messages_in_channel(u32::MAX),
                Policy {
                    limit: 100,
                    preferred: Side::Child,
                },
            ),
            &[7],
        );
        assert_eq!(asked, vec!["conversations"]);
        let kept = outcome.expect("kept");
        assert_eq!(kept.query.main_table.table.as_str(), "messages");
        assert_eq!(kept.hidden, HashSet::from([QueryPart::join(0)]));
    }

    /// The child is too big and the parent fits: the edge is turned, the
    /// parent becomes the driver read whole, the child the root it
    /// narrows, and the hidden part follows the child to the root.
    #[test]
    fn a_big_child_and_a_small_parent_turn_the_edge() {
        let (asked, outcome) = drive(
            Planner::new(
                messages_in_channel(u32::MAX),
                Policy {
                    limit: 100,
                    preferred: Side::Child,
                },
            ),
            &[101, 40],
        );
        assert_eq!(asked, vec!["conversations", "messages"]);
        let turned = outcome.expect("turned");
        assert_eq!(turned.query.main_table.table.as_str(), "conversations");
        assert_eq!(turned.query.inner_joins.len(), 1);
        let demoted = &turned.query.inner_joins[0];
        assert_eq!(demoted.sub.main_table.table.as_str(), "messages");
        assert_eq!(demoted.main_table_column.as_str(), "conversationId");
        assert_eq!(demoted.sub_table_column.as_str(), "conversationId");
        assert_eq!(
            demoted.sub.main_table.filter,
            Where::AND(Vec::new()),
            "the EXISTS leaf is the edge itself, expressed from the other side now"
        );
        assert_eq!(turned.hidden, HashSet::from([QueryPart::main()]));
    }

    /// Both sides over the limit: refused, naming both.
    #[test]
    fn two_big_sides_are_refused() {
        let (_, outcome) = drive(
            Planner::new(
                messages_in_channel(u32::MAX),
                Policy {
                    limit: 100,
                    preferred: Side::Child,
                },
            ),
            &[101, 101],
        );
        let reason = outcome.expect_err("refused");
        assert!(
            reason.contains("conversations holds more than 100"),
            "{reason}"
        );
        assert!(reason.contains("messages holds more than 100"), "{reason}");
    }

    /// A root with a page cannot be turned: a big child refuses the
    /// query without counting the parent, and says why.
    #[test]
    fn a_paged_root_cannot_be_turned() {
        let (asked, outcome) = drive(
            Planner::new(
                messages_in_channel(50),
                Policy {
                    limit: 100,
                    preferred: Side::Child,
                },
            ),
            &[101],
        );
        assert_eq!(asked, vec!["conversations"]);
        let reason = outcome.expect_err("refused");
        assert!(reason.contains("page can only be driven"), "{reason}");
    }

    /// Preferring the parent counts it first and turns the edge when it
    /// fits, whatever the child's size.
    #[test]
    fn a_preferred_parent_that_fits_drives() {
        let (asked, outcome) = drive(
            Planner::new(
                messages_in_channel(u32::MAX),
                Policy {
                    limit: 100,
                    preferred: Side::Parent,
                },
            ),
            &[40, 5],
        );
        assert_eq!(asked, vec!["messages", "conversations"]);
        assert_eq!(
            outcome.expect("turned").query.main_table.table.as_str(),
            "conversations"
        );
    }

    /// Preferring the parent when it is too big falls back to the child,
    /// which is kept when it fits.
    #[test]
    fn a_preferred_parent_too_big_falls_back_to_the_child() {
        let (asked, outcome) = drive(
            Planner::new(
                messages_in_channel(u32::MAX),
                Policy {
                    limit: 100,
                    preferred: Side::Parent,
                },
            ),
            &[101, 5],
        );
        assert_eq!(asked, vec!["messages", "conversations"]);
        assert_eq!(
            outcome.expect("kept").query.main_table.table.as_str(),
            "messages"
        );
    }

    /// A LEFT join's root is read whole: over the limit it is refused,
    /// and there is no other side to turn to.
    #[test]
    fn a_big_left_root_is_refused() {
        let users = MultiTableReadQuery::single(node("users", Where::AND(Vec::new()), u32::MAX));
        let query = MultiTableReadQuery {
            main_table: node("tickets", Where::AND(Vec::new()), u32::MAX),
            left_joins: vec![Join::new(users, "assigned_to", "id")],
            right_joins: Vec::new(),
            inner_joins: Vec::new(),
        };
        let translated = Translated {
            query,
            hidden: HashSet::new(),
        };
        let (asked, outcome) = drive(
            Planner::new(
                translated,
                Policy {
                    limit: 100,
                    preferred: Side::Child,
                },
            ),
            &[101],
        );
        assert_eq!(
            asked,
            vec!["tickets"],
            "the LEFT child is driven and never counted"
        );
        assert!(outcome.is_err());
    }

    /// A paged root with LEFT joins holds its window and is never
    /// counted; nothing is asked.
    #[test]
    fn a_paged_left_root_is_bounded_by_its_window() {
        let users = MultiTableReadQuery::single(node("users", Where::AND(Vec::new()), u32::MAX));
        let query = MultiTableReadQuery {
            main_table: node("tickets", Where::AND(Vec::new()), 50),
            left_joins: vec![Join::new(users, "assigned_to", "id")],
            right_joins: Vec::new(),
            inner_joins: Vec::new(),
        };
        let translated = Translated {
            query,
            hidden: HashSet::new(),
        };
        let (asked, outcome) = drive(
            Planner::new(
                translated,
                Policy {
                    limit: 100,
                    preferred: Side::Child,
                },
            ),
            &[],
        );
        assert!(asked.is_empty());
        assert!(outcome.is_ok());
    }

    /// Turning renumbers the other EXISTS leaves and moves the other
    /// children under the demoted root, hidden parts included.
    #[test]
    fn turning_keeps_the_other_edges_in_place() {
        let channels =
            MultiTableReadQuery::single(node("channels", Where::AND(Vec::new()), u32::MAX));
        let users = MultiTableReadQuery::single(node("users", Where::AND(Vec::new()), u32::MAX));
        let attachments =
            MultiTableReadQuery::single(node("attachments", Where::AND(Vec::new()), u32::MAX));
        let query = MultiTableReadQuery {
            main_table: node(
                "messages",
                Where::AND(vec![
                    Where::exists("channelId", 0),
                    Where::exists("senderId", 1),
                ]),
                u32::MAX,
            ),
            left_joins: vec![Join::new(attachments, "id", "messageId")],
            right_joins: Vec::new(),
            inner_joins: vec![
                Join::new(channels, "channelId", "id"),
                Join::new(users, "senderId", "id"),
            ],
        };
        let translated = Translated {
            query,
            hidden: HashSet::from([QueryPart::join(1), QueryPart::join(2)]),
        };
        let turned = turn(&translated, 0);
        assert_eq!(turned.query.main_table.table.as_str(), "channels");
        let demoted = &turned.query.inner_joins[0].sub;
        assert_eq!(demoted.main_table.table.as_str(), "messages");
        assert_eq!(
            demoted.left_joins.len(),
            1,
            "the attachments edge stays under messages"
        );
        assert_eq!(demoted.inner_joins.len(), 1, "only the users edge is left");
        assert_eq!(
            demoted.main_table.filter,
            Where::AND(vec![Where::exists("senderId", 0)]),
            "the users leaf is renumbered to 0"
        );
        assert_eq!(
            turned.hidden,
            HashSet::from([QueryPart::main(), QueryPart(vec![0, 1])]),
            "channels is the hidden root, users the hidden second child of the demoted messages"
        );
    }

    /// `OR(x, EXISTS)` is true once the EXISTS is assumed, so the whole
    /// clause drops out of the count's filter; a bare AND of leaves keeps
    /// the others.
    #[test]
    fn assuming_exists_true_simplifies_the_filter() {
        let filter = Where::AND(vec![
            Where::condition("status", ComparisonOperator::EQ, "OPEN"),
            Where::OR(vec![
                Where::condition("visibility", ComparisonOperator::EQ, "PUBLIC"),
                Where::exists("id", 0),
            ]),
        ]);
        assert_eq!(
            assume_exists_true(&filter),
            Where::AND(vec![Where::condition(
                "status",
                ComparisonOperator::EQ,
                "OPEN"
            )])
        );
        assert_eq!(
            assume_exists_true(&Where::exists("id", 0)),
            Where::AND(Vec::new())
        );
    }
}
