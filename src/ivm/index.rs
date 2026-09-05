//! Per-table routing index: DNF disjunct counters keyed by their conditions.
//!
//! One [`TableIndex`] holds everything needed to answer "which subscriptions
//! does this row image match" for one table. Each DNF disjunct is a single
//! [`DisjunctCounter`] shared (via `Rc<RefCell<_>>`) under every condition
//! it contains — sharing is what makes counting correct: all conditions of
//! an AND-disjunct must bump the *same* counter for it to ever reach its
//! size. The same sharing also deduplicates across subscriptions: filters
//! that normalize to an identical [`Disjunct`] reuse one counter, and a
//! single firing fans out to every subscriber.
//!
//! # Single-threaded by design (for now)
//!
//! The engine works on a single thread only; multithreading is a later step
//! to be thought through deliberately when it comes up. `Rc<RefCell<_>>` is
//! not a concession to threading — it is the single-threaded tool for the
//! aliasing this index needs: one counter object *owned by several map
//! entries* (`Rc`), mutated through those shared handles (`RefCell`).
//! Without it, the same counter could not sit under N condition keys at
//! all. "Shared" therefore means strictly within the one engine: across
//! condition keys and across subscriptions — never across threads, and the
//! compiler enforces exactly that: `Rc` is not `Send`, so code trying to
//! move or share the engine across threads is rejected at compile time
//! instead of racing at runtime.
//!
//! For whenever multithreading is designed, the sketch on record is table
//! sharding (one owning thread per table; no counter, frame, or epoch is
//! ever shared between tables), under which this module can stay as it is
//! per shard.

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

use super::predicate::eval_condition;
use super::stats::IvmStats;
use super::QueryId;
use crate::model::{Condition, Disjunct, Value};

/// Shared handle to one disjunct's counting state; cloned under every
/// condition key the disjunct contains.
type SharedCounter = Rc<RefCell<DisjunctCounter>>;

/// One DNF disjunct's counting state plus every subscription it fires.
///
/// - `size`: number of distinct conditions in the disjunct; the counter
///   fires when `satisfied` reaches it.
/// - `epoch`: the write that `satisfied` belongs to. A stale counter is
///   reset the first time the current write touches it, so no per-write
///   sweep is needed.
/// - `satisfied`: conditions of this disjunct matched so far in `epoch`.
/// - `subscribers`: the subscriptions whose filters contain this disjunct
///   shape; all of them are impacted when the counter fires.
struct DisjunctCounter {
    size: usize,
    epoch: u64,
    satisfied: usize,
    subscribers: Vec<QueryId>,
}

/// The routing index for one table.
///
/// - `by_condition`: condition → the shared counters of every disjunct
///   containing it; the probe path of a write.
/// - `by_disjunct`: [`Disjunct`] (as produced by
///   [`crate::model::Where::to_dnf`], its conditions carried inside) → its
///   shared counter, so an identical disjunct registered again reuses the
///   counter instead of duplicating it. Disjuncts that differ only in
///   condition order are not merged — harmless, they just keep separate
///   counters.
/// - `unconditional`: subscriptions owning an empty (vacuously true)
///   disjunct; they match every row write on the table and never appear in
///   `by_condition`.
#[derive(Default)]
pub(super) struct TableIndex {
    by_condition: HashMap<Condition, Vec<SharedCounter>>,
    by_disjunct: HashMap<Disjunct, SharedCounter>,
    unconditional: Vec<QueryId>,
}

impl TableIndex {
    /// Index `subscriber`'s DNF disjuncts.
    ///
    /// An empty disjunct goes to `unconditional`; a disjunct already present
    /// (from another subscription, or a duplicate within this one) only
    /// gains the subscriber — no new counter, no new links, and therefore no
    /// double-counting. `conditions_indexed` counts only links actually
    /// added.
    pub(super) fn register(
        &mut self,
        subscriber: &QueryId,
        disjuncts: Vec<Disjunct>,
        stats: &mut IvmStats,
    ) {
        for disjunct in disjuncts {
            stats.disjuncts_registered += 1;
            if disjunct.conditions.is_empty() {
                if !self.unconditional.contains(subscriber) {
                    self.unconditional.push(subscriber.clone());
                }
                continue;
            }
            if let Some(existing) = self.by_disjunct.get(&disjunct) {
                let mut counter = existing.borrow_mut();
                if !counter.subscribers.contains(subscriber) {
                    counter.subscribers.push(subscriber.clone());
                }
                continue;
            }
            let counter = Rc::new(RefCell::new(DisjunctCounter {
                size: disjunct.conditions.len(),
                epoch: 0,
                satisfied: 0,
                subscribers: vec![subscriber.clone()],
            }));
            for condition in &disjunct.conditions {
                self.by_condition
                    .entry(condition.clone())
                    .or_default()
                    .push(Rc::clone(&counter));
                stats.conditions_indexed += 1;
            }
            self.by_disjunct.insert(disjunct, counter);
        }
    }

    /// Remove every trace of `subscriber`: its unconditional entry, its
    /// membership in shared counters, and — once a counter has no
    /// subscribers left — the counter itself and all its condition links.
    pub(super) fn unregister(&mut self, subscriber: &str) {
        self.unconditional
            .retain(|candidate| candidate.as_str() != subscriber);
        let mut dead_disjuncts: Vec<Disjunct> = Vec::new();
        for (disjunct, counter) in &self.by_disjunct {
            let mut counter = counter.borrow_mut();
            counter
                .subscribers
                .retain(|candidate| candidate.as_str() != subscriber);
            if counter.subscribers.is_empty() {
                dead_disjuncts.push(disjunct.clone());
            }
        }
        for disjunct in &dead_disjuncts {
            let Some(dead) = self.by_disjunct.remove(disjunct) else {
                continue;
            };
            for condition in &disjunct.conditions {
                if let Some(counters) = self.by_condition.get_mut(condition) {
                    counters.retain(|candidate| !Rc::ptr_eq(candidate, &dead));
                }
            }
        }
        self.by_condition.retain(|_, counters| !counters.is_empty());
    }

    /// Whether the index routes nothing anymore (so the engine can drop it).
    pub(super) fn is_empty(&self) -> bool {
        self.by_disjunct.is_empty() && self.unconditional.is_empty()
    }

    /// The subscriptions whose filters the row image satisfies.
    ///
    /// Evaluates each distinct condition exactly once; every match bumps the
    /// shared counters of the disjuncts containing it, and a counter
    /// reaching its size fires all of its subscribers. `epoch` must be a
    /// fresh, monotonically increased write number — it is what lazily
    /// invalidates counts left over from earlier writes. Unconditional
    /// subscribers are always included.
    pub(super) fn matched(
        &self,
        row: &HashMap<String, Value>,
        epoch: u64,
        stats: &mut IvmStats,
    ) -> BTreeSet<QueryId> {
        let mut fired = BTreeSet::new();
        for (condition, counters) in &self.by_condition {
            if !eval_condition(condition, row, &mut stats.conditions_evaluated) {
                continue;
            }
            stats.index_hits += 1;
            for shared in counters {
                let mut counter = shared.borrow_mut();
                if counter.epoch != epoch {
                    counter.epoch = epoch;
                    counter.satisfied = 0;
                }
                counter.satisfied += 1;
                stats.disjunct_increments += 1;
                if counter.satisfied == counter.size {
                    stats.disjuncts_fired += 1;
                    fired.extend(counter.subscribers.iter().cloned());
                }
            }
        }
        for subscriber in &self.unconditional {
            fired.insert(subscriber.clone());
        }
        fired
    }
}
