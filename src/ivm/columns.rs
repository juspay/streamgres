//! Per-column value index over one table's conditions: given a written
//! value, the conditions it satisfies, found by lookup instead of by
//! evaluating every condition on the table.
//!
//! Each [`ColumnIndex`] files the conditions on one column by operator
//! family so that one written value reaches exactly its matches:
//!
//! - **Equality** (`=`, `IN`): a hash map from value to conditions; an
//!   `IN` is filed under each of its list values (or, for a set-valued
//!   `IN`, its current members, with [`ColumnIndex::file_key`] and
//!   [`ColumnIndex::unfile_key`] following the set one member at a time). Keys fold in the numeric
//!   coercion of [`Value::loose_eq`] (an integral float files as the
//!   integer), so `Int(5)` finds `= 5.0`.
//! - **Inequality** (`<>`, `NOT IN`, `IS NOT NULL`): the mirror map records
//!   where each condition *fails*; the matches for a value are every
//!   inequality on the column except those filed under it. A `NOT IN`
//!   whose list holds `NULL` is never true and is never filed. `IS NOT
//!   NULL` fails only at `NULL`, so it is filed under the `NULL` key and
//!   matched by every other value.
//! - **Null** (`IS NULL`): filed under the `NULL` key of the equality map,
//!   a key no `=` or `IN` ever files under; a written `NULL` yields that
//!   key and nothing else, since every comparison is false for it.
//! - **Ranges** (`>`, `>=`, `<`, `<=`): ordered maps from threshold to
//!   conditions, one per comparison class (numeric, string, bool, date,
//!   datetime) so incomparable types never fall inside a range; the
//!   matches for a value are one ordered-map range scan, with the strict
//!   operators dropped at a tying threshold.
//!
//! `NaN` matches only what equality semantics say it does (`= NaN`, and
//! every inequality). Each condition sits in one family under keys a
//! single value hits at most once, so a probe yields every condition at
//! most once and the disjunct counting stays exact. Conditions are held as
//! [`CondRef`]s, shared pointers hashed by identity, so filing an `IN`
//! under a thousand keys costs a thousand pointer inserts, not a thousand
//! copies of the list.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use super::window::order_cmp;
use crate::model::{ComparisonOperator, Condition, Value};

/// A shared handle to an indexed condition, hashed and compared by pointer
/// identity: the same condition object under every key it is filed at.
#[derive(Clone, Debug)]
pub(super) struct CondRef(pub(super) Rc<Condition>);

impl PartialEq for CondRef {
    /// Pointer identity.
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for CondRef {}

impl Hash for CondRef {
    /// Hashes the pointer, never the condition.
    fn hash<H: Hasher>(&self, state: &mut H) {
        (Rc::as_ptr(&self.0) as usize).hash(state);
    }
}

/// A range-map key: a value ordered by [`order_cmp`], with equality meaning
/// "compares equal", so `Int(1)` and `Float(1.0)` share one key.
#[derive(Clone, Debug)]
struct OrdValue(Value);

impl PartialEq for OrdValue {
    /// Equal when the values compare equal.
    fn eq(&self, other: &Self) -> bool {
        order_cmp(&self.0, &other.0) == Ordering::Equal
    }
}

impl Eq for OrdValue {}

impl PartialOrd for OrdValue {
    /// Delegates to [`Ord`].
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdValue {
    /// The window module's total order.
    fn cmp(&self, other: &Self) -> Ordering {
        order_cmp(&self.0, &other.0)
    }
}

/// The conditions on one column, filed by operator family.
#[derive(Default)]
pub(super) struct ColumnIndex {
    equal: HashMap<Value, HashSet<CondRef>>,
    unequal: HashMap<Value, HashSet<CondRef>>,
    unequal_all: HashSet<CondRef>,
    above: HashMap<u8, BTreeMap<OrdValue, HashSet<CondRef>>>,
    below: HashMap<u8, BTreeMap<OrdValue, HashSet<CondRef>>>,
    /// Set-valued `IN` conditions (a join edge's driven values), filed
    /// once each and probed by membership, so registering one costs no
    /// more than its condition and a member coming or going touches the
    /// set alone.
    sets: HashSet<CondRef>,
    /// Set-valued `NOT IN` conditions, probed by non-membership.
    not_sets: HashSet<CondRef>,
}

impl ColumnIndex {
    /// File one condition under every key it can match at; a condition
    /// that can never be true (`= NULL`, `NOT IN` with a `NULL`, a
    /// threshold nothing compares with, `IS` with a non-`NULL` operand, an
    /// unbound `EXISTS`) is filed nowhere.
    pub(super) fn file(&mut self, condition: &CondRef) {
        use ComparisonOperator::*;
        let value = &condition.0.value;
        match condition.0.comparison_operator {
            EQ => {
                if let Some(key) = value.equality_key() {
                    self.equal.entry(key).or_default().insert(condition.clone());
                }
            }
            IS => {
                if value.is_null() {
                    self.equal
                        .entry(Value::Null)
                        .or_default()
                        .insert(condition.clone());
                }
            }
            IS_NOT => {
                if value.is_null() {
                    self.unequal
                        .entry(Value::Null)
                        .or_default()
                        .insert(condition.clone());
                    self.unequal_all.insert(condition.clone());
                }
            }
            EXISTS => {}
            IN => {
                if let Value::Set(_) = value {
                    self.sets.insert(condition.clone());
                } else {
                    for key in member_keys(value) {
                        self.equal.entry(key).or_default().insert(condition.clone());
                    }
                }
            }
            NEQ => {
                if let Some(key) = value.equality_key() {
                    self.unequal
                        .entry(key)
                        .or_default()
                        .insert(condition.clone());
                    self.unequal_all.insert(condition.clone());
                }
            }
            NOT_IN => {
                if let Value::Set(_) = value {
                    self.not_sets.insert(condition.clone());
                } else if negation_is_satisfiable(value) {
                    for key in member_keys(value) {
                        self.unequal
                            .entry(key)
                            .or_default()
                            .insert(condition.clone());
                    }
                    self.unequal_all.insert(condition.clone());
                }
            }
            GT | GTE => {
                if let Some(class) = class_of(value) {
                    self.above
                        .entry(class)
                        .or_default()
                        .entry(OrdValue(value.clone()))
                        .or_default()
                        .insert(condition.clone());
                }
            }
            LT | LTE => {
                if let Some(class) = class_of(value) {
                    self.below
                        .entry(class)
                        .or_default()
                        .entry(OrdValue(value.clone()))
                        .or_default()
                        .insert(condition.clone());
                }
            }
        }
    }

    /// Remove one condition from every key it was filed under.
    pub(super) fn unfile(&mut self, condition: &CondRef) {
        use ComparisonOperator::*;
        let value = &condition.0.value;
        match condition.0.comparison_operator {
            EQ => remove_under(&mut self.equal, value.equality_key(), condition),
            IS => remove_under(&mut self.equal, Some(Value::Null), condition),
            IS_NOT => {
                remove_under(&mut self.unequal, Some(Value::Null), condition);
                self.unequal_all.remove(condition);
            }
            EXISTS => {}
            IN => {
                if let Value::Set(_) = value {
                    self.sets.remove(condition);
                } else {
                    for key in member_keys(value) {
                        remove_under(&mut self.equal, Some(key), condition);
                    }
                }
            }
            NEQ => {
                remove_under(&mut self.unequal, value.equality_key(), condition);
                self.unequal_all.remove(condition);
            }
            NOT_IN => {
                if let Value::Set(_) = value {
                    self.not_sets.remove(condition);
                } else {
                    for key in member_keys(value) {
                        remove_under(&mut self.unequal, Some(key), condition);
                    }
                    self.unequal_all.remove(condition);
                }
            }
            GT | GTE => remove_threshold(&mut self.above, value, condition),
            LT | LTE => remove_threshold(&mut self.below, value, condition),
        }
    }

    /// Whether nothing is filed on the column anymore.
    pub(super) fn is_empty(&self) -> bool {
        self.equal.is_empty()
            && self.unequal_all.is_empty()
            && self.above.is_empty()
            && self.below.is_empty()
            && self.sets.is_empty()
            && self.not_sets.is_empty()
    }

    /// Append the conditions a written value satisfies to `out`: for a
    /// `NULL`, the null tests alone. The value is looked up as it is; a
    /// copy is made only for the range probe, and only when the column
    /// has range conditions of the value's class.
    pub(super) fn candidates(&self, value: &Value, out: &mut Vec<CondRef>) {
        use ComparisonOperator::*;
        if value.is_null() {
            if let Some(matching) = self.equal.get(&Value::Null) {
                out.extend(matching.iter().cloned());
            }
            return;
        }
        let Some(key) = value.equality_key_ref() else {
            return;
        };
        if let Some(matching) = self.equal.get(&*key) {
            out.extend(matching.iter().cloned());
        }
        for condition in &self.sets {
            if let Value::Set(set) = &condition.0.value
                && set.contains(&key)
            {
                out.push(condition.clone());
            }
        }
        for condition in &self.not_sets {
            if let Value::Set(set) = &condition.0.value
                && !set.contains(&key)
            {
                out.push(condition.clone());
            }
        }
        if !self.unequal_all.is_empty() {
            let failing = self.unequal.get(&*key);
            out.extend(
                self.unequal_all
                    .iter()
                    .filter(|condition| !failing.is_some_and(|set| set.contains(condition)))
                    .cloned(),
            );
        }
        let Some(class) = class_of(value) else {
            return;
        };
        let above = self.above.get(&class);
        let below = self.below.get(&class);
        if above.is_none() && below.is_none() {
            return;
        }
        let probe = OrdValue(value.clone());
        if let Some(thresholds) = above {
            for (threshold, conditions) in thresholds.range(..=&probe) {
                let tying = *threshold == probe;
                out.extend(
                    conditions
                        .iter()
                        .filter(|condition| !(tying && condition.0.comparison_operator == GT))
                        .cloned(),
                );
            }
        }
        if let Some(thresholds) = below {
            for (threshold, conditions) in thresholds.range(&probe..) {
                let tying = *threshold == probe;
                out.extend(
                    conditions
                        .iter()
                        .filter(|condition| !(tying && condition.0.comparison_operator == LT))
                        .cloned(),
                );
            }
        }
    }
}

/// Remove `condition` from the set under `key`, dropping an emptied set.
fn remove_under(
    map: &mut HashMap<Value, HashSet<CondRef>>,
    key: Option<Value>,
    condition: &CondRef,
) {
    let Some(key) = key else {
        return;
    };
    if let Some(set) = map.get_mut(&key) {
        set.remove(condition);
        if set.is_empty() {
            map.remove(&key);
        }
    }
}

/// Remove `condition` from the threshold map of its class, dropping
/// emptied sets and maps.
fn remove_threshold(
    maps: &mut HashMap<u8, BTreeMap<OrdValue, HashSet<CondRef>>>,
    value: &Value,
    condition: &CondRef,
) {
    let Some(class) = class_of(value) else {
        return;
    };
    let Some(thresholds) = maps.get_mut(&class) else {
        return;
    };
    let key = OrdValue(value.clone());
    if let Some(set) = thresholds.get_mut(&key) {
        set.remove(condition);
        if set.is_empty() {
            thresholds.remove(&key);
        }
    }
    if thresholds.is_empty() {
        maps.remove(&class);
    }
}

/// The equality keys of an `IN` / `NOT IN` operand: one per non-null list
/// item, or the current members of a shared set; empty for anything else.
fn member_keys(value: &Value) -> Vec<Value> {
    match value {
        Value::List(items) => items.iter().filter_map(Value::equality_key).collect(),
        _ => Vec::new(),
    }
}

/// Whether a `NOT IN` operand can ever be true: a list holding `NULL`
/// cannot; a shared set never holds `NULL`.
fn negation_is_satisfiable(value: &Value) -> bool {
    match value {
        Value::List(items) => !items.iter().any(Value::is_null),
        Value::Set(_) => true,
        _ => false,
    }
}

/// The comparison class a value takes part in range comparisons under;
/// `None` for values no range condition can match (`NULL`, `NaN`, lists,
/// maps).
fn class_of(value: &Value) -> Option<u8> {
    match value {
        Value::Int(_) => Some(1),
        Value::Float(f) if !f.is_nan() => Some(1),
        Value::String(_) => Some(2),
        Value::Bool(_) => Some(3),
        Value::Date(_) => Some(4),
        Value::Datetime(_) => Some(5),
        _ => None,
    }
}
