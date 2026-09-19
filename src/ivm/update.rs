//! What an engine hands the transport: deltas addressed to **clients**.
//! Inside the engine every operation is produced per subscription (and,
//! in the join layer, per part of a subscription); a client with several
//! subscriptions holding one row would receive that row once per
//! subscription. The grouping here folds one step's operations per client
//! and row: each row image travels to a client once, tagged with every
//! subscription and part it applies to, and a subscription that lost and
//! regained the row within the step (an in-place change) appears only on
//! the `Add`, which a receiver applies as insert-or-replace.

use std::collections::HashMap;

use crate::model::{ClientId, DataFrameKey, DataFrameOperation, DataFrameRow, SubId, TableName};

/// Which node of a subscription's join tree a part is: the path of join
/// positions from the root. The root is the empty path; a single-table
/// subscription has only the root.
///
/// The path is kept inline, at most [`QueryPart::MAX_DEPTH`] steps of at
/// most 255 each, so a part is eight bytes, `Copy`, and hashed as one
/// integer: every delta names its part without an allocation.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct QueryPart {
    len: u8,
    path: [u8; QueryPart::MAX_DEPTH],
}

impl QueryPart {
    /// The deepest a tree may nest.
    pub const MAX_DEPTH: usize = 7;

    /// The root part.
    pub fn main() -> Self {
        QueryPart::default()
    }

    /// The root's `index`-th join.
    pub fn join(index: usize) -> Self {
        QueryPart::main().child(index)
    }

    /// The part at `path`; `None` when it nests deeper than
    /// [`QueryPart::MAX_DEPTH`] or a step is past 255.
    pub fn try_new(path: &[usize]) -> Option<Self> {
        if path.len() > Self::MAX_DEPTH {
            return None;
        }
        let mut part = QueryPart::default();
        for (slot, &step) in part.path.iter_mut().zip(path) {
            *slot = u8::try_from(step).ok()?;
        }
        part.len = path.len() as u8;
        Some(part)
    }

    /// The part at `path`.
    ///
    /// # Panics
    ///
    /// When the path nests deeper than [`QueryPart::MAX_DEPTH`] or a step
    /// is past 255 (a tree the translation would have refused).
    pub fn new(path: &[usize]) -> Self {
        Self::try_new(path).expect("a query part within the engine's depth and width")
    }

    /// The join positions from the root.
    pub fn path(&self) -> &[u8] {
        &self.path[..self.len as usize]
    }

    /// Whether this is the root.
    pub fn is_main(&self) -> bool {
        self.len == 0
    }

    /// The join index of a first-level part; `None` for the root and for
    /// nested parts.
    pub fn join_index(&self) -> Option<usize> {
        (self.len == 1).then_some(self.path[0] as usize)
    }

    /// The `index`-th child of this part.
    pub(crate) fn child(&self, index: usize) -> Self {
        let mut child = *self;
        assert!(
            (child.len as usize) < Self::MAX_DEPTH,
            "a query tree nests deeper than {} joins",
            Self::MAX_DEPTH
        );
        child.path[child.len as usize] = u8::try_from(index).expect("a node has at most 256 joins");
        child.len += 1;
        child
    }
}

impl std::hash::Hash for QueryPart {
    /// The whole path as one integer.
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let mut bytes = [0u8; 8];
        bytes[0] = self.len;
        bytes[1..].copy_from_slice(&self.path);
        state.write_u64(u64::from_le_bytes(bytes));
    }
}

impl std::fmt::Debug for QueryPart {
    /// `QueryPart([1, 0])`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "QueryPart({:?})", self.path())
    }
}

/// One subscription's part an operation applies to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Target {
    pub sub: SubId,
    pub part: QueryPart,
}

/// One delta for one client: the operation, the table it lands on, and
/// every subscription part of that client it applies to. The transport
/// forwards it to the client as is; the client applies the operation to
/// its per-table store once and to each target's membership.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientUpdate {
    pub client: ClientId,
    pub table: TableName,
    pub op: DataFrameOperation,
    pub targets: Vec<Target>,
}

/// One per-subscription operation before grouping, with the client it
/// belongs to.
pub(crate) struct Raw {
    pub client: ClientId,
    pub table: TableName,
    pub target: Target,
    pub op: DataFrameOperation,
}

/// One client's operations on one row within a step, sorted into the
/// three slots the per-key ordering contract allows: a `Delete` before any
/// `Add` (the row leaving), the `Add` (the row entering, or its new image
/// when it was held: the old half of an in-place change is dropped), and
/// a `Delete` after an `Add` (admitted and evicted within the step).
struct Folded {
    client: ClientId,
    table: TableName,
    key: DataFrameKey,
    leaving: Option<(DataFrameRow, Vec<Target>)>,
    entering: Option<(DataFrameRow, Vec<Target>)>,
    evicted: Option<(DataFrameRow, Vec<Target>)>,
}

impl Folded {
    /// Push `target` under `slot` with `image`, once.
    fn slot(slot: &mut Option<(DataFrameRow, Vec<Target>)>, image: &DataFrameRow, target: &Target) {
        let (held, targets) = slot.get_or_insert_with(|| (image.clone(), Vec::new()));
        debug_assert!(
            held == image,
            "one step gives every holder of a row the same image: {held:?} and {image:?} for {target:?} beside {targets:?}"
        );
        if !targets.contains(target) {
            targets.push(target.clone());
        }
    }

    /// Whether `target` already entered in this step.
    fn entered(&self, target: &Target) -> bool {
        self.entering
            .as_ref()
            .is_some_and(|(_, targets)| targets.contains(target))
    }

    /// Sort one operation of `target` into its slot.
    fn fold(&mut self, target: &Target, op: DataFrameOperation) {
        match op {
            DataFrameOperation::Delete(_, image) if self.entered(target) => {
                Self::slot(&mut self.evicted, &image, target);
            }
            DataFrameOperation::Delete(_, image) => {
                Self::slot(&mut self.leaving, &image, target);
            }
            DataFrameOperation::Add(_, image) => {
                if let Some((_, leaving)) = self.leaving.as_mut() {
                    leaving.retain(|leaving| leaving != target);
                }
                if let Some((_, evicted)) = self.evicted.as_mut() {
                    evicted.retain(|evicted| evicted != target);
                }
                match self.entering.as_mut() {
                    Some((held, targets)) => {
                        *held = image;
                        if !targets.contains(target) {
                            targets.push(target.clone());
                        }
                    }
                    None => self.entering = Some((image, vec![target.clone()])),
                }
            }
        }
    }

    /// The client's deltas for this row, in the order they apply.
    fn into_updates(self) -> Vec<ClientUpdate> {
        let Folded {
            client,
            table,
            key,
            leaving,
            entering,
            evicted,
        } = self;
        let mut out = Vec::new();
        let mut push = |op: DataFrameOperation, targets: Vec<Target>| {
            if !targets.is_empty() {
                out.push(ClientUpdate {
                    client,
                    table: table.clone(),
                    op,
                    targets,
                });
            }
        };
        if let Some((image, targets)) = leaving {
            push(DataFrameOperation::Delete(key.clone(), image), targets);
        }
        if let Some((image, targets)) = entering {
            push(DataFrameOperation::Add(key.clone(), image), targets);
        }
        if let Some((image, targets)) = evicted {
            push(DataFrameOperation::Delete(key, image), targets);
        }
        out
    }
}

/// Group one step's per-subscription operations per client and row,
/// keeping rows in the order they first appear.
pub(crate) fn group(raw: Vec<Raw>) -> Vec<ClientUpdate> {
    let mut folded: Vec<Folded> = Vec::new();
    let mut index: HashMap<(ClientId, TableName, DataFrameKey), usize> = HashMap::new();
    for Raw {
        client,
        table,
        target,
        op,
    } in raw
    {
        let slot = *index
            .entry((client, table.clone(), op.key().clone()))
            .or_insert_with(|| {
                folded.push(Folded {
                    client,
                    table: table.clone(),
                    key: op.key().clone(),
                    leaving: None,
                    entering: None,
                    evicted: None,
                });
                folded.len() - 1
            });
        folded[slot].fold(&target, op);
    }
    folded.into_iter().flat_map(Folded::into_updates).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Value;

    /// A row image with one `v` column.
    fn image(v: i64) -> DataFrameRow {
        DataFrameRow::new([("v", Value::Int(v))])
    }

    /// The key of row `id`.
    fn key(id: i64) -> DataFrameKey {
        DataFrameKey::new([("id", Value::Int(id))])
    }

    /// A raw operation of subscription `sub` (root part) for client `client`.
    fn raw(client: u64, sub: u64, op: DataFrameOperation) -> Raw {
        Raw {
            client: ClientId(client),
            table: TableName::from("t"),
            target: Target {
                sub: SubId(sub),
                part: QueryPart::main(),
            },
            op,
        }
    }

    /// Two subscriptions of one client holding one row: an in-place change
    /// (the pair for each) becomes one `Add` naming both; a third
    /// subscription that lost the row is one `Delete`; another client's
    /// operations stay apart.
    #[test]
    fn folds_per_client_and_row() {
        let out = group(vec![
            raw(1, 10, DataFrameOperation::Delete(key(1), image(1))),
            raw(1, 10, DataFrameOperation::Add(key(1), image(2))),
            raw(1, 11, DataFrameOperation::Delete(key(1), image(1))),
            raw(1, 11, DataFrameOperation::Add(key(1), image(2))),
            raw(1, 12, DataFrameOperation::Delete(key(1), image(1))),
            raw(2, 20, DataFrameOperation::Add(key(1), image(2))),
        ]);
        assert_eq!(out.len(), 3, "{out:?}");
        assert_eq!(out[0].client, ClientId(1));
        assert!(matches!(&out[0].op, DataFrameOperation::Delete(_, img) if *img == image(1)));
        assert_eq!(out[0].targets.len(), 1);
        assert_eq!(out[0].targets[0].sub, SubId(12));
        assert!(matches!(&out[1].op, DataFrameOperation::Add(_, img) if *img == image(2)));
        assert_eq!(
            out[1].targets.iter().map(|t| t.sub).collect::<Vec<_>>(),
            vec![SubId(10), SubId(11)]
        );
        assert_eq!(out[2].client, ClientId(2));
    }

    /// A row admitted and evicted within one step keeps its `Add` before its
    /// `Delete`, and rows keep their first-appearance order.
    #[test]
    fn keeps_admit_then_evict_and_row_order() {
        let out = group(vec![
            raw(1, 10, DataFrameOperation::Add(key(5), image(5))),
            raw(1, 10, DataFrameOperation::Add(key(9), image(9))),
            raw(1, 10, DataFrameOperation::Delete(key(9), image(9))),
        ]);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].op.key(), &key(5));
        assert!(matches!(out[1].op, DataFrameOperation::Add(..)));
        assert!(matches!(out[2].op, DataFrameOperation::Delete(..)));
        assert_eq!(out[2].op.key(), &key(9));
    }
}
