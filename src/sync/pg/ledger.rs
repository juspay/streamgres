//! The xid ledger: the transaction id and commit location of every
//! transaction the change feed has delivered, in delivery (commit) order.
//! It is how the XID method turns a snapshot's `(xmin, xmax, xip)` into a
//! WAL location without asking Postgres for one: a delivered transaction
//! the snapshot does not see (in progress at snapshot time, or assigned
//! after it) is invisible, and everything delivered before the earliest
//! such transaction is visible, so the read's location is the one just
//! below it, or the newest delivered location when nothing delivered is
//! invisible. Writes above that location are brought up to the engine by
//! the runtime before the read lands; visible ones among them are already
//! in the result and reapply as no-ops.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use crate::model::Lsn;

/// A ledger shared between the poller that writes it and the storage
/// that converts with it, on the engine's thread.
pub type SharedLedger = Rc<RefCell<XidLedger>>;

/// The delivered transactions, bounded to the most recent `capacity`.
///
/// - `entries`: (epoch-extended xid, commit location), oldest first.
/// - `by_xid`: the same, by xid.
/// - `capacity`: how many to keep; an invisible transaction is always a
///   recent one (it was still running when the snapshot was taken, or
///   started after), so a bounded window loses nothing that matters.
pub struct XidLedger {
    entries: VecDeque<(u64, Lsn)>,
    by_xid: HashMap<u64, Lsn>,
    capacity: usize,
}

impl XidLedger {
    /// An empty ledger keeping the most recent `capacity` transactions.
    pub fn new(capacity: usize) -> Self {
        XidLedger {
            entries: VecDeque::new(),
            by_xid: HashMap::new(),
            capacity: capacity.max(1),
        }
    }

    /// A shared empty ledger with room for 65 536 transactions.
    pub fn shared() -> SharedLedger {
        Rc::new(RefCell::new(XidLedger::new(1 << 16)))
    }

    /// Record one delivered transaction.
    pub fn record(&mut self, xid: u64, commit: Lsn) {
        self.entries.push_back((xid, commit));
        self.by_xid.insert(xid, commit);
        while self.entries.len() > self.capacity {
            if let Some((old, _)) = self.entries.pop_front() {
                self.by_xid.remove(&old);
            }
        }
    }

    /// The commit location of a delivered transaction.
    pub fn commit_of(&self, xid: u64) -> Option<Lsn> {
        self.by_xid.get(&xid).copied()
    }

    /// The newest delivered commit location.
    pub fn newest(&self) -> Option<Lsn> {
        self.entries.back().map(|(_, lsn)| *lsn)
    }

    /// How many transactions the ledger holds.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether nothing has been delivered yet.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The location a snapshot with the given `xmax` and in-progress ids
    /// reflects every commit up to: just below the earliest delivered
    /// transaction the snapshot does not see, or the newest delivered
    /// location when it sees every delivered one. `None` when nothing has
    /// been delivered, in which case location zero is the safe answer.
    pub fn position_of(&self, xmax: u64, xip: &[u64]) -> Option<Lsn> {
        let newest = self.newest()?;
        let earliest_invisible = self
            .entries
            .iter()
            .filter(|(xid, _)| *xid >= xmax || xip.contains(xid))
            .map(|(_, lsn)| *lsn)
            .min();
        Some(match earliest_invisible {
            Some(lsn) => Lsn(lsn.0.saturating_sub(1)).min(newest),
            None => newest,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// With every delivered transaction visible the position is the
    /// newest delivered location; an invisible one pulls it just below
    /// itself; an empty ledger answers nothing.
    #[test]
    fn converts_visibility_to_a_location() {
        let mut ledger = XidLedger::new(8);
        assert_eq!(ledger.position_of(100, &[]), None);
        ledger.record(90, Lsn(10));
        ledger.record(91, Lsn(20));
        ledger.record(92, Lsn(30));
        assert_eq!(ledger.position_of(100, &[]), Some(Lsn(30)));
        assert_eq!(ledger.position_of(92, &[]), Some(Lsn(29)), "92 was assigned after the snapshot");
        assert_eq!(ledger.position_of(100, &[91]), Some(Lsn(19)), "91 was still running");
        assert_eq!(ledger.commit_of(91), Some(Lsn(20)));
    }

    /// The window is bounded and drops the oldest entries first.
    #[test]
    fn keeps_a_bounded_window() {
        let mut ledger = XidLedger::new(2);
        ledger.record(1, Lsn(1));
        ledger.record(2, Lsn(2));
        ledger.record(3, Lsn(3));
        assert_eq!(ledger.len(), 2);
        assert_eq!(ledger.commit_of(1), None);
        assert_eq!(ledger.newest(), Some(Lsn(3)));
    }
}
