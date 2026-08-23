//! Operation counters for the IVM engine.
//!
//! The whole point of the reverse index is to make routing a write cheaper
//! than re-checking every registered query. These counters make that
//! claim measurable: how many conditions were probed, how many candidates
//! survived, how many full predicate evaluations were actually paid for.
//! The demo binary (`src/main.rs`) prints a per-write diff of them.

use std::fmt;

/// Cumulative counters, monotonically increasing over an [`crate::ivm::IVM`]'s
/// lifetime. Use [`IvmStats::diff`] to isolate the cost of a single write.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IvmStats {
    // -- registration ----------------------------------------------------
    /// Queries registered via `register_query`.
    pub queries_registered: u64,
    /// Leaf conditions inserted into the reverse index (one per condition
    /// per subscribing query).
    pub conditions_indexed: u64,

    // -- write routing ---------------------------------------------------
    /// Writes processed by `incremental_update`.
    pub writes_processed: u64,
    /// Reverse-index conditions probed against a write's row image.
    pub index_probes: u64,
    /// Probes that matched, promoting their query to candidate.
    pub index_hits: u64,
    /// Registered queries checked for currently holding the written row.
    pub membership_probes: u64,
    /// Membership checks that found the row, promoting the query to candidate.
    pub membership_hits: u64,
    /// Candidates whose full `Where` tree was then evaluated.
    pub full_evaluations: u64,
    /// Total leaf conditions evaluated (index probes + full evaluations;
    /// short-circuiting means this can be less than tree size).
    pub conditions_evaluated: u64,
    /// Candidates confirmed as impacted.
    pub queries_impacted: u64,

    // -- emitted operations ----------------------------------------------
    /// `Add` operations emitted (row entered a result set, or changed in place).
    pub ops_add: u64,
    /// `Delete` operations emitted (row left a result set).
    pub ops_delete: u64,
}

impl IvmStats {
    /// Counter movement since `earlier` — the cost of the work done between
    /// the two snapshots.
    pub fn diff(&self, earlier: &IvmStats) -> IvmStats {
        IvmStats {
            queries_registered: self.queries_registered - earlier.queries_registered,
            conditions_indexed: self.conditions_indexed - earlier.conditions_indexed,
            writes_processed: self.writes_processed - earlier.writes_processed,
            index_probes: self.index_probes - earlier.index_probes,
            index_hits: self.index_hits - earlier.index_hits,
            membership_probes: self.membership_probes - earlier.membership_probes,
            membership_hits: self.membership_hits - earlier.membership_hits,
            full_evaluations: self.full_evaluations - earlier.full_evaluations,
            conditions_evaluated: self.conditions_evaluated - earlier.conditions_evaluated,
            queries_impacted: self.queries_impacted - earlier.queries_impacted,
            ops_add: self.ops_add - earlier.ops_add,
            ops_delete: self.ops_delete - earlier.ops_delete,
        }
    }

    /// Compact single-line rendering of the routing counters, used by the
    /// demo for per-write output.
    pub fn routing_summary(&self) -> String {
        format!(
            "index {}/{} hit, membership {}/{} hit, {} full evals, {} condition evals, {} impacted, ops +{}/-{}",
            self.index_hits,
            self.index_probes,
            self.membership_hits,
            self.membership_probes,
            self.full_evaluations,
            self.conditions_evaluated,
            self.queries_impacted,
            self.ops_add,
            self.ops_delete,
        )
    }
}

impl fmt::Display for IvmStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "queries registered ......... {}", self.queries_registered)?;
        writeln!(f, "conditions indexed ......... {}", self.conditions_indexed)?;
        writeln!(f, "writes processed ........... {}", self.writes_processed)?;
        writeln!(f, "index probes / hits ........ {} / {}", self.index_probes, self.index_hits)?;
        writeln!(f, "membership probes / hits ... {} / {}", self.membership_probes, self.membership_hits)?;
        writeln!(f, "full evaluations ........... {}", self.full_evaluations)?;
        writeln!(f, "conditions evaluated ....... {}", self.conditions_evaluated)?;
        writeln!(f, "queries impacted ........... {}", self.queries_impacted)?;
        write!(f, "ops emitted ................ {} adds, {} deletes", self.ops_add, self.ops_delete)
    }
}
