use crate::catalog::{Key, Row};

/// One row change inside a transaction.
#[derive(Clone, Debug, PartialEq)]
pub enum Change {
    /// The full new row.
    Insert { table: String, row: Row },
    /// New row image, maybe missing unchanged big columns; `old_key` is set when the key changed.
    Update {
        table: String,
        old_key: Option<Key>,
        row: Row,
    },
    /// Only the key of the deleted row.
    Delete { table: String, key: Key },
    /// Every row of the table is gone.
    Truncate { table: String },
}

impl Change {
    pub fn table(&self) -> &str {
        match self {
            Change::Insert { table, .. }
            | Change::Update { table, .. }
            | Change::Delete { table, .. }
            | Change::Truncate { table } => table,
        }
    }
}

/// One committed transaction; with no changes it only reports progress.
#[derive(Clone, Debug, PartialEq)]
pub struct Txn<P> {
    pub position: P,
    pub changes: Vec<Change>,
}

impl<P> Txn<P> {
    pub fn mark(position: P) -> Self {
        Txn {
            position,
            changes: vec![],
        }
    }

    pub fn is_mark(&self) -> bool {
        self.changes.is_empty()
    }
}
