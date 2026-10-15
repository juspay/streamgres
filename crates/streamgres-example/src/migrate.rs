//! Schema changes, one of each kind the engine treats differently.
//!
//! The engine hears of a migration through the schema-change trigger,
//! in the WAL, at the position of the change. A column added with a
//! constant default it absorbs: held rows are widened and nothing
//! restarts (`widen`). A new enum type and a column of it it absorbs
//! too, but the type's OID is not in the map the engine read at start,
//! so a bind of that type is refused until the engine restarts
//! (`new-type`). A column dropped it cannot follow: the engine stops,
//! and restarted serves the schema as it is (`drop`, which also undoes
//! the other two).

use diesel::QueryResult;
use diesel::connection::SimpleConnection;

/// One migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// `tags text NOT NULL DEFAULT ''` on tickets: absorbed.
    Widen,
    /// The enum `ticket_kind` and `kind ticket_kind` on tickets, filled
    /// in: absorbed, but binds of the type are refused until a restart.
    NewType,
    /// Drops both columns and the type: the engine stops.
    Drop,
}

impl Step {
    /// A step by its command-line name.
    pub fn parse(name: &str) -> Option<Step> {
        match name {
            "widen" => Some(Step::Widen),
            "new-type" => Some(Step::NewType),
            "drop" => Some(Step::Drop),
            _ => None,
        }
    }

    /// The SQL the step runs.
    pub fn sql(self) -> &'static str {
        match self {
            Step::Widen => "ALTER TABLE tickets ADD COLUMN IF NOT EXISTS tags text NOT NULL DEFAULT ''",
            Step::NewType => "
                DO $$ BEGIN
                    CREATE TYPE ticket_kind AS ENUM ('bug', 'task', 'question');
                EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$;
                ALTER TABLE tickets ADD COLUMN IF NOT EXISTS kind ticket_kind;
                UPDATE tickets SET kind = (ARRAY['bug', 'task', 'question'])[1 + id % 3]::ticket_kind
                 WHERE kind IS NULL",
            Step::Drop => "
                ALTER TABLE tickets DROP COLUMN IF EXISTS tags;
                ALTER TABLE tickets DROP COLUMN IF EXISTS kind;
                DROP TYPE IF EXISTS ticket_kind",
        }
    }

    /// What to expect of the engine after the step.
    pub fn expectation(self) -> &'static str {
        match self {
            Step::Widen => {
                "the engine absorbs the column; cached reads keep working and keep being served from rows"
            }
            Step::NewType => {
                "the engine absorbs the column, but `of_kind` (a bind of the new type) is refused and served live until the engine restarts"
            }
            Step::Drop => {
                "the engine cannot follow a dropped column: it stops, and restarted serves the schema as it is; reads go to Postgres meanwhile"
            }
        }
    }

    /// Runs the step on a blocking connection.
    pub fn run<C: SimpleConnection>(self, conn: &mut C) -> QueryResult<()> {
        conn.batch_execute(self.sql())
    }
}
