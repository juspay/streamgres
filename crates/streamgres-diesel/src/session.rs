//! What a connection's session is, as far as the cache must know: whether
//! it is checked as a reader ([`streamgres::cache::Reader`]) and still as
//! it was when checked, and whether a transaction it opened in raw SQL is
//! open.
//!
//! A [`Tracker`] is the connection's Diesel instrumentation. It sees every
//! statement the connection runs, so it learns when the session may have
//! changed in a way that changes what Postgres would answer (`SET`, `SET
//! ROLE`, `RESET`, `DISCARD`, `set_config`, schema changes, privileges)
//! and when a raw `BEGIN` opens a transaction. The adapter finds a
//! connection's tracker by sending its instrumentation a probe: the
//! tracker answers by leaving its session where the adapter, on the same
//! thread, picks it up. A connection without a tracker is never answered
//! from the cache.

use std::cell::RefCell;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use streamgres::cache::Reader;

use crate::diesel::connection::{Instrumentation, InstrumentationEvent};

/// How long a reader's check stands without a statement that could have
/// changed it: a change made by another session (a grant revoked, a table
/// created earlier in the search path) is seen within this.
pub(crate) const RECHECK_EVERY: Duration = Duration::from_secs(10);

/// One connection's session as the cache needs it.
#[derive(Debug, Default)]
pub(crate) struct Session {
    /// The reader the connection was last checked as, and when.
    pub reader: Option<(Reader, Instant)>,
    /// A statement since the check may have changed what it would read.
    pub changed: bool,
    /// A transaction the connection opened in SQL of its own is open.
    pub in_transaction: bool,
}

impl Session {
    /// The reader to read as now, if the check still stands.
    pub fn current_reader(&self) -> Option<&Reader> {
        match &self.reader {
            Some((reader, at)) if !self.changed && at.elapsed() < RECHECK_EVERY => Some(reader),
            _ => None,
        }
    }

    /// Takes in a statement the connection is about to run.
    fn saw(&mut self, sql: &str) {
        for statement in sql.split(';') {
            let words: Vec<String> = statement
                .split_whitespace()
                .take(2)
                .map(str::to_ascii_lowercase)
                .collect();
            let first = words.first().map(String::as_str).unwrap_or("");
            let second = words.get(1).map(String::as_str).unwrap_or("");
            match (first, second) {
                ("begin" | "start", _) => self.in_transaction = true,
                ("commit" | "end" | "abort", _) => self.in_transaction = false,
                ("rollback", "to") => {}
                ("rollback", _) => self.in_transaction = false,
                ("prepare", "transaction") => self.in_transaction = false,
                ("set", "local" | "transaction" | "constraints") => {}
                (
                    "set" | "reset" | "discard" | "call" | "do" | "grant" | "revoke" | "create"
                    | "alter" | "drop" | "comment" | "security" | "load",
                    _,
                ) => self.changed = true,
                _ => {}
            }
        }
        if contains_ignoring_case(sql, "set_config") {
            self.changed = true;
        }
    }
}

/// Whether `text` holds `word`, in any case.
fn contains_ignoring_case(text: &str, word: &str) -> bool {
    text.as_bytes()
        .windows(word.len())
        .any(|window| window.eq_ignore_ascii_case(word.as_bytes()))
}

/// What the probe's text starts with: no statement does.
const PROBE: &str = "\u{0}streamgres-diesel probe";

/// The statement-shaped value the probe sends.
struct Probe;

impl fmt::Display for Probe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(PROBE)
    }
}

impl fmt::Debug for Probe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(PROBE)
    }
}

impl crate::diesel::connection::DebugQuery for Probe {}

thread_local! {
    /// Where a tracker leaves its session for the probe that asked.
    static ANSWER: RefCell<Option<Arc<Mutex<Session>>>> = const { RefCell::new(None) };
}

/// A connection's instrumentation that keeps its [`Session`], passing
/// every event on to the instrumentation it wraps, if any.
pub struct Tracker {
    session: Arc<Mutex<Session>>,
    inner: Option<Box<dyn Instrumentation>>,
}

impl Tracker {
    /// A tracker of its own.
    pub fn new() -> Self {
        Tracker {
            session: Arc::default(),
            inner: None,
        }
    }

    /// A tracker passing every event on to `inner` as well.
    pub fn wrapping(inner: impl Instrumentation) -> Self {
        Tracker {
            session: Arc::default(),
            inner: Some(Box::new(inner)),
        }
    }
}

impl Default for Tracker {
    fn default() -> Self {
        Self::new()
    }
}

impl Instrumentation for Tracker {
    fn on_connection_event(&mut self, event: InstrumentationEvent<'_>) {
        if let InstrumentationEvent::StartQuery { query, .. } = &event {
            let sql = statement_text(*query);
            if sql == PROBE {
                ANSWER.with(|answer| *answer.borrow_mut() = Some(self.session.clone()));
                return;
            }
            lock(&self.session).saw(&sql);
        }
        if let Some(inner) = &mut self.inner {
            inner.on_connection_event(event);
        }
    }
}

/// The most of a statement's text the tracker reads.
const TEXT_LIMIT: usize = 4096;

/// Where Diesel's text of a query turns from the SQL to its binds.
const BINDS_MARK: &str = " -- binds: ";

/// The SQL of a statement, at most [`TEXT_LIMIT`] bytes of it, without
/// the binds Diesel writes after it: formatting stops there, so binds are
/// never formatted.
fn statement_text(query: &dyn crate::diesel::connection::DebugQuery) -> String {
    struct Head(String);
    impl fmt::Write for Head {
        fn write_str(&mut self, text: &str) -> fmt::Result {
            self.0.push_str(text);
            if let Some(at) = self.0.find(BINDS_MARK) {
                self.0.truncate(at);
                return Err(fmt::Error);
            }
            if self.0.len() >= TEXT_LIMIT {
                return Err(fmt::Error);
            }
            Ok(())
        }
    }
    let mut head = Head(String::new());
    let _ = fmt::write(&mut head, format_args!("{query}"));
    head.0
}

/// The session behind its lock; every change to it is whole.
pub(crate) fn lock(session: &Mutex<Session>) -> MutexGuard<'_, Session> {
    session
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The session of the connection whose instrumentation is
/// `instrumentation`; `None` when it is not a [`Tracker`].
pub(crate) fn session_of(instrumentation: &mut dyn Instrumentation) -> Option<Arc<Mutex<Session>>> {
    ANSWER.with(|answer| answer.borrow_mut().take());
    instrumentation.on_connection_event(InstrumentationEvent::start_query(&Probe));
    ANSWER.with(|answer| answer.borrow_mut().take())
}

/// Makes every connection opened from now on tracked, so it can be
/// answered from the cache.
pub fn install() -> crate::diesel::QueryResult<()> {
    crate::diesel::connection::set_default_instrumentation(|| Some(Box::new(Tracker::new())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_that_change_the_session_are_seen() {
        let mut session = Session::default();
        session.saw("SELECT \"orders\".\"id\" FROM \"orders\" WHERE \"orders\".\"id\" = $1");
        assert!(!session.changed);
        session.saw("SET LOCAL statement_timeout = 10");
        assert!(
            !session.changed,
            "a local setting ends with its transaction"
        );
        for changing in [
            "SET search_path = tenant, public",
            "set role reporting",
            "RESET ALL",
            "DISCARD ALL",
            "SELECT set_config('search_path', 'x', false)",
            "CREATE TABLE orders (id int8)",
            "GRANT SELECT ON orders TO someone",
        ] {
            let mut session = Session::default();
            session.saw(changing);
            assert!(session.changed, "{changing}");
        }
    }

    #[test]
    fn raw_transactions_are_followed() {
        let mut session = Session::default();
        session.saw("BEGIN");
        assert!(session.in_transaction);
        session.saw("ROLLBACK TO SAVEPOINT a");
        assert!(session.in_transaction);
        session.saw("COMMIT");
        assert!(!session.in_transaction);
        session.saw("START TRANSACTION ISOLATION LEVEL REPEATABLE READ; SELECT 1");
        assert!(session.in_transaction);
        session.saw("ROLLBACK");
        assert!(!session.in_transaction);
    }
}
