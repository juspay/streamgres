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
//! connection's tracker by its type, so the tracker must be the
//! connection's instrumentation itself, with any other instrumentation
//! inside it ([`Tracker::wrapping`]). A connection without a tracker is
//! never answered from the cache.
//!
//! What the tracker cannot see is a change made where no statement names
//! it: a function that runs `SET` inside, a grant revoked by another
//! session. The adapter checks every reader again after
//! [`RECHECK_EVERY`], which bounds how long such a change goes unseen.

use std::fmt;
use std::time::{Duration, Instant};

use streamgres::cache::Reader;

use crate::diesel::connection::{Instrumentation, InstrumentationEvent};

/// How long a reader's check stands without a statement that could have
/// changed it.
pub const RECHECK_EVERY: Duration = Duration::from_secs(10);

/// When a connection is checked again as a reader, besides every
/// [`RECHECK_EVERY`].
///
/// A check is one round trip, paid by the next cached read. The choice is
/// between never missing a change of the session and not paying that
/// round trip after statements that change nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Recheck {
    /// After every statement the connection sends to the database. No
    /// change of the session is missed, a function that runs `SET`
    /// inside included. A cached read that follows a database statement
    /// costs one round trip; cached reads that follow one another cost
    /// none.
    #[default]
    AfterEveryStatement,
    /// After the statements that name a change: `SET`, `RESET`,
    /// `DISCARD`, `set_config`, `CALL`, `DO`, schema changes, grants. A
    /// change made inside a function a query calls (a `set_tenant()` that
    /// sets `search_path`, a `SET ROLE` in a trigger) is not seen for up
    /// to [`RECHECK_EVERY`], and reads in that time may be answered for
    /// the session as it was. Only for an application that changes its
    /// session's `search_path` and role in plain statements alone.
    AfterSessionStatements,
}

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

    /// Takes in the SQL the connection is about to run: one statement or
    /// several.
    fn saw(&mut self, sql: &str) {
        each_statement(sql, |words| self.saw_statement(words));
        if contains_ignoring_case(sql, "set_config") {
            self.changed = true;
        }
    }

    /// Takes in one statement by its leading keywords.
    fn saw_statement(&mut self, words: &[&str]) {
        let word = |at: usize| words.get(at).copied().unwrap_or("");
        let is = |at: usize, options: &[&str]| {
            options
                .iter()
                .any(|option| word(at).eq_ignore_ascii_case(option))
        };
        let has = |keyword: &str| words.iter().any(|word| word.eq_ignore_ascii_case(keyword));
        if is(0, &["begin", "start"]) {
            self.in_transaction = true;
        } else if is(0, &["rollback"])
            && (is(1, &["to"]) || (is(1, &["work", "transaction"]) && is(2, &["to"])))
        {
            // Back to a savepoint: the transaction stays open.
        } else if is(0, &["commit", "rollback"]) && is(1, &["prepared"]) {
            // Ends a transaction prepared earlier, not this session's.
        } else if is(0, &["commit", "end", "abort", "rollback"]) {
            // `AND CHAIN` opens the next transaction at once.
            self.in_transaction = has("chain") && !has("no");
        } else if is(0, &["prepare"]) && is(1, &["transaction"]) {
            self.in_transaction = false;
        } else if is(0, &["set"]) && is(1, &["local", "transaction", "constraints"]) {
            // Ends with the transaction it is set in.
        } else if is(
            0,
            &[
                "set", "reset", "discard", "call", "do", "grant", "revoke", "create", "alter",
                "drop", "comment", "security", "load",
            ],
        ) {
            self.changed = true;
        }
    }
}

/// The most leading keywords of a statement [`each_statement`] hands
/// over: enough for `COMMIT TRANSACTION AND NO CHAIN`.
const LEADING_WORDS: usize = 6;

/// Calls `each` with the leading keywords of every statement of `sql`.
/// Statements end at a `;` outside comments, quoted text and dollar
/// quotes, none of whose contents are read as keywords.
fn each_statement<'s>(sql: &'s str, mut each: impl FnMut(&[&'s str])) {
    let bytes = sql.as_bytes();
    let is_word = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
    let mut words: Vec<&'s str> = Vec::with_capacity(LEADING_WORDS);
    let mut at = 0;
    while let Some(&byte) = bytes.get(at) {
        let next = bytes.get(at + 1).copied();
        match byte {
            b'-' if next == Some(b'-') => {
                at = find(bytes, at, b"\n").unwrap_or(bytes.len());
            }
            b'/' if next == Some(b'*') => {
                let mut depth = 1;
                at += 2;
                while depth > 0 && at < bytes.len() {
                    if bytes[at..].starts_with(b"/*") {
                        depth += 1;
                        at += 2;
                    } else if bytes[at..].starts_with(b"*/") {
                        depth -= 1;
                        at += 2;
                    } else {
                        at += 1;
                    }
                }
            }
            b'\'' | b'"' => {
                // An `E'...'` string escapes with a backslash as well.
                let escapes = byte == b'\''
                    && at > 0
                    && bytes[at - 1].eq_ignore_ascii_case(&b'e')
                    && (at < 2 || !is_word(bytes[at - 2]));
                at += 1;
                while let Some(&inside) = bytes.get(at) {
                    at += 1;
                    if inside == b'\\' && escapes {
                        at += 1;
                    } else if inside == byte {
                        if bytes.get(at) == Some(&byte) {
                            at += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            b'$' if at == 0 || !is_word(bytes[at - 1]) => {
                // `$tag$ ... $tag$`; a `$1` parameter has no closing `$`.
                let tag_end = bytes[at + 1..]
                    .iter()
                    .position(|byte| !is_word(*byte))
                    .map(|length| at + 1 + length);
                match tag_end {
                    Some(end)
                        if bytes.get(end) == Some(&b'$')
                            && !bytes.get(at + 1).is_some_and(u8::is_ascii_digit) =>
                    {
                        let tag = &bytes[at..=end];
                        at = find(bytes, end + 1, tag)
                            .map_or(bytes.len(), |close| close + tag.len());
                    }
                    _ => at += 1,
                }
            }
            b';' => {
                each(&words);
                words.clear();
                at += 1;
            }
            _ if is_word(byte) => {
                let end = bytes[at..]
                    .iter()
                    .position(|byte| !is_word(*byte))
                    .map_or(bytes.len(), |length| at + length);
                if words.len() < LEADING_WORDS
                    && let Some(word) = sql.get(at..end)
                {
                    words.push(word);
                }
                at = end;
            }
            _ => at += 1,
        }
    }
    each(&words);
}

/// Where `needle` next starts in `bytes` at or after `from`.
fn find(bytes: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    bytes
        .get(from..)?
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|at| from + at)
}

/// Whether `text` holds `word`, in any case.
fn contains_ignoring_case(text: &str, word: &str) -> bool {
    text.as_bytes()
        .windows(word.len())
        .any(|window| window.eq_ignore_ascii_case(word.as_bytes()))
}

/// A connection's instrumentation that keeps its session for the cache,
/// passing every event on to the instrumentation it wraps, if any.
///
/// Give it to a connection with `set_instrumentation`, or to every new
/// connection with [`install`] or Diesel's `set_default_instrumentation`.
/// It must be the connection's instrumentation itself: an instrumentation
/// of the application's own goes inside it ([`Tracker::wrapping`]), not
/// around it.
pub struct Tracker {
    pub(crate) session: Session,
    inner: Option<Box<dyn Instrumentation>>,
    /// The statement being looked at; kept to reuse its space.
    text: String,
    /// A failed COMMIT must not make an open transaction look closed.
    transaction_before: bool,
    recheck: Recheck,
}

impl Tracker {
    /// A tracker of its own.
    pub fn new() -> Self {
        Tracker {
            session: Session::default(),
            inner: None,
            text: String::new(),
            transaction_before: false,
            recheck: Recheck::default(),
        }
    }

    /// This tracker checking its reader again by `recheck`.
    pub fn rechecking(mut self, recheck: Recheck) -> Self {
        self.recheck = recheck;
        self
    }

    /// A tracker passing every event on to `inner` as well.
    pub fn wrapping(inner: impl Instrumentation) -> Self {
        Tracker {
            inner: Some(Box::new(inner)),
            ..Tracker::new()
        }
    }
}

impl Tracker {
    /// Cache hits are read-only; expose balanced events to the host without
    /// treating them as database statements that may have changed the session.
    pub(crate) fn cache_event(&mut self, event: InstrumentationEvent<'_>) {
        if let Some(inner) = &mut self.inner {
            inner.on_connection_event(event);
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
            self.transaction_before = self.session.in_transaction;
            self.text.clear();
            let _ = fmt::write(&mut UpToBinds(&mut self.text), format_args!("{query}"));
            self.session.saw(&self.text);
            // Even a SELECT may call a function that changes the session;
            // cache hits never come this way.
            if self.recheck == Recheck::AfterEveryStatement {
                self.session.changed = true;
            }
        }
        if let InstrumentationEvent::FinishQuery { error: Some(_), .. } = &event {
            self.session.in_transaction |= self.transaction_before;
            // A batch can fail after an earlier statement changed the session.
            self.session.changed = true;
        }
        if let Some(inner) = &mut self.inner {
            inner.on_connection_event(event);
        }
    }
}

/// What Diesel writes between a query's SQL and its binds.
const BINDS_MARK: &str = " -- binds: ";

/// Takes a query's text up to where its binds start: Diesel writes the
/// SQL, then [`BINDS_MARK`] as a piece of its own, then the binds, which
/// are never formatted since writing stops at the mark.
struct UpToBinds<'t>(&'t mut String);

impl fmt::Write for UpToBinds<'_> {
    fn write_str(&mut self, piece: &str) -> fmt::Result {
        if piece == BINDS_MARK {
            return Err(fmt::Error);
        }
        self.0.push_str(piece);
        Ok(())
    }
}

/// The tracker of the connection whose instrumentation is
/// `instrumentation`; `None` when it is not a [`Tracker`].
pub(crate) fn tracker_of(instrumentation: &mut dyn Instrumentation) -> Option<&mut Tracker> {
    instrumentation.downcast_mut::<Tracker>()
}

/// A balanced cache-attempt event pair, including when an async caller
/// cancels its future. The application sees the attempt; session tracking
/// knows no database statement ran.
pub(crate) struct CacheEvents<'a, 'q> {
    tracker: Option<&'a mut Tracker>,
    query: crate::diesel::connection::StrQueryHelper<'q>,
    finished: bool,
}
impl<'a, 'q> CacheEvents<'a, 'q> {
    pub(crate) fn new(instrumentation: &'a mut dyn Instrumentation, sql: &'q str) -> Self {
        let mut events = Self {
            tracker: tracker_of(instrumentation),
            query: crate::diesel::connection::StrQueryHelper::new(sql),
            finished: false,
        };
        if let Some(tracker) = &mut events.tracker {
            tracker.cache_event(InstrumentationEvent::start_query(&events.query));
        }
        events
    }
    pub(crate) fn finish(mut self, error: Option<&crate::diesel::result::Error>) {
        self.finished = true;
        if let Some(tracker) = &mut self.tracker {
            tracker.cache_event(InstrumentationEvent::finish_query(&self.query, error));
        }
    }
}
impl Drop for CacheEvents<'_, '_> {
    fn drop(&mut self) {
        if !self.finished {
            let error = crate::diesel::result::Error::QueryBuilderError(
                "the cached read was cancelled".into(),
            );
            if let Some(tracker) = &mut self.tracker {
                tracker.cache_event(InstrumentationEvent::finish_query(
                    &self.query,
                    Some(&error),
                ));
            }
        }
    }
}

/// Makes every connection opened from now on tracked, so it can be
/// answered from the cache.
///
/// Refuses to replace application instrumentation already installed.
/// An application with a default of its own sets it itself, wrapped:
///
/// ```ignore
/// diesel::connection::set_default_instrumentation(|| {
///     Some(Box::new(Tracker::wrapping(MyInstrumentation::new())))
/// })?;
/// ```
pub fn install() -> crate::diesel::QueryResult<()> {
    if let Some(existing) = crate::diesel::connection::get_default_instrumentation() {
        if existing.is::<Tracker>() {
            return Ok(());
        }
        return Err(crate::diesel::result::Error::QueryBuilderError(
            "Diesel already has default instrumentation; wrap it in Tracker::wrapping in your factory".into()));
    }
    crate::diesel::connection::set_default_instrumentation(|| Some(Box::new(Tracker::new())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn after(sql: &str) -> Session {
        let mut session = Session::default();
        session.saw(sql);
        session
    }

    #[test]
    fn statements_that_change_the_session_are_seen() {
        assert!(
            !after("SELECT \"orders\".\"id\" FROM \"orders\" WHERE \"orders\".\"id\" = $1").changed
        );
        assert!(
            !after("SET LOCAL statement_timeout = 10").changed,
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
            "/* tenant 7 */ SET search_path = tenant",
            "-- switch\nSET ROLE reporting",
            "SELECT 1; \n SET search_path = other",
            "  \t\nSET\nsearch_path = other",
        ] {
            assert!(after(changing).changed, "{changing}");
        }
    }

    #[test]
    fn what_is_quoted_or_commented_is_not_a_statement() {
        for inert in [
            "SELECT 'x; SET search_path = other'",
            "SELECT \"set; SET role\" FROM t",
            "SELECT 1 -- ; SET search_path = other",
            "SELECT 1 /* ; SET /* nested */ role x */",
            "SELECT $body$ ; SET search_path = x $body$",
            "SELECT $$ ; BEGIN $$, $1",
            "SELECT E'it\\'s; SET role x'",
            "SELECT 'it''s; SET role x'",
            "INSERT INTO t VALUES ($1, $2); SELECT 1",
        ] {
            let session = after(inert);
            assert!(!session.changed && !session.in_transaction, "{inert}");
        }
    }

    #[test]
    fn a_statement_however_long_is_read_whole() {
        let long = format!(
            "INSERT INTO t VALUES ('{}'); SET search_path = other",
            "x".repeat(100_000)
        );
        assert!(after(&long).changed);
    }

    #[test]
    fn raw_transactions_are_followed() {
        let mut session = Session::default();
        session.saw("BEGIN");
        assert!(session.in_transaction);
        session.saw("ROLLBACK TO SAVEPOINT a");
        assert!(session.in_transaction);
        session.saw("ROLLBACK WORK TO SAVEPOINT a");
        assert!(session.in_transaction);
        session.saw("ROLLBACK TRANSACTION TO a");
        assert!(session.in_transaction);
        session.saw("COMMIT");
        assert!(!session.in_transaction);
        session.saw("START TRANSACTION ISOLATION LEVEL REPEATABLE READ; SELECT 1");
        assert!(session.in_transaction);
        session.saw("COMMIT AND CHAIN");
        assert!(session.in_transaction, "the next transaction is open");
        session.saw("ROLLBACK TRANSACTION AND NO CHAIN");
        assert!(!session.in_transaction);
        session.saw("/* retry */ begin; update t set x = 1");
        assert!(session.in_transaction);
        session.saw("END");
        assert!(!session.in_transaction);
        session.saw("COMMIT PREPARED 'other'");
        assert!(!session.in_transaction);
    }

    /// Under the default every database statement asks for a new check;
    /// under `AfterSessionStatements` only the ones that name a change.
    #[test]
    fn the_recheck_choice_decides_what_a_plain_statement_does() {
        use crate::diesel::connection::{Instrumentation, InstrumentationEvent, StrQueryHelper};
        let ran = |tracker: &mut Tracker, sql: &str| {
            tracker.session.changed = false;
            tracker.on_connection_event(InstrumentationEvent::start_query(&StrQueryHelper::new(
                sql,
            )));
            tracker.session.changed
        };
        let mut strict = Tracker::new();
        assert!(ran(&mut strict, "SELECT set_tenant($1)"));
        let mut relaxed = Tracker::new().rechecking(Recheck::AfterSessionStatements);
        assert!(!ran(&mut relaxed, "SELECT set_tenant($1)"));
        assert!(ran(&mut relaxed, "SET search_path = tenant"));
    }

    #[test]
    fn the_text_stops_where_the_binds_start() {
        use std::fmt::Write;
        let mut text = String::new();
        let query = crate::diesel::sql_query("SELECT $1")
            .bind::<crate::diesel::sql_types::Text, _>("BEGIN");
        let _ = write!(
            UpToBinds(&mut text),
            "{}",
            crate::diesel::debug_query::<crate::diesel::pg::Pg, _>(&query)
        );
        assert_eq!(text, "SELECT $1");
    }
    #[test]
    fn cancelled_cache_attempts_finish_their_instrumentation_event() {
        use std::sync::{Arc, Mutex};
        let counts = Arc::new(Mutex::new((0, 0)));
        let captured = counts.clone();
        let mut tracker = Tracker::wrapping(move |event: InstrumentationEvent<'_>| {
            let mut counts = captured.lock().unwrap();
            match event {
                InstrumentationEvent::StartQuery { .. } => counts.0 += 1,
                InstrumentationEvent::FinishQuery { .. } => counts.1 += 1,
                _ => {}
            }
        });
        drop(CacheEvents::new(&mut tracker, "SELECT id FROM orders"));
        assert_eq!(*counts.lock().unwrap(), (1, 1));
        assert!(
            !tracker.session.changed,
            "a cache event cannot change the database session"
        );
    }

    #[test]
    fn install_preserves_an_existing_application_default() {
        crate::diesel::connection::set_default_instrumentation(|| {
            Some(Box::new(|_: InstrumentationEvent<'_>| {}))
        })
        .unwrap();
        assert!(install().is_err());
        let existing = crate::diesel::connection::get_default_instrumentation().unwrap();
        assert!(!existing.is::<Tracker>());
        crate::diesel::connection::set_default_instrumentation(|| None).unwrap();
    }
}
