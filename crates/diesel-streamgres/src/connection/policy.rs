//! The decisions both wrappers make about a statement, as functions of
//! what they already know.
//!
//! The blocking and the async wrapper learn the same facts in different
//! ways: a query run on the connection there and then, or a future
//! pipelined with the read. What follows from those facts is the same,
//! and lives here, so a rule changes in one place. Nothing here touches
//! a connection; the I/O, cancellation and cursors stay with each
//! wrapper.

use std::sync::atomic::Ordering;
use std::time::Duration;

use diesel::pg::Pg;
use diesel::query_builder::QueryFragment;

use crate::cache::{Cache, Read};
use crate::route::{Route, Served};
use crate::session::{Effect, RECHECK_EVERY, Scope};

/// The route a read takes: the flag on the query, else the
/// [`with_route`](super::SyncedConnection::with_route) scope it runs in,
/// else the connection's own.
pub(crate) fn route(named: Option<Route>, scoped: Option<Route>, own: Route) -> Route {
    named.or(scoped).unwrap_or(own)
}

/// Whether what a statement did to the session means the context the
/// connection reads in must be read again before the cache serves it:
/// the session is back as established, or it may have changed in a way
/// the statement does not say.
pub(crate) fn rechecks(effect: Effect) -> bool {
    match effect {
        Effect::Restored | Effect::Recheck => true,
        Effect::None | Effect::Changed => false,
    }
}

/// The context a connection reads in, `since` its last reading: as read,
/// or to be read again once [`RECHECK_EVERY`] has passed.
pub(crate) fn scope_now(scope: Scope, since: Duration) -> Scope {
    if since >= RECHECK_EVERY {
        Scope::Unchecked
    } else {
        scope
    }
}

/// Why a read the cache could serve goes to Postgres instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Bypass {
    /// It locks rows (`FOR UPDATE`), which a cache cannot.
    Locking,
    /// A write through the cache may not have reached it yet.
    AfterWrites,
    /// The engine is further behind the database than reads may trust.
    Lagging,
    /// The connection's context cannot be served, or its session has
    /// changed since it was established.
    OtherContext,
    /// The connection's context is being read, with this read. Only the
    /// async connection reads it alongside the read; the blocking one
    /// reads it first.
    #[cfg_attr(not(feature = "async"), allow(dead_code))]
    ReadingContext,
    /// A bind names a type whose OID is being looked up, with this read.
    /// The blocking connection looks it up on the spot instead.
    #[cfg_attr(not(feature = "async"), allow(dead_code))]
    UnknownTypes,
    /// The engine refused the query, now or recently.
    Refused,
}

impl Bypass {
    /// Counts the bypass in the cache's stats, where it has a counter of
    /// its own, and reports the read as served by Postgres.
    pub(crate) fn record(self, cache: &Cache) {
        let counter = match self {
            Bypass::Locking => Some(&cache.stats.locking),
            Bypass::AfterWrites => Some(&cache.stats.after_write),
            Bypass::Lagging => Some(&cache.stats.lagging),
            Bypass::OtherContext => Some(&cache.stats.other_context),
            Bypass::ReadingContext | Bypass::UnknownTypes | Bypass::Refused => None,
        };
        if let Some(counter) = counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        crate::metrics::report(|metrics| metrics.served(Served::Postgres));
    }

    /// Whether the read waits for a fallback permit
    /// ([`Cache::fallback_permit`]), which bounds the reads a cache sends
    /// to Postgres at once. A locking read does not: it goes to Postgres
    /// whatever the cache holds, as it would without one.
    pub(crate) fn limited(self) -> bool {
        match self {
            Bypass::Locking => false,
            Bypass::AfterWrites
            | Bypass::Lagging
            | Bypass::OtherContext
            | Bypass::ReadingContext
            | Bypass::UnknownTypes
            | Bypass::Refused => true,
        }
    }
}

/// How far the database a refill read had replayed, as
/// [`REPLAY_POSITION_SQL`](crate::cache::REPLAY_POSITION_SQL) said.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Replayed {
    /// A primary, which is current; it need not be asked again.
    Primary,
    /// A standby, which has (`true`) or has not replayed past the last
    /// change the engine will not report for the query.
    Standby(bool),
    /// It could not say.
    Unknown,
}

impl Replayed {
    /// What `read` says of a connection, for a query whose last change
    /// the engine will not report is at `since`. A standby with no
    /// position to compare is not trusted.
    pub(crate) fn of(read: Option<Read>, since: Option<u64>) -> Replayed {
        match read {
            Some(Read::Null) => Replayed::Primary,
            Some(Read::At(replayed)) => {
                Replayed::Standby(since.is_some_and(|since| replayed >= since))
            }
            None => Replayed::Unknown,
        }
    }

    /// Whether the rows the read returned may be kept.
    pub(crate) fn keep(self) -> bool {
        match self {
            Replayed::Primary => true,
            Replayed::Standby(past) => past,
            Replayed::Unknown => false,
        }
    }
}

/// Whether `sql` ends a transaction with its writes: a `COMMIT` or an
/// `END`, as the ANSI manager and hand-written SQL send them.
pub(crate) fn commits(sql: &str) -> bool {
    let word = sql.split_whitespace().next().unwrap_or("");
    let word = word.trim_end_matches(';');
    word.eq_ignore_ascii_case("COMMIT") || word.eq_ignore_ascii_case("END")
}

/// The SQL a statement renders to, for [`may_write`]; a statement that
/// cannot be rendered is taken for a write.
pub(crate) fn rendered<T: QueryFragment<Pg>>(query: &T) -> String {
    let mut builder = diesel::pg::PgQueryBuilder::default();
    match query.to_sql(&mut builder, &Pg) {
        Ok(()) => diesel::query_builder::QueryBuilder::finish(builder),
        Err(_) => String::new(),
    }
}

/// Whether raw SQL could change what a read sees: any statement of it
/// but a plain `SELECT` (a pool's `SELECT 1` on every checkout would
/// otherwise keep the read-your-writes window open for good) and the
/// statements that only move a transaction along without committing it.
/// A batch is split on every `;`, one inside a string literal included:
/// a piece that then starts oddly counts as a write, which only sends a
/// few more reads to Postgres.
pub(crate) fn may_write(sql: &str) -> bool {
    sql.split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .any(|statement| !reads_only(statement))
}

/// Whether one statement is a plain read or only moves a transaction
/// along.
fn reads_only(statement: &str) -> bool {
    let head = statement.trim_start_matches('(');
    let word = head
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .next()
        .unwrap_or("");
    [
        "SELECT",
        "BEGIN",
        "START",
        "SAVEPOINT",
        "RELEASE",
        "ROLLBACK",
        "SET",
        "RESET",
        "DISCARD",
        "SHOW",
    ]
    .iter()
    .any(|statement| word.eq_ignore_ascii_case(statement))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::subscriptions::Unsynced;

    /// Reads and the statements that only move a transaction along or
    /// change the session are not writes; anything else, or a batch with
    /// anything else in it, is.
    #[test]
    fn what_counts_as_a_write() {
        for sql in [
            "SELECT 1",
            "(SELECT 1) UNION (SELECT 2)",
            "BEGIN",
            "START TRANSACTION",
            "SAVEPOINT a",
            "RELEASE a",
            "ROLLBACK",
            "SET app.tenant = 'a'",
            "RESET ALL",
            "DISCARD ALL",
            "SHOW search_path",
            "SET ROLE r; SELECT 1;",
        ] {
            assert!(!may_write(sql), "{sql}");
        }
        for sql in [
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "COMMIT",
            "SELECT 1; DELETE FROM t",
            "WITH gone AS (DELETE FROM t RETURNING *) SELECT * FROM gone",
            "CALL p()",
        ] {
            assert!(may_write(sql), "{sql}");
        }
    }

    /// `COMMIT` and `END` commit, however spelt; a rollback, a savepoint
    /// or a `COMMIT` inside another word does not.
    #[test]
    fn what_commits() {
        for sql in ["COMMIT", "commit;", "END", "end transaction", "  COMMIT  "] {
            assert!(commits(sql), "{sql}");
        }
        for sql in ["ROLLBACK", "RELEASE a", "COMMITTED", "SELECT 'COMMIT'", ""] {
            assert!(!commits(sql), "{sql}");
        }
    }

    /// The query's flag beats the scope, which beats the connection.
    #[test]
    fn the_nearest_route_wins() {
        use Route::{Postgres, Streamgres};
        assert_eq!(route(None, None, Postgres), Postgres);
        assert_eq!(route(None, Some(Streamgres), Postgres), Streamgres);
        assert_eq!(route(Some(Postgres), Some(Streamgres), Streamgres), Postgres);
        assert_eq!(route(Some(Streamgres), None, Postgres), Streamgres);
    }

    /// Only a session back as established, or one changed unseen, reads
    /// its context again.
    #[test]
    fn what_reads_the_context_again() {
        assert!(rechecks(Effect::Restored));
        assert!(rechecks(Effect::Recheck));
        assert!(!rechecks(Effect::Changed));
        assert!(!rechecks(Effect::None));
    }

    /// A context read stands until [`RECHECK_EVERY`] has passed.
    #[test]
    fn a_context_is_read_again_after_a_while() {
        let read = Scope::In(3);
        assert_eq!(scope_now(read, Duration::ZERO), read);
        assert_eq!(scope_now(read, RECHECK_EVERY - Duration::from_millis(1)), read);
        assert_eq!(scope_now(read, RECHECK_EVERY), Scope::Unchecked);
        assert_eq!(scope_now(Scope::Own, RECHECK_EVERY), Scope::Unchecked);
    }

    /// A primary is current; a standby only once it has replayed past
    /// the position, and never without one; a connection that cannot say
    /// is not trusted.
    #[test]
    fn what_a_refill_may_keep() {
        assert_eq!(Replayed::of(Some(Read::Null), None), Replayed::Primary);
        assert!(Replayed::of(Some(Read::Null), Some(9)).keep());
        assert!(Replayed::of(Some(Read::At(10)), Some(10)).keep());
        assert!(!Replayed::of(Some(Read::At(9)), Some(10)).keep());
        assert_eq!(
            Replayed::of(Some(Read::At(10)), None),
            Replayed::Standby(false)
        );
        assert_eq!(Replayed::of(None, Some(1)), Replayed::Unknown);
        assert!(!Replayed::Unknown.keep());
    }

    /// Each bypass with a counter of its own counts there and nowhere
    /// else; only a locking read skips the fallback permit.
    #[test]
    fn each_bypass_counts_where_it_should() {
        let all = [
            Bypass::Locking,
            Bypass::AfterWrites,
            Bypass::Lagging,
            Bypass::OtherContext,
            Bypass::ReadingContext,
            Bypass::UnknownTypes,
            Bypass::Refused,
        ];
        for bypass in all {
            let cache = Cache::new(1 << 20, Arc::new(Unsynced));
            bypass.record(&cache);
            let stats = &cache.stats;
            let counts = [
                stats.locking.load(Ordering::Relaxed),
                stats.after_write.load(Ordering::Relaxed),
                stats.lagging.load(Ordering::Relaxed),
                stats.other_context.load(Ordering::Relaxed),
            ];
            let expected = match bypass {
                Bypass::Locking => [1, 0, 0, 0],
                Bypass::AfterWrites => [0, 1, 0, 0],
                Bypass::Lagging => [0, 0, 1, 0],
                Bypass::OtherContext => [0, 0, 0, 1],
                Bypass::ReadingContext | Bypass::UnknownTypes | Bypass::Refused => [0; 4],
            };
            assert_eq!(counts, expected, "{bypass:?}");
            assert_eq!(bypass.limited(), bypass != Bypass::Locking, "{bypass:?}");
        }
    }
}
