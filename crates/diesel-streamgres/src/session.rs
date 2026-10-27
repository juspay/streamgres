//! What a connection's session has been told since it was established,
//! as far as it changes what a read may see.
//!
//! A cached answer is only right for a reader who would have read the
//! same rows. Who that is depends on the session as much as on the SQL:
//! `SET ROLE` or `SET SESSION AUTHORIZATION` change whose privileges and
//! row-level security apply, `search_path` which table a bare name
//! means, and a setting such as `app.tenant` what a policy lets through.
//! PostgreSQL lists none of the custom settings, so the wrapper cannot
//! read them back to compare: it watches the statements that change
//! them instead, and a connection whose session has changed reads
//! Postgres until it is back as it was established.
//!
//! - `SET name …`, `SET SESSION name …`, `SET ROLE`, `SET SESSION
//!   AUTHORIZATION`: changed, until `RESET` of the same name.
//! - `SET LOCAL …`: lasts until the transaction ends, and reads inside a
//!   transaction go to Postgres anyway, so it changes nothing here.
//! - `set_config(…)` anywhere in a statement: changed, and only `RESET
//!   ALL` or `DISCARD ALL` undoes it, the name and scope being in its
//!   arguments.
//! - `RESET ALL`, `DISCARD ALL`: back as established.
//! - A few settings that change no query's rows ([`HARMLESS`]), which
//!   pools often set on checkout: no change.
//!
//! What a function or a procedure sets inside itself is not seen, nor
//! settings passed when connecting (`options=-c …`); connections that
//! differ there must not share a cache.

use std::collections::BTreeSet;

/// Settings that change how a query runs but not which rows it returns.
const HARMLESS: &[&str] = &[
    "application_name",
    "client_min_messages",
    "idle_in_transaction_session_timeout",
    "idle_session_timeout",
    "lock_timeout",
    "statement_timeout",
    "work_mem",
];

/// Whether a connection's reads may use its cache, as far as what it
/// reads as goes ([`crate::cache::Cache::admits`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope {
    /// Not read yet, or not since its session was last restored.
    Unchecked = 0,
    /// It reads as the cache's answers are for.
    Shared = 1,
    /// It reads as something else.
    Own = 2,
}

impl Scope {
    /// The scope stored as `value` ([`Scope`] as `u8`).
    pub(crate) fn from_u8(value: u8) -> Scope {
        match value {
            1 => Scope::Shared,
            2 => Scope::Own,
            _ => Scope::Unchecked,
        }
    }
}

/// The name standing for a `set_config` call, whose setting is not known.
const SET_CONFIG: &str = "set_config()";

/// The settings a connection's statements have changed from how it was
/// established and not yet reset.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SessionChanges {
    changed: BTreeSet<String>,
}

/// What one batch of statements did to a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Effect {
    /// Nothing a read depends on.
    None,
    /// It changed, or it changes more.
    Changed,
    /// It is back as it was established, after having changed.
    Restored,
}

impl SessionChanges {
    /// Whether the session differs from how it was established.
    pub(crate) fn changed(&self) -> bool {
        !self.changed.is_empty()
    }

    /// Takes in what `sql`, one statement or a batch, does to the
    /// session. A batch is split on every `;`, as [`crate::may_write`]
    /// splits it.
    pub(crate) fn apply(&mut self, sql: &str) -> Effect {
        let was = self.changed();
        let mut moved = false;
        for statement in sql.split(';').map(str::trim).filter(|statement| !statement.is_empty()) {
            moved |= self.apply_one(statement);
        }
        match (moved, self.changed()) {
            (false, _) => Effect::None,
            (true, true) => Effect::Changed,
            (true, false) if was => Effect::Restored,
            (true, false) => Effect::None,
        }
    }

    /// [`SessionChanges::apply`] for one statement: whether the changes
    /// it holds moved.
    fn apply_one(&mut self, statement: &str) -> bool {
        if mentions_set_config(statement) {
            return self.changed.insert(SET_CONFIG.to_owned());
        }
        let mut words = statement
            .split(|c: char| c.is_whitespace() || c == '=')
            .filter(|word| !word.is_empty());
        let Some(head) = words.next() else {
            return false;
        };
        let everything = |changed: &mut BTreeSet<String>| {
            let had = !changed.is_empty();
            changed.clear();
            had
        };
        if head.eq_ignore_ascii_case("DISCARD") {
            words.next().is_some_and(|what| what.eq_ignore_ascii_case("ALL")) && everything(&mut self.changed)
        } else if head.eq_ignore_ascii_case("RESET") {
            match words.next().map(setting) {
                Some(name) if name == "all" => everything(&mut self.changed),
                Some(name) => self.changed.remove(&name),
                None => false,
            }
        } else if head.eq_ignore_ascii_case("SET") {
            set_name(words).is_some_and(|name| self.changed.insert(name))
        } else {
            false
        }
    }
}

/// The setting a `SET` (its words after `SET`) changes for the session;
/// `None` for one that changes nothing a read depends on beyond its
/// transaction (`SET LOCAL`, `SET TRANSACTION`, `SET CONSTRAINTS`,
/// `SET SESSION CHARACTERISTICS`) or a [`HARMLESS`] setting.
fn set_name<'a>(mut words: impl Iterator<Item = &'a str>) -> Option<String> {
    let mut word = words.next()?;
    if word.eq_ignore_ascii_case("LOCAL") {
        return None;
    }
    if word.eq_ignore_ascii_case("SESSION") {
        word = words.next()?;
        if word.eq_ignore_ascii_case("AUTHORIZATION") {
            return Some("session_authorization".to_owned());
        }
        if word.eq_ignore_ascii_case("CHARACTERISTICS") {
            return None;
        }
    }
    if ["TRANSACTION", "CONSTRAINTS"]
        .iter()
        .any(|scoped| word.eq_ignore_ascii_case(scoped))
    {
        return None;
    }
    if word.eq_ignore_ascii_case("TIME") {
        return Some("timezone".to_owned());
    }
    let name = setting(word);
    (!HARMLESS.contains(&name.as_str())).then_some(name)
}

/// A setting's name as `SET` and `RESET` spell it: quotes dropped,
/// folded to lower case as PostgreSQL folds an unquoted name.
fn setting(word: &str) -> String {
    word.trim_matches('"').to_ascii_lowercase()
}

/// Whether `statement` calls `set_config`, which changes a setting from
/// inside any statement, a `SELECT` included.
fn mentions_set_config(statement: &str) -> bool {
    const NAME: &[u8] = b"set_config";
    statement
        .as_bytes()
        .windows(NAME.len())
        .any(|window| window.eq_ignore_ascii_case(NAME))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What each statement leaves the session as, applied in turn.
    fn after(statements: &[&str]) -> (SessionChanges, Vec<Effect>) {
        let mut session = SessionChanges::default();
        let effects = statements.iter().map(|sql| session.apply(sql)).collect();
        (session, effects)
    }

    /// A tenant setting, a role and a search path change the session
    /// until reset; resetting the one that changed puts it back.
    #[test]
    fn what_changes_a_read_is_tracked_until_reset() {
        for sql in [
            "SET app.tenant = 'bob'",
            "set app.tenant to 'bob'",
            "SET SESSION app.tenant = 'bob'",
            "SET ROLE reader",
            "SET SESSION ROLE reader",
            "SET SESSION AUTHORIZATION reader",
            "SET search_path TO tenant_a, public",
            "SET \"search_path\" = tenant_a",
            "SET TIME ZONE 'Asia/Kolkata'",
            "SELECT set_config('app.tenant', 'bob', false)",
            "select pg_catalog.SET_CONFIG($1, $2, false)",
        ] {
            let (session, effects) = after(&[sql]);
            assert!(session.changed(), "{sql}");
            assert_eq!(effects, [Effect::Changed], "{sql}");
        }
        let (session, effects) = after(&["SET app.tenant = 'bob'", "RESET app.tenant"]);
        assert!(!session.changed());
        assert_eq!(effects, [Effect::Changed, Effect::Restored]);
        let (session, _) = after(&["SET ROLE reader", "RESET ROLE"]);
        assert!(!session.changed());
        let (session, _) = after(&["SET app.tenant = 'a'", "SET ROLE r", "RESET ROLE"]);
        assert!(session.changed(), "the tenant is still set");
    }

    /// `set_config` names its setting in its arguments: only a reset of
    /// everything undoes it. `RESET ALL` and `DISCARD ALL` undo all.
    #[test]
    fn everything_reset_undoes_everything() {
        let (session, _) = after(&["SELECT set_config('app.tenant', 'bob', false)", "RESET app.tenant"]);
        assert!(session.changed());
        for reset in ["RESET ALL", "reset all", "DISCARD ALL"] {
            let (session, effects) = after(&["SET ROLE r", "SELECT set_config('a.b', 'c', false)", reset]);
            assert!(!session.changed(), "{reset}");
            assert_eq!(effects.last(), Some(&Effect::Restored), "{reset}");
        }
        let (session, _) = after(&["SET ROLE r", "DISCARD PLANS"]);
        assert!(session.changed(), "only DISCARD ALL resets the session");
    }

    /// What lasts a transaction, or changes no rows, is not a change.
    #[test]
    fn what_cannot_change_a_read_is_not_tracked() {
        for sql in [
            "SET LOCAL app.tenant = 'bob'",
            "SET LOCAL ROLE reader",
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
            "SET CONSTRAINTS ALL DEFERRED",
            "SET statement_timeout = 5000",
            "SET application_name TO 'worker'",
            "SET lock_timeout='1s'",
            "SELECT 1",
            "SHOW search_path",
            "BEGIN",
            "COMMIT",
        ] {
            let (session, effects) = after(&[sql]);
            assert!(!session.changed(), "{sql}");
            assert_eq!(effects, [Effect::None], "{sql}");
        }
    }

    /// A batch's statements apply in order: the last word stands.
    #[test]
    fn a_batch_applies_in_order() {
        let (session, effects) = after(&["SET ROLE r; DISCARD ALL"]);
        assert!(!session.changed());
        assert_eq!(effects, [Effect::None], "changed and restored within the batch");
        let (_, effects) = after(&["SET ROLE r", "SET ROLE r"]);
        assert_eq!(effects, [Effect::Changed, Effect::None], "already changed");
        let (session, effects) = after(&["DISCARD ALL; SET ROLE r"]);
        assert!(session.changed());
        assert_eq!(effects, [Effect::Changed]);
    }
}
