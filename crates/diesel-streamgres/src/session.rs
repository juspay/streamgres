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
//! - `set_config(…)` anywhere in a statement, or a `DO` block: changed,
//!   and only `RESET ALL` or `DISCARD ALL` undoes it, what it set being
//!   in its arguments or its body.
//! - `RESET ALL`, `DISCARD ALL`: back as established.
//! - A few settings that change no query's rows ([`HARMLESS`]), which
//!   pools often set on checkout: no change.
//!
//! A custom setting made where no statement shows it (inside a function,
//! when connecting) cannot change an answer: it matters to a query only
//! through row-level security or a function, both of which the engine
//! refuses. What else such code could change (the search path, a table
//! that shadows another, a privilege) is in the connection's context,
//! read again after any statement that may have changed it unseen
//! ([`Effect::Recheck`]) and at least every [`RECHECK_EVERY`].

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

/// How long a connection's context stands once read: past it, the next
/// read through the cache reads it again. It bounds how long a change
/// the connection cannot see goes unnoticed: a `GRANT`, `REVOKE` or
/// table created by another session, or a setting a trigger made.
pub(crate) const RECHECK_EVERY: std::time::Duration = std::time::Duration::from_secs(10);

/// Whether a connection's reads may use its cache, and in which context
/// ([`crate::cache::Cache::context_id`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope {
    /// Not read yet, or to be read again.
    Unchecked,
    /// It reads in the context with this id.
    In(u32),
    /// Its readers must read Postgres.
    Own,
}

// Only the async connection keeps its scope in an atomic.
#[cfg_attr(not(feature = "async"), allow(dead_code))]
impl Scope {
    /// The scope as one number, for an atomic.
    pub(crate) fn bits(self) -> u64 {
        match self {
            Scope::Unchecked => 0,
            Scope::Own => 1,
            Scope::In(id) => 2 + u64::from(id),
        }
    }

    /// The scope [`Scope::bits`] gave.
    pub(crate) fn from_bits(bits: u64) -> Scope {
        match bits {
            0 => Scope::Unchecked,
            1 => Scope::Own,
            n => u32::try_from(n - 2).map_or(Scope::Own, Scope::In),
        }
    }
}

/// The name standing for a `DO` block, which may set anything.
const DO_BLOCK: &str = "do";

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
    /// It may have changed in a way the statements do not say (a table
    /// created or a privilege changed, a procedure or a function called):
    /// what the connection reads as must be read again.
    Recheck,
}

impl SessionChanges {
    /// Whether the session differs from how it was established.
    pub(crate) fn changed(&self) -> bool {
        !self.changed.is_empty()
    }

    /// Takes in what `sql`, one statement or a batch, does to the
    /// session. A batch is split only on the semicolons between its
    /// statements ([`statements`]): text in a literal that reads like
    /// `RESET ALL` must not pass for one.
    pub(crate) fn apply(&mut self, sql: &str) -> Effect {
        let was = self.changed();
        let (mut moved, mut unseen) = (false, false);
        for statement in statements(sql) {
            moved |= self.apply_one(statement);
            unseen |= changes_unseen(statement);
        }
        match (moved, self.changed()) {
            (true, true) => Effect::Changed,
            (true, false) if was => Effect::Restored,
            _ if unseen => Effect::Recheck,
            _ => Effect::None,
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
        if head.eq_ignore_ascii_case("DO") {
            self.changed.insert(DO_BLOCK.to_owned())
        } else if head.eq_ignore_ascii_case("DISCARD") {
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

/// Statements that may change what a connection reads as without saying
/// so: they create, alter or drop relations (a table that shadows one in
/// `public`), change privileges, or run code of the database's own.
const UNSEEN: &[&str] = &[
    "ALTER", "CALL", "CREATE", "DROP", "GRANT", "IMPORT", "REASSIGN", "REVOKE",
];

/// Words that may stand before `(` in a read without calling a function
/// of the database's own: keywords, type names used in a cast, and the
/// built-in functions reads commonly call, none of which changes the
/// session. Any other name before `(` may be a function that does.
/// Sorted, for the binary search.
const NOT_CALLS: &[&str] = &[
    "abs", "age", "all", "and", "any", "array", "array_agg", "array_length", "array_position", "as",
    "avg", "between", "bigint", "bool", "bool_and", "bool_or", "boolean", "btrim", "by",
    "cardinality", "case", "cast", "ceil", "ceiling", "char", "char_length", "character",
    "coalesce", "concat", "concat_ws", "count", "date", "date_part", "date_trunc", "decimal",
    "dense_rank", "distinct", "double", "else", "end", "every", "except", "exists", "extract",
    "fetch", "filter", "first_value", "float", "floor", "for", "from", "generate_series",
    "greatest", "group", "groups", "having", "ilike", "in", "int", "integer", "intersect",
    "interval", "into", "is", "join", "json_agg", "json_build_object", "json_extract_path_text",
    "json_object_agg", "jsonb_agg", "jsonb_array_length", "jsonb_build_object",
    "jsonb_extract_path", "jsonb_extract_path_text", "jsonb_object_agg", "lag", "last_value",
    "lateral", "lead", "least", "left", "length", "like", "limit", "lower", "lpad", "ltrim", "max",
    "md5", "min", "mod", "not", "now", "ntile", "nullif", "numeric", "octet_length", "offset",
    "on", "only", "or", "order", "over", "partition", "position", "power", "range", "rank", "real",
    "recursive", "regexp_match", "regexp_replace", "replace", "returning", "right", "round",
    "row", "row_number", "row_to_json", "rows", "rpad", "rtrim", "select", "similar", "some",
    "split_part", "sqrt", "string_agg", "strpos", "substr", "substring", "sum", "table",
    "tablesample", "text", "then", "time", "timestamp", "timestamptz", "to_char", "to_date",
    "to_json", "to_jsonb", "to_number", "to_timestamp", "trim", "trunc", "union", "unnest",
    "upper", "using", "uuid", "values", "varchar", "when", "where", "window", "with", "within",
];

/// Whether `statement` may change what the connection reads as in a way
/// it does not say ([`Effect::Recheck`]): one of [`UNSEEN`], or a read
/// calling a function not among [`NOT_CALLS`]. A write is not looked at
/// for calls (an `INSERT`'s column list reads like one); what its
/// triggers might set is left to [`RECHECK_EVERY`].
fn changes_unseen(statement: &str) -> bool {
    let head = statement.trim_start_matches('(');
    let word = head
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .next()
        .unwrap_or("");
    if UNSEEN.iter().any(|unseen| word.eq_ignore_ascii_case(unseen)) {
        return true;
    }
    let reads = ["SELECT", "WITH", "VALUES", "TABLE"]
        .iter()
        .any(|read| word.eq_ignore_ascii_case(read));
    reads && calls_a_function(statement)
}

/// Whether `statement` calls a function not among [`NOT_CALLS`]: a name
/// outside any literal or comment, bare or quoted, followed by `(`.
fn calls_a_function(statement: &str) -> bool {
    let bytes = statement.as_bytes();
    let mut at = 0;
    while at < bytes.len() {
        let (name, next) = match bytes[at] {
            b'\'' => (None, past_quote(bytes, at + 1, b'\'', false)),
            b'"' => {
                let end = past_quote(bytes, at + 1, b'"', false);
                let name = statement.get(at + 1..end.saturating_sub(1)).map(str::to_owned);
                (name, end)
            }
            b'-' if bytes.get(at + 1) == Some(&b'-') => (
                None,
                position(bytes, at + 2, b"\n").map_or(bytes.len(), |end| end + 1),
            ),
            b'/' if bytes.get(at + 1) == Some(&b'*') => (
                None,
                position(bytes, at + 2, b"*/").map_or(bytes.len(), |end| end + 2),
            ),
            b'$' => match dollar_tag(bytes, at) {
                Some(tag) => (
                    None,
                    position(bytes, at + tag.len(), tag)
                        .map_or(bytes.len(), |end| end + tag.len()),
                ),
                None => (None, at + 1),
            },
            byte if byte.is_ascii_alphabetic() || byte == b'_' || !byte.is_ascii() => {
                let end = at + bytes[at..].iter().take_while(|byte| is_word_byte(**byte)).count();
                (Some(statement[at..end].to_ascii_lowercase()), end)
            }
            _ => (None, at + 1),
        };
        at = next;
        let Some(name) = name else { continue };
        let calls = bytes[at..]
            .iter()
            .find(|byte| !byte.is_ascii_whitespace())
            .is_some_and(|byte| *byte == b'(');
        if calls && NOT_CALLS.binary_search(&name.as_str()).is_err() {
            return true;
        }
    }
    false
}

/// The statements of `sql`, trimmed, split on the semicolons between
/// them. A semicolon inside a string (`'…'`, `E'…'` with its backslash
/// escapes), a quoted name (`"…"`), a dollar-quoted body (`$tag$…$tag$`)
/// or a comment (`-- …`, `/* … */`) ends nothing; a quote left open runs
/// to the end.
fn statements(sql: &str) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let mut found = Vec::new();
    let (mut start, mut at) = (0, 0);
    while at < bytes.len() {
        at = match bytes[at] {
            b';' => {
                found.push(&sql[start..at]);
                start = at + 1;
                at + 1
            }
            b'\'' => {
                let escapes = at > 0
                    && matches!(bytes[at - 1], b'E' | b'e')
                    && (at < 2 || !is_word_byte(bytes[at - 2]));
                past_quote(bytes, at + 1, b'\'', escapes)
            }
            b'"' => past_quote(bytes, at + 1, b'"', false),
            b'-' if bytes.get(at + 1) == Some(&b'-') => {
                position(bytes, at + 2, b"\n").map_or(bytes.len(), |end| end + 1)
            }
            b'/' if bytes.get(at + 1) == Some(&b'*') => {
                position(bytes, at + 2, b"*/").map_or(bytes.len(), |end| end + 2)
            }
            b'$' => match dollar_tag(bytes, at) {
                Some(tag) => position(bytes, at + tag.len(), tag).map_or(bytes.len(), |end| end + tag.len()),
                None => at + 1,
            },
            _ => at + 1,
        };
    }
    found.push(&sql[start..]);
    found
        .into_iter()
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .collect()
}

/// Where a quoted run that began before `from` ends: just past its
/// closing `quote`, a doubled one being part of it (and with `escapes`,
/// a backslash escaping the next byte); the end when it never closes.
fn past_quote(bytes: &[u8], mut from: usize, quote: u8, escapes: bool) -> usize {
    while from < bytes.len() {
        match bytes[from] {
            b'\\' if escapes => from += 2,
            byte if byte == quote && bytes.get(from + 1) == Some(&quote) => from += 2,
            byte if byte == quote => return from + 1,
            _ => from += 1,
        }
    }
    bytes.len()
}

/// The dollar-quote tag (`$$`, `$body$`) starting at `at`, if one does:
/// a `$`, an optional name not starting with a digit, and a `$`. A
/// parameter (`$1`) is not one.
fn dollar_tag(bytes: &[u8], at: usize) -> Option<&[u8]> {
    let name = &bytes[at + 1..];
    if name.first().is_some_and(u8::is_ascii_digit) {
        return None;
    }
    let length = name.iter().take_while(|byte| is_word_byte(**byte)).count();
    (name.get(length) == Some(&b'$')).then(|| &bytes[at..at + length + 2])
}

/// Where `needle` next occurs in `bytes` from `from` on.
fn position(bytes: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    bytes
        .get(from..)?
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|offset| from + offset)
}

/// Whether `byte` can be part of a name.
fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || !byte.is_ascii()
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

    /// Text in a literal, a quoted name, a dollar-quoted body or a
    /// comment is not a statement: it cannot reset a changed session.
    #[test]
    fn a_reset_inside_a_literal_resets_nothing() {
        for sql in [
            "INSERT INTO notes VALUES ('x; RESET ALL ')",
            "INSERT INTO notes VALUES ('it''s; RESET ALL ')",
            r"INSERT INTO notes VALUES (E'\'; RESET ALL; ')",
            "SELECT 1 AS \"a; RESET ALL \"",
            "SELECT $$; RESET ALL; $$",
            "SELECT $body$; DISCARD ALL; $body$",
            "SELECT 1 -- ; RESET ALL\n",
            "SELECT 1 /* ; RESET ALL; */",
        ] {
            let (session, _) = after(&["SET app.tenant = 'bob'", sql]);
            assert!(session.changed(), "{sql}");
        }
        assert_eq!(statements("SELECT $1; RESET ALL"), ["SELECT $1", "RESET ALL"], "a parameter is no tag");
        assert_eq!(statements("SELECT 'a;b'; SELECT 2;"), ["SELECT 'a;b'", "SELECT 2"]);
        let (session, _) = after(&["SET ROLE r", "SELECT 'x'; RESET ALL"]);
        assert!(!session.changed(), "a real RESET ALL after a literal still resets");
    }

    /// A `DO` block may set anything: only a reset of everything undoes it.
    #[test]
    fn a_do_block_changes_the_session() {
        let block = "DO $$ BEGIN EXECUTE 'SET ROLE reader'; END $$";
        let (session, effects) = after(&[block]);
        assert!(session.changed());
        assert_eq!(effects, [Effect::Changed]);
        let (session, _) = after(&[block, "RESET ROLE"]);
        assert!(session.changed(), "the block's settings are not known");
        let (session, _) = after(&[block, "DISCARD ALL"]);
        assert!(!session.changed());
    }

    /// What may change the context unseen asks for it to be read again:
    /// DDL, privileges, a procedure, a read calling a function of the
    /// database's own. Built-ins, keywords before `(`, an `INSERT`'s
    /// column list and names inside literals or comments do not.
    #[test]
    fn what_may_change_the_context_unseen_asks_for_a_recheck() {
        for sql in [
            "CREATE TEMP TABLE tickets (id int)",
            "create schema tenant",
            "ALTER ROLE app SET search_path = tenant",
            "DROP TABLE tenant.tickets",
            "GRANT SELECT ON tickets TO app",
            "REVOKE SELECT ON tickets FROM app",
            "CALL switch_tenant('acme')",
            "SELECT set_tenant('acme')",
            r#"SELECT "tenants"."enter"($1)"#,
            "WITH t AS (SELECT 1) SELECT my_fn() FROM t",
        ] {
            let (_, effects) = after(&[sql]);
            assert_eq!(effects, [Effect::Recheck], "{sql}");
        }
        for sql in [
            r#"SELECT "t"."id" FROM "t" WHERE (("t"."a" = $1) AND ("t"."b" IN ($2, $3))) LIMIT $4"#,
            r#"SELECT COUNT(*) FROM ("a" INNER JOIN "b" ON ("a"."id" = "b"."a_id"))"#,
            r#"SELECT "t"."id" FROM "t" WHERE "t"."id" = ANY($1) AND EXISTS (SELECT 1)"#,
            "SELECT lower(name), coalesce(a, b), CAST(x AS numeric(10, 2)) FROM t",
            r#"INSERT INTO "t" ("a", "b") VALUES ($1, $2)"#,
            "UPDATE t SET a = 1",
            "SELECT 'my_fn(1)' -- other_fn(2)",
            "SELECT $$ evil() $$",
        ] {
            let (_, effects) = after(&[sql]);
            assert_eq!(effects, [Effect::None], "{sql}");
        }
        let mut sorted = NOT_CALLS.to_vec();
        sorted.sort_unstable();
        assert_eq!(sorted, NOT_CALLS, "kept sorted, for the binary search");
    }

    /// A scope survives the trip through an atomic.
    #[test]
    fn a_scope_round_trips_through_its_bits() {
        for scope in [Scope::Unchecked, Scope::Own, Scope::In(0), Scope::In(63), Scope::In(u32::MAX)] {
            assert_eq!(Scope::from_bits(scope.bits()), scope);
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
