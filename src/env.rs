//! Settings read from the environment the way the server always has
//! read them: a name is also looked up under the legacy
//! `STREAMGRES_SYNC_` and `XYNE_SYNC_` prefixes, and an empty value
//! counts as unset. The engine core never reads the environment itself;
//! the server's configuration and an embedded engine's
//! [`crate::embedded::Config::from_env`] read it through this, so the
//! two take the same names. The engine settings both read ([`Env::row_limit`],
//! [`Env::schemas`] and the rest) are parsed here once, so the two cannot
//! read them differently.

use std::str::FromStr;
use std::time::Duration;

use crate::model::TableName;
use crate::plan::Side;
use crate::sync::pg::{DEFAULT_READ_ROW_LIMIT, estimate_cache};

/// Where settings are looked up: the process environment ([`process`]),
/// or a test's.
pub struct Env<F> {
    lookup: F,
}

/// The process environment.
pub fn process() -> Env<fn(&str) -> Option<String>> {
    Env::new(|name| std::env::var(name).ok())
}

impl<F: Fn(&str) -> Option<String>> Env<F> {
    /// Settings `lookup` answers.
    pub fn new(lookup: F) -> Self {
        Env { lookup }
    }

    /// `name`'s value, or its legacy forms' (`STREAMGRES_X` is also read
    /// as `STREAMGRES_SYNC_X`, then `XYNE_SYNC_X`); empty counts as unset.
    pub fn value(&self, name: &str) -> Option<String> {
        let read = |name: &str| (self.lookup)(name).filter(|value| !value.is_empty());
        read(name).or_else(|| {
            let suffix = name.strip_prefix("STREAMGRES_")?;
            read(&format!("STREAMGRES_SYNC_{suffix}")).or_else(|| read(&format!("XYNE_SYNC_{suffix}")))
        })
    }

    /// The first of `names` that is set.
    pub fn first(&self, names: &[&str]) -> Option<String> {
        names.iter().find_map(|name| self.value(name))
    }

    /// `name` as milliseconds, `default` when unset; an error naming it
    /// when it is not a number.
    pub fn millis(&self, name: &str, default: u64) -> Result<Duration, String> {
        match self.value(name) {
            Some(text) => text
                .trim()
                .parse::<u64>()
                .map(Duration::from_millis)
                .map_err(|_| format!("{name} must be a number of milliseconds, got `{text}`")),
            None => Ok(Duration::from_millis(default)),
        }
    }

    /// `name` as a number, `default` when unset; an error naming it when
    /// it does not parse.
    pub fn number<T: FromStr>(&self, name: &str, default: T) -> Result<T, String> {
        match self.value(name) {
            Some(text) => text
                .trim()
                .parse()
                .map_err(|_| format!("{name} must be a number, got `{text}`")),
            None => Ok(default),
        }
    }

    /// `name` as a number above zero, `default` when unset.
    pub fn positive(&self, name: &str, default: usize) -> Result<usize, String> {
        match self.value(name) {
            Some(text) => text
                .trim()
                .parse()
                .ok()
                .filter(|number| *number > 0)
                .ok_or_else(|| format!("{name} must be a positive number, got `{text}`")),
            None => Ok(default),
        }
    }

    /// `name` as a boolean (`true`/`yes`/`on`/`1` or
    /// `false`/`no`/`off`/`0`), `default` when unset.
    pub fn flag(&self, name: &str, default: bool) -> Result<bool, String> {
        match self.value(name).as_deref().map(str::trim) {
            None => Ok(default),
            Some("true" | "yes" | "on" | "1") => Ok(true),
            Some("false" | "no" | "off" | "0") => Ok(false),
            Some(other) => Err(format!("{name} must be a boolean, got `{other}`")),
        }
    }

    /// `name` as a comma-separated list, blanks dropped; empty when unset.
    pub fn list(&self, name: &str) -> Vec<String> {
        self.value(name)
            .map(|text| {
                text.split(',')
                    .map(str::trim)
                    .filter(|item| !item.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `STREAMGRES_ROW_LIMIT` (or `STREAMGRES_READ_ROW_LIMIT`,
    /// `STREAMGRES_JOIN_LIMIT`): the most rows one read may return.
    pub fn row_limit(&self) -> Result<usize, String> {
        match self.first(&[
            "STREAMGRES_ROW_LIMIT",
            "STREAMGRES_READ_ROW_LIMIT",
            "STREAMGRES_JOIN_LIMIT",
        ]) {
            Some(text) => text.trim().parse().ok().filter(|limit| *limit > 0).ok_or_else(|| {
                format!("STREAMGRES_ROW_LIMIT must be a positive number of rows, got `{text}`")
            }),
            None => Ok(DEFAULT_READ_ROW_LIMIT),
        }
    }

    /// `STREAMGRES_JOIN_PREFERRED_SIDE`: which side of an inner join
    /// drives it (`parent`).
    pub fn join_preferred(&self) -> Result<Side, String> {
        match self.value("STREAMGRES_JOIN_PREFERRED_SIDE").as_deref().map(str::trim) {
            None | Some("parent") => Ok(Side::Parent),
            Some("child") => Ok(Side::Child),
            Some(other) => Err(format!(
                "STREAMGRES_JOIN_PREFERRED_SIDE must be child or parent, got `{other}`"
            )),
        }
    }

    /// `STREAMGRES_SCHEMAS`: the schemas whose tables are followed;
    /// `public` and the app's own (`<app>_<shard>`) when unset or blank.
    pub fn schemas(&self, app: &str, shard: u32) -> Vec<String> {
        let schemas = self.list("STREAMGRES_SCHEMAS");
        if schemas.is_empty() {
            vec!["public".to_owned(), format!("{app}_{shard}")]
        } else {
            schemas
        }
    }

    /// `STREAMGRES_MEMORY_TABLES`: tables mirrored in memory (none).
    pub fn memory_tables(&self) -> Vec<TableName> {
        self.list("STREAMGRES_MEMORY_TABLES")
            .into_iter()
            .map(TableName::from)
            .collect()
    }

    /// `STREAMGRES_ESTIMATE_TTL_MS` and `STREAMGRES_ESTIMATE_CACHE`: how
    /// long, and how many, the planner's estimates are kept. A value that
    /// is not a number keeps the default, as it always has.
    pub fn estimates(&self) -> (Duration, usize) {
        let number = |name: &str| self.value(name).and_then(|text| text.trim().parse::<u64>().ok());
        (
            number("STREAMGRES_ESTIMATE_TTL_MS")
                .map_or(estimate_cache::DEFAULT_TTL, Duration::from_millis),
            number("STREAMGRES_ESTIMATE_CACHE").map_or(estimate_cache::DEFAULT_CAPACITY, |n| {
                usize::try_from(n).unwrap_or(usize::MAX)
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A legacy name stands in for the current one, the current one wins
    /// over both, an empty value is unset, and a bad number is named.
    #[test]
    fn legacy_names_and_defaults() {
        let env = Env::new(|name: &str| match name {
            "XYNE_SYNC_ADDR" => Some("legacy".to_owned()),
            "STREAMGRES_SYNC_SHARD" => Some("2".to_owned()),
            "XYNE_SYNC_SHARD" => Some("9".to_owned()),
            "STREAMGRES_EMPTY" => Some(String::new()),
            "STREAMGRES_WAIT_MS" => Some("soon".to_owned()),
            "STREAMGRES_PADDED_MS" => Some(" 7 ".to_owned()),
            _ => None,
        });
        assert_eq!(env.value("STREAMGRES_ADDR").as_deref(), Some("legacy"));
        assert_eq!(env.number("STREAMGRES_SHARD", 0u32), Ok(2));
        assert_eq!(env.value("STREAMGRES_EMPTY"), None);
        assert_eq!(env.millis("STREAMGRES_UNSET_MS", 5), Ok(Duration::from_millis(5)));
        assert!(env.millis("STREAMGRES_WAIT_MS", 5).unwrap_err().contains("STREAMGRES_WAIT_MS"));
        assert_eq!(env.millis("STREAMGRES_PADDED_MS", 5), Ok(Duration::from_millis(7)));
    }
}
