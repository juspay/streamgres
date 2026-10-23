//! Settings read from the environment the way the server always has
//! read them: a name is also looked up under the legacy
//! `STREAMGRES_SYNC_` and `XYNE_SYNC_` prefixes, and an empty value
//! counts as unset. The engine core never reads the environment itself;
//! the server's configuration and an embedded engine's
//! [`crate::embedded::Config::from_env`] read it through this, so the
//! two take the same names.

use std::str::FromStr;
use std::time::Duration;

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
            _ => None,
        });
        assert_eq!(env.value("STREAMGRES_ADDR").as_deref(), Some("legacy"));
        assert_eq!(env.number("STREAMGRES_SHARD", 0u32), Ok(2));
        assert_eq!(env.value("STREAMGRES_EMPTY"), None);
        assert_eq!(env.millis("STREAMGRES_UNSET_MS", 5), Ok(Duration::from_millis(5)));
        assert!(env.millis("STREAMGRES_WAIT_MS", 5).unwrap_err().contains("STREAMGRES_WAIT_MS"));
    }
}
