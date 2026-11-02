//! The engine a live test talks to: the one `STREAMGRES_S2S_URL` names,
//! over a socket; or, with the `embedded` feature and
//! `STREAMGRES_TEST_EMBEDDED` set, one this test process runs, configured
//! from the environment as a backend would configure it
//! (`embedded::Config::from_env`), one engine for every test of the
//! binary. The tests are the same either way.

use diesel_streamgres::engine::{Config, EngineClient};

#[path = "../require/mod.rs"]
pub mod require;

/// A client with `config`; `None`, and the test skipped, when no engine
/// is given ([`require::skip`], requirement `engine`).
pub fn client(config: Config) -> Option<EngineClient> {
    #[cfg(feature = "embedded")]
    if std::env::var_os("STREAMGRES_TEST_EMBEDDED").is_some() {
        return Some(EngineClient::embedded(embedded::engine(), config));
    }
    let Ok(url) = std::env::var("STREAMGRES_S2S_URL") else {
        require::skip("engine", "neither STREAMGRES_S2S_URL nor STREAMGRES_TEST_EMBEDDED set");
        return None;
    };
    Some(EngineClient::with_config(url, config))
}

/// Waits until `engine` knows the table `query` reads, one a test created
/// after the engine started: the engine takes a new table in from the
/// change feed a moment after its `CREATE`, and until then refuses a
/// query on it as on an unknown table. The engine is asked directly, not
/// through a cache, which would keep that refusal and send the test's own
/// reads to Postgres for a while. `conn` renders the query.
#[allow(dead_code, reason = "each test binary uses what it needs of the transport module")]
pub fn adopted<Q: diesel::query_builder::QueryFragment<diesel::pg::Pg>>(
    engine: &EngineClient,
    conn: &mut diesel::PgConnection,
    query: Q,
) {
    use diesel_streamgres::key::QueryKey;
    use diesel_streamgres::subscriptions::{Outcome, Subscriptions, Verdict};
    use std::time::{Duration, Instant};

    let key = QueryKey::of(&query, conn).expect("key");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match Subscriptions::subscribe(engine, &key) {
            Outcome::Verdict(Verdict::Refused(reason)) if reason.contains("unknown table") => {
                assert!(Instant::now() < deadline, "the engine never took the table in: {reason}");
                std::thread::sleep(Duration::from_millis(50));
            }
            _ => break,
        }
    }
    Subscriptions::unsubscribe(engine, &key);
}

/// The test process's own engine.
#[cfg(feature = "embedded")]
mod embedded {
    use std::sync::OnceLock;
    use std::time::{Duration, Instant};

    use streamgres::embedded::{Config, Engine, State};

    /// The engine, started on first use and serving: one for every test
    /// of the binary, never stopped. Its slot outlives the process, and the
    /// next engine on the database drops it, its owner lock gone with the
    /// process.
    pub fn engine() -> Engine {
        static ENGINE: OnceLock<Engine> = OnceLock::new();
        ENGINE
            .get_or_init(|| {
                let config = Config::from_env().expect("an embedded engine's configuration");
                let engine = Engine::start(config);
                let deadline = Instant::now() + Duration::from_secs(60);
                while engine.state() != State::Serving {
                    assert!(
                        Instant::now() < deadline,
                        "the embedded engine never served: {:?}",
                        engine.state()
                    );
                    std::thread::sleep(Duration::from_millis(20));
                }
                engine
            })
            .clone()
    }
}
