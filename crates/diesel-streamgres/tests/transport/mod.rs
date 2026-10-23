//! The engine a live test talks to: the one `STREAMGRES_S2S_URL` names,
//! over a socket; or, with the `embedded` feature and
//! `STREAMGRES_TEST_EMBEDDED` set, one this test process runs, configured
//! from the environment as a backend would configure it
//! (`embedded::Config::from_env`), one engine for every test of the
//! binary. The tests are the same either way.

use diesel_streamgres::engine::{Config, EngineClient};

/// A client with `config`; `None`, and the test skipped, when no engine
/// is given.
pub fn client(config: Config) -> Option<EngineClient> {
    #[cfg(feature = "embedded")]
    if std::env::var_os("STREAMGRES_TEST_EMBEDDED").is_some() {
        return Some(EngineClient::embedded(embedded::engine(), config));
    }
    let Ok(url) = std::env::var("STREAMGRES_S2S_URL") else {
        eprintln!("STREAMGRES_S2S_URL not set; skipping");
        return None;
    };
    Some(EngineClient::with_config(url, config))
}

/// The test process's own engine.
#[cfg(feature = "embedded")]
mod embedded {
    use std::sync::OnceLock;
    use std::time::{Duration, Instant};

    use streamgres::embedded::{Config, Engine, State};

    /// The engine, started on first use and serving: one for every test
    /// of the binary, stopped with the process (its temporary slot goes
    /// with its connection).
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
