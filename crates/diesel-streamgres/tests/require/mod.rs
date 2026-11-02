//! Whether a live test may skip itself. Each skips when what it needs is
//! not given, so the suite runs on a laptop with nothing set up; CI says
//! what it gives instead: `STREAMGRES_REQUIRE_LIVE` lists requirements
//! (`postgres`, `engine`, `embedded`), and a test that would skip for
//! want of one listed fails.

/// Whether `STREAMGRES_REQUIRE_LIVE` lists `requirement`.
fn required(requirement: &str) -> bool {
    std::env::var("STREAMGRES_REQUIRE_LIVE")
        .is_ok_and(|list| list.split(',').any(|listed| listed.trim() == requirement))
}

/// A live test skips itself for want of `requirement`, saying `why`; or,
/// when `STREAMGRES_REQUIRE_LIVE` lists the requirement, fails instead,
/// so a run that was meant to exercise it cannot pass without doing so.
pub fn skip(requirement: &str, why: &str) {
    assert!(
        !required(requirement),
        "{why}; STREAMGRES_REQUIRE_LIVE lists `{requirement}`, so this test must run here"
    );
    eprintln!("{why}: skipped");
}
