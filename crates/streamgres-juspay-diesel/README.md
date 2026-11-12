# streamgres-juspay-diesel

The synchronous Streamgres adapter built on Juspay Diesel 2.3.1. Import its
`LoadCached` and `Tracker` exports in the same way as the upstream adapter.
Async support belongs to `streamgres-diesel`, since the fork has no compatible
`diesel-async` package.

See [the library guide](../../docs/library-guide.md) for setup and guarantees.
Source files are synchronized from `crates/streamgres-diesel/src`; maintainers
run `python3 scripts/sync-diesel-adapters.py`, and CI checks for divergence.
