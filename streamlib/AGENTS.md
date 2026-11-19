# AGENTS.md

## Design
- Contract first: everything above the traits works with any source.
- No hard-coding: tunables in config, types through a codec, names through a prefix.
- Refuse early: reject what we can't follow at `open`, never at the first read.
- Never degrade silently: wrong data is worse than an error.
- Errors say what to do: `Transient` retry, `Refused` never, `CannotResume` re-sync, `Contract` source bug.
- Keep the engine dumb about sources and sources dumb about the engine.

## Code
- Small files, small functions, one job each.
- `unwrap` only for invariants; runtime failures return errors.
- No `println`/`eprintln` in libraries; use `tracing`.
- Few dependencies, same versions as streamgres where shared.
- Clean up what you create: slots, connections, tasks (also on `Drop`).

## Comments
- One line each, plain English.
- No multi-line blocks, arrows or shorthand.
- Say why, not what.

## Tests
- Every bug fix ships with a test that fails without it.
- Contract tests run on the memory source and on real Postgres.
- Broken sources must be caught by the kit.
- Postgres tests isolate: own table, publication and slot prefix; clean up after.

## Before done
- `nix develop path:.` then `scripts/pg.sh start`.
- `cargo fmt`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test`: all green.
- No leftover slots or publications after the run.

## Docs
- README stays one line.
- Decisions live in `.agent-memory/streamlib/{human,agent}/decisions.md`; human file in short lines.
- Discuss first; write decision files only when asked.
