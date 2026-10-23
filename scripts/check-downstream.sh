#!/usr/bin/env bash
# Build the crates the way another project does: from a crate outside this
# workspace that depends on them by path. Such a build reads none of this
# workspace's settings (no `[patch]`, no lockfile of its own), exactly as a
# git dependency would, so it catches a crate that builds only from inside.
#
# Two builds: the engine alone with the wrapper's `embedded` and `async`
# features (a backend running the engine in its process), and the same with
# the engine's `server` feature. The workspace's lockfile is copied in to pin
# the same versions.
#
#   scripts/check-downstream.sh            # needs the registry, or a warm cache
#   OFFLINE=1 scripts/check-downstream.sh  # from the local cache only
#
# Environment: CARGO (default `cargo`; `cargo +1.89.0` picks a toolchain),
# OFFLINE, CARGO_TARGET_DIR (default target/downstream).
set -euo pipefail
cd "$(dirname "$0")/.."
root=$(pwd)
read -r -a cargo <<< "${CARGO:-cargo}"
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$root/target/downstream}
offline=()
if [ -n "${OFFLINE:-}" ]; then
    offline=(--offline)
fi

dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
mkdir "$dir/src"
cat > "$dir/Cargo.toml" <<EOF
[package]
name = "streamgres-downstream"
version = "0.0.0"
edition = "2024"
publish = false

[features]
server = ["streamgres/server"]

[dependencies]
streamgres = { path = "$root", default-features = false }
diesel-streamgres = { path = "$root/crates/diesel-streamgres", features = ["embedded", "async"] }
EOF
cat > "$dir/src/main.rs" <<'EOF'
//! Names a piece of each public surface a backend reaches for, so the
//! build is the one a backend would make.

fn main() {
    let config = streamgres::embedded::Config::new("postgres://localhost/app");
    let _ = streamgres::embedded::setup_sql("zero", 0, &config.publication);
    let _ = diesel_streamgres::engine::EngineClient::from_env();
    let _: Option<diesel_streamgres::AsyncSyncedConnection> = None;
}
EOF
cp "$root/Cargo.lock" "$dir/Cargo.lock"

echo "== the engine embedded, the wrapper with embedded and async"
"${cargo[@]}" check "${offline[@]}" --manifest-path "$dir/Cargo.toml"
echo "== the same with the server"
"${cargo[@]}" check "${offline[@]}" --manifest-path "$dir/Cargo.toml" --features server
