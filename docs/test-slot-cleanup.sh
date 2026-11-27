#!/usr/bin/env bash
set -euo pipefail

# Point PG_BIN at PostgreSQL 16 or later's bin directory. Run as a normal user.
: "${PG_BIN:?Set PG_BIN to a PostgreSQL 16+ bin directory}"
export PATH="$PG_BIN:$HOME/.cargo/bin:$PATH"
cd "$(dirname "$0")/.."
cluster=$(mktemp -d "/tmp/streamgres-slots.XXXXXX")
heartbeat_pid=
started=false
cleanup() {
    if [[ -n "$heartbeat_pid" ]]; then
        kill "$heartbeat_pid" 2>/dev/null || true
        wait "$heartbeat_pid" 2>/dev/null || true
    fi
    if [[ "$started" == true ]]; then
        pg_ctl -D "$cluster/data" -m immediate -w stop >/dev/null
    fi
    rm -rf "$cluster"
}
trap cleanup EXIT

# Private Unix socket, no TCP listener, and a fresh cluster isolate destructive tests.
initdb -D "$cluster/data" -A trust >"$cluster/init.log"
pg_ctl -D "$cluster/data" -l "$cluster/server.log" \
    -o "-k $cluster -c listen_addresses='' -c wal_level=logical -c max_replication_slots=32 -c max_wal_senders=32" \
    -w start
started=true
export STREAMGRES_PG_DSN="host='$cluster' user='$(id -un)' dbname=postgres"
psql "$STREAMGRES_PG_DSN" -c 'SELECT version()'

# Logical slot creation on an idle primary may await a standby snapshot record.
(
    while true; do
        psql "$STREAMGRES_PG_DSN" -qc 'SELECT pg_log_standby_snapshot()' >/dev/null
        sleep 0.5
    done
) &
heartbeat_pid=$!

if [[ $(psql "$STREAMGRES_PG_DSN" -Atqc "SHOW server_version_num") -lt 170000 ]]; then
    cargo test --locked --test slot_cleanup feed_opens_and_reconnects_with_cleanup_disabled -- --ignored --exact
    exit
fi
cargo test --lib client::config::tests -- --test-threads=1
cargo test --test slot_cleanup -- --ignored --test-threads=1
