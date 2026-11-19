#!/usr/bin/env bash
# Local Postgres for the tests (inside `nix develop path:.`): start | stop | reset.
set -euo pipefail

start() {
  if [ ! -d "$PGDATA" ]; then
    mkdir -p "$PGHOST"
    initdb -D "$PGDATA" -U postgres --auth=trust >/dev/null
    cat >>"$PGDATA/postgresql.conf" <<CONF
listen_addresses = '127.0.0.1'
port = $PGPORT
unix_socket_directories = '$PGHOST'
wal_level = logical
max_replication_slots = 64
max_wal_senders = 64
CONF
    # Require passwords over TCP so the tests cover SCRAM login.
    echo "host all all 127.0.0.1/32 scram-sha-256" >"$PGDATA/pg_hba.conf"
    echo "host replication all 127.0.0.1/32 scram-sha-256" >>"$PGDATA/pg_hba.conf"
    echo "local all all trust" >>"$PGDATA/pg_hba.conf"
  fi
  pg_ctl -D "$PGDATA" status >/dev/null 2>&1 || pg_ctl -D "$PGDATA" -l "$PGHOST/log" -w start >/dev/null
  psql -U postgres -d postgres -qtc "SELECT 1 FROM pg_roles WHERE rolname = 'streamlib'" | grep -q 1 || \
    psql -U postgres -d postgres -qc "CREATE ROLE streamlib LOGIN REPLICATION CREATEDB PASSWORD 'streamlib'"
  psql -U postgres -d postgres -qtc "SELECT 1 FROM pg_database WHERE datname = 'streamlib'" | grep -q 1 || \
    psql -U postgres -d postgres -qc "CREATE DATABASE streamlib OWNER streamlib"
  echo "postgres up: $STREAMLIB_PG_DSN"
}

case "${1:-start}" in
  start) start ;;
  stop) pg_ctl -D "$PGDATA" -w stop ;;
  reset) pg_ctl -D "$PGDATA" -w stop 2>/dev/null || true; rm -rf "$PGHOST"; start ;;
esac
