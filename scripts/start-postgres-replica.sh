#!/bin/sh
set -eu

: "${PRIMARY_HOST:?PRIMARY_HOST is required}"
: "${PRIMARY_PORT:=5432}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${REPLICATION_USER:?REPLICATION_USER is required}"
: "${REPLICATION_PASSWORD:?REPLICATION_PASSWORD is required}"
: "${PGDATA:=/var/lib/postgresql/data}"

chmod 0700 "$PGDATA"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  passfile="$(mktemp)"
  trap 'rm -f "$passfile"' EXIT HUP INT TERM
  chmod 0600 "$passfile"
  printf '%s:%s:replication:%s:%s\n' \
    "$PRIMARY_HOST" "$PRIMARY_PORT" "$REPLICATION_USER" "$REPLICATION_PASSWORD" \
    > "$passfile"

  until pg_isready -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" -U "$REPLICATION_USER"; do
    sleep 2
  done

  if [ -n "$(find "$PGDATA" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    echo "Refusing to initialize replica over non-empty PGDATA without PG_VERSION" >&2
    exit 1
  fi

  PGPASSFILE="$passfile" pg_basebackup \
    --host="$PRIMARY_HOST" \
    --port="$PRIMARY_PORT" \
    --username="$REPLICATION_USER" \
    --pgdata="$PGDATA" \
    --format=plain \
    --wal-method=stream \
    --write-recovery-conf \
    --progress
  mv "$passfile" "$PGDATA/.pgpass"
  chmod 0600 "$PGDATA/.pgpass"
  trap - EXIT HUP INT TERM
elif [ ! -f "$PGDATA/.pgpass" ]; then
  echo "Replica data exists but its replication password file is missing" >&2
  exit 1
fi

export PGPASSFILE="$PGDATA/.pgpass"
exec postgres -c hot_standby=on
