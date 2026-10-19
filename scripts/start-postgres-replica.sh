#!/bin/sh
set -eu

: "${PRIMARY_HOST:?PRIMARY_HOST is required}"
: "${PRIMARY_PORT:=5432}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${REPLICATION_USER:?REPLICATION_USER is required}"
: "${REPLICATION_PASSWORD:?REPLICATION_PASSWORD is required}"
: "${PGDATA:=/var/lib/postgresql/data}"
: "${PGPASSFILE:=$PGDATA/.pgpass}"

chmod 0700 "$PGDATA"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  if [ -n "$(find "$PGDATA" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    echo "Refusing to initialize replica over non-empty PGDATA without PG_VERSION" >&2
    exit 1
  fi

  passfile="$(mktemp)"
  trap 'rm -f "$passfile"' EXIT HUP INT TERM
  printf '%s:%s:replication:%s:%s\n' \
    "$PRIMARY_HOST" "$PRIMARY_PORT" "$REPLICATION_USER" "$REPLICATION_PASSWORD" \
    > "$passfile"
  chmod 0600 "$passfile"

  until pg_isready -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" -U "$REPLICATION_USER"; do
    sleep 2
  done

  PGPASSFILE="$passfile" pg_basebackup \
    --host="$PRIMARY_HOST" \
    --port="$PRIMARY_PORT" \
    --username="$REPLICATION_USER" \
    --pgdata="$PGDATA" \
    --format=plain \
    --wal-method=stream \
    --write-recovery-conf \
    --progress
  mv "$passfile" "$PGPASSFILE"
  chmod 0600 "$PGPASSFILE"
  trap - EXIT HUP INT TERM
else
  printf '%s:%s:replication:%s:%s\n' \
    "$PRIMARY_HOST" "$PRIMARY_PORT" "$REPLICATION_USER" "$REPLICATION_PASSWORD" \
    > "$PGPASSFILE"
  chmod 0600 "$PGPASSFILE"
fi

if [ -f "$PGDATA/postgresql.auto.conf" ]; then
  sed -i "s|passfile=''[^']*''|passfile=''$PGPASSFILE''|" \
    "$PGDATA/postgresql.auto.conf"
fi

export PGPASSFILE
exec postgres -c hot_standby=on
