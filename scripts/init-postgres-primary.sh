#!/bin/sh
set -eu

: "${POSTGRES_REPLICATION_USER:?POSTGRES_REPLICATION_USER is required}"
: "${POSTGRES_REPLICATION_PASSWORD:?POSTGRES_REPLICATION_PASSWORD is required}"

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=replication_user="$POSTGRES_REPLICATION_USER" \
  --set=replication_password="$POSTGRES_REPLICATION_PASSWORD" \
  --set=ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE :"replication_user" WITH REPLICATION LOGIN ENCRYPTED PASSWORD :'replication_password';
SQL

printf '\nhost replication %s 0.0.0.0/0 scram-sha-256\n' \
  "$POSTGRES_REPLICATION_USER" >> "$PGDATA/pg_hba.conf"
