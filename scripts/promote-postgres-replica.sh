#!/bin/sh
set -eu

service="${1:-postgres-replica}"
echo "Promoting $service. Stop NameNodes first and update POSTGRES_PRIMARY_DSN before restarting them."
docker compose exec "$service" pg_ctl promote -w -D /var/lib/postgresql/data
docker compose exec "$service" pg_isready
