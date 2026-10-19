param(
    [string]$Service = "postgres-replica"
)

$ErrorActionPreference = "Stop"
Write-Warning "Stop both NameNodes and update POSTGRES_PRIMARY_DSN before restarting them."
docker compose exec $Service pg_ctl promote -w -D /var/lib/postgresql/data
if ($LASTEXITCODE -ne 0) {
    throw "PostgreSQL replica promotion failed with exit code $LASTEXITCODE"
}
docker compose exec $Service pg_isready
if ($LASTEXITCODE -ne 0) {
    throw "Promoted PostgreSQL is not ready (exit code $LASTEXITCODE)"
}
