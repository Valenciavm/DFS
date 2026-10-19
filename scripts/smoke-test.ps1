$ErrorActionPreference = "Stop"
docker compose exec -T client python scripts/smoke_test.py
if ($LASTEXITCODE -ne 0) {
    throw "DFSha Compose smoke test failed with exit code $LASTEXITCODE"
}
