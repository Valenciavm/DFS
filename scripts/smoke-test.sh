#!/bin/sh
set -eu

docker compose exec -T client python scripts/smoke_test.py
