#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
case "${1:-}" in
  -h|--help) echo 'Usage: scripts/verify-connections.sh
Verifies PostgreSQL readiness and executes one application query in both modes.
Restores the original application mode afterward. APP_URL overrides the host URL.'; exit 0 ;;
  '') ;;
  *) echo 'Unexpected argument (see --help)' >&2; exit 2 ;;
esac
preflight
compose exec -T postgres sh -ec 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
wait_app
python3 - "$(application_url)" <<'PY'
import json, os, sys, urllib.request
base = sys.argv[1]
def call(path, post=False):
    request = urllib.request.Request(base + path, method="POST" if post else "GET")
    if post and os.environ.get("ADMIN_TOKEN"):
        request.add_header("Authorization", "Bearer " + os.environ["ADMIN_TOKEN"])
    with urllib.request.urlopen(request, timeout=180) as response:
        return json.load(response)
original = call("/ready")["mode"]
try:
    for mode in ("direct", "ojp"):
        call("/mode?value=" + mode, True)
        ready = call("/ready")
        result = call("/work?op=browse&seed=12345&phase=warmup&scenario=integration&level=1")
        if not ready.get("ready") or not result.get("success") or not result.get("sql_completed"):
            raise SystemExit(f"{mode}: readiness or SQL completion failed: {ready} {result}")
        print(mode + ": SQL query succeeded")
finally:
    call("/mode?value=" + original, True)
PY
