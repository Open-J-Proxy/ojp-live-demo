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
import json, sys, urllib.request
base = sys.argv[1]
def call(path, post=False):
    req = urllib.request.Request(base + path, data=b"" if post else None)
    with urllib.request.urlopen(req, timeout=30) as response:
        body = response.read().decode()
        try:
            return json.loads(body)
        except ValueError:
            return body
original = call("/ready")
mode = original.get("mode", "direct") if isinstance(original, dict) else "direct"
try:
    for selected in ("direct", "ojp"):
        call("/mode?value=" + selected, True)
        call("/ready")
        result = call("/work?op=browse&seed=12345&phase=warmup&scenario=integration&level=1")
        if isinstance(result, dict) and result.get("success") is False:
            raise RuntimeError(result)
        print(selected + ": query succeeded")
finally:
    call("/mode?value=" + mode, True)
PY
