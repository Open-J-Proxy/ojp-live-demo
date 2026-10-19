#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
case "${1:-}" in
  -h|--help) echo 'Usage: scripts/reset.sh [--destroy-data]
Without a flag, restores the seeded application baseline through POST /reset.
--destroy-data explicitly deletes this project'\''s named volumes; does not delete results.'; exit 0 ;;
  --destroy-data)
    [[ $# == 1 ]] || { echo 'Unexpected arguments' >&2; exit 2; }
    preflight
    compose --profile ha --profile tools down --volumes --remove-orphans
    ;;
  '')
    python3 - "$(application_url)" <<'PY'
import os, sys, urllib.request
request = urllib.request.Request(sys.argv[1] + "/reset", method="POST")
if os.environ.get("ADMIN_TOKEN"):
    request.add_header("Authorization", "Bearer " + os.environ["ADMIN_TOKEN"])
with urllib.request.urlopen(request, timeout=180) as response:
    print(response.read().decode())
PY
    ;;
  *) echo 'Unknown option; volume deletion requires --destroy-data (see --help)' >&2; exit 2 ;;
esac
