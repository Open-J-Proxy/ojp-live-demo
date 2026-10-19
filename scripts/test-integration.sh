#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  echo 'Usage: scripts/test-integration.sh [--keep] [--small] [--only TEST_SUBSTRING] [--ramp-duration N]
Runs mandatory integration coverage in an isolated project (22 checks).
Default asserts 200 simultaneous direct PostgreSQL sessions. --small (or IT_SMALL=true)
explicitly uses 20 sessions instead; it is not the full 200-session validation.
Reduced fixtures: 200 users, 100 products, 1000 orders, 3000 items, 500 reviews.
Logs/results persist under results/integration/. Only this test project is cleaned.
Ramps use 2,4,8 RPS for 10 seconds per level; IT_RAMP_DURATION or --ramp-duration overrides.
Repeat --only to select multiple related checks. Real SQL never uses artificial sleeps.
--keep preserves this project and its volumes for debugging.'
  exit 0
fi
preflight
exec python3 "$SCRIPT_DIR/integration.py" "$@"
