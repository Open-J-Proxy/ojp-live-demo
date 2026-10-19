#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  echo 'Usage: scripts/slow-queries.sh [load-runner options]
Enables verified OJP slow-query segregation with 20% slow admission slots.
All load-runner options are forwarded, including --mode, --rps and --duration.'
  exit 0
fi
preflight
previous="${SLOW_QUERY_ENABLED:-false}"
restore() { SLOW_QUERY_ENABLED="$previous" compose up -d --no-deps --force-recreate --wait ojp; }
trap restore EXIT
SLOW_QUERY_ENABLED=true compose up -d --no-deps --force-recreate --wait ojp
run_load slow-queries "$@"
