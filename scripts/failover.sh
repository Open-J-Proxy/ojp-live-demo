#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  echo 'Usage: scripts/failover.sh [--recovery-timeout SECONDS] [load-runner options]
Starts the HA profile and recreates app with OJP_HA_URL (or its three-node default).
Stops service ojp during measurement, records failure/recovery timestamps and
transient errors in results, then restores ojp and the original app configuration.
Mode must be ojp. Default: --warmup 5 --duration 30. Options are forwarded.'
  exit 0
fi
preflight
mkdir -p "$RESULTS_DIR"
exec python3 "$SCRIPT_DIR/failover.py" "$@"
