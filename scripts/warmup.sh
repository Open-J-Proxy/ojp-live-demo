#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  echo 'Usage: scripts/warmup.sh [load-runner options]
Runs the HTAP warm-up only, verifies OJP segregation, and exports no measured phase.'
  exit 0
fi
exec bash "$SCRIPT_DIR/htap.sh" --warmup-only "$@"
