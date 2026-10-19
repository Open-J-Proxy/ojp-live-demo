#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  echo 'Usage: scripts/slow-queries.sh [load-runner options]
Runs --scenario slow-queries; application-side SQL delays exercise pool contention.
All load-runner options are forwarded, including --mode, --rps and --duration.'
  exit 0
fi
run_load slow-queries "$@"
