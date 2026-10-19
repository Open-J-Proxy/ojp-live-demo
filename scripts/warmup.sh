#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  echo 'Usage: scripts/warmup.sh [load-runner options]
Runs warm-up only, drains and exports results without a measured phase.
Example: --mode both --rps 8,16 --warmup 30 --seed 12345'
  exit 0
fi
run_load death-spiral --warmup-only "$@"
