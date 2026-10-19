#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  echo 'Usage: scripts/death-spiral.sh [load-runner options]
Runs --scenario death-spiral. Forward --rps CSV to choose the entire ordered ramp.
Example: --mode both --rps 50,100,200,400 --warmup 10 --duration 30'
  exit 0
fi
run_load death-spiral "$@"
