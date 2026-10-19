#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  echo 'Usage: scripts/warmup.sh [load-runner options]
Runs --scenario warmup; all options are forwarded unchanged to the load runner.
Example: --mode both --rps 10,25 --warmup 20 --duration 30 --seed 12345'
  exit 0
fi
run_load warmup "$@"
