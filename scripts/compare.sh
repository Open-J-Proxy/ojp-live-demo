#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  echo 'Usage: scripts/compare.sh [load-runner options]
Runs --mode both sequentially with baseline reset and equal seeded request plans.
All options are forwarded; specify --rps CSV --warmup N --duration N as needed.'
  exit 0
fi
run_load compare --mode both "$@"
