#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  echo 'Usage: scripts/throttling.sh [load-runner options]
Runs --scenario throttling. Choose offered load using --rps and --max-in-flight.
All options are forwarded; runner admission drops and SQL completions stay separate.'
  exit 0
fi
run_load throttling "$@"
