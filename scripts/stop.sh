#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
case "${1:-}" in
  -h|--help) echo 'Usage: scripts/stop.sh
Stops this Compose project. Named volumes and benchmark results are preserved.'; exit 0 ;;
  '') ;;
  *) echo 'Usage: scripts/stop.sh (no arguments; see --help)' >&2; exit 2 ;;
esac
preflight
compose --profile ha --profile tools down --remove-orphans
