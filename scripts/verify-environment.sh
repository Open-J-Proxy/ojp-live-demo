#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
case "${1:-}" in
  -h|--help) echo 'Usage: scripts/verify-environment.sh
Checks Docker Engine, Compose v2, Python 3, and Compose configuration. Does not start services.'; exit 0 ;;
  '') ;;
  *) echo 'Unexpected argument (see --help)' >&2; exit 2 ;;
esac
preflight
command -v python3 >/dev/null || { echo 'Python 3 is required. Ubuntu: sudo apt-get install python3' >&2; exit 1; }
docker compose version
docker info --format 'Docker server: {{.ServerVersion}}; CPUs: {{.NCPU}}; memory: {{.MemTotal}} bytes'
compose --profile ha --profile tools config --quiet
echo 'Environment checks passed.'
