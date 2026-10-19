#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
case "${1:-}" in
  -h|--help) echo 'Usage: scripts/verify-connections.sh
Verifies PostgreSQL readiness and executes one application query in both modes.
Restores the original application mode afterward. APP_URL overrides the host URL.'; exit 0 ;;
  '') ;;
  *) echo 'Unexpected argument (see --help)' >&2; exit 2 ;;
esac
preflight
compose exec -T postgres sh -ec 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
wait_app
python3 "$SCRIPT_DIR/http_client.py" verify --url "$(application_url)"
