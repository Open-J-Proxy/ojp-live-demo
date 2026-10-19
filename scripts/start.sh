#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
build=true; seed=true; ha=false
while (($#)); do
  case "$1" in
    -h|--help) echo 'Usage: scripts/start.sh [--no-build] [--skip-seed] [--ha]
Starts PostgreSQL, seeds an empty database, then starts the application and monitoring.
Existing data is preserved. SEED_* variables control first-start fixture sizes.'; exit 0 ;;
    --no-build) build=false ;;
    --skip-seed) seed=false ;;
    --ha) ha=true ;;
    *) echo "Unknown option: $1 (see --help)" >&2; exit 2 ;;
  esac
  shift
done
preflight
mkdir -p "$RESULTS_DIR"
compose up -d --wait --wait-timeout "${START_TIMEOUT:-180}" postgres
if "$seed"; then "$SCRIPT_DIR/seed-data.sh"; fi
args=(up -d)
if "$build"; then args+=(--build); fi
if "$ha"; then
  compose --profile ha "${args[@]}" ojp ojp2 ojp3 app prometheus grafana postgres-exporter
else
  compose "${args[@]}" ojp app prometheus grafana postgres-exporter
fi
wait_app
echo "Application ready: $(application_url)"
