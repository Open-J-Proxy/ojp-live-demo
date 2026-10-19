#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
  cat <<'EOF'
Usage: scripts/htap.sh [load-runner options]
Runs the sequential Direct JDBC/HikariCP versus OJP HTAP comparison.
Defaults: --mode both --rps 16,32,64 --warmup 10 --duration 10
OJP slow-query segregation is mandatory with a 20% slow lane.
EOF
  exit 0
fi

for argument in "$@"; do
  case "$argument" in
    --scenario|--scenario=*)
      echo 'The HTAP benchmark scenario is fixed; workload selection uses the OLTP/OLAP mix options.' >&2
      exit 2
      ;;
  esac
done

export SLOW_QUERY_ENABLED=true
export SLOW_QUERY_THRESHOLD_MS="${SLOW_QUERY_THRESHOLD_MS:-100}"
if [[ ! "$SLOW_QUERY_THRESHOLD_MS" =~ ^[1-9][0-9]*$ ]]; then
  echo 'SLOW_QUERY_THRESHOLD_MS must be a positive integer.' >&2
  exit 2
fi

preflight
compose up -d --no-deps --force-recreate --wait --wait-timeout "${START_TIMEOUT:-180}" ojp
ojp_id="$(compose ps -q ojp)"
if [[ -z "$ojp_id" ]]; then
  echo 'Could not identify the running OJP container to verify slow-query configuration.' >&2
  exit 1
fi
server_env="$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$ojp_id")"
for property in \
  '-Dojp.server.slowQuerySegregation.enabled=true' \
  '-Dojp.server.slowQuerySegregation.slowSlotPercentage=20' \
  '-Dojp.server.slowQuerySegregation.classificationMode=ABSOLUTE_THRESHOLD' \
  "-Dojp.server.slowQuerySegregation.slowQueryThresholdMs=$SLOW_QUERY_THRESHOLD_MS"; do
  if [[ "$server_env" != *"$property"* ]]; then
    echo "OJP slow-query segregation configuration is not effective: missing $property" >&2
    exit 1
  fi
done

server_logs="$(compose logs --no-color ojp 2>&1)"
for property in \
  '-Dojp.server.slowQuerySegregation.enabled=true' \
  '-Dojp.server.slowQuerySegregation.slowSlotPercentage=20' \
  '-Dojp.server.slowQuerySegregation.classificationMode=ABSOLUTE_THRESHOLD' \
  "-Dojp.server.slowQuerySegregation.slowQueryThresholdMs=$SLOW_QUERY_THRESHOLD_MS"; do
  if [[ "$server_logs" != *"$property"* ]]; then
    echo "OJP JVM did not confirm its runtime options; refusing to run HTAP without verified segregation ($property)." >&2
    exit 1
  fi
done

export OJP_SLOW_QUERY_SEGREGATION_VERIFIED=true
run_load htap "$@"
