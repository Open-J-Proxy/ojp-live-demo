#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
case "${1:-}" in
  -h|--help) echo 'Usage: scripts/seed-data.sh
Seeds PostgreSQL using /seed/schema.sql and /seed/seed.sql.
Existing baseline data is retained and validated.
SEED_USERS=20000 SEED_PRODUCTS=5000 SEED_ORDERS=200000 SEED_ITEMS=600000
SEED_REVIEWS=80000 SEED=12345. Regeneration requires reset.sh --destroy-data.'; exit 0 ;;
  '') ;;
  *) echo 'Unknown option (see --help)' >&2; exit 2 ;;
esac
preflight
for variable in SEED_USERS SEED_PRODUCTS SEED_ORDERS SEED_ITEMS SEED_REVIEWS; do
  if [[ -v "$variable" && ! "${!variable}" =~ ^[1-9][0-9]*$ ]]; then
    echo "$variable must be a positive integer" >&2
    exit 2
  fi
done
if [[ ! "${SEED:-12345}" =~ ^-?[0-9]+$ ]]; then echo 'SEED must be an integer' >&2; exit 2; fi
compose up -d --wait --wait-timeout "${START_TIMEOUT:-180}" postgres
psql_cmd=(exec -T postgres sh -ec 'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"' psql)
compose "${psql_cmd[@]}" \
  -v "users=${SEED_USERS:-20000}" -v "products=${SEED_PRODUCTS:-5000}" \
  -v "orders=${SEED_ORDERS:-200000}" -v "items=${SEED_ITEMS:-600000}" \
  -v "reviews=${SEED_REVIEWS:-80000}" -v "seed=${SEED:-12345}" \
  -f /seed/schema.sql -f /seed/seed.sql
