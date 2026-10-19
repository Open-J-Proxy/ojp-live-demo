#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
force=false
case "${1:-}" in
  -h|--help) echo 'Usage: scripts/seed-data.sh [--force]
Seeds PostgreSQL using /seed/schema.sql and /seed/seed.sql.
Existing seeded databases are skipped unless --force is supplied.
SEED_USERS=20000 SEED_PRODUCTS=5000 SEED_ORDERS=200000 SEED_ITEMS=600000
SEED_REVIEWS=80000 SEED=12345. --force replaces data according to seed.sql.'; exit 0 ;;
  --force) force=true; [[ $# == 1 ]] || exit 2 ;;
  '') ;;
  *) echo 'Unknown option (see --help)' >&2; exit 2 ;;
esac
preflight
compose up -d --wait --wait-timeout "${START_TIMEOUT:-180}" postgres
psql_cmd=(exec -T postgres sh -ec 'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"' psql)
if ! "$force"; then
  exists="$(compose "${psql_cmd[@]}" -Atc "SELECT to_regclass('public.benchmark_seed') IS NOT NULL")"
  if [[ "$exists" == t ]]; then
    count="$(compose "${psql_cmd[@]}" -Atc 'SELECT count(*) FROM public.benchmark_seed')"
    if [[ "$count" != 0 ]]; then
      echo 'Seed data already present; preserving database.'
      exit 0
    fi
  fi
fi
compose "${psql_cmd[@]}" \
  -v "users=${SEED_USERS:-20000}" -v "products=${SEED_PRODUCTS:-5000}" \
  -v "orders=${SEED_ORDERS:-200000}" -v "items=${SEED_ITEMS:-600000}" \
  -v "reviews=${SEED_REVIEWS:-80000}" -v "seed=${SEED:-12345}" \
  -f /seed/schema.sql -f /seed/seed.sql
