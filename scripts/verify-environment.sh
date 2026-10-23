#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
case "${1:-}" in
  -h|--help) echo 'Usage: scripts/verify-environment.sh
Checks Docker Engine, Compose v2, JDK 25+, and Compose configuration. Does not start services.'; exit 0 ;;
  '') ;;
  *) echo 'Unexpected argument (see --help)' >&2; exit 2 ;;
esac
preflight
command -v java >/dev/null && command -v javac >/dev/null || {
  echo 'JDK 25 or newer is required on PATH for Java source-file tooling.' >&2
  exit 1
}
java_version="$(javac -version 2>&1)"
java_version="${java_version#javac }"
java_major="${java_version%%[.+-]*}"
if [[ ! "$java_major" =~ ^[0-9]+$ ]] || (( java_major < 25 )); then
  echo "JDK 25 or newer is required; found javac $java_version." >&2
  exit 1
fi
java_version="$(java -version 2>&1)"
java_release="${java_version#*\"}"
java_major="${java_release%%[.+\"-]*}"
if [[ ! "$java_major" =~ ^[0-9]+$ ]] || (( java_major < 25 )); then
  echo "Java 25 or newer is required; found $java_version." >&2
  exit 1
fi
printf '%s\n' "$java_version"
docker compose version
docker info --format 'Docker server: {{.ServerVersion}}; CPUs: {{.NCPU}}; memory: {{.MemTotal}} bytes'
compose --profile ha --profile tools config --quiet
echo 'Environment checks passed.'
