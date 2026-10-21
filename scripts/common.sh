#!/usr/bin/env bash
# Shared by entrypoints; never depends on the caller's working directory.
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
export RESULTS_DIR="${RESULTS_DIR:-$ROOT_DIR/results}"
[[ "$RESULTS_DIR" = /* ]] || RESULTS_DIR="$ROOT_DIR/$RESULTS_DIR"
export RESULTS_DIR
export LOCAL_UID="${LOCAL_UID:-$(id -u)}"
export LOCAL_GID="${LOCAL_GID:-$(id -g)}"

compose() {
  local files=(-f "$ROOT_DIR/compose.yaml")
  if [[ -n "${COMPOSE_OVERRIDE_FILE:-}" ]]; then files+=(-f "$COMPOSE_OVERRIDE_FILE"); fi
  docker compose --project-directory "$ROOT_DIR" "${files[@]}" "$@"
}
application_url() {
  if [[ -n "${APP_URL:-}" ]]; then
    printf '%s\n' "$APP_URL"
  elif [[ "${APP_PORT:-8080}" == 0 ]]; then
    printf 'http://%s\n' "$(compose port app 8080)"
  else
    printf 'http://127.0.0.1:%s\n' "${APP_PORT:-8080}"
  fi
}
preflight() {
  if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
    cat >&2 <<'EOF'
Docker Engine and the Docker Compose v2 plugin are required.
Ubuntu: follow https://docs.docker.com/engine/install/ubuntu/ and install
docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin.
Then run: sudo systemctl enable --now docker
For non-root access: sudo usermod -aG docker "$USER"; log out and back in.
EOF
    return 1
  fi
  if ! docker info >/dev/null 2>&1; then
    echo 'Docker daemon is unreachable. Ubuntu: sudo systemctl start docker; check docker group membership and DOCKER_HOST.' >&2
    return 1
  fi
  if [[ ! -f "$ROOT_DIR/compose.yaml" ]]; then
    echo "Missing Compose file: $ROOT_DIR/compose.yaml" >&2
    return 1
  fi
}
wait_app() {
  python3 "$SCRIPT_DIR/http_client.py" ready --url "$(application_url)" \
    --timeout "${START_TIMEOUT:-180}"
}
