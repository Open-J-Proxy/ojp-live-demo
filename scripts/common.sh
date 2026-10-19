#!/usr/bin/env bash
# Shared by entrypoints; never depends on the caller's working directory.
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
export RESULTS_DIR="${RESULTS_DIR:-$ROOT_DIR/results}"
[[ "$RESULTS_DIR" = /* ]] || RESULTS_DIR="$ROOT_DIR/$RESULTS_DIR"
export RESULTS_DIR

compose() { docker compose --project-directory "$ROOT_DIR" -f "$ROOT_DIR/compose.yaml" "$@"; }
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
  python3 - "$(application_url)" "${START_TIMEOUT:-180}" <<'PY'
import sys, time, urllib.request
url, timeout = sys.argv[1], float(sys.argv[2])
deadline = time.monotonic() + timeout
last = ""
while time.monotonic() < deadline:
    try:
        with urllib.request.urlopen(url + "/ready", timeout=5) as response:
            if response.status == 200:
                break
    except Exception as error:
        last = str(error)
    time.sleep(1)
else:
    raise SystemExit(f"Application readiness timed out at {url}/ready: {last}")
PY
}
run_load() {
  local scenario="$1"; shift
  preflight
  mkdir -p "$RESULTS_DIR"
  compose --profile tools run --rm --user "$(id -u):$(id -g)" load \
    --output /results --scenario "$scenario" "$@"
}
