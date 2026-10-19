"""Resolve the base Compose file and optional project-local integration override."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def compose_command(env=None):
    env = os.environ if env is None else env
    command = ["docker", "compose", "--project-directory", str(ROOT), "-f", str(ROOT / "compose.yaml")]
    if env.get("COMPOSE_OVERRIDE_FILE"):
        command += ["-f", env["COMPOSE_OVERRIDE_FILE"]]
    return command
