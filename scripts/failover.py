#!/usr/bin/env python3
"""HA orchestration and independently observed, bounded recovery evidence."""
import argparse
import datetime
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "--project-directory", str(ROOT), "-f", str(ROOT / "compose.yaml")]


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def http(base, path, timeout=5):
    try:
        with urllib.request.urlopen(base + path, timeout=timeout) as response:
            raw = response.read().decode()
            try:
                body = json.loads(raw)
            except ValueError:
                body = raw
            return response.status, body
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()
    except (OSError, urllib.error.URLError) as error:
        return 0, str(error)


def ready(base, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if http(base, "/ready")[0] == 200:
            return
        time.sleep(.5)
    raise RuntimeError("Application did not become ready after HA reconfiguration")


def application_url():
    if os.environ.get("APP_URL"):
        return os.environ["APP_URL"]
    if os.environ.get("APP_PORT") == "0":
        return "http://" + subprocess.check_output(COMPOSE + ["port", "app", "8080"],
                                                   text=True).strip()
    return "http://127.0.0.1:" + os.environ.get("APP_PORT", "8080")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recovery-timeout", type=float, default=60)
    parser.add_argument("--mode", choices=["ojp"], default="ojp")
    parser.add_argument("--warmup", type=float, default=5)
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--run-id", default="failover-" + uuid.uuid4().hex[:12])
    parser.add_argument("--scenario", default="failover")
    args, forwarded = parser.parse_known_args()
    if (not all(map(math.isfinite, (args.recovery_timeout, args.duration, args.warmup)))
            or args.recovery_timeout <= 0 or args.duration < 3 or args.warmup < 0):
        parser.error("recovery timeout must be positive, duration >= 3 and warmup >= 0")
    output = Path(os.environ.get("RESULTS_DIR", ROOT / "results"))
    output.mkdir(parents=True, exist_ok=True)
    safe_id = "".join(c if c.isalnum() or c in "._-" else "_" for c in args.run_id)
    evidence = output / (safe_id + "-failover-events.json")
    log_path = output / (safe_id + "-failover-runner.log")
    base = None
    env = os.environ.copy()
    env["OJP_URL"] = env.get(
        "OJP_HA_URL", "jdbc:ojp[ojp:1059,ojp2:1059,ojp3:1059]_postgresql://postgres:5432/shop?ApplicationName=demo-ojp")
    report = {"run_id": args.run_id, "scenario": args.scenario, "started_at": now(),
              "stopped_service": "ojp", "recovery_timeout_seconds": args.recovery_timeout,
              "observations": [], "transient_errors": 0}
    process = None
    log = None
    failure = None
    try:
        subprocess.run(COMPOSE + ["--profile", "ha", "up", "-d", "ojp", "ojp2", "ojp3"],
                       check=True)
        subprocess.run(COMPOSE + ["up", "-d", "--no-deps", "--force-recreate", "app"],
                       env=env, check=True)
        base = application_url()
        ready(base)
        runner = COMPOSE + ["--profile", "tools", "run", "--rm",
                            "--user", f"{os.getuid()}:{os.getgid()}", "load", "--output", "/results",
                            "--mode", "ojp", "--scenario", args.scenario,
                            "--warmup", str(args.warmup), "--duration", str(args.duration),
                            "--run-id", args.run_id] + forwarded
        log = log_path.open("w")
        process = subprocess.Popen(runner, stdout=log, stderr=subprocess.STDOUT, env=env)
        deadline = time.monotonic() + args.warmup + 180
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("Load runner exited before measurement; see " + str(log_path))
            _, health = http(base, "/health")
            context = health.get("context", health) if isinstance(health, dict) else {}
            measuring = context.get("phase") == "measure" and context.get("run_id") == args.run_id
            if not measuring:
                _, metrics = http(base, "/metrics")
                if isinstance(metrics, str):
                    for line in metrics.splitlines():
                        if not line.startswith("demo_completions_total{"):
                            continue
                        labels = dict(re.findall(r'(\w+)="([^"]*)"', line))
                        if (labels.get("phase") == "measure"
                                and labels.get("scenario") == args.scenario
                                and float(line.rsplit(" ", 1)[1]) > 0):
                            measuring = True
                            break
            if measuring:
                report["measurement_observed_at"] = now()
                break
            time.sleep(.2)
        else:
            raise RuntimeError("Never observed this run's measurement context")
        time.sleep(min(2, args.duration / 3))
        if process.poll() is not None:
            raise RuntimeError("Measurement ended before fault injection")
        report["fault_requested_at"] = now()
        fault = time.monotonic()
        subprocess.run(COMPOSE + ["stop", "-t", "0", "ojp"], check=True)
        report["node_stopped_at"] = now()
        consecutive = 0
        while time.monotonic() - fault < args.recovery_timeout:
            status, body = http(base, "/work?" + urllib.parse.urlencode({
                "op": "browse", "seed": 12345, "phase": "measure",
                "scenario": args.scenario, "level": 0,
            }), timeout=min(5, max(.1, args.recovery_timeout - (time.monotonic() - fault))))
            success = (status == 200 and isinstance(body, dict)
                       and body.get("ok", body.get("success", False))
                       and body.get("sql_completed", body.get("completed", False))
                       and not body.get("rolledBack", body.get("rollback", False)))
            report["observations"].append({"timestamp": now(), "status": status,
                                            "sql_success": bool(success), "response": body})
            if success:
                consecutive += 1
                if consecutive >= 3:
                    report["recovered_at"] = now()
                    report["recovery_seconds"] = time.monotonic() - fault
                    break
            else:
                consecutive = 0
                report["transient_errors"] += 1
            time.sleep(.2)
        else:
            raise RuntimeError("No three consecutive SQL successes within recovery bound")
        report["runner_exit_code"] = process.wait(
            timeout=args.duration + args.warmup + args.recovery_timeout + 180)
        if report["runner_exit_code"]:
            raise RuntimeError("Load runner failed; see " + str(log_path))
        report["success"] = True
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        failure = str(error)
        report["success"] = False
        report["error"] = failure
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if log is not None:
            log.close()
        # Run both restoration steps even if one fails.
        for command in (
            COMPOSE + ["start", "ojp"],
            COMPOSE + ["up", "-d", "--no-deps", "--force-recreate", "app"],
        ):
            result = subprocess.run(command, check=False)
            if result.returncode:
                report.setdefault("cleanup_errors", []).append(result.returncode)
        if not report.get("cleanup_errors"):
            try:
                base = application_url()
                ready(base)
            except (RuntimeError, OSError, subprocess.SubprocessError) as error:
                report.setdefault("cleanup_errors", []).append(str(error))
        report["finished_at"] = now()
        evidence.write_text(json.dumps(report, indent=2) + "\n")
        print(str(evidence), flush=True)
    if failure or report.get("cleanup_errors"):
        raise SystemExit(failure or "HA cleanup failed")


if __name__ == "__main__":
    main()
