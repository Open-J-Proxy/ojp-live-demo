#!/usr/bin/env python3
"""Timestamp actual Docker CPU/memory statistics alongside a workload command."""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "--project-directory", str(ROOT), "-f", str(ROOT / "compose.yaml")]


class ResourceSampler:
    def __init__(self, output, run_id, env=None):
        safe = re.sub(r"[^a-zA-Z0-9_.-]", "_", run_id)
        if safe in ("", ".", ".."):
            safe = "run_" + safe.replace(".", "_")
        self.directory = Path(output) / safe
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "resources.jsonl"
        if self.path.exists():
            raise FileExistsError("Resource evidence already exists; choose a new --run-id: " + run_id)
        self.run_id = run_id
        self.env = env
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.collect, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=25)
        if not self.thread.is_alive():
            self.annotate_intervals()

    def annotate_intervals(self):
        intervals = []
        safe_id = re.sub(r"[^a-zA-Z0-9_.-]", "_", self.run_id)
        for result_path in self.directory.parent.glob(safe_id + "-*.json"):
            try:
                result = json.loads(result_path.read_text())
            except (OSError, ValueError):
                continue
            for phase in ("warmup", "measure"):
                report = result.get(phase, {})
                if "start_epoch_seconds" not in report or "end_epoch_seconds" not in report:
                    continue
                intervals.append({
                    "phase": phase, "mode": result["mode"], "scenario": result["scenario"],
                    "level": result["level"], "start_epoch_seconds": report["start_epoch_seconds"],
                    "end_arrivals_epoch_seconds": report["end_arrivals_epoch_seconds"],
                    "end_epoch_seconds": report["end_epoch_seconds"],
                })
        if not intervals or not self.path.exists():
            return
        staging = self.path.with_suffix(".jsonl.partial")
        with self.path.open() as source, staging.open("w") as destination:
            for line in source:
                record = json.loads(line)
                start = datetime.datetime.fromisoformat(record["timestamp"]).timestamp()
                end = datetime.datetime.fromisoformat(
                    record.get("collected_at", record["timestamp"])).timestamp()
                record["workload_intervals"] = [
                    {**interval, "arrival_window_overlap": (
                        start <= interval["end_arrivals_epoch_seconds"]
                        and end >= interval["start_epoch_seconds"])}
                    for interval in intervals if start <= interval["end_epoch_seconds"]
                    and end >= interval["start_epoch_seconds"]
                ]
                destination.write(json.dumps(record) + "\n")
        staging.replace(self.path)

    def collect(self):
        with self.path.open("w", buffering=1) as output:
            while not self.stop_event.is_set():
                timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
                try:
                    result = subprocess.run(COMPOSE + ["ps", "-q", "postgres", "app", "ojp"],
                                            text=True, capture_output=True, timeout=10, check=True,
                                            env=self.env)
                    ids = result.stdout.split()
                    if not ids:
                        raise RuntimeError("No running postgres/app/ojp containers")
                    stats = subprocess.run(["docker", "stats", "--no-stream", "--format",
                                            "{{json .}}", *ids], text=True, capture_output=True,
                                            timeout=10, check=True, env=self.env)
                    for line in stats.stdout.splitlines():
                        record = {"timestamp": timestamp, "run_id": self.run_id,
                                  "source": "docker_stats", "sampling_scope": "whole_run",
                                  "collected_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                  "stats": json.loads(line)}
                        output.write(json.dumps(record) + "\n")
                        if record["stats"]["Name"].endswith("-postgres-1"):
                            try:
                                self.publish(record["stats"])
                            except (OSError, ValueError, subprocess.SubprocessError) as error:
                                output.write(json.dumps({"timestamp": timestamp, "source": "docker_stats",
                                                         "telemetry_error": str(error)}) + "\n")
                except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                    output.write(json.dumps({"timestamp": timestamp, "run_id": self.run_id,
                                             "source": "docker_stats", "error": str(error)}) + "\n")
                self.stop_event.wait(1)

    def publish(self, stats):
        mapping = subprocess.run(COMPOSE + ["port", "app", "8080"], text=True, capture_output=True,
                                 timeout=5, check=True, env=self.env).stdout.strip().splitlines()
        if not mapping:
            return
        query = urllib.parse.urlencode({
            "cpu_percent": float(stats["CPUPerc"].removesuffix("%")),
            "memory_percent": float(stats["MemPerc"].removesuffix("%")),
        })
        request = urllib.request.Request("http://" + mapping[0] + "/resources?" + query, method="POST")
        token = (self.env if self.env is not None else os.environ).get("ADMIN_TOKEN")
        if token:
            request.add_header("Authorization", "Bearer " + token)
        with urllib.request.urlopen(request, timeout=2) as response:
            response.read()


def main():
    forwarded = sys.argv[1:]
    metadata = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    metadata.add_argument("--run-id", default=uuid.uuid4().hex[:12])
    run, _ = metadata.parse_known_args(forwarded)
    output = os.environ.get("RESULTS_DIR", str(ROOT / "results"))
    subprocess.run(COMPOSE + ["--profile", "tools", "build", "load"], check=True)
    sampler = ResourceSampler(output, run.run_id)
    command = COMPOSE + ["--profile", "tools", "run", "--rm", "--user",
                         os.environ.get("LOCAL_UID", str(os.getuid())) + ":"
                         + os.environ.get("LOCAL_GID", str(os.getgid())),
                         "load", "--output", "/results",
                         "--run-id", run.run_id, *forwarded,
                         "--scenario", "htap", "--mode", "both"]
    sampler.start()
    try:
        return subprocess.call(command)
    finally:
        sampler.stop()
        print("Container CPU/memory evidence: " + str(sampler.path), flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
