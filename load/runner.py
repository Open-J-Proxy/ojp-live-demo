#!/usr/bin/env python3
"""Dependency-free, bounded open-loop HTTP workload runner."""

import argparse
from array import array
import bisect
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime, timezone
from fractions import Fraction
import functools
import itertools
import json
import math
import os
from pathlib import Path
import random
import re
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


OPERATIONS = {"browse": 30, "history": 25, "checkout": 20,
              "review": 10, "account": 5, "report": 10}
LATENCY_BUCKETS = (.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60)


def positive_float(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be finite and greater than zero")
    return number


def nonnegative_float(value):
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("must be finite and nonnegative")
    return number


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return number


def percentage(value):
    number = int(value)
    if not 0 <= number <= 100:
        raise argparse.ArgumentTypeError("must be an integer from 0 to 100")
    return number


def rates(value):
    try:
        result = [positive_int(part.strip()) for part in value.split(",")]
    except (ValueError, argparse.ArgumentTypeError) as exc:
        raise argparse.ArgumentTypeError("expected ordered, positive integer CSV rates") from exc
    if any(rate > 10000 for rate in result):
        raise argparse.ArgumentTypeError("RPS must not exceed 10000")
    if len(set(result)) > 64:
        raise argparse.ArgumentTypeError("at most 64 distinct RPS levels are supported")
    return result


def parser():
    result = argparse.ArgumentParser(
        description=__doc__,
        epilog="Each mode/rate resets, warms up, drains, resets, then measures. "
               "Arrivals never wait for capacity: excess work is recorded as dropped. "
               "HTTP 202 is not SQL completion. Optional ADMIN_TOKEN authenticates control POSTs. "
               "Docker build context is the repository root.")
    result.add_argument("--url", default="http://app:8080", help="application base URL")
    result.add_argument("--mode", choices=("direct", "ojp", "both"), default="both")
    result.add_argument("--rps", type=rates, default=rates("8,16,32,64,128,256"),
                        help="ordered integer CSV rates, max 10000/64 distinct levels "
                             "(default: 8,16,32,64,128,256)")
    result.add_argument("--warmup", type=nonnegative_float, default=30,
                        help="warmup arrival seconds; fully drained (default: 30)")
    result.add_argument("--duration", type=positive_float, default=60,
                        help="measurement arrival seconds, excluding drain (default: 60)")
    result.add_argument("--max-in-flight", type=positive_int, default=512,
                        help="maximum outstanding requests AND worker threads (default: 512)")
    result.add_argument("--request-timeout", type=positive_float, default=30,
                        help="HTTP socket timeout seconds (default: 30)")
    result.add_argument("--readiness-timeout", type=positive_float, default=90,
                        help="maximum readiness polling seconds (default: 90)")
    result.add_argument("--seed", type=int, default=12345,
                        help="reproducible work seed (default: 12345)")
    result.add_argument("--run-id", default=uuid.uuid4().hex[:12])
    result.add_argument("--scenario", default="death-spiral",
                        choices=("death-spiral", "slow-queries", "failover",
                                 "throttling", "integration"))
    result.add_argument("--oltp-percent", type=percentage, default=90,
                        help="integer OLTP share; rescales 30:25:20:10:5 (default: 90)")
    result.add_argument("--olap-percent", type=percentage, default=10,
                        help="integer report share; total shares must be 100 (default: 10)")
    result.add_argument("--output", type=Path,
                        default=Path(os.environ.get(
                            "RESULTS_DIR", "/results" if Path("/results").is_dir() else "results")),
                        help="JSON/CSV output directory (default: RESULTS_DIR, mounted /results, "
                             "or local results)")
    return result


def arguments(argv=None):
    cli = parser()
    args = cli.parse_args(argv)
    if args.oltp_percent + args.olap_percent != 100:
        cli.error("--oltp-percent and --olap-percent must total 100")
    parsed = urllib.parse.urlsplit(args.url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        cli.error("--url must be an absolute http(s) URL")
    if not args.run_id or len(args.run_id) > 64:
        cli.error("--run-id must contain 1 to 64 characters")
    if not args.scenario or len(args.scenario) > 128:
        cli.error("--scenario must contain 1 to 128 characters")
    return args


def operation_weights(oltp=90, olap=10):
    weights = {op: weight * oltp for op, weight in OPERATIONS.items() if op != "report"}
    weights["report"] = olap * 90
    divisor = functools.reduce(math.gcd, weights.values())
    return {op: weight // divisor for op, weight in weights.items()}


def operations(seed, weights):
    """Shuffle exact-ratio blocks; independent RNG keeps work seeds stable."""
    rng = random.Random(seed)
    block = [op for op, weight in weights.items() for _ in range(weight)]
    while True:
        rng.shuffle(block)
        yield from block


def request(base_url, path, timeout, method="GET", query=None):
    url = base_url.rstrip("/") + path
    if query:
        url += "?" + urllib.parse.urlencode(query)
    req = urllib.request.Request(url, method=method)
    admin_token = os.environ.get("ADMIN_TOKEN")
    if method == "POST" and admin_token:
        req.add_header("Authorization", "Bearer " + admin_token)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            status = response.status
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        status = exc.code
        body = exc.read().decode("utf-8", errors="replace")
    try:
        data = json.loads(body)
    except (ValueError, TypeError):
        data = None
    return status, data, body


def control(args, path, query=None):
    status, data, _ = request(args.url, path, args.request_timeout, "POST", query)
    if status != 200 or (isinstance(data, dict) and data.get("success") is False):
        raise RuntimeError(f"{path} failed: HTTP {status}, response={data!r}")
    return data


def is_ready(status, data):
    if status != 200 or not isinstance(data, dict):
        return False
    if "ready" in data:
        return data["ready"] is True
    if "healthy" in data:
        return data["healthy"] is True
    return data.get("status") in ("ready", "ok", "healthy", "UP", "up")


def work(args, op, seed, phase, level):
    started = time.monotonic()
    result = {"accepted": False, "sql_completed": False, "successful_transactions": False,
              "timeouts": False, "error": None}
    try:
        status, data, _ = request(args.url, "/work", args.request_timeout, query={
            "op": op, "seed": seed, "phase": phase, "scenario": args.scenario, "level": level})
        result["accepted"] = (data["accepted"] is True
                              if isinstance(data, dict) and "accepted" in data
                              else 200 <= status < 300)
        if status != 200:
            result["error"] = f"http_{status}"
        elif not isinstance(data, dict):
            result["error"] = "invalid_json"
        elif data.get("operation", data.get("op")) != op:
            result["error"] = "operation_mismatch"
        elif data.get("success") is not True:
            result["error"] = "sql_failed"
        elif not data.get("sql_completed", True):
            result["error"] = "sql_not_completed"
        else:
            result["sql_completed"] = True
            result["successful_transactions"] = not bool(data.get("rollback", False))
            if not result["successful_transactions"]:
                result["error"] = "rollback"
    except (TimeoutError, socket.timeout):
        result["timeouts"] = True
        result["error"] = "timeout"
    except urllib.error.URLError as exc:
        result["timeouts"] = isinstance(exc.reason, (TimeoutError, socket.timeout))
        result["error"] = "timeout" if result["timeouts"] else "transport_error"
    except (OSError, ValueError) as exc:
        result["error"] = type(exc).__name__
    except Exception as exc:
        # A malformed/aborted HTTP response must not strand the drain condition.
        result["error"] = type(exc).__name__
    result["started"] = started
    result["completed"] = time.monotonic()
    return result


def readiness(args, level):
    deadline = time.monotonic() + args.readiness_timeout
    last = {}
    while time.monotonic() < deadline:
        try:
            for path in ("/health", "/ready"):
                status, data, _ = request(
                    args.url, path, min(args.request_timeout, max(.001, deadline - time.monotonic())))
                last[path] = {"status": status, "response": data}
                if not is_ready(status, data):
                    break
            else:
                probe = work(args, "browse" if args.oltp_percent else "report",
                             args.seed, "warmup", level)
                last["work_probe"] = probe
                if probe["successful_transactions"]:
                    return last
        except (OSError, urllib.error.URLError, ValueError) as exc:
            last["error"] = str(exc)
        time.sleep(min(.2, max(0, deadline - time.monotonic())))
    raise RuntimeError(f"readiness or real-work probe timed out: {last!r}")


def percentiles(samples):
    ordered = sorted(samples)
    if not ordered:
        return {key: None for key in ("p50", "p90", "p95", "p99", "max")}
    result = {}
    for name, fraction in (("p50", .50), ("p90", .90), ("p95", .95), ("p99", .99)):
        result[name] = ordered[max(0, math.ceil(len(ordered) * fraction) - 1)] * 1000
    result["max"] = ordered[-1] * 1000
    return result


def utc_timestamp(epoch_seconds):
    return datetime.fromtimestamp(epoch_seconds, timezone.utc).isoformat(
        timespec="milliseconds").replace("+00:00", "Z")


def run_phase(args, rps, duration, phase, level, seed):
    counts = Counter({name: 0 for name in (
        "scheduled", "dispatched", "accepted", "sql_completed", "successful_transactions",
        "failed_transactions", "timeouts", "dropped", "in_flight", "peak_in_flight", "completed")})
    per_op = {op: Counter(scheduled=0, completed=0, dispatched=0, dropped=0,
                          sql_completed=0, successful_transactions=0) for op in OPERATIONS}
    samples = {op: {"end_to_end": array("d"), "service": array("d")} for op in OPERATIONS}
    errors = Counter()
    pending = set()
    condition = threading.Condition(threading.RLock())
    histogram = [0] * (len(LATENCY_BUCKETS) + 1)
    latency_sum = 0.0
    telemetry = {"samples_sent": 0, "errors": Counter(), "final_sent": False}
    stop_telemetry = threading.Event()
    rng = random.Random(seed)
    schedule = operations(seed, operation_weights(args.oltp_percent, args.olap_percent))
    # Decimal CLI values avoid phantom arrivals from binary float multiplication.
    target_fraction = Fraction(str(rps)) * Fraction(str(duration))
    target = math.ceil(target_fraction)
    wall_start = time.time()
    start = time.monotonic()

    def publish(final=False):
        with condition:
            payload = {key: counts[key] for key in (
                "scheduled", "dispatched", "dropped", "accepted", "sql_completed",
                "successful_transactions", "failed_transactions", "timeouts", "completed")}
            cumulative = list(itertools.accumulate(histogram))
            payload.update({"inflight": counts["in_flight"], "phase": phase, "level": level,
                            "scenario": args.scenario, "run_id": args.run_id,
                            "elapsed_seconds": time.monotonic() - start,
                            "final": str(final).lower(), "latency_count": counts["completed"],
                            "latency_sum_seconds": latency_sum,
                            "latency_bucket_counts": ",".join(map(str, cumulative))})
        try:
            control(args, "/arrivals", payload)
            telemetry["samples_sent"] += 1
            telemetry["final_sent"] = final
        except Exception as exc:
            telemetry["errors"][type(exc).__name__] += 1

    def publish_periodically():
        while not stop_telemetry.wait(1):
            publish()

    def complete(future, op, scheduled):
        nonlocal latency_sum
        result = future.result()
        with condition:
            for key in ("accepted", "sql_completed", "successful_transactions", "timeouts"):
                counts[key] += int(result[key])
            counts["failed_transactions"] += int(not result["successful_transactions"])
            counts["completed"] += 1
            counts["in_flight"] -= 1
            per_op[op]["completed"] += 1
            per_op[op]["sql_completed"] += int(result["sql_completed"])
            per_op[op]["successful_transactions"] += int(result["successful_transactions"])
            latency = result["completed"] - scheduled
            samples[op]["end_to_end"].append(latency)
            histogram[bisect.bisect_left(LATENCY_BUCKETS, latency)] += 1
            latency_sum += latency
            samples[op]["service"].append(result["completed"] - result["started"])
            if result["error"]:
                errors[result["error"]] += 1
            pending.remove(future)
            condition.notify_all()

    telemetry_thread = threading.Thread(target=publish_periodically, name="load-telemetry", daemon=True)
    telemetry_thread.start()
    try:
        with ThreadPoolExecutor(max_workers=args.max_in_flight) as executor:
            for index in range(target):
                scheduled = start + index / rps
                delay = scheduled - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                op = next(schedule)
                work_seed = rng.getrandbits(63)
                with condition:
                    counts["scheduled"] += 1
                    per_op[op]["scheduled"] += 1
                    if len(pending) >= args.max_in_flight:
                        counts["dropped"] += 1
                        per_op[op]["dropped"] += 1
                        continue
                    counts["dispatched"] += 1
                    counts["in_flight"] += 1
                    counts["peak_in_flight"] = max(counts["peak_in_flight"], counts["in_flight"])
                    per_op[op]["dispatched"] += 1
                    future = executor.submit(work, args, op, work_seed, phase, level)
                    pending.add(future)
                    future.add_done_callback(functools.partial(complete, op=op, scheduled=scheduled))
            arrival_end = start + duration
            delay = arrival_end - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            with condition:
                in_flight_at_end = counts["in_flight"]
                while pending:
                    condition.wait()
        finished = time.monotonic()
    finally:
        stop_telemetry.set()
        telemetry_thread.join()
        publish(final=True)
    combined = {kind: array("d") for kind in ("end_to_end", "service")}
    for op in OPERATIONS:
        for kind in combined:
            combined[kind].extend(samples[op][kind])
        per_op[op]["latency_ms"] = {kind: percentiles(values)
                                     for kind, values in samples[op].items()}
    return {"phase": phase, "rps": rps, "arrival_duration_seconds": duration,
            "start_time": utc_timestamp(wall_start),
            "end_arrivals_time": utc_timestamp(wall_start + duration),
            "end_time": utc_timestamp(wall_start + finished - start),
            "start_epoch_seconds": wall_start,
            "end_arrivals_epoch_seconds": wall_start + duration,
            "end_epoch_seconds": wall_start + finished - start,
            "elapsed_seconds": finished - start,
            "drain_seconds": max(0, finished - arrival_end),
            "in_flight_at_arrival_end": in_flight_at_end,
            "counts": dict(counts), "operations": per_op, "errors": dict(errors),
            "telemetry": telemetry,
            "scheduled_latency_histogram": {
                "buckets": dict(zip([*map(str, LATENCY_BUCKETS), "+Inf"],
                                    itertools.accumulate(histogram))),
                "count": counts["completed"], "sum_seconds": latency_sum},
            "latency_ms": {kind: percentiles(values) for kind, values in combined.items()}}


def snapshot(args, path):
    try:
        status, data, body = request(args.url, path, args.request_timeout)
        return {"status": status, "response": data if data is not None else body}
    except (OSError, urllib.error.URLError, ValueError) as exc:
        return {"error": str(exc)}


def run_step(args, mode, rps, index):
    level = format(rps, ".12g")
    result = {"run_id": args.run_id, "scenario": args.scenario, "mode": mode, "level": level,
              "rps": rps, "seed": args.seed, "request_timeout": args.request_timeout,
              "max_in_flight": args.max_in_flight,
              "counter_semantics": {
                  "accepted": "Application accepted=true if supplied, otherwise HTTP 2xx; "
                              "admission is not SQL completion",
                  "failed_transactions": "Dispatched attempts without a successful transaction, "
                                         "including rejection, timeout, SQL failure and rollback",
                  "completed": "Finished HTTP attempts, including failures and timeouts"},
              "operation_weights": operation_weights(args.oltp_percent, args.olap_percent)}
    try:
        result["mode_response"] = control(args, "/mode", {"value": mode})
        result["warmup_reset_response"] = control(args, "/reset")
        context = {"phase": "warmup", "scenario": args.scenario, "level": level,
                   "run_id": args.run_id}
        control(args, "/context", context)
        result["readiness_before_warmup"] = readiness(args, level)
        # Remove effects of the real-work readiness probe.
        control(args, "/reset")
        control(args, "/context", context)
        result["warmup"] = run_phase(args, rps, args.warmup, "warmup", level, args.seed + index)
        result["measurement_reset_response"] = control(args, "/reset")
        result["readiness_before_measure"] = readiness(args, level)
        control(args, "/reset")
        context["phase"] = "measure"
        control(args, "/context", context)
        result["metrics_before_measure"] = snapshot(args, "/metrics")
        result["measure"] = run_phase(args, rps, args.duration, "measure", level, args.seed + index)
    except (OSError, urllib.error.URLError, ValueError, RuntimeError) as exc:
        result["error"] = str(exc)
    finally:
        result["readiness_after"] = snapshot(args, "/ready")
        result["metrics_after"] = snapshot(args, "/metrics")
    return result


def export(args, result, step):
    args.output.mkdir(parents=True, exist_ok=True)
    safe_id = re.sub(r"[^a-zA-Z0-9_.-]", "_", args.run_id)
    stem = args.output / f"{safe_id}-{step:03d}-{result['mode']}-{result['level']}"
    json_path = stem.with_name(stem.name + ".json")
    staging = json_path.with_name(json_path.name + ".partial")
    staging.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(staging, json_path)
    rows = []
    for phase in ("warmup", "measure"):
        if phase not in result:
            continue
        report = result[phase]
        for op in ("all", *OPERATIONS):
            values = report["counts"] if op == "all" else report["operations"][op]
            row = {"run_id": args.run_id, "scenario": args.scenario, "mode": result["mode"],
                   "level": result["level"], "phase": phase, "operation": op,
                   "start_time": report["start_time"],
                   "end_arrivals_time": report["end_arrivals_time"],
                   "end_time": report["end_time"],
                   "errors_json": json.dumps(report["errors"], sort_keys=True),
                   "readiness_before_measure_json": json.dumps(
                       result.get("readiness_before_measure"), sort_keys=True),
                   "readiness_after_json": json.dumps(result.get("readiness_after"), sort_keys=True),
                   **{key: value for key, value in values.items() if key != "latency_ms"}}
            latency = report["latency_ms"] if op == "all" else values["latency_ms"]
            for kind, quantiles in latency.items():
                row.update({f"{kind}_{key}_ms": value for key, value in quantiles.items()})
            rows.append(row)
    if not rows:
        rows = [{"run_id": args.run_id, "mode": result["mode"],
                 "level": result["level"], "error": result.get("error", "")}]
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with stem.with_name(stem.name + ".csv").open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return json_path


def main(argv=None):
    args = arguments(argv)
    modes = ("direct", "ojp") if args.mode == "both" else (args.mode,)
    step = 0
    for mode in modes:
        for index, rps in enumerate(args.rps):
            step += 1
            result = run_step(args, mode, rps, index)
            output = export(args, result, step)
            print(json.dumps({"output": str(output), "mode": mode, "rps": rps,
                              "counts": result.get("measure", {}).get("counts"),
                              "error": result.get("error")}), flush=True)
            if "error" in result:
                return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
