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
import sys
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


def operation_mix(value):
    weights = {}
    try:
        for entry in value.split(","):
            name, weight = (part.strip() for part in entry.split("="))
            if name not in OPERATIONS or name in weights:
                raise ValueError("unknown or duplicate operation")
            number = int(weight)
            if number < 0:
                raise ValueError("negative operation weight")
            weights[name] = number
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "expected each known operation exactly once with nonnegative integer weights") from exc
    if set(weights) != set(OPERATIONS) or sum(weights.values()) != 100:
        raise argparse.ArgumentTypeError("all six operation weights are required and must total 100")
    return weights


def parser():
    result = argparse.ArgumentParser(
        description=__doc__,
        epilog="Default profile: --mode both --rps 8,256,8,8 --warmup 10 --duration 30 "
               "--oltp-percent 98 --olap-percent 2. Each mode/rate resets, warms up, "
               "drains, resets, then measures. "
               "Arrivals never wait for capacity: excess work is recorded as dropped. "
               "HTTP 202 is not SQL completion. Optional ADMIN_TOKEN authenticates control POSTs. "
               "Docker build context is the repository root.")
    result.add_argument("--url", default="http://app:8080", help="application base URL")
    result.add_argument("--mode", choices=("direct", "ojp", "both"), default="both",
                        help="connection path to test (default: both, sequentially)")
    result.add_argument("--rps", type=rates, default=rates("8,256,8,8"),
                        help="ordered integer CSV rates, max 10000/64 distinct levels "
                             "(default: 8,256,8,8; baseline, spike, two post-spike windows)")
    result.add_argument("--warmup", type=nonnegative_float, default=10,
                        help="warmup arrival seconds; fully drained (default: 10)")
    result.add_argument("--warmup-only", action="store_true",
                        help="reset, check readiness, warm up and drain; never measure")
    result.add_argument("--duration", type=positive_float, default=30,
                        help="measurement arrival seconds, excluding drain (default: 30)")
    result.add_argument("--max-in-flight", type=positive_int, default=512,
                        help="maximum outstanding requests AND worker threads (default: 512)")
    result.add_argument("--request-timeout", type=positive_float, default=30,
                        help="measured SQL request timeout seconds (default: 30)")
    result.add_argument("--control-timeout", type=positive_float, default=210,
                        help="mode-switch/reset/telemetry timeout seconds (default: 210)")
    result.add_argument("--readiness-timeout", type=positive_float, default=90,
                        help="maximum readiness polling seconds (default: 90)")
    result.add_argument("--seed", type=int, default=12345,
                        help="reproducible work seed (default: 12345)")
    result.add_argument("--run-id", default=uuid.uuid4().hex[:12])
    result.add_argument("--scenario", default="htap",
                        choices=("htap", "failover", "integration"))
    result.add_argument("--ojp-metrics-url", default="http://ojp:9159/metrics",
                        help="OJP Prometheus endpoint used to verify real slow-query classification")
    result.add_argument("--oltp-percent", type=percentage, default=98,
                        help="integer OLTP share; rescales 30:25:20:10:5 (default: 98)")
    result.add_argument("--olap-percent", type=percentage, default=2,
                        help="integer report share; total shares must be 100 (default: 2)")
    result.add_argument("--operation-mix", type=operation_mix,
                        help="optional exact integer percentages, all six keys required: "
                             "browse=30,history=25,checkout=20,review=10,account=5,report=10; "
                             "report must match --olap-percent and others --oltp-percent; zeros allowed")
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
    if args.operation_mix is not None and (
            args.operation_mix["report"] != args.olap_percent or
            sum(weight for op, weight in args.operation_mix.items() if op != "report") !=
            args.oltp_percent):
        cli.error("--operation-mix report/other weights must match --olap-percent/--oltp-percent")
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


def control(args, path, query=None, timeout=None):
    status, data, _ = request(args.url, path, timeout or args.control_timeout, "POST", query)
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


def work(args, op, seed, phase, level, timeout=None):
    started = time.monotonic()
    result = {"accepted": False, "sql_completed": False, "successful_transactions": False,
              "timeouts": False, "error": None}
    try:
        status, data, _ = request(args.url, "/work", timeout or args.request_timeout, query={
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
                    args.url, path, min(args.control_timeout, max(.001, deadline - time.monotonic())))
                last[path] = {"status": status, "response": data}
                if not is_ready(status, data):
                    break
            else:
                weights = args.operation_mix or operation_weights(
                    args.oltp_percent, args.olap_percent)
                probe = work(args, next(op for op, weight in weights.items() if weight),
                             args.seed, "warmup", level,
                             timeout=min(args.control_timeout,
                                         max(.001, deadline - time.monotonic())))
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


def run_phase(args, rps, duration, phase, level, seed, mode=None):
    mode = mode or ("direct" if args.mode == "both" else args.mode)
    counts = Counter({name: 0 for name in (
        "scheduled", "dispatched", "accepted", "sql_completed", "successful_transactions",
        "failed_transactions", "timeouts", "dropped", "in_flight", "peak_in_flight", "completed",
        "arrival_window_completed", "arrival_window_sql_completed",
        "arrival_window_successful_transactions")})
    per_op = {op: Counter(scheduled=0, completed=0, dispatched=0, dropped=0,
                          sql_completed=0, successful_transactions=0) for op in OPERATIONS}
    samples = {op: {"end_to_end": array("d"), "service": array("d")} for op in OPERATIONS}
    errors = Counter()
    pending = set()
    condition = threading.Condition(threading.RLock())
    histogram = [0] * (len(LATENCY_BUCKETS) + 1)
    latency_sum = 0.0
    telemetry = {"samples_sent": 0, "errors": Counter(), "final_sent": False}
    postgres_cpu_samples = []
    postgres_cpu_sample_times = []
    postgres_connection_samples = []
    postgres_connection_sample_times = []
    stop_telemetry = threading.Event()
    rng = random.Random(seed)
    weights = args.operation_mix or operation_weights(args.oltp_percent, args.olap_percent)
    schedule = operations(seed, weights)
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
                            "mode": mode, "errors": counts["failed_transactions"],
                            "completed": counts["successful_transactions"],
                            "http_completed": counts["completed"],
                            "reset": str(telemetry["samples_sent"] == 0).lower(),
                            "scenario": args.scenario, "run_id": args.run_id,
                            "elapsed_seconds": time.monotonic() - start,
                            "final": str(final).lower(), "latency_count": counts["completed"],
                            "latency_sum_seconds": latency_sum,
                            "latency_bucket_counts": ",".join(map(str, cumulative))})
        try:
            control(args, "/arrivals", payload, timeout=min(args.control_timeout, 10))
            telemetry["samples_sent"] += 1
            telemetry["final_sent"] = final
        except Exception as exc:
            telemetry["errors"][type(exc).__name__] += 1
            if final:
                print(f"WARNING: final /arrivals publication failed for phase={phase} "
                      f"level={level} ({type(exc).__name__}); metrics may be stale",
                      file=sys.stderr, flush=True)

    def publish_periodically():
        while not stop_telemetry.wait(1):
            publish()
            if phase == "measure":
                try:
                    sample_timeout = min(args.control_timeout, 10)
                    ready_status, ready_data, _ = request(args.url, "/ready", sample_timeout)
                    status, _, body = request(args.url, "/metrics", sample_timeout)
                    if status == 200:
                        cpu = metric_value(body, "demo_postgres_cpu_percent")
                        sampled_at = metric_value(body, "demo_postgres_resource_timestamp_seconds")
                        if (cpu is not None and sampled_at is not None
                                and wall_start <= sampled_at <= wall_start + duration):
                            postgres_cpu_samples.append(cpu)
                            postgres_cpu_sample_times.append(sampled_at)
                    if ready_status == 200 and isinstance(ready_data, dict):
                        sampled_at = ready_data.get("postgres_snapshot_timestamp_seconds")
                        physical = ready_data.get("direct_sessions" if mode == "direct" else "ojp_sessions")
                        if (isinstance(sampled_at, (int, float))
                                and isinstance(physical, (int, float)) and physical >= 0
                                and wall_start <= sampled_at <= wall_start + duration):
                            postgres_connection_samples.append(physical)
                            postgres_connection_sample_times.append(sampled_at)
                except (OSError, urllib.error.URLError, ValueError, KeyError, TypeError):
                    pass

    def complete(future, op, scheduled):
        nonlocal latency_sum
        result = future.result()
        with condition:
            for key in ("accepted", "sql_completed", "successful_transactions", "timeouts"):
                counts[key] += int(result[key])
            counts["failed_transactions"] += int(not result["successful_transactions"])
            counts["completed"] += 1
            if result["completed"] <= start + duration:
                counts["arrival_window_completed"] += 1
                counts["arrival_window_sql_completed"] += int(result["sql_completed"])
                counts["arrival_window_successful_transactions"] += int(
                    result["successful_transactions"])
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

    # Clear reused metric labels before any arrivals, without charging control latency
    # to the open-loop schedule.
    publish()
    wall_start = time.time()
    start = time.monotonic()
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
            "throughput": {
                "offered_per_second": counts["scheduled"] / duration if duration else 0,
                "successful_completions_per_second": counts["successful_transactions"] /
                                                    (finished - start),
                "sql_completions_per_second": counts["sql_completed"] / (finished - start),
                "completion_elapsed_seconds": finished - start,
                "arrival_window_successful_completions_per_second":
                    counts["arrival_window_successful_transactions"] / duration if duration else 0,
                "arrival_window_sql_completions_per_second":
                    counts["arrival_window_sql_completed"] / duration if duration else 0},
            "error_fraction": (counts["failed_transactions"] / counts["dispatched"]
                               if counts["dispatched"] else 0),
            "drop_fraction": counts["dropped"] / counts["scheduled"] if counts["scheduled"] else 0,
            "telemetry": telemetry,
            "postgres_cpu_samples": postgres_cpu_samples,
            "postgres_cpu_sample_times": postgres_cpu_sample_times,
            "postgres_connection_samples": postgres_connection_samples,
            "postgres_connection_sample_times": postgres_connection_sample_times,
            "scheduled_latency_histogram": {
                "buckets": dict(zip([*map(str, LATENCY_BUCKETS), "+Inf"],
                                    itertools.accumulate(histogram))),
                "count": counts["completed"], "sum_seconds": latency_sum},
            "latency_ms": {kind: percentiles(values) for kind, values in combined.items()},
            "oltp_latency_ms": percentiles([sample for op in OPERATIONS if op != "report"
                                              for sample in samples[op]["end_to_end"]]),
            "olap_latency_ms": percentiles(samples["report"]["end_to_end"])}


def snapshot(args, path):
    try:
        status, data, body = request(args.url, path, min(args.control_timeout, 10))
        return {"status": status, "response": data if data is not None else body}
    except (OSError, urllib.error.URLError, ValueError) as exc:
        return {"error": str(exc)}


def slow_query_metrics(args):
    try:
        status, _, body = request(args.ojp_metrics_url, "/metrics", min(args.control_timeout, 10))
        if status != 200:
            return None
        totals = {}
        pattern = re.compile(r'^ojp_sql_slow_executions_total(?:\{([^}]*)\})?\s+([0-9.eE+-]+)$')
        for line in body.splitlines():
            match = pattern.match(line)
            if match:
                labels = dict(re.findall(r'(\w+)="([^"]*)"', match.group(1) or ""))
                statement = labels.get("sql_statement", "unlabeled")
                totals[statement] = totals.get(statement, 0.0) + float(match.group(2))
        return totals
    except (OSError, urllib.error.URLError, ValueError):
        return None


def metric_value(text, name):
    pattern = re.compile(r"^" + re.escape(name) + r"(?:\{[^}]*\})?\s+([0-9.eE+-]+)$")
    for line in text.splitlines():
        match = pattern.match(line)
        if match:
            return float(match.group(1))
    return None


def run_step(args, mode, rps, index):
    level = format(rps, ".12g")
    result = {"run_id": args.run_id, "scenario": args.scenario, "mode": mode, "level": level,
              "warmup_only": args.warmup_only,
              "rps": rps, "seed": args.seed, "request_timeout": args.request_timeout,
    "control_timeout": args.control_timeout,
              "max_in_flight": args.max_in_flight,
              "counter_semantics": {
                  "accepted": "Application accepted=true if supplied, otherwise HTTP 2xx; "
                              "admission is not SQL completion",
                  "failed_transactions": "Dispatched attempts without a successful transaction, "
                                         "including rejection, timeout, SQL failure and rollback",
                  "completed": "Finished HTTP attempts, including failures and timeouts"},
              "telemetry_counter_semantics": {
                  "completed": "Successful transactions, as required by /arrivals",
                  "http_completed": "Finished HTTP attempts, matching phase counts.completed",
                  "sql_completed": "Synchronous SQL completions, including deliberate rollback"},
              "operation_weights": args.operation_mix or operation_weights(
                  args.oltp_percent, args.olap_percent)}
    slow_before = slow_query_metrics(args) if mode == "ojp" else None
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
        result["warmup"] = run_phase(args, rps, args.warmup, "warmup", level,
                                     args.seed + index, mode=mode)
        if args.warmup_only:
            if args.warmup > 0 and result["warmup"]["counts"]["sql_completed"] == 0:
                result["error"] = "warmup-only interval completed zero SQL operations"
            return result
        result["measurement_reset_response"] = control(args, "/reset")
        result["readiness_before_measure"] = readiness(args, level)
        control(args, "/reset")
        context["phase"] = "measure"
        control(args, "/context", context)
        result["metrics_before_measure"] = snapshot(args, "/metrics")
        result["measure"] = run_phase(args, rps, args.duration, "measure", level,
                                      args.seed + index, mode=mode)
        if result["measure"]["counts"]["sql_completed"] == 0:
            result["error"] = "measurement interval completed zero SQL operations"
        if mode == "ojp":
            slow_after = slow_query_metrics(args)
            if slow_before is not None and slow_after is not None:
                statements = sorted(set(slow_before) | set(slow_after))
                deltas = {statement: max(0.0, slow_after.get(statement, 0.0)
                                          - slow_before.get(statement, 0.0))
                          for statement in statements}
                count = sum(deltas.values())
                result["slow_query_classification"] = {
                    "observed": count > 0, "count": count, "by_statement": deltas,
                    "message": ("OJP classified slow SQL during this measurement."
                                if count > 0 else
                                "No slow SQL classification was observed; segregation is enabled, "
                                "but analytical-query isolation was not demonstrated.")}
            else:
                result["slow_query_classification"] = {
                    "observed": None, "count": None, "by_statement": {},
                    "message": "OJP slow-query metrics were unavailable; classification could not be verified."}
    except (OSError, urllib.error.URLError, ValueError, RuntimeError) as exc:
        result["error"] = str(exc)
    finally:
        result["readiness_after"] = snapshot(args, "/ready")
        result["metrics_after"] = snapshot(args, "/metrics")
    report = result.get("measure")
    if report:
        cpu_samples = report["postgres_cpu_samples"]
        physical_samples = report["postgres_connection_samples"]
        cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else None
        physical = sum(physical_samples) / len(physical_samples) if physical_samples else None
        if physical is not None and cpu is not None:
            classification = result.get("slow_query_classification", {})
            values = {
                "mode": mode, "phase": "measure", "scenario": args.scenario, "level": level,
                "successful_rps": report["throughput"]["successful_completions_per_second"],
                "oltp_p95_ms": (report["oltp_latency_ms"]["p95"]
                                if report["oltp_latency_ms"]["p95"] is not None else -1),
                "olap_p95_ms": (report["olap_latency_ms"]["p95"]
                                if report["olap_latency_ms"]["p95"] is not None else -1),
                "error_percent": report["error_fraction"] * 100,
                "dropped_requests": report["counts"]["dropped"],
                "physical_connections": physical, "postgres_cpu_percent": cpu,
                "slow_classifications": classification.get("count") or 0,
            }
            result["postgres_cpu_measurement_samples"] = len(cpu_samples)
            result["postgres_cpu_sample_times"] = report["postgres_cpu_sample_times"]
            result["postgres_connection_measurement_samples"] = len(physical_samples)
            result["postgres_connection_sample_times"] = report["postgres_connection_sample_times"]
            try:
                result["benchmark_result_published"] = control(
                    args, "/benchmark-result", values)
            except (OSError, urllib.error.URLError, ValueError, RuntimeError) as exc:
                result["benchmark_result_error"] = str(exc)
        else:
            result["benchmark_result_error"] = (
                "Measured result could not be published: no PostgreSQL CPU sample overlapped the "
                "measurement arrival window or physical connection samples were unavailable.")
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
            if op == "all":
                row.update(report["throughput"])
                row.update(error_fraction=report["error_fraction"],
                           drop_fraction=report["drop_fraction"])
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
    if (args.scenario == "htap" and args.mode in ("ojp", "both")
            and os.environ.get("OJP_SLOW_QUERY_SEGREGATION_VERIFIED") != "true"):
        print("ERROR: OJP HTAP requires scripts/htap.sh to verify the running server's "
              "slow-query segregation configuration.", file=sys.stderr)
        return 2
    safe_id = re.sub(r"[^a-zA-Z0-9_.-]", "_", args.run_id)
    if any(path.is_file() for extension in ("json", "csv")
           for path in args.output.glob(f"{safe_id}-*.{extension}")):
        print(f"ERROR: results already exist for run ID {args.run_id!r} in {args.output}; "
              "choose a unique --run-id", file=sys.stderr)
        return 2
    modes = ("direct", "ojp") if args.mode == "both" else (args.mode,)
    step = 0
    failures = []
    for mode in modes:
        for index, rps in enumerate(args.rps):
            step += 1
            result = run_step(args, mode, rps, index)
            output = export(args, result, step)
            phase = "warmup" if args.warmup_only else "measure"
            print(json.dumps({"output": str(output), "mode": mode, "rps": rps,
                              "phase": phase, "counts": result.get(phase, {}).get("counts"),
                              "slow_query_classification": result.get("slow_query_classification"),
                              "benchmark_result_published": bool(result.get("benchmark_result_published")),
                              "error": result.get("error")}), flush=True)
            if "error" in result:
                failures.append({"mode": mode, "rps": rps, "error": result["error"]})
    if failures:
        print(json.dumps({"benchmark_failures": failures}), file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
