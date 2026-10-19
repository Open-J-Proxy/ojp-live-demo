#!/usr/bin/env python3
"""Real-container integration tests. Uses only the Python standard library."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import csv
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from resources import ResourceSampler
from compose_command import compose_command
from http_client import HTTPBridge

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts"
COMPOSE = ["docker", "compose", "--project-directory", str(ROOT), "-f", str(ROOT / "compose.yaml")]
TABLES = ("users", "products", "orders", "order_items", "reviews")
DELAY_PROXY = r'''
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os, time, urllib.request, urllib.error
class Proxy(BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def forward(self):
        if self.path.startswith("/work?"):
            time.sleep(.25)
        data = self.rfile.read(int(self.headers.get("Content-Length", "0"))) if self.command == "POST" else None
        headers = {}
        if self.headers.get("Authorization"):
            headers["Authorization"] = self.headers["Authorization"]
        request = urllib.request.Request(os.environ["TARGET_APP_URL"] + self.path, data=data,
                                         method=self.command, headers=headers)
        try:
            response = urllib.request.urlopen(request, timeout=60)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = response.read()
            self.send_response(response.code)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
    do_GET = forward
    do_POST = forward
ThreadingHTTPServer(("0.0.0.0", 8080), Proxy).serve_forever()
'''


def metric_rows(text, name):
    pattern = re.compile(r"^" + re.escape(name) + r"(?:\{([^}]*)\})?\s+([0-9.eE+-]+)$")
    rows = []
    for line in text.splitlines():
        match = pattern.match(line)
        if match:
            labels = dict(re.findall(r'(\w+)="([^"]*)"', match.group(1) or ""))
            rows.append((labels, float(match.group(2))))
    return rows


class Environment:
    def __init__(self, options):
        self.options = options
        self.project = "ojp-it-" + str(os.getpid())
        self.output = ROOT / "results" / "integration" / self.project
        self.output.mkdir(parents=True, exist_ok=True)
        self.env = os.environ.copy()
        self.env.update({
            "COMPOSE_PROJECT_NAME": self.project,
            "TEST_ENDPOINTS": "true", "DIRECT_POOL_SIZE": "20" if options.small else "200",
            "HTTP_WORKERS": "512",
            "PG_MAX_CONNECTIONS": "300", "START_TIMEOUT": "300",
            "SLOW_QUERY_ENABLED": "true", "SLOW_QUERY_THRESHOLD_MS": "1",
            "SEED_USERS": "200", "SEED_PRODUCTS": "100", "SEED_ORDERS": "1000",
            "SEED_ITEMS": "3000", "SEED_REVIEWS": "500", "SEED": "12345",
            "RESULTS_DIR": str(self.output),
            "LOCAL_UID": str(os.getuid()), "LOCAL_GID": str(os.getgid()),
        })
        for name in ("APP_PORT", "POSTGRES_PORT", "PROMETHEUS_PORT", "GRAFANA_PORT"):
            self.env[name] = "0"
        self.env.pop("APP_URL", None)
        self.base = None
        self.bridge = None
        # Never inherit a user's production connection override into the isolated stack.
        self.env.pop("OJP_URL", None)
        self.env.pop("OJP_HA_URL", None)
        self.env.pop("COMPOSE_COMPATIBILITY", None)
        postgres = self.project + "-postgres-1"
        primary = self.project + "-ojp-1"
        self.env["APP_INTERNAL_URL"] = "http://" + self.project + "-app-1:8080"
        backend = "_postgresql://" + postgres + ":5432/shop?ApplicationName=demo-ojp"
        self.env["OJP_HA_URL"] = "jdbc:ojp[" + ",".join(
            self.project + "-" + node + "-1:1059" for node in ("ojp", "ojp2", "ojp3")) + "]" + backend
        prometheus = {
            "global": {"scrape_interval": "2s", "evaluation_interval": "2s"},
            "scrape_configs": [
                {"job_name": job, "static_configs": [{"targets": [target]}]}
                for job, target in (
                    ("app", self.project + "-app-1:8081"),
                    ("postgres", self.project + "-postgres-exporter-1:9187"),
                    ("ojp", primary + ":9159"), ("prometheus", "localhost:9090"))
            ] + [{"job_name": "ojp-ha", "dns_sd_configs": [{
                "names": [self.project + "-ojp2-1", self.project + "-ojp3-1"],
                "type": "A", "port": 9159, "refresh_interval": "5s"}]}],
        }
        prom_file = self.output / "prometheus.json"
        prom_file.write_text(json.dumps(prometheus, indent=2) + "\n")
        override = {
            "services": {
                "app": {"environment": {
                    "PG_URL": "jdbc:postgresql://" + postgres + ":5432/shop",
                    "OJP_URL": "${OJP_URL:-jdbc:ojp[" + primary + ":1059]" + backend + "}",
                }},
                "postgres-exporter": {"environment": {
                    "DATA_SOURCE_NAME": "postgresql://demo:${DB_PASSWORD:-demo-local-only}@"
                    + postgres + ":5432/shop?sslmode=disable",
                }},
                "prometheus": {"volumes": [str(prom_file) + ":/etc/prometheus/prometheus.yml:ro"]},
            },
        }
        override_file = self.output / "compose.override.json"
        override_file.write_text(json.dumps(override, indent=2) + "\n")
        self.env["COMPOSE_OVERRIDE_FILE"] = str(override_file)
        self.sessions = 20 if options.small else 200
        self.commands = (self.output / "commands.log").open("w", buffering=1)

    def run(self, args, timeout=600, capture=False, check=True):
        self.commands.write("$ " + " ".join(map(str, args)) + "\n")
        result = subprocess.run(list(map(str, args)), env=self.env, cwd=ROOT, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
        self.commands.write(result.stdout)
        if check and result.returncode:
            raise RuntimeError(f"Command exited {result.returncode}: {' '.join(map(str, args))}\n"
                               + result.stdout[-12000:])
        if not capture and result.returncode:
            print(result.stdout[-12000:], file=sys.stderr)
        return result.stdout

    def compose(self, *args, **kwargs):
        return self.run(compose_command(self.env) + list(args), **kwargs)

    def sql(self, query):
        return self.compose("exec", "-T", "postgres", "sh", "-ec",
                            'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1"',
                            "psql", query, capture=True).strip()

    def service_url(self, service, port):
        if self.bridge:
            return f"http://{self.project}-{service}-1:{port}"
        return "http://" + self.compose("port", service, str(port), capture=True).strip()

    def refresh_base(self):
        self.base = self.service_url("app", 8080)

    def counts(self):
        query = "SELECT " + ", ".join(f"(SELECT count(*) FROM {table})" for table in TABLES)
        return tuple(map(int, self.sql(query).split("|")))

    def call(self, path, data=None, method=None, timeout=60, base=None, headers=None):
        raw = None if data is None else json.dumps(data).encode()
        request = urllib.request.Request((base or self.base) + path, data=raw, method=method,
                                         headers={"Content-Type": "application/json", **(headers or {})})
        if method == "POST" and self.env.get("ADMIN_TOKEN"):
            request.add_header("Authorization", "Bearer " + self.env["ADMIN_TOKEN"])
        if self.bridge:
            return self.bridge.request((base or self.base) + path, data, method, timeout,
                                       dict(request.header_items()))
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                status, body = response.status, response.read().decode()
        except urllib.error.HTTPError as error:
            status, body = error.code, error.read().decode()
        try:
            body = json.loads(body)
        except ValueError:
            pass
        return status, body

    def mode(self, selected):
        status, body = self.call("/mode?value=" + selected, {}, "POST")
        if status != 200:
            raise AssertionError((status, body))

    def reset(self):
        status, body = self.call("/reset", {}, "POST", timeout=120)
        if status != 200:
            raise AssertionError((status, body))

    def work(self, op, **parameters):
        query = {"op": op, "seed": 12345, "phase": "measure", "scenario": "integration", "level": 1}
        query.update(parameters)
        return self.call("/work?" + urllib.parse.urlencode(query))

    def metrics(self):
        status, body = self.call("/metrics")
        if status != 200 or not isinstance(body, str):
            raise AssertionError((status, body))
        return body

    def runner(self, run_id, *options):
        sampler = ResourceSampler(self.output, run_id, self.env)
        sampler.transport = self.bridge
        sampler.start()
        try:
            self.compose("--profile", "tools", "run", "--rm", "--user",
                         f"{os.getuid()}:{os.getgid()}", "load", "--url", self.env["APP_INTERNAL_URL"],
                         "--scenario", "integration", "--run-id", run_id,
                         "--output", "/results", *options)
        finally:
            sampler.stop()
        results = [json.loads(path.read_text())
                   for path in sorted(self.output.glob(run_id + "-*.json"))]
        if not results:
            raise AssertionError("Runner created no JSON results")
        return results

    def diagnostics(self, label):
        safe = re.sub(r"[^a-zA-Z0-9_.-]", "_", label)
        for suffix, args in (
            ("ps", ["--profile", "ha", "--profile", "tools", "ps", "-a"]),
            ("logs", ["--profile", "ha", "--profile", "tools", "logs", "--no-color", "--tail", "300"]),
        ):
            try:
                (self.output / (safe + "-" + suffix + ".log")).write_text(
                    self.compose(*args, check=False, capture=True, timeout=60))
            except Exception as error:
                (self.output / (safe + "-" + suffix + ".log")).write_text(str(error))
        try:
            (self.output / (safe + "-metrics.txt")).write_text(self.metrics())
            (self.output / (safe + "-sessions.txt")).write_text(
                self.sql("SELECT pid,application_name,state,wait_event,query FROM pg_stat_activity"))
        except Exception:
            pass

    def close(self):
        if self.bridge:
            self.bridge.close()
            self.bridge = None
        if not self.options.keep:
            try:
                self.compose("--profile", "ha", "--profile", "tools", "down",
                             "--volumes", "--remove-orphans", timeout=180)
            except Exception as error:
                print("Cleanup failed for " + self.project + ": " + str(error), file=sys.stderr)
        else:
            print("Preserved project: " + self.project)
        self.commands.close()


E = None


class Integration(unittest.TestCase):
    def tearDown(self):
        result = self._outcome.result
        if any(test is self for test, _ in result.errors + result.failures):
            E.diagnostics(self.id())

    def success(self, response):
        status, body = response
        self.assertEqual(status, 200, body)
        self.assertIsInstance(body, dict)
        self.assertTrue(body.get("ok", body.get("success", False)), body)
        self.assertNotEqual(body.get("success"), False, body)
        self.assertNotEqual(body.get("sql_completed"), False, body)
        return body

    def test_00_infrastructure(self):
        text = E.compose("ps", "--format", "json", capture=True)
        services = []
        for line in text.splitlines():
            value = json.loads(line)
            services.extend(value if isinstance(value, list) else [value])
        running = {item["Service"] for item in services if item["State"] == "running"}
        self.assertTrue({"postgres", "ojp", "app", "prometheus", "grafana",
                         "postgres-exporter"}.issubset(running), services)
        self.assertEqual(E.sql("SELECT 1"), "1")
        self.assertEqual(E.call("/health")[0], 200)
        self.assertTrue(E.call("/ready")[1]["ready"])

    def test_01_seed_cardinality_and_foreign_keys(self):
        E.reset()
        self.assertEqual(E.counts(), (200, 100, 1000, 3000, 500))
        self.assertEqual(E.sql("SELECT seed,users,products,orders,items,reviews FROM benchmark_seed"),
                         "12345|200|100|1000|3000|500")
        self.assertEqual(E.sql("SELECT count(*) FROM order_items i LEFT JOIN orders o ON o.id=i.order_id "
                               "LEFT JOIN products p ON p.id=i.product_id WHERE o.id IS NULL OR p.id IS NULL"), "0")
        self.assertEqual(E.sql("SELECT count(*) FROM pg_constraint WHERE connamespace='public'::regnamespace "
                               "AND contype='f' AND convalidated"), "5")
        indexes = set(E.sql("SELECT indexname FROM pg_indexes WHERE schemaname='public'").splitlines())
        self.assertTrue({"orders_user_date", "orders_date", "items_order", "items_product",
                         "reviews_product", "reviews_user", "products_price"}.issubset(indexes), indexes)
        self.assertEqual(E.sql("SELECT count(*) FROM pg_constraint WHERE connamespace='public'::regnamespace "
                               "AND contype='c' AND convalidated"), "4")
        E.run([SCRIPT / "seed-data.sh"])
        self.assertEqual(E.counts(), (200, 100, 1000, 3000, 500))

    def test_02_health_and_mode_real_queries(self):
        for mode in ("direct", "ojp"):
            E.mode(mode)
            self.assertEqual(E.call("/health")[0], 200)
            self.assertEqual(E.call("/ready")[0], 200)
            self.success(E.work("browse"))
        status, _ = E.call("/mode?value=invalid", {}, "POST")
        self.assertEqual(status, 400)
        E.run([SCRIPT / "verify-connections.sh"])

    def test_03_all_read_operations_both_modes(self):
        rows = {}
        for mode in ("direct", "ojp"):
            E.mode(mode)
            E.reset()
            rows[mode] = {}
            for op in ("browse", "history", "account", "report"):
                with self.subTest(mode=mode, operation=op):
                    rows[mode][op] = self.success(E.work(op))["rows"]
        self.assertEqual(rows["direct"], rows["ojp"], "Same baseline/seed must return consistent SQL row counts")

    def test_04_committed_checkout_review_counts(self):
        for mode in ("direct", "ojp"):
            E.mode(mode)
            E.reset()
            baseline = E.counts()
            self.success(E.work("checkout"))
            self.success(E.work("review"))
            changed = E.counts()
            self.assertEqual(changed[0:2], baseline[0:2])
            self.assertEqual(changed[2], baseline[2] + 1)
            self.assertGreater(changed[3], baseline[3])
            self.assertEqual(changed[4], baseline[4] + 1)
        E.reset()

    def test_05_transaction_rollback_both_modes(self):
        for mode in ("direct", "ojp"):
            E.mode(mode)
            before = E.counts()
            status, body = E.call("/rollback?seed=12345&phase=measure&scenario=integration&level=1",
                                  {}, "POST")
            self.assertEqual(status, 200, body)
            self.assertTrue(body.get("rolledBack", body.get("rollback")), body)
            self.assertEqual(E.counts(), before)

    def test_06_reset_restores_replay_baseline(self):
        E.mode("direct")
        E.run([SCRIPT / "reset.sh"])
        baseline = E.counts()
        for _ in range(2):
            self.success(E.work("checkout", seed=9876))
            self.success(E.work("review", seed=9876))
            self.assertNotEqual(E.counts(), baseline)
            E.reset()
            self.assertEqual(E.counts(), baseline)

    def test_07_direct_200_simultaneous_physical_sessions(self):
        E.mode("direct")
        expected_pool = int(E.env["DIRECT_POOL_SIZE"])
        application_name = E.call("/ready")[1]["direct_application_name"]
        physical = int(E.sql("SELECT count(*) FROM pg_stat_activity WHERE "
                             "datname=current_database() AND application_name='"
                             + application_name + "'"))
        self.assertEqual(physical, expected_pool)
        opened_before = sum(value for labels, value in metric_rows(
            E.metrics(), "demo_logical_connections_opened_total") if labels.get("mode") == "direct")
        barrier = threading.Barrier(E.sessions + 1)
        def hold():
            barrier.wait(timeout=30)
            return E.call("/hold?millis=10000", timeout=90)
        peak = 0
        with ThreadPoolExecutor(max_workers=E.sessions) as executor:
            futures = [executor.submit(hold) for _ in range(E.sessions)]
            barrier.wait(timeout=30)
            deadline = time.monotonic() + 9
            while time.monotonic() < deadline:
                active = int(sum(value for _, value in metric_rows(
                    E.metrics(), "demo_hold_active")))
                peak = max(peak, active)
                if peak >= E.sessions:
                    break
                time.sleep(.1)
            for future in futures:
                self.success(future.result())
        (E.output / "direct-sessions.json").write_text(json.dumps({
            "expected_http_concurrency": E.sessions, "observed_http_peak": peak,
            "physical_sessions": physical, "small_profile": E.options.small}))
        self.assertGreaterEqual(peak, E.sessions,
                                f"Expected {E.sessions} simultaneous HTTP handlers, observed {peak}")
        self.assertEqual(sum(value for labels, value in metric_rows(
            E.metrics(), "demo_logical_connections_opened_total") if labels.get("mode") == "direct"),
                         opened_before, "/hold must not manufacture database work")

    def test_08_logical_connection_lifecycle_balanced(self):
        for mode in ("direct", "ojp"):
            E.mode(mode)
            for _ in range(10):
                self.success(E.work("browse"))
        metrics = E.metrics()
        opened = metric_rows(metrics, "demo_logical_connections_opened_total")
        closed = metric_rows(metrics, "demo_logical_connections_closed_total")
        self.assertTrue(opened and closed)
        for mode in ("direct", "ojp"):
            opened_count = sum(value for labels, value in opened if labels.get("mode") == mode)
            closed_count = sum(value for labels, value in closed if labels.get("mode") == mode)
            self.assertGreater(opened_count, 0, mode)
            self.assertEqual(opened_count, closed_count, mode)

    def test_09_ojp_multiplexes_bounded_physical_pool(self):
        E.mode("ojp")
        self.assertEqual(E.sql("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'demo-direct-%'"), "0")
        for name in ("demo_hikari_total", "demo_hikari_active", "demo_hikari_idle"):
            self.assertEqual(metric_rows(E.metrics(), name)[0][1], 0)
        concurrency = 60 if not E.options.small else 20
        barrier = threading.Barrier(concurrency + 1)
        def hold():
            barrier.wait(timeout=30)
            return E.work("report", scenario="htap")
        peak = 0
        successes = rejected = 0
        def completed():
            return sum(value for labels, value in metric_rows(E.metrics(), "demo_completions_total")
                       if labels.get("mode") == "ojp" and labels.get("op") == "report"
                       and labels.get("scenario") == "htap")
        before_completed = completed()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(hold) for _ in range(concurrency)]
            barrier.wait(timeout=30)
            while not all(future.done() for future in futures):
                peak = max(peak, int(E.sql("SELECT count(*) FROM pg_stat_activity WHERE "
                                          "datname=current_database() AND application_name='demo-ojp'")))
                time.sleep(.1)
            for future in futures:
                status, body = future.result()
                if status == 200:
                    self.success((status, body))
                    successes += 1
                else:
                    self.assertEqual(status, 503, body)
                    self.assertEqual(body.get("code"), "08001", body)
                    rejected += 1
        self.assertGreaterEqual(peak, 10)
        self.assertGreater(successes, 0)
        self.assertEqual(successes + rejected, concurrency)
        self.assertEqual(completed() - before_completed, successes)
        self.assertLessEqual(peak, 40)
        if not E.options.small:
            self.assertLess(peak, concurrency)
        (E.output / "ojp-sessions.json").write_text(json.dumps({
            "logical_concurrency": concurrency, "physical_peak": peak, "physical_limit": 40,
            "sql_successes": successes, "pool_acquisition_rejections": rejected}))

    def test_10_phase_histograms_and_level_sql_completions(self):
        E.mode("direct")
        before = E.metrics()
        for phase in ("warmup", "measure"):
            for level in (17, 23):
                for _ in range(3):
                    self.success(E.work("browse", phase=phase, level=level))
        after = E.metrics()
        for phase in ("warmup", "measure"):
            for level in ("17", "23"):
                for name in ("demo_completions_total", "demo_sql_duration_seconds_count",
                             "demo_request_duration_seconds_count", "demo_acquisition_duration_seconds_count"):
                    def matching(text):
                        return sum(value for labels, value in metric_rows(text, name)
                                   if all(labels.get(k) == v for k, v in {
                                       "mode": "direct", "op": "browse", "scenario": "integration",
                                       "phase": phase, "level": level}.items()))
                    self.assertEqual(matching(after) - matching(before), 3, (phase, level, name))
        buckets = metric_rows(after, "demo_request_duration_seconds_bucket")
        self.assertTrue(any(labels.get("le") == "+Inf" and value > 0 for labels, value in buckets))

    def test_11_ordered_sequential_comparison_and_metadata(self):
        results = E.runner("it-compare", "--mode", "both", "--rps", "3,7",
                           "--warmup", "1", "--duration", "2", "--seed", "4242",
                           "--max-in-flight", "17", "--request-timeout", "45")
        self.assertEqual([(item["mode"], item["rps"]) for item in results],
                         [("direct", 3), ("direct", 7), ("ojp", 3), ("ojp", 7)])
        for item in results:
            self.assertNotIn("error", item, item)
            self.assertEqual(item["seed"], 4242)
            self.assertEqual(item["max_in_flight"], 17)
            self.assertEqual(item["request_timeout"], 45)
            self.assertEqual(item["scenario"], "integration")
            self.assertEqual(item["run_id"], "it-compare")
            self.assertEqual(item["readiness_before_measure"]["/health"]["status"], 200)
            self.assertEqual(item["readiness_before_measure"]["/ready"]["status"], 200)
            self.assertTrue(item["readiness_before_measure"]["work_probe"]["sql_completed"])
            self.assertEqual(item["readiness_after"]["status"], 200)
            self.assertEqual(item["metrics_before_measure"]["status"], 200)
            self.assertEqual(item["metrics_after"]["status"], 200)
            self.assertTrue(item.get("benchmark_result_published"), item)
            for phase, duration in (("warmup", 1), ("measure", 2)):
                report = item[phase]
                self.assertLessEqual(report["start_epoch_seconds"], report["end_arrivals_epoch_seconds"])
                self.assertLessEqual(report["end_arrivals_epoch_seconds"], report["end_epoch_seconds"])
                self.assertAlmostEqual(report["end_arrivals_epoch_seconds"] - report["start_epoch_seconds"],
                                       duration, places=4)
                self.assertTrue(report["start_time"].endswith("Z"))
                self.assertTrue(report["end_time"].endswith("Z"))
                counts = item[phase]["counts"]
                self.assertEqual(counts["scheduled"], math.ceil(item["rps"] * duration))
                self.assertEqual(counts["sql_completed"], counts["scheduled"])
                self.assertEqual(counts["successful_transactions"], counts["sql_completed"])
                self.assertEqual(counts["failed_transactions"], 0)
                self.assertEqual(counts["in_flight"], 0)
                self.assertIsNotNone(item[phase]["latency_ms"]["end_to_end"]["p99"])
                telemetry = item[phase]["telemetry"]
                self.assertTrue(telemetry["final_sent"], telemetry)
                self.assertGreater(telemetry["samples_sent"], 0)
                self.assertFalse(telemetry["errors"], telemetry)
                histogram = item[phase]["scheduled_latency_histogram"]
                self.assertEqual(histogram["count"], counts["completed"])
                self.assertEqual(histogram["count"], counts["dispatched"])
                self.assertEqual(histogram["buckets"]["+Inf"], histogram["count"])
                ordered = [value for _, value in sorted(
                    histogram["buckets"].items(), key=lambda pair: float(pair[0]))]
                self.assertEqual(ordered, sorted(ordered))
                self.assertGreaterEqual(histogram["sum_seconds"], 0)
            def sql_completions(snapshot):
                return sum(value for labels, value in metric_rows(
                    snapshot["response"], "demo_completions_total")
                    if all(labels.get(key) == value for key, value in {
                        "mode": item["mode"], "phase": "measure",
                        "scenario": item["scenario"], "level": item["level"]}.items()))
            self.assertEqual(sql_completions(item["metrics_after"])
                             - sql_completions(item["metrics_before_measure"]),
                             item["measure"]["counts"]["sql_completed"],
                             "Per-level application SQL completions must match runner completions")
            csv_file = E.output / f"it-compare-{results.index(item)+1:03d}-{item['mode']}-{item['level']}.csv"
            self.assertTrue(csv_file.exists(), csv_file)
            with csv_file.open(newline="") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 14)
            self.assertEqual({row["phase"] for row in rows}, {"warmup", "measure"})
            for row in rows:
                self.assertEqual(row["mode"], item["mode"])
                self.assertEqual(row["run_id"], item["run_id"])
                self.assertEqual(row["start_time"], item[row["phase"]]["start_time"])
                if row["operation"] == "all":
                    self.assertEqual(int(row["sql_completed"]),
                                     item[row["phase"]]["counts"]["sql_completed"])
        for direct, ojp in zip(results[:2], results[2:]):
            self.assertEqual(
                {op: value["scheduled"] for op, value in direct["measure"]["operations"].items()},
                {op: value["scheduled"] for op, value in ojp["measure"]["operations"].items()})
        summary_metrics = E.metrics()
        for item in results:
            labels = {"mode": item["mode"], "phase": "measure",
                      "scenario": item["scenario"], "level": item["level"]}
            self.assertTrue(any(all(row_labels.get(key) == value for key, value in labels.items())
                                for row_labels, _ in metric_rows(
                                    summary_metrics, "demo_benchmark_result_successful_rps")))

    def test_12_throttling_admission_not_sql_completion(self):
        results = E.runner("it-throttle", "--scenario", "integration", "--mode", "direct",
                           "--rps", "1000", "--warmup", "0", "--duration", "2",
                           "--max-in-flight", "1", "--request-timeout", "30")
        self.assertEqual(len(results), 1)
        self.assertNotIn("error", results[0], results[0])
        counts = results[0]["measure"]["counts"]
        self.assertGreater(counts["dropped"], 0)
        self.assertGreater(counts["sql_completed"], 0)
        self.assertEqual(counts["scheduled"], counts["dispatched"] + counts["dropped"])
        self.assertLessEqual(counts["peak_in_flight"], 1)
        self.assertEqual(counts["accepted"], counts["sql_completed"])
        self.assertLess(counts["sql_completed"], counts["scheduled"])
        E.mode("ojp")
        switched = E.call("/ready")[1]
        self.assertEqual(switched["direct_sessions"], 0, switched)
        self.assertEqual(switched["mode"], "ojp", switched)

    def test_13_analytical_queries_execute_real_aggregations(self):
        def server_counts():
            status, text = E.call("/metrics", base=E.service_url("ojp", 9159))
            self.assertEqual(status, 200)
            executions = metric_rows(text, "ojp_sql_executions_total")
            slow = metric_rows(text, "ojp_sql_slow_executions_total")
            self.assertTrue(executions, "Missing actual OJP SQL execution counter")
            return sum(v for _, v in executions), sum(v for _, v in slow) if slow else None
        E.mode("ojp")
        before_server = server_counts()
        result_rows = {}
        for mode in ("direct", "ojp"):
            E.mode(mode)
            E.reset()
            before = sum(value for labels, value in metric_rows(E.metrics(), "demo_completions_total")
                         if labels.get("mode") == mode and labels.get("op") == "report"
                         and labels.get("scenario") == "htap")
            result = self.success(E.work("report", scenario="htap"))
            result_rows[mode] = result["rows"]
            self.assertGreater(result["rows"], 0, "Sales aggregation must return real product groups")
            after = sum(value for labels, value in metric_rows(E.metrics(), "demo_completions_total")
                        if labels.get("mode") == mode and labels.get("op") == "report"
                        and labels.get("scenario") == "htap")
            self.assertEqual(after - before, 1)
            E.reset()
        self.assertEqual(result_rows["direct"], result_rows["ojp"])
        after_server = server_counts()
        self.assertGreater(after_server[0], before_server[0])
        if after_server[1] is not None:
            self.assertGreaterEqual(after_server[1], before_server[1] or 0)
            self.assertLessEqual(after_server[1], after_server[0])
        else:
            self.assertIsNone(before_server[1])
        (E.output / "ojp-sql-classification.json").write_text(json.dumps({
            "configured_threshold_ms": 1, "before": before_server, "after": after_server,
            "note": "Absent slow series means no observed classifications; no latency/sleep guarantee."
        }, indent=2) + "\n")

    def test_14_monitoring_targets_and_exported_data(self):
        resource_path = E.output / "it-compare" / "resources.jsonl"
        if resource_path.exists():
            records = [json.loads(line) for line in resource_path.read_text().splitlines()]
            postgres = [row for row in records if row.get("stats", {}).get("Name", "").endswith("-postgres-1")]
            self.assertTrue(postgres, "No real PostgreSQL Docker resource statistics")
            self.assertTrue(any(
                interval["phase"] == "measure" and interval["arrival_window_overlap"]
                for row in postgres for interval in row.get("workload_intervals", [])),
                "No PostgreSQL CPU sample overlaps an actual measurement arrival window")
            for row in postgres:
                self.assertEqual(row["source"], "docker_stats")
                self.assertIn("timestamp", row)
                self.assertGreaterEqual(float(row["stats"]["CPUPerc"].removesuffix("%")), 0)
                self.assertIn("MemUsage", row["stats"])
        elif not E.options.only:
            self.fail("Comparison did not persist PostgreSQL CPU evidence")
        prometheus = E.service_url("prometheus", 9090)
        grafana = E.service_url("grafana", 3000)
        self.assertEqual(E.call("/api/health", base=grafana)[0], 200)
        credentials = (E.env.get("GRAFANA_USER", "admin") + ":"
                       + E.env.get("GRAFANA_PASSWORD", "demo-local-only"))
        authorization = "Basic " + base64.b64encode(credentials.encode()).decode()
        status, dashboard = E.call("/api/dashboards/uid/ojp-benchmark", base=grafana,
                                   headers={"Authorization": authorization})
        self.assertEqual(status, 200, dashboard)
        self.assertEqual(dashboard["dashboard"]["uid"], "ojp-benchmark")
        self.assertTrue(dashboard["dashboard"]["panels"])
        deadline = time.monotonic() + 90
        targets = []
        required_jobs = {"app", "postgres", "ojp", "prometheus"}
        while time.monotonic() < deadline:
            status, body = E.call("/api/v1/targets", base=prometheus)
            self.assertEqual(status, 200, body)
            targets = body["data"]["activeTargets"]
            jobs = {target["labels"].get("job"): target["health"] for target in targets
                    if target["labels"].get("job") in required_jobs}
            if required_jobs.issubset(jobs) and all(value == "up" for value in jobs.values()):
                break
            time.sleep(2)
        self.assertTrue(required_jobs.issubset(jobs), targets)
        self.assertTrue(all(value == "up" for value in jobs.values()), targets)
        (E.output / "prometheus-targets.json").write_text(json.dumps(targets, indent=2) + "\n")
        status, body = E.call("/api/v1/query?" + urllib.parse.urlencode({
            "query": "sum(demo_completions_total)"}), base=prometheus)
        self.assertEqual(status, 200, body)
        self.assertTrue(body["data"]["result"], body)
        self.assertGreater(float(body["data"]["result"][0]["value"][1]), 0)
        dashboard = json.loads((ROOT / "monitoring/grafana/dashboards/benchmark.json").read_text())
        for panel in dashboard["panels"]:
            for target in panel.get("targets", []):
                expression = target["expr"].replace("$mode", "direct")
                expression = expression.replace("$phase", "measure")
                expression = expression.replace("$scenario", "htap").replace("$level", "16")
                status, response = E.call("/api/v1/query?" + urllib.parse.urlencode({
                    "query": expression}), base=prometheus)
                self.assertEqual(status, 200, (expression, response))
                self.assertEqual(response.get("status"), "success", (expression, response))

    def test_15_ha_failover_bounded_sql_recovery(self):
        E.run([SCRIPT / "failover.sh", "--run-id", "it-failover", "--rps", "10",
               "--warmup", "1", "--duration", "8", "--recovery-timeout", "60"], timeout=600)
        E.refresh_base()
        report = json.loads((E.output / "it-failover-failover-events.json").read_text())
        self.assertTrue(report["success"], report)
        self.assertEqual({node["service"] for node in report["ha_nodes_before_fault"]},
                         {"ojp", "ojp2", "ojp3"})
        self.assertTrue(all(node["state"] == "running" for node in report["ha_nodes_before_fault"]))
        self.assertIn("fault_requested_at", report)
        self.assertIn("node_stopped_at", report)
        self.assertIn("recovered_at", report)
        self.assertGreater(len(report["observations"]), 2)
        self.assertLessEqual(report["recovery_seconds"], 60)
        self.assertTrue(all(item["sql_success"] for item in report["observations"][-3:]))
        self.assertFalse(report.get("cleanup_errors"), report)
        state = E.compose("ps", "--format", "json", "ojp", capture=True)
        self.assertIn("running", state.lower())

    def assert_mode_isolation(self, item):
        readiness = item["readiness_before_measure"]["/ready"]["response"]
        self.assertEqual(readiness["mode"], item["mode"])
        expected = 0 if item["mode"] == "ojp" else int(E.env["DIRECT_POOL_SIZE"])
        self.assertEqual(readiness["direct_sessions"], expected)
        for name in ("demo_hikari_total", "demo_hikari_active", "demo_hikari_idle"):
            values = metric_rows(item["metrics_before_measure"]["response"], name)
            self.assertTrue(values, name)
            if item["mode"] == "ojp":
                self.assertEqual(values[0][1], 0, name)

    def check_ramp(self, mode):
        duration = E.options.ramp_duration
        results = E.runner("it-ramp-" + mode, "--mode", mode, "--rps", "2,4,8",
                           "--warmup", "1", "--duration", str(duration))
        self.assertEqual([item["rps"] for item in results], [2, 4, 8])
        for item in results:
            self.assertNotIn("error", item, item)
            self.assertEqual(item["mode"], mode)
            counts = item["measure"]["counts"]
            self.assertEqual(counts["scheduled"], math.ceil(item["rps"] * duration))
            self.assertEqual(counts["sql_completed"], counts["scheduled"])
            self.assertEqual(counts["successful_transactions"], counts["scheduled"])
            self.assertEqual(counts["failed_transactions"], 0)
            self.assertEqual(counts["in_flight"], 0)
            self.assert_mode_isolation(item)

    def test_16_direct_ordered_ramp(self):
        self.check_ramp("direct")

    def test_17_ojp_ordered_ramp_and_mode_isolation(self):
        self.check_ramp("ojp")

    def test_18_completed_workload_proportions(self):
        results = E.runner("it-proportions", "--mode", "both", "--rps", "20",
                           "--warmup", "1", "--duration", "5", "--oltp-percent", "90",
                           "--olap-percent", "10", "--seed", "12345")
        self.assertEqual([item["mode"] for item in results], ["direct", "ojp"])
        for item in results:
            self.assertNotIn("error", item, item)
            counts = item["measure"]["counts"]
            self.assertEqual(counts["scheduled"], 100)
            self.assertEqual(counts["sql_completed"], 100)
            self.assertEqual(counts["successful_transactions"], 100)
            operations = item["measure"]["operations"]
            self.assertEqual(operations["report"]["sql_completed"], 10)
            self.assertEqual(sum(value["sql_completed"] for name, value in operations.items()
                                 if name != "report"), 90)
            self.assert_mode_isolation(item)
            def phase_total(snapshot, phase):
                return sum(value for labels, value in metric_rows(snapshot["response"], "demo_completions_total")
                           if labels.get("mode") == item["mode"] and labels.get("phase") == phase
                           and labels.get("level") == item["level"]
                           and labels.get("scenario") == item["scenario"])
            before, after = item["metrics_before_measure"], item["metrics_after"]
            self.assertEqual(phase_total(after, "measure") - phase_total(before, "measure"), 100)
            self.assertEqual(phase_total(after, "warmup"), phase_total(before, "warmup"))

    def test_19_warmup_only_excludes_measurements(self):
        def measured():
            return sum(value for labels, value in metric_rows(E.metrics(), "demo_completions_total")
                       if labels.get("phase") == "measure")
        before = measured()
        results = E.runner("it-warmup-only", "--mode", "direct", "--rps", "2",
                           "--warmup", "1", "--warmup-only")
        self.assertEqual(len(results), 1)
        item = results[0]
        self.assertNotIn("error", item, item)
        self.assertNotIn("measure", item)
        self.assertEqual(item["warmup"]["counts"]["scheduled"], 2)
        self.assertEqual(item["warmup"]["counts"]["sql_completed"], 2)
        self.assertEqual(measured(), before)

    def test_20_independent_open_loop_arrivals(self):
        name = E.project + "-controlled-http"
        try:
            E.compose("--profile", "tools", "run", "-d", "--rm", "--no-deps",
                      "--name", name, "--env", "TARGET_APP_URL=" + E.env["APP_INTERNAL_URL"],
                      "--entrypoint", "python", "load", "-u", "-c", DELAY_PROXY)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                try:
                    if E.call("/health", base="http://" + name + ":8080", timeout=3)[0] == 200:
                        break
                except OSError:
                    pass
                time.sleep(.2)
            else:
                self.fail("Controlled HTTP-delay proxy did not become ready")
            results = E.runner("it-open-loop", "--url", "http://" + name + ":8080",
                               "--mode", "direct", "--rps", "20", "--warmup", "0",
                               "--duration", "2", "--max-in-flight", "1")
            self.assertEqual(len(results), 1)
            self.assertNotIn("error", results[0], results[0])
            report = results[0]["measure"]
            counts = report["counts"]
            self.assertEqual(counts["scheduled"], 40, "Arrivals must not wait for preceding responses")
            self.assertGreater(counts["dropped"], 20)
            self.assertGreater(counts["sql_completed"], 0)
            self.assertEqual(counts["sql_completed"], counts["dispatched"])
            self.assertEqual(counts["scheduled"], counts["dispatched"] + counts["dropped"])
            self.assertEqual(counts["peak_in_flight"], 1)
            self.assertGreaterEqual(report["latency_ms"]["end_to_end"]["p50"], 250)
            self.assertLess(report["elapsed_seconds"], 4)
        finally:
            E.run(["docker", "rm", "-f", name], check=False)

    def test_21_invalid_cli_parameters(self):
        invalid = (
            ["--rps", "0"], ["--rps", "-1"], ["--rps", "2,2.5"], ["--rps", "10001"],
            ["--rps", ",".join(map(str, range(1, 66)))], ["--warmup", "-1"],
            ["--duration", "0"], ["--max-in-flight", "0"], ["--request-timeout", "nan"],
            ["--duration", "inf"], ["--oltp-percent", "90", "--olap-percent", "20"],
            ["--mode", "invalid"], ["--scenario", "invalid"],
        )
        for arguments in invalid:
            with self.subTest(arguments=arguments):
                command = [sys.executable, str(ROOT / "load" / "runner.py"), *arguments]
                result = subprocess.run(command, cwd=ROOT, env=E.env, capture_output=True,
                                        text=True, timeout=15)
                E.commands.write("$ " + " ".join(command) + "\n" + result.stderr)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)

    def test_22_htap_entrypoint_verifies_segregation_and_keeps_both_results(self):
        E.run([SCRIPT / "htap.sh", "--mode", "both", "--rps", "2",
               "--warmup", "0", "--duration", "4", "--max-in-flight", "32",
               "--request-timeout", "15", "--run-id", "it-htap"], timeout=300)
        results = [json.loads(path.read_text())
                   for path in sorted(E.output.glob("it-htap-*.json"))]
        self.assertEqual([(item["mode"], item["rps"]) for item in results],
                         [("direct", 2), ("ojp", 2)])
        for item in results:
            self.assertNotIn("error", item, item)
            self.assertEqual(item["scenario"], "htap")
            self.assertEqual(item["measure"]["counts"]["sql_completed"], 8)
            self.assertTrue(item.get("benchmark_result_published"), item)
        classification = results[1]["slow_query_classification"]
        self.assertIn(classification["observed"], (True, False, None))
        if classification["observed"] is False:
            self.assertIn("not demonstrated", classification["message"])
        if classification["observed"] is None:
            self.assertIn("could not be verified", classification["message"])


def main():
    global E
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--small", action="store_true",
                        default=os.environ.get("IT_SMALL", "").lower() == "true")
    parser.add_argument("--only", action="append",
                        help="Run matching test names; repeat to select related tests; default runs the full suite")
    parser.add_argument("--ramp-duration", type=float, default=float(os.environ.get("IT_RAMP_DURATION", "10")),
                        help="Seconds per 2,4,8 RPS ramp level (default 10, or IT_RAMP_DURATION)")
    options = parser.parse_args()
    if not math.isfinite(options.ramp_duration) or options.ramp_duration <= 0:
        parser.error("--ramp-duration must be positive and finite")
    E = Environment(options)
    print(f"Project: {E.project}; evidence: {E.output}", flush=True)
    if options.small:
        print("EXPLICIT SMALL PROFILE: 20 direct sessions, not the default 200-session validation.", flush=True)
    succeeded = False
    summary = {"project": E.project, "small_profile": options.small, "passed": False}
    try:
        # Help must not require Docker or depend on the caller's directory.
        for script in sorted(SCRIPT.glob("*.sh")):
            if script.name != "common.sh":
                E.run([script, "--help"], timeout=20)
        E.run([SCRIPT / "verify-environment.sh"], timeout=60)
        E.compose("--profile", "tools", "build", "load", timeout=600)
        E.run([SCRIPT / "start.sh"], timeout=1200)
        E.bridge = HTTPBridge(E.env)
        E.refresh_base()
        names = unittest.defaultTestLoader.getTestCaseNames(Integration)
        selected = [name for name in names if not options.only
                    or any(fragment in name for fragment in options.only)]
        if not selected:
            raise RuntimeError("--only did not match any integration scenario")
        suite = unittest.TestSuite(Integration(name) for name in selected)
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        summary.update(tests_run=result.testsRun, failures=len(result.failures),
                       errors=len(result.errors), scenarios=selected)
        succeeded = result.wasSuccessful()
        summary["passed"] = succeeded
    except Exception as error:
        print(str(error), file=sys.stderr)
        summary["startup_error"] = str(error)
        E.diagnostics("startup-failure")
    finally:
        if not succeeded:
            E.diagnostics("final-failure")
        (E.output / "integration-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        E.close()
    return 0 if succeeded else 1


if __name__ == "__main__":
    raise SystemExit(main())
