#!/usr/bin/env python3
"""Real-container integration tests. Uses only the Python standard library."""
import argparse
from concurrent.futures import ThreadPoolExecutor
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

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts"
COMPOSE = ["docker", "compose", "--project-directory", str(ROOT), "-f", str(ROOT / "compose.yaml")]
TABLES = ("users", "products", "orders", "order_items", "reviews")


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
            "SEED_USERS": "200", "SEED_PRODUCTS": "100", "SEED_ORDERS": "1000",
            "SEED_ITEMS": "3000", "SEED_REVIEWS": "500", "SEED": "12345",
            "RESULTS_DIR": str(self.output),
        })
        for name in ("APP_PORT", "POSTGRES_PORT", "PROMETHEUS_PORT", "GRAFANA_PORT"):
            self.env[name] = "0"
        self.env.pop("APP_URL", None)
        self.base = None
        # Never inherit a user's production connection override into the isolated stack.
        self.env.pop("OJP_URL", None)
        self.env.pop("OJP_HA_URL", None)
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
        return self.run(COMPOSE + list(args), **kwargs)

    def sql(self, query):
        return self.compose("exec", "-T", "postgres", "sh", "-ec",
                            'exec psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1"',
                            "psql", query, capture=True).strip()

    def service_url(self, service, port):
        return "http://" + self.compose("port", service, str(port), capture=True).strip()

    def refresh_base(self):
        self.base = self.service_url("app", 8080)

    def counts(self):
        query = "SELECT " + ", ".join(f"(SELECT count(*) FROM {table})" for table in TABLES)
        return tuple(map(int, self.sql(query).split("|")))

    def call(self, path, data=None, method=None, timeout=60, base=None):
        raw = None if data is None else json.dumps(data).encode()
        request = urllib.request.Request((base or self.base) + path, data=raw, method=method,
                                         headers={"Content-Type": "application/json"})
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
        self.compose("--profile", "tools", "run", "--rm", "--user",
                     f"{os.getuid()}:{os.getgid()}", "load", "--url", "http://app:8080",
                     "--scenario", "integration", "--run-id", run_id,
                     "--output", "/results", *options)
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

    def test_01_seed_cardinality_and_foreign_keys(self):
        E.reset()
        self.assertEqual(E.counts(), (200, 100, 1000, 3000, 500))
        self.assertEqual(E.sql("SELECT seed,users,products,orders,items,reviews FROM benchmark_seed"),
                         "12345|200|100|1000|3000|500")
        self.assertEqual(E.sql("SELECT count(*) FROM order_items i LEFT JOIN orders o ON o.id=i.order_id "
                               "LEFT JOIN products p ON p.id=i.product_id WHERE o.id IS NULL OR p.id IS NULL"), "0")
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

    def test_03_all_read_operations_both_modes(self):
        for mode in ("direct", "ojp"):
            E.mode(mode)
            for op in ("browse", "history", "account", "report"):
                with self.subTest(mode=mode, operation=op):
                    self.success(E.work(op))

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
        E.reset()
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
        physical = int(E.sql("SELECT count(*) FROM pg_stat_activity WHERE "
                             "datname=current_database() AND application_name='demo-direct'"))
        self.assertEqual(physical, expected_pool)
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

    def test_08_logical_connection_lifecycle_balanced(self):
        for mode in ("direct", "ojp"):
            E.mode(mode)
            for _ in range(10):
                self.success(E.work("browse"))
        metrics = E.metrics()
        opened = metric_rows(metrics, "demo_logical_connections_opened_total")
        closed = metric_rows(metrics, "demo_logical_connections_closed_total")
        self.assertTrue(opened and closed)
        self.assertGreater(opened[0][1], 0)
        self.assertEqual(opened[0][1], closed[0][1])

    def test_09_ojp_multiplexes_bounded_physical_pool(self):
        E.mode("ojp")
        concurrency = 60 if not E.options.small else 20
        barrier = threading.Barrier(concurrency + 1)
        def hold():
            barrier.wait(timeout=30)
            return E.work("report", scenario="slow-queries")
        peak = 0
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(hold) for _ in range(concurrency)]
            barrier.wait(timeout=30)
            while not all(future.done() for future in futures):
                peak = max(peak, int(E.sql("SELECT count(*) FROM pg_stat_activity WHERE "
                                          "datname=current_database() AND wait_event='PgSleep'")))
                time.sleep(.1)
            for future in futures:
                self.success(future.result())
        self.assertGreater(peak, 0)
        self.assertLessEqual(peak, 40)
        if not E.options.small:
            self.assertLess(peak, concurrency)
        (E.output / "ojp-sessions.json").write_text(json.dumps({
            "logical_concurrency": concurrency, "physical_peak": peak, "physical_limit": 40}))

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
                           "--warmup", "1", "--duration", "2", "--seed", "12345")
        self.assertEqual([(item["mode"], item["rps"]) for item in results],
                         [("direct", 3), ("direct", 7), ("ojp", 3), ("ojp", 7)])
        for item in results:
            self.assertNotIn("error", item, item)
            self.assertEqual(item["seed"], 12345)
            self.assertEqual(item["scenario"], "integration")
            self.assertEqual(item["run_id"], "it-compare")
            for phase, duration in (("warmup", 1), ("measure", 2)):
                counts = item[phase]["counts"]
                self.assertEqual(counts["scheduled"], math.ceil(item["rps"] * duration))
                self.assertEqual(counts["sql_completed"], counts["scheduled"])
                self.assertEqual(counts["successful_transactions"], counts["sql_completed"])
                self.assertEqual(counts["failed_transactions"], 0)
                self.assertEqual(counts["in_flight"], 0)
                self.assertIsNotNone(item[phase]["latency_ms"]["end_to_end"]["p99"])
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
        for direct, ojp in zip(results[:2], results[2:]):
            self.assertEqual(
                {op: value["scheduled"] for op, value in direct["measure"]["operations"].items()},
                {op: value["scheduled"] for op, value in ojp["measure"]["operations"].items()})

    def test_12_throttling_admission_not_sql_completion(self):
        results = E.runner("it-throttle", "--scenario", "slow-queries", "--mode", "direct",
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

    def test_13_slow_queries_execute_real_sql_delay(self):
        E.mode("ojp")
        start = time.monotonic()
        self.success(E.work("report", scenario="slow-queries"))
        elapsed = time.monotonic() - start
        self.assertGreaterEqual(elapsed, .1, "Slow scenario must contain an actual SQL delay")
        self.assertLess(elapsed, 60)
        E.reset()

    def test_14_monitoring_targets_and_exported_data(self):
        prometheus = E.service_url("prometheus", 9090)
        grafana = E.service_url("grafana", 3000)
        self.assertEqual(E.call("/api/health", base=grafana)[0], 200)
        deadline = time.monotonic() + 90
        targets = []
        while time.monotonic() < deadline:
            status, body = E.call("/api/v1/targets", base=prometheus)
            self.assertEqual(status, 200, body)
            targets = body["data"]["activeTargets"]
            jobs = {target["labels"].get("job"): target["health"] for target in targets}
            if len(targets) >= 3 and all(value == "up" for value in jobs.values()):
                break
            time.sleep(2)
        self.assertGreaterEqual(len(targets), 3, targets)
        self.assertTrue(all(target["health"] == "up" for target in targets), targets)
        status, body = E.call("/api/v1/query?" + urllib.parse.urlencode({
            "query": "sum(demo_completions_total)"}), base=prometheus)
        self.assertEqual(status, 200, body)
        self.assertTrue(body["data"]["result"], body)
        self.assertGreater(float(body["data"]["result"][0]["value"][1]), 0)

    def test_15_ha_failover_bounded_sql_recovery(self):
        E.run([SCRIPT / "failover.sh", "--run-id", "it-failover", "--rps", "10",
               "--warmup", "1", "--duration", "8", "--recovery-timeout", "60"], timeout=600)
        E.refresh_base()
        report = json.loads((E.output / "it-failover-failover-events.json").read_text())
        self.assertTrue(report["success"], report)
        self.assertIn("fault_requested_at", report)
        self.assertIn("node_stopped_at", report)
        self.assertIn("recovered_at", report)
        self.assertGreater(len(report["observations"]), 2)
        self.assertLessEqual(report["recovery_seconds"], 60)
        self.assertTrue(all(item["sql_success"] for item in report["observations"][-3:]))
        self.assertFalse(report.get("cleanup_errors"), report)
        state = E.compose("ps", "--format", "json", "ojp", capture=True)
        self.assertIn("running", state.lower())


def main():
    global E
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--small", action="store_true",
                        default=os.environ.get("IT_SMALL", "").lower() == "true")
    parser.add_argument("--only", help="Run matching test names; default runs all 15")
    options = parser.parse_args()
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
        E.run([SCRIPT / "start.sh"], timeout=1200)
        E.refresh_base()
        E.compose("--profile", "tools", "build", "load", timeout=600)
        names = unittest.defaultTestLoader.getTestCaseNames(Integration)
        selected = [name for name in names if not options.only or options.only in name]
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
