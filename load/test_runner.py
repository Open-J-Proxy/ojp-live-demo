"""Black-box producer regressions: python -m unittest discover -s load -v (JDK 25)."""

from collections import Counter
from contextlib import contextmanager, redirect_stdout
import csv
import http.server
import io
import importlib.util
import json
import os
from pathlib import Path
import random
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock, patch
import urllib.error
import urllib.request
from urllib.parse import parse_qs, urlsplit


ROOT = Path(__file__).resolve().parent.parent
OPERATIONS = ("browse", "history", "checkout", "review", "account", "report")


class Application:
    """Real HTTP control/work/metrics fixture, retaining the producer's wire traffic."""

    def __init__(self, work=None, control=None, arrivals_delay=0, slow_metrics=True):
        self.work = work
        self.control = control
        self.arrivals_delay = arrivals_delay
        self.slow_metrics = slow_metrics
        self.events = []
        self.lock = threading.Lock()
        self.mode = "direct"
        self.slow = 4
        self.active = 0
        self.peak = 0

    @contextmanager
    def serve(self):
        app = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                self.payload = b""
                self.respond()

            def do_POST(self):
                self.payload = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                self.respond()

            def respond(self):
                parsed = urlsplit(self.path)
                query = {key: values[0] for key, values in parse_qs(
                    parsed.query, keep_blank_values=True).items()}
                event = {"method": self.command, "path": parsed.path, "query": query,
                         "authorization": self.headers.get("Authorization"),
                         "content_type": self.headers.get("Content-Type"),
                         "body": getattr(self, "payload", b"").decode(),
                         "received_at": time.monotonic()}
                with app.lock:
                    if parsed.path == "/mode":
                        app.mode = query["value"]
                    event["mode"] = app.mode
                    app.events.append(event)
                status, body, delay = 200, {"success": True}, 0
                if parsed.path in ("/health", "/ready"):
                    body = {"ready": True, "status": "up", "active_connections": 2,
                            "direct_sessions": 3, "ojp_sessions": 5,
                            "postgres_snapshot_timestamp_seconds": time.time()}
                elif parsed.path == "/work":
                    with app.lock:
                        app.active += 1
                        app.peak = max(app.peak, app.active)
                        if query["op"] == "report":
                            app.slow += 1
                    body = {"success": True, "operation": query["op"]}
                    if app.work:
                        status, body, delay = app.work(event)
                elif parsed.path == "/metrics":
                    body = ("demo_postgres_cpu_percent 12.5\n"
                            f"demo_postgres_resource_timestamp_seconds {time.time()}\n")
                elif parsed.path == "/ojp/metrics":
                    body = (f'ojp_sql_slow_executions_total{{sql_statement="select report"}} {app.slow}\n'
                            'ojp_sql_slow_executions_total{sql_statement="select browse"} 2\n'
                            if app.slow_metrics else "# no slow-query counters\n")
                elif parsed.path == "/arrivals":
                    delay = app.arrivals_delay
                if app.control and parsed.path != "/work":
                    override = app.control(event)
                    if override is not None:
                        status, body, delay = override
                try:
                    time.sleep(delay)
                    payload = (body if isinstance(body, str) else json.dumps(body)).encode()
                    self.send_response(status)
                    self.send_header("Content-Length", str(len(payload)))
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                finally:
                    if parsed.path == "/work":
                        with app.lock:
                            app.active -= 1

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.url = f"http://127.0.0.1:{server.server_port}"
        try:
            yield self
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def requests(self, path, phase=None):
        return [event for event in self.events if event["path"] == path
                and (phase is None or event["query"].get("phase") == phase)]


class RunnerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workspace = tempfile.TemporaryDirectory(prefix="ojp-runner-tests-")
        cls.addClassCleanup(cls.workspace.cleanup)
        cls.directory = Path(cls.workspace.name)
        java_home = os.environ.get("JAVA_HOME")
        cls.java = str(Path(java_home) / "bin/java") if java_home else shutil.which("java")
        if not cls.java:
            raise RuntimeError("JDK 25 java must be available via JAVA_HOME or PATH")
        javac = str(Path(cls.java).resolve().with_name("javac"))
        cls.source = ROOT / "load" / "Runner.java"
        sources = [ROOT / "load" / name for name in
                   ("Runner.java", "LoadJson.java", "HttpBridge.java", "DelayProxy.java")]
        compiled = subprocess.run([javac, "--release", "25", "-d", str(cls.directory),
                                   *map(str, sources)],
                                  capture_output=True, text=True, timeout=60)
        if compiled.returncode:
            raise RuntimeError("JDK 25 compilation failed:\n" + compiled.stdout + compiled.stderr)
        cls.command = [cls.java, "-cp", str(cls.directory), "Runner"]

    def invoke(self, options=(), env=None, source=False, output=None):
        directory = output or self.directory / self._testMethodName
        directory.mkdir(exist_ok=True)
        environment = os.environ.copy()
        environment.pop("ADMIN_TOKEN", None)
        environment.pop("OJP_SLOW_QUERY_SEGREGATION_VERIFIED", None)
        environment.update(env or {})
        command = [self.java, str(self.source)] if source else self.command
        result = subprocess.run([*command, "--output", str(directory), *options],
                                env=environment, capture_output=True, text=True, timeout=45)
        return result, directory

    def run_local(self, app=None, options=(), expected=0, env=None, source=False):
        app = app or Application()
        with app.serve():
            result, directory = self.invoke([
                "--url", app.url, "--ojp-metrics-url", app.url + "/ojp",
                "--scenario", "integration", "--mode", "direct",
                "--run-id", "test", "--rps", "20", "--warmup", "0", "--duration", ".2",
                "--request-timeout", "2", "--control-timeout", "2",
                "--readiness-timeout", "2", *options], env=env, source=source)
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        paths = sorted(directory.glob("test-*.json"))
        return [json.loads(path.read_text()) for path in paths], app, result, directory

    def test_source_launcher_smoke_and_contract(self):
        def legacy_readiness(event):
            if event["path"] == "/health":
                return 200, {"status": "up"}, 0
            if event["path"] == "/ready":
                return 200, {"healthy": True, "active_connections": 2}, 0
            return None

        reports, app, result, directory = self.run_local(
            Application(control=legacy_readiness), source=True)
        self.assertEqual(len(reports), 1)
        report = reports[0]
        self.assertEqual((report["mode"], report["rps"], report["seed"]),
                         ("direct", 20, 12345))
        self.assertEqual(report["measure"]["counts"]["scheduled"], 4)
        self.assertTrue(report["measure"]["telemetry"]["final_sent"])
        for kind in ("end_to_end", "service"):
            quantiles = report["measure"]["latency_ms"][kind]
            self.assertEqual(quantiles["p95"], quantiles["max"])
            self.assertEqual(quantiles["p99"], quantiles["max"])
        self.assertIn("counter_semantics", report)
        self.assertIn("telemetry_counter_semantics", report)
        self.assertIn("benchmark_result_error", report)
        self.assertFalse(app.requests("/benchmark-result"))
        self.assertFalse(list(directory.glob("*.partial")))
        summary = json.loads(result.stdout.splitlines()[-1])
        self.assertEqual(summary["counts"], report["measure"]["counts"])
        self.assertEqual(Path(summary["output"]).name, next(directory.glob("*.json")).name)

    def test_validation_is_strict_and_precedes_http(self):
        invalid = (
            ["--rps", "0"], ["--rps", "1,,2"], ["--rps", "nan"], ["--rps", "inf"],
            ["--rps", "2.5"], ["--rps", "10001"],
            ["--rps", ",".join(map(str, range(1, 66)))],
            ["--duration", "-1"], ["--duration", "nan"], ["--duration", "0"],
            ["--warmup", "-1"], ["--warmup", "inf"], ["--max-in-flight", "0"],
            ["--request-timeout", "0"], ["--control-timeout", "0"],
            ["--readiness-timeout", "0"], ["--oltp-percent", "80"],
            ["--olap-percent", "101"], ["--mode", "invalid"], ["--url", "app:8080"],
            ["--scenario", "arbitrary"], ["--mo", "direct"], ["--warmup-o"],
            ["--scen", "integration"], ["--run-id", ""], ["--run-id", "x" * 65],
            ["--seed", "1.5"], ["--max-in-flight", "2.5"],
        )
        with Application().serve() as app:
            for options in invalid:
                with self.subTest(options=options):
                    result, directory = self.invoke(["--url", app.url, *options])
                    self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                    self.assertFalse(list(directory.glob("*.json")))
            self.assertEqual(app.events, [])

    def test_operation_mix_validation(self):
        invalid = (
            "browse=90,report=10",
            "browse=30,history=25,checkout=20,review=10,account=5,report=nan",
            "browse=30,history=25,checkout=20,review=10,account=5,report=inf",
            "browse=30,history=25,checkout=20,review=10,account=5,report=10.0",
            "browse=31,history=25,checkout=20,review=10,account=5,report=10",
            "browse=40,history=25,checkout=20,review=10,account=-5,report=10",
            "browse=30,history=25,checkout=20,review=10,account=5,other=10",
            "browse=30,history=25,checkout=20,review=10,account=5,report=10,report=10",
            "browse=40,history=25,checkout=20,review=10,account=5,report=0",
        )
        for mix in invalid:
            with self.subTest(mix=mix):
                result, _ = self.invoke(["--operation-mix", mix])
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)

    def test_default_profile_and_empty_warmup_only(self):
        with Application().serve() as app:
            result, directory = self.invoke([
                "--url", app.url, "--ojp-metrics-url", app.url + "/ojp",
                "--scenario", "integration", "--warmup-only", "--warmup", "0",
                "--run-id", "defaults"])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        reports = [json.loads(path.read_text()) for path in sorted(directory.glob("*.json"))]
        self.assertEqual([(report["mode"], report["rps"]) for report in reports],
                         [(mode, rate) for mode in ("direct", "ojp") for rate in (8, 256, 8, 8)])
        for report in reports:
            self.assertEqual((report["seed"], report["max_in_flight"],
                              report["request_timeout"], report["control_timeout"]),
                             (12345, 512, 30, 210))
            weights = report["operation_weights"]
            self.assertAlmostEqual(weights["report"] / sum(weights.values()), .02)
            self.assertAlmostEqual(weights["browse"] / sum(weights.values()), .98 / 3)
            self.assertNotIn("measure", report)
            self.assertNotIn("measurement_reset_response", report)
            self.assertEqual(report["warmup"]["counts"]["scheduled"], 0)
            self.assertEqual(report["warmup"]["throughput"]["offered_per_second"], 0)
            self.assertIsNone(report["warmup"]["latency_ms"]["end_to_end"]["p99"])
            self.assertIn("readiness_after", report)
        self.assertFalse(app.requests("/context", "measure"))

    def test_slow_endpoint_remains_open_loop_bounded_and_drains(self):
        app = Application(work=lambda event: (200, {
            "success": True, "operation": event["query"]["op"]},
            .15 if event["query"]["phase"] == "measure" else 0))
        reports, app, _, _ = self.run_local(app, ["--rps", "100", "--duration", ".35",
                                                 "--max-in-flight", "2"])
        phase = reports[0]["measure"]
        counts = phase["counts"]
        self.assertEqual(counts["scheduled"], 35)
        self.assertEqual(counts["scheduled"], counts["dispatched"] + counts["dropped"])
        self.assertGreater(counts["dropped"], 15)
        self.assertLessEqual(counts["peak_in_flight"], 2)
        self.assertLessEqual(app.peak, 2)
        self.assertEqual(counts["in_flight"], 0)
        self.assertEqual(counts["dispatched"], counts["sql_completed"])
        self.assertEqual(counts["completed"], counts["dispatched"])
        self.assertLess(phase["elapsed_seconds"], 2)
        self.assertGreater(phase["latency_ms"]["end_to_end"]["p50"], 100)
        self.assertGreater(phase["in_flight_at_arrival_end"], 0)
        self.assertGreater(phase["drain_seconds"], 0)
        self.assertEqual(sum(op["scheduled"] for op in phase["operations"].values()), 35)
        self.assertEqual(phase["scheduled_latency_histogram"]["count"], counts["completed"])
        self.assertEqual(phase["scheduled_latency_histogram"]["buckets"]["+Inf"],
                         counts["dispatched"])
        for kind in ("end_to_end", "service"):
            values = phase["latency_ms"][kind]
            quantiles = [values[key] for key in ("p50", "p90", "p95", "p99", "max")]
            self.assertEqual(quantiles, sorted(quantiles))
        self.assertTrue(phase["start_time"].endswith("Z"))
        self.assertLessEqual(phase["start_time"], phase["end_arrivals_time"])
        self.assertLessEqual(phase["end_arrivals_time"], phase["end_time"])
        self.assertAlmostEqual(phase["end_arrivals_epoch_seconds"] -
                               phase["start_epoch_seconds"], .35, places=5)
        self.assertEqual(phase["throughput"]["offered_per_second"], 100)
        self.assertAlmostEqual(phase["throughput"]["sql_completions_per_second"],
                               counts["sql_completed"] / phase["elapsed_seconds"])
        self.assertEqual(phase["drop_fraction"], counts["dropped"] / counts["scheduled"])
        self.assertLessEqual(counts["arrival_window_sql_completed"], counts["sql_completed"])

    def test_202_is_not_sql_success_and_telemetry_does_not_claim_success(self):
        app = Application(work=lambda event: (
            202 if event["query"]["phase"] == "measure" else 200,
            {"success": True, "operation": event["query"]["op"], "accepted": True}, 0))
        reports, app, _, _ = self.run_local(app, expected=1)
        phase = reports[0]["measure"]
        counts = phase["counts"]
        self.assertGreater(counts["accepted"], 0)
        self.assertEqual(counts["sql_completed"], 0)
        self.assertEqual(counts["successful_transactions"], 0)
        self.assertEqual(counts["dispatched"], counts["failed_transactions"])
        self.assertEqual(phase["error_fraction"], 1)
        self.assertIn("zero SQL", reports[0]["error"])
        final = app.requests("/arrivals", "measure")[-1]["query"]
        self.assertEqual(final["completed"], "0")
        self.assertEqual(int(final["http_completed"]), counts["completed"])
        self.assertEqual(int(final["errors"]), counts["failed_transactions"])

    def test_readiness_requires_real_sql_not_just_ready(self):
        app = Application(work=lambda event: (202, {
            "success": True, "operation": event["query"]["op"]}, 0))
        reports, app, _, _ = self.run_local(
            app, ["--readiness-timeout", ".35"], expected=1)
        self.assertIn("readiness", reports[0]["error"])
        self.assertNotIn("measure", reports[0])
        self.assertTrue(app.requests("/ready"))
        self.assertTrue(app.requests("/work"))
        self.assertFalse(app.requests("/work", "measure"))

    def test_nonempty_warmup_only_with_zero_sql_is_exportable_failure(self):
        app = Application()

        def work(event):
            measuring = bool(app.requests("/arrivals", "warmup"))
            return (503 if measuring else 200,
                    {"success": not measuring, "operation": event["query"]["op"]}, 0)

        app.work = work
        reports, app, _, _ = self.run_local(
            app, ["--warmup-only", "--warmup", ".1"], expected=1)
        self.assertIn("zero SQL", reports[0]["error"])
        self.assertEqual(reports[0]["warmup"]["counts"]["failed_transactions"], 2)
        self.assertNotIn("measure", reports[0])
        self.assertFalse(app.requests("/context", "measure"))

    def test_readiness_requires_ready_flag_before_sql_probe(self):
        app = Application(control=lambda event: (
            (200, {"ready": False}, 0) if event["path"] == "/ready" else None))
        reports, app, _, _ = self.run_local(
            app, ["--readiness-timeout", ".35"], expected=1)
        self.assertIn("readiness", reports[0]["error"])
        self.assertNotIn("warmup", reports[0])
        self.assertFalse(app.requests("/work"))

    def test_control_reset_warmup_isolation_and_admin_token(self):
        reports, app, _, _ = self.run_local(
            options=["--warmup", ".1"], env={"ADMIN_TOKEN": "fixture-only"})
        report = reports[0]
        self.assertEqual(report["warmup"]["counts"]["scheduled"], 2)
        self.assertEqual(report["measure"]["counts"]["scheduled"], 4)
        self.assertEqual(report["measure"]["counts"]["sql_completed"], 4)
        self.assertEqual(report["readiness_after"]["response"]["active_connections"], 2)
        events = app.events
        self.assertEqual((events[0]["method"], events[0]["path"], events[0]["query"]),
                         ("POST", "/mode", {"value": "direct"}))
        context = next(i for i, event in enumerate(events)
                       if event["path"] == "/context" and event["query"]["phase"] == "measure")
        self.assertEqual([event["path"] for event in events[context - 1:context + 4]],
                         ["/reset", "/context", "/metrics", "/arrivals", "/work"])
        for event in events:
            self.assertEqual(event["authorization"],
                             "Bearer " + "fixture-only" if event["method"] == "POST" else None)
        for phase in ("warmup", "measure"):
            publications = app.requests("/arrivals", phase)
            initial, final = publications[0]["query"], publications[-1]["query"]
            self.assertEqual(initial["reset"], "true")
            self.assertEqual(initial["scheduled"], "0")
            self.assertEqual(initial["completed"], "0")
            self.assertEqual(final["reset"], "false")
            self.assertEqual(final["final"], "true")
            self.assertEqual(final["inflight"], "0")
            self.assertEqual(int(final["latency_count"]), report[phase]["counts"]["completed"])
            buckets = list(map(int, final["latency_bucket_counts"].split(",")))
            self.assertEqual(len(buckets), 14)
            self.assertEqual(buckets, sorted(buckets))
            self.assertEqual(buckets[-1], int(final["latency_count"]))
        warmup_final = app.requests("/arrivals", "warmup")[-1]
        self.assertLess(events.index(warmup_final), context)

    def test_request_timeout_is_failed_and_drained(self):
        app = Application(work=lambda event: (
            200, {"success": True, "operation": event["query"]["op"]},
            .4 if event["query"]["phase"] == "measure" else 0))
        reports, _, _, _ = self.run_local(app, [
            "--request-timeout", ".08", "--rps", "10", "--duration", ".2"], expected=1)
        counts = reports[0]["measure"]["counts"]
        self.assertEqual(counts["timeouts"], 2)
        self.assertEqual(counts["completed"], 2)
        self.assertEqual(counts["sql_completed"], 0)
        self.assertEqual(counts["in_flight"], 0)

    def test_control_timeout_is_exported_and_no_load_is_started(self):
        app = Application(control=lambda event: (
            (200, {"success": True}, .4) if event["path"] == "/mode" else None))
        reports, app, _, _ = self.run_local(
            app, ["--control-timeout", ".08"], expected=1)
        self.assertIn("error", reports[0])
        self.assertNotIn("warmup", reports[0])
        self.assertFalse(app.requests("/work"))

    def test_work_response_contract_admission_and_transient_errors(self):
        responses = [
            (200, {"success": False, "accepted": False}),
            (500, {"success": False, "accepted": True}),
            (200, {"success": True, "sql_completed": 0}),
            (200, {"success": True, "rollback": True}),
            (200, {"success": True, "operation": "unknown"}),
            (200, "not json"),
            (200, {"success": True}),
        ]
        lock = threading.Lock()
        measured = 0

        def work(event):
            nonlocal measured
            if event["query"]["phase"] != "measure":
                return 200, {"success": True, "op": event["query"]["op"]}, 0
            with lock:
                status, body = responses[measured]
                measured += 1
            if isinstance(body, dict):
                body = {"operation": event["query"]["op"], **body}
            return status, body, 0

        reports, _, _, _ = self.run_local(
            Application(work=work), ["--rps", "10", "--duration", ".7"])
        phase = reports[0]["measure"]
        self.assertNotIn("error", reports[0])
        self.assertEqual(phase["counts"]["accepted"], 6)
        self.assertEqual(phase["counts"]["sql_completed"], 2)
        self.assertEqual(phase["counts"]["successful_transactions"], 1)
        self.assertEqual(phase["counts"]["failed_transactions"], 6)
        self.assertEqual(phase["errors"], {
            "sql_failed": 1, "http_500": 1, "sql_not_completed": 1,
            "rollback": 1, "operation_mismatch": 1, "invalid_json": 1})

    def test_periodic_telemetry_cumulative_resource_samples_and_benchmark(self):
        reports, app, _, _ = self.run_local(
            Application(arrivals_delay=.15), ["--rps", "40", "--duration", "2.4"])
        report = reports[0]
        phase = report["measure"]
        self.assertEqual(phase["counts"]["scheduled"], 96)
        self.assertEqual(phase["counts"]["sql_completed"], 96)
        self.assertLess(phase["elapsed_seconds"], 5)
        publications = [event["query"] for event in app.requests("/arrivals", "measure")]
        self.assertGreaterEqual(len(publications), 3)
        for key in ("scheduled", "dispatched", "completed", "errors", "http_completed"):
            values = [int(event[key]) for event in publications]
            self.assertEqual(values, sorted(values), key)
        self.assertEqual(publications[0]["reset"], "true")
        self.assertTrue(all(event["reset"] == "false" for event in publications[1:]))
        self.assertEqual(publications[-1]["scheduled"], "96")
        self.assertEqual(publications[-1]["final"], "true")
        self.assertEqual(phase["postgres_cpu_samples"], [12.5] * len(phase["postgres_cpu_samples"]))
        self.assertGreater(len(phase["postgres_cpu_samples"]), 0)
        self.assertTrue(all(value == 3 for value in phase["postgres_connection_samples"]))
        self.assertGreater(len(phase["postgres_connection_samples"]), 0)
        for field in ("postgres_cpu_sample_times", "postgres_connection_sample_times"):
            for timestamp in phase[field]:
                self.assertLessEqual(phase["start_epoch_seconds"], timestamp)
                self.assertLessEqual(timestamp, phase["end_arrivals_epoch_seconds"])
        benchmark = app.requests("/benchmark-result")[-1]["query"]
        self.assertEqual(float(benchmark["physical_connections"]), 3)
        self.assertEqual(float(benchmark["postgres_cpu_percent"]), 12.5)
        self.assertEqual(benchmark["mode"], "direct")
        self.assertTrue(report["benchmark_result_published"])

    def test_failed_final_telemetry_warns_but_preserves_sql_results(self):
        app = Application(control=lambda event: (
            (503, {"success": False}, 0) if event["path"] == "/arrivals" else None))
        reports, _, result, _ = self.run_local(app)
        self.assertFalse(reports[0]["measure"]["telemetry"]["final_sent"])
        self.assertIn("WARNING: final /arrivals publication failed", result.stderr)
        self.assertGreater(reports[0]["measure"]["counts"]["sql_completed"], 0)

    def test_zero_weights_and_csv_json_contract(self):
        mix = "browse=0,history=0,checkout=0,review=0,account=0,report=100"
        reports, app, _, directory = self.run_local(options=[
            "--operation-mix", mix, "--oltp-percent", "0", "--olap-percent", "100",
            "--warmup-only", "--warmup", ".1"])
        report = reports[0]
        self.assertTrue(report["warmup_only"])
        self.assertNotIn("measure", report)
        self.assertTrue(all(event["query"]["op"] == "report" for event in app.requests("/work")))
        self.assertEqual(report["operation_weights"], dict.fromkeys(OPERATIONS[:-1], 0) | {"report": 100})
        with next(directory.glob("*.csv")).open(newline="") as source:
            rows = {row["operation"]: row for row in csv.DictReader(source)}
        self.assertEqual(set(rows), {"all", *OPERATIONS})
        self.assertEqual(float(rows["all"]["offered_per_second"]), 20)
        self.assertGreater(float(rows["all"]["successful_completions_per_second"]), 0)
        self.assertEqual(rows["browse"]["scheduled"], "0")
        self.assertIsNone(report["warmup"]["operations"]["browse"]["latency_ms"]["service"]["p50"])
        aggregate = [*report["warmup"]["throughput"], "error_fraction", "drop_fraction"]
        for operation in OPERATIONS:
            for field in aggregate:
                self.assertEqual(rows[operation][field], "", (operation, field))
            self.assertEqual(json.loads(rows[operation]["errors_json"]), report["warmup"]["errors"])
            self.assertEqual(json.loads(rows[operation]["readiness_after_json"]),
                             report["readiness_after"])

    def test_exact_operation_blocks_and_same_seeded_plan_for_sequential_modes(self):
        mix = "browse=30,history=25,checkout=20,review=10,account=5,report=10"
        reports, app, _, _ = self.run_local(options=[
            "--mode", "both", "--rps", "100", "--duration", "1",
            "--operation-mix", mix, "--oltp-percent", "90", "--olap-percent", "10"])
        self.assertEqual([report["mode"] for report in reports], ["direct", "ojp"])
        plans = {}
        for mode in ("direct", "ojp"):
            work = [event["query"] for event in app.requests("/work", "measure")
                    if event["mode"] == mode]
            self.assertEqual(len(work), 100)
            self.assertEqual(Counter(event["op"] for event in work),
                             {"browse": 30, "history": 25, "checkout": 20,
                              "review": 10, "account": 5, "report": 10})
            # Arrival order at the server may differ for simultaneous worker starts.
            plans[mode] = sorted((event["seed"], event["op"]) for event in work)
        self.assertEqual(plans["direct"], plans["ojp"])
        block = [operation for operation, weight in
                 (("browse", 30), ("history", 25), ("checkout", 20),
                  ("review", 10), ("account", 5), ("report", 10))
                 for _ in range(weight)]
        random.Random(12345).shuffle(block)
        work_random = random.Random(12345)
        expected = sorted((str(work_random.getrandbits(63)), operation) for operation in block)
        self.assertEqual(plans["direct"], expected)
        modes = [event["query"]["value"] for event in app.requests("/mode")]
        self.assertEqual(modes, ["direct", "ojp"])
        classification = reports[1]["slow_query_classification"]
        self.assertTrue(classification["observed"])
        self.assertEqual(classification["count"], 10)
        self.assertEqual(classification["by_statement"],
                         {"select report": 10, "select browse": 0})

    def test_negative_and_arbitrary_precision_seeds_preserve_python_work_seeds(self):
        mix = "browse=0,history=0,checkout=0,review=0,account=0,report=100"
        for index, seed in enumerate((-12345, (1 << 200) + 17)):
            with self.subTest(seed=seed):
                reports, app, _, _ = self.run_local(options=[
                    "--run-id", f"test-seed-{index}", "--seed", str(seed),
                    "--operation-mix", mix, "--oltp-percent", "0", "--olap-percent", "100",
                    "--duration", ".1"])
                self.assertTrue(any(report["seed"] == seed for report in reports))
                rng = random.Random(seed)
                self.assertEqual(
                    sorted(int(event["query"]["seed"]) for event in app.requests("/work", "measure")),
                    sorted(rng.getrandbits(63) for _ in range(2)))

    def test_both_modes_and_all_rates_attempted_after_direct_failure(self):
        app = Application(control=lambda event: (
            (503, {"success": False}, 0)
            if event["path"] == "/mode" and event["mode"] == "direct" else None))
        reports, app, _, directory = self.run_local(app, [
            "--mode", "both", "--rps", "30,10,30", "--duration", ".1"], expected=1)
        self.assertEqual([(report["mode"], report["rps"]) for report in reports],
                         [(mode, rate) for mode in ("direct", "ojp") for rate in (30, 10, 30)])
        self.assertTrue(all("error" in report for report in reports[:3]))
        self.assertTrue(all(report["measure"]["counts"]["sql_completed"] > 0
                            for report in reports[3:]))
        self.assertEqual([event["query"]["value"] for event in app.requests("/mode")],
                         ["direct"] * 3 + ["ojp"] * 3)
        with sorted(directory.glob("*.csv"))[0].open(newline="") as source:
            error_rows = list(csv.DictReader(source))
        self.assertEqual(len(error_rows), 1)
        self.assertEqual(error_rows[0]["error"], reports[0]["error"])

    def test_duplicate_run_id_refused_for_either_format_before_http(self):
        directory = self.directory / self._testMethodName
        directory.mkdir()
        for extension in ("json", "csv"):
            with self.subTest(extension=extension), Application().serve() as app:
                existing = directory / ("reused_run-001-direct-1." + extension)
                existing.write_text("do not overwrite")
                result, _ = self.invoke([
                    "--url", app.url, "--mode", "direct", "--run-id", "reused/run"],
                    output=directory)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertIn("choose a unique --run-id", result.stderr)
                self.assertEqual(existing.read_text(), "do not overwrite")
                self.assertEqual(app.events, [])
                existing.unlink()

    def test_preexisting_sampler_directory_does_not_block_run(self):
        directory = self.directory / self._testMethodName
        (directory / "test").mkdir(parents=True)
        reports, _, _, _ = self.run_local()
        self.assertEqual(len(reports), 1)

    def test_ojp_htap_requires_verified_segregation_before_http(self):
        with Application().serve() as app:
            result, directory = self.invoke(["--url", app.url, "--rps", "1"])
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("requires scripts/htap.sh", result.stderr)
        self.assertEqual(app.events, [])
        self.assertFalse(list(directory.glob("*.json")))

    def test_verified_htap_ojp_and_no_slow_classification_claim(self):
        reports, _, _, _ = self.run_local(Application(slow_metrics=False), [
            "--mode", "ojp", "--scenario", "htap"],
            env={"OJP_SLOW_QUERY_SEGREGATION_VERIFIED": "true"})
        classification = reports[0]["slow_query_classification"]
        self.assertFalse(classification["observed"])
        self.assertEqual(classification["count"], 0)
        self.assertIn("not demonstrated", classification["message"])

    def test_unavailable_ojp_metrics_do_not_claim_classification(self):
        app = Application(control=lambda event: (
            (503, "unavailable", 0) if event["path"] == "/ojp/metrics" else None))
        reports, _, _, _ = self.run_local(app, ["--mode", "ojp"])
        classification = reports[0]["slow_query_classification"]
        self.assertIsNone(classification["observed"])
        self.assertIsNone(classification["count"])
        self.assertIn("unavailable", classification["message"])

    def test_stdio_http_bridge_preserves_ids_status_body_errors_and_admin_header(self):
        def control(event):
            if event["path"] == "/echo":
                return 201, {"echo": "snowman \u2603\nquoted \"value\""}, 0
            if event["path"] == "/unavailable":
                return 503, "upstream unavailable", 0
            return None

        with Application(control=control).serve() as app:
            messages = [
                {"id": "ready", "url": app.url + "/ready", "timeout": 5},
                {"id": 42, "url": app.url + "/echo?text=a%26b", "timeout": 5,
                 "method": "POST", "data": {"nested": [None, True, 3, "snowman \u2603"]},
                 "headers": {"Authorization": "Bearer " + "fixture-only",
                             "Content-Type": "application/json"}},
                {"id": "http-error", "url": app.url + "/unavailable", "timeout": 5},
                {"id": "invalid-url", "url": "http://[broken?private-marker", "timeout": 5},
            ]
            result = subprocess.run(
                [self.java, "-cp", str(self.directory), "HttpBridge"],
                input="\n".join(map(json.dumps, messages)) + "\n",
                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        replies = {reply["id"]: reply for reply in
                   map(json.loads, result.stdout.splitlines())}
        self.assertEqual(set(replies), {"ready", 42, "http-error", "invalid-url"})
        self.assertEqual(replies["ready"]["status"], 200)
        self.assertTrue(json.loads(replies["ready"]["body"])["ready"])
        self.assertEqual(replies[42]["status"], 201)
        self.assertEqual(json.loads(replies[42]["body"]),
                         {"echo": "snowman \u2603\nquoted \"value\""})
        self.assertEqual(replies["http-error"]["status"], 503)
        self.assertEqual(replies["http-error"]["body"], "upstream unavailable")
        self.assertIn("error", replies["invalid-url"])
        self.assertNotIn("status", replies["invalid-url"])
        self.assertNotIn("private-marker", replies["invalid-url"]["error"])
        posted = app.requests("/echo")[0]
        self.assertEqual(posted["query"], {"text": "a&b"})
        self.assertEqual(json.loads(posted["body"]), messages[1]["data"])
        self.assertEqual(posted["authorization"], "Bearer " + "fixture-only")
        self.assertEqual(posted["content_type"], "application/json")
        self.assertIsNone(app.requests("/ready")[0]["authorization"])

    def test_delay_proxy_forwards_query_post_body_status_and_delays_work(self):
        def control(event):
            if event["path"] == "/echo":
                return 201, {"echo": event["body"]}, 0
            if event["path"] == "/unavailable":
                return 503, "upstream unavailable", 0
            return None

        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        with Application(control=control).serve() as app:
            environment = dict(os.environ, TARGET_APP_URL=app.url, PORT=str(port))
            process = subprocess.Popen(
                [self.java, "-cp", str(self.directory), "DelayProxy"],
                env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                base = f"http://127.0.0.1:{port}"
                deadline = time.monotonic() + 20
                while True:
                    if process.poll() is not None:
                        self.fail("DelayProxy exited: " + process.communicate()[1])
                    try:
                        with urllib.request.urlopen(base + "/ready", timeout=1) as response:
                            self.assertTrue(json.load(response)["ready"])
                        break
                    except (OSError, urllib.error.URLError):
                        if time.monotonic() >= deadline:
                            self.fail("DelayProxy did not become responsive")
                        time.sleep(.05)
                started = time.monotonic()
                with urllib.request.urlopen(
                        base + "/work?op=report&phase=measure&seed=7", timeout=5) as response:
                    self.assertEqual(response.status, 200)
                    self.assertEqual(json.load(response)["operation"], "report")
                forwarded = app.requests("/work")[0]
                self.assertGreaterEqual(forwarded["received_at"] - started, .20)
                self.assertEqual(forwarded["query"],
                                 {"op": "report", "phase": "measure", "seed": "7"})
                payload = b'{"message":"forwarded"}'
                request = urllib.request.Request(
                    base + "/echo?label=a%26b", data=payload, method="POST",
                    headers={"Authorization": "Bearer " + "fixture-only",
                             "Content-Type": "application/json"})
                with urllib.request.urlopen(request, timeout=5) as response:
                    self.assertEqual(response.status, 201)
                    self.assertEqual(json.load(response)["echo"], payload.decode())
                posted = app.requests("/echo")[0]
                self.assertEqual(posted["method"], "POST")
                self.assertEqual(posted["query"], {"label": "a&b"})
                self.assertEqual(posted["authorization"], "Bearer " + "fixture-only")
                self.assertEqual(posted["content_type"], "application/json")
                with self.assertRaises(urllib.error.HTTPError) as failed:
                    urllib.request.urlopen(base + "/unavailable", timeout=5)
                self.assertEqual(failed.exception.code, 503)
                self.assertEqual(failed.exception.read(), b"upstream unavailable")
                failed.exception.close()
            finally:
                process.terminate()
                try:
                    process.communicate(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=5)

    def test_htap_entrypoint_enforces_segregation_and_removes_old_benchmarks(self):
        script = (ROOT / "scripts" / "htap.sh").read_text()
        readme = (ROOT / "README.md").read_text()
        self.assertIn("export SLOW_QUERY_ENABLED=true", script)
        self.assertIn("export OJP_SLOW_QUERY_SEGREGATION_VERIFIED=true", script)
        self.assertIn("-Dojp.server.slowQuerySegregation.enabled=true", script)
        self.assertIn("-Dojp.server.slowQuerySegregation.slowSlotPercentage=20", script)
        self.assertIn("--rps 8,256,8,8 --warmup 10 --duration 30", script)
        self.assertIn("--oltp-percent 98 --olap-percent 2", script)
        self.assertIn('python3 "$SCRIPT_DIR/resources.py" "$@"', script)
        self.assertIn("```mermaid", readme)
        for component in ("Docker Compose", "PostgreSQL", "OJP server", "Prometheus",
                          "Grafana", "postgres-exporter", "Host resource sampler",
                          "Java open-loop load runner"):
            self.assertIn(component, readme)
        for old in ("death-spiral.sh", "slow-queries.sh", "throttling.sh", "compare.sh",
                    "failover.sh", "warmup.sh"):
            self.assertFalse((ROOT / "scripts" / old).exists())
            self.assertNotIn(old, readme)

    def test_htap_rejects_alternate_modes_before_docker(self):
        script = ROOT / "scripts" / "htap.sh"
        for options in (["--scenario", "failover"], ["--scenario=integration"],
                        ["--mode", "direct"], ["--mode=ojp"], ["--warmup-only"]):
            with self.subTest(options=options):
                result = subprocess.run(["bash", str(script), *options],
                                        capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertIn("HTAP always measures Direct JDBC then OJP", result.stderr)

    def test_htap_rebuilds_runner_before_sampling_and_running(self):
        path = ROOT / "scripts" / "resources.py"
        spec = importlib.util.spec_from_file_location("htap_resources", path)
        resources = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(resources)
        events = MagicMock()
        with patch.object(resources.subprocess, "run") as build, \
                patch.object(resources.subprocess, "call", return_value=7) as run, \
                patch.object(resources, "ResourceSampler") as sampler, \
                patch.object(resources.sys, "argv", [
                    str(path), "--run-id", "test-htap", "--rps", "2"]):
            sampler.return_value.path = Path("/results/test-htap/resources.jsonl")
            events.attach_mock(build, "build")
            events.attach_mock(sampler, "sampler")
            events.attach_mock(run, "run")
            with redirect_stdout(io.StringIO()):
                self.assertEqual(resources.main(), 7)
            build.assert_called_once_with(
                resources.COMPOSE + ["--profile", "tools", "build", "load"], check=True)
            self.assertEqual([call[0] for call in events.mock_calls],
                             ["build", "sampler", "sampler().start", "run",
                              "sampler().stop"])
            command = run.call_args.args[0]
            self.assertEqual(command[-4:], ["--scenario", "htap", "--mode", "both"])
            self.assertEqual(command[command.index("--rps") + 1], "2")
        with patch.object(resources.subprocess, "run",
                          side_effect=subprocess.CalledProcessError(1, "build")), \
                patch.object(resources.subprocess, "call") as run, \
                patch.object(resources, "ResourceSampler") as sampler, \
                patch.object(resources.sys, "argv", [str(path)]):
            with self.assertRaises(subprocess.CalledProcessError):
                resources.main()
            run.assert_not_called()
            sampler.assert_not_called()


if __name__ == "__main__":
    unittest.main()
