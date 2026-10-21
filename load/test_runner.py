"""Run with: python -m unittest discover -s load -v."""

from collections import Counter
from contextlib import redirect_stderr, redirect_stdout
import csv
import http.server
import io
import itertools
import json
import os
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

import runner


class Handler(http.server.BaseHTTPRequestHandler):
    delay = .12
    status_code = 200

    def do_GET(self):
        from urllib.parse import parse_qs, urlsplit
        query = parse_qs(urlsplit(self.path).query)
        time.sleep(self.delay)
        body = json.dumps({"success": True, "operation": query["op"][0]}).encode()
        self.send_response(self.status_code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass

    def do_POST(self):
        body = b'{"success": true}'
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class RunnerTests(unittest.TestCase):
    def test_validation(self):
        for argv in (["--rps", "0"], ["--rps", "1,,2"], ["--rps", "nan"],
                     ["--rps", "inf"], ["--duration", "-1"], ["--warmup", "-1"],
                     ["--max-in-flight", "0"], ["--request-timeout", "0"],
                     ["--control-timeout", "0"],
                     ["--oltp-percent", "80"], ["--olap-percent", "101"],
                     ["--mode", "invalid"], ["--url", "app:8080"],
                     ["--scenario", "arbitrary"], ["--rps", "2.5"], ["--rps", "10001"],
                     ["--rps", ",".join(map(str, range(1, 66)))]):
            with self.subTest(argv=argv), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    runner.arguments(argv)
        args = runner.arguments(["--rps", "30,10,30", "--warmup", "0",
                                 "--oltp-percent", "80", "--olap-percent", "20"])
        self.assertEqual(args.rps, [30, 10, 30])
        defaults = runner.arguments([])
        self.assertEqual(defaults.rps, [8, 256, 8, 8])
        self.assertEqual((defaults.warmup, defaults.duration, defaults.max_in_flight,
                          defaults.seed), (10, 30, 512, 12345))
        self.assertEqual(defaults.mode, "both")
        self.assertEqual((defaults.oltp_percent, defaults.olap_percent), (98, 2))
        self.assertEqual(defaults.scenario, "htap")
        profile_weights = runner.operation_weights(
            defaults.oltp_percent, defaults.olap_percent)
        profile_size = sum(profile_weights.values())
        self.assertEqual(profile_weights["report"] / profile_size, .02)
        self.assertEqual(sum(value for op, value in profile_weights.items()
                             if op != "report") / profile_size, .98)
        custom = runner.arguments(["--rps", "5,11", "--warmup", "3", "--duration", "7",
                                   "--request-timeout", "9", "--control-timeout", "42",
                                   "--max-in-flight", "23"])
        self.assertEqual(custom.rps, [5, 11])
        self.assertEqual((custom.warmup, custom.duration, custom.request_timeout,
                          custom.control_timeout, custom.max_in_flight), (3, 7, 9, 42, 23))
        self.assertTrue(runner.is_ready(200, {"status": "up"}))

    def test_weighted_schedule(self):
        weights = runner.operation_weights()
        stream = runner.operations(17, weights)
        block = list(itertools.islice(stream, sum(weights.values())))
        self.assertEqual(Counter(block), weights)
        self.assertEqual(block, list(itertools.islice(runner.operations(17, weights), len(block))))
        for oltp in (0, 1, 50, 80, 100):
            weights = runner.operation_weights(oltp, 100 - oltp)
            total = sum(weights.values())
            self.assertEqual(weights["report"] / total, (100 - oltp) / 100)
            self.assertEqual(weights["browse"] / total, oltp / 300)

    def test_htap_entrypoint_enforces_segregation_and_removes_old_benchmarks(self):
        root = Path(__file__).resolve().parent.parent
        script = (root / "scripts" / "htap.sh").read_text()
        readme = (root / "README.md").read_text()
        self.assertIn("export SLOW_QUERY_ENABLED=true", script)
        self.assertIn("export OJP_SLOW_QUERY_SEGREGATION_VERIFIED=true", script)
        self.assertIn("-Dojp.server.slowQuerySegregation.enabled=true", script)
        self.assertIn("-Dojp.server.slowQuerySegregation.slowSlotPercentage=20", script)
        self.assertIn("--mode both --rps 8,256,8,8 --warmup 10 --duration 30", script)
        self.assertIn("--oltp-percent 98 --olap-percent 2", script)
        self.assertIn("run_load htap", script)
        self.assertIn("```mermaid", readme)
        for component in ("Docker Compose", "PostgreSQL", "OJP server", "Prometheus",
                          "Grafana", "postgres-exporter", "Host resource sampler",
                          "Python open-loop load runner"):
            self.assertIn(component, readme)
        for old in ("death-spiral.sh", "slow-queries.sh", "throttling.sh", "compare.sh"):
            self.assertFalse((root / "scripts" / old).exists())
            self.assertNotIn(old, readme)

    def test_ojp_slow_query_counter_reports_observed_statement_deltas(self):
        body = ('ojp_sql_slow_executions_total{sql_statement="select report"} 4\n'
                'ojp_sql_slow_executions_total{sql_statement="select browse"} 2\n')
        args = runner.arguments([])
        with patch.object(runner, "request", return_value=(200, None, body)):
            self.assertEqual(runner.slow_query_metrics(args),
                             {"select report": 4, "select browse": 2})

    def test_nearest_rank_percentiles(self):
        self.assertIsNone(runner.percentiles([])["p99"])
        self.assertEqual(runner.percentiles([.1, .2, .3])["p50"], 200)

    def run_local(self, status=200):
        handler = type("ConfiguredHandler", (Handler,), {"status_code": status})
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            args = runner.arguments(["--url", f"http://127.0.0.1:{server.server_port}",
                                     "--max-in-flight", "2", "--request-timeout", "2"])
            return runner.run_phase(args, 100, .3, "measure", "100", 42)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_slow_endpoint_remains_open_loop_and_bounded(self):
        result = self.run_local()
        counts = result["counts"]
        self.assertEqual(counts["scheduled"], 30)
        self.assertEqual(counts["scheduled"], counts["dispatched"] + counts["dropped"])
        self.assertGreater(counts["dropped"], 15)
        self.assertLessEqual(counts["peak_in_flight"], 2)
        self.assertEqual(counts["in_flight"], 0)
        self.assertEqual(counts["dispatched"], counts["sql_completed"])
        self.assertLess(result["elapsed_seconds"], .8)
        self.assertGreater(result["latency_ms"]["end_to_end"]["p50"], 100)
        self.assertEqual(sum(op["scheduled"] for op in result["operations"].values()), 30)
        self.assertEqual(result["scheduled_latency_histogram"]["count"], counts["completed"])
        self.assertEqual(result["scheduled_latency_histogram"]["buckets"]["+Inf"],
                         counts["dispatched"])
        self.assertTrue(result["start_time"].endswith("Z"))
        self.assertLessEqual(result["start_time"], result["end_arrivals_time"])
        self.assertLessEqual(result["end_arrivals_time"], result["end_time"])
        self.assertAlmostEqual(result["end_arrivals_epoch_seconds"] -
                               result["start_epoch_seconds"], .3, places=5)
        self.assertEqual(result["throughput"]["offered_per_second"], 100)
        self.assertAlmostEqual(result["throughput"]["sql_completions_per_second"],
                               counts["sql_completed"] / result["elapsed_seconds"])
        self.assertEqual(result["drop_fraction"], counts["dropped"] / counts["scheduled"])
        self.assertLessEqual(counts["arrival_window_sql_completed"], counts["sql_completed"])

    def test_202_is_never_completion(self):
        result = self.run_local(202)
        counts = result["counts"]
        self.assertGreater(counts["accepted"], 0)
        self.assertEqual(counts["sql_completed"], 0)
        self.assertEqual(counts["successful_transactions"], 0)
        self.assertEqual(counts["dispatched"], counts["failed_transactions"])

    def test_reset_drain_context_and_mode_sequence(self):
        events = []

        def fake_request(url, path, timeout, method="GET", query=None):
            events.append((method, path, dict(query or {})))
            if path == "/work":
                return 200, {"success": True, "operation": query["op"]}, ""
            return 200, {"ready": True, "active_connections": 2}, ""

        args = runner.arguments(["--rps", "100", "--warmup", ".01", "--duration", ".01"])
        with patch.object(runner, "request", side_effect=fake_request):
            result = runner.run_step(args, "direct", 100, 0)
        self.assertNotIn("error", result)
        self.assertEqual(result["measure"]["counts"]["scheduled"], 1)
        self.assertEqual(result["measure"]["counts"]["sql_completed"], 1)
        self.assertEqual(events[0], ("POST", "/mode", {"value": "direct"}))
        measure_context = next(index for index, event in enumerate(events)
                               if event[1] == "/context" and event[2]["phase"] == "measure")
        self.assertEqual(events[measure_context - 1][1], "/reset")
        self.assertEqual(events[measure_context + 1][1], "/metrics")
        self.assertEqual(events[measure_context + 2][1], "/arrivals")
        self.assertEqual(events[measure_context + 3][1], "/work")
        self.assertEqual(result["readiness_after"]["response"]["active_connections"], 2)
        arrivals = [event[2] for event in events if event[1] == "/arrivals"]
        self.assertEqual(len(arrivals), 4)
        self.assertTrue(result["measure"]["telemetry"]["final_sent"])
        self.assertEqual(arrivals[-1]["scheduled"], 1)
        self.assertEqual(arrivals[-1]["inflight"], 0)
        self.assertEqual(arrivals[-1]["latency_count"], 1)
        self.assertEqual(arrivals[-2]["scheduled"], 0)
        self.assertEqual(arrivals[-2]["completed"], 0)
        self.assertEqual(arrivals[-2]["reset"], "true")
        buckets = [int(value) for value in arrivals[-1]["latency_bucket_counts"].split(",")]
        self.assertEqual(len(buckets), 14)
        self.assertEqual(buckets[-1], 1)
        self.assertEqual(buckets, sorted(buckets))

    def test_readiness_requires_both_ready_and_real_sql(self):
        args = runner.arguments(["--readiness-timeout", ".01"])

        def fake_request(url, path, timeout, method="GET", query=None):
            if path == "/work":
                return 202, {"success": True, "operation": query["op"]}, ""
            return 200, {"ready": True}, ""

        with patch.object(runner, "request", side_effect=fake_request):
            with self.assertRaises(RuntimeError):
                runner.readiness(args, "100")

    def test_timeout_and_zero_sql_are_not_success(self):
        args = runner.arguments([])
        with patch.object(runner, "request", side_effect=TimeoutError):
            self.assertTrue(runner.work(args, "browse", 1, "measure", "1")["timeouts"])
        with patch.object(runner, "request", return_value=(
                200, {"success": True, "operation": "browse", "sql_completed": 0}, "")):
            self.assertFalse(runner.work(args, "browse", 1, "measure", "1")["sql_completed"])

    def test_periodic_arrivals_are_cumulative_and_do_not_block_scheduler(self):
        arrivals = []

        def fake_request(url, path, timeout, method="GET", query=None):
            if path == "/arrivals":
                arrivals.append(dict(query))
                time.sleep(.15)
                return 200, {"success": True}, ""
            return 200, {"success": True, "operation": query["op"]}, ""

        args = runner.arguments(["--max-in-flight", "2"])
        with patch.object(runner, "request", side_effect=fake_request):
            result = runner.run_phase(args, 100, 1.1, "measure", "100", 42)
        self.assertEqual(result["counts"]["scheduled"], 110)
        self.assertLess(result["elapsed_seconds"], 1.4)
        self.assertGreaterEqual(len(arrivals), 2)
        self.assertEqual(arrivals[-1]["scheduled"], 110)
        self.assertEqual(arrivals[-1]["final"], "true")
        self.assertEqual(arrivals[-1]["latency_count"], result["counts"]["completed"])
        self.assertEqual(arrivals[0]["mode"], "direct")
        self.assertEqual(arrivals[0]["reset"], "true")
        self.assertEqual(arrivals[-1]["reset"], "false")
        self.assertEqual(sorted(item["scheduled"] for item in arrivals),
                         [item["scheduled"] for item in arrivals])

    def test_optional_admin_token_only_on_control_posts(self):
        response = MagicMock()
        response.__enter__.return_value.status = 200
        response.__enter__.return_value.read.return_value = b'{"ready": true}'
        with patch.dict(os.environ, {"ADMIN_TOKEN": "test-only"}):
            with patch.object(runner.urllib.request, "urlopen", return_value=response) as opened:
                runner.request("http://app:8080", "/context", 1, method="POST")
                self.assertEqual(opened.call_args.args[0].get_header("Authorization"),
                                 "Bearer " + os.environ["ADMIN_TOKEN"])
                runner.request("http://app:8080", "/ready", 1)
                self.assertIsNone(opened.call_args.args[0].get_header("Authorization"))

    def test_application_admission_overrides_http_acceptance(self):
        args = runner.arguments([])
        for status, admitted in ((200, False), (500, True)):
            with self.subTest(status=status), patch.object(runner, "request", return_value=(
                    status, {"success": False, "operation": "browse", "accepted": admitted}, "")):
                result = runner.work(args, "browse", 1, "measure", "1")
                self.assertEqual(result["accepted"], admitted)
                self.assertFalse(result["sql_completed"])

    def test_warmup_only_never_enters_measurement(self):
        events = []

        def fake_request(url, path, timeout, method="GET", query=None):
            events.append((path, dict(query or {})))
            if path == "/work":
                return 200, {"success": True, "operation": query["op"]}, ""
            return 200, {"ready": True}, ""

        args = runner.arguments(["--warmup-only", "--warmup", ".01", "--rps", "100"])
        with patch.object(runner, "request", side_effect=fake_request):
            result = runner.run_step(args, "direct", 100, 0)
        self.assertNotIn("error", result)
        self.assertNotIn("measure", result)
        self.assertNotIn("measurement_reset_response", result)
        self.assertTrue(result["warmup_only"])
        self.assertEqual(result["warmup"]["counts"]["sql_completed"], 1)
        self.assertEqual(result["warmup"]["counts"]["in_flight"], 0)
        self.assertTrue(result["warmup"]["telemetry"]["final_sent"])
        self.assertTrue(all(query.get("phase") != "measure" for _, query in events))
        self.assertIn("readiness_after", result)

    def test_failed_final_telemetry_warns_on_stderr(self):
        args = runner.arguments([])
        stderr = io.StringIO()
        with patch.object(runner, "request", side_effect=OSError), redirect_stderr(stderr):
            result = runner.run_phase(args, 1, 0, "warmup", "1", 1)
        self.assertFalse(result["telemetry"]["final_sent"])
        self.assertIn("WARNING: final /arrivals publication failed", stderr.getvalue())

    def test_telemetry_success_counter_excludes_failed_http_completions(self):
        arrivals = []

        def fake_request(url, path, timeout, method="GET", query=None):
            if path == "/arrivals":
                arrivals.append(dict(query))
                return 200, {"success": True}, ""
            return 202, {"accepted": True}, ""

        args = runner.arguments([])
        with patch.object(runner, "request", side_effect=fake_request):
            result = runner.run_phase(args, 1, .001, "measure", "1", 1, mode="ojp")
        self.assertEqual(result["counts"]["completed"], 1)
        self.assertEqual(arrivals[-1]["completed"], 0)
        self.assertEqual(arrivals[-1]["http_completed"], 1)
        self.assertEqual(arrivals[-1]["errors"], 1)
        self.assertEqual(arrivals[-1]["mode"], "ojp")
        self.assertEqual(arrivals[-1]["sql_completed"], 0)

    def test_repeated_phase_resets_zero_counters_before_work(self):
        events = []
        previous = {}

        def fake_request(url, path, timeout, method="GET", query=None):
            events.append((path, dict(query)))
            if path != "/arrivals":
                return 200, {"success": True, "op": query["op"]}, ""
            if query["reset"] == "true":
                previous.clear()
                self.assertEqual(query["scheduled"], 0)
                self.assertEqual(query["completed"], 0)
            for key in ("scheduled", "dispatched", "completed", "errors"):
                self.assertGreaterEqual(query[key], previous.get(key, 0))
                previous[key] = query[key]
            self.assertLessEqual(query["completed"] + query["errors"], query["dispatched"])
            return 200, {"success": True}, ""

        args = runner.arguments([])
        with patch.object(runner, "request", side_effect=fake_request):
            for _ in range(2):
                result = runner.run_phase(args, 1, .001, "measure", "1", 1, mode="ojp")
                self.assertTrue(result["telemetry"]["final_sent"])
                self.assertFalse(result["telemetry"]["errors"])
        self.assertEqual([path for path, _ in events],
                         ["/arrivals", "/work", "/arrivals"] * 2)

    def test_zero_sql_is_exportable_error_and_nonzero_exit(self):
        def fake_request(url, path, timeout, method="GET", query=None):
            if path == "/work":
                return 503, {"success": False}, ""
            return 200, {"ready": True}, ""

        for extra in ([], ["--warmup-only", "--warmup", ".001"]):
            args = runner.arguments(["--mode", "direct", "--rps", "1",
                                     "--duration", ".001", "--warmup", "0", *extra])
            with patch.object(runner, "request", side_effect=fake_request), \
                    patch.object(runner, "readiness", return_value={}):
                result = runner.run_step(args, "direct", 1, 0)
            phase = "warmup" if args.warmup_only else "measure"
            self.assertIn("zero SQL", result["error"])
            self.assertEqual(result[phase]["counts"]["failed_transactions"], 1)
            self.assertEqual(result[phase]["error_fraction"], 1)
            with patch.object(runner, "run_step", return_value=result), \
                    patch.object(runner, "export", return_value=runner.Path("unused.json")), \
                    redirect_stdout(io.StringIO()):
                self.assertEqual(runner.main(["--mode", "direct", "--rps", "1"]), 1)

    def test_zero_length_warmup_only_is_not_all_failed(self):
        args = runner.arguments(["--warmup-only", "--warmup", "0"])
        with patch.object(runner, "request", return_value=(200, {"ready": True}, "")), \
                patch.object(runner, "readiness", return_value={}):
            result = runner.run_step(args, "direct", 1, 0)
        self.assertNotIn("error", result)
        self.assertEqual(result["warmup"]["counts"]["scheduled"], 0)
        self.assertEqual(result["warmup"]["throughput"]["offered_per_second"], 0)

    def test_transient_failures_with_sql_success_are_allowed(self):
        work_count = 0

        def fake_request(url, path, timeout, method="GET", query=None):
            nonlocal work_count
            if path == "/work":
                work_count += 1
                if work_count == 1:
                    return 503, {"success": False}, ""
                return 200, {"success": True, "op": query["op"]}, ""
            return 200, {"ready": True}, ""

        args = runner.arguments(["--warmup", "0", "--duration", ".02", "--rps", "100"])
        with patch.object(runner, "request", side_effect=fake_request), \
                patch.object(runner, "readiness", return_value={}):
            result = runner.run_step(args, "ojp", 100, 0)
        self.assertNotIn("error", result)
        self.assertEqual(result["measure"]["counts"]["sql_completed"], 1)
        self.assertEqual(result["measure"]["counts"]["failed_transactions"], 1)

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
            "browse=40,history=25,checkout=20,review=10,account=5,report=0")
        for mix in invalid:
            with self.subTest(mix=mix), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    runner.arguments(["--operation-mix", mix])

    def test_custom_operation_mix_and_csv_preserve_zero_weight_operations(self):
        mix = "browse=0,history=0,checkout=0,review=0,account=0,report=100"
        args = runner.arguments(["--operation-mix", mix, "--oltp-percent", "0",
                                 "--olap-percent", "100", "--warmup-only",
                                 "--warmup", ".01", "--rps", "100"])

        def fake_request(url, path, timeout, method="GET", query=None):
            if path == "/work":
                self.assertEqual(query["op"], "report")
                return 200, {"success": True, "op": query["op"]}, ""
            return 200, {"ready": True}, ""

        with patch.object(runner, "request", side_effect=fake_request), \
                patch.object(runner, "readiness", return_value={}):
            result = runner.run_step(args, "direct", 100, 0)
        self.assertNotIn("error", result)
        self.assertEqual(result["operation_weights"], args.operation_mix)
        self.assertEqual(result["warmup"]["operations"]["report"]["scheduled"], 1)
        self.assertEqual(result["warmup"]["operations"]["browse"]["scheduled"], 0)
        csv_output = io.StringIO()
        csv_context = MagicMock()
        csv_context.__enter__.return_value = csv_output
        with patch.object(runner.Path, "mkdir"), patch.object(runner.Path, "write_text"), \
                patch.object(runner.os, "replace"), \
                patch.object(runner.Path, "open", return_value=csv_context):
            runner.export(args, result, 1)
        rows = {row["operation"]: row for row in csv.DictReader(io.StringIO(csv_output.getvalue()))}
        aggregate_fields = [*result["warmup"]["throughput"], "error_fraction", "drop_fraction"]
        self.assertEqual(float(rows["all"]["offered_per_second"]), 100)
        self.assertGreater(float(rows["all"]["successful_completions_per_second"]), 0)
        self.assertEqual(rows["browse"]["scheduled"], "0")
        for op in runner.OPERATIONS:
            for field in aggregate_fields:
                self.assertEqual(rows[op][field], "", (op, field))

    def test_duplicate_run_id_refused_before_requests_or_export(self):
        existing_file = MagicMock()
        existing_file.is_file.return_value = True
        for extension in ("json", "csv"):
            stderr = io.StringIO()

            def existing_results(pattern):
                return [existing_file] if pattern == f"reused_run-*.{extension}" else []

            with self.subTest(extension=extension), \
                    patch.object(runner.Path, "glob", side_effect=existing_results), \
                    patch.object(runner, "run_step") as run_step, \
                    patch.object(runner, "export") as export, redirect_stderr(stderr):
                status = runner.main(["--mode", "direct", "--run-id", "reused/run", "--output", "results"])
            self.assertEqual(status, 2)
            self.assertIn("choose a unique --run-id", stderr.getvalue())
            run_step.assert_not_called()
            export.assert_not_called()

    def test_ojp_htap_runner_refuses_unverified_segregation(self):
        with patch.dict(os.environ, {"OJP_SLOW_QUERY_SEGREGATION_VERIFIED": "false"}), \
                patch.object(runner.Path, "glob", return_value=[]), \
                patch.object(runner, "run_step") as run_step, \
                redirect_stderr(io.StringIO()) as stderr:
            self.assertEqual(runner.main(["--rps", "1"]), 2)
        self.assertIn("requires scripts/htap.sh", stderr.getvalue())
        run_step.assert_not_called()

    def test_preexisting_sampler_directory_does_not_block_run(self):
        existing_directory = MagicMock()
        existing_directory.is_file.return_value = False
        with patch.object(runner.Path, "glob", return_value=[existing_directory]), \
                patch.object(runner, "run_step", return_value={}) as run_step, \
                patch.object(runner, "export", return_value=runner.Path("unused.json")), \
                redirect_stdout(io.StringIO()):
            status = runner.main(["--run-id", "sampler", "--mode", "direct", "--rps", "1"])
        self.assertEqual(status, 0)
        run_step.assert_called_once()

    def test_both_modes_are_attempted_after_direct_level_failure(self):
        calls = []

        def run_step(args, mode, rps, index):
            calls.append((mode, rps))
            return {"error": "direct overload"} if mode == "direct" else {}

        with patch.object(runner.Path, "glob", return_value=[]), \
                patch.object(runner, "run_step", side_effect=run_step), \
                patch.object(runner, "export", return_value=runner.Path("unused.json")), \
                patch.dict(os.environ, {"OJP_SLOW_QUERY_SEGREGATION_VERIFIED": "true"}), \
                redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            status = runner.main(["--mode", "both", "--rps", "7", "--output", "unused"])
        self.assertEqual(status, 1)
        self.assertEqual(calls, [("direct", 7), ("ojp", 7)])


if __name__ == "__main__":
    unittest.main()
