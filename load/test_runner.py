"""Run with: python -m unittest discover -s load -v."""

from collections import Counter
from contextlib import redirect_stderr
import http.server
import io
import itertools
import json
import os
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


class RunnerTests(unittest.TestCase):
    def test_validation(self):
        for argv in (["--rps", "0"], ["--rps", "1,,2"], ["--rps", "nan"],
                     ["--rps", "inf"], ["--duration", "-1"], ["--warmup", "-1"],
                     ["--max-in-flight", "0"], ["--request-timeout", "0"],
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
        self.assertEqual(defaults.rps, [8, 16, 32, 64, 128, 256])
        self.assertEqual((defaults.warmup, defaults.duration, defaults.max_in_flight,
                          defaults.seed), (30, 60, 512, 12345))
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
        self.assertEqual(events[measure_context + 2][1], "/work")
        self.assertEqual(result["readiness_after"]["response"]["active_connections"], 2)
        arrivals = [event[2] for event in events if event[1] == "/arrivals"]
        self.assertEqual(len(arrivals), 2)
        self.assertTrue(result["measure"]["telemetry"]["final_sent"])
        self.assertEqual(arrivals[-1]["scheduled"], 1)
        self.assertEqual(arrivals[-1]["inflight"], 0)
        self.assertEqual(arrivals[-1]["latency_count"], 1)
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


if __name__ == "__main__":
    unittest.main()
