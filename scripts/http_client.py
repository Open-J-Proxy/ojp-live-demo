#!/usr/bin/env python3
"""HTTP controls; internal stdio transport supports isolated ephemeral-port tests."""
import argparse
from concurrent.futures import Future
import itertools
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import urllib.request
from compose_command import compose_command

CLIENT = r'''
from concurrent.futures import ThreadPoolExecutor
import json, sys, threading, urllib.request, urllib.error
lock = threading.Lock()
def request(message):
    try:
        payload = message.get("data")
        data = None if payload is None else json.dumps(payload).encode()
        req = urllib.request.Request(message["url"], data=data, method=message.get("method"),
                                     headers=message.get("headers", {}))
        try:
            with urllib.request.urlopen(req, timeout=message["timeout"]) as response:
                result = {"status": response.status, "body": response.read().decode()}
        except urllib.error.HTTPError as error:
            result = {"status": error.code, "body": error.read().decode()}
    except Exception as error:
        result = {"error": str(error)}
    result["id"] = message["id"]
    with lock:
        print(json.dumps(result), flush=True)
with ThreadPoolExecutor(max_workers=256) as executor:
    for line in sys.stdin:
        executor.submit(request, json.loads(line))
'''


class HTTPBridge:
    def __init__(self, env=None):
        env = os.environ if env is None else env
        self.name = env.get("COMPOSE_PROJECT_NAME", "ojp-demo") + "-http-" + str(os.getpid())
        output = Path(env.get("RESULTS_DIR", Path(__file__).resolve().parent.parent / "results"))
        output.mkdir(parents=True, exist_ok=True)
        self.log = (output / ("http-client-" + str(os.getpid()) + ".log")).open("w")
        command = compose_command(env) + [
            "--profile", "tools", "run", "--rm", "--no-deps", "-T", "--name",
            self.name, "--entrypoint", "python", "load", "-u", "-c", CLIENT]
        self.process = subprocess.Popen(command, env=env, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=self.log,
                                        text=True, bufsize=1)
        self.lock = threading.Lock()
        self.pending = {}
        self.ids = itertools.count()
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()

    def read(self):
        failure = RuntimeError("Internal HTTP client exited; inspect http-client log")
        try:
            for line in self.process.stdout:
                try:
                    result = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(result, dict) or "id" not in result:
                    continue
                with self.lock:
                    future = self.pending.pop(result["id"], None)
                if future:
                    future.set_result(result)
        except Exception as error:
            failure = error
        with self.lock:
            pending, self.pending = self.pending, {}
        for future in pending.values():
            future.set_exception(failure)

    def request(self, url, data=None, method=None, timeout=60, headers=None):
        future = Future()
        with self.lock:
            if self.process.poll() is not None:
                raise RuntimeError("Internal HTTP client is not running")
            request_id = next(self.ids)
            self.pending[request_id] = future
            self.process.stdin.write(json.dumps({
                "id": request_id, "url": url, "data": data, "method": method,
                "timeout": timeout, "headers": headers or {},
            }) + "\n")
            self.process.stdin.flush()
        result = future.result(timeout=timeout + 60)
        if "error" in result:
            raise OSError(result["error"])
        body = result["body"]
        try:
            body = json.loads(body)
        except ValueError:
            pass
        return result["status"], body

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait()
        subprocess.run(["docker", "rm", "-f", self.name], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, check=False)
        self.reader.join(timeout=5)
        self.log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("ready", "reset", "verify"))
    parser.add_argument("--url", required=True)
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    bridge = None
    if os.environ.get("APP_PORT") == "0" and not os.environ.get("APP_URL"):
        bridge = HTTPBridge()
        args.url = os.environ.get("APP_INTERNAL_URL", "http://app:8080")

    def call(path, post=False, timeout=120):
        headers = {}
        if post and os.environ.get("ADMIN_TOKEN"):
            headers["Authorization"] = "Bearer " + os.environ["ADMIN_TOKEN"]
        if bridge:
            status, body = bridge.request(args.url + path, {} if post else None,
                                           "POST" if post else "GET", timeout, headers)
            if status != 200:
                raise RuntimeError(f"HTTP {status}: {body}")
            return body
        request = urllib.request.Request(args.url + path, data=b"" if post else None, headers=headers)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode()
            try:
                return json.loads(raw)
            except ValueError:
                return raw

    try:
        if args.command == "ready":
            deadline, last = time.monotonic() + args.timeout, ""
            while time.monotonic() < deadline:
                try:
                    if call("/ready", timeout=5).get("ready") is True:
                        return
                except Exception as error:
                    last = str(error)
                time.sleep(.5)
            raise SystemExit(f"Application readiness timed out: {last}")
        if args.command == "reset":
            print(json.dumps(call("/reset", True)))
        else:
            mode = call("/ready").get("mode", "direct")
            try:
                for selected in ("direct", "ojp"):
                    call("/mode?value=" + selected, True)
                    result = call("/work?op=browse&seed=12345&phase=warmup&scenario=integration&level=1")
                    if not result.get("sql_completed", result.get("completed", False)):
                        raise RuntimeError(result)
                    print(selected + ": SQL query succeeded")
            finally:
                call("/mode?value=" + mode, True)
    finally:
        if bridge:
            bridge.close()


if __name__ == "__main__":
    main()
