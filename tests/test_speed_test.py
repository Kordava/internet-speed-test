"""Tests use a local HTTP server; no internet access is required."""

import contextlib
import io
import itertools
import json
import os
import re
import signal
import socket
import socketserver
import ssl
import struct
import subprocess
import sys
import tempfile
import time
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import get_args
from unittest import mock

import speed_test

from speed_test import REQUEST_COUNT, main, measure, positive_seconds

PAYLOAD = b"0123456789abcdef" * 4096  # 65,536 bytes


class TestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.server.request_count += 1
        if self.path == "/error":
            self.send_error(503)
            return

        body = b"" if self.path == "/empty" else PAYLOAD
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class SpeedTestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), TestHandler)
        cls.server.request_count = 0
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def setUp(self):
        self.server.request_count = 0

    def test_ten_complete_downloads_and_correct_summary(self):
        with contextlib.redirect_stdout(io.StringIO()):
            result = measure(self.base_url + "/payload")

        self.assertEqual(self.server.request_count, REQUEST_COUNT)
        self.assertEqual(len(result.downloads), REQUEST_COUNT)
        self.assertTrue(all(item.bytes_downloaded == len(PAYLOAD) for item in result.downloads))
        self.assertEqual(result.total_bytes, REQUEST_COUNT * len(PAYLOAD))
        self.assertGreater(result.total_seconds, 0)
        self.assertAlmostEqual(result.average_seconds, result.total_seconds / REQUEST_COUNT)
        self.assertAlmostEqual(
            result.speed_mbps, result.total_bytes * 8 / result.total_seconds / 1_000_000
        )

    def test_empty_response_measures_zero_speed(self):
        with contextlib.redirect_stdout(io.StringIO()):
            result = measure(self.base_url + "/empty")
        self.assertEqual(result.total_bytes, 0)
        self.assertEqual(result.speed_mbps, 0)
        self.assertEqual(self.server.request_count, REQUEST_COUNT)

    def test_http_error_stops_the_series(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            exit_code = main([self.base_url + "/error"])
        self.assertEqual(exit_code, 1)
        self.assertEqual(self.server.request_count, 1)
        self.assertIn("Request 1/10 failed", stderr.getvalue())

    def test_non_http_url_is_rejected(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                main(["ftp://example.com/file"])
        self.assertEqual(raised.exception.code, 2)

    def test_timeout_must_be_positive(self):
        for value in ("0", "-3", "nan", "inf", "abc"):
            with self.subTest(value=value), self.assertRaises(Exception):
                positive_seconds(value)


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"


def framed(framing, body=b"abc"):
    if framing == "fixed":
        return f"Content-Length: {len(body)}\r\n".encode(), body
    if framing == "chunked":
        return b"Transfer-Encoding: chunked\r\n", (
            (f"{len(body):x}\r\n".encode() + body + b"\r\n") if body else b""
        ) + b"0\r\n\r\n"
    return b"", body


def response(sock, headers=b"Content-Length: 3\r\n", body=b"abc", status=200):
    sock.sendall(f"HTTP/1.1 {status} Test\r\n".encode() + headers + b"\r\n" + body)


class RawHandler(socketserver.BaseRequestHandler):
    def handle(self):
        server = self.server
        sock = self.request
        sock.settimeout(3)
        try:
            if server.tls:
                sock = server.tls.wrap_socket(sock, server_side=True)
            with sock:
                request = b""
                while b"\r\n\r\n" not in request:
                    data = sock.recv(4096)
                    if not data:
                        return
                    request += data
                path = request.split(b" ")[1].decode("ascii")
                with server.lock:
                    server.paths.append(path)
                    index = len(server.paths)
                    server.active += 1
                    server.maximum_active = max(server.maximum_active, server.active)
                try:
                    server.scenario(sock, index, path, server)
                finally:
                    with server.lock:
                        server.active -= 1
        except (OSError, ssl.SSLError):
            pass  # The client deliberately closes timed-out/rejected responses.


class RawServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, scenario, tls=False):
        super().__init__(("127.0.0.1", 0), RawHandler)
        self.scenario = scenario
        self.tls = None
        if tls:
            self.tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            self.tls.load_cert_chain(FIXTURES / "server.pem", FIXTURES / "server-key.pem")
        self.paths = []
        self.errors = []
        self.active = 0
        self.maximum_active = 0
        self.lock = threading.Lock()
        self.ready = threading.Event()
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.serve_forever, kwargs={"poll_interval": .02})
        self.thread.start()
        self.url = f"{'https' if tls else 'http'}://127.0.0.1:{self.server_address[1]}"

    def handle_error(self, request, client_address):
        self.errors.append(sys.exc_info()[1])

    def __exit__(self, *args):
        self.stop.set()
        self.shutdown()
        self.server_close()
        self.thread.join(2)
        if self.errors and args[0] is None:
            raise self.errors[0]


def process_alive(pid):
    # Linux allows checking even descendants after the CLI parent has exited.
    try:
        return Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1][0] != "Z"
    except (FileNotFoundError, ProcessLookupError):
        return False


class CLIHarness(unittest.TestCase):
    json_mode = False
    command = (sys.executable, "-u", str(ROOT / "speed_test.py"))
    cwd = None

    def cli(self, server, args=(), cancel=False, env_extra=None, trust=True):
        env = os.environ.copy()
        for key in list(env):
            if key.lower().endswith("_proxy") or key in ("SSL_CERT_FILE", "SSL_CERT_DIR"):
                env.pop(key)
        env.pop("PYTHONPATH", None)
        env.pop("PYTHONHOME", None)
        env.update(NO_PROXY="*", PYTHONDONTWRITEBYTECODE="1")
        if trust:
            env["SSL_CERT_FILE"] = str(FIXTURES / "ca.pem")
        env.update(env_extra or {})
        process = subprocess.Popen(
            [*self.command, server.url, *args,
             *(["--json"] if self.json_mode else [])],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env, cwd=self.cwd,
            start_new_session=os.name == "posix",
        )
        pids = set()
        roles = {}
        watching = threading.Event()

        def observe_children():
            path = Path(f"/proc/{process.pid}/task/{process.pid}/children")
            while not watching.wait(.01):
                try:
                    for pid in map(int, path.read_text().split()):
                        pids.add(pid)
                        try:
                            roles[pid] = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode()
                        except (FileNotFoundError, ProcessLookupError):
                            pass
                except (FileNotFoundError, ProcessLookupError):
                    return

        watcher = threading.Thread(target=observe_children)
        watcher.start()
        started = time.monotonic()
        try:
            if cancel:
                self.assertTrue(server.ready.wait(5), "server never reached interrupt point")
                time.sleep(.15)  # Let a complete progress frame reach the parent.
                os.killpg(process.pid, signal.SIGINT)
            stdout, stderr = process.communicate(timeout=15)
        finally:
            if process.poll() is None:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
                process.communicate(timeout=3)
            watching.set()
            watcher.join(2)
            server.stop.set()
        # The resource tracker may be exiting just after its parent. Bound the
        # observation, then reject any remaining non-zombie descendant.
        until = time.monotonic() + 2
        while any(process_alive(pid) for pid in pids) and time.monotonic() < until:
            time.sleep(.01)
        for pid in pids:
            self.assertFalse(process_alive(pid), f"live child {pid} after CLI exit: {roles.get(pid)}")
        self.assertNotIn("Traceback", stdout + stderr)
        if self.json_mode:
            self.assert_json_report(stdout, process.returncode, stderr)
        return process.returncode, stdout, stderr, time.monotonic() - started

    def assert_json_report(self, out, code, err):
        report = json.loads(out)  # Reject extra documents and console output.
        self.assertTrue(out.endswith("\n"))
        self.assertEqual(set(report), {"schema_version", "url", "status", "requested_count",
                                      "completed_count", "downloads", "statistics", "partial",
                                      "error", "warnings"})
        self.assertEqual(report["schema_version"], 1)
        self.assertEqual(report["status"], "completed" if code == 0 else "incomplete")
        items = report["downloads"]
        self.assertEqual(report["completed_count"], len(items))
        self.assertEqual([item["number"] for item in items], list(range(1, len(items) + 1)))
        times = sorted(item["elapsed_seconds"] for item in items)
        stats = report["statistics"]
        self.assertEqual(stats["total_bytes"], sum(item["bytes_downloaded"] for item in items))
        self.assertAlmostEqual(stats["total_seconds"], sum(times))
        if items:
            n = len(times)
            self.assertAlmostEqual(stats["average_seconds"], sum(times) / n)
            self.assertEqual(stats["min_seconds"], times[0])
            self.assertEqual(stats["max_seconds"], times[-1])
            self.assertEqual(stats["median_seconds"], (times[(n - 1) // 2] + times[n // 2]) / 2)
            expected_speed = stats["total_bytes"] / sum(times) / 125000 if sum(times) else 0
            self.assertAlmostEqual(stats["speed_mbps"], expected_speed)
        else:
            self.assertEqual(stats, dict(total_bytes=0, total_seconds=0, average_seconds=None,
                                         min_seconds=None, median_seconds=None, max_seconds=None,
                                         speed_mbps=None))
        short = bool(items) and stats["average_seconds"] < 1
        self.assertEqual(report["warnings"], ["short_measurement"] if short else [])
        self.assertEqual(err.count("Warning:"), int(short))
        if code == 0:
            self.assertIsNone(report["error"])
            self.assertIsNone(report["partial"])
            self.assertEqual(len(items), report["requested_count"])
        else:
            self.assertIn(report["error"]["reason"], err)
            self.assertEqual(code, 130 if report["error"]["reason"] == "cancelled" else 1)
        for item in items:
            self.assertIs(type(item["bytes_downloaded"]), int)
        partial = report["partial"]
        if partial is not None:
            self.assertIs(type(partial["exact_bytes"]), bool)
            self.assertIs(type(partial["bytes_downloaded"]), int)
        self.assertNotRegex(out, r"\b(?:NaN|Infinity)\b")
        return report

    def assert_total(self, out, total):
        if self.json_mode:
            self.assertEqual(json.loads(out)["statistics"]["total_bytes"], total)
        else:
            self.assertIn(f"Total downloaded:     {total:,} bytes", out)

    def assert_complete(self, out):
        if self.json_mode:
            self.assertEqual(json.loads(out)["status"], "completed")
        else:
            self.assertIn("Results", out)

    def assert_stopped(self, result, reason, completed, partial=3, forced=False, count=10):
        code, out, err, duration = result
        self.assertEqual(code, 130 if reason == "cancelled" else 1, (out, err))
        self.assertIn(reason, err)
        self.assertNotIn("Results", out)
        self.assertNotIn("Download speed:", out)
        self.assertIn(f"Completed: {completed}/{count} requests, {completed * 3} bytes,", err)
        self.assertEqual(len(re.findall(rf"Request \d\d/{count}:", err)), completed)
        prefix = "at least " if forced else ""
        match = re.search(rf"Partial request {completed + 1}: {prefix}([\d,]+) bytes", err)
        self.assertIsNotNone(match, err)
        partial_bytes = int(match[1].replace(",", ""))
        if forced:
            self.assertGreaterEqual(partial_bytes, partial)
        else:
            self.assertEqual(partial_bytes, partial)
        self.assertLess(duration, 8)
        if self.json_mode:
            report = json.loads(out)
            self.assertEqual(report["error"]["reason"], reason)
            self.assertEqual(report["completed_count"], completed)
            self.assertEqual(report["requested_count"], count)
            self.assertEqual(report["partial"]["number"], completed + 1)
            self.assertEqual(report["partial"]["exact_bytes"], not forced)
            self.assertEqual(report["partial"]["bytes_downloaded"], int(match[1].replace(",", "")))
        else:
            self.assertIn("Series incomplete", out)
            self.assertIn("Completed requests only", out)
            self.assertIn("Completed-only speed:", out)

class CLIReliabilityTests(CLIHarness):
    def test_complete_http_https_domain(self):
        for tls, framing, body in itertools.product((False, True), ("fixed", "chunked", "eof"), (b"", b"abc")):
            with self.subTest(tls=tls, framing=framing, body=body):
                def scenario(sock, index, path, server):
                    response(sock, *framed(framing, body))
                with RawServer(scenario, tls) as server:
                    code, out, err, _ = self.cli(server)
                    self.assertEqual(code, 0, err)
                    self.assertEqual(server.paths, ["/"] * 10)
                    self.assertEqual(server.maximum_active, 1)
                    self.assert_total(out, len(body) * 10)
                    self.assertEqual(len(re.findall(r"Request \d\d/10:", err)), 10)
                    self.assert_complete(out)

    def test_stop_http_https_domain(self):
        for tls, framing, cause, completed in itertools.product(
            (False, True), ("fixed", "chunked", "eof"), ("idle", "deadline", "cancel"), (0, 2),
        ):
            if cause == "cancel" and os.name != "posix":
                continue  # Real terminal group SIGINT is exercised on POSIX.
            with self.subTest(tls=tls, framing=framing, cause=cause, completed=completed):
                def scenario(sock, index, path, server):
                    if index <= completed:
                        response(sock, *framed(framing))
                        return
                    headers = {"fixed": b"Content-Length: 1000\r\n", "chunked": b"Transfer-Encoding: chunked\r\n", "eof": b""}[framing]
                    response(sock, headers, b"3e8\r\nabc" if framing == "chunked" else b"abc")
                    server.ready.set()
                    if cause == "deadline":
                        while not server.stop.wait(.06):
                            sock.sendall(b"x")
                    else:
                        server.stop.wait(4)
                with RawServer(scenario, tls) as server:
                    args = ["--timeout", ".3", "--download-timeout", "3" if cause == "idle" else ".9"]
                    result = self.cli(server, args, cancel=cause == "cancel")
                    reason = {"idle": "io_timeout", "deadline": "download_timeout", "cancel": "cancelled"}[cause]
                    self.assert_stopped(result, reason, completed, forced=cause != "idle")
                    self.assertEqual(len(server.paths), completed + 1)
                    self.assertEqual(server.maximum_active, 1)

    def test_truncated_responses_at_both_positions(self):
        broken = [
            (b"Content-Length: 8\r\n", b"abc"),
            (b"Transfer-Encoding: chunked\r\n", b"8\r\nabc"),
            (b"Transfer-Encoding: chunked\r\n", b"3\r\nabc\r\n"),
            (b"Transfer-Encoding: chunked\r\n", b"3\r\nabc\r\n0\r\n"),
            (b"Transfer-Encoding: chunked\r\n", b"3\r\nabc\r"),
        ]
        for tls, completed, (headers, body) in itertools.product((False, True), (0, 2), broken):
            with self.subTest(tls=tls, completed=completed, body=body):
                def scenario(sock, index, path, server):
                    response(sock) if index <= completed else response(sock, headers, body)
                with RawServer(scenario, tls) as server:
                    self.assert_stopped(self.cli(server), "incomplete_response", completed)
                    self.assertEqual(len(server.paths), completed + 1)

    def test_protocol_errors(self):
        chunked = b"Transfer-Encoding: chunked\r\n"
        cases = [
            (b"Content-Length: -1\r\n", b"", 0),
            (b"Content-Length: bad\r\n", b"", 0),
            (b"Content-Length: 1, 2\r\n", b"abc", 0),
            (b"Content-Length: 1\r\nContent-Length: 2\r\n", b"abc", 0),
            (b"Content-Length: 3,\r\n", b"abc", 0),
            (chunked + b"Content-Length: 3\r\n", b"", 0),
            (b"Transfer-Encoding: gzip\r\n", b"", 0),
            (b"Transfer-Encoding: gzip, chunked\r\n", b"", 0),
            (chunked, b"-1\r\n", 0), (chunked, b"+1\r\n", 0),
            (chunked, b"1\n", 0), (chunked, b"Z\r\n", 0),
            (chunked, b"3\r\nabcXX", 3),
            (chunked, b"0\r\nNot-a-header\r\n\r\n", 0),
            (chunked, b"0\r\n" + b"X: a\r\n" * 101 + b"\r\n", 0),
            (chunked, b"1;" + b"x" * 65535 + b"\r\n", 0),
        ]
        for tls, (headers, body, count) in itertools.product((False, True), cases):
            with self.subTest(tls=tls, headers=headers, body_prefix=body[:40]):
                with RawServer(lambda s, i, p, srv: response(s, headers, body), tls) as server:
                    self.assert_stopped(self.cli(server), "protocol_error", 0, partial=count)
                    self.assertEqual(len(server.paths), 1)

    def test_valid_framing_variations_and_statuses(self):
        cases = [
            (b"Content-Length: 3, 03\r\nContent-Length: 3\r\n", b"abc", 200, 3),
            (b"Transfer-Encoding: Chunked \r\n", b"3;x=y\r\nabc\r\n0\r\nX: a\r\n\r\n", 200, 3),
            (b"Transfer-Encoding: chunked\r\n", b"0\r\n" + b"X: a\r\n" * 100 + b"\r\n", 200, 0),
            (b"", b"", 204, 0),
            (b"Content-Length: 3\r\n", b"abc", 206, 3),
            (b"Content-Length: 3\r\nContent-Encoding: gzip\r\n", b"abc", 200, 3),
        ]
        for tls, (headers, body, status, count) in itertools.product((False, True), cases):
            with self.subTest(tls=tls, status=status, headers=headers):
                with RawServer(lambda s, i, p, srv: response(s, headers, body, status), tls) as server:
                    code, out, err, _ = self.cli(server)
                    self.assertEqual(code, 0, err)
                    self.assert_total(out, count * 10)

    def test_http_errors_keep_completed_prefix(self):
        for tls, completed, status in itertools.product((False, True), (0, 2), (304, 404, 503)):
            with self.subTest(tls=tls, completed=completed, status=status):
                def scenario(sock, index, path, server):
                    response(sock) if index <= completed else response(sock, status=status)
                with RawServer(scenario, tls) as server:
                    self.assert_stopped(self.cli(server), "http_error", completed, partial=0)
                    self.assertEqual(len(server.paths), completed + 1)

    def test_redirects_close_intermediate_body_and_normalize_location(self):
        for tls, status in itertools.product((False, True), (301, 302, 303, 307, 308)):
            with self.subTest(tls=tls, status=status):
                def scenario(sock, index, path, server):
                    if path == "/":
                        response(sock, b"Location: /new path\r\nContent-Length: 999999\r\n", b"", status)
                        # A redirect-body read would block here. Closing arrives promptly.
                        self.assertEqual(sock.recv(1), b"")
                    else:
                        response(sock)
                with RawServer(scenario, tls) as server:
                    code, out, err, _ = self.cli(server, ["--timeout", ".3"])
                    self.assertEqual(code, 0, err)
                    self.assertEqual(server.paths, ["/", "/new%20path"] * 10)
                    self.assert_total(out, 30)

    def test_redirect_limits_and_invalid_targets(self):
        for tls, target in itertools.product((False, True), (None, "ftp://example.com/file", "http://", "http://localhost:bad/", "/bad\x01", "/loop")):
            with self.subTest(tls=tls, target=target):
                headers = (b"Location: " + target.encode() + b"\r\n") if target else b""
                with RawServer(lambda s, i, p, srv: response(s, headers, b"", 302), tls) as server:
                    self.assert_stopped(self.cli(server), "protocol_error", 0, partial=0)
                    self.assertEqual(len(server.paths), 11 if target == "/loop" else 1)
        for tls in (False, True):
            def chain(sock, index, path, server):
                step = int(path.strip("/") or 0)
                if step < 10:
                    response(sock, f"Location: /{step + 1}\r\n".encode(), b"", 302)
                else:
                    response(sock)
            with RawServer(chain, tls) as server:
                code, out, err, _ = self.cli(server)
                self.assertEqual(code, 0, err)
                self.assertEqual(len(server.paths), 110)
                self.assert_total(out, 30)

    def test_redirect_time_is_measured_and_deadline_is_not_reset(self):
        for tls in (False, True):
            def scenario(sock, index, path, server):
                time.sleep(.12)
                if path == "/":
                    response(sock, b"Location: /final\r\n", b"", 302)
                else:
                    response(sock)
            with RawServer(scenario, tls) as server:
                code, out, err, _ = self.cli(server)
                self.assertEqual(code, 0, err)
                times = re.findall(r"Request \d\d/10: ([\d.]+) s", err)
                self.assertEqual(len(times), 10)
                self.assertTrue(all(float(value) >= .24 for value in times))
            def redirect_loop(sock, index, path, server):
                time.sleep(.12)
                response(sock, b"Location: /next\r\n", b"", 302)
            with RawServer(redirect_loop, tls) as server:
                self.assert_stopped(self.cli(server, ["--timeout", "1", "--download-timeout", ".5"]), "download_timeout", 0, partial=0, forced=True)
                self.assertGreater(len(server.paths), 1)
                self.assertLess(len(server.paths), 11)

    def test_cross_scheme_redirects_and_http_proxy(self):
        for tls in (False, True):
            with RawServer(lambda s, i, p, srv: response(s), not tls) as target:
                headers = f"Location: {target.url}/final\r\n".encode()
                with RawServer(lambda s, i, p, srv: response(s, headers, b"", 302), tls) as source:
                    code, out, err, _ = self.cli(source)
                    self.assertEqual(code, 0, err)
                    self.assertEqual(len(source.paths), 10)
                    self.assertEqual(target.paths, ["/final"] * 10)
        with RawServer(lambda s, i, p, srv: response(s)) as proxy:
            address = proxy.url
            proxy.url = "http://unresolvable.invalid/resource"
            code, out, err, _ = self.cli(proxy, env_extra={"http_proxy": address, "NO_PROXY": ""})
            self.assertEqual(code, 0, err)
            self.assertEqual(proxy.paths, ["http://unresolvable.invalid/resource"] * 10)

    def test_tls_verification_is_not_disabled(self):
        with RawServer(lambda s, i, p, srv: response(s), True) as server:
            self.assert_stopped(self.cli(server, trust=False), "network_error", 0, partial=0)
            self.assertEqual(server.paths, [])

    def test_deadline_covers_tls_headers_dns_and_connect(self):
        def no_headers(sock, index, path, server):
            server.stop.wait(3)
        with RawServer(no_headers) as server:
            self.assert_stopped(self.cli(server, ["--download-timeout", ".4"]), "download_timeout", 0, partial=0, forced=True)
        # A TCP listener that never replies to ClientHello stalls the TLS handshake.
        with RawServer(no_headers) as server:
            server.url = server.url.replace("http:", "https:")
            self.assert_stopped(self.cli(server, ["--download-timeout", ".4"]), "download_timeout", 0, partial=0, forced=True)
        for stage in ("dns", "connect"):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as temp:
                patch = "socket.getaddrinfo" if stage == "dns" else "socket.socket.connect"
                Path(temp, "sitecustomize.py").write_text(
                    f"import socket,time\ndef stall(*args, **kwargs):\n time.sleep(10)\n{patch}=stall\n"
                )
                with RawServer(no_headers) as server:
                    self.assert_stopped(self.cli(server, ["--download-timeout", ".4"], env_extra={"PYTHONPATH": temp}), "download_timeout", 0, partial=0, forced=True)
                    self.assertEqual(server.paths, [])

    def test_dns_and_connect_errors(self):
        with tempfile.TemporaryDirectory() as temp:
            Path(temp, "sitecustomize.py").write_text(
                "import socket\ndef fail(*a, **k):\n raise socket.gaierror(-2, 'name resolution failed')\nsocket.getaddrinfo=fail\n"
            )
            with RawServer(lambda s, i, p, srv: response(s)) as server:
                self.assert_stopped(self.cli(server, env_extra={"PYTHONPATH": temp}), "network_error", 0, partial=0)
        with RawServer(lambda s, i, p, srv: response(s)) as server:
            # Reserve but do not listen on a different local port.
            with socket.socket() as closed:
                closed.bind(("127.0.0.1", 0))
                server.url = f"http://127.0.0.1:{closed.getsockname()[1]}"
                self.assert_stopped(self.cli(server), "network_error", 0, partial=0)


class CLIControlsTests(CLIHarness):
    def test_complete_budget_domain(self):
        for tls, framing, count, mode, body in itertools.product(
            (False, True), ("fixed", "chunked", "eof"), (10, 3),
            ("none", "time", "bytes", "both"), (b"", b"abc"),
        ):
            with self.subTest(tls=tls, framing=framing, count=count, mode=mode, body=body):
                def scenario(sock, index, path, server):
                    response(sock, *framed(framing, body))
                args = [] if count == 10 else ["--count", str(count)]
                if mode in ("time", "both"):
                    args += ["--max-time", "10"]
                if mode in ("bytes", "both"):
                    args += ["--max-bytes", str(count * len(body) + 1)]
                with RawServer(scenario, tls) as server:
                    code, out, err, _ = self.cli(server, args)
                    self.assertEqual(code, 0, err)
                    self.assertEqual(server.paths, ["/"] * count)
                    self.assertEqual(server.maximum_active, 1)
                    self.assert_total(out, count * len(body))
                    self.assertEqual(len(re.findall(rf"Request \d\d/{count}:", err)), count)
                    self.assert_complete(out)

    def test_stopped_budget_domain(self):
        for tls, framing, completed, mode in itertools.product(
            (False, True), ("fixed", "chunked", "eof"), (0, 2),
            ("time", "bytes", "both_time", "both_bytes"),
        ):
            with self.subTest(tls=tls, framing=framing, completed=completed, mode=mode):
                def scenario(sock, index, path, server):
                    if index <= completed:
                        response(sock, *framed(framing))
                        return
                    headers = {"fixed": b"Content-Length: 1000000\r\n",
                               "chunked": b"Transfer-Encoding: chunked\r\n", "eof": b""}[framing]
                    response(sock, headers, b"f4240\r\nabc" if framing == "chunked" else b"abc")
                    while not server.stop.wait(.03):
                        sock.sendall(b"x")
                time_first = mode in ("time", "both_time")
                args = ["--count", "4", "--download-timeout", "5"]
                if mode != "bytes":
                    args += ["--max-time", "1.2" if time_first else "5"]
                if mode != "time":
                    args += ["--max-bytes", "100000000" if time_first else str(completed * 3 + 3)]
                with RawServer(scenario, tls) as server:
                    result = self.cli(server, args)
                    self.assert_stopped(result, "time_limit" if time_first else "byte_limit",
                                        completed, forced=time_first, count=4)
                    self.assertEqual(server.paths, ["/"] * (completed + 1))
                    self.assertEqual(server.maximum_active, 1)

    def test_byte_boundaries_and_block_sizes(self):
        for tls, framing, size, delta in itertools.product(
            (False, True), ("fixed", "chunked", "eof"), (3, 65539), (-1, 0, 1),
        ):
            with self.subTest(tls=tls, framing=framing, size=size, delta=delta):
                body = b"x" * size
                cap = size + delta
                def scenario(sock, index, path, server):
                    response(sock, *framed(framing, body))
                with RawServer(scenario, tls) as server:
                    code, out, err, _ = self.cli(server, ["--count", "1", "--max-bytes", str(cap)])
                    complete = delta > 0 or (delta == 0 and framing == "fixed")
                    self.assertEqual(code, 0 if complete else 1, (out, err))
                    self.assertEqual(server.paths, ["/"])
                    if complete:
                        self.assert_total(out, size)
                    else:
                        self.assertIn("byte_limit", err)
                        self.assertIn(f"Partial request 1: {cap:,} bytes", err)
                        self.assertIn("Completed: 0/1 requests", err)
                        self.assertNotIn("Results", out)

    def test_between_requests_and_empty_204(self):
        for tls in (False, True):
            with self.subTest(tls=tls):
                with RawServer(lambda s, i, p, srv: response(s), tls) as server:
                    code, out, err, _ = self.cli(server, ["--count", "3", "--max-bytes", "3"])
                    self.assertEqual(code, 1)
                    self.assertIn("Series stopped: byte_limit", err)
                    self.assertIn("Completed: 1/3 requests, 3 bytes", err)
                    self.assertNotIn("Partial request", err)
                    self.assertNotIn("Results", out)
                    self.assertEqual(server.paths, ["/"])
                with RawServer(lambda s, i, p, srv: response(s, b"", b"", 204), tls) as server:
                    code, out, err, _ = self.cli(server, ["--count", "3", "--max-bytes", "1"])
                    self.assertEqual(code, 0, err)
                    self.assertEqual(len(server.paths), 3)
                    self.assert_total(out, 0)

    def test_redirect_bodies_do_not_use_budget(self):
        for tls in (False, True):
            def scenario(sock, index, path, server):
                if path == "/":
                    response(sock, b"Location: /end\r\nContent-Length: 1000\r\n", b"x" * 1000, 302)
                else:
                    response(sock)
            with self.subTest(tls=tls), RawServer(scenario, tls) as server:
                code, out, err, _ = self.cli(server, ["--count", "1", "--max-bytes", "3"])
                self.assertEqual(code, 0, err)
                self.assert_total(out, 3)
                self.assertEqual(server.paths, ["/", "/end"])

    def test_errors_before_cap_and_custom_count_cancellation(self):
        cases = ((b"Content-Length: 10\r\n", b"ab", "incomplete_response", 2),
                 (b"Transfer-Encoding: chunked\r\n", b"2\r\nabXX", "protocol_error", 2),
                 (b"Content-Length: 3\r\n", b"abc", "cancelled", 3))
        for tls, (headers, body, reason, partial) in itertools.product((False, True), cases):
            if reason == "cancelled" and os.name != "posix":
                continue
            with self.subTest(tls=tls, reason=reason):
                def scenario(sock, index, path, server):
                    if index <= 2:
                        response(sock)
                        return
                    response(sock, b"Content-Length: 1000\r\n" if reason == "cancelled" else headers, body)
                    server.ready.set()
                    if reason == "cancelled":
                        server.stop.wait(4)
                with RawServer(scenario, tls) as server:
                    self.assert_stopped(self.cli(server, ["--count", "4", "--max-bytes", "100"],
                                                 cancel=reason == "cancelled"), reason, 2,
                                        partial=partial, forced=reason == "cancelled", count=4)
                    self.assertEqual(len(server.paths), 3)

    def test_global_deadline_covers_transport_setup(self):
        def stall(sock, index, path, server):
            server.stop.wait(3)
        for stage in ("headers", "tls", "dns", "connect"):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as temp:
                extra = {}
                if stage in ("dns", "connect"):
                    target = "socket.getaddrinfo" if stage == "dns" else "socket.socket.connect"
                    Path(temp, "sitecustomize.py").write_text(
                        f"import socket,time\ndef stall(*a, **k):\n time.sleep(10)\n{target}=stall\n"
                    )
                    extra["PYTHONPATH"] = temp
                with RawServer(stall) as server:
                    if stage == "tls":
                        server.url = server.url.replace("http:", "https:")
                    self.assert_stopped(self.cli(server, ["--max-time", ".4"], env_extra=extra),
                                        "time_limit", 0, partial=0, forced=True)
                    if stage in ("dns", "connect", "tls"):
                        self.assertEqual(server.paths, [])


def crash_worker(sender, url, timeout, max_bytes=None):
    os._exit(7)


def partial_ipc_worker(sender, url, timeout, max_bytes=None):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    os.write(sender.fileno(), struct.pack("!i", 500) + b"unfinished")
    time.sleep(10)


def timed_worker(sender, url, timeout, max_bytes=None):
    time.sleep(.2)
    sender.send(speed_test._Complete(speed_test.Download(.0125, 3)))
    sender.close()


def stubborn_worker(sender, url, timeout, max_bytes=None):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    sender.send(speed_test._Progress(3))
    time.sleep(10)


class JSONReliabilityTests(CLIReliabilityTests):
    json_mode = True


class JSONControlsTests(CLIControlsTests):
    json_mode = True


class ReportingTests(CLIHarness):
    def run_result(self, result, json_mode):
        out, err = io.StringIO(), io.StringIO()
        operation = {"side_effect": result} if isinstance(result, Exception) else {"return_value": result}
        with mock.patch.object(speed_test, "measure", **operation):
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = main(['http://localhost/?q="quoted"&x=\\', *(["--json"] if json_mode else [])])
        return code, out.getvalue(), err.getvalue()

    def test_identical_terminal_results_in_both_formats(self):
        # Literal oracles cover zero, one, odd/even and zero-duration sets.
        fixtures = [
            ((), (0, 0, None, None, None, None, None)),
            ((speed_test.Download(.5, 125000),), (125000, .5, .5, .5, .5, .5, 2)),
            ((speed_test.Download(4., 1000000), speed_test.Download(2., 500000)),
             (1500000, 6, 3, 2, 3, 4, 2)),
            ((speed_test.Download(4., 1000000), speed_test.Download(1., 500000),
              speed_test.Download(7., 1500000)), (3000000, 12, 4, 1, 4, 7, 2)),
            ((speed_test.Download(0., 3),), (3, 0, 0, 0, 0, 0, 0)),
        ]
        for downloads, expected in fixtures:
            results = [speed_test.DownloadError(None, downloads, requested_count=4,
                                               reason="cancelled", detail='stop "now"\nnext')]
            if downloads:
                results.append(speed_test.Summary(downloads))
            for result in results:
                with self.subTest(downloads=downloads, incomplete=isinstance(result, Exception)):
                    code, raw, err = self.run_result(result, True)
                    report = self.assert_json_report(raw, code, err)
                    self.assertEqual(tuple(report["statistics"].values()), expected)
                    text_code, text, text_err = self.run_result(result, False)
                    self.assertEqual(text_code, code)
                    self.assertEqual(text_err, err)
                    self.assertIn(f"Completed: {len(downloads)}/{report['requested_count']} requests", text)
                    self.assertIn(f"Total downloaded:     {expected[0]:,} bytes", text)
                    for key, label in (("average_seconds", "Average request time:"),
                                       ("min_seconds", "Min request time:"),
                                       ("median_seconds", "Median request time:"),
                                       ("max_seconds", "Max request time:"),
                                       ("total_seconds", "Total request time:"),
                                       ("speed_mbps", "Download speed:" if code == 0 else "Completed-only speed:")):
                        line = next(line for line in text.splitlines() if line.startswith(label))
                        value = report["statistics"][key]
                        self.assertEqual(line.split(":", 1)[1].strip(), "N/A" if value is None else
                                         f"{value:.2f} Mbit/s" if key == "speed_mbps" else f"{value:.3f} s")
                    if code:
                        self.assertNotIn("Results", text)

    def test_all_reasons_and_partial_provenance(self):
        for reason, exact in itertools.product(get_args(speed_test.Reason), (False, True)):
            partial = speed_test.PartialDownload(2, 999999, 500., reason, exact, "detail")
            result = speed_test.DownloadError(partial, (speed_test.Download(2., 125000),), requested_count=4)
            for mode in (False, True):
                with self.subTest(reason=reason, exact=exact, json=mode):
                    code, out, err = self.run_result(result, mode)
                    self.assertEqual(code, 130 if reason == "cancelled" else 1)
                    self.assertIn("Partial request 2: " + ("" if exact else "at least ") + "999,999 bytes", err)
                    if mode:
                        report = self.assert_json_report(out, code, err)
                        self.assertEqual(report["partial"], dict(number=2, bytes_downloaded=999999,
                                                               elapsed_seconds=500., exact_bytes=exact))
                        self.assertEqual(report["error"], dict(reason=reason, detail="detail"))
                        self.assertEqual(report["statistics"]["speed_mbps"], .5)
                    else:
                        self.assertIn("Series incomplete", out)
                        self.assertIn("Completed-only speed: 0.50 Mbit/s", out)
        # A started zero-byte attempt is present, even with an empty prefix.
        result = speed_test.DownloadError(speed_test.PartialDownload(1, 0, .1, "network_error", True, "no data"))
        code, out, err = self.run_result(result, True)
        self.assertEqual(self.assert_json_report(out, code, err)["partial"]["bytes_downloaded"], 0)

    def test_warning_threshold_and_authoritative_status(self):
        for seconds in (.999, 1., 1.001):
            for mode in (False, True):
                for incomplete in (False, True):
                    downloads = (speed_test.Download(seconds, 1),)
                    result = speed_test.DownloadError(None, downloads, requested_count=1,
                                                     reason="cancelled", detail="after final append") if incomplete else speed_test.Summary(downloads)
                    code, out, err = self.run_result(result, mode)
                    self.assertEqual(code, 130 if incomplete else 0)
                    self.assertEqual(err.count("Warning:"), int(seconds < 1))
                    if mode:
                        self.assert_json_report(out, code, err)

    def test_measure_is_silent_and_callback_keeps_last_completion(self):
        with mock.patch.object(speed_test, "download_once", return_value=speed_test.Download(.2, 3)):
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(len(measure("http://localhost/", count=1).downloads), 1)
            self.assertEqual(out.getvalue(), "")
            callback = mock.Mock(side_effect=KeyboardInterrupt)
            with self.assertRaises(speed_test.DownloadError) as caught:
                measure("http://localhost/", count=1, progress=callback)
            callback.assert_called_once_with(1, speed_test.Download(.2, 3))
            self.assertEqual(caught.exception.requested_count, 1)
            self.assertEqual(len(caught.exception.downloads), 1)
            self.assertEqual(caught.exception.reason, "cancelled")

    def test_json_rejects_nonfinite_numbers(self):
        for value in (float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                self.run_result(speed_test.Summary((speed_test.Download(value, 1),)), True)

    def test_help_and_invalid_json_arguments_never_measure(self):
        for argv, expected in ((["--json", "--help"], 0), (["--json"], 2),
                               (["http://localhost/", "--json", "--count", "0"], 2)):
            with mock.patch.object(speed_test, "measure") as operation:
                out, err = io.StringIO(), io.StringIO()
                with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                    with self.assertRaises(SystemExit) as caught:
                        main(argv)
                self.assertEqual(caught.exception.code, expected)
                operation.assert_not_called()
                if expected == 2:
                    self.assertEqual(out.getvalue(), "")
                else:
                    self.assertIn("--json", out.getvalue())

    def test_real_short_and_long_measurements(self):
        for mode, delay in itertools.product((False, True), (0, 1.05)):
            def scenario(sock, index, path, server):
                time.sleep(delay)
                response(sock)
            with self.subTest(json=mode, delay=delay), RawServer(scenario) as server:
                code, out, err, _ = self.cli(server, ["--count", "1", *(["--json"] if mode else [])])
                self.assertEqual(code, 0, err)
                self.assertEqual(err.count("Warning:"), int(delay == 0))
                if mode:
                    self.assert_json_report(out, code, err)
                else:
                    self.assertIn("Results", out)

    def test_cli_worker_start_failure_in_both_formats(self):
        with tempfile.TemporaryDirectory() as temp:
            Path(temp, "sitecustomize.py").write_text(
                "import multiprocessing.context\n"
                "def fail(*a, **k):\n raise OSError('out of descriptors')\n"
                "multiprocessing.context.SpawnContext.Pipe=fail\n"
            )
            for mode in (False, True):
                with self.subTest(json=mode), RawServer(lambda s, i, p, srv: response(s)) as server:
                    code, out, err, _ = self.cli(server, ["--count", "1", *(["--json"] if mode else [])],
                                                 env_extra={"PYTHONPATH": temp})
                    self.assertEqual(code, 1)
                    self.assertEqual(server.paths, [])
                    self.assertIn("worker_error", err)
                    self.assertIn("out of descriptors", err)
                    if mode:
                        report = self.assert_json_report(out, code, err)
                        self.assertEqual(report["partial"]["bytes_downloaded"], 0)
                        self.assertFalse(report["partial"]["exact_bytes"])
                    else:
                        self.assertIn("Series incomplete: worker_error", out)
                        self.assertIn("Completed-only speed: N/A", out)


class ControlsUnitTests(unittest.TestCase):
    def test_invalid_budget_values_never_start_measurement(self):
        cases = []
        for flag in ("--count", "--max-bytes", "--max-time"):
            values = ("0", "-1", "nan", "inf", "-inf", "bad", "", "1MiB")
            if flag != "--max-time":
                values += ("1.5", "1e3")
            cases += [["http://localhost/", flag, value] for value in values]
            cases.append(["http://localhost/", flag])
        cases += [["http://localhost/", "--count", "3", "--max-time", "2", "--max-bytes", "0"],
                  ["http://localhost/", "--count", "0", "--max-time", "2", "--max-bytes", "3"],
                  ["http://localhost/", "--count", "3", "--max-time", "nan", "--max-bytes", "3"]]
        with mock.patch.object(speed_test, "measure") as operation:
            for argv in cases:
                with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as caught:
                        main(argv)
                    self.assertEqual(caught.exception.code, 2)
            operation.assert_not_called()

    def test_large_valid_budgets_do_not_overflow(self):
        with mock.patch.object(speed_test, "_download_worker", timed_worker):
            with contextlib.redirect_stdout(io.StringIO()):
                result = measure("http://localhost/", count=1, max_time=1e300, max_bytes=10**100)
        self.assertEqual(result.total_bytes, 3)
        summary = speed_test.Summary((speed_test.Download(1, 3),))
        with mock.patch.object(speed_test, "measure", return_value=summary) as operation:
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(["http://localhost/", "--count", str(10**100),
                                       "--max-time", "1e300", "--max-bytes", str(10**100)]), 0)
            self.assertEqual(operation.call_args.kwargs["count"], 10**100)
            self.assertEqual(operation.call_args.kwargs["max_bytes"], 10**100)

    def test_reader_never_returns_payload_beyond_cap(self):
        for framing, size, cap in itertools.product(("fixed", "chunked", "eof"), (3, 65539), (2, 3, 65538, 65539, 65540)):
            with self.subTest(framing=framing, size=size, cap=cap):
                headers, body = framed(framing, b"x" * size)
                sock = mock.Mock()
                sock.makefile.return_value = io.BytesIO(b"HTTP/1.1 200 OK\r\n" + headers + b"\r\n" + body)
                reply = speed_test._StrictResponse(sock)
                reply.begin()
                actual_read = reply.read1
                delivered = 0
                def bounded_read(n):
                    nonlocal delivered
                    self.assertLessEqual(n, cap - delivered)
                    self.assertGreater(n, 0)
                    data = actual_read(n)
                    delivered += len(data)
                    self.assertLessEqual(delivered, cap)
                    return data
                reply.read1 = bounded_read
                sender = mock.Mock()
                opener = mock.Mock()
                opener.open.return_value = reply
                with mock.patch.object(speed_test, "build_opener", return_value=opener):
                    with mock.patch.object(speed_test.signal, "signal"):
                        speed_test._download_worker(sender, "http://localhost/", 1, cap)
                messages = [call.args[0] for call in sender.send.call_args_list]
                terminal = messages[-1]
                complete = size < cap or (size == cap and framing == "fixed")
                self.assertIsInstance(terminal, speed_test._Complete if complete else speed_test._Failure)
                if not complete:
                    self.assertEqual(terminal.reason, "byte_limit")
                    self.assertEqual(terminal.bytes_downloaded, cap)
                    self.assertTrue(terminal.exact_bytes)
                self.assertEqual(delivered, min(size, cap))
                sender.close.assert_called_once()

    def test_shared_deadline_includes_attempts_cleanup_and_progress(self):
        for overhead in ("cleanup", "progress"):
            with self.subTest(overhead=overhead):
                clock = [10.0]
                def attempt(*args, **kwargs):
                    clock[0] += .3 if overhead == "cleanup" else .1
                    return speed_test.Download(.01, 3)
                def output(*args, **kwargs):
                    if overhead == "progress":
                        clock[0] += .2
                with mock.patch.object(speed_test, "perf_counter", side_effect=lambda: clock[0]):
                    with mock.patch.object(speed_test, "download_once", side_effect=attempt) as download:
                        with self.assertRaises(speed_test.DownloadError) as caught:
                            measure("http://localhost/", count=3, max_time=.25, max_bytes=3,
                                    progress=output)
                self.assertEqual(caught.exception.reason, "time_limit")  # Time wins between attempts.
                self.assertIsNone(caught.exception.partial)
                self.assertEqual(caught.exception.requested_count, 3)
                self.assertEqual(len(caught.exception.downloads), 1)
                self.assertEqual(download.call_count, 1)
                self.assertEqual(download.call_args.kwargs, {"series_deadline": 10.25, "max_bytes": 3})

    def test_remaining_budget_and_absolute_deadline_are_forwarded(self):
        clock = [10.0]
        def attempt(*args, **kwargs):
            clock[0] += .1
            return speed_test.Download(.01, 3)
        with mock.patch.object(speed_test, "perf_counter", side_effect=lambda: clock[0]):
            with mock.patch.object(speed_test, "download_once", side_effect=attempt) as download:
                with contextlib.redirect_stdout(io.StringIO()):
                    result = measure("http://localhost/", count=3, max_time=1, max_bytes=10)
        self.assertEqual(result.total_bytes, 9)
        self.assertEqual([call.kwargs["max_bytes"] for call in download.call_args_list], [10, 7, 4])
        self.assertEqual([call.kwargs["series_deadline"] for call in download.call_args_list], [11.0] * 3)

    def test_late_terminal_and_equal_deadline_precedence(self):
        for terminal in (speed_test._Complete(speed_test.Download(.1, 3)),
                         speed_test._Failure("byte_limit", "cap", 3, .1)):
            for series_deadline, expected in ((1, "time_limit"), (2, "download_timeout")):
                with self.subTest(terminal=terminal, series_deadline=series_deadline):
                    messages = speed_test.queue.Queue()
                    messages.put(terminal)
                    context = mock.Mock()
                    context.Pipe.return_value = (mock.Mock(), mock.Mock())
                    with mock.patch.object(speed_test, "perf_counter", side_effect=[0, 0, 1, 1, 1]):
                        with mock.patch.object(speed_test.multiprocessing, "get_context", return_value=context):
                            with mock.patch.object(speed_test.threading, "Thread"), mock.patch.object(speed_test, "_cleanup", return_value=True):
                                with mock.patch.object(speed_test.queue, "Queue", return_value=messages):
                                    with self.assertRaises(speed_test.DownloadError) as caught:
                                        speed_test.download_once("http://localhost/", 1, 1, series_deadline=series_deadline)
                    self.assertEqual(caught.exception.reason, expected)

    def test_terminal_before_deadline_keeps_reason_and_cleanup_can_override(self):
        for clean in (True, False):
            messages = speed_test.queue.Queue()
            messages.put(speed_test._Failure("byte_limit", "cap", 3, .1))
            context = mock.Mock()
            context.Pipe.return_value = (mock.Mock(), mock.Mock())
            with mock.patch.object(speed_test, "perf_counter", return_value=0):
                with mock.patch.object(speed_test.multiprocessing, "get_context", return_value=context):
                    with mock.patch.object(speed_test.threading, "Thread"), mock.patch.object(speed_test, "_cleanup", return_value=clean):
                        with mock.patch.object(speed_test.queue, "Queue", return_value=messages):
                            with self.assertRaises(speed_test.DownloadError) as caught:
                                speed_test.download_once("http://localhost/", 1, series_deadline=1)
            self.assertEqual(caught.exception.reason, "byte_limit" if clean else "worker_error")
            self.assertEqual(caught.exception.partial.bytes_downloaded, 3)

    def test_expired_deadline_never_spawns_and_last_completion_survives_cleanup(self):
        with mock.patch.object(speed_test, "perf_counter", return_value=10):
            with mock.patch.object(speed_test.multiprocessing, "get_context") as context:
                with self.assertRaises(speed_test.DownloadError) as caught:
                    speed_test.download_once("http://localhost/", 1, series_deadline=10)
            context.assert_not_called()
            self.assertIsNone(caught.exception.partial)
            self.assertEqual(caught.exception.reason, "time_limit")
        clock = [0.0]
        messages = speed_test.queue.Queue()
        messages.put(speed_test._Complete(speed_test.Download(.1, 3)))
        context = mock.Mock()
        context.Pipe.return_value = (mock.Mock(), mock.Mock())
        def slow_cleanup(*args):
            clock[0] = 2.0
            return True
        with mock.patch.object(speed_test, "perf_counter", side_effect=lambda: clock[0]):
            with mock.patch.object(speed_test.multiprocessing, "get_context", return_value=context):
                with mock.patch.object(speed_test.threading, "Thread"), mock.patch.object(speed_test, "_cleanup", side_effect=slow_cleanup):
                    with mock.patch.object(speed_test.queue, "Queue", return_value=messages):
                        with contextlib.redirect_stdout(io.StringIO()):
                            result = measure("http://localhost/", count=1, max_time=1)
        self.assertEqual(result.total_bytes, 3)


class SupervisorTests(unittest.TestCase):
    def test_crash_and_partial_ipc_are_bounded(self):
        for worker, reason in ((crash_worker, "worker_error"), (partial_ipc_worker, "download_timeout")):
            if worker is partial_ipc_worker and os.name != "posix":
                continue
            with self.subTest(worker=worker), mock.patch.object(speed_test, "_download_worker", worker):
                before = {child.pid for child in speed_test.multiprocessing.active_children()}
                started = time.monotonic()
                with self.assertRaises(speed_test.DownloadError) as caught:
                    speed_test.download_once("http://localhost/", 1, .4)
                self.assertEqual(caught.exception.partial.reason, reason)
                self.assertFalse(caught.exception.partial.exact_bytes)
                self.assertLess(time.monotonic() - started, 2)
                self.assertEqual({child.pid for child in speed_test.multiprocessing.active_children()}, before)

    def test_measurement_time_excludes_spawn_and_cleanup(self):
        with mock.patch.object(speed_test, "_download_worker", timed_worker):
            started = time.monotonic()
            result = speed_test.download_once("http://localhost/", 1, 3)
            self.assertGreater(time.monotonic() - started, .2)
            self.assertEqual(result, speed_test.Download(.0125, 3))

    @unittest.skipUnless(os.name == "posix", "POSIX signal delivery")
    def test_repeated_ctrl_c_does_not_abandon_cleanup(self):
        def interrupts():
            time.sleep(.25)
            os.kill(os.getpid(), signal.SIGINT)
            time.sleep(.1)
            os.kill(os.getpid(), signal.SIGINT)
        with mock.patch.object(speed_test, "_download_worker", stubborn_worker):
            thread = threading.Thread(target=interrupts)
            thread.start()
            with self.assertRaises(speed_test.DownloadError) as caught:
                speed_test.download_once("http://localhost/", 1, 3)
            thread.join(2)
            self.assertEqual(caught.exception.partial.reason, "cancelled")
            self.assertEqual(caught.exception.partial.bytes_downloaded, 3)
            self.assertEqual(speed_test.multiprocessing.active_children(), [])

    def test_completed_prefix_is_retained(self):
        partial = speed_test.PartialDownload(1, 2, .1, "incomplete_response", True, "short body")
        with mock.patch.object(speed_test, "download_once", side_effect=[speed_test.Download(.2, 3), speed_test.Download(.3, 4), speed_test.DownloadError(partial)]):
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(speed_test.DownloadError) as caught:
                measure("http://localhost/")
        self.assertEqual(caught.exception.partial.number, 3)
        self.assertEqual([item.bytes_downloaded for item in caught.exception.downloads], [3, 4])

    def test_parser_rejects_invalid_inputs_before_measure(self):
        cases = [[url] for url in ("http://", "ftp://example.com/", "http://localhost:bad/", "http://localhost:65536/", "http://localhost:0/", "http://localhost:/", "http://localhost/\nx", "http://localhost/\tx", "http://localhost/\x7f", "http://[invalid]/")]
        cases += [["http://localhost/", option, value] for option in ("--timeout", "--download-timeout") for value in ("0", "-1", "nan", "inf", "bad")]
        cases.append(["http://localhost/", "--timeout", "1e300"])
        with mock.patch.object(speed_test, "measure") as operation:
            for argv in cases:
                with self.subTest(argv=argv), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
                    main(argv)
                self.assertEqual(caught.exception.code, 2)
            operation.assert_not_called()

    def test_cancel_between_requests_retains_completed_download(self):
        with mock.patch.object(speed_test, "download_once", return_value=speed_test.Download(.2, 3)):
            with self.assertRaises(speed_test.DownloadError) as caught:
                measure("http://localhost/", progress=mock.Mock(side_effect=KeyboardInterrupt))
        self.assertEqual(caught.exception.reason, "cancelled")
        self.assertIsNone(caught.exception.partial)
        self.assertEqual(len(caught.exception.downloads), 1)

    def test_large_deadline_does_not_overflow_queue_wait(self):
        with mock.patch.object(speed_test, "_download_worker", timed_worker):
            self.assertEqual(speed_test.download_once("http://localhost/", 1, 1e300).bytes_downloaded, 3)

    def test_worker_start_failure_is_reported_without_network(self):
        context = speed_test.multiprocessing.get_context("spawn")
        with mock.patch.object(speed_test.multiprocessing, "get_context", return_value=context):
            with mock.patch.object(context, "Pipe", side_effect=OSError("out of descriptors")):
                with self.assertRaises(speed_test.DownloadError) as caught:
                    speed_test.download_once("http://localhost/", 1)
        self.assertEqual(caught.exception.partial.reason, "worker_error")
        self.assertEqual(caught.exception.partial.bytes_downloaded, 0)

    def test_unconfirmed_cleanup_never_returns_success(self):
        cleanup = speed_test._cleanup
        def unconfirmed(*args):
            self.assertTrue(cleanup(*args))
            return False
        with mock.patch.object(speed_test, "_download_worker", timed_worker):
            with mock.patch.object(speed_test, "_cleanup", side_effect=unconfirmed):
                with self.assertRaises(speed_test.DownloadError) as caught:
                    speed_test.download_once("http://localhost/", 1)
        self.assertEqual(caught.exception.partial.reason, "worker_error")
        self.assertEqual(caught.exception.partial.bytes_downloaded, 3)

    def test_summary_formula_has_independent_oracle(self):
        result = speed_test.Summary((speed_test.Download(2, 1_000_000), speed_test.Download(6, 3_000_000)))
        self.assertEqual(result.total_bytes, 4_000_000)
        self.assertEqual(result.total_seconds, 8)
        self.assertEqual(result.average_seconds, 4)
        self.assertEqual(result.speed_mbps, 4)


if __name__ == "__main__":
    unittest.main()
