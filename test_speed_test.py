"""Tests use a local HTTP server; no internet access is required."""

import contextlib
import io
import itertools
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


ROOT = Path(__file__).resolve().parent
FIXTURES = ROOT / "tests" / "fixtures"


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


class CLIReliabilityTests(unittest.TestCase):
    def cli(self, server, args=(), cancel=False, env_extra=None, trust=True):
        env = os.environ.copy()
        for key in list(env):
            if key.lower().endswith("_proxy") or key in ("SSL_CERT_FILE", "SSL_CERT_DIR"):
                env.pop(key)
        env.update(NO_PROXY="*", PYTHONDONTWRITEBYTECODE="1")
        if trust:
            env["SSL_CERT_FILE"] = str(FIXTURES / "ca.pem")
        env.update(env_extra or {})
        process = subprocess.Popen(
            [sys.executable, "-u", str(ROOT / "speed_test.py"), server.url, *args],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
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
        return process.returncode, stdout, stderr, time.monotonic() - started

    def assert_stopped(self, result, reason, completed, partial=3, forced=False):
        code, out, err, duration = result
        self.assertEqual(code, 130 if reason == "cancelled" else 1, (out, err))
        self.assertIn(reason, err)
        self.assertNotIn("Results", out)
        self.assertNotIn("Download speed:", out)
        self.assertIn(f"Completed: {completed}/10 requests, {completed * 3} bytes,", err)
        self.assertEqual(len(re.findall(r"Request \d\d/10:", out)), completed)
        prefix = "at least " if forced else ""
        match = re.search(rf"Partial request {completed + 1}: {prefix}([\d,]+) bytes", err)
        self.assertIsNotNone(match, err)
        count = int(match[1].replace(",", ""))
        if forced:
            self.assertGreaterEqual(count, partial)
        else:
            self.assertEqual(count, partial)
        self.assertLess(duration, 8)

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
                    self.assertIn(f"Total downloaded:     {len(body) * 10} bytes", out)
                    self.assertEqual(len(re.findall(r"Request \d\d/10:", out)), 10)
                    self.assertIn("Results", out)

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
                    self.assertIn(f"Total downloaded:     {count * 10} bytes", out)

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
                    self.assertIn("Total downloaded:     30 bytes", out)

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
                self.assertIn("Total downloaded:     30 bytes", out)

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
                times = re.findall(r"Request \d\d/10: ([\d.]+) s", out)
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


def crash_worker(sender, url, timeout):
    os._exit(7)


def partial_ipc_worker(sender, url, timeout):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    os.write(sender.fileno(), struct.pack("!i", 500) + b"unfinished")
    time.sleep(10)


def timed_worker(sender, url, timeout):
    time.sleep(.2)
    sender.send(speed_test._Complete(speed_test.Download(.0125, 3)))
    sender.close()


def stubborn_worker(sender, url, timeout):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    sender.send(speed_test._Progress(3))
    time.sleep(10)


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
            with mock.patch("builtins.print", side_effect=KeyboardInterrupt):
                with self.assertRaises(speed_test.DownloadError) as caught:
                    measure("http://localhost/")
        self.assertEqual(caught.exception.partial.reason, "cancelled")
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
