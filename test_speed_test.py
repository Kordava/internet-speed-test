"""Tests use a local HTTP server; no internet access is required."""

import contextlib
import io
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

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


if __name__ == "__main__":
    unittest.main()
