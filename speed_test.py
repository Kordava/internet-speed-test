"""Measure the throughput of ten sequential HTTP(S) downloads."""

import argparse
import http.client
import math
import multiprocessing
import queue
import re
import signal
import socket
import string
import sys
import threading
from dataclasses import dataclass, replace
from multiprocessing.connection import Connection
from time import perf_counter
from typing import Literal
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin, urlsplit
from urllib.request import HTTPErrorProcessor, HTTPHandler, HTTPSHandler, Request, build_opener

REQUEST_COUNT = 10
CHUNK_SIZE = 64 * 1024
DEFAULT_TIMEOUT = 30.0
DEFAULT_DOWNLOAD_TIMEOUT = 120.0
CLEANUP_TIMEOUT = 0.5
MAX_REDIRECTS = 10
Reason = Literal[
    "http_error", "network_error", "io_timeout", "download_timeout",
    "incomplete_response", "protocol_error", "cancelled", "worker_error",
]


@dataclass(frozen=True)
class Download:
    elapsed_seconds: float
    bytes_downloaded: int


@dataclass(frozen=True)
class Summary:
    downloads: tuple[Download, ...]

    @property
    def total_bytes(self) -> int:
        return sum(item.bytes_downloaded for item in self.downloads)

    @property
    def total_seconds(self) -> float:
        return sum(item.elapsed_seconds for item in self.downloads)

    @property
    def average_seconds(self) -> float:
        return self.total_seconds / len(self.downloads)

    @property
    def speed_mbps(self) -> float:
        if self.total_seconds == 0:
            return 0.0
        return self.total_bytes * 8 / self.total_seconds / 1_000_000


@dataclass(frozen=True)
class PartialDownload:
    number: int
    bytes_downloaded: int
    elapsed_seconds: float
    reason: Reason
    exact_bytes: bool
    detail: str


class DownloadError(Exception):
    """An incomplete series, with its successful prefix and failed attempt."""

    def __init__(self, partial: PartialDownload, downloads: tuple[Download, ...] = ()):
        self.partial = partial
        self.downloads = downloads
        super().__init__(
            f"Request {partial.number}/{REQUEST_COUNT} failed: "
            f"{partial.reason}: {partial.detail}"
        )


class _ProtocolError(http.client.HTTPException):
    pass


class _StrictResponse(http.client.HTTPResponse):
    """Keep stdlib status/headers/transport; validate the body framing strictly."""

    def begin(self):
        super().begin()
        self._chunk_remaining = 0
        self._need_separator = False
        lengths = self.headers.get_all("Content-Length", [])
        encodings = self.headers.get_all("Transfer-Encoding", [])
        if encodings and (lengths or ",".join(encodings).strip().lower() != "chunked"):
            raise _ProtocolError("ambiguous or unsupported Transfer-Encoding")
        if lengths:
            tokens = [part.strip() for value in lengths for part in value.split(",")]
            if not all(re.fullmatch(r"[0-9]+", part) for part in tokens):
                raise _ProtocolError("invalid Content-Length")
            try:
                values = [int(part) for part in tokens]
            except ValueError as exc:
                raise _ProtocolError("invalid Content-Length") from exc
            if len(set(values)) != 1:
                raise _ProtocolError("conflicting Content-Length values")
            self.length = values[0]
        # Header token matching in HTTPResponse is less permissive about OWS.
        self.chunked = bool(encodings)
        if self.status == 204:
            self.chunked = False
            self.length = 0

    def _framing_line(self) -> bytes:
        line = self.fp.readline(CHUNK_SIZE + 1)
        if len(line) > CHUNK_SIZE:
            raise _ProtocolError("framing line exceeds 64 KiB")
        if not line.endswith(b"\n"):
            raise http.client.IncompleteRead(b"")
        if not line.endswith(b"\r\n"):
            raise _ProtocolError("framing line requires CRLF")
        return line[:-2]

    def read1(self, n=CHUNK_SIZE):
        n = min(n, CHUNK_SIZE) if n >= 0 else CHUNK_SIZE
        if not n or self.fp is None:
            return b""
        if not self.chunked:
            data = super().read1(n)
            if not data and self.length:
                raise http.client.IncompleteRead(b"", self.length)
            return data
        if not self._chunk_remaining:
            if self._need_separator:
                separator = self.fp.read(2)
                if len(separator) < 2:
                    raise http.client.IncompleteRead(b"")
                if separator != b"\r\n":
                    raise _ProtocolError("invalid chunk separator")
            size = self._framing_line().split(b";", 1)[0]
            if not re.fullmatch(rb"[0-9a-fA-F]+", size):
                raise _ProtocolError("invalid chunk size")
            self._chunk_remaining = int(size, 16)
            if not self._chunk_remaining:
                for count in range(101):
                    trailer = self._framing_line()
                    if not trailer:
                        self._close_conn()
                        return b""
                    if count == 100:
                        raise _ProtocolError("more than 100 trailer lines")
                    if not re.fullmatch(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+:[\t\x20-\x7e\x80-\xff]*", trailer):
                        raise _ProtocolError("invalid trailer")
        data = self.fp.read1(min(n, self._chunk_remaining))
        if not data:
            raise http.client.IncompleteRead(b"", self._chunk_remaining)
        self._chunk_remaining -= len(data)
        self._need_separator = True
        return data


class _HTTPConnection(http.client.HTTPConnection):
    response_class = _StrictResponse


class _HTTPSConnection(http.client.HTTPSConnection):
    response_class = _StrictResponse


class _HTTPHandler(HTTPHandler):
    def http_open(self, req):
        return self.do_open(_HTTPConnection, req)


class _HTTPSHandler(HTTPSHandler):
    def https_open(self, req):
        return self.do_open(_HTTPSConnection, req, context=self._context)


class _ResponseProcessor(HTTPErrorProcessor):
    def http_response(self, request, response):
        # Inspect statuses ourselves: urllib's redirect handler reads the body.
        return response

    https_response = http_response


def _validate_url(url: str) -> str:
    if any(ord(char) <= 32 or ord(char) == 127 for char in url):
        raise ValueError("URL contains whitespace or control characters")
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError("URL must start with http:// or https:// and contain a host")
    # Accessing port checks non-numeric and out-of-range values before any I/O.
    if parts.port == 0 or parts.netloc.endswith(":"):
        raise ValueError("invalid port")
    try:
        url.encode("ascii")
    except UnicodeError as exc:
        raise ValueError("URL must use ASCII host names and percent-encoded paths") from exc
    return url


@dataclass(frozen=True)
class _Progress:
    bytes_downloaded: int


@dataclass(frozen=True)
class _Complete:
    download: Download


@dataclass(frozen=True)
class _Failure:
    reason: Reason
    detail: str
    bytes_downloaded: int
    elapsed_seconds: float
    exact_bytes: bool = True


def _download_worker(sender: Connection, url: str, timeout: float) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    count = 0
    started = perf_counter()
    try:
        opener = build_opener(_HTTPHandler(), _HTTPSHandler(), _ResponseProcessor())
        started = perf_counter()
        for redirects in range(MAX_REDIRECTS + 1):
            request = Request(url, headers={
                "User-Agent": "internet-speed-test/1.0", "Cache-Control": "no-cache",
            })
            with opener.open(request, timeout=timeout) as response:
                if response.status in (301, 302, 303, 307, 308):
                    location = response.headers.get("Location")
                    if not location or redirects == MAX_REDIRECTS:
                        raise _ProtocolError("missing Location or too many redirects (maximum 10)")
                    if any(ord(char) < 32 or ord(char) == 127 for char in location):
                        raise _ProtocolError("redirect URL contains control characters")
                    location = quote(location, encoding="iso-8859-1", safe=string.punctuation)
                    if urlsplit(location).scheme:
                        _validate_url(location)
                    url = _validate_url(urljoin(url, location))
                    continue
                if not 200 <= response.status < 300:
                    raise HTTPError(url, response.status, response.reason, response.headers, None)
                while data := response.read1(CHUNK_SIZE):
                    count += len(data)
                    sender.send(_Progress(count))
                result = Download(perf_counter() - started, count)
            sender.send(_Complete(result))
            return
    except Exception as exc:
        # This is the worker boundary. Unexpected defects remain worker_error.
        if isinstance(exc, HTTPError):
            reason = "http_error"
        elif isinstance(exc, http.client.IncompleteRead):
            reason = "incomplete_response"
        elif isinstance(exc, (http.client.HTTPException, ValueError)):
            reason = "protocol_error"
        elif isinstance(exc, TimeoutError) or (
            isinstance(exc, URLError) and isinstance(exc.reason, TimeoutError)
        ):
            reason = "io_timeout"
        elif isinstance(exc, (URLError, OSError)):
            reason = "network_error"
        else:
            reason = "worker_error"
        sender.send(_Failure(reason, str(exc), count, perf_counter() - started))
    finally:
        sender.close()


def _receive(receiver: Connection, messages: queue.Queue) -> None:
    """Only this thread may block on a (possibly partial) IPC frame."""
    count = 0
    try:
        while True:
            try:
                message = receiver.recv()
                if not isinstance(message, (_Progress, _Complete, _Failure)):
                    raise ValueError("invalid worker message")
                if isinstance(message, _Progress):
                    count = message.bytes_downloaded
            except Exception as exc:
                message = _Failure("worker_error", f"worker channel closed: {exc}", count, 0, False)
            # One producer, one consumer. Replace a queued progress value, never
            # a terminal message: the producer exits immediately after terminal.
            try:
                messages.put_nowait(message)
            except queue.Full:
                try:
                    messages.get_nowait()
                except queue.Empty:
                    pass
                messages.put_nowait(message)
            if not isinstance(message, _Progress):
                return
    finally:
        receiver.close()


def _cleanup(worker, sender, receiver, reader, completed: bool) -> bool:
    """Bounded teardown; a second Ctrl+C must not abandon a live worker."""
    in_main = threading.current_thread() is threading.main_thread()
    previous = signal.signal(signal.SIGINT, signal.SIG_IGN) if in_main else None
    try:
        if sender is not None:
            sender.close()
        if worker is not None and worker.pid is not None:
            if not completed and worker.is_alive():
                worker.terminate()
            worker.join(CLEANUP_TIMEOUT)
            if worker.is_alive():
                worker.kill()
                worker.join(CLEANUP_TIMEOUT)
            if worker.is_alive():
                return False
        if reader is not None and reader.ident is not None:
            reader.join(CLEANUP_TIMEOUT)
            if reader.is_alive():
                return False
        if receiver is not None:
            receiver.close()
        if worker is not None:
            worker.close()
        return True
    except (OSError, ValueError):
        return False
    finally:
        if in_main:
            signal.signal(signal.SIGINT, previous)


def download_once(
    url: str, timeout: float, download_timeout: float = DEFAULT_DOWNLOAD_TIMEOUT,
) -> Download:
    """Supervise one attempt, including startup, DNS and every redirect."""
    started = perf_counter()
    deadline = started + download_timeout
    receiver = sender = worker = reader = None
    messages = queue.Queue(maxsize=1)
    count = 0
    result = None
    failure = None
    try:
        context = multiprocessing.get_context("spawn")
        receiver, sender = context.Pipe(duplex=False)
        worker = context.Process(target=_download_worker, args=(sender, url, timeout))
        reader = threading.Thread(target=_receive, args=(receiver, messages), daemon=True)
        worker.start()
        sender.close()
        reader.start()
        while True:
            remaining = deadline - perf_counter()
            if remaining <= 0:
                failure = _Failure("download_timeout", "download deadline exceeded", count,
                                   perf_counter() - started, False)
                break
            try:
                message = messages.get(timeout=min(remaining, 0.1))
            except queue.Empty:
                continue
            if isinstance(message, _Progress):
                count = message.bytes_downloaded
                continue
            if perf_counter() >= deadline:
                continue  # A late terminal message cannot undo the deadline.
            if isinstance(message, _Complete):
                result = message.download
            else:
                failure = message
                if not failure.exact_bytes:
                    failure = replace(failure, bytes_downloaded=max(count, failure.bytes_downloaded),
                                      elapsed_seconds=perf_counter() - started)
            break
    except KeyboardInterrupt:
        failure = _Failure("cancelled", "interrupted by user", count,
                           perf_counter() - started, False)
    except (OSError, ValueError, RuntimeError) as exc:
        failure = _Failure("worker_error", str(exc), count, perf_counter() - started, False)
    finally:
        if failure is not None and not failure.exact_bytes:
            try:
                pending = messages.get_nowait()
            except queue.Empty:
                pending = None
            if isinstance(pending, _Progress):
                failure = replace(failure, bytes_downloaded=max(
                    failure.bytes_downloaded, pending.bytes_downloaded,
                ))
        clean = _cleanup(worker, sender, receiver, reader, result is not None)
    if not clean:
        count = max(count, result.bytes_downloaded if result is not None else 0,
                    failure.bytes_downloaded if failure is not None else 0)
        failure = _Failure("worker_error", "worker cleanup could not be confirmed", count,
                           perf_counter() - started, False)
    if failure is not None:
        raise DownloadError(PartialDownload(
            1, failure.bytes_downloaded, failure.elapsed_seconds,
            failure.reason, failure.exact_bytes, failure.detail,
        ))
    return result


def measure(
    url: str, timeout: float = DEFAULT_TIMEOUT,
    download_timeout: float = DEFAULT_DOWNLOAD_TIMEOUT,
) -> Summary:
    """Perform exactly ten downloads, one after another."""
    downloads = []
    try:
        for index in range(1, REQUEST_COUNT + 1):
            try:
                item = download_once(url, timeout, download_timeout)
            except DownloadError as exc:
                raise DownloadError(replace(exc.partial, number=index), tuple(downloads)) from exc
            downloads.append(item)
            print(
                f"Request {index:02d}/{REQUEST_COUNT}: "
                f"{item.elapsed_seconds:.3f} s, {item.bytes_downloaded:,} bytes",
                flush=True,
            )
    except KeyboardInterrupt:
        raise DownloadError(PartialDownload(
            min(len(downloads) + 1, REQUEST_COUNT), 0, 0,
            "cancelled", False, "interrupted by user",
        ), tuple(downloads)) from None
    return Summary(tuple(downloads))


def positive_seconds(value: str) -> float:
    try:
        result = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("timeout must be a number") from exc
    if not math.isfinite(result) or result <= 0:
        raise argparse.ArgumentTypeError("timeout must be a positive finite number")
    return result


def socket_seconds(value: str) -> float:
    result = positive_seconds(value)
    try:
        with socket.socket() as probe:
            probe.settimeout(result)
    except (OverflowError, OSError, ValueError) as exc:
        raise argparse.ArgumentTypeError("timeout is not representable on this platform") from exc
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Measure download speed using 10 sequential HTTP(S) requests."
    )
    parser.add_argument("url", help="URL of an HTTP(S) resource")
    parser.add_argument(
        "--timeout", type=socket_seconds, default=DEFAULT_TIMEOUT, metavar="SECONDS",
        help="timeout for blocking network operations, excluding DNS (default: 30)",
    )
    parser.add_argument(
        "--download-timeout", type=positive_seconds, default=DEFAULT_DOWNLOAD_TIMEOUT,
        metavar="SECONDS", help="total deadline for each download, including DNS (default: 120)",
    )
    args = parser.parse_args(argv)
    try:
        _validate_url(args.url)
    except ValueError as exc:
        parser.error(f"invalid URL: {exc}")
    print(f"URL: {args.url}\n", flush=True)
    try:
        summary = measure(args.url, timeout=args.timeout, download_timeout=args.download_timeout)
    except DownloadError as exc:
        partial = exc.partial
        print(f"Error: {exc}", file=sys.stderr)
        print(
            f"Completed: {len(exc.downloads)}/{REQUEST_COUNT} requests, "
            f"{sum(item.bytes_downloaded for item in exc.downloads):,} bytes, "
            f"{sum(item.elapsed_seconds for item in exc.downloads):.3f} s", file=sys.stderr,
        )
        accuracy = "" if partial.exact_bytes else "at least "
        print(f"Partial request {partial.number}: {accuracy}{partial.bytes_downloaded:,} bytes, "
              f"{partial.elapsed_seconds:.3f} s", file=sys.stderr)
        return 130 if partial.reason == "cancelled" else 1
    print("\nResults")
    print(f"Average request time: {summary.average_seconds:.3f} s")
    print(
        f"Total downloaded:     {summary.total_bytes:,} bytes "
        f"({summary.total_bytes / (1024 ** 2):.2f} MiB)"
    )
    print(f"Total request time:   {summary.total_seconds:.3f} s")
    print(f"Download speed:       {summary.speed_mbps:.2f} Mbit/s")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted", file=sys.stderr)
        sys.exit(130)
