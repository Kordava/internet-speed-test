"""Measure the throughput of a bounded series of HTTP(S) downloads."""

import argparse
import http.client
import json
import math
import multiprocessing
import queue
import re
import signal
import socket
import statistics
import string
import sys
import threading
from dataclasses import dataclass, replace
from multiprocessing.connection import Connection
from time import perf_counter
from typing import Callable, Literal, TypedDict
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin, urlsplit
from urllib.request import HTTPErrorProcessor, HTTPHandler, HTTPSHandler, Request, build_opener

REQUEST_COUNT = 10
CHUNK_SIZE = 64 * 1024
PROGRESS_BYTES = 1024 * 1024
PROGRESS_INTERVAL = 0.1
DEFAULT_TIMEOUT = 30.0
DEFAULT_DOWNLOAD_TIMEOUT = 120.0
CLEANUP_TIMEOUT = 0.5
MAX_REDIRECTS = 10
Reason = Literal[
    "http_error", "network_error", "io_timeout", "download_timeout",
    "incomplete_response", "protocol_error", "cancelled", "worker_error",
    "time_limit", "byte_limit",
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
    """An incomplete series, with a successful prefix and optional attempt."""

    def __init__(
        self, partial: PartialDownload | None, downloads: tuple[Download, ...] = (),
        *, requested_count: int = REQUEST_COUNT, reason: Reason | None = None,
        detail: str | None = None,
    ):
        self.partial = partial
        self.downloads = downloads
        self.requested_count = requested_count
        if partial is not None:
            self.reason = partial.reason
            self.detail = partial.detail
            label = f"Request {partial.number}/{requested_count} failed"
        else:
            if reason is None or detail is None:
                raise ValueError("a series stop requires a reason and detail")
            self.reason = reason
            self.detail = detail
            label = "Series stopped"
        super().__init__(f"{label}: {self.reason}: {self.detail}")


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


def _download_worker(
    sender: Connection, url: str, timeout: float, max_bytes: int | None = None,
) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    count = 0
    last_progress_count = 0
    last_progress_at = 0.0
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
                while True:
                    remaining = None if max_bytes is None else max_bytes - count
                    if remaining == 0:
                        # A fixed-length body proves completion at the boundary.
                        # Chunked/EOF would require another read, violating the cap.
                        if not response.chunked and response.length == 0:
                            break
                        sender.send(_Failure("byte_limit", "series byte budget exhausted", count,
                                             perf_counter() - started))
                        return
                    data = response.read1(CHUNK_SIZE if remaining is None else min(CHUNK_SIZE, remaining))
                    if not data:
                        break
                    count += len(data)
                    now = perf_counter()
                    # Preserve the first payload even if the next read stalls.
                    if (last_progress_count == 0
                            or count - last_progress_count >= PROGRESS_BYTES
                            or now - last_progress_at >= PROGRESS_INTERVAL):
                        sender.send(_Progress(count))
                        last_progress_count = count
                        last_progress_at = now
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
    *, series_deadline: float | None = None, max_bytes: int | None = None,
) -> Download:
    """Supervise one attempt, including startup, DNS and every redirect."""
    started = perf_counter()
    deadline = started + download_timeout
    deadline_reason: Reason = "download_timeout"
    if series_deadline is not None and series_deadline <= deadline:
        deadline = series_deadline
        deadline_reason = "time_limit"
    if series_deadline is not None and started >= series_deadline:
        raise DownloadError(None, reason="time_limit", detail="series deadline exceeded")
    receiver = sender = worker = reader = None
    messages = queue.Queue(maxsize=1)
    count = 0
    result = None
    failure = None
    try:
        context = multiprocessing.get_context("spawn")
        receiver, sender = context.Pipe(duplex=False)
        worker = context.Process(target=_download_worker, args=(sender, url, timeout, max_bytes))
        reader = threading.Thread(target=_receive, args=(receiver, messages), daemon=True)
        worker.start()
        sender.close()
        reader.start()
        while True:
            remaining = deadline - perf_counter()
            if remaining <= 0:
                failure = _Failure(deadline_reason, "download deadline exceeded" if deadline_reason == "download_timeout"
                                   else "series deadline exceeded", count,
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
    *, count: int = REQUEST_COUNT, max_time: float | None = None, max_bytes: int | None = None,
    progress: Callable[[int, Download], None] | None = None,
) -> Summary:
    """Perform sequential downloads until count is complete or a budget stops us."""
    deadline = None if max_time is None else perf_counter() + max_time
    downloads = []
    total_bytes = 0
    try:
        for index in range(1, count + 1):
            if deadline is not None and perf_counter() >= deadline:
                raise DownloadError(None, tuple(downloads), requested_count=count,
                                    reason="time_limit", detail="series deadline exceeded")
            remaining = None if max_bytes is None else max_bytes - total_bytes
            if remaining is not None and remaining <= 0:
                raise DownloadError(None, tuple(downloads), requested_count=count,
                                    reason="byte_limit", detail="series byte budget exhausted")
            try:
                item = download_once(url, timeout, download_timeout,
                                     series_deadline=deadline, max_bytes=remaining)
            except DownloadError as exc:
                partial = None if exc.partial is None else replace(exc.partial, number=index)
                raise DownloadError(partial, tuple(downloads), requested_count=count,
                                    reason=exc.reason, detail=exc.detail) from exc
            downloads.append(item)
            total_bytes += item.bytes_downloaded
            if progress is not None:
                progress(index, item)
    except KeyboardInterrupt:
        raise DownloadError(None, tuple(downloads), requested_count=count,
                            reason="cancelled", detail="interrupted by user") from None
    return Summary(tuple(downloads))


def positive_seconds(value: str) -> float:
    try:
        result = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("timeout must be a number") from exc
    if not math.isfinite(result) or result <= 0:
        raise argparse.ArgumentTypeError("timeout must be a positive finite number")
    return result


def positive_integer(value: str) -> int:
    try:
        result = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if result <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return result


def socket_seconds(value: str) -> float:
    result = positive_seconds(value)
    try:
        with socket.socket() as probe:
            probe.settimeout(result)
    except (OverflowError, OSError, ValueError) as exc:
        raise argparse.ArgumentTypeError("timeout is not representable on this platform") from exc
    return result


class DownloadPayload(TypedDict):
    number: int
    elapsed_seconds: float
    bytes_downloaded: int


class StatisticsPayload(TypedDict):
    total_bytes: int
    total_seconds: float
    average_seconds: float | None
    min_seconds: float | None
    median_seconds: float | None
    max_seconds: float | None
    speed_mbps: float | None


class PartialPayload(DownloadPayload):
    exact_bytes: bool


class ErrorPayload(TypedDict):
    reason: Reason
    detail: str


class _ReportBase(TypedDict):
    schema_version: Literal[1]
    url: str
    requested_count: int
    completed_count: int
    downloads: list[DownloadPayload]
    statistics: StatisticsPayload
    warnings: list[Literal["short_measurement"]]


class CompletedReport(_ReportBase):
    status: Literal["completed"]
    partial: None
    error: None


class IncompleteReport(_ReportBase):
    status: Literal["incomplete"]
    partial: PartialPayload | None
    error: ErrorPayload


Report = CompletedReport | IncompleteReport


def build_report(url: str, result: Summary | DownloadError) -> Report:
    """Describe one terminal result; partial attempts never enter statistics."""
    summary = Summary(result.downloads)
    times = [item.elapsed_seconds for item in summary.downloads]
    average = summary.average_seconds if times else None
    base: _ReportBase = {
        "schema_version": 1, "url": url,
        "requested_count": result.requested_count if isinstance(result, DownloadError) else len(times),
        "completed_count": len(times),
        "downloads": [DownloadPayload(number=n, elapsed_seconds=item.elapsed_seconds,
                                      bytes_downloaded=item.bytes_downloaded)
                      for n, item in enumerate(summary.downloads, 1)],
        "statistics": {
            "total_bytes": summary.total_bytes, "total_seconds": summary.total_seconds,
            "average_seconds": average, "min_seconds": min(times) if times else None,
            "median_seconds": statistics.median(times) if times else None,
            "max_seconds": max(times) if times else None,
            "speed_mbps": summary.speed_mbps if times else None,
        },
        "warnings": ["short_measurement"] if average is not None and average < 1.0 else [],
    }
    if isinstance(result, DownloadError):
        partial = result.partial
        return IncompleteReport(
            **base, status="incomplete", error={"reason": result.reason, "detail": result.detail},
            partial=None if partial is None else PartialPayload(
                number=partial.number, bytes_downloaded=partial.bytes_downloaded,
                elapsed_seconds=partial.elapsed_seconds, exact_bytes=partial.exact_bytes),
        )
    return CompletedReport(**base, status="completed", error=None, partial=None)


def _display(value: float | None, precision: int = 3, unit: str = "s") -> str:
    return "N/A" if value is None else f"{value:.{precision}f} {unit}"


def render_text(report: Report) -> None:
    print(f"URL: {report['url']}\n")
    if report["status"] == "completed":
        print("Results")
    else:
        print(f"Series incomplete: {report['error']['reason']}: {report['error']['detail']}")
        print("Completed requests only")
    print(f"Completed: {report['completed_count']}/{report['requested_count']} requests")
    for item in report["downloads"]:
        print(f"Download {item['number']:02d}: {item['elapsed_seconds']:.3f} s, "
              f"{item['bytes_downloaded']:,} bytes")
    stats = report["statistics"]
    print(f"Average request time: {_display(stats['average_seconds'])}")
    print(f"Min request time:     {_display(stats['min_seconds'])}")
    print(f"Median request time:  {_display(stats['median_seconds'])}")
    print(f"Max request time:     {_display(stats['max_seconds'])}")
    print(f"Total downloaded:     {stats['total_bytes']:,} bytes "
          f"({stats['total_bytes'] / (1024 ** 2):.2f} MiB)")
    print(f"Total request time:   {stats['total_seconds']:.3f} s")
    label = "Download speed:      " if report["status"] == "completed" else "Completed-only speed:"
    print(f"{label} {_display(stats['speed_mbps'], 2, 'Mbit/s')}")
    partial = report["partial"]
    if partial is not None:
        accuracy = "" if partial["exact_bytes"] else "at least "
        print(f"Partial request {partial['number']}: {accuracy}{partial['bytes_downloaded']:,} bytes, "
              f"{partial['elapsed_seconds']:.3f} s")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Measure download speed using sequential HTTP(S) requests."
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
    parser.add_argument("--count", type=positive_integer, default=REQUEST_COUNT, metavar="N",
                        help="number of sequential downloads (default: 10)")
    parser.add_argument("--max-time", type=positive_seconds, metavar="SECONDS",
                        help="total series time budget, including startup and cleanup (default: unlimited)")
    parser.add_argument("--max-bytes", type=positive_integer, metavar="BYTES",
                        help="series HTTP body byte cap (default: unlimited); chunked/EOF responses "
                             "at the exact cap remain incomplete without a further read")
    parser.add_argument("--json", action="store_true", help="write one JSON report to stdout")
    args = parser.parse_args(argv)
    try:
        _validate_url(args.url)
    except ValueError as exc:
        parser.error(f"invalid URL: {exc}")
    def progress(number: int, item: Download) -> None:
        print(f"Request {number:02d}/{args.count}: "
              f"{item.elapsed_seconds:.3f} s, {item.bytes_downloaded:,} bytes",
              file=sys.stderr, flush=True)

    result: Summary | DownloadError
    try:
        result = measure(args.url, timeout=args.timeout, download_timeout=args.download_timeout,
                         count=args.count, max_time=args.max_time, max_bytes=args.max_bytes,
                         progress=progress)
    except DownloadError as exc:
        result = exc
        print(f"Error: {exc}", file=sys.stderr)
    report = build_report(args.url, result)
    if report["status"] == "incomplete":
        stats = report["statistics"]
        print(
            f"Completed: {report['completed_count']}/{report['requested_count']} requests, "
            f"{stats['total_bytes']:,} bytes, {stats['total_seconds']:.3f} s", file=sys.stderr,
        )
        partial = report["partial"]
        if partial is not None:
            accuracy = "" if partial["exact_bytes"] else "at least "
            print(f"Partial request {partial['number']}: {accuracy}{partial['bytes_downloaded']:,} bytes, "
                  f"{partial['elapsed_seconds']:.3f} s", file=sys.stderr)
    if report["warnings"]:
        print("Warning: average completed request time is below 1 s; the result depends more on "
              "latency and does not guarantee a bandwidth estimate.", file=sys.stderr)
    if args.json:
        print(json.dumps(report, allow_nan=False))
    else:
        render_text(report)
    if report["status"] == "incomplete":
        return 130 if report["error"]["reason"] == "cancelled" else 1
    return 0


def cli() -> int:
    """Shared command entry point, including terminal interruption handling."""
    try:
        return main()
    except KeyboardInterrupt:
        print("\nInterrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(cli())
