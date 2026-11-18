"""Measure the throughput of ten sequential HTTP(S) downloads."""

import argparse
import math
import sys
from dataclasses import dataclass
from time import perf_counter
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

REQUEST_COUNT = 10
CHUNK_SIZE = 64 * 1024
DEFAULT_TIMEOUT = 30.0


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


class DownloadError(Exception):
    """A download in the series could not be completed."""


def download_once(url: str, timeout: float) -> Download:
    """Read the complete HTTP response and measure the elapsed time."""
    request = Request(
        url,
        headers={
            "User-Agent": "internet-speed-test/1.0",
            "Cache-Control": "no-cache",
        },
    )
    started = perf_counter()
    bytes_downloaded = 0

    with urlopen(request, timeout=timeout) as response:
        while True:
            chunk = response.read(CHUNK_SIZE)
            if not chunk:
                break
            bytes_downloaded += len(chunk)

    return Download(
        elapsed_seconds=perf_counter() - started,
        bytes_downloaded=bytes_downloaded,
    )


def measure(url: str, timeout: float = DEFAULT_TIMEOUT) -> Summary:
    """Perform exactly ten downloads, one after another."""
    downloads = []
    for index in range(1, REQUEST_COUNT + 1):
        try:
            item = download_once(url, timeout)
        except (HTTPError, URLError, OSError, ValueError) as exc:
            raise DownloadError(f"Request {index}/{REQUEST_COUNT} failed: {exc}") from exc

        downloads.append(item)
        print(
            f"Request {index:02d}/{REQUEST_COUNT}: "
            f"{item.elapsed_seconds:.3f} s, {item.bytes_downloaded:,} bytes"
        )

    return Summary(tuple(downloads))


def positive_seconds(value: str) -> float:
    try:
        result = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("timeout must be a number") from exc
    if not math.isfinite(result) or result <= 0:
        raise argparse.ArgumentTypeError("timeout must be a positive finite number")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Measure download speed using 10 sequential HTTP(S) requests."
    )
    parser.add_argument("url", help="URL of an HTTP(S) resource")
    parser.add_argument(
        "--timeout",
        type=positive_seconds,
        default=DEFAULT_TIMEOUT,
        metavar="SECONDS",
        help="timeout per request in seconds (default: 30)",
    )
    args = parser.parse_args(argv)

    try:
        parts = urlsplit(args.url)
    except ValueError as exc:
        parser.error(f"invalid URL: {exc}")
    if parts.scheme not in ("http", "https") or not parts.netloc:
        parser.error("URL must start with http:// or https:// and contain a host")

    print(f"URL: {args.url}\n")
    try:
        summary = measure(args.url, timeout=args.timeout)
    except DownloadError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

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
