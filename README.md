# Internet Speed Test

A Python command-line tool for measuring download speed from a specified HTTP/HTTPS URL.

## What it does

1. Takes the resource URL as a command-line argument.
2. Performs **10 sequential HTTP GET requests by default**, or the number set with `--count`. Each request starts only after the previous response has been fully read and its worker cleaned up.
3. Measures **each request from its start until the entire response body has been read**, using `time.perf_counter`.
4. Calculates request-time statistics, total downloaded bytes, and average download speed.
5. Prints the results in the terminal. Response bodies are read in blocks of up to 64 KiB without being saved to disk.

**Requirements:** Python 3.10 or newer. The tool uses only the Python standard library at runtime. Building and installing it requires pip and setuptools>=70.1; pip normally prepares the build backend automatically, which may require network access.

## Getting started

Clone the repository:

```bash
git clone https://github.com/Kordava/internet-speed-test.git
cd internet-speed-test
```

Install the command in a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install .
internet-speed-test --help
internet-speed-test "https://example.com/path/to/large-file.bin" --count 3 --json
```

On Windows, activate the environment with `.venv\Scripts\activate` in cmd or
`.venv\Scripts\Activate.ps1` in PowerShell. The installed command works from any
directory and supports all the options described below.

You can also run the source directly without installing it. Provide the URL of an
**existing** large file that is accessible without authentication:

```bash
python3 speed_test.py "https://example.com/path/to/large-file.bin"
```

> **Important:** `example.com/path/to/large-file.bin` is a placeholder. Replace it with a real file URL. For a meaningful measurement, choose a file that is several megabytes or larger.

URLs must use HTTP or HTTPS, an ASCII hostname, and percent-encoded paths where
needed. Whitespace, control characters, and invalid ports are rejected before any
download starts.

`--timeout` sets the timeout for **blocking network operations** (30 seconds by
default). It does not limit DNS resolution or the total download duration.

`--download-timeout` sets the **overall deadline for each attempt** (120 seconds by
default), including worker startup, DNS, connection establishment, TLS, redirects,
headers, and the response body. It also stops continuously slow transfers:

```bash
python3 speed_test.py "https://example.com/path/to/large-file.bin" --timeout 60 --download-timeout 180
```

On Windows, you can use `python` instead of `python3`.

### Request count and series budgets

`--count N` sets a positive integer number of requests (default: 10).
`--max-time SECONDS` limits the entire series to a positive, finite number of seconds.
`--max-bytes BYTES` limits the total response-body bytes to a positive integer without unit suffixes.
Both series limits are disabled by default and can be used together:

```bash
python3 speed_test.py "https://example.com/path/to/large-file.bin" --count 3 --max-time 20 --max-bytes 10000000
```

The series deadline starts before the first worker and includes process startup,
DNS, connections, TLS, redirects, reads, gaps between requests, progress output,
and cleanup of previous attempts. The earlier of this deadline and
`--download-timeout` applies; the series deadline takes precedence on a tie.
The deadline stops the current download, including a slow or endless body.
Cleanup after the deadline takes additional time. A final complete response
accepted before the deadline remains successful once cleanup is confirmed.
Output assumes responsive stdout/stderr consumers: a blocked console consumer
can delay program exit.

The byte cap applies to final HTTP response bodies after chunk framing has been
removed, without decoding Content-Encoding. Redirect bodies, framing, and OS/TLS
buffers are excluded; this is **not a network traffic cap**. The worker never asks
the reader for more than the remaining budget. Once the budget is exhausted, it
does not read an extra byte to check for EOF. A complete Content-Length body that
ends exactly at the cap is successful. Chunked and EOF-delimited responses remain
incomplete at the exact cap: confirming the terminating chunk, trailers, or EOF
requires another read. For these responses, set a budget larger than the body if
you need to confirm completion. If the budget is exhausted between requests, the
next attempt does not start, even if its response could be empty.

A series time limit produces `time_limit`; a byte limit produces `byte_limit`.
Both exit with code `1`. Between requests, time is checked before bytes. During a
request, a terminal error or limit result accepted before the deadline retains
its reason; a late result cannot override the time limit. Completed requests and
known progress from the current attempt are retained. If the series stops between
attempts, there is no `Partial request` line. For an incomplete series, statistics
and speed cover only complete responses and are explicitly labeled
`Completed requests only` / `Completed-only speed`.

## Text and JSON reports

Progress lines such as `Request 01/10: ...`, warnings, and diagnostics go to
`stderr`. The final report goes to `stdout`. Example text output for `--count 2`
(illustrative values):

```text
URL: https://example.com/path/to/large-file.bin

Results
Completed: 2/2 requests
Download 01: 2.000 s, 1,000,000 bytes
Download 02: 6.000 s, 3,000,000 bytes
Average request time: 4.000 s
Min request time:     2.000 s
Median request time:  4.000 s
Max request time:     6.000 s
Total downloaded:     4,000,000 bytes (3.81 MiB)
Total request time:   8.000 s
Download speed:       4.00 Mbit/s
```

`--json` writes a single JSON document followed by a newline for a completed or
incomplete measurement:

```bash
python3 speed_test.py "https://example.com/path/to/large-file.bin" --count 2 --json > result.json
```

The same result in JSON (formatted here for readability):

```json
{
  "schema_version": 1,
  "url": "https://example.com/path/to/large-file.bin",
  "status": "completed",
  "requested_count": 2,
  "completed_count": 2,
  "downloads": [
    {"number": 1, "elapsed_seconds": 2.0, "bytes_downloaded": 1000000},
    {"number": 2, "elapsed_seconds": 6.0, "bytes_downloaded": 3000000}
  ],
  "statistics": {
    "total_bytes": 4000000, "total_seconds": 8.0, "average_seconds": 4.0,
    "min_seconds": 2.0, "median_seconds": 4.0, "max_seconds": 6.0, "speed_mbps": 4.0
  },
  "partial": null,
  "error": null,
  "warnings": []
}
```

All fields are also present when a measurement stops early, with
`status="incomplete"` and `error={"reason": ..., "detail": ...}`. The text report
prints `Series incomplete` and the reason instead of `Results`. `downloads`
contains only complete responses, and `statistics` covers only those responses.
With no complete responses, totals are zero and other statistics are `null`
(`N/A` in text). For an even number of responses, the median is the mean of the two
middle request times. JSON numbers are not rounded for display; NaN and Infinity
are prohibited. Argument errors and interrupts outside measurement handling can
exit without a report.

`partial` is `null` when the series stops between requests, or an object such as
`{"number": 3, "bytes_downloaded": 123, "elapsed_seconds": 0.5, "exact_bytes": false}`.
An attempt that started but received zero bytes is also retained.
`exact_bytes=false` indicates a lower bound, shown as `at least` in text. Partial
attempts do not contribute to completed-response totals, times, or speed. Even
cancellation after the last complete response leaves the series incomplete.

The first body block is reported to the parent process immediately. Subsequent
progress is sent after another 1 MiB has accumulated or 100 ms has elapsed, checked
after each read. No updates are sent while a read is blocked, so the lower bound
after a forced stop can lag behind the bytes actually read. Complete results and
errors reported by the worker retain an exact byte count regardless of progress
update frequency.

If the average complete-request time is **less than 1 second**, `warnings` contains
`"short_measurement"` and `stderr` receives one warning: the result is more
sensitive to latency and does not guarantee a bandwidth estimate. There is no
warning when the average is at least 1 second or there are no complete responses.
The rule also applies to completed responses in an incomplete series and does not
change the exit code. Use a sufficiently large file for a more useful measurement.

## Units and formula

- **Request time:** seconds (`s`).
- **Total data:** bytes, also displayed as mebibytes (`MiB = 1,048,576 bytes`).
- **Speed:** megabits per second (`Mbit/s`, where `1 Mbit = 1,000,000 bits`).

Speed is calculated from complete downloads:

```text
speed_mbit_s = total_bytes * 8 / sum(request_times_seconds) / 1_000_000
```

Request time includes DNS, connection establishment, TLS, redirects, waiting for
the response, and reading the entire final body. Worker startup and cleanup, gaps
between requests, and terminal output are excluded from the speed denominator.
The measured bytes are HTTP body bytes after chunk framing has been removed,
without decoding Content-Encoding.

## Pre-commit checks

Install the development tool and enable the hook once per checkout, using the
virtual environment from the installation instructions:

```bash
python -m pip install pre-commit
pre-commit install
pre-commit run --all-files
```

The hook runs Ruff lint checks targeting Python 3.10, validates TOML and YAML,
checks for merge conflict markers and private keys, and fixes trailing whitespace
and missing final newlines. Markdown hard line breaks are preserved. The private
key check excludes only `tests/fixtures/server-key.pem`, used by the local HTTPS
test server. Hook versions are pinned in `.pre-commit-config.yaml`.

The first run needs network access to prepare the hook environments. If a hook
changes files, review and stage those changes before retrying the commit. Tests
are run separately with the commands below.

## Tests

Tests use local HTTP/HTTPS servers and **do not require internet access**. TLS is
checked using the test CA in `tests/fixtures`; the server key is only for these
tests:

```bash
python3 -m unittest -v tests.test_speed_test
```

Installation tests also require pip>=22.3 and setuptools>=70.1 in the environment
running the tests. Prepare the build tools beforehand (this step requires network
access):

```bash
python -m pip install "pip>=22.3" "setuptools>=70.1"
python -m unittest discover -s tests -t . -v
```

The full suite builds a wheel from a temporary source copy, installs it into a
clean temporary virtual environment without network access, checks package
contents, and repeats CLI checks using the installed command outside the checkout.
Missing build tools cause an explicit installation-test error. Temporary files
use the system temporary directory, which can be selected with `TMPDIR`.

Coverage includes complete and empty Content-Length, chunked, and EOF-delimited
responses; custom request counts; series limits and their interactions; exact byte
boundaries; truncated responses and malformed framing; redirects; TLS, HTTP, DNS,
and connection errors; both timeouts; text and JSON reports; statistics and
warnings; partial results; worker cleanup; and calculation formulas. Process-group
signal scenarios and corrupted IPC checks rely on POSIX facilities; checks for
leftover CLI processes use Linux `/proc`. Use Linux for the full suite.

## Error handling and limitations

- Exit code `0` means every requested response was fully read. Only this outcome prints `Results` or sets JSON `status="completed"`.
- Network, HTTP, framing, timeout, and series-limit failures stop the series with code `1`; Ctrl+C exits with code `130`. Expected errors are reported without a traceback.
- On an incomplete measurement, `stderr` includes the count, bytes, and total time of completed requests, plus the current attempt's number, known bytes, time, and stop reason when available. After a forced stop, `at least N bytes` is a lower bound based on the latest received progress; unreported counts and OS/TLS buffers are excluded.
- Stop reasons are `http_error`, `network_error`, `io_timeout`, `download_timeout`, `time_limit`, `byte_limit`, `incomplete_response`, `protocol_error`, `cancelled`, and `worker_error`.
- Content-Length must be nonnegative and consistent across all repeated values. Chunked responses require complete framing, a terminating zero chunk, and complete trailers. Malformed or ambiguous lengths are rejected. Without Content-Length or Transfer-Encoding, EOF marks a complete response; an unknown shortfall cannot be detected. A 204 response is a successful empty response.
- Up to ten 301/302/303/307/308 redirects are supported, with HTTP(S) targets only. Intermediate bodies are not downloaded or counted, but redirect time is included. Failed requests are not retried.
- Standard HTTPS certificate verification and environment-based proxy handling remain enabled.
- Each attempt runs in a separate process using `spawn`; the next attempt starts only after that process exits and cleanup completes. After a deadline, cleanup can take up to another 0.5 seconds for each worker join (after terminate and, if needed, kill) and receiver-thread join. These are not OS real-time guarantees. Unconfirmed cleanup produces `worker_error` and stops the series.
- Invalid URLs, timeouts, request counts, or limits are rejected before measurements start (`argparse`, exit code `2`).
- The measurement reflects **HTTP throughput to a specific server**, rather than a guaranteed maximum speed for your internet plan. Server performance, network latency, TLS, and file size all affect the result.
