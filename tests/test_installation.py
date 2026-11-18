"""Build and exercise the installed artifact offline with prepared build tools."""

import configparser
import contextlib
import email.parser
import importlib.metadata
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import venv
import zipfile

import speed_test
from tests import test_speed_test as support


def clean_environment():
    """Isolate installation checks from Python and pip environment settings."""
    env = os.environ.copy()
    for key in ("PYTHONPATH", "PYTHONHOME"):
        env.pop(key, None)
    env.update(PYTHONDONTWRITEBYTECODE="1", PIP_NO_INPUT="1",
               PIP_DISABLE_PIP_VERSION_CHECK="1", PIP_CONFIG_FILE=os.devnull)
    return env


def run(args, cwd, env=None):
    """Run an isolated command and fail with captured diagnostics."""
    result = subprocess.run(args, cwd=cwd, env=env or clean_environment(),
                            capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise AssertionError(f"{args!r} exited {result.returncode}\n"
                             f"{result.stdout}\n{result.stderr}")
    return result


def setUpModule():
    """Build and install an offline wheel in a clean runtime environment."""
    for name, minimum in (("pip", (22, 3)), ("setuptools", (70, 1))):
        try:
            version = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError as exc:
            raise RuntimeError(f"Installation tests require {name}>={'.'.join(map(str, minimum))}; "
                               "prepare build tools before running the offline suite") from exc
        if tuple(map(int, re.match(r"(\d+)\.(\d+)", version).groups())) < minimum:
            raise RuntimeError(f"Installation tests require {name}>={minimum}; found {version}")

    global workspace, wheel, runtime_python, command, outside
    workspace = tempfile.TemporaryDirectory(prefix="internet-speed-test-install-")
    unittest.addModuleCleanup(workspace.cleanup)
    root = Path(workspace.name)
    source, artifacts, outside = root / "source", root / "artifacts", root / "outside"
    source.mkdir()
    artifacts.mkdir()
    outside.mkdir()
    for name in ("pyproject.toml", "README.md", "speed_test.py"):
        shutil.copy2(support.ROOT / name, source / name)
    shutil.copytree(support.ROOT / "tests", source / "tests",
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    run([sys.executable, "-m", "pip", "wheel", "--no-index", "--no-deps",
         "--no-build-isolation", "--no-cache-dir", "--wheel-dir", str(artifacts), str(source)], outside)
    wheels = list(artifacts.glob("*.whl"))
    if len(wheels) != 1:
        raise AssertionError(f"Expected one project wheel, got {wheels}")
    wheel = wheels[0]
    target = root / "runtime"
    venv.EnvBuilder(with_pip=False).create(target)
    scripts = target / ("Scripts" if os.name == "nt" else "bin")
    runtime_python = scripts / ("python.exe" if os.name == "nt" else "python")
    run([sys.executable, "-m", "pip", "--python", str(runtime_python), "install",
         "--no-index", "--no-deps", "--no-cache-dir", str(wheel)], outside)
    # Remove the build copy before any runtime check: the artifact must stand alone.
    shutil.rmtree(source)
    command = (str(scripts / ("internet-speed-test.exe" if os.name == "nt" else "internet-speed-test")),)


class InstalledCommand:
    """Configure inherited CLI tests to use the installed command."""

    @classmethod
    def setUpClass(cls):
        """Select the installed command and an unrelated working directory."""
        super().setUpClass()
        cls.command = command
        cls.cwd = outside


class InstalledReliabilityTests(InstalledCommand, support.CLIReliabilityTests):
    """Check transport reliability through the installed command."""

    pass


class InstalledJSONReliabilityTests(InstalledCommand, support.JSONReliabilityTests):
    """Check installed transport reliability with JSON reporting."""

    pass


class InstalledControlsTests(InstalledCommand, support.CLIControlsTests):
    """Check measurement budgets through the installed command."""

    pass


class InstalledJSONControlsTests(InstalledCommand, support.JSONControlsTests):
    """Check installed measurement budgets with JSON reporting."""

    pass


class InstalledReportingTests(InstalledCommand, support.CLIHarness):
    """Check real measurements and startup failures after installation."""

    test_real_short_and_long_measurements = support.ReportingTests.test_real_short_and_long_measurements
    test_cli_worker_start_failure_in_both_formats = support.ReportingTests.test_cli_worker_start_failure_in_both_formats


class InstallationTests(unittest.TestCase):
    """Check wheel contents, runtime isolation, and command entry points."""

    def test_wheel_metadata_entry_point_and_contents(self):
        """Verify wheel metadata, command registration, and packaged source."""
        with zipfile.ZipFile(wheel) as archive:
            names = archive.namelist()
            metadata = email.parser.Parser().parsestr(archive.read(
                "internet_speed_test-0.1.0.dist-info/METADATA").decode())
            self.assertEqual(metadata["Name"], "internet-speed-test")
            self.assertEqual(metadata["Version"], "0.1.0")
            self.assertEqual(metadata["Requires-Python"], ">=3.10")
            self.assertEqual(metadata.get_all("Requires-Dist", []), [])
            entries = configparser.ConfigParser()
            entries.read_string(archive.read(
                "internet_speed_test-0.1.0.dist-info/entry_points.txt").decode())
            self.assertEqual(dict(entries["console_scripts"]), {"internet-speed-test": "speed_test:cli"})
            self.assertEqual([name for name in names if ".dist-info/" not in name], ["speed_test.py"])
            self.assertEqual(archive.read("speed_test.py"), (support.ROOT / "speed_test.py").read_bytes())

    def test_import_comes_from_clean_runtime_site_packages(self):
        """Verify imports use the installed package in a clean runtime."""
        result = run([str(runtime_python), "-I", "-c",
                      "import json, speed_test, importlib.util; "
                      "print(json.dumps([speed_test.__file__, "
                      "importlib.util.find_spec('pip') is None, "
                      "importlib.util.find_spec('setuptools') is None]))"], outside)
        location, no_pip, no_setuptools = json.loads(result.stdout)
        self.assertTrue(Path(location).is_relative_to(Path(workspace.name) / "runtime"))
        self.assertIn("site-packages", location)
        self.assertTrue(no_pip)
        self.assertTrue(no_setuptools)

    def test_installed_help_and_invalid_arguments(self):
        """Verify installed command help and argument validation."""
        for args, code in ((["--help"], 0), ([], 2), (["ftp://localhost/"], 2),
                           (["http://localhost/", "--count", "0", "--json"], 2),
                           (["http://localhost/", "--unknown"], 2)):
            with self.subTest(args=args):
                result = subprocess.run([*command, *args], cwd=outside, env=clean_environment(),
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, code, result.stderr)
                self.assertNotIn("Traceback", result.stderr)
                if code == 0:
                    for option in ("--count", "--timeout", "--download-timeout", "--max-time", "--max-bytes", "--json"):
                        self.assertIn(option, result.stdout)
                else:
                    self.assertEqual(result.stdout, "")
                    self.assertIn("error:", result.stderr)

    def test_common_entry_returns_main_status_and_handles_interrupt(self):
        """Verify the shared entry point preserves status and handles interrupts."""
        with mock.patch.object(speed_test, "main", return_value=1) as main:
            self.assertEqual(speed_test.cli(), 1)
            main.assert_called_once_with()
        with mock.patch.object(speed_test, "main", side_effect=KeyboardInterrupt):
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                self.assertEqual(speed_test.cli(), 130)
            self.assertEqual(err.getvalue(), "\nInterrupted\n")

    def test_outer_interrupt_through_both_real_entry_points(self):
        """Verify both real entry points handle early interruption consistently."""
        with tempfile.TemporaryDirectory() as temp:
            Path(temp, "sitecustomize.py").write_text(
                "import argparse\n"
                "def interrupt(*args, **kwargs):\n raise KeyboardInterrupt\n"
                "argparse.ArgumentParser.parse_args=interrupt\n")
            env = clean_environment()
            env["PYTHONPATH"] = temp
            for entry in (command, (sys.executable, str(support.ROOT / "speed_test.py"))):
                with self.subTest(entry=entry):
                    result = subprocess.run([*entry, "--help"], cwd=outside, env=env,
                                            capture_output=True, text=True, timeout=10)
                    self.assertEqual(result.returncode, 130)
                    self.assertEqual(result.stdout, "")
                    self.assertEqual(result.stderr, "\nInterrupted\n")
