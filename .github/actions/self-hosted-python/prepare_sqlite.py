"""Qualify SQLite and, only when needed, build a pinned job-local Linux library."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import urllib.request
import zipfile
from pathlib import Path

SQLITE_VERSION = "3.54.0"
SQLITE_ARCHIVE = "sqlite-amalgamation-3540000"
SQLITE_URL = f"https://www.sqlite.org/2026/{SQLITE_ARCHIVE}.zip"
# Published at https://www.sqlite.org/download.html; never trust an unpinned download.
SQLITE_SHA3 = "7b670a62fdfbd672b75fef004cb703c8a3e87d3a5cc7d675b4a08337004a2d93"
MAX_ARCHIVE_BYTES = 16 * 1024 * 1024

PROBE = r'''
import _sqlite3, json, os, platform, sqlite3, sys
from pathlib import Path
result = dict(python=platform.python_version(), executable=sys.executable,
              pid=os.getpid(),
              sqlite=sqlite3.sqlite_version, extension=getattr(_sqlite3, '__file__', '<built-in>'),
              library_path_set=bool(os.environ.get('LD_LIBRARY_PATH')),
              preload_set=bool(os.environ.get('LD_PRELOAD')), libraries=[])
if sys.platform == 'linux':
    result['libraries'] = sorted({line.split(maxsplit=5)[-1]
        for line in Path('/proc/self/maps').read_text().splitlines()
        if 'libsqlite3' in line and len(line.split(maxsplit=5)) == 6})
try:
    with sqlite3.connect(':memory:') as db:
        payload = json.dumps(dict(value=1))
        assert db.execute('SELECT ? -> ?, ? ->> ?',
            (payload, '$.value', payload, '$.value')).fetchone() == ('1', 1)
        db.execute('CREATE TABLE ci_probe (payload_json TEXT NOT NULL)')
        db.execute("CREATE INDEX ci_json_index ON ci_probe "
                   "(CAST(payload_json -> '$.value' AS TEXT) || '')")
        db.execute('INSERT INTO ci_probe VALUES (?)', (payload,))
        assert db.execute("SELECT payload_json ->> '$.value' FROM ci_probe").fetchone() == (1,)
    result['supported'] = True
except (sqlite3.Error, AssertionError) as error:
    result.update(supported=False, error=str(error))
print(json.dumps(result, sort_keys=True))
'''


def probe(environment: dict[str, str]) -> dict:
    # A fresh process is essential: changing a loader path cannot replace a
    # SQLite library already loaded by the parent Python interpreter.
    result = subprocess.run(
        [sys.executable, "-I", "-c", PROBE], env=environment,
        check=False, capture_output=True, text=True,
    )
    diagnostic = f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    if result.returncode:
        raise RuntimeError(f"SQLite probe failed with exit code {result.returncode}\n{diagnostic}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"SQLite probe returned invalid JSON\n{diagnostic}") from error


def job_root(environment: dict[str, str]) -> Path:
    root = Path(environment["FCP_CI_JOB_ROOT"])
    parent = Path(environment["RUNNER_TEMP"]).resolve(strict=True)
    if (not root.is_absolute() or root.is_symlink()
            or root.resolve(strict=True).parent != parent
            or not re.fullmatch(r"fcp-job-[A-Za-z0-9]+", root.name)):
        raise RuntimeError("SQLite build requires the unique job-owned temporary directory")
    return root.resolve(strict=True)


def extract_source(payload: bytes, directory: Path) -> None:
    if hashlib.sha3_256(payload).hexdigest() != SQLITE_SHA3:
        raise RuntimeError("SQLite source SHA3-256 verification failed")
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        # Copy only named source files; archive paths never become output paths.
        for name in ("sqlite3.c", "sqlite3.h", "sqlite3ext.h"):
            member = f"{SQLITE_ARCHIVE}/{name}"
            if archive.namelist().count(member) != 1:
                raise RuntimeError(f"Missing or duplicate SQLite source member: {name}")
            (directory / name).write_bytes(archive.read(member))


def build_library(root: Path) -> Path:
    compiler = shutil.which("cc") or shutil.which("gcc")
    if not compiler:
        raise RuntimeError("A preinstalled C compiler is required for job-local SQLite")
    directory = root / f"sqlite-{SQLITE_VERSION}"
    directory.mkdir()  # Refuse an existing directory or symlink.
    with urllib.request.urlopen(SQLITE_URL, timeout=60) as response:
        payload = response.read(MAX_ARCHIVE_BYTES + 1)
    if len(payload) > MAX_ARCHIVE_BYTES:
        raise RuntimeError("SQLite source archive exceeds the bounded download size")
    extract_source(payload, directory)
    library = directory / "libsqlite3.so.0"
    subprocess.run([
        compiler, "-O2", "-fPIC", "-shared", "-DSQLITE_THREADSAFE=1",
        "-DSQLITE_ENABLE_COLUMN_METADATA", "-DSQLITE_ENABLE_FTS5",
        "-DSQLITE_ENABLE_RTREE", "-DSQLITE_ENABLE_MATH_FUNCTIONS",
        "-DSQLITE_SECURE_DELETE", "-Wl,-soname,libsqlite3.so.0",
        "-o", str(library), str(directory / "sqlite3.c"), "-lm", "-ldl", "-lpthread",
    ], check=True)
    print(f"Built job-local SQLite {SQLITE_VERSION} from {SQLITE_URL}; SHA3-256={SQLITE_SHA3}")
    return library


def library_environment(library: Path, environment: dict[str, str]) -> dict[str, str]:
    selected = environment.copy()
    inherited = selected.get("LD_LIBRARY_PATH", "")
    selected["LD_LIBRARY_PATH"] = str(library.parent) + (f":{inherited}" if inherited else "")
    if any(character in selected["LD_LIBRARY_PATH"] for character in "\r\n"):
        raise RuntimeError("Invalid newline in the job library search path")
    # Preserve LD_PRELOAD and every other inherited security setting verbatim.
    return selected


def prepare(*, probe_only: bool = False) -> None:
    environment = dict(os.environ)
    original = probe(environment)
    print("SQLite runtime before setup: " + json.dumps(original, sort_keys=True), flush=True)
    if original["python"] != platform.python_version():
        raise RuntimeError("SQLite probe changed the pinned Python version")
    if original["supported"]:
        return
    if probe_only or sys.platform != "linux":
        raise RuntimeError("The pinned Python runtime lacks the required SQLite JSON operators/index")
    library = build_library(job_root(environment))
    selected = library_environment(library, environment)
    qualified = probe(selected)
    print("SQLite runtime after setup: " + json.dumps(qualified, sort_keys=True), flush=True)
    if (not qualified["supported"] or qualified["python"] != original["python"]
            or qualified["sqlite"] != SQLITE_VERSION
            or str(library.resolve()) not in qualified["libraries"]):
        raise RuntimeError("The exact Python interpreter did not select the qualified job-local SQLite")
    with Path(environment["GITHUB_ENV"]).open("a", encoding="utf-8") as output:
        output.write(f"LD_LIBRARY_PATH={selected['LD_LIBRARY_PATH']}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe-only", action="store_true")
    prepare(probe_only=parser.parse_args().probe_only)
