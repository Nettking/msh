"""Run the exact ICSE artifact in the prebuilt, network-isolated container."""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

INPUT = Path("/input")
OUTPUT = Path("/output")

def require(ok: bool, reason: str) -> None:
    if not ok:
        raise ValueError(reason)

def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

def verified_archive() -> tuple[Path, str]:
    require(INPUT.is_dir() and OUTPUT.is_dir(), "mount read-only /input and writable /output")
    sidecar = INPUT / "ZENODO_SHA256"
    require(sidecar.is_file(), "missing canonical ZENODO_SHA256")
    record = re.fullmatch(
        r"([0-9a-f]{64})  (fcp-icse-tool-demo-[A-Za-z0-9._-]+\.zip)",
        sidecar.read_text(encoding="utf8").strip(),
    )
    require(record is not None, "invalid checksum sidecar")
    expected, name = record.groups()
    archive = INPUT / name
    require(archive.is_file() and not archive.is_symlink(), "missing publication ZIP")
    require(digest(archive) == expected, "publication ZIP digest mismatch")
    return archive, expected

def extract_checked(archive: Path, destination: Path) -> Path:
    """Only regular files inside a single canonical root, with bounded expansion."""
    root = archive.stem
    seen: set[str] = set()
    total = 0
    with zipfile.ZipFile(archive) as zipped:
        for entry in zipped.infolist():
            name = entry.filename.rstrip("/")
            require(name and "\\" not in name and not name.startswith("/"), "unsafe archive name")
            parts = PurePosixPath(name).parts
            require(parts and parts[0] == root and all(p not in ("", ".", "..") for p in parts),
                    "archive path outside root")
            require(name.casefold() not in seen, "duplicate archive path")
            seen.add(name.casefold())
            kind = stat.S_IFMT((entry.external_attr >> 16) & 0xFFFF)
            require(kind in (0, stat.S_IFREG, stat.S_IFDIR), "archive link or special file")
            require(not entry.flag_bits & 1, "encrypted archive entry")
            require(entry.file_size <= 128 * 1024 * 1024, "oversized archive entry")
            total += entry.file_size
            require(total <= 1024 * 1024 * 1024, "archive expansion limit exceeded")
            target = destination.joinpath(*parts)
            if entry.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with zipped.open(entry) as src, target.open("xb") as dst:
                    shutil.copyfileobj(src, dst)
    extracted = destination / root
    require((extracted / "source/demo/icse/network/run.py").is_file(), "missing network source")
    require((extracted / "artifact/artifact-manifest.json").is_file(), "missing manifest")
    return extracted

def run(source: Path, args: list[str], env: dict[str, str]) -> None:
    result = subprocess.run([sys.executable, "-B", "-m", *args], cwd=source, env=env, check=False)
    require(result.returncode == 0, "demonstration failed; do not interpret as PASS")

def main() -> None:
    require(sys.version_info[:2] == (3, 12), "Python 3.12 required")
    archive, archive_digest = verified_archive()
    with tempfile.TemporaryDirectory(prefix="fcp-icse-reviewer-") as directory:
        base = extract_checked(archive, Path(directory))
        manifest = json.loads((base / "artifact/artifact-manifest.json").read_text(encoding="utf8"))
        require(manifest.get("schema") == "fcp.icse-artifact-manifest.v1", "manifest schema mismatch")
        sha = manifest.get("source_revision")
        require(isinstance(sha, str) and re.fullmatch("[a-f0-9]{40}", sha) is not None, "source SHA missing")
        require(sha == os.environ.get("FCP_BUILD_COMMIT"), "runtime image and source SHA mismatch")
        require(manifest.get("publication_archive", {}).get("sha256_sidecar") == "ZENODO_SHA256",
                "manifest checksum metadata mismatch")
        require(not any(OUTPUT.iterdir()), "use a new empty results directory")
        source = base / "source"
        env = dict(os.environ, FCP_BUILD_COMMIT=sha, PYTHONDONTWRITEBYTECODE="1")
        components = OUTPUT / "components.json"
        run(source, ["demo.icse.run", "--output", str(components)], env)
        component_summary = json.loads(components.read_text(encoding="utf8"))
        require(component_summary.get("passed") == 4 and component_summary.get("total") == 4
                and component_summary.get("implementation_commit") == sha, "component evidence failed")
        private_run = Path(directory) / "network-run"
        command = [
            "demo.icse.network.run", "--source-sha", sha,
            "--source-archive", str(archive), "--archive-sha256", archive_digest,
            "--output", str(private_run),
        ]
        ref = manifest.get("source_ref")
        if isinstance(ref, str) and ref.startswith("refs/tags/fcp-icse-tool-demo-v"):
            command.extend(["--release-tag", ref.removeprefix("refs/tags/")])
        run(source, command, env)
        public = private_run / "public"
        network = json.loads((public / "summary.json").read_text(encoding="utf8"))
        checks = network.get("checks")
        require(
            network.get("result") == "PASS" and network.get("source_sha") == sha
            and network.get("all_owned_processes_stopped") is True
            and isinstance(checks, dict) and len(checks) == 10
            and all(result == "PASS" for result in checks.values()),
            "network evidence failed",
        )
        shutil.copytree(public, OUTPUT / "network")
        (OUTPUT / "PROVENANCE.json").write_text(
            json.dumps({
                "source_sha": sha,
                "source_ref": ref,
                "archive_sha256": archive_digest,
                "component_results": "4/4 PASS",
                "network_checks": "10/10 PASS",
                "scope": "same-host independent processes and synthetic data; not physical acceptance",
            }, indent=2, sort_keys=True) + "\n", encoding="utf8"
        )
    print("FCP_ICSE_REVIEWER=PASS; open results/network/operator-report.html", flush=True)

if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, zipfile.BadZipFile, subprocess.SubprocessError) as error:
        print(f"FCP_ICSE_REVIEWER=FAIL: {error}", file=sys.stderr, flush=True)
        sys.exit(1)
