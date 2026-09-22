"""Bounded original shard-2 retention audit. No network, tests, or source writes.

Run once on AQG7NCC-Linux before checkout, with a fresh read-only GitHub job
API response supplied by the caller. Only --output is written; it must be new.
This does not create an archive manifest, producer identity, or qualification.
"""

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import sys
import xml.etree.ElementTree as ET
import zipfile

sys.dont_write_bytecode = True
BASE = Path("/home/fcp-ci-aqg7ncc/actions-runner/_work")
PENDING = BASE / "fcp-archive-pending/9330ad7deb3a485ab980b920f71ec0a6"
TEMP = BASE / "_temp"
RUN = 35732223087
JOB = 106760397484
HEAD = "5d5d7e8f215717cf9b7012d3b357eaf03a1400c9"
MERGE = "e9a23147795b73eea7bb73f3f0b4e416e3e3b6fd"
TREE = "b9f70b26495fc3174d10cd60c93548f45a2ab466"
TOOL = "a7c41a7d6c5a47a9b159ed5cde931fbb82c99385b38c786e91583d90a22e6b93"
FILES = {"junit-2.xml", "shard-2.json"}
LIMIT = (
    32 * 1024**2
)  # Bounded audit; larger package needs separate review, never a partial read.


def require(ok, message):
    if not ok:
        raise ValueError(message)


def canonical(value):
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read_regular(path, limit=8 * 1024**2):
    path = Path(path).absolute()
    for part in (path, *path.parents):
        require(not part.is_symlink(), f"Symlink forbidden: {part}")
    before = path.stat()
    require(
        stat.S_ISREG(before.st_mode) and before.st_size <= limit,
        f"Invalid/big file: {path}",
    )
    with path.open("rb") as stream:
        content = stream.read(limit + 1)
    after = path.stat()
    require(
        (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns),
        f"File changed: {path}",
    )
    require(len(content) == before.st_size, f"Incomplete read: {path}")
    return content


def preserve(path, output, relative, limit=8 * 1024**2):
    content = read_regular(path, limit)
    target = output / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("xb") as stream:
        stream.write(content)
    require(
        read_regular(path, limit) == content,
        f"Original changed after preservation: {path}",
    )
    return content, {
        "path": str(path),
        "audit_copy": str(target),
        "size": len(content),
        "sha256": digest(content),
    }


def original_native_job(path):
    raw = read_regular(path)
    job = json.loads(raw)
    expected = {
        "id": JOB,
        "run_id": RUN,
        "run_attempt": 1,
        "head_sha": HEAD,
        "runner_id": 31,
        "runner_name": "AQG7NCC-Linux",
        "name": "Linux regression shard (2)",
        "status": "completed",
        "conclusion": "failure",
    }
    for key, value in expected.items():
        require(job.get(key) == value, f"Original GitHub job mismatch: {key}")
    require(
        job.get("started_at") == "2026-09-22T13:45:14Z", "Original job start changed"
    )
    require(
        job.get("completed_at") == "2026-09-22T13:51:33Z", "Original job end changed"
    )
    return job, raw


def inspect_pending(output, report):
    raw, info = preserve(
        PENDING / "INCOMPLETE.json", output, "pending-original/INCOMPLETE.json"
    )
    report["pending_receipt_original"] = info
    receipt = json.loads(raw)
    expected = {
        "schema": "fcp.local-incomplete-evidence.v1",
        "archive_complete": False,
        "qualification_status": "NOT_EVALUATED",
        "repo": "Nettking/msh",
        "tested_sha": MERGE,
        "run_id": str(RUN),
        "run_attempt": "1",
        "job": "linux-regressions",
        "artifact": "linux-regression-shard-2",
        "native_job_id": None,
        "native_job_binding": "NOT_VERIFIED_BY_LOCAL_FAILURE_RETENTION",
        "workflow_sha": MERGE,
        "workflow_ref": "Nettking/msh/.github/workflows/federation-v1-release.yml@refs/pull/497/merge",
    }
    for key, value in expected.items():
        require(receipt.get(key) == value, f"Pending receipt mismatch: {key}")
    require(
        receipt.get("missing_patterns") == [],
        "Original pending receipt had missing inputs",
    )
    rows = receipt.get("files", [])
    require(
        len(rows) == 2 and {row["path"] for row in rows} == FILES,
        "Pending inventory is not exactly two inputs",
    )
    data = {}
    report["pending_files"] = []
    for row in rows:
        name = row["path"]
        raw, info = preserve(
            PENDING / "files" / name, output, "pending-original/files/" + name
        )
        report["pending_files"].append(info)
        require(
            info["size"] == row["size"] and info["sha256"] == row["sha256"],
            f"Pending hash mismatch: {name}",
        )
        data[name] = raw
    report["pending_inputs_valid"] = True
    return rows, data


def inspect_contents(data):
    shard = json.loads(data["shard-2.json"])
    identity = {"sha": MERGE, "tree": TREE}
    for key, value in {
        "schema": 2,
        "shard_index": 2,
        "shard_count": 4,
        "source_sha": MERGE,
        "source_identity_before": identity,
        "source_identity_after": identity,
        "source_error": None,
        "collection_complete": True,
        "exit_code": 0,
    }.items():
        require(shard.get(key) == value, f"Shard content mismatch: {key}")
    require(
        len(shard["collected"]) == len(set(shard["collected"])) == 4678,
        "Collected suite mismatch",
    )
    require(
        shard["selected"] == shard["executed"]
        and len(shard["selected"]) == len(set(shard["selected"])) == 1169,
        "Selected/executed shard mismatch",
    )
    root = ET.fromstring(data["junit-2.xml"])
    cases = list(root.iter("testcase"))
    totals = {
        "tests": len(cases),
        "failures": sum(len(c.findall("failure")) for c in cases),
        "errors": sum(len(c.findall("error")) for c in cases),
        "skipped": sum(len(c.findall("skipped")) for c in cases),
    }
    require(
        totals == {"tests": 1169, "failures": 0, "errors": 0, "skipped": 3},
        "JUnit content mismatch",
    )
    return {
        "junit": totals,
        "passed": 1166,
        "collected": 4678,
        "full_four_shard_contract_verification": "STILL_REQUIRED; this inspection does not replace it",
    }


def candidate_spools():
    require(not TEMP.is_symlink(), "Temp directory is a symlink")
    result = []
    with os.scandir(TEMP) as entries:
        for count, entry in enumerate(entries, 1):
            require(
                count <= 4096,
                "Top-level temp inventory exceeds bound; no partial conclusion",
            )
            if entry.name.startswith("fcp-evidence-") and entry.is_dir(
                follow_symlinks=False
            ):
                require(len(result) < 128, "Spool candidate count exceeds bound")
                result.append(Path(entry.path))
    return sorted(result)


def verify_package(path, manifest_raw, native, pending_rows, data, output):
    manifest = json.loads(manifest_raw)
    require(manifest_raw == canonical(manifest), "Original manifest is not canonical")
    for key, value in {
        "schema": "fcp.ssh-artifact.v1",
        "repo": "Nettking/msh",
        "tested_sha": MERGE,
        "run_id": str(RUN),
        "run_attempt": 1,
        "job": "linux-regressions",
        "artifact": "linux-regression-shard-2",
        "matrix": {"shard": 2},
        "runner": "AQG7NCC-Linux",
        "tool_sha256": TOOL,
        "workflow_sha": MERGE,
        "event_sha": MERGE,
        "workflow_ref": "Nettking/msh/.github/workflows/federation-v1-release.yml@refs/pull/497/merge",
        "provenance_kind": "github-actions-checkout",
        "job_status_at_archive": "success",
    }.items():
        require(manifest.get(key) == value, f"Package manifest mismatch: {key}")
    binding = manifest.get("native_github_job", {})
    for key in (
        "id",
        "name",
        "run_id",
        "run_attempt",
        "head_sha",
        "runner_id",
        "runner_name",
        "started_at",
    ):
        require(
            binding.get(key) == native[key], f"Native producer binding mismatch: {key}"
        )

    def parse_time(value):
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))

    require(
        parse_time(native["started_at"])
        <= parse_time(manifest["archived_at"])
        <= parse_time(native["completed_at"]),
        "Original archive timestamp outside native job lifetime",
    )
    rows = [
        {key: row[key] for key in ("path", "size", "sha256")} for row in pending_rows
    ]
    require(
        sorted(manifest["files"], key=lambda row: row["path"])
        == sorted(rows, key=lambda row: row["path"]),
        "Original manifest/input inventory mismatch",
    )
    bundle_raw = read_regular(path / "bundle.zip", LIMIT)
    require(
        shutil.disk_usage(output).free >= 66 * 1024**3 + len(bundle_raw),
        "Physical/operator capacity reserve not met",
    )
    with zipfile.ZipFile(path / "bundle.zip") as bundle:
        infos = bundle.infolist()
        require(
            len(infos) == 2 and {item.filename for item in infos} == FILES,
            "ZIP inventory mismatch",
        )
        for item in infos:
            require(
                item.compress_type == zipfile.ZIP_STORED
                and not item.is_dir()
                and not stat.S_ISLNK(item.external_attr >> 16),
                "Invalid ZIP member",
            )
            require(
                bundle.read(item) == data[item.filename],
                "ZIP does not contain exact retained input bytes",
            )
    require(
        read_regular(path / "bundle.zip", LIMIT) == bundle_raw,
        "Original ZIP changed during verification",
    )
    preserved = []
    for name in ("manifest.json", "bundle.zip"):
        _, info = preserve(path / name, output, "original-package/" + name, LIMIT)
        preserved.append(info)
    receipt_files = []
    with os.scandir(path) as entries:
        for count, entry in enumerate(entries, 1):
            require(count <= 32, "Original spool entries exceed bound")
            if entry.name in ("COMPLETE.json", "receipt.json") or (
                entry.name.startswith("transfer-") and entry.name.endswith(".json")
            ):
                _, info = preserve(
                    Path(entry.path), output, "original-package/" + entry.name
                )
                receipt_files.append(info)
    return {
        "source_directory": str(path),
        "manifest_sha256": digest(manifest_raw),
        "zip_sha256": digest(bundle_raw),
        "zip_size": len(bundle_raw),
        "preserved_files": preserved,
        "original_receipts_or_transfer_outputs": receipt_files,
        "original_native_identity_verified_against_separate_api_response": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-job-json", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output).absolute()
    require(not output.exists(), "Audit output must be new")
    require(
        not output.is_relative_to(PENDING) and not output.is_relative_to(TEMP),
        "Audit output must be outside original pending/temp",
    )
    require(
        shutil.disk_usage(output.parent).free >= 66 * 1024**3 + 32 * 1024**2,
        "Physical/operator capacity reserve not met",
    )
    output.mkdir(mode=0o700)
    report = {
        "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "repo": "Nettking/msh",
        "run_id": RUN,
        "run_attempt": 1,
        "native_job_id": JOB,
        "head_sha": HEAD,
        "tested_sha": MERGE,
        "runner": "AQG7NCC-Linux",
        "pending_path": str(PENDING),
        "archive_replay_eligible": False,
        "qualification_status": "NOT_EVALUATED",
        "no_tests_run": True,
        "no_original_files_written": True,
        "no_network_operations": True,
    }
    try:
        native, raw = original_native_job(args.native_job_json)
        (output / "original-native-job-api.json").write_bytes(raw)
        report["native_api_response_sha256"] = digest(raw)
        pending_rows, data = inspect_pending(output, report)
        report["content_observations"] = inspect_contents(data)
        report["spool_scan"] = {
            "root": str(TEMP),
            "depth": 1,
            "recursive": False,
            "candidates": [],
        }
        matching = []
        for path in candidate_spools():
            entry = {"path": str(path)}
            report["spool_scan"]["candidates"].append(entry)
            try:
                raw = read_regular(path / "manifest.json")
                manifest = json.loads(raw)
                match = (
                    manifest.get("run_id") == str(RUN)
                    and manifest.get("run_attempt") == 1
                    and manifest.get("job") == "linux-regressions"
                    and manifest.get("matrix") == {"shard": 2}
                )
                entry["matches_original_identity_selector"] = match
                if match:
                    matching.append((path, raw))
            except (OSError, ValueError) as exc:
                entry["read_error"] = str(exc)
        require(
            len(matching) == 1,
            f"Expected one original completed package; found {len(matching)}. Do not rebuild from inputs.",
        )
        report["original_package"] = verify_package(
            *matching[0], native, pending_rows, data, output
        )
        report["archive_replay_eligible"] = True
        report["decision"] = (
            "Exact original manifest/ZIP verified; root review required before any existing-transport replay."
        )
    except Exception as exc:
        report["decision"] = "FAIL_CLOSED_NO_UPLOAD_NO_REBUILD"
        report["limitation"] = f"{type(exc).__name__}: {exc}"
    (output / "inspection.json").write_bytes(canonical(report))
    print(json.dumps(report, sort_keys=True))
    return 0 if report["archive_replay_eligible"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
