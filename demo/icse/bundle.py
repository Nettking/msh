"""Build and validate the publication bundle for the ICSE tool artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import zipfile
from pathlib import Path

SUMMARY_SCHEMA = "fcp.icse-demo-summary.v1"
MANIFEST_SCHEMA = "fcp.icse-artifact-manifest.v1"
TAG_PREFIX = "fcp-icse-tool-demo-v"
EXPECTED_SCENARIOS = (
    "E1-selective-contribution",
    "E2-authority-boundary",
    "E3-runtime-eligibility",
    "E4-ownership-boundary",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_version(version: str) -> str:
    return version.removeprefix(TAG_PREFIX)


def _load_summary(path: Path, source_revision: str) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != SUMMARY_SCHEMA:
        raise ValueError(f"{path}: unexpected summary schema")
    if payload.get("implementation_commit") != source_revision:
        raise ValueError(f"{path}: evidence does not match source revision")
    if payload.get("passed") != 4 or payload.get("total") != 4:
        raise ValueError(f"{path}: expected four passing scenarios")

    scenarios = payload.get("scenarios")
    if not isinstance(scenarios, list):
        raise TypeError(f"{path}: scenarios must be a list")
    names = tuple(item.get("scenario") for item in scenarios if isinstance(item, dict))
    if names != EXPECTED_SCENARIOS:
        raise ValueError(f"{path}: unexpected scenario set or ordering")
    if any(item.get("result") != "pass" for item in scenarios):
        raise ValueError(f"{path}: at least one scenario did not pass")
    return payload


def _write_checksums(output: Path) -> None:
    lines: list[str] = []
    checksum_path = output / "SHA256SUMS"
    for path in sorted(p for p in output.rglob("*") if p.is_file()):
        if path == checksum_path:
            continue
        relative = path.relative_to(output).as_posix()
        lines.append(f"{_sha256(path)}  {relative}")
    checksum_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _create_publication_archive(
    *,
    source_revision: str,
    version: str,
    output: Path,
) -> tuple[str, str]:
    archive_name = f"fcp-icse-tool-demo-{version}.zip"
    archive_path = output / archive_name
    prefix = f"fcp-icse-tool-demo-{version}/"

    subprocess.run(
        [
            "git",
            "archive",
            "--format=zip",
            f"--prefix={prefix}source/",
            "-o",
            str(archive_path),
            source_revision,
        ],
        check=True,
    )

    with zipfile.ZipFile(
        archive_path,
        mode="a",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
    ) as archive:
        for path in sorted(p for p in output.rglob("*") if p.is_file()):
            if path == archive_path or path.name == "ZENODO_SHA256":
                continue
            relative = path.relative_to(output).as_posix()
            archive.write(path, f"{prefix}artifact/{relative}")

    archive_digest = _sha256(archive_path)
    (output / "ZENODO_SHA256").write_text(
        f"{archive_digest}  {archive_name}\n",
        encoding="utf-8",
    )
    return archive_name, archive_digest


def build_bundle(
    *,
    source_revision: str,
    source_ref: str,
    workflow_run: str,
    version: str,
    evidence_root: Path,
    output: Path,
) -> None:
    canonical_version = _canonical_version(version)
    archive_name = f"fcp-icse-tool-demo-{canonical_version}.zip"
    archive_prefix = f"fcp-icse-tool-demo-{canonical_version}/"

    output.mkdir(parents=True, exist_ok=True)
    evidence_output = output / "evidence"
    evidence_output.mkdir(parents=True, exist_ok=True)

    evidence_entries: list[dict[str, object]] = []
    artifact_dirs = sorted(path for path in evidence_root.iterdir() if path.is_dir())
    if len(artifact_dirs) != 3:
        raise ValueError(f"expected three evidence artifacts, found {len(artifact_dirs)}")

    for artifact_dir in artifact_dirs:
        source = artifact_dir / "icse-summary.json"
        if not source.is_file():
            raise ValueError(f"{artifact_dir}: missing icse-summary.json")
        payload = _load_summary(source, source_revision)
        label = artifact_dir.name.removeprefix("icse-summary-")
        target = evidence_output / f"{label}.json"
        shutil.copyfile(source, target)
        evidence_entries.append(
            {
                "execution": label,
                "file": target.relative_to(output).as_posix(),
                "sha256": _sha256(target),
                "summary_schema": payload["schema"],
                "scenarios": list(EXPECTED_SCENARIOS),
            }
        )

    shutil.copyfile("CITATION.cff", output / "CITATION.cff")
    shutil.copyfile("demo/icse/README.md", output / "README.md")
    shutil.copyfile("demo/icse/ARTIFACT.md", output / "ARTIFACT.md")

    license_files = sorted(path.name for path in Path(".").glob("LICENSE*") if path.is_file())
    manifest = {
        "schema": MANIFEST_SCHEMA,
        "artifact": "FCP ICSE Tool Demonstration",
        "artifact_version": canonical_version,
        "source_revision": source_revision,
        "source_ref": source_ref,
        "workflow_run": workflow_run,
        "summary_schema": SUMMARY_SCHEMA,
        "scenarios": list(EXPECTED_SCENARIOS),
        "evidence": evidence_entries,
        "publication_archive": {
            "file": archive_name,
            "source_prefix": f"{archive_prefix}source/",
            "artifact_prefix": f"{archive_prefix}artifact/",
            "sha256_sidecar": "ZENODO_SHA256",
        },
        "citation_file": "CITATION.cff",
        "license": {
            "status": "declared" if license_files else "not-declared",
            "files": license_files,
        },
    }
    (output / "artifact-manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_checksums(output)
    _create_publication_archive(
        source_revision=source_revision,
        version=canonical_version,
        output=output,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--source-ref", required=True)
    parser.add_argument("--workflow-run", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--evidence-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    build_bundle(
        source_revision=args.source_revision,
        source_ref=args.source_ref,
        workflow_run=args.workflow_run,
        version=args.version,
        evidence_root=args.evidence_root,
        output=args.output,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
