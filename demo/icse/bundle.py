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
NETWORK_SUMMARY_SCHEMA = "fcp.icse-network-demo.v1"
NETWORK_EXECUTIONS = ("Linux", "Windows")
NETWORK_PUBLIC_FILES = ("summary.json", "events.jsonl", "operator-report.html")
NETWORK_REQUIRED_CHECKS = (
    "independent_processes",
    "authenticated_quorum_bootstrap",
    "authenticated_enrollment_and_join",
    "discovery_and_owner_authorization",
    "authenticated_payload_delivery",
    "automatic_quorum_failover_and_continuity",
    "successor_reconnect_and_delivery",
    "returning_leader_fenced",
    "minority_mutation_refused",
    "source_identity_unchanged",
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


def _load_network_summary(path: Path, source_revision: str) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema") != NETWORK_SUMMARY_SCHEMA:
        raise ValueError(f"{path}: unexpected network summary schema")
    if payload.get("source_sha") != source_revision:
        raise ValueError(f"{path}: network evidence does not match source revision")
    if payload.get("result") != "PASS":
        raise ValueError(f"{path}: network demonstration did not pass")
    checks = payload.get("checks")
    if (
        not isinstance(checks, dict)
        or not set(NETWORK_REQUIRED_CHECKS).issubset(checks)
        or any(value != "PASS" for value in checks.values())
    ):
        raise ValueError(f"{path}: incomplete or failing network checks")
    if payload.get("all_owned_processes_stopped") is not True:
        raise ValueError(f"{path}: network process teardown did not complete")
    if payload.get("failure") or payload.get("shutdown_errors"):
        raise ValueError(f"{path}: network summary contains a failure")
    events = payload.get("events")
    if not isinstance(events, list) or not events or not all(isinstance(e, dict) for e in events):
        raise ValueError(f"{path}: network observations are missing or malformed")
    return payload


def _linked_path(path: Path) -> bool:
    return path.is_symlink() or path.is_junction()


def _copy_network_evidence(
    evidence_root: Path, source_revision: str, output: Path
) -> list[dict[str, object]]:
    """Validate both public-only network artifacts before copying their files."""
    if _linked_path(evidence_root) or not evidence_root.is_dir():
        raise ValueError("network evidence root must be an ordinary directory")
    artifacts = sorted(evidence_root.iterdir())
    expected = {f"icse-network-summary-{label}" for label in NETWORK_EXECUTIONS}
    if {path.name for path in artifacts} != expected:
        raise ValueError("expected exactly Linux and Windows network evidence artifacts")

    validated: list[tuple[str, Path]] = []
    for artifact in artifacts:
        if _linked_path(artifact) or not artifact.is_dir():
            raise ValueError(f"{artifact}: network artifact must be an ordinary directory")
        files = list(artifact.iterdir())
        if {path.name for path in files} != set(NETWORK_PUBLIC_FILES):
            raise ValueError(f"{artifact}: expected only the three public network files")
        if any(_linked_path(path) or not path.is_file() for path in files):
            raise ValueError(f"{artifact}: public network files must be ordinary files")
        summary = _load_network_summary(artifact / "summary.json", source_revision)
        events = [
            json.loads(line)
            for line in (artifact / "events.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        if events != summary["events"]:
            raise ValueError(f"{artifact}: network event log does not match summary")
        if not (artifact / "operator-report.html").read_text(encoding="utf-8").strip():
            raise ValueError(f"{artifact}: network operator report is empty")
        validated.append((artifact.name.removeprefix("icse-network-summary-"), artifact))

    entries: list[dict[str, object]] = []
    for label, artifact in validated:
        destination = output / "network-evidence" / label
        destination.mkdir(parents=True)
        files = []
        for name in NETWORK_PUBLIC_FILES:
            target = destination / name
            shutil.copyfile(artifact / name, target)
            files.append({"file": target.relative_to(output).as_posix(), "sha256": _sha256(target)})
        entries.append({
            "execution": label,
            "summary_schema": NETWORK_SUMMARY_SCHEMA,
            "source_revision": source_revision,
            "checks": list(NETWORK_REQUIRED_CHECKS),
            "files": files,
        })
    return entries


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
    network_evidence_root: Path | None = None,
) -> None:
    canonical_version = _canonical_version(version)
    archive_name = f"fcp-icse-tool-demo-{canonical_version}.zip"
    archive_prefix = f"fcp-icse-tool-demo-{canonical_version}/"

    if network_evidence_root is not None and output.exists() and any(output.iterdir()):
        raise ValueError("network publication bundle output must be new or empty")
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

    network_entries = (
        _copy_network_evidence(network_evidence_root, source_revision, output)
        if network_evidence_root is not None
        else []
    )

    shutil.copyfile("CITATION.cff", output / "CITATION.cff")
    shutil.copyfile("demo/icse/README.md", output / "README.md")
    shutil.copyfile("demo/icse/ARTIFACT.md", output / "ARTIFACT.md")
    for name in ("DEMONSTRATION.md", "ARCHITECTURE.md", "VIDEO.md", "RELEASE.md"):
        shutil.copyfile(Path("demo/icse") / name, output / name)
    (output / "network").mkdir(exist_ok=True)
    shutil.copyfile("demo/icse/network/README.md", output / "network/README.md")
    (output / "figures").mkdir(exist_ok=True)
    shutil.copyfile(
        "demo/icse/figures/federation-v1-overview.svg",
        output / "figures/federation-v1-overview.svg",
    )

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
        "network_evidence": network_entries,
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
    parser.add_argument("--network-evidence-root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    build_bundle(
        source_revision=args.source_revision,
        source_ref=args.source_ref,
        workflow_run=args.workflow_run,
        version=args.version,
        evidence_root=args.evidence_root,
        output=args.output,
        network_evidence_root=args.network_evidence_root,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
