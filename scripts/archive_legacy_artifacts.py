"""Copy reviewed, already retained GitHub artifacts byte-for-byte to Nitro.

The plan must bind every original artifact to its native job, attempt and tested
checkout. This tool does not guess missing provenance, delete originals, or admit
qualification. It fetches every uploaded package back before marking it copied.
"""

import argparse
import json
import shutil
from pathlib import Path

from scripts import artifact_archive as archive


def migrate(config, plan, output):
    if not config["host"].startswith("fcp-archive@"):
        raise ValueError(
            "Release evidence requires the dedicated production archive account"
        )
    output.mkdir(parents=True, exist_ok=True)
    results = []
    for item in plan["artifacts"]:
        artifact = item["github_artifact"]
        source = Path(item["original_zip"])
        digest = archive.sha_file(source)
        if digest != item["original_zip_sha256"]:
            raise ValueError("Retained original bytes changed")
        if artifact.get("digest") and artifact["digest"] != "sha256:" + digest:
            raise ValueError("Original GitHub digest does not match retained ZIP")
        metadata = item["metadata"]
        if metadata["native_github_job"]["id"] != item["native_job_id"]:
            raise ValueError("Native job identity missing")
        if item["native_checkout_commits"] != [metadata["tested_sha"]]:
            raise ValueError("Original checkout identity not proven")
        native_log = Path(item["native_job_log"])
        if archive.sha_file(native_log) != item["native_job_log_sha256"]:
            raise ValueError("Native log bytes changed")
        local = output / str(artifact["id"])
        if not local.exists():
            local.mkdir()
            inputs = local / "inputs"
            inputs.mkdir()
            shutil.copyfile(source, inputs / "original-github-artifact.zip")
            shutil.copyfile(native_log, inputs / "original-native-job.log")
            (inputs / "original-github-metadata.json").write_bytes(
                archive.canonical(artifact)
            )
            (inputs / "source-binding.json").write_bytes(archive.canonical(item))
            archive.build_package(metadata, [str(inputs)], local / "package")
        receipt = archive.upload(config, local / "package")
        retrieved = local / "retrieved"
        if not retrieved.exists():
            archive.fetch(config, receipt, retrieved)
            archive.extract(retrieved, local / "verified-files")
        if (
            archive.sha_file(local / "verified-files/original-github-artifact.zip")
            != digest
        ):
            raise ValueError("Retrieved original ZIP differs")
        row = {
            "artifact_id": artifact["id"],
            "original_digest": artifact.get("digest"),
            "original_zip_sha256": digest,
            "receipt": receipt,
            "retrieved_verified": True,
            "github_original_not_deleted": True,
        }
        (local / "verified.json").write_bytes(archive.canonical(row))
        results.append(row)
    result = {
        "status": "COPIED_AND_RETRIEVED_VERIFIED",
        "artifacts": results,
        "deletion_authorized": False,
        "qualification_status": "NOT_EVALUATED",
    }
    (output / "migration.json").write_bytes(archive.canonical(result))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = migrate(
        json.loads(Path(args.config).read_bytes()),
        json.loads(Path(args.plan).read_bytes()),
        args.output,
    )
    print(result["status"], len(result["artifacts"]))
