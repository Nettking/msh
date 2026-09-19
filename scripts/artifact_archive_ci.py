"""Actions adapter. Missing credentials/evidence are errors, never cloud fallback."""

import csv
import fnmatch
import json
import os
import shutil
import subprocess
import uuid
from pathlib import Path

from scripts import artifact_archive as archive


def trusted_event(env, event):
    if env.get("GITHUB_REPOSITORY") != archive.REPO:
        raise ValueError("Archive credentials are restricted to Nettking/msh")
    name = env["GITHUB_EVENT_NAME"]
    if name not in ("push", "workflow_dispatch", "pull_request"):
        raise ValueError("This event is not authorized for the private archive")
    if (
        name == "pull_request"
        and event["pull_request"]["head"]["repo"]["full_name"] != archive.REPO
    ):
        raise ValueError("Fork PRs cannot access the private archive")


def private_directory(path):
    path.mkdir(mode=0o700)
    if os.name == "nt":
        rows = subprocess.check_output(
            ["whoami", "/user", "/fo", "csv", "/nh"], text=True
        )
        sid = next(csv.reader(rows.splitlines()))[1]
        subprocess.run(
            [
                "icacls",
                str(path),
                "/inheritance:r",
                "/grant:r",
                "*" + sid + ":(OI)(CI)F",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )


def main():
    env = os.environ
    trusted_event(env, json.loads(Path(env["GITHUB_EVENT_PATH"]).read_bytes()))
    for key in ("FCP_ARCHIVE_SSH_KEY", "FCP_ARCHIVE_HOST", "FCP_ARCHIVE_KNOWN_HOSTS"):
        if not env.get(key):
            raise ValueError(
                "Archive incomplete: required configuration missing: " + key
            )
    temp = Path(env["RUNNER_TEMP"])
    credentials = temp / ("fcp-archive-ssh-" + uuid.uuid4().hex)
    private_directory(credentials)
    try:
        (credentials / "key").write_text(
            env["FCP_ARCHIVE_SSH_KEY"].strip() + "\n", encoding="utf-8"
        )
        (credentials / "key").chmod(0o600)
        (credentials / "known_hosts").write_text(
            env["FCP_ARCHIVE_KNOWN_HOSTS"].strip() + "\n", encoding="utf-8"
        )
        config = {
            "host": env["FCP_ARCHIVE_HOST"],
            "identity_file": str(credentials / "key"),
            "known_hosts": str(credentials / "known_hosts"),
        }
        tested = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
        common = {
            "repo": archive.REPO,
            "tested_sha": tested,
            "run_id": env["GITHUB_RUN_ID"],
            "run_attempt": int(env["GITHUB_RUN_ATTEMPT"]),
        }
        operation = env["ARCHIVE_OPERATION"]
        if operation == "upload":
            metadata = dict(
                common,
                job=env["GITHUB_JOB"],
                matrix=json.loads(env["ARCHIVE_MATRIX"]),
                artifact=env["ARCHIVE_NAME"],
                runner=env["RUNNER_NAME"],
                provenance_kind="github-actions-checkout",
                workflow_ref=env["GITHUB_WORKFLOW_REF"],
                workflow_sha=env["GITHUB_WORKFLOW_SHA"],
                event_sha=env["GITHUB_SHA"],
                job_status_at_archive=env["ARCHIVE_JOB_STATUS"],
            )
            package = archive.build_package(
                metadata,
                env["ARCHIVE_PATH"].strip().splitlines(),
                temp / ("fcp-evidence-" + uuid.uuid4().hex),
            )
            receipt = archive.upload(config, package)
            summary = (
                "Nitro archive COMPLETE\n\n```json\n"
                + json.dumps(receipt, sort_keys=True)
                + "\n```\n"
            )
        elif operation == "download":
            inventory = temp / ("fcp-archive-list-" + uuid.uuid4().hex + ".json")
            archive.exchange(config, dict(operation="list", **common), inventory)
            rows = json.loads(inventory.read_bytes())
            selected = {}
            for receipt in rows:
                parts = receipt["reference"].split("/")
                if not fnmatch.fnmatchcase(parts[-1], env["ARCHIVE_PATTERN"]):
                    continue
                # Reuse earlier successful immutable packages only for the SAME
                # run/source/job/matrix; preserve their original attempt identity.
                key = tuple(parts[5:])
                old = selected.get(key)
                if old is None or int(parts[4]) > int(old["reference"].split("/")[4]):
                    selected[key] = receipt
            if not selected:
                raise FileNotFoundError(
                    "Required archive packages missing; no GitHub fallback"
                )
            names = set()
            downloaded = []
            for key, receipt in sorted(selected.items()):
                if key[-1].casefold() in names:
                    raise ValueError("Ambiguous artifact name across jobs")
                names.add(key[-1].casefold())
                package = temp / ("fcp-evidence-download-" + uuid.uuid4().hex)
                manifest = archive.fetch(config, receipt, package)
                for field in ("repo", "tested_sha", "run_id"):
                    if manifest[field] != common[field]:
                        raise ValueError("Downloaded source/run mismatch")
                dest = Path(env["ARCHIVE_PATH"])
                if env["ARCHIVE_MERGE_MULTIPLE"] != "true":
                    dest = dest / manifest["artifact"]
                archive.extract(package, dest)
                downloaded.append(receipt)
            summary = (
                "Verified Nitro archive inputs\n\n```json\n"
                + json.dumps(downloaded, sort_keys=True)
                + "\n```\n"
            )
        else:
            raise ValueError("Unknown archive operation")
        with Path(env["GITHUB_STEP_SUMMARY"]).open("a", encoding="utf-8") as stream:
            stream.write(summary)
    finally:
        # Only ephemeral SSH credentials are removed; evidence spools survive failure.
        shutil.rmtree(credentials)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001 -- preserve explicit incomplete job summary
        message = "Nitro archive INCOMPLETE: " + str(exc)
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            with Path(os.environ["GITHUB_STEP_SUMMARY"]).open(
                "a", encoding="utf-8"
            ) as stream:
                stream.write(
                    message
                    + "\nOriginal files and any local evidence spool are retained.\n"
                )
        raise SystemExit(message) from None
