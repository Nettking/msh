"""Actions adapter. Missing credentials/evidence are errors, never cloud fallback."""

import csv
import fnmatch
import json
import os
import shutil
import subprocess
import time
import urllib.request
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


def restrict_private_key(path):
    if os.name != "nt":
        path.chmod(0o600)
        return
    # Service accounts can inherit an OWNER RIGHTS ACE which OpenSSH rejects.
    # Replace the DACL with only this SID; retain OpenSSH's strict checks.
    script = r"""
$ErrorActionPreference = 'Stop'
$sid = [Security.Principal.WindowsIdentity]::GetCurrent().User
$acl = [Security.AccessControl.FileSecurity]::new()
$acl.SetOwner($sid)
$acl.SetAccessRuleProtection($true, $false)
$rule = [Security.AccessControl.FileSystemAccessRule]::new($sid, 'Read', 'Allow')
$acl.AddAccessRule($rule)
Set-Acl -LiteralPath $env:FCP_ARCHIVE_KEY_PATH -AclObject $acl
"""
    subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
        env=dict(os.environ, FCP_ARCHIVE_KEY_PATH=str(path)),
        check=True,
        capture_output=True,
    )


def native_job_identity(env):
    """Bind a running job to GitHub's numeric identity, not an invented ID."""
    run_id = env["GITHUB_RUN_ID"]
    attempt_text = env["GITHUB_RUN_ATTEMPT"]
    if not run_id.isdecimal() or not attempt_text.isdecimal() or int(attempt_text) < 1:
        raise ValueError("Invalid GitHub run ID")
    attempt = int(attempt_text)
    request = urllib.request.Request(
        f"https://api.github.com/repos/{archive.REPO}/actions/runs/{run_id}/attempts/{attempt}/jobs?per_page=100",
        headers={
            "Authorization": "Bearer " + env["ARCHIVE_GITHUB_TOKEN"],
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    # GitHub can publish the running job/runner assignment after steps begin.
    # Re-read only absent identity within the same 20-second API time budget.
    # Ambiguous identity is always an error; never guess or reuse another job.
    deadline = time.monotonic() + 20
    observations = []
    matches = []
    while time.monotonic() < deadline:
        with urllib.request.urlopen(
            request, timeout=max(0.001, deadline - time.monotonic())
        ) as response:
            data = json.load(response)
        if data["total_count"] > 100:
            raise ValueError(
                "Native job inventory exceeds bound; explicit binding required"
            )
        matches = [
            job
            for job in data["jobs"]
            if job["status"] == "in_progress"
            and job["runner_name"] == env["RUNNER_NAME"]
            and job["run_attempt"] == attempt
            and job["run_id"] == int(run_id)
        ]
        observations.append(
            {
                "matches": len(matches),
                "jobs": [
                    {
                        key: job.get(key)
                        for key in (
                            "id",
                            "run_id",
                            "run_attempt",
                            "status",
                            "runner_id",
                            "runner_name",
                        )
                    }
                    for job in data["jobs"]
                ],
            }
        )
        if matches:
            break
        time.sleep(min(1, max(0, deadline - time.monotonic())))
    if len(matches) != 1:
        diagnostic = Path(env["RUNNER_TEMP"]) / (
            "fcp-native-job-binding-" + uuid.uuid4().hex + ".json"
        )
        diagnostic.write_bytes(
            archive.canonical(
                {
                    "run_id": run_id,
                    "attempt": attempt,
                    "runner": env["RUNNER_NAME"],
                    "observations": observations,
                }
            )
        )
        raise ValueError(
            "Native GitHub job binding is missing or ambiguous; evidence: "
            + str(diagnostic)
        )
    job = matches[0]
    identity = {
        key: job.get(key)
        for key in (
            "id",
            "name",
            "run_id",
            "run_attempt",
            "head_sha",
            "runner_id",
            "runner_name",
            "started_at",
        )
    }
    identity["binding_observations"] = observations
    return identity


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
        restrict_private_key(credentials / "key")
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
        if operation == "probe":
            inventory = temp / ("fcp-archive-probe-" + uuid.uuid4().hex + ".json")
            archive.exchange(config, dict(operation="list", **common), inventory)
            if not isinstance(json.loads(inventory.read_bytes()), list):
                raise ValueError("Invalid archive inventory response")
            summary = "Nitro SSH authentication and pinned host verified; no package archived.\n"
        elif operation == "upload":
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
                native_github_job=native_job_identity(env),
                job_status_at_archive=env["ARCHIVE_JOB_STATUS"],
            )
            package = archive.build_package(
                metadata,
                env["ARCHIVE_PATH"].strip().splitlines(),
                temp / ("fcp-evidence-" + uuid.uuid4().hex),
            )
            receipt = archive.upload(config, package)
            manifest = json.loads((package / "manifest.json").read_bytes())
            print(
                "Archive local space: "
                + json.dumps(
                    {
                        "admission": manifest["local_capacity_admission"],
                        "input_bytes": sum(row["size"] for row in manifest["files"]),
                        "zip_bytes": receipt["zip_size"],
                        "manifest_bytes": (package / "manifest.json").stat().st_size,
                        "receipt_bytes": len(archive.canonical(receipt)),
                    }
                )
            )
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
