"""Retain new Nitro artifacts with native GitHub jobs/logs; never declare qualification.

GH_TOKEN supplies read-only Actions API access. Artifact bytes come only from Nitro.
Existing GitHub-ID evidence retains its old readers; no synthetic artifact IDs.
"""

import argparse
import hashlib
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from scripts import artifact_archive as archive


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


def github(path, *, raw=False):
    request = urllib.request.Request(
        "https://api.github.com/repos/" + archive.REPO + path,
        headers={
            "Authorization": "Bearer " + os.environ["GH_TOKEN"],
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        response = urllib.request.build_opener(NoRedirect).open(request, timeout=30)
    except urllib.error.HTTPError as exc:
        if not raw or exc.code not in (301, 302, 303, 307, 308):
            raise
        location = exc.headers["Location"]
        if urllib.parse.urlsplit(location).scheme != "https":
            raise ValueError("Non-HTTPS log download refused") from None
        # Never forward the GitHub token to redirected object storage.
        response = urllib.request.urlopen(location, timeout=30)
    with response:
        data = response.read(32 * 1024**2 + 1)
    if len(data) > 32 * 1024**2:
        raise ValueError("Native log/API retention bound exceeded")
    return data if raw else json.loads(data)


def checkout_commits(raw):
    lines = raw.decode("utf-8", errors="replace").splitlines()
    commits = set()
    for index, line in enumerate(lines):
        if re.search(r'git(?:\.exe)?"? log -1 --format=%H', line):
            for following in lines[index + 1 : index + 4]:
                commits.update(re.findall(r"\b[0-9a-f]{40}\b", following))
    return sorted(commits)


def retain(config, source, run_id, attempt, output):
    if not re.fullmatch("[0-9a-f]{40}", source) or not str(run_id).isdecimal():
        raise ValueError("Exact candidate and numeric run ID required")
    output.mkdir(parents=True, exist_ok=False)
    run = github(f"/actions/runs/{run_id}")
    (output / "github-run.json").write_bytes(archive.canonical(run))
    inventory = output / "nitro-inventory.json"
    archive.exchange(
        config,
        {
            "operation": "list",
            "repo": archive.REPO,
            "tested_sha": source,
            "run_id": str(run_id),
            "run_attempt": attempt,
        },
        inventory,
    )
    receipts = json.loads(inventory.read_bytes())
    if not receipts:
        raise FileNotFoundError(
            "No Nitro packages for exact source/run; no GitHub fallback"
        )
    records = []
    jobs_by_attempt = {}
    for index, receipt in enumerate(receipts):
        package = output / f"package-{index}"
        manifest = archive.fetch(config, receipt, package)
        if manifest["tested_sha"] != source or manifest["run_id"] != str(run_id):
            raise ValueError("Archive source/run mismatch")
        native = manifest["native_github_job"]
        original_attempt = manifest["run_attempt"]
        if original_attempt not in jobs_by_attempt:
            result = github(
                f"/actions/runs/{run_id}/attempts/{original_attempt}/jobs?per_page=100"
            )
            if result["total_count"] > 100:
                raise ValueError("Explicit native job pagination required")
            jobs_by_attempt[original_attempt] = {j["id"]: j for j in result["jobs"]}
            (output / f"github-jobs-attempt-{original_attempt}.json").write_bytes(
                archive.canonical(result)
            )
        job = jobs_by_attempt[original_attempt][native["id"]]
        for field in ("id", "run_id", "run_attempt", "runner_id", "started_at"):
            if job[field] != native[field]:
                raise ValueError("Native GitHub job binding changed: " + field)
        if job["status"] != "completed":
            raise ValueError("Job is not terminal; retention incomplete")
        log = github(f"/actions/jobs/{job['id']}/logs", raw=True)
        (package / "native-job.log").write_bytes(log)
        if checkout_commits(log) != [source]:
            raise ValueError("Native checkout log does not prove this exact source")
        records.append(
            {
                "receipt": receipt,
                "native_job_id": job["id"],
                "run_attempt": original_attempt,
                "job_conclusion": job["conclusion"],
                "checkout_commits": [source],
                "native_log_sha256": hashlib.sha256(log).hexdigest(),
                "github_artifact_id": None,
                "identity_kind": "nitro-ssh-manifest-v1",
            }
        )
    result = {
        "source": source,
        "run_id": run_id,
        "requested_attempt": attempt,
        "records": records,
        "retention_status": "VERIFIED",
        "qualification_status": "NOT_EVALUATED",
        "note": "Job failures remain failures. Gate coverage, tests and contract acceptance require their normal audit.",
    }
    (output / "retention.json").write_bytes(archive.canonical(result))
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", required=True)
    p.add_argument("--source", required=True)
    p.add_argument("--run-id", required=True, type=int)
    p.add_argument("--attempt", required=True, type=int)
    p.add_argument("--output", required=True, type=Path)
    args = p.parse_args()
    result = retain(
        json.loads(Path(args.config).read_bytes()),
        args.source,
        args.run_id,
        args.attempt,
        args.output,
    )
    print(json.dumps({k: v for k, v in result.items() if k != "records"}))
