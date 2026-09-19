# Nitro artifact archive

This is an infrastructure change, independent of the frozen Federation candidate.
Do not merge it, move the freeze, or start release qualification implicitly.
There are no new subscriptions, public services, runner pools or storage platforms.
Keep all GitHub budgets at $0 with Stop usage enabled. Never fall back to Actions
artifacts, LFS, Packages, release assets, cloud storage, or paid runners.

## Current deployment boundary (2026-09-19)

- Implemented on `codex/nitro-artifact-archive`, based on live main `a9bb08a2`.
- All ten existing upload steps and three download steps use the SSH adapter.
  Twenty-nine `setup-python` pip caches are disabled. Existing Go caches were
  already explicitly disabled; setup-node v4 has no configured cache. Local
  dependency caches and dependency versions are unchanged.
- Production receiver is installed at `/srv/fcp-artifacts`, owned by the dedicated
  `fcp-archive` account (UID997/GID973, mode0700). Production Actions credentials
  are configured. **Main workflows are not migrated until reviewed merge.**
- Administrator setup appeared during the user's manual-setup turn and was
  independently verified: root-owned receiver/authorized_keys, forced SSH command,
  no supplementary groups, ordinary `martin` runner denied read/write to archive.
  No general passwordless sudo or sshd change was made.
- Nitro had 728,220,061,696 free bytes at inspection. Receiver admission reserves
  200 GiB and limits an individual package to 2 GiB. Local packing, retrieval and
  extraction keep 66 GiB free (64 GiB floor plus 2 GiB margin). On WSL the check
  covers both the local filesystem and `/mnt/c`; missing host-volume access fails
  closed. A virtual disk's logical free space is not physical host capacity.
- No P06/P07/P12 run was started, stopped or changed. P07/P12 remain unstarted.

## Verification and remaining host prerequisite

Transport implementation `5a86c57c`, run `35443127083`, attempt 1:

- Restricted SSH and pinned-host verification passed on Nettking Windows/Linux
  and Beast Windows/Linux, under their actual runner identities.
- Nettking uploaded and fetched the tiny packages on both operating systems.
  Independent retention fetched them again, verified every hash, and bound the
  native job IDs and checkout logs to the exact implementation commit.
- Archive unit tests: each Linux job 11 passed; each Windows job 9 passed plus
  two Linux receiver tests skipped. They cover concurrent publication, idempotence,
  conflicts, corruption, truncation, capacity refusal and original-file retention.
- Beast upload was correctly refused: Windows had 14,242,746,368 free bytes;
  WSL measured 14,242,881,536 on the same physical C: volume, below the preserved
  66 GiB margin. **Overall smoke result is FAILURE, not four-runner acceptance.**
  No retry, cleanup, reserve reduction or storage fallback resolves this implicitly.
- The earlier Windows service-key ACL failure is preserved in run `35441797000`;
  the corrected service-account transport passed in run `35441947753`.
- All four transport runs created zero GitHub artifacts. A separate unavailable
  SSH destination test failed explicitly with the original file/package retained.
  Account billing UI showed all five budgets at $0, Stop usage enabled; Actions
  cache API reported zero entries/bytes. Neither setting was changed.

Production run `35449153626`, source `94d787e5`, attempt1: all four runner SSH probes
passed as **fcp-archive**; Nettking Windows/Linux upload, streamed download and
independent native-job/source retention passed. Beast Windows was refused by the
unchanged capacity guard; Beast Linux stopped earlier with native GitHub job
binding missing/ambiguous. That earlier API snapshot was not retained, so its
precise cause is unresolved. No retry was started; overall run is **FAILURE**. There are zero GitHub
artifacts from this run. All-runner transition remains blocked pending the archive
policy scope decision below. The previous smoke root contains synthetic data only.

All 26 historical ZIPs were uploaded and fetched back through fcp-archive with
unchanged original bytes/IDs/digests and native source/attempt bindings. Another
package protects 52 retained P06 original-failure/diagnostic files (805,294 original
bytes); all hashes were verified after retrieval. Existing Windows evidence and
GitHub originals remain intact. Nitro is not the only verified copy of these files.

### Beast clarification, 2026-09-19 14:25 UTC

Read-only run `35448691277` used the existing Beast Windows runner, without a
checkout, tests, archive transfer or cleanup. C: is NTFS, serial `86F91634`, total
118,917,951,488 bytes (110.75 GiB), free 14,240,579,584 bytes (13.26 GiB). The WSL
client's `/mnt/c` check protects this same physical volume.

The 66 GiB archive rule was introduced in this infrastructure branch; it is not
the archive's data size and is not a demonstrated existing blanket Beast test
requirement. The existing `ci_release_disk_preflight.py` refuses at or below
12 GiB and warns at or below 16 GiB. `storage_allocation.default_floor_bytes`
derives `min(max(10 GiB, 5% of volume size), 64 GiB)`, which is 10 GiB on this C:.
The physical Windows campaign's 64 GiB floor and measured growth margins still
apply to its actual acceptance volumes. This inspection is not a fresh test gate
or an audit of every filesystem visible inside Beast WSL.

The archive guard remains unchanged as instructed. Its Windows upload admission
was `66 GiB + 8 MiB + 39 bytes`; therefore it refuses even a 39-byte input. The
equivalent retained Nettking package uses 171 ZIP bytes, 1,262 manifest bytes and
338 receipt bytes: 1,771 additional logical bytes, not 66 GiB. Beast's job metadata
can differ slightly; its physical NTFS allocation was not measured because the
guard prevented packaging. The 8 MiB admission allowance is conservative header
space, not an actual allocation. No archive staging directories were found in the
bounded `RUNNER_TEMP/fcp-evidence-*` inventory; no cleanup candidates are approved.

Retrieval now streams directly into one ZIP and verifies its declared length,
manifest, ZIP SHA-256 and all member hashes before publishing a local receipt.
This removes the redundant `download.wire` copy: peak package bytes fall from
`2 * ZIP size` to `ZIP size`, plus bounded metadata; extraction adds the restored
file bytes. One upload ZIP remains necessary for a stable, re-verifiable retry.
No free-space floor, SSH check, original evidence or frozen source changed.
Local verification: Linux 12 passed, Windows 10 passed / 2 Linux-only skips;
real Windows retrieval from the isolated Nitro receiver passed with no wire copy.
Production and all-runner verification remain pending. Do not clean 52+ GiB on
Beast merely to satisfy this new unscoped archive rule without a separate decision.

## Administrator setup

Review `scripts/install_nitro_archive.py` and `scripts/artifact_archive.py`.
Run the installer on Nitro as administrator, supplying only an Ed25519 **public**
key. It creates a dedicated, nonprivileged account, a private archive directory,
and a root-owned immutable receiver. It does not change sshd, runner services,
Docker, Recorder mounts or any existing data. No new listener is created.

```sh
sudo python3 scripts/install_nitro_archive.py --public-key /path/archive.pub
```

The account's root-owned authorized_keys forces the receiver command and uses
OpenSSH `restrict` (no forwarding, PTY or arbitrary shell). The account has no
sudo/docker/shared supplementary groups. Root and existing privileged host
administrators remain trusted; never execute untrusted fork code on such hosts.

After validating SSH access and negative ordinary-runner file access, configure:

- Repository secret `NITRO_ARTIFACT_SSH_KEY`: this restricted account's private key.
- Repository variable `NITRO_ARTIFACT_HOST`: `fcp-archive@<verified-Nitro-host>`.
- Repository variable `NITRO_ARTIFACT_KNOWN_HOSTS`: the **previously verified** host
  key entry matching that hostname. Do not bootstrap trust from an unverified scan.

Only archive steps receive the credential. Fork PRs receive no Actions secret;
the adapter also refuses foreign repositories, fork heads, pull_request_target
and other unapproved event types. SSH keys are written to ephemeral owner-only
files, then removed. Windows keys have a DACL containing only the current SID.
The smoke key is separate and permits only synthetic packages in its own root.
Do not repoint it at production evidence or grant the normal `martin` SSH key to CI.

## Package and failure semantics

Layout:

```
Nettking/msh/<actual-tested-SHA>/<run-id>/<attempt>/<job>-<matrix-hash>/<artifact>/
    bundle.zip
    manifest.json
    COMPLETE.json
```

The manifest records actual `git rev-parse HEAD` (not blindly `github.sha`), event
SHA, workflow ref/SHA, client tool hash, native GitHub job ID, runner identity,
matrix JSON, run/attempt, UTC capture time, and every filename/size/SHA-256.
The archive is uncompressed to minimize CPU contention and preserves file paths.
Legacy copies retain their original ZIP **unchanged** inside the new package,
with original API metadata, IDs/digests and native checkout/job logs. Unknown
legacy workflow SHA is explicitly null with an explanation, never fabricated.

The receiver reserves actual disk blocks under a lock, writes a unique staging
directory, verifies the ZIP and every file hash, fsyncs files, and publishes by
rename. Published files are read-only; existing references cannot be overwritten.
Repeating the exact package returns the same receipt. A conflicting package
fails. Parallel publishers cannot mix files. Readers verify both manifest and
package hashes and refuse extraction collisions, traversal and symlinks.

Archive failure fails the archive step and writes INCOMPLETE to the job summary.
Original test steps and their outcomes remain visible. Uploads attempt to preserve
available files even after a failing test. Missing files, unavailable SSH or low
disk never become COMPLETE; original files and local spools remain available.
There is no automatic deletion/retention job. Interrupted staging is not evidence;
review any orphan manually instead of pruning the host or deleting runtime data.

## Retrieve and verify

Save the complete JSON receipt printed in the producing job summary. A local
SSH configuration file contains `host`, `identity_file` and `known_hosts` paths;
it contains no key material and stays outside Git.

```sh
python -m scripts.artifact_archive get --config archive-ssh.json \
  --receipt receipt.json --output retrieved-package
python -m scripts.artifact_archive extract --package retrieved-package \
  --output restored-files
```

Both Linux and Windows use the same commands. Retrieval rejects different bytes,
different metadata or a different immutable reference. The original paths are
restored beneath the requested destination; existing files are not replaced.

For qualification retention of **new** runs:

```sh
# GH_TOKEN is read-only Actions access; never include it in command-line arguments.
python -m scripts.retain_archive_run --config archive-ssh.json \
  --source <actual-tested-SHA> --run-id <id> --attempt <attempt> --output retained-run
```

This retrieves packages from Nitro and binds their native job IDs to GitHub job
records and exact checkout logs. Original attempt identities survive failed-job
recovery; the workflow download adapter selects the newest available package per
job/matrix/name at or before the requested attempt, only within the same SHA/run.
Coverage checks still consume their original JUnit/shard/publication files.

**Retention is not qualification.** `qualification_status` stays NOT_EVALUATED.
Normal gate/source/coverage/no-skip/physical audits are still required. No workflow
in the scanned tree uses GitHub artifact attestations. Old ID-based qualification
scripts/receipts remain historical readers for old packages; new records use
`identity_kind=nitro-ssh-manifest-v1` and `github_artifact_id=null`. Do not synthesize
GitHub artifact IDs or count a missing provenance audit as satisfied. Review this
explicit identity substitution before using the new storage in a release audit.

## Preserve historical evidence and deletion boundary

`scripts/archive_legacy_artifacts.py` consumes a reviewed plan binding original
ZIPs, GitHub digests, native upload log IDs, exact checkout SHA and original
attempt. It uploads each package, fetches it again and checks the original ZIP.
It requires the dedicated production account; it cannot send release evidence
to the smoke receiver. Example:

```sh
python -m scripts.archive_legacy_artifacts --config archive-ssh.json \
  --plan legacy-migration-plan.json --output migration-receipts
```

Prepared inventory: 17 current-candidate artifacts across release35435674907,
ICSE35435674912 and Phase2 35435674918; nine artifacts from release34821054538,
whose actual tested merge is e6717470, not its API head3e7becd9. All 26 ZIPs already
have byte-identical, digest-verified copies on Windows (6,117,979 bytes total).
Both original attempts of Phase2 and the old release recovery remain identified.
No GitHub object, workflow run or release evidence has been deleted.

These 26 IDs are **deletion candidates only after** production Nitro upload AND
retrieval verification, continued verification of the independent Windows copy,
provenance review, and explicit user approval. The exact ID list and binding plan
remain outside Git in the operator's `nitro-archive-setup` receipts. Current status
is COPIED_AND_RETRIEVED_VERIFIED, but none is approved for deletion. The 52 retained
P06 failure/diagnostic files now also have a verified Nitro copy. This does not
constitute physical acceptance or resolve the original P06 failure.

## Review and activation

A branch push triggers only `Nitro artifact transport smoke`: four small jobs on
existing Nettking and Beast Windows/Linux runners, with no dependency install, external
cache, or Actions artifact upload. Opening a PR triggers broad existing release
workflows because `.github/actions/**` is watched. Therefore do not open a PR or
merge automatically merely to publish this change for review; review the branch
first and coordinate normal introduction without an unsolicited qualification.

Until reviewed merge, main still contains its original GitHub upload/cache steps.
No main workflow was dispatched as part of this change. GitHub continues to host
code, coordination, native job logs/status and short summaries. Those logs are a
deliberate remaining GitHub dependency, not a substitute archive for evidence
packages. Existing artifact storage is retained pending explicit deletion consent.
