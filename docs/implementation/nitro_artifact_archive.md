# Nitro artifact archive

> **Scope update (2026-10-04):** This document records the Nitro SSH archive
> implementation for workflows that still use it. The Federation v1 software
> release gate and campaign-tooling checks no longer depend on Nitro or another
> fixed runner: they use GitHub-hosted runners and immutable run/attempt-bound
> Actions artifacts. Final v1 closeout carries accepted CI evidence into the
> GitHub Release asset before the public 90-day artifact retention expires.
> This does not change the separate MSH-to-Nitro Recorder backup or the physical
> acceptance host requirements. Do not use this historical implementation to
> reintroduce Nitro as a v1 software-qualification prerequisite.

This is an infrastructure change, independent of the frozen Federation candidate.
Introduce it through normal PR review and required checks. Infrastructure merge
does not move the product freeze or authorize physical acceptance on a new SHA.
There are no new subscriptions, public services, runner pools or storage platforms.
Keep all GitHub budgets at $0 with Stop usage enabled. Never fall back to Actions
artifacts, LFS, Packages, release assets, cloud storage, or paid runners.

## Current deployment and machine roles (2026-09-19)

- Nitro is the persistent artifact archive: `/srv/fcp-artifacts`, dedicated
  `fcp-archive` UID997/GID973, mode0700. Root owns the forced receiver and SSH
  authorization. Ordinary `martin` runner read/write access is denied.
- **Beast is CI-only.** It is neither a permanent artifact archive nor a P01-P12
  physical acceptance host. Windows `Beast` and Linux `Beast-Linux-WSL` retain
  only job-local staging; package storage is on Nitro. Failed uploads retain
  bounded, incomplete inputs outside `RUNNER_TEMP` until manual recovery.
- All four repository CI identities (`Beast`, `Beast-Linux-WSL`, `Nettking`, and
  `Nettking-Linux`) use the product's existing **12 GiB** CI admission floor,
  including the bounded pending write. The archive helper's separate 66 GiB
  margin remains for non-CI/operator use; this does not alter product
  host-resource thresholds, physical acceptance floors, or measured growth
  margins. WSL checks both its Linux filesystem and physical `/mnt/c`. Nitro
  retains its 200 GiB reserve and 2 GiB per-package bound. No runner labels,
  accounts or services were changed.
- Ten upload steps and three download steps use the SSH adapter on this branch;
  29 external setup-python pip caches are disabled. Production SSH credentials
  are configured, but **main still uses its original storage until reviewed merge**.
- No paid services, budget changes, release recovery, freeze change or physical
  acceptance runs are part of this infrastructure work.

## Verified transport and provenance

Targeted Beast run **35450087031**, attempt1, exact source **0444aaf8**, is PASS:
Windows job105915609398 and Linux job105915609366 uploaded, downloaded and verified
content as `fcp-archive`. Independent operator retention fetched both packages again
and bound their hashes to native GitHub job IDs, attempts and exact checkout logs.
No GitHub Actions artifacts were created. Local and runner tests: Linux14 passed;
Windows12 passed plus two Linux-only receiver skips. Ambiguous identity and a
missing identity at the deadline still fail; no assertion or identity check was
relaxed. The original failures remain retained.

The Linux run directly captured GitHub API publication lag: two responses showed
the executing job as `queued` with null runner identity, then the third showed
`in_progress`, runner29/Beast-Linux-WSL and the correct native job ID. The helper
uses the exact attempt endpoint and only re-reads missing assignment within one
20-second API budget. It never substitutes another attempt/job or accepts an
ambiguous match. Safe API observations are retained in the manifest (or a local
failure diagnostic); no token is recorded.

At admission physical C: had14,234,320,896 bytes free, against12 GiB plus the
8 MiB conservative metadata allowance and the39/38-byte source file. The actual
Windows ZIP/manifest/receipt total was2,194 logical bytes; Linux was2,785 bytes.
Extraction adds the source-file size. Filesystem allocation and unrelated runner
logs are separate. Streamed retrieval keeps one ZIP, avoiding a duplicate wire
copy. No cleanup or additional disk purchase was needed for the targeted check.

Earlier production run35449153626 verified Nettking Windows/Linux upload/get and
all four SSH identities. Its two Beast failures remain recorded; the targeted
new run resolves them without replaying a release gate. Branch pushes now run
only the authorized Beast pair; manual transport verification retains both
Nettking and Beast families in its matrix. Prior green evidence keeps its own
source identity; this is not a declaration of release qualification.

All26 historical GitHub ZIPs were copied and fetched back with original bytes,
IDs/digests and native source/attempt bindings unchanged. Another package protects
52 retained P06 original-failure/diagnostic files (805,294 source bytes). Windows
copies and GitHub originals remain intact; Nitro is not their only verified copy.
Production migration receipts, raw logs and deletion candidates remain outside Git
in the operator's `.acceptance/nitro-archive-setup` directory. No deletion is approved.

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

The owner-approved trust model is **Nettking's reviewed, trusted branches only**.
Repository secrets do not isolate the archive key from people who can change and
execute workflow code. The live review found only Nettking with write/admin access;
private-repository fork-PR workflow execution, write tokens and secrets/variables
were disabled. Keep untrusted code off these persistent runners, including code
created by agents or bots that has not been reviewed. Reassess this model **before**
granting other writers access or admitting untrusted contributions to these runners.

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
disk never become COMPLETE. Because Actions deletes `RUNNER_TEMP` at job end,
failed uploads preserve available inputs in the private `fcp-archive-pending`
directory beside `RUNNER_TEMP`, outside that cleanup scope. Unique directories
contain an `INCOMPLETE.json` inventory and only the selected evidence files;
credentials are never included. Source files are not moved or removed.

Same-volume hard links avoid duplicating payload bytes; cross-volume/unsupported
links require a capacity-admitted copy. Link/directory metadata and copy growth
are checked against the unchanged client reserve. Partial input inventories remain
explicitly incomplete. If local preservation also fails, the original archive
failure remains visible and retention is reported NOT CONFIRMED; no durability is
claimed for files left under `RUNNER_TEMP`. No storage fallback is attempted.

This is temporary pending recovery, not a second permanent archive on Beast.
Hard links survive unlinking during job cleanup but are not isolated from in-place
modification: verify every recorded size and SHA-256 before manual recovery.
The local record is not a native-job qualification receipt, an archive COMPLETE,
or authorization to retry. No automatic recovery or pending-file deletion occurs.
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
The client uses an existing OpenSSH executable from PATH, or on Windows the
installed Windows/Git for Windows OpenSSH path if the service PATH omits it.
The resolved executable is logged; no software is installed and pinned host-key
checking remains mandatory. This executable selection is not a storage fallback.

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

A branch push triggers only `Nitro artifact transport smoke`: two small jobs on
existing Beast Windows/Linux runners (manual matrix also retains Nettking), with no dependency install, external
cache, or Actions artifact upload. Opening a PR triggers broad existing release
workflows because `.github/actions/**` is watched. The introduction audit matches
46 PR workflows and 40 main-push workflows for this change. Use those automatic
runs without duplicate dispatches, preserving all required checks and zero-dollar
Stop usage budgets. A failed job does not authorize another recovery attempt.

After normal merge, record the actual resulting main SHA and verify the four
remaining Nitro-backed producers (`icse-tool-demo`, `phase2-federation`,
`phase-f7-closeout`, `cf8-role-retirement`) against their native checkout logs.
Verify an ordinary download consumer and its immutable receipts, plus the run's
empty GitHub artifact inventory. Transport smoke alone does not prove main adoption.

The frozen product candidate remains `a9bb08a2b3391e8bf072755c5b26b2ef3ebc5759`.
Its qualification is not automatically qualification of the infrastructure merge.
The checked-in physical impact map treats the five new top-level archive scripts
as unmapped paths: selecting the new main as a product candidate requires the
normal qualification, freeze and revalidation process; no physical evidence is
carried by assumption. The current physical campaign has no completed PASS.

Until reviewed merge, main still contains its original GitHub upload/cache steps.
No main workflow was dispatched as part of this change. GitHub continues to host
code, coordination, native job logs/status and short summaries. Those logs are a
deliberate remaining GitHub dependency, not a substitute archive for evidence
packages. Existing artifact storage is retained pending explicit deletion consent.
