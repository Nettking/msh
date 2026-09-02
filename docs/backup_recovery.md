# Backup and recovery

Status: **current administrator guide**

Reviewed: **2026-09-02 Europe/Amsterdam**

This guide defines the supported Federation v1 backup and recovery boundary. It is intentionally conservative: recovery must preserve authority rather than manufacture a replacement identity that merely looks like the failed device.

## Recovery model

FCP distinguishes three cases:

1. **Same-installation recovery** — restore the same FCP device only when its cryptographic identity remains usable.
2. **Replacement member** — a permanently lost ordinary member is replaced by a fresh FCP identity, then paired/rejoined and reconfigured.
3. **Creator loss** — the Federation creator is special because creator provenance and human credential authority do not transfer to the current operational leader.

Portable identity cloning is not a Federation v1 feature.

## What must be protected

For the default deployment, protect these items together:

| State | Default location | Recovery importance |
| --- | --- | --- |
| Device identity, Federation/member state, recorder/source configuration, checkpoints, recorded/imported data and local capability state | effective `data/` bind | Critical |
| Human account database and authentication secrets | `data/auth/` | Critical, especially on the creator/credential-authority installation |
| Federation coordinator authority database | retained `relay_state` Docker volume | Critical on the coordinator/creator installation |
| Local deployment settings | `.env` when present | Important when non-default paths, binds or service settings are used |
| Workflow and analysis results | effective `results/` bind | Optional historical state; preserve when results must survive |
| Ollama/provider model volumes | Docker volumes | Re-downloadable; not required for authority recovery |
| Docker images | local Docker cache | Rebuildable; not required for authority recovery |

The supported backup command resolves the effective Compose bind and volume sources rather than assuming repository-local defaults. Human-authentication files are one recovery unit. In particular, back up `users.sqlite3`, `flask-secret`, and `password-salt` together. Restoring only part of that set can invalidate passwords or browser sessions and can create contradictory authentication state.

## Windows identity portability rule

On Windows, FCP protects the persistent node private key with Windows DPAPI for the Windows user that created it. The public `identity.json` file is not sufficient to recover the device.

**Copying `data/` to an arbitrary replacement Windows PC or user account is not a supported same-identity restore.**

A same-identity Windows recovery is supported only when the restored `identity.pem` remains decryptable by the applicable Windows security context. If the key cannot be opened, stop. Do not delete the key, regenerate a key under the old metadata, edit `identity.json`, or copy another member's identity in an attempt to impersonate the failed device.

## Quiesced backup requirement

Use the repository-owned backup command rather than manually copying live directories or volumes:

```text
python -m catalog.federation.backup_recovery backup \
  --repo-root <FCP checkout> \
  --destination <new directory on an independent backup filesystem>
```

The destination is intentionally explicit. It must be a **new directory on a different backing filesystem** from:

- the Git checkout;
- the effective FCP data bind;
- the effective results bind, when present; and
- Docker's proven persistent backing resource.

A directory inside the checkout, `data/`, `results/`, or Docker's own backing filesystem is refused even when it appears to have enough free bytes. The purpose of a backup is defeated if creating it can exhaust the production resource it is meant to protect.

Before any production writer is stopped, the command:

1. requires a clean exact source checkout and records `HEAD`;
2. resolves the effective Compose data/results/relay layout;
3. proves Docker's actual backing resource;
4. measures the backup destination and proves it is independent;
5. sizes the local state plus the retained relay volume using the already-present relay image; and
6. requires sufficient destination bytes and file/inode capacity with a safety margin.

The relay-volume sizing helper uses the exact local relay image with `--pull=never` and `--network=none`. The backup therefore has no post-quiescence image-download or network prerequisite.

### Quiescence and update exclusion

The backup then acquires the same checkout-scoped host-mutation boundary used by supported launchers and update agents. While that lease is held, a supported update/build/activation cannot begin.

For the ordinary Compose-managed topology, the command:

- refuses a live native standalone recorder rather than copying underneath it;
- stops a matching tailnet join responder only after its PID **and OS process-creation identity** are proven to match the recorded responder instance;
- runs `docker compose stop` and positively proves the core `flask`, `relay`, and managed `recorder` containers are no longer running; and
- remeasures the exact backup payload and destination capacity after quiescence before copying.

Do **not** run `docker compose down -v` as part of backup or recovery. The `-v` option removes named volumes and can destroy the retained relay/coordinator state that the backup is specifically intended to preserve. A normal backup must never delete Docker volumes.

A native recorder that is intentionally part of the selected recovery topology must be stopped through its supported supervisor before running this command. Do not bypass the refusal by editing its status file. A POSIX/macOS native recorder without the Windows supervisor remains outside the automated quiescence contract and must be administratively stopped first.

### What is copied

After quiescence the command copies:

- the effective data bind to `data/`;
- the effective results bind to `results/` when present;
- `.env` when present; and
- the stopped relay container's retained `/var/lib/fcp-relay` volume state to `relay-state/`.

It does **not** follow symlinks/junctions/reparse points in managed backup roots and refuses special files it cannot classify safely. The relay copy uses `docker cp` from the already-existing stopped relay container; it does not create a helper image or download anything.

Every copied SQLite database identified by the SQLite file header is opened from the **backup copy** and must pass `PRAGMA quick_check`. Only after those checks pass does the command atomically publish `backup-manifest.json`, record `source-commit.txt`, and remove the `.fcp-backup-incomplete` marker.

A failed or interrupted copy therefore never looks like a completed backup. Keep or remove an incomplete directory only after diagnosing the failure; do not treat it as recovery material.

### FCP deliberately remains stopped

The backup command does **not** run `docker compose start`, even after success. This is intentional. A backup failure, copy failure, or newly discovered capacity problem must never implicitly restart writers onto the host merely because a `finally` block ran.

After a successful backup:

1. verify it explicitly;
2. inspect host capacity and the reason for any warnings/refusals; then
3. resume FCP through its normal supported launcher (`start.cmd --resume`, `start.sh --resume`, or the corresponding supported supervised path).

Do not use a generic `docker compose start` as a substitute for the supported launcher/recovery checks.

## Backup examples

### Windows PowerShell

Choose a destination on an independently backed-up drive or mounted volume. Do not use the FCP checkout drive when it is the same backing resource.

```powershell
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$backup = "E:\FCP-backups\fcp-$stamp"
python -m catalog.federation.backup_recovery backup `
  --repo-root (Get-Location) `
  --destination $backup

python -m catalog.federation.backup_recovery verify --backup $backup
```

After verification, resume only through the supported launcher, for example:

```powershell
.\start.cmd --resume
```

### Linux/macOS

Choose a mounted destination whose filesystem is independent of the checkout/data/results/Docker resource:

```bash
backup="/mnt/fcp-backups/fcp-$(date +%Y%m%d-%H%M%S)"
python -m catalog.federation.backup_recovery backup \
  --repo-root "$PWD" \
  --destination "$backup"

python -m catalog.federation.backup_recovery verify --backup "$backup"
```

Then resume through the supported launcher for that installation. POSIX identity files are permission-restricted and the backup contains sensitive authority material; preserve filesystem protections on the backup medium.

## Verifying an existing backup

Run:

```text
python -m catalog.federation.backup_recovery verify --backup <backup-directory>
```

Verification refuses:

- an incomplete marker;
- a missing or malformed completion manifest;
- SQLite corruption;
- a changed SQLite inventory relative to the completion manifest; or
- link-like/special content inside the checked backup tree.

The manifest records non-secret structural evidence: the exact source commit, retained relay volume name, aggregate source size/file estimates, required destination capacity, the SQLite databases that passed quick-check, and a digest of that SQLite inventory. The backup itself remains sensitive and must not be committed to Git or attached to public release evidence.

## Same-installation restore

A same-installation restore means the same logical FCP device is being recovered. It is not permission to clone one identity onto two live hosts.

Before restoring:

1. keep the failed/original instance offline;
2. verify the backup with the command above;
3. use the exact source commit recorded in `source-commit.txt`, or a separately validated migration path known to accept that state;
4. restore `data/`, `.env` when present, and `results/` when required as one coherent snapshot;
5. restore `relay-state/` into a **new empty Docker volume** and explicitly select that volume for the isolated recovery start;
6. on Windows, confirm the restored private identity remains decryptable by the Windows security context before treating the recovery as the same node; and
7. start through the supported `--resume` launcher path and verify the node ID/Federation membership before re-enabling normal operation.

Do not unpack a backup over an active installation or merge an old relay database into a newer one. The original logical instance must remain offline while the recovery target is started. Restore to empty target state or a dedicated recovery environment so old and new authority records are never mixed.

The core recovery path must not depend on downloading an AI model. FCP's model capability is optional; if the model is absent, core workbench/Federation/recorder/control availability must still be established before any optional model installation is retried.

If startup reports an identity/key mismatch, unknown node, ambiguous relay state, authentication inconsistency, or failed saved-setup resume, stop and diagnose the snapshot. Do not repair the problem by deleting only the file that produced the error.

## Replacing a permanently lost ordinary member

If the old member identity cannot be recovered, treat the replacement as a new device:

1. revoke/retire the lost node when the Federation authority is available;
2. install FCP fresh on the replacement machine;
3. allow FCP to generate a new cryptographic identity;
4. pair/join the replacement through the normal signed `FCP1-...` flow;
5. inspect the replacement and explicitly restore the desired contribution configuration/authority; and
6. copy historical non-authority data only when needed and only through a reviewed import/restore path.

Do **not** copy the old member's identity/Federation state into the replacement merely to keep the old node ID.

## Creator and credential-authority loss

The Federation creator is not equivalent to the current operational leader. Operational leader failover does not transfer immutable creator provenance or the creator-backed human password database.

To recover the same Federation after creator-host failure, the recovery must retain usable creator identity material, the creator's human-authentication state, and the authoritative Federation coordinator state required by that deployment.

If the creator's cryptographic identity is irrecoverably lost, Federation v1 does not provide a mechanism for another member to impersonate or rewrite the old creator. If the existing authority cannot be recovered safely, create a new Federation, generate fresh device identities where required, re-enrol trusted devices, and recreate human accounts on the new credential authority. Do not edit databases or identity files to manufacture continuity.

This limitation is intentional and should be treated as an operator-visible disaster-recovery boundary, not bypassed by broadening node or human authority.

## Release-candidate recovery rehearsal

Federation v1 release acceptance must rehearse backup/recovery on the **exact candidate commit**. Automated tests prove the ordering and refusal contracts, but green CI does not replace the physical rehearsal.

Minimum rehearsal:

- record the exact release-candidate commit;
- use a physically independent destination and record its non-secret resource identity/capacity result;
- exercise one insufficient-capacity refusal **before** production is stopped;
- exercise one copy/verification failure and prove FCP remains stopped rather than automatically restarting;
- take a successful quiesced backup with the supported command;
- run the explicit `verify` command and record only the pass/fail outcome and SQLite database count, never database contents;
- restore into an isolated recovery target that does not run concurrently with the original identity;
- restore the relay state into a clean volume rather than merging it with an existing one;
- verify the same node ID where same-identity recovery is being tested;
- verify Federation reconnect/resume and persisted human-auth state where applicable;
- verify recorded data/checkpoints remain present;
- verify core recovery succeeds without requiring an AI model download;
- verify a replacement-member exercise uses a new identity rather than copied authority state; and
- explicitly record whether creator recovery was tested or remains a documented limitation of the candidate environment.

Release evidence must remain redacted: never commit private keys, passwords, hashes, salts, raw pairing codes, relay databases, `.env` secrets, source URLs containing credentials, or backup archives.

## What this guide does not promise

Federation v1 does not promise:

- portable cloning of a Windows DPAPI-protected node identity to arbitrary hardware/users;
- automatic transfer of creator or human credential authority to a promoted operational leader;
- online/hot copying of active SQLite/relay state;
- automatic quiescence of a live unsupervised native recorder;
- automatic restart after backup; or
- a generic restore command that rewrites identity or Federation authority.

Those capabilities require separately reviewed authority, migration and threat-model work.

## Related guides

- [Server setup and deployment](server_setup.md)
- [Human users, sign-in, and permissions](human-authentication.md)
- [Federation operations](federation_operations.md)
- [Troubleshooting](troubleshooting.md)
- [Federation v1 scope](releases/federation_v1_scope.md)
