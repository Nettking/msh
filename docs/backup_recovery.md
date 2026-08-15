# Backup and recovery

Status: **current administrator guide**

Reviewed: **2026-08-15 Europe/Oslo**

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
| Device identity, Federation/member state, recorder/source configuration, checkpoints, recorded/imported data and local capability state | `data/` | Critical |
| Human account database and authentication secrets | `data/auth/` | Critical, especially on the creator/credential-authority installation |
| Federation coordinator authority database | retained `relay_state` Docker volume | Critical on the coordinator/creator installation |
| Local deployment settings | `.env` when present | Important when non-default paths, binds or service settings are used |
| Workflow and analysis results | `results/` | Optional historical state; preserve when results must survive |
| Ollama/provider model volumes | Docker volumes | Re-downloadable; not required for authority recovery |
| Docker images | local Docker cache | Rebuildable; not required for authority recovery |

If `FCP_DATA_DIR`, `FCP_RESULTS_DIR`, or custom volume names are configured, back up the configured paths/volumes rather than assuming the defaults above.

Human-authentication files are one recovery unit. In particular, back up `users.sqlite3`, `flask-secret`, and `password-salt` together. Restoring only part of that set can invalidate passwords or browser sessions and can create contradictory authentication state.

## Windows identity portability rule

On Windows, FCP protects the persistent node private key with Windows DPAPI for the Windows user that created it. The public `identity.json` file is not sufficient to recover the device.

**Copying `data/` to an arbitrary replacement Windows PC or user account is not a supported same-identity restore.**

A same-identity Windows recovery is supported only when the restored `identity.pem` remains decryptable by the applicable Windows security context. If the key cannot be opened, stop. Do not delete the key, regenerate a key under the old metadata, edit `identity.json`, or copy another member's identity in an attempt to impersonate the failed device.

## Quiesced backup requirement

Take an authority-consistent backup while FCP services are stopped. Do not copy live SQLite databases or the relay volume while Flask, relay, recorder or recovery/update processes may still be writing them.

Use `docker compose stop`, not `docker compose down -v`. A normal backup must never delete Docker volumes.

The examples below assume the default repository-local `data/` and `results/` paths and an ordinary Compose-managed installation. They also record the exact source commit because the restore rehearsal must use code compatible with the saved state.

## Backup on Windows

Run the following from PowerShell in the FCP checkout:

```powershell
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$backup = Join-Path (Get-Location) "fcp-backup-$stamp"
New-Item -ItemType Directory -Path $backup | Out-Null

$relayContainer = (docker compose ps -a -q relay).Trim()
if (-not $relayContainer) { throw "Relay container not found. Start FCP once before taking a supported backup." }
$relayVolume = (docker inspect $relayContainer --format '{{range .Mounts}}{{if eq .Destination "/var/lib/fcp-relay"}}{{.Name}}{{end}}{{end}}').Trim()
if (-not $relayVolume) { throw "Could not resolve the retained Federation relay volume." }

git rev-parse --verify HEAD^{commit} | Set-Content (Join-Path $backup "source-commit.txt")
$relayVolume | Set-Content (Join-Path $backup "relay-volume-name.txt")

docker compose stop
try {
    Copy-Item data -Destination (Join-Path $backup "data") -Recurse
    if (Test-Path results) { Copy-Item results -Destination (Join-Path $backup "results") -Recurse }
    if (Test-Path .env) { Copy-Item .env -Destination (Join-Path $backup ".env") }

    docker run --rm `
      --mount "type=volume,src=$relayVolume,dst=/source,readonly" `
      --mount "type=bind,src=$backup,dst=/backup" `
      alpine sh -c 'cd /source && tar czf /backup/relay-state.tgz .'
}
finally {
    docker compose start
}
```

Keep the resulting backup outside the Git checkout and protect it as sensitive operational state. It can contain private device identity material, local authentication data and Federation authority state.

## Backup on Linux/macOS

Run from the FCP checkout:

```bash
set -eu
backup="$(pwd)/fcp-backup-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$backup"

relay_container="$(docker compose ps -a -q relay)"
[ -n "$relay_container" ] || { echo "Relay container not found. Start FCP once before taking a supported backup." >&2; exit 1; }
relay_volume="$(docker inspect "$relay_container" --format '{{range .Mounts}}{{if eq .Destination "/var/lib/fcp-relay"}}{{.Name}}{{end}}{{end}}')"
[ -n "$relay_volume" ] || { echo "Could not resolve the retained Federation relay volume." >&2; exit 1; }

git rev-parse --verify 'HEAD^{commit}' > "$backup/source-commit.txt"
printf '%s\n' "$relay_volume" > "$backup/relay-volume-name.txt"

docker compose stop
trap 'docker compose start' EXIT
cp -a data "$backup/data"
[ ! -d results ] || cp -a results "$backup/results"
[ ! -f .env ] || cp -p .env "$backup/.env"

docker run --rm \
  --mount "type=volume,src=$relay_volume,dst=/source,readonly" \
  --mount "type=bind,src=$backup,dst=/backup" \
  alpine sh -c 'cd /source && tar czf /backup/relay-state.tgz .'

docker compose start
trap - EXIT
```

POSIX identity files are permission-restricted and must remain private. Preserve ownership/permissions where the platform and backup medium support them.

## Same-installation restore

A same-installation restore means the same logical FCP device is being recovered. It is not permission to clone one identity onto two live hosts.

Before restoring:

1. keep the failed/original instance offline;
2. use the exact source commit recorded in `source-commit.txt`, or a separately validated migration path known to accept that state;
3. restore `data/`, `data/auth/`, `.env` when used, and `results/` when required as one coherent snapshot;
4. restore `relay-state.tgz` into a clean Docker volume and explicitly select that volume for the recovery start;
5. on Windows, confirm the restored private identity remains decryptable by the Windows security context before treating the recovery as the same node; and
6. start through the supported `--resume` launcher path and verify the node ID/Federation membership before re-enabling normal operation.

Do not unpack a backup over an active installation or merge an old relay database into a newer one. Restore to an empty target state or a dedicated recovery environment so old and new authority records are never mixed.

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

Federation v1 release acceptance must rehearse backup/recovery on the exact candidate commit. The evidence should record only non-secret identifiers and outcomes.

Minimum rehearsal:

- record the exact release-candidate commit;
- take a quiesced backup using the supported procedure;
- prove the backup includes the expected data/auth directories and relay archive without publishing their contents;
- restore into an isolated recovery target that does not run concurrently with the original identity;
- verify the same node ID where same-identity recovery is being tested;
- verify Federation reconnect/resume and persisted human-auth state where applicable;
- verify recorded data/checkpoints remain present;
- verify a replacement-member exercise uses a new identity rather than copied authority state; and
- explicitly record whether creator recovery was tested or remains a documented limitation of the candidate environment.

A green CI run does not replace this rehearsal. Release evidence must remain redacted: never commit private keys, passwords, hashes, salts, raw pairing codes, relay databases, `.env` secrets, source URLs containing credentials, or backup archives.

## What this guide does not promise

Federation v1 does not promise:

- portable cloning of a Windows DPAPI-protected node identity to arbitrary hardware/users;
- automatic transfer of creator or human credential authority to a promoted operational leader;
- online/hot copying of active SQLite/relay state;
- automatic restoration of standalone recorder processes launched outside the normal Compose/update-agent runtime; or
- a generic `restore` command that rewrites identity or Federation authority.

Those capabilities require separately reviewed authority, migration and threat-model work.

## Related guides

- [Server setup and deployment](server_setup.md)
- [Human users, sign-in, and permissions](human-authentication.md)
- [Federation operations](federation_operations.md)
- [Troubleshooting](troubleshooting.md)
- [Federation v1 scope](releases/federation_v1_scope.md)
