# Troubleshooting

Status: **current operator guide**
Reviewed: **2026-08-12**

Use `/status` first for local runtime/data problems and **Federation** first for membership, leadership, distributed update, contribution, discovery, or standalone-recorder control problems.

Do not delete identity, Federation databases, Docker volumes, auth state, recorder checkpoints, or capability evidence merely to clear a warning.

## Fresh installation shows no normal login

A fresh local authority has no default human account. FCP should redirect normal browser requests to:

```text
/admin/users/bootstrap
```

Create the first administrator there with a valid email and a confirmed password of at least 12 characters.

After the first account commits, the anonymous bootstrap closes and normal sign-in takes over.

If the bootstrap claim exists while the user table is unexpectedly empty, FCP deliberately fails closed instead of allowing another anonymous administrator. Investigate the auth database/state; do not delete a single guard file to bypass it.

A remotely paired member with an empty shadow-user database does not show local first-admin setup; use Federation sign-in.

## Federation device is missing or will not reconnect

Check:

1. the device is using its existing `data/` directory and stable identity;
2. relay/Flask networking is reachable on the trusted LAN/VPN/Tailscale path;
3. the device was paired through a current signed `FCP1-...` code or already has a saved trusted binding; and
4. the issuing FCP address encoded by the pairing flow was reachable from the joining host.

Browser pairing codes are one-use and valid for up to 10 minutes. Generate a new code after expiry/redemption.

## Tailscale discovery finds nothing

Run on the host:

```bash
tailscale status
tailscale ip -4
```

Check that:

- both devices are signed in to the same intended tailnet;
- the existing FCP device is online;
- its FCP web interface is reachable over its Tailscale IPv4 address and configured port;
- a firewall is not blocking that port; and
- the advertising FCP device has local Federation authority rather than being only a remotely paired member.

Discovery is a startup snapshot. Run `start-tailscale.cmd` or `bash start-tailscale.sh` again to refresh it.

If the Federation is shown but onboarding still asks for `FCP1-...`, that is expected: Tailscale discovery is reachability, not Federation membership.

See [Tailscale Federation discovery](tailscale_federation_discovery.md).

## Leader-only controls disappeared from this device

Open Federation Overview and inspect **creator**, **current leader**, and **leadership term**.

After a valid leader transition, the former leader is fenced from leader-only controls such as Federation-wide updates, leader capability requests, and reviewed member/provider administration.

If the previous leader only disconnected briefly, bounded grace prevents immediate churn. If it remains offline and a valid connected successor exists, the coordinator may advance the leadership term.

If the authoritative coordinator/relay service itself is unavailable, automatic leader failover cannot replace it; the current implementation does not provide replicated coordinator quorum.

Human sign-in may still use the creator-backed credential authority even when a different device is current operational leader.

## Federation update check times out

Verify the normal host-owned update agent is running. Supported launchers start it automatically:

```cmd
start.cmd
```

or:

```bash
bash start.sh
```

Inspect:

Windows:

```cmd
type data\federation\update-agent\result.json
```

Linux/macOS:

```bash
cat data/federation/update-agent/result.json
```

Common safe failure states include dirty checkout, ahead/diverged checkout, unapproved remote/branch, unavailable target, runtime verification failure, or timeout.

Do not use `git reset --hard`, `git clean`, or delete Federation state to bypass checks.

## Update is stuck on Activation Queued

`Activation Queued` means the device accepted the update request but has not yet proved the exact running target.

Wait for build/restart/verification before the current leader starts another rollout. Check:

```bash
docker compose ps
```

and the update-agent result file.

The successful UI state is **Updated**, which means the running runtime commit equals the exact requested target.

## Older Windows installation cannot participate in Update all

Use the conservative migration bootstrap:

```cmd
migrate.cmd
```

It preserves current identity/Federation/evidence/data/auth/model state and fails closed on ambiguous old relay state rather than deleting/guessing.

## A member did not benchmark/contribute after a leader request

Leader capability requests target only remote members that are reachable when the request is created. Offline members are not queued.

On the target, the request may legitimately produce no contribution when:

- no registered benchmark is locally runnable;
- the candidate is not locally `ALLOWED`;
- prerequisites are missing; or
- provider enrollment still requires explicit approval.

Issue another request after a member reconnects. A current-leader request cannot bypass member-local policy.

## Device name change is rejected

A device can rename itself under **Federation -> This device**. The current leader can rename other members under **Devices**.

Names must satisfy bounded validation and case-insensitive uniqueness/reserved-name rules. The stable `node_id` remains the technical identity even when the display label changes.

## Standalone recorder does not find machines

First join:

```bash
python start_recorder.py FCP1-...
```

If no suitable private IPv4 `/24` can be inferred, join first and request a scan later from `/federation/recorders`, or pass an explicit validated network:

```bash
python start_recorder.py FCP1-... --scan-cidr 192.168.200.0/24
```

Verify the MTConnect Agent is reachable from the recorder host itself.

## Remote recorder scan/source change does not complete

Open **Federation -> Recorders** and verify:

- the recorder is connected;
- its Federation control worker is running;
- it can reach the target private network; and
- source additions come from its latest scan result.

Remote additions cannot inject arbitrary URLs/credentials. Removing a source does not delete historical telemetry/checkpoints.

## Recorder records locally but Federation storage is empty

Local recording is the primary commit boundary. During relay/storage outages, capture can continue while publication waits in the durable outbox.

Check current Federation membership, ready logical-storage authority/group, and publication reconnect state. Do not move checkpoints backward to force upload.

## Data from another Federation device does not appear

There are two separate paths:

### Recorder telemetry

Recorder batches become visible only through the committed manifest/checkpoint path and are materialized under the verified local recorder mirror.

Check membership, recorder committed watermark, storage authority/group, and local mirror quota.

### Generic non-recorder JSONL

Supported non-recorder `data/**/*.jsonl`, including browser-uploaded JSONL by default, can be published through the generic Federation logical-storage bridge and materialized under the local `data/federation/shared/...` boundary.

Check:

- both devices are current Federation members;
- the publishing file is valid JSONL inside the supported `data/` corpus;
- generic Federation storage synchronization is running;
- the local generic mirror has quota available; and
- the publishing installation has not set:

```text
FCP_FEDERATED_JSONL_PUBLISH_UPLOADS=0
```

Do not point legacy scanners directly at storage-provider internal directories. Remote data is accepted only after the relevant Federation verification/materialization path succeeds.

## Flask starts but no local data appears

Check:

- `data/` mount/path;
- JSONL placement under scanned roots;
- parseable timestamps/filenames; and
- `/status` bootstrap/filter state.

Example:

```bash
find data -name '*.jsonl' -print | head
```

## Playback has no selectable data

Playback requires a workflow session with filtered data and a timeline export under `results/workflows/`. Rerun the workflow/data visualizer from `/control` when the export is missing/stale.

## A script failed during bootstrap/catch-up

One best-effort script failure does not prevent Flask from starting. Inspect `/status` and `/control`, fix the specific data/dependency error, then rerun the failed action.

## Docker/dependency issues

Prefer the supported launcher/update path. For targeted development troubleshooting:

```bash
docker compose up -d --build flask
docker compose ps
docker compose logs -f
```

## Runtime appears stuck after restart

Open `/onboarding` when setup is incomplete. Otherwise use `/status` and `/control`.

Do not restore old role-first startup behavior; retained legacy setup state is migration input only.

## Related guides

- [Quick start](quick_start.md)
- [Human users, sign-in, and permissions](human-authentication.md)
- [Federation operations](federation_operations.md)
- [Tailscale Federation discovery](tailscale_federation_discovery.md)
- [Standalone recorder](standalone_recorder.md)
- [Server setup](server_setup.md)
