# Standalone MTConnect recorder

Status: **current user/operator guide**
Reviewed: **2026-08-18**

The standalone recorder is a headless FCP device for loss-aware MTConnect capture. It can join an existing Federation, discover local MTConnect Agents, keep recording locally through Federation outages, publish checkpoint-committed observations to Federation logical storage, and accept bounded recorder-local source controls from trusted Federation devices.

## Normal Windows/Tailscale first start

Install/sign in to Tailscale on the recorder host, use the same reviewed same-owner/same-tailnet environment as the Federation, then run from the recorder checkout:

```cmd
start-tailscale-recorder.cmd
```

That is the normal v1 first-start command.

It does **not** ask for:

- an `FCP1-...` pairing code;
- Federation email/username or password;
- a Federation/relay IP address; or
- a storage-group ID when exactly one ready logical-storage group is available.

The launcher:

1. verifies that local Tailscale has a signed-in IPv4 address inside `100.64.0.0/10`;
2. performs bounded FCP Federation discovery over online Tailscale peers;
3. requires exactly one unambiguous discovered Federation;
4. asks that Federation's host responder to authorize the recorder using the same reviewed Tailscale peer-identity boundary as full FCP devices;
5. receives the existing signed, short-lived, one-use pairing grant;
6. keeps the grant out of shell arguments/history and hands it only to the in-process recorder enrollment path;
7. persists only the resulting stable device identity and public-safe reconnect binding;
8. validates the saved/issued relay as a literal Tailscale peer and checks TCP reachability;
9. requires a usable Federation publication path;
10. runs the existing bounded private-network MTConnect discovery; and
11. starts local-first capture plus Federation publication/control workers.

If automatic Tailscale identity authorization is refused, the launcher fails closed. It does not replace that refusal with a hidden manual token or a local-only recorder that looks healthy.

## Later starts

After the first successful join, run the same command:

```cmd
start-tailscale-recorder.cmd
```

The recorder reuses its stable device identity, saved Federation binding, source selection, capture checkpoints, and durable publication state. It does not rediscover/re-enroll merely because the process restarted.

## MTConnect source discovery and late source arrival

On an unconfigured recorder, the normal bounded private-network scan runs on first startup. If exactly the intended local source is discoverable, normal source-selection rules can select it without manual URL entry.

If the MTConnect Agent does not exist or is not reachable yet, the recorder retains the existing start-before-source behavior: retry is bounded/backed off rather than a busy-poll, and recording begins when the source later becomes available.

If the host cannot infer the intended private `/24`, an explicit recovery/deployment network remains available:

```cmd
start-tailscale-recorder.cmd --scan-cidr 192.168.10.0/24
```

Network discovery remains bounded to validated private IPv4 scopes with the existing timeout, redirect, and response-size protections.

## Logical-storage publication

Recorder capture is always local-first. A Federation acknowledgement is not part of the local capture commit boundary.

Checkpoint-committed observations become eligible for publication through the existing authenticated logical-storage authority. Publication applies the current assignment, fencing, acknowledgement, primary/replica, and manifest rules; the recorder never selects an arbitrary database directly.

If exactly one ready logical-storage group exists, it may be selected automatically. With genuinely multiple groups, choose one explicitly:

```cmd
start-tailscale-recorder.cmd --storage-group telemetry
```

A normal full FCP Federation creator supervises logical-storage authority automatically. The existing authority monitor still refuses creator-owned authority service on non-creator members.

The leader's storage control plane is the coordinator database itself, named by `FCP_FEDERATION_COORDINATOR_DATABASE` (Compose points both the relay and Flask services at `/var/lib/fcp-relay/control.sqlite3` on the shared `relay_state` volume). The relay reconciles trusted GREEN storage into that control plane and the supervised authority announces its ready groups from the same file, so both sides always agree. An authority reading any other database would report no ready groups and every recorder would correctly refuse to publish.

When Federation/storage is temporarily unavailable after an established recorder is running, capture stays local and durable publication state retries later. Do not delete the publication outbox or move checkpoints backward as a recovery technique.

## Data paths

The recorder's durable local state normally includes:

```text
data/capabilities/config.json
data/source_state/mtconnect_recorder_state.json
data/source_state/mtconnect_recorder_status.json
data/source_state/mtconnect_recorder.log
data/source_state/mtconnect_recorder_autoconfig.json
data/federation/device/
data/federation/onboarding/
data/federation/recorder_publication/
data/federation/recorder_control/
data/federation/recorder_update/
data/federation/recorder-update-agent/
data/federation/jsonl-sync.sqlite3
data/federation/jsonl-cache/
```

`recorder_update/` and `recorder-update-agent/` hold only bounded updater state:
the replayed Federation update revision, the current activation, and a bounded
journal of recent activations. They are the one thing an update may rewrite;
everything else above is preserved across updates.

Recorder observations under:

```text
data/sources/mtconnect_recorder/jsonl/**
```

use the stronger checkpoint/sequence/hash-verified recorder publication path. Other supported non-recorder JSONL files use the generic authenticated JSONL synchronization path. Recorder JSONL is excluded from the generic path to avoid duplicate storage/analysis. Raw MTConnect XML/probe archives remain local-only.

## Federation recorder controls

From any trusted workbench device open:

```text
/federation/recorders
```

A trusted operator can request a recorder-local bounded scan, review machines returned by that recorder, add source IDs from the recorder's latest validated scan, and remove currently configured sources.

Remote recorder control does not grant arbitrary shell or arbitrary network authority. A remote member cannot inject executables, arbitrary MTConnect URLs/credentials, unrestricted scan targets, or target a normal device that does not advertise the recorder capability.

## Explicit/manual recovery pairing

The signed `FCP1-...` path remains available for deployments where automatic same-owner Tailscale authorization is deliberately not applicable:

```bash
python start_recorder.py FCP1-...
```

Later manual-path starts can use:

```bash
python start_recorder.py
```

This is a recovery/manual deployment surface, not the normal Tailscale first-start flow. Pairing codes remain signed, one-use, short-lived and are not persisted after successful enrollment.

Do not put an `FCP1-...` code on the `start-tailscale-recorder.cmd` command line. That launcher intentionally rejects such arguments.

## Useful options

```text
--device-name NAME            Recorder display label
--storage-group ID            Explicit logical Federation storage group
--federation-timeout SECONDS  Federation request timeout
--require-federation          Fail instead of local-only initial membership
--require-data-sharing        Require a ready/confirmed publication route
--sharing-timeout SECONDS     Bounded wait for required sharing
--scan-cidr CIDR              Explicit private IPv4 scan network
--scan-port PORT              MTConnect scan port (default 5000)
--no-auto-scan                Deliberately skip startup discovery
--once                        Run one recorder catch-up cycle and exit
```

The Tailscale wrapper adds `--require-federation` and `--require-data-sharing` itself for the normal supported path.

## Federation software updates

A recorder started with `start-tailscale-recorder.cmd` runs under a native
supervisor and participates in **Check for updates -> Update all devices** like
any other Federation member. There is no separate updater to start and no
recurring manual Git procedure.

Update commands travel over the recorder's existing authenticated Federation
connection. No second Federation identity, relay connection, or reader is
created, and no peer ever supplies an executable, path, command, argument, URL,
or environment value.

### What an update does on this host

1. **Prevalidate while still recording.** The checkout must be the expected one,
   on `main`, with the canonical approved FCP origin, a clean tree including
   untracked files, and a full 40-character target that is a fast-forward from
   the current commit on fetched approved `main`. An update that would change
   Python dependency inputs (`requirements.txt`, constraints files,
   `pyproject.toml`) is refused here rather than after capture has stopped.
2. **Stop gracefully.** The host agent asks this exact process to stop, naming
   the request, commit, supervisor, process ID, process-instance nonce and an
   expiry. The recorder accepts it only when every field matches and it has a
   durable pending Federation update request for that commit. It then finishes
   the current capture/commit boundary and exits, exactly as `Ctrl+C` would.
   Nothing is force-killed.
3. **Update the source only after the process is gone.** The checkout is
   fast-forwarded with `git merge --ff-only` and nothing else -- never `reset`,
   `clean`, `stash`, or a rewrite.
4. **Relaunch once, then prove it.** The supervisor starts exactly one
   replacement using the same locally resolved interpreter and arguments it was
   started with. Success requires a different process ID *and* a different
   process-instance nonce under the same supervisor, running the exact target
   commit, with a heartbeat newer than the activation and a connected
   Federation membership.

An interrupted update is resumable: a bounded durable journal under
`data/federation/recorder-update-agent/journal.json` records the stage reached,
duplicate requests are answered from the retained result rather than applied
twice, and a stop request left behind by an earlier process is ignored by any
recorder it does not name.

Nothing durable is touched: pairing, the stable device identity, MTConnect
checkpoints, raw and JSONL captures, the publication outbox and the backlog all
survive an update unchanged.

### One-time bootstrap for recorders installed before this change

A recorder cannot install an updater that does not exist in its current
checkout, so each already-deployed recorder needs one manual move to current
`main`. On the recorder host:

```cmd
REM 1. Stop the running recorder cleanly (Ctrl+C in its window), then:
cd C:\path\to\your\fcp-checkout
git status --porcelain --untracked-files=all
git fetch --no-tags origin main
git merge --ff-only origin/main
start-tailscale-recorder.cmd
```

`git status` must print nothing before the merge. If it does not, review those
local changes first -- do not use `git reset --hard`, `git clean`, or `git
stash` to clear them.

After that one start, the recorder participates in **Update all devices**
automatically.

### Limitations

- An update that changes Python dependency inputs is refused rather than
  applied. Prepare that environment and bootstrap manually as above.
- A recorder started directly with `python start_recorder.py` has no supervisor,
  so it reports update checks but cannot activate an update.

## Failure behavior

The zero-touch Tailscale recorder exits non-zero rather than silently degrading when initial trust/publication requirements are not met. Exit code `75` is reserved for one thing only: an approved Federation update restart, which the supervisor handles automatically. Typical actionable failures include:

- Tailscale missing/logged out/no valid IPv4;
- no Federation discovered;
- multiple ambiguous Federations discovered;
- peer identity refused by the Federation responder;
- no valid one-use grant returned;
- saved/issued relay not a literal reachable Tailscale peer;
- Federation membership invalid; or
- required logical-storage publication unavailable.

Once local capture is established, later Federation outages remain local-first/durable as described above.

## Related guides

- [Quick start](quick_start.md)
- [One-command setup](one_command_setup.md)
- [Tailscale Federation discovery](tailscale_federation_discovery.md)
- [Federation operations](federation_operations.md)
- [Troubleshooting](troubleshooting.md)
