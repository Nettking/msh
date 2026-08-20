# Federation operations

Status: **current operator/administrator guide**
Reviewed: **2026-08-12**

This guide covers the user-facing Federation actions that intentionally mutate trusted distributed state: pairing, device naming, current-leader failover semantics, benchmark/contribution requests, provider review, software updates, recorder control, and Federation-visible JSONL.

Human web permissions and Federation device authority are separate. A browser user must have the relevant human permission, and the FCP device/session must independently satisfy the Federation-side authority check.

## Creator provenance, current leader, and coordinator availability

FCP separates three concepts that older documentation sometimes treated as one:

- **Federation creator** — immutable creation provenance.
- **Current operational leader** — the member holding the current coordinator-authored monotonic leadership term.
- **Authoritative coordinator/relay service** — the durable service/database that validates membership and leadership transitions.

The Federation overview exposes creator, current leader, and leadership term so operators can see which device currently owns leader-only product controls.

### Leader failover

A brief disconnect does not immediately move leadership. If the active leader remains offline beyond the bounded heartbeat/offline timeout and a valid connected successor exists, the coordinator can promote a deterministic successor.

After a valid transition:

- the leadership term increases monotonically;
- the former leader is fenced from current-leader operations;
- the successor receives the reviewed current-leader product controls; and
- an ordinary member cannot self-promote by publishing a lookalike event.

If no connected successor exists, FCP fails closed rather than inventing a leader.

This is **not** replicated coordinator/quorum failover. If the machine holding the authoritative coordinator database/relay is unavailable, leader promotion cannot replace that missing coordinator service.

### Human credential authority is separate

Operational leader transfer does not move the human password database. The immutable Federation creator remains the human credential/password authority used by Federation SSO. See [Human users, sign-in, and permissions](human-authentication.md).

## Pair another FCP device

The current Federation leader can generate a signed `FCP1-...` pairing code from the Federation onboarding surface.

The code is:

- signed;
- one-use;
- valid for up to **10 minutes**;
- scoped to the existing Federation/session and relay address; and
- not a persistent credential.

Generate a new code whenever another pairing attempt is needed.

For a normal FCP installation, paste it into the joining device's Federation onboarding flow. For a standalone recorder:

```bash
python start_recorder.py FCP1-...
```

After successful pairing, the joining device persists its stable identity and public-safe reconnect binding. It does not need the pairing code on later starts.

### Reachable address requirement

When another physical machine must connect, the issuing FCP installation must be reachable through a trusted LAN/VPN address rather than only `localhost`. Normal launchers publish both the web interface and Federation relay to loopback only. Use `start-tailscale.cmd`/`start-tailscale.sh`, or deliberately set both `FCP_WEB_BIND` and `FCP_RELAY_BIND` to trusted reachable interfaces before normal startup. Setting only the web bind is not enough because the relay remains loopback-only by default.

Do not expose the Flask workbench, relay, Ollama, or recorder control directly to the public internet.

## Optional Tailscale discovery

If the joining device and an existing FCP device are already signed in to the same Tailscale tailnet, start the joining FCP device with:

```cmd
start-tailscale.cmd
```

or:

```bash
bash start-tailscale.sh
```

FCP uses the local Tailscale client to discover public-safe FCP Federation advertisements from online Tailscale IPv4 peers. It does not request/store a Tailscale API key or auth key.

Discovery proves reachability only. The joining device must still obtain and redeem the normal `FCP1-...` pairing code. See [Tailscale Federation discovery](tailscale_federation_discovery.md).

## Rename Federation devices

Display names make it easier to identify physical machines without changing authority identity.

- Any trusted member can rename **itself** from **Federation -> This device**.
- The **current leader** can rename other current members from **Federation -> Devices**.

Names are bounded public Federation metadata. They must satisfy validation/uniqueness rules and do not replace the stable cryptographic `node_id`, membership, routing, provider authority, storage assignment, or update authority.

## Ask reachable members to benchmark and contribute

The **current Federation leader** can issue one bounded request asking all currently reachable remote members to refresh local capability state, run eligible locally registered benchmarks, and request contribution for locally allowed candidates.

This is a request for an outcome, not remote execution authority. The leader cannot select commands, executable paths, credentials, benchmark code, arbitrary candidate IDs, provider grants, or host configuration.

The boundary is:

1. the human browser user must have `federation.manage` permission;
2. the issuing device must be the current leader for the leadership term in force;
3. only currently reachable remote members are targeted;
4. the request is written through the authenticated Federation event log and expires after 10 minutes;
5. each target verifies the ordered leadership chain and accepts the command only from the leader valid at that revision;
6. the target refreshes its own local read-only capability inspection;
7. only locally registered/runnable benchmark definitions may execute;
8. contribution is requested only for locally eligible candidates; and
9. local contribution/provider policy is re-evaluated before activation.

Capabilities requiring separate approval may remain pending/registering. A leader request cannot bypass those approval or provider-policy gates.

Offline members are not queued for later. Issue another request after they reconnect if you want them included.

## Review pending contributions/providers

A device may request contribution while provider/control-plane authority is not yet active. The candidate then appears as registering/pending rather than silently granting itself authority.

Open:

```text
/provider-federation
```

on the **current Federation leader**.

The leader can create/review the durable enrollment record and explicitly approve, suspend, reject/revoke, or reconcile the candidate through the existing provider-enrollment authority.

Approval and activation remain separate. For example:

- AI approval grants no storage or compute authority;
- compute approval does not allow arbitrary executable code;
- storage approval does not invent a storage primary/replica assignment; and
- recorder control stays on its separate bounded recorder-control path.

## Federation-visible JSONL data

Supported non-recorder `data/**/*.jsonl` can be published through Federation logical storage and materialized on connected workbench members inside the normal local `data/` scan boundary.

The compatibility boundary remains local-file based: remote content is authenticated, hash/size verified, and materialized locally before unchanged recursive JSONL consumers see it.

Important behavior:

- recorder JSONL keeps its separate checkpoint/manifest publication path;
- generic JSONL publication requires current Federation membership;
- producer identity is bound to the authenticated Federation actor;
- traversal/non-JSONL paths fail closed;
- chunks/manifests are verified before materialization;
- exact duplicate file content is deduplicated; and
- publication/mirroring is bounded per reconnect pass.

Browser uploads under the supported `data/` corpus are included by default in this generic JSONL bridge when they are JSONL. To withhold uploaded JSONL from Federation publication on a specific installation:

```text
FCP_FEDERATED_JSONL_PUBLISH_UPLOADS=0
```

The local generic JSONL mirror is also quota-bounded. `FCP_FEDERATED_JSONL_MAX_MIRROR_BYTES` controls that advanced deployment limit.

This still does not expose arbitrary host files outside the reviewed FCP data corpus.

## Standalone recorder control

From any trusted Federation device, open **Federation -> Recorders**.

A trusted operator can request a bounded scan on a connected standalone recorder and add/remove sources selected from that recorder's own latest scan. The scan executes on the recorder host.

Remote members cannot inject an arbitrary MTConnect URL/credential or unrestricted network scan.

## Check for software updates

Open **Federation** on the **current operational leader** and choose **Check for updates**.

Other members can see resulting state but do not receive current-leader update controls.

The update system accepts only an exact commit on the approved repository `main`. Every participating host independently validates its checkout and requested target.

Typical results include:

- already running target;
- update available;
- activation required;
- dirty checkout;
- ahead/diverged checkout;
- offline/unavailable host; or
- bounded validation/activation failure.

A check does not apply an update.

## Update all devices

After reviewing the check, the current leader can choose **Update all devices**.

The rollout is manual. For each eligible normal FCP host, the host-owned agent:

1. revalidates the authenticated handoff, repository, branch, target, and clean tree;
2. permits only a fast-forward to the exact approved target;
3. rebuilds the required FCP runtime images;
4. restarts services;
5. preserves/resumes the saved device/Federation setup;
6. verifies required model/runtime readiness; and
7. reports success only when the running runtime proves the exact target commit.

A standalone MTConnect recorder is a native process rather than a Compose
service, so its host agent follows the same contract with a different
activation:

1. it revalidates the handoff, repository, branch, target and clean tree, and
   additionally refuses any target that changes Python dependency inputs, all
   while capture is still running;
2. it asks the running recorder to stop, naming the exact request, commit,
   supervisor, process ID and process-instance nonce it is for;
3. the recorder finishes its current capture/commit boundary and exits;
4. only then is the checkout fast-forwarded;
5. the supervisor starts exactly one replacement recorder; and
6. success requires that replacement to be a *different* process -- new process
   ID and new process-instance nonce, same supervisor -- running the exact
   target commit, with a fresh heartbeat and a connected Federation membership.

The successful UI state is **Updated**. A Git fast-forward alone is not success.

Each device is evaluated independently. One dirty/offline/failed member cannot make another member's result successful or failed.

Offline devices are not silently queued for later. Run another check after they reconnect.

## Host update-agent requirement

Normal FCP installations launched with the supported launchers start the bounded host update agent automatically:

### Windows

```cmd
start.cmd
```

### Linux/macOS

```bash
bash start.sh
```

A legacy Windows installation may need:

```cmd
migrate.cmd
```

### Standalone MTConnect recorder

A standalone recorder started with the supported launcher runs under a native
supervisor that starts its host update agent automatically:

```cmd
start-tailscale-recorder.cmd
```

No separate updater command is needed, and no recurring manual Git procedure is
expected. **Check for updates -> Update all devices** covers the recorder like
any other member.

One-time bootstrap: a recorder installed before this capability existed cannot
install an updater that is not in its current checkout. Each such recorder needs
one manual move to current `main` first -- see
[Standalone MTConnect recorder](standalone_recorder.md#federation-software-updates).

A recorder started directly with `python start_recorder.py` runs without that
supervisor. It still records, and still reports update *checks*, but it cannot
activate an update by itself; use the supported launcher for hosts that should
participate in **Update all devices**.

## If an update fails

Inspect the per-device Federation result first, then on the affected host:

Windows:

```cmd
type data\federation\update-agent\result.json
docker compose ps
```

Linux/macOS:

```bash
cat data/federation/update-agent/result.json
docker compose ps
```

On a standalone recorder host the equivalent files are the recorder's own, and
its journal names the exact stage the activation reached:

```cmd
type data\federation\recorder-update-agent\result.json
type data\federation\recorder-update-agent\journal.json
```

Do not use `git reset --hard`, `git clean`, delete Docker volumes, or remove Federation state simply to clear an update warning.

## Related guides

- [Quick start](quick_start.md)
- [Human users, sign-in, and permissions](human-authentication.md)
- [Tailscale Federation discovery](tailscale_federation_discovery.md)
- [Standalone recorder](standalone_recorder.md)
- [Server setup](server_setup.md)
- [Troubleshooting](troubleshooting.md)
