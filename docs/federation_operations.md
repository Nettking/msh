# Federation operations

Status: **current operator/administrator guide**

Reviewed: **2026-08-11**

This guide covers the operational Federation actions that intentionally mutate trusted distributed state: pairing another device, reviewing pending contribution enrollment, checking for software updates, and rolling an approved update across the Federation.

These actions are deliberately narrower than general remote administration. FCP does not expose a Federation shell, arbitrary process execution, peer-selected Git repositories, or unrestricted host configuration.

## Pair another device

A Federation member with local Federation authority can generate a signed `FCP1-...` pairing code from the onboarding Federation surface.

The current browser-generated code is:

- signed by the issuing device;
- one-use;
- valid for up to **10 minutes**;
- scoped to the existing Federation/session and relay address; and
- never intended to become a persistent credential.

You may generate another code whenever a fresh pairing attempt is needed. Each generated code keeps its own one-use and expiry boundary; generating another code does not convert either code into a reusable secret.

For another normal FCP installation, paste the code into its Federation onboarding flow. For a standalone recorder, pass it directly on first start:

```bash
python start_recorder.py FCP1-...
```

After successful pairing, the joining device persists its stable device identity and public-safe reconnect binding. It does **not** need the pairing code on later starts.

### Reachable address requirement

When another physical machine must connect, open the issuing FCP installation through a LAN or VPN address reachable by the joining machine before generating the code. A code created while the product is only reachable as `localhost` cannot make that loopback address reachable from a different computer.

Do not expose the Flask workbench, relay, Ollama, or recorder control surface directly to the public internet.

## Review pending contributions

A member may explicitly enable a supported local contribution while its runtime/control-plane authority is not yet active. The member then publishes the candidate to the Federation as **Registering** rather than silently granting itself authority.

On the Federation creator/session-owner device, open:

```text
/provider-federation
```

The leader can create the durable review record and then explicitly approve, suspend, reject/revoke, or reconcile the candidate through the existing provider-enrollment authority. Other members do not receive those mutation controls.

Approval and activation are deliberately separate:

- approving a `REGISTERING` candidate records the leader's explicit decision;
- the approved record remains ineligible for resource binding until the announcing member provides the existing `READY` runtime evidence;
- storage approval does not create a storage primary/replica assignment or invent a storage-provider runtime;
- compute approval does not start a worker or transfer executable code;
- AI approval grants no storage or compute authority; and
- recorder control stays on the separate bounded recorder-control path.

For a newly visible candidate, **Request** first creates the durable revision-fenced enrollment record. **Approve** then records the leader decision. A pending record can be rejected through **Revoke**; the product retains the audit trail instead of deleting the candidate history.

If an already-authoritative local storage provider also appears as a candidate-only row on the Storage page, that is a projection error: candidate decision IDs and storage provider IDs are separate identity domains. The Storage page should suppress the candidate-only duplicate when its explicit provider identity is already represented by storage-control-plane state.

See [the active pending-contribution approval contract](implementation/federation/active/pending_contribution_approval.md) for the authority and acceptance boundary.

## Check for software updates

Open **Federation** on the Federation coordinator/session-creator device and use **Check for updates**.

The check is coordinator-managed. Other Federation members can see the resulting state but do not receive the coordinator's update controls.

The update system accepts only an exact commit from the approved repository `main`. Every participating host independently validates its local checkout and the requested commit before reporting whether activation is possible.

Typical outcomes include:

- already running the approved target;
- update available;
- source is current but runtime activation is required;
- dirty local checkout;
- ahead or diverged checkout;
- offline or unavailable host; or
- another bounded validation/activation failure.

A check does not automatically apply an update.

## Update all devices

After reviewing a successful check, the coordinator can explicitly choose **Update all devices**.

The rollout is manual by design. FCP does not automatically update on start or reconnect.

For every eligible queued host, the host-owned update agent:

1. revalidates the request, repository, branch, exact commit, and clean working tree;
2. fetches the approved `main` and permits only a fast-forward to the exact target;
3. rebuilds the `relay`, `flask`, and `recorder` images with the target commit baked into the runtime;
4. restarts the required services;
5. preserves the saved device/Federation state and resumes the existing setup;
6. verifies the configured Ollama model when required; and
7. proves that the running Flask image reports the exact target commit and that `relay`, `recorder`, and `flask` are running.

The coordinator is activated after the remote eligible devices have been queued so the initiating state is durable before the coordinator restarts itself.

### What success means

The internal terminal activation state is `runtime_verified`. The product UI presents that successful state as **Updated** with the green success indicator.

Success means the running runtime commit equals the exact approved target. A Git fast-forward by itself is not reported as success.

### Independent failure semantics

Each device is evaluated independently. One dirty, offline, divergent, or failed host cannot make another host's result successful or failed.

Offline devices are not silently queued for later. Run another check after they reconnect.

Do not repeatedly press **Update all devices** while an activation is already queued. Allow the hosts to rebuild/restart and report their terminal state.

## Host update-agent requirement

Federation-wide updates require the bounded host update agent to already be installed and running on each participating device.

The supported launchers start it automatically:

### Windows

```cmd
start.cmd
```

### Linux/macOS

```bash
bash start.sh
```

A legacy Windows installation from before the update capability was introduced may need the one-shot migration bootstrap first:

```cmd
migrate.cmd
```

The migration path preserves existing identity, Federation state, evidence, data, models, and retained relay state while bringing the checkout onto the current supported launcher/update-agent path. It fails closed instead of resetting or guessing when the old checkout or relay state cannot be identified safely.

## Why Flask cannot update the host directly

The Flask process does not receive Git, Docker, shell, or arbitrary host-execution authority. Federation update intent is a bounded authenticated session event. Flask writes a bounded local handoff under the FCP data directory, and the separate host-owned agent validates that handoff again before it can mutate Git or Docker.

The host agent permits only the locally fixed update procedure. It does not accept peer-supplied commands, repositories, paths, executables, or arguments.

## If an update fails

Start with the per-device result shown in Federation. On the affected host, inspect the host-agent result file and service state rather than deleting Federation state:

```cmd
type data\federation\update-agent\result.json
docker compose ps
```

On Linux/macOS:

```bash
cat data/federation/update-agent/result.json
docker compose ps
```

A dirty checkout must be reviewed and cleaned intentionally before an update can proceed. Do not use `git reset --hard`, `git clean`, delete Docker volumes, or remove device/Federation state merely to make an update indicator green.

See also:

- [Quick start](quick_start.md)
- [Server setup](server_setup.md)
- [Standalone recorder](standalone_recorder.md)
- [Troubleshooting](troubleshooting.md)
- [Pending-contribution approval contract](implementation/federation/active/pending_contribution_approval.md)
- [Detailed update security design](implementation/federation/active/manual_updates.md)
