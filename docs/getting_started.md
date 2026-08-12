# Getting started with FCP

Status: **current user guide**
Reviewed: **2026-08-12**

This guide gives the product mental model. For installation commands, start with [Quick start](quick_start.md).

## What FCP is

FCP is a workbench for collecting, understanding, and sharing machine-related capabilities across trusted devices.

One installation represents one persistent device. A device can use the workbench and independently contribute capabilities such as recording, a language model, registered compute handlers, or storage capacity. A device does not receive one permanent product role.

People have separate human web accounts. Human roles control browser actions; they do not replace device identity or Federation authority.

## The ideas to understand first

### 1. Human users and devices are different identities

A fresh local FCP authority has no default human administrator. Start FCP and open the browser. While the local user database is empty, FCP redirects you to `/admin/users/bootstrap`, where you create exactly the first active administrator. Then sign in and continue device onboarding.

Additional users are administered at `/admin/users`. See [Human users, sign-in, and permissions](human-authentication.md).

A remotely paired member with no local shadow users does not expose first-admin bootstrap; it signs people in through the Federation human credential authority instead.

### 2. A device has a persistent identity

Each FCP installation has a stable cryptographic node identity. Normal restarts and updates reuse it. A display name is only human-friendly Federation metadata and never replaces the stable `node_id`.

You can rename your own device under **Federation -> This device**. The current Federation leader can rename other members under **Federation -> Devices**.

### 3. Devices meet inside a Federation

A Federation is the trusted boundary in which FCP devices share approved state and capabilities. Network visibility is not trust.

Additional devices normally join with a signed one-use `FCP1-...` pairing code, valid for up to 10 minutes.

If both devices already use Tailscale, FCP can discover a reachable Federation through the already signed-in Tailscale client. That only helps you find the other FCP device; it does not grant membership. You still redeem the normal pairing code. See [Tailscale Federation discovery](tailscale_federation_discovery.md).

### 4. Creator provenance and current leadership are different

The Federation creator is immutable provenance. Operational leader authority can move to another connected member through a coordinator-authored monotonic leadership term.

If the current leader remains offline beyond the bounded timeout and a valid connected successor exists, the coordinator can promote a deterministic successor. The former leader is then fenced from current-leader controls.

This leader failover assumes the authoritative coordinator/relay service remains available. It is not replicated-quorum failover of the coordinator database itself.

### 5. Human credential authority is separate from operational leadership

Federation human passwords are not replicated to every member. The immutable Federation creator remains the human credential/password authority and members use signed Federation SSO assertions.

Operational leader transfer does not automatically move the password database. This separation prevents a temporary operational leader transition from silently transferring human credential custody.

### 6. Inspection describes what a device can do

During onboarding, FCP inspects the device and stores capability evidence. Inspection is evidence, not permission.

### 7. Contribution is an explicit choice

A device can independently contribute recorder, AI, compute, or storage-related capabilities. Contribution intent is separate from provider/storage authority and may still require leader review or another control-plane decision.

### 8. Federation controls are bounded operations, not remote shell access

Reviewed distributed actions include:

- current-leader pairing/member administration;
- current-leader benchmark/contribution requests;
- provider-enrollment review;
- manual Federation-wide software updates; and
- standalone-recorder scan/source control.

Peers send authenticated declarative intent. Targets execute only fixed locally validated operations. FCP does not expose a general Federation shell.

### 9. Recording is local-first

The MTConnect recorder commits capture/checkpoints locally before Federation publication. Relay or storage outages do not move the checkpoint backward or block normal polling.

### 10. The workbench is where you use the system

After onboarding, the workbench exposes Federation, Monitor, Knowledge, System, sources, recording, workflows, AI, playback, and analysis according to the human user's permissions and current device/Federation authority.

## Your first ten minutes

1. Run `start.cmd` on Windows or `bash start.sh` on Linux/macOS.
2. Open the FCP URL and create the first administrator in the browser if prompted.
3. Sign in.
4. Complete **Identity**.
5. Join/reconnect/create the **Federation**.
6. Run **Inspect**.
7. Finish setup and open **Federation**.
8. Give the device a useful display name if needed.
9. Pair additional trusted devices, optionally using Tailscale discovery to find the existing FCP endpoint.
10. Review optional benchmarks/contributions only when relevant.

```text
Start FCP
  -> browser first-admin setup
  -> Human sign-in
  -> Identity
  -> Federation
  -> Inspect
  -> finish setup
```

## Common next steps

### Manage human users

Use `/admin/users` and [Human users, sign-in, and permissions](human-authentication.md).

### Connect another device

Use [Federation operations](federation_operations.md). If both hosts already use Tailscale, also see [Tailscale Federation discovery](tailscale_federation_discovery.md).

### Ask members to benchmark/contribute

The **current operational leader** can ask currently reachable remote members to refresh local inspection, run eligible locally registered benchmarks, and request contributions that local policy allows. The request cannot inject commands or bypass provider approval.

### Update all devices

The **current operational leader** runs **Check for updates** and then **Update all devices** after reviewing the exact approved target and per-device states.

### Use a standalone recorder

```bash
python start_recorder.py FCP1-...
```

See [Standalone recorder](standalone_recorder.md).

### Change appearance

Use the light/dark switch in the top menu. FCP follows the OS preference until you make an explicit browser-local choice.

## Important boundaries

- human login is not device identity;
- a human `admin` permission does not bypass Federation-side authority;
- Tailscale/network discovery is not Federation membership;
- inspection and benchmarks are evidence, not activation;
- contribution intent is not provider/storage authority;
- display names do not replace cryptographic node identity;
- current operational leader authority is not the same as immutable creator provenance;
- human credential authority remains separate from transferable operational leadership;
- AI does not approve, assign authority, or execute unregistered code;
- Federation updates cannot select arbitrary repositories/commands;
- recorder control cannot inject arbitrary URLs or unrestricted scans.

## Where to read next

- [Quick start](quick_start.md)
- [Operator guide](operator_guide.md)
- [Human users, sign-in, and permissions](human-authentication.md)
- [Federation operations](federation_operations.md)
- [Tailscale Federation discovery](tailscale_federation_discovery.md)
- [Troubleshooting](troubleshooting.md)
- [Current architecture](architecture.md)
- [Federated network reference](federated_session_network.md)
