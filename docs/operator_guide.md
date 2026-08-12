# Operator guide

Status: **current user guide**
Reviewed: **2026-08-12**

This guide describes normal operation after FCP starts. A device is capability-first and is not assigned one permanent server role.

## First use

A new local authority uses this path:

```text
Start FCP
  -> create first administrator in browser
  -> Human sign-in
  -> Identity
  -> Federation
  -> Inspect
  -> finish setup
```

When there are zero local human users, FCP redirects normal browser requests to `/admin/users/bootstrap`. Create the first active administrator there, sign in, and continue onboarding. See [Human users, sign-in, and permissions](human-authentication.md).

A remotely paired Federation member with an empty local shadow-user database uses Federation sign-in instead of local first-admin bootstrap.

## Federation

Use Federation to understand and operate distributed product state.

- **Overview** — current device, creator/current leader/term, connection, recommendations, software updates, capability-request state, and safe high-level status.
- **This device** — identity, self display name, inspection, local capabilities, and contribution state.
- **Devices** — authorized Federation members, names, and connection state.
- **Services** — contributed service state without private endpoint disclosure.
- **Recorders** — connected standalone recorder scans/source selection.
- **Benchmarks** — optional benchmark evidence and rerun/invalidation state.
- **Storage** — storage candidates and assigned authority.
- **Jobs** — authorized job state and registered-handler execution.
- **Activity** — public-safe Federation events/diagnostics.

### Creator and current leader

The Federation creator is immutable provenance. Operational leader authority can move to a successor through a coordinator-authored monotonic term after the previous leader remains offline beyond the bounded timeout.

The current leader owns reviewed leader-only product controls. A former leader is fenced after a valid transition.

Leader failover assumes the authoritative coordinator/relay remains available; it is not replicated-quorum failover of that service.

Human credential/password authority remains creator-backed and separate from transferable operational leadership.

### Rename devices

- Rename this device from **Federation -> This device**.
- The current leader can rename other members from **Federation -> Devices**.

Names are Federation-scoped public display metadata. They do not replace stable `node_id` identity or change authority.

### Pair another device

The current leader generates a signed one-use `FCP1-...` pairing code, valid for up to 10 minutes. The joining device redeems it in onboarding.

If both hosts already use Tailscale, the joining host can run `start-tailscale.cmd` or `bash start-tailscale.sh` to find the existing FCP Federation. Discovery only finds a reachable endpoint; the `FCP1-...` code is still required. See [Tailscale Federation discovery](tailscale_federation_discovery.md).

### Ask members to benchmark and contribute

The current leader can ask currently reachable remote members to refresh local inspection, run eligible locally registered benchmarks, and request locally allowed contributions.

The request cannot inject commands, paths, arbitrary benchmark code, provider grants, credentials, or host configuration. Each member re-evaluates local policy and any separate approval requirements.

Offline members are not queued. Issue another request after they reconnect.

### Review provider/contribution requests

Pending/registering provider candidates are reviewed on `/provider-federation` by the current leader. Approval does not automatically create unrelated compute/storage/AI authority.

### Software updates

Only the current leader receives **Check for updates** and **Update all devices**.

Use them in order:

1. **Check for updates**.
2. Review the exact approved target and every device state.
3. **Update all devices** only when you intend to activate that target.
4. Wait for terminal per-device results.

A device is shown as **Updated** only after its running runtime proves the exact target commit.

### Federation-visible JSONL

Supported non-recorder JSONL under `data/` can be published through Federation logical storage, verified, and materialized under the local data scan boundary on connected workbench members.

Recorder telemetry keeps its separate checkpoint/manifest path. Exact duplicate generic JSONL content is deduplicated.

Browser-uploaded JSONL is included by default; an advanced deployment can set `FCP_FEDERATED_JSONL_PUBLISH_UPLOADS=0` to withhold uploaded JSONL from Federation publication on that installation.

### Standalone recorder control

Open **Federation -> Recorders** from any trusted Federation device. A scan executes on the recorder host, and additions can select only opaque source IDs from that recorder's latest bounded scan. Remote users cannot inject arbitrary URLs or unrestricted scans.

## Monitor

Use Monitor to inspect current/replayed telemetry and data visible to this workbench.

- **Overview** — local runtime/data readiness.
- **Live** — recent telemetry.
- **Playback** — prepared historical replay.
- **Assist** — possible causes/actions/risks with operator validation.
- **Status/Diagnostics** — runtime, recorder, source, cache, and failure details.

Federation JSONL and recorder mirrors are materialized into the normal local data boundary only after their respective verification paths succeed.

## Knowledge

Use Knowledge to capture and structure operator experience:

- **Capture** — save a raw statement quickly.
- **Review Notes** — structure captured statements.
- **Strategies** — compare context/action/evidence/confidence/outcomes.
- **Intervention Logic** — maintain technical detection rules.
- **SysML Export** — current compatibility export.

Federation knowledge sharing may degrade to the local cache when Federation access is unavailable. Credential-shaped content is refused at the shared-event write boundary rather than being committed into durable Federation history.

## System

Use System for local configuration and support:

- **Sources** — configure supported machine/data connections.
- **Documentation** — browse current repository docs through `/docs`.
- **Diagnostics** — inspect local runtime/failures.

Human admins manage web accounts at `/admin/users`.

## Appearance

Use the light/dark switch in the top menu. With no explicit choice, FCP follows the operating-system preference. An explicit choice is saved in that browser and applies before first paint across the normal shell, onboarding, sign-in/account surfaces, and docs portal.

## Recommended workflow

1. Start FCP with the supported launcher.
2. On a fresh local authority, create the first administrator in the browser and sign in.
3. Complete Identity, Federation, and Inspect when onboarding is incomplete.
4. Open Federation Overview and confirm the expected current leader and connection state.
5. Give devices useful display names.
6. Pair additional trusted devices; optionally use Tailscale discovery to find the existing endpoint.
7. Configure/verify data sources.
8. Enable recording explicitly after source verification.
9. Use Monitor for telemetry/playback.
10. Run benchmarks when capacity/suitability evidence is useful.
11. Enable contributions through reviewed product surfaces.
12. Review pending provider candidates where separate approval is required.
13. Use Federation -> Recorders for remote recorder scan/source control.
14. Use Federation updates only after reviewing the exact target and member states.
15. Use Knowledge capture during field work.
16. Use Diagnostics instead of deleting state when something is blocked/degraded.

## Optional benchmarks

Benchmarks are evidence, not authority. A benchmark may be run/skipped/rerun/cancelled/invalidated. Missing benchmark evidence does not block the workbench after accepted inspection.

A leader request can ask for eligible member-local benchmark work, but members only execute locally registered definitions.

## Contributions

One device may contribute several independent capabilities. Enabling one does not grant unrelated authority or remove membership when another is disabled.

## Troubleshooting order

1. Confirm human sign-in/role.
2. Open Federation Overview for current leader, connection, update, and capability-request state.
3. For recorder issues, open Federation -> Recorders.
4. Open Status/Diagnostics.
5. Read [Troubleshooting](troubleshooting.md).
6. Preserve identity, Federation state, evidence, auth data, telemetry, and checkpoints unless a documented reset explicitly names them.

## Related guides

- [Federation operations](federation_operations.md)
- [Tailscale Federation discovery](tailscale_federation_discovery.md)
- [Human users, sign-in, and permissions](human-authentication.md)
- [Standalone recorder](standalone_recorder.md)
- [Troubleshooting](troubleshooting.md)
