# Operator guide

Status: **current user guide**

Reviewed: **2026-08-07**

This guide describes normal operation after MSH starts. It does not assign the device one permanent server role.

## First use

A new device completes:

```text
Identity
  -> Federation
  -> Inspect
  -> finish setup
  -> open Federation
```

A current inspection is sufficient to finish setup. Benchmarks and contribution decisions are optional follow-up work.

The first device may create a local Federation. Additional devices join through an authenticated trusted binding or signed expiring pairing flow. A public device ID or network presence never grants membership.

## Main product areas

### Federation

Use Federation to understand the distributed product state.

- **Overview** — current device, connection, recommendations, and safe high-level status.
- **This device** — identity, inspection, local capability availability, and contribution state.
- **Devices** — authorized Federation members and connection state.
- **Services** — contributed service state without private endpoint disclosure.
- **Benchmarks** — optional benchmark evidence, expiry, invalidation, and rerun status.
- **Storage** — storage candidates and coordinator-assigned authority.
- **Jobs** — authorized job state and registered-handler execution.
- **Activity** — public-safe Federation events and diagnostics.
- **Settings** — supported Federation and compatibility information.

Federation pages are read-only product projections unless a specific reviewed action surface states otherwise.

### Monitor

Use Monitor to inspect current or replayed telemetry.

- **Overview** — local runtime and data readiness.
- **Live** — recent telemetry from supported data sources.
- **Playback** — machine/day replay from prepared timeline exports.
- **Assist** — possible causes, next actions, risks, alternatives, and operator confirmation.
- **Status/Diagnostics** — recorder, runtime, source, cache, and failure details.

### Knowledge

Use Knowledge to capture and structure operator experience.

- **Capture** — save one raw statement quickly.
- **Review Notes** — review and structure captured statements.
- **Strategies** — compare structured strategies by context, action, evidence, confidence, outcome, and trade-off.
- **Intervention Logic** — maintain technical detection rules when telemetry can identify a candidate situation.
- **SysML Export** — export supported structured records through the current compatibility path.

OSL production implementation has not started. Existing operator records and SysML exports are compatibility inputs, not proof of future OSL conformance, review, approval, or publication.

### System

Use System for local configuration and support.

- **Sources** — configure machines, MTConnect endpoints, vibration sensors, and supported connectors.
- **Documentation** — browse canonical repository documentation through `/docs`.
- **Diagnostics** — inspect local runtime and failure reasons.
- **Compatibility setup** — administer retained deployment settings where still required during migration.

Compatibility settings do not define the device’s product identity and do not grant Federation or contribution authority.

## Recommended operator workflow

1. Start MSH using `start.cmd` or the documented Compose command.
2. Complete Identity, Federation, and Inspect when onboarding is incomplete.
3. Open Federation Overview and confirm the expected device and connection state.
4. Configure supported machine sources under System -> Sources.
5. Test MTConnect and network reachability from the MSH runtime.
6. Enable recording explicitly only after the intended sources are verified.
7. Use Monitor -> Live or Playback to inspect telemetry.
8. Run optional benchmarks only when suitability or capacity evidence is needed.
9. Enable or disable independent contributions through their reviewed product surfaces.
10. Use Knowledge -> Capture during field work and structure notes later.
11. Use Diagnostics when any surface reports degraded, blocked, expired, or unavailable state.

## Source checks

### MTConnect test

The MTConnect test verifies that the Flask runtime can reach the configured endpoint and receive a response. A successful connection does not enable recording automatically.

A base adapter address may be normalized to the supported MTConnect endpoint, for example:

```text
10.0.0.20:5000
-> http://10.0.0.20:5000/current
```

### Network test

The network test opens a bounded TCP connection from the MSH runtime to the configured host and port. It proves application-level reachability, not the operating system’s VPN state and not Federation trust.

## Recording

Recording must remain explicit.

- Source discovery identifies candidates only.
- The operator selects the intended source.
- Recorder state and checkpoints remain durable across normal restarts.
- Disabling recorder contribution fences future Federation use without deleting device membership or unrelated contributions.
- Local telemetry and checkpoints remain local unless an authorized storage path processes them.

## Optional benchmarks

Benchmarks are evidence, not authority.

- A benchmark may be run, skipped, rerun, cancelled, expire, or be invalidated.
- Missing benchmark evidence does not block the workbench after current inspection.
- An enabled contribution may be suspended when required evidence expires or a dependency changes.
- Recovery requires the explicit reviewed rerun and reconciliation path.

## Contributions

One device may contribute several independent capabilities.

- Recorder enablement grants no AI, compute, or storage authority.
- AI enablement grants no job, storage, or membership authority.
- Compute exposes only explicitly registered local handlers.
- Storage remains candidate-only until the existing control plane assigns authority.
- Disabling one contribution must not remove membership or unrelated contributions.

## Knowledge capture

The current supported knowledge workflow is:

```text
raw statement
  -> later review
  -> structured strategy
  -> comparison or compatibility export
  -> intervention logic only when detectable
```

Capture should remain fast. Review and structuring may add context, observation, hypothesis, action, rationale, risk, evidence, confidence, and outcome.

## Troubleshooting order

1. Open Federation Overview for connection and recommendation state.
2. Open Status/Diagnostics for local runtime, recorder, source, and cache failures.
3. Open `/docs` and the [Troubleshooting guide](troubleshooting.md).
4. Preserve device identity, Federation state, telemetry, checkpoints, and evidence unless a documented reset explicitly names the deletion boundary.

Do not delete state merely to clear a UI warning. Use `start.cmd --fresh` only when intentionally creating a fresh device identity and Federation setup.
