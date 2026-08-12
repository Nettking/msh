# Operator guide

Status: **current user guide**

Reviewed: **2026-08-12**

This guide describes normal operation after FCP starts. It does not assign the device one permanent server role.

## First use

A new production installation first needs a human administrator and sign-in, then the device completes capability-first onboarding:

```text
create first administrator
  -> Human sign-in
  -> Identity
  -> Federation
  -> Inspect
  -> finish setup
  -> open Federation
```

Create the first administrator with the `fcp-user create-admin` command documented in [Human users, sign-in, and permissions](human-authentication.md). Human accounts authorize people using the web application; they are separate from device identity and Federation membership.

A current inspection is sufficient to finish setup. Benchmarks and contribution decisions are optional follow-up work.

The first device may create a local Federation. Additional devices join through an authenticated trusted binding or signed expiring pairing flow. A public device ID or network presence never grants membership.

Current browser-generated pairing codes begin with `FCP1-`, are signed, one-use, valid for up to 10 minutes, and can be generated again whenever another pairing attempt is needed.

## Main product areas

### Federation

Use Federation to understand and operate the distributed product state.

- **Overview** — current device, connection, recommendations, software-update state, leader capability-request state, and safe high-level status.
- **This device** — identity, inspection, local capability availability, and contribution state.
- **Devices** — authorized Federation members and connection state.
- **Services** — contributed service state without private endpoint disclosure.
- **Recorders** — connected standalone MTConnect recorders, bounded remote scan requests, and add/remove source selection.
- **Benchmarks** — optional benchmark evidence, invalidation, and rerun status.
- **Storage** — storage candidates and coordinator-assigned authority.
- **Jobs** — authorized job state and registered-handler execution.
- **Activity** — public-safe Federation events and diagnostics.
- **Settings** — supported Federation information and retained migration context where applicable.

Most Federation content is a public-safe projection. Mutations exist only on explicit reviewed action surfaces such as pairing, contribution choices, leader capability requests, recorder control, and coordinator-owned software updates.

#### Ask members to benchmark and contribute

The Federation leader/session creator can ask all currently reachable remote members to inspect local capability state, run eligible registered benchmarks, and request contribution for locally allowed candidates.

The request does not grant remote shell or provider authority. Each member authenticates the session creator, runs only locally registered bounded benchmark definitions, and re-evaluates its own contribution/provider policy. Capabilities that require separate approval may remain pending.

Offline members are not queued for later. Issue another request after they reconnect if you want them included.

This leader action requires the human browser user to have Federation-management permission as well as the issuing device having leader/session-creator authority. See [Federation operations](federation_operations.md).

#### Software updates

Only the Federation coordinator/session creator receives **Check for updates** and **Update all devices** controls.

Use them in this order:

1. run **Check for updates**;
2. review the exact target and every device result;
3. run **Update all devices** only when you intend to activate that target; and
4. wait for terminal per-device results before starting another rollout.

A successful device is shown as **Updated** with the green success indicator. This means the running runtime proved the exact requested commit; a source fast-forward alone is not success.

The operation requires the supported host-owned update agent on each normal FCP installation. See [Federation operations](federation_operations.md).

#### Standalone recorder control

Open **Federation -> Recorders** from any trusted Federation device. Choose a connected standalone recorder and request a scan or source change.

The scan executes on the recorder's network, not on the device displaying the browser. Source additions can select only opaque IDs returned by that recorder's latest bounded scan, so another Federation member cannot inject an arbitrary URL or credential.

Removing a source stops future capture for that source but does not delete previously recorded telemetry or historical checkpoints.

#### Federation-visible JSONL data

Supported non-recorder JSONL under `data/` can now be published through Federation logical storage and hash-verified/materialized on connected workbench members. Existing recursive JSONL consumers still operate on local paths; the Federation synchronization layer makes verified remote data visible inside that local compatibility boundary.

Recorder telemetry keeps its separate checkpoint-gated publication path. Generic Federation JSONL synchronization does not expose arbitrary host files and rejects invalid paths, non-JSONL content declarations, identity mismatches, contradictory versions, and failed hash/size verification.

Exact duplicate content is deduplicated so an existing recursive data scan does not count the same immutable file twice.

### Monitor

Use Monitor to inspect current or replayed telemetry and data visible to this workbench.

- **Overview** — local runtime and data readiness.
- **Live** — recent telemetry from supported data sources.
- **Playback** — machine/day replay from prepared timeline exports.
- **Assist** — possible causes, next actions, risks, alternatives, and operator confirmation.
- **Status/Diagnostics** — recorder, runtime, source, cache, and failure details.

Because supported Federation JSONL is materialized into the normal local data scan boundary, existing analyses/workflows may discover verified data originally produced by another Federation member without requiring a separate legacy-analysis code path.

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
- **Migration/compatibility state** — retained only where an upgraded installation still needs deterministic migration input.

Human administrators manage human web accounts at `/admin/users`. This is a separate administrative surface from device onboarding and Federation membership. See [Human users, sign-in, and permissions](human-authentication.md).

The installed product is capability-first. Retained migration state and old command aliases do not define product authority or a permanent device role.

## Recommended operator workflow

1. Start FCP using `start.cmd` or `bash start.sh`.
2. On a fresh production installation, create the first human administrator and sign in.
3. Complete Identity, Federation, and Inspect when onboarding is incomplete.
4. Open Federation Overview and confirm the expected device and connection state.
5. Pair additional FCP devices only through the signed trusted pairing flow.
6. Configure supported machine sources under System -> Sources, or use the standalone-recorder first-run scan.
7. Test MTConnect and network reachability from the FCP runtime or recorder host.
8. Enable recording explicitly only after the intended sources are verified.
9. Use Monitor -> Live or Playback to inspect telemetry.
10. Run optional benchmarks only when suitability or capacity evidence is needed, or let the Federation leader request eligible member-local benchmark/contribution work.
11. Enable or disable independent contributions through their reviewed product surfaces.
12. Review any contribution that still requires provider-enrollment approval rather than assuming a leader request activated it.
13. Use Federation -> Recorders when another trusted device must manage a standalone recorder's discovery/source selection.
14. Use Federation software updates only after reviewing the exact checked target and device states.
15. Use Knowledge -> Capture during field work and structure notes later.
16. Use Diagnostics when any surface reports degraded, blocked, unavailable, or failed state.

## Source checks

### MTConnect test

The MTConnect test verifies that the Flask runtime can reach the configured endpoint and receive a response. A successful connection does not grant Federation contribution authority.

A base adapter address may be normalized to the supported MTConnect endpoint, for example:

```text
10.0.0.20:5000
-> http://10.0.0.20:5000/current
```

### Network test

The network test opens a bounded TCP connection from the FCP runtime to the configured host and port. It proves application-level reachability, not the operating system's VPN state and not Federation trust.

### Standalone-recorder scan

The standalone recorder can run the existing bounded private-network MTConnect discovery automatically on first configuration. A trusted Federation member may later request the same recorder-local scan remotely.

Discovery remains restricted to validated RFC1918 private IPv4 networks of `/24` or smaller and retains address, timeout, redirect, and response-size limits.

## Recording

Recording configuration and contribution authority remain separate.

- Source discovery identifies candidates only.
- The operator selects the intended source, or the standalone recorder auto-selects discovered sources only during its first configuration.
- Recorder state and checkpoints remain durable across normal restarts.
- A deliberately removed standalone-recorder source set is not silently repopulated on later restart.
- Disabling recorder contribution fences future Federation use without deleting device membership or unrelated contributions.
- Local telemetry capture remains the recorder's primary commit boundary.
- Federation publication retries independently from a durable outbox and must not block MTConnect polling.

See [Standalone recorder](standalone_recorder.md) for the headless recorder workflow.

## Optional benchmarks

Benchmarks are evidence, not authority.

- A benchmark may be run, skipped, rerun, cancelled, or invalidated.
- Missing benchmark evidence does not block the workbench after current inspection.
- A changed benchmark identity, implementation version, or declared dependency input requires new evidence before that evidence can support contribution reconciliation.
- Elapsed wall-clock time alone is not an automatic product rerun trigger.
- A Federation leader may request eligible member-local benchmark work, but the member decides what registered benchmark definitions are runnable and never accepts arbitrary benchmark code from the request.

## Contributions

One device may contribute several independent capabilities.

- Recorder enablement grants no AI, compute, or storage authority.
- AI enablement grants no job, storage, or membership authority.
- Compute exposes only explicitly registered local handlers.
- Storage remains candidate-only until the existing control plane assigns authority.
- Disabling one contribution must not remove membership or unrelated contributions.
- A leader capability request can request eligible local contribution intent but cannot bypass member-local policy or any separate provider-approval requirement.

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

1. Confirm that the human account is signed in and has the role required for the intended operation.
2. Open Federation Overview for connection, update, capability-request, and recommendation state.
3. If a standalone recorder is involved, open Federation -> Recorders and inspect its latest scan/source-change report.
4. Open Status/Diagnostics for local runtime, recorder, source, and cache failures.
5. Open `/docs` and the [Troubleshooting guide](troubleshooting.md).
6. Preserve device identity, Federation state, telemetry, checkpoints, evidence, and human authentication state unless a documented reset explicitly names the deletion boundary.

Do not delete state merely to clear a UI warning. Use `start.cmd --fresh` only when intentionally creating a fresh device identity and Federation setup; that reset intentionally preserves human accounts and authentication secrets.
