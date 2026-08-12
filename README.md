# Federated Capability Platform (FCP)

**Federated Capability Platform (FCP)** is a Flask-first CNC telemetry workbench with capability-first Federation support. One installation represents one persistent device. A device may use the workbench and independently contribute recording, language-model, registered-compute, or storage-candidate capabilities without being assigned one permanent product role.

## Start FCP

### Windows

```cmd
start.cmd
```

### Linux or macOS

```bash
bash start.sh
```

The supported launchers preserve existing device/Federation/auth/data state, start the core services, bake the exact Git commit into the runtime images, verify the configured Ollama model, and start the bounded host-owned update agent used by Federation-wide software updates.

Open the URL printed by the launcher, normally `http://localhost:5000`.

## First production start

A fresh local authority has no default username or password. While the local human-user database is empty, normal browser requests redirect to the dedicated first-user page:

```text
/admin/users/bootstrap
```

Create the first active administrator there with a valid email and a confirmed password of at least 12 characters. Then sign in and continue capability-first onboarding.

The supported first-run flow is:

```text
Start FCP
  -> browser first-admin setup
  -> Human sign-in
  -> Identity
  -> Federation
  -> Inspect
  -> finish setup
```

A remotely paired Federation member with no local shadow users uses Federation human sign-in rather than local first-admin bootstrap.

## Pair and discover devices

The current Federation leader creates signed one-use `FCP1-...` pairing codes, valid for up to 10 minutes.

If both hosts already use the same Tailscale tailnet, the joining host can run:

```cmd
start-tailscale.cmd
```

or:

```bash
bash start-tailscale.sh
```

FCP then uses the already signed-in local Tailscale client to discover reachable FCP Federations. It does not require a Tailscale API/auth key. **Discovery is reachability only**; the joining device still needs the normal `FCP1-...` pairing code.

## Federation leadership

Federation creation provenance and operational leadership are separate.

- the creator remains immutable provenance;
- the current operational leader is tracked by a coordinator-authored monotonic leadership term;
- bounded offline-leader failover can promote a deterministic connected successor while the authoritative coordinator/relay remains available; and
- former leaders are fenced from current-leader controls after a valid transition.

Current-leader product controls include Federation-wide software updates, benchmark/contribution requests, member/invitation administration, and reviewed provider actions.

Human credential/password authority remains creator-backed and does not automatically move with operational leadership.

## Current product highlights

- **Browser first-user setup + RBAC** — create the first administrator in the browser; `viewer`, `operator`, and `admin` permissions are enforced server-side.
- **Federation SSO without password replication** — trusted members authenticate people through the creator-backed human credential authority using short-lived signed assertions.
- **Current-leader failover** — operational leader authority can transfer through bounded coordinator-authored terms without rewriting immutable creator provenance.
- **Device naming** — members can name themselves; the current leader can rename other members without changing stable node identity.
- **Tailscale Federation discovery** — find reachable Federations through an existing Tailscale login while keeping Federation admission on the signed pairing path.
- **Manual Federation-wide updates** — the current leader checks one approved `main` target and explicitly rolls it out; success requires runtime verification.
- **Leader capability requests** — the current leader can ask reachable members to run locally eligible benchmarks and request locally allowed contributions without remote-shell authority.
- **Standalone recorder** — `python start_recorder.py FCP1-...` can identify, pair, scan locally, record loss-aware telemetry, and publish committed data.
- **Federation-visible JSONL** — supported non-recorder JSONL can be verified/materialized across connected workbench members through logical storage.
- **Dark/light appearance** — the top-menu switch follows OS preference until an explicit browser-local choice is made.

## Documentation

Start here:

- [Documentation index](docs/index.md)
- [Quick start](docs/quick_start.md)
- [Getting started](docs/getting_started.md)
- [Human users, sign-in, and permissions](docs/human-authentication.md)
- [Federation operations](docs/federation_operations.md)
- [Tailscale Federation discovery](docs/tailscale_federation_discovery.md)
- [Operator guide](docs/operator_guide.md)
- [Standalone recorder](docs/standalone_recorder.md)
- [Server setup](docs/server_setup.md)
- [Troubleshooting](docs/troubleshooting.md)

## Authority boundaries

Discovery is not trust. A display name is not identity. Inspection/benchmark evidence is not authority. Contribution intent is not provider/storage activation. Human `admin` permission does not bypass Federation device authority. Operational leader transfer does not transfer human password custody. FCP peers send bounded authenticated intent rather than arbitrary commands.

## Current acceptance status

- Capability-first Federation baseline: merged.
- Browser first-user setup and Federation human SSO: merged.
- Fenced operational leader failover/current-leader product authority: merged.
- Verified manual Federation-wide runtime updates: merged.
- Tailscale onboarding discovery: merged.
- Complete physical CF7 acceptance: **not accepted**.
- Complete Federation v1 end-to-end acceptance: **not accepted**.
- Federation v1 release tag: not created.

The machine-readable acceptance source remains `catalog/federation/tests/cf7_acceptance/scenarios.json`.
