# Federated Capability Platform (FCP)

**Federated Capability Platform (FCP)** is a Flask-first CNC telemetry workbench with capability-first Federation support. One installation represents one persistent device. A device may use the workbench and independently contribute recording, language-model, registered-compute, or storage-candidate capabilities without being assigned one permanent product role.

## Start Federated Capability Platform

### Windows

```cmd
start.cmd
```

### Linux or macOS

```bash
bash start.sh
```

The supported launchers preserve existing device/Federation state, start the core services, bake the exact Git commit into the runtime images, verify the configured Ollama model, and start the bounded host-owned update agent used by Federation-wide software updates.

On the first production start, FCP creates persistent human-auth session and password-hashing secrets under `data/auth/`. A fresh local authority has no default human administrator. Open the FCP web interface; while the local human-user database is empty, normal browser requests redirect to `/admin/users/bootstrap`. Create exactly the first active administrator there with a valid email address and a confirmed password of at least 12 characters, then sign in at `/login` and continue device onboarding.

A remotely paired Federation member with an empty local shadow-user database does not reopen anonymous first-admin setup. It continues through Federation human sign-in.

Open `http://localhost:5000/onboarding` on a device that has not completed setup. Returning devices normally open the workbench or Federation surface using their persisted identity and trusted Federation binding.

## Required first-run flow

```text
Create first administrator in browser
  -> Human sign-in
  -> Identity
  -> Federation
  -> Inspect
  -> finish setup
  -> open Federation
```

A current device inspection is sufficient to finish setup. Benchmarks and contribution decisions are optional follow-up work. They never grant membership, provider authority, storage assignment, job ownership, or artifact access by themselves.

## Optional Tailscale Federation discovery

If a joining FCP host is already signed in to the same Tailscale tailnet as an existing Federation, use:

```cmd
start-tailscale.cmd
```

or:

```bash
bash start-tailscale.sh
```

FCP uses only the existing local Tailscale login to find reachable FCP Federation advertisements. It does not require or store a Tailscale API key or auth key. Discovery proves reachability only; the joining device must still redeem the normal signed one-use `FCP1-...` pairing code.

See [Tailscale Federation discovery](docs/tailscale_federation_discovery.md).

## Current product highlights

- **Human authentication and central RBAC** — browser users sign in with separate human accounts. `viewer`, `operator`, and `admin` permissions are enforced server-side without reusing Federation or device credentials. A fresh local authority creates its first administrator through the one-time browser bootstrap.
- **Federation human SSO** — passwords/password hashes remain on the creator-backed human credential authority; trusted members use short-lived signed assertions and local shadow accounts.
- **Capability-first runtime** — the retired role-first product runtime is no longer a normal authority path. Retained legacy state is migration input only.
- **Current operational leader with fenced failover** — immutable creator provenance is separate from a coordinator-authored monotonic leadership term. After a bounded offline timeout, a valid connected successor may receive reviewed leader-only product controls while the former leader is fenced. This does not provide replicated coordinator/quorum failover if the authoritative coordinator service itself is lost.
- **Federation device names** — members can name themselves from **Federation -> This device**; the current leader can rename other members from **Devices** without changing stable node identity.
- **Manual Federation-wide updates** — the current operational leader can check an exact approved `main` commit and explicitly update eligible devices. Successful runtime verification is shown as **Updated**.
- **Leader capability requests** — the current leader can ask reachable remote members to run locally eligible benchmarks and request locally allowed contributions without receiving remote-shell/provider authority.
- **Headless standalone recorder** — `python start_recorder.py FCP1-...` can identify, pair, scan its local private network, start loss-aware recording, and publish checkpoint-committed data to Federation logical storage.
- **Verified Federation telemetry visibility** — connected members can discover committed recorder batches through the authoritative manifest and materialize hash-verified, schema-checked telemetry into their local workbench without exposing provider paths.
- **Federation-wide recorder control** — any trusted Federation device can request a bounded scan on a connected standalone recorder and add/remove sources selected from that recorder's latest scan.
- **Federation-visible JSONL** — supported non-recorder `data/**/*.jsonl`, including browser-uploaded JSONL by default, can be verified and materialized through Federation logical storage; deployments can set `FCP_FEDERATED_JSONL_PUBLISH_UPLOADS=0` to withhold uploads.
- **Short-lived pairing** — browser-generated `FCP1-...` codes are signed, one-use, valid for up to 10 minutes, and can be generated again whenever another pairing attempt is needed.
- **Tailscale reachability discovery** — an already logged-in Tailscale client can locate FCP Federations without becoming an FCP trust credential.
- **Light/dark appearance** — the top-menu appearance switch follows the OS when no explicit browser choice exists and persists an explicit choice per browser.

See:

- [Documentation index](docs/index.md)
- [Quick start](docs/quick_start.md)
- [Human authentication](docs/human-authentication.md)
- [Federation operations](docs/federation_operations.md)
- [Tailscale Federation discovery](docs/tailscale_federation_discovery.md)
- [Standalone recorder](docs/standalone_recorder.md)
- [Server setup](docs/server_setup.md)
- [Operator guide](docs/operator_guide.md)
- [Troubleshooting](docs/troubleshooting.md)

## Current acceptance status

- Capability-first Federation baseline: merged.
- Browser first-user bootstrap and Federation human SSO: merged.
- Fenced operational leader failover/current-leader product authority: merged.
- Role-first runtime compatibility retirement (CF8): merged for the installed product; migration seams remain where explicitly documented.
- Verified manual Federation-wide updates: merged.
- Tailscale Federation discovery: merged.
- Complete physical CF7 acceptance: **not accepted**.
- Complete Federation v1 end-to-end acceptance: **not accepted**.
- Federation v1 release tag: not created.

The machine-readable acceptance source is `catalog/federation/tests/cf7_acceptance/scenarios.json`. Automated gates demonstrate executable contracts and regression coverage, but they do not replace the required physical evidence.

## Current development tracks

- [Federation implementation](docs/implementation/federation/) — active plans, acceptance, reference material, and historical delivery evidence.
- [Capability-first Federation plan](docs/implementation/federation/active/capability_first_federation_plan.md) — authoritative Federation product behavior and acceptance sequence.
- [OSL integration](docs/implementation/osl_integration/) — active planning package; production implementation has not started.
- [Current task handoff](docs/implementation/current_task_handoff.md) — development blockers and exact next deliveries where still current.

## Repository map

- `catalog/flask_app/` — supported Flask application and operator surface.
- `catalog/federation/` — Federation identity, membership, authority, storage, transport, projections, onboarding, update and recorder-control contracts, and recovery components.
- `catalog/node/` — persistent device identity and outbound Federation client behavior.
- `catalog/relay/` — authenticated relay service.
- `catalog/capabilities/` — inspection, benchmarking, contribution, provider, job, and handler contracts.
- `catalog/storage/` — logical storage contracts and providers.
- `catalog/ai/` — local and connected language-model support.
- `catalog/standalone-recorder_v2/` — supported loss-aware MTConnect recorder.
- `start_recorder.py` — standalone recorder launcher, Federation bootstrap, startup discovery, and managed recorder-control composition.
- `data/` — local telemetry, capability/source configuration, checkpoints, and device/Federation state.
- `results/` — generated workflow and analysis artifacts.
- `docs/` — product, operator, architecture, implementation, acceptance, release, and historical documentation.

## Authority boundaries

Discovery is not trust. Tailscale reachability is not Federation membership. A display name is not cryptographic identity. Benchmark evidence is not authority. Contribution intent is not activation. Storage candidates cannot self-assign primary or replica authority. Compute is limited to explicitly registered local handlers. AI may explain or propose, but it does not approve, publish, assign authority, or execute unregistered code.

Human web permissions and Federation device authority are independent. Operational leader transfer does not automatically transfer human password custody. Federation software updates and recorder controls follow the same bounded-intent principle: peers send reviewed authenticated intent, while the target host independently validates and executes only locally fixed operations. FCP does not expose a general Federation shell.
