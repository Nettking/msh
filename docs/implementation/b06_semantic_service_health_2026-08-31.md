# B06 semantic service health — 2026-08-31

Baseline: `main` at `17e279c01ae6d48ca9c0f4a0b3eaddbb5922d0ef`.

This reconciliation was started only after checking the active PR set. The branch avoids B01 writer/resource-admission work in #383, B03 recorder-publication work in #384, B05 activation recovery in #382, and the ICSE reviewer artifact in #381. The former registered-compute PR #386 is now closed with no diff.

## Reconciled B06 state

The older robustness ledger says B06 is 4/8 automated-proven. Current production code contains two additional merged properties that the count has not incorporated:

- the Windows native-recorder supervisor owns ordinary child restart with a bounded backoff ladder and a deterministic rapid-crash fence;
- operator/Ctrl+C stop plus update/trial/rollback ownership retain explicit non-restart semantics, so an external service manager may own the supervisor but not compete for the recorder child.

B06 is therefore **6/8 automated-proven and still OPEN** before this branch's semantic-health property is accepted.

The two remaining properties at branch start were:

1. service-specific liveness/readiness/degraded-dependency semantics for Flask, relay and managed recorder;
2. an FCP-visible crash-loop/resource-failure state for Docker's bounded-rate restart behavior.

The second property is deliberately not implemented on this branch while #383 is active. A durable restart-history ledger would itself become a new persistent writer and must not be introduced behind B01's resource-admission inventory.

## Semantic-health implementation

`catalog/flask_app/services/core_service_health.py` defines one read-only model with distinct `liveness`, `readiness`, and `dependency` fields.

The implementation intentionally reuses existing evidence rather than creating another heartbeat or process owner:

- **relay liveness** is a bounded listener connection probe;
- **relay readiness** additionally requires the existing authoritative coordinator SQLite store to be readable through a bounded read-only connection;
- **managed-recorder liveness** is a fresh existing recorder heartbeat, not a cross-container PID guess;
- **managed-recorder readiness** accepts the recorder's healthy `recording` and intentional healthy-standby `stopped` states;
- recorder Federation connectivity is represented independently as dependency health, so local capture can remain ready while Federation connectivity is degraded;
- **Flask liveness/readiness** is represented by the fact that its serving request path can construct the snapshot; relay/recorder failures degrade its dependency field rather than falsely declaring the workbench/control surface dead.

The product route supplies `BoundedRelayListenerProbe`, which separately bounds Compose-name resolution and TCP connection. One daemon resolver worker owns name lookup and its queue admits at most one additional request; a wedged resolver therefore fails health closed without creating an unbounded thread backlog. Resolved endpoint attempts share one total 0.5-second deadline.

The model has no Docker-socket access, process mutation, authority mutation, state-file writes, or health-history persistence.

## Operator exposure

`GET /federation/health` exposes the public-safe semantic snapshot through an already-registered Federation blueprint. It returns `Cache-Control: no-store` and deliberately returns HTTP 200 even when the snapshot is `degraded`.

That response contract is intentional. This endpoint is operator evidence, not a Docker liveness/restart contract: a dead relay or disconnected recorder must not be transformed into "Flask failed" when the web/control surface is still serving. Consumers must inspect the explicit per-service `liveness`, `readiness`, and `dependency` fields.

The endpoint publishes only bounded state names, public-safe error codes and generic messages. It does not expose database paths, relay addresses, process identifiers, node/session identifiers, credentials, or authority contents.

## Consequence evidence

`catalog/flask_app/tests/test_core_service_health.py` pins the semantic distinctions rather than merely checking symbols:

- relay listener alive + authority store unavailable => alive but not ready;
- listener + a real read-only SQLite authority store => ready;
- stale recorder heartbeat => not live/not ready;
- fresh recorder heartbeat + Federation reconnecting => local recorder ready, dependency degraded;
- fresh recorder heartbeat + recorder error state => alive but not ready;
- Flask remains ready while reporting a degraded core dependency;
- the composed snapshot keeps Flask, relay and recorder states separate.

`catalog/flask_app/tests/test_core_service_health_route.py` proves a degraded snapshot remains HTTP 200/no-store and that the product route wires the bounded relay resolver probe.

`catalog/flask_app/tests/test_bounded_relay_probe.py` drives a deliberately blocked resolver and proves the caller returns within its deadline rather than waiting for name resolution or spawning a new resolver per request.

## Current status

Latest branch head before this documentation commit: `657c3da703736a7e94b536b730c0a617b808c066`.

The semantic-health property now has implementation, operator exposure and consequence evidence. It is **not yet counted as automated-proven** until exact-head CI and final adversarial preflight are green. If accepted, B06 advances from 6/8 to 7/8, leaving only FCP-visible Docker crash-loop/resource-failure state.

No physical Federation machine, Update All operation, physical evidence, or acceptance flag changed.
