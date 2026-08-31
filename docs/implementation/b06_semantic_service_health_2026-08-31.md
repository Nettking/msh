# B06 semantic service health — 2026-08-31

Baseline: `main` at `17e279c01ae6d48ca9c0f4a0b3eaddbb5922d0ef`.

This reconciliation was started only after checking the active PR set. The branch avoids B01 writer/resource-admission work in #383, B03 recorder-publication work in #384, registered-compute provider composition in #386, B05 activation recovery in #382, and the ICSE reviewer artifact in #381.

## Reconciled B06 state

The older robustness ledger says B06 is 4/8 automated-proven. Current production code contains two additional merged properties that the count has not incorporated:

- the Windows native-recorder supervisor owns ordinary child restart with a bounded backoff ladder and a deterministic rapid-crash fence;
- operator/Ctrl+C stop plus update/trial/rollback ownership retain explicit non-restart semantics, so an external service manager may own the supervisor but not compete for the recorder child.

B06 is therefore **6/8 automated-proven and still OPEN**.

The two remaining properties are:

1. service-specific liveness/readiness/degraded-dependency semantics for Flask, relay and managed recorder;
2. an FCP-visible crash-loop/resource-failure state for Docker's bounded-rate restart behavior.

The second property is deliberately not implemented on this branch while #383 is active. A durable restart-history ledger would itself become a new persistent writer and must not be introduced behind B01's resource-admission inventory.

## Semantic-health implementation

`catalog/flask_app/services/core_service_health.py` now defines one read-only model with distinct `liveness`, `readiness`, and `dependency` fields.

The implementation intentionally reuses existing evidence rather than creating another heartbeat or process owner:

- **relay liveness** is a bounded listener connection probe;
- **relay readiness** additionally requires the existing authoritative coordinator SQLite store to be readable through a bounded read-only connection;
- **managed-recorder liveness** is a fresh existing recorder heartbeat, not a cross-container PID guess;
- **managed-recorder readiness** accepts the recorder's healthy `recording` and intentional healthy-standby `stopped` states;
- recorder Federation connectivity is represented independently as dependency health, so local capture can remain ready while Federation connectivity is degraded;
- **Flask liveness/readiness** is represented by the fact that its serving request path can construct the snapshot; relay/recorder failures degrade its dependency field rather than falsely declaring the workbench/control surface dead.

All active probes are bounded to 0.5 seconds. The model has no Docker-socket access, process mutation, authority mutation, state-file writes, or health-history persistence.

## Consequence evidence

`catalog/flask_app/tests/test_core_service_health.py` pins the semantic distinctions rather than merely checking symbols:

- relay listener alive + authority store unavailable => alive but not ready;
- listener + a real read-only SQLite authority store => ready;
- stale recorder heartbeat => not live/not ready;
- fresh recorder heartbeat + Federation reconnecting => local recorder ready, dependency degraded;
- fresh recorder heartbeat + recorder error state => alive but not ready;
- Flask remains ready while reporting a degraded core dependency;
- the composed snapshot keeps Flask, relay and recorder states separate.

## Current status

Latest branch head: `e8f52bb60d58b6082d3e1e8b2b0126448129800e`.

Exact-head CI has been queued. No B06 closure is claimed yet: the semantic model still needs its final product/operator exposure and exact-head CI/adversarial review, and Docker crash-loop visibility remains a separate open B06 property.

No physical Federation machine, Update All operation, physical evidence, or acceptance flag changed.
