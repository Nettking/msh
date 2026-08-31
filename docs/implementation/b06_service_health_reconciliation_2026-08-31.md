# B06 required-process and service-health reconciliation — 2026-08-31

Base: `main @ 17e279c01ae6d48ca9c0f4a0b3eaddbb5922d0ef`.

This note reconciles B06 against current merged production code before another implementation is started. It changes no acceptance flag and makes no physical-evidence claim.

## Current scorecard

The authoritative robustness document still says **4/8**, but current `main` contains two further automated-proven properties from the merged Windows native-recorder supervisor work. The current implementation score is therefore **6/8 automated-proven; B06 remains OPEN**.

| B06 property | Current assessment | Evidence / consequence |
| --- | --- | --- |
| Service-specific liveness/readiness/degraded-dependency semantics for Flask, relay and managed recorder | **OPEN** | Compose gives `restart: unless-stopped` to the three core services, but only optional Ollama services have Docker healthchecks. More importantly, Docker health alone is not the requirement: the product has no one bounded operator model that distinguishes process liveness, service readiness, and dependency degradation for Flask, relay and recorder. |
| Fatal internal relay failure becomes process/container failure | **PROVEN** | Merged relay supervision preserves the fatal cause, wakes both supported owners, exits nonzero, and lets Compose restart the process instead of leaving a PID-alive/connection-dead relay. |
| Docker bounded-rate restart becomes visible as an FCP crash-loop/resource-failure state | **OPEN** | Core services use `restart: unless-stopped`, but no FCP-visible state currently distinguishes a repeatedly restarting core service from an ordinary transient restart. Implementing this with a new durable restart ledger is deliberately deferred while B01 is reconciling every persistent writer. |
| Windows native-recorder supervisor owns ordinary restart with bounded backoff and deterministic-crash fence | **PROVEN** | Current `fcp_recorder_supervisor.ps1` owns one child per checkout, restarts only a recorder that has previously got past startup, uses a finite backoff ladder, decays the rapid-failure streak after a healthy runtime, and exits with distinct crash-fence code 6 instead of looping indefinitely. |
| Ctrl+C/operator stop and update/trial exits retain non-restart semantics | **PROVEN** | The same supervisor checks intentional stop before restart accounting, propagates zero/STATUS_CONTROL_C_EXIT, keeps trial/rollback children transition-owned, and routes update/trial exits through the existing finalize/launch-plan state machine rather than the ordinary-restart path. |
| Recorder status/final-status I/O failure is contained | **PROVEN** | Merged recorder runtime work contains heartbeat `OSError`, preserves stale-file truth, carries the bounded publication error into the next successful heartbeat, and avoids amplifying shutdown failure. |
| Recorder publication and analysis scheduler driver failure is observable/recoverable | **PROVEN** | Merged publication supervision and analysis-driver health record retryable durable-store faults, bounded retry/restart state, and unexpected driver termination instead of silently stranding durable work. |
| Stale responder cleanup verifies process identity beyond bare PID | **PROVEN** | Merged tailnet responder replacement uses PID plus process-creation identity and stable Windows/Linux process handles, failing closed where stable identity cannot be proved. |

## Active-PR collision boundary

Before implementation, the current open PRs were re-read and treated as ownership constraints:

- `#383` B01 owns host-resource admission and may touch recorder outbox, JSONL, uploads, analysis writers/workspaces, logical storage, observer export, telemetry cache, Docker/model paths and other persistent writers.
- `#384` B03 owns recorder publication/archive/outbox frontier semantics.
- `#386` owns registered-compute provider enrollment/health/selection composition.
- `#382` owns host update activation recovery in POSIX/Windows update runners.
- `#381` owns the ICSE reviewer artifact under `demo/icse` and its workflow.

The next B06 implementation must avoid those surfaces unless a dependency is proved and explicitly reconciled first.

## Selected next property

The next implementation target is **read-only semantic service health**, not crash-loop persistence.

A safe v1 health model must keep these concepts separate:

- **liveness** — is the service process/runtime currently reachable/alive?
- **readiness** — can the service perform its required product role now?
- **degraded dependency** — is the service alive/usable for some work while a dependent capability is unavailable?

The implementation should reuse existing evidence rather than create a new writer:

- Flask: the supported runtime/route itself plus existing startup/runtime-gate evidence;
- relay: a bounded read-only probe through the existing relay/product client seam, not Docker socket authority;
- managed recorder: the existing recorder heartbeat/runtime status and freshness semantics.

The first delivery must be fail-closed and bounded: probe failure is `unavailable`/`degraded`, never healthy by default; no probe may create authority, mutate Federation state, or wait without a finite timeout.

## Explicitly deferred in this lane

The remaining crash-loop property may ultimately need restart history. No new durable restart ledger will be introduced while #383/B01 is active, because that would create a new persistent writer before its admission/retention contract is reconciled.

No physical Federation machine, Update All operation, rollout state or acceptance flag is touched by this reconciliation.