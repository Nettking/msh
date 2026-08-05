# CFI-4 capability-first benchmark composition

Status: implemented on `agent/cfi4-benchmark-composition`; contribution product actions remain blocked.

## Purpose

CFI-4 connects the already merged CF2-A benchmark registry, runner, durable result store, validity evaluator, and CF2-B concrete adapters to the supported capability-first Flask onboarding flow after CFI-3.

The integration is intentionally limited to:

1. deriving a benchmark plan from the current, device-bound CFI-3 inspection snapshot;
2. running one explicitly selected, registered, bounded local benchmark target;
3. persisting immutable CF1 `BenchmarkResult` evidence and restart-safe run reservations;
4. supporting optional skip, explicit rerun, cooperative cancellation, expiry, and dependency invalidation;
5. rendering safe evidence in the existing CF5 Benchmarks step;
6. preserving all contribution actions and authority binding as a later reviewed change.

## Production composition

`CapabilityBenchmarkService` reuses the exact registry and adapter instances already composed by `CapabilityInspectionService`. It does not rediscover services, register a second provider inventory, infer targets from browser input, or create a second authority source.

For every benchmark recommended by the current inspection, CFI-4:

- matches only target identifiers already present in the safe inspection snapshot;
- obtains invalidation inputs from the owning trusted adapter on the server;
- supplies prerequisites from the inspection-backed recommendation;
- rejects targets that no longer exist or cannot produce the declared dependency inputs;
- generates the run ID on the server;
- applies the benchmark definition's duration and parallelism limits through the existing runner.

The browser may submit only the current `benchmark_id` and `target_service_id`. It cannot submit a run ID, device ID, actor, federation/session context, prerequisites, dependency fingerprints, timeout, endpoint, credentials, model configuration, path, grant, lease, term, or fencing token.

## Flask boundary

The CFI-4 blueprint is registered before the CFI-3, CFI-2, and role-first fallback gates and owns:

| Method | Route | Behavior |
| --- | --- | --- |
| `GET`, `HEAD` | `/onboarding` | Render CFI-2 identity/Federation state, CFI-3 inspection state, and CFI-4 benchmark evidence |
| `POST` | `/onboarding/benchmarks/run` | Run or rerun one current server-planned target |
| `POST` | `/onboarding/benchmarks/skip` | Persist local skip decisions for the current inspection revision without creating benchmark results |
| `POST` | `/onboarding/benchmarks/cancel` | Request cooperative cancellation of one active local run |

The existing safe `409` prerequisite boundary is retained when identity exists but trusted Federation context or current inspection evidence is missing.

`/onboarding/contributions` remains owned by the unchanged blocked fallback and returns `409`.

## Persistence boundary

The existing `SQLiteBenchmarkResultStore` writes immutable `msh.onboarding.benchmark-result.v1` objects and restart-safe run reservations to the onboarding benchmark database.

CFI-4 adds one local `benchmark_skip_decision` table keyed by:

- device identity;
- inspection revision;
- benchmark ID;
- target service ID.

Skip decisions are onboarding progress only. They are not synthetic benchmark results, contribution intent, policy decisions, provider activation, membership state, storage assignment, job ownership, grants, leases, terms, or fencing.

A successful explicit run clears the matching skip decision. A new inspection revision naturally invalidates previous skip progress because the key includes the inspection revision.

## Validity and rerun behavior

The existing CF2 validity evaluator marks evidence non-current when:

- its TTL expires;
- the benchmark implementation version changes;
- declared dependency inputs change;
- the stored benchmark ID no longer matches the definition.

Expired and stale results remain visible as historical evidence but do not complete the Benchmarks step. The UI requests a rerun. Reruns create new immutable results with new server-generated run IDs; earlier results are not overwritten.

Cancelled and failed results are safe reviewed outcomes but do not recommend activation. Passing evidence also grants no authority and cannot enable a contribution automatically.

## Concrete adapter boundaries

- **Ollama:** inspection continues to use bounded `GET /api/tags`; only an explicit benchmark POST can invoke the adapter's bounded `/api/chat` check. The URL and model remain private.
- **MTConnect:** evaluates the existing bounded discovery snapshot and never starts a scan or recorder.
- **Compute:** evaluates registered descriptor evidence only and never invokes or dispatches a handler.
- **Storage:** runs only an explicitly supplied candidate write/read/cleanup seam; storage remains candidate-only and receives no authority.
- **Network:** measures only an explicitly supplied already-authenticated path; private route descriptors are excluded.

## Authority and privacy controls

- every action requires the existing browser CSRF token;
- actor, device, Federation, membership, target inventory, dependency inputs, and execution bounds are server-owned;
- current trusted membership is revalidated through `authorized_context()` before run, skip, or cancel;
- the current inspection must belong to the same device and remain unexpired;
- raw adapter exceptions, endpoints, credentials, paths, and unsafe diagnostics are not rendered;
- benchmark execution does not change membership, session revision, provider enrollment, health, contribution intent, storage authority, job state, grants, leases, terms, or fencing.

## Acceptance evidence

`catalog/flask_app/tests/test_capability_benchmark_route.py` covers:

- app-factory registration and route ownership before earlier gates;
- trusted Federation and current inspection prerequisites;
- durable safe result persistence and rendering;
- no Federation authority mutation;
- rerun with new immutable run IDs;
- skip without probe execution or contribution activation;
- skip clearing after explicit run;
- expiry and dependency invalidation;
- cooperative cancellation through Flask;
- CSRF and browser attempts to override server-owned execution inputs;
- corrupt-state fail-closed behavior and raw-byte leakage prevention;
- explicit Ollama inference only after the benchmark POST;
- continued contribution blocking.

The dedicated CFI-4 workflow runs focused and affected tests, compilation, Compose validation, Ruff, and diff hygiene on Ubuntu and Windows. The permanent CF7-A workflow and scenario manifest are extended through CFI-4.

## Remaining work

The following remain separate integration and acceptance changes:

- contribution candidate rendering and local intent actions through Flask;
- recorder, AI, compute, and storage authority binding through existing control planes;
- compatibility-controlled migration writes and startup/runtime transition;
- physical Windows and Linux installation checks;
- real MTConnect, Ollama/accelerator, storage, multi-host, desktop, and mobile acceptance;
- complete capability-first and Federation v1 end-to-end acceptance;
- eventual CF8 retirement of role-first setup.
