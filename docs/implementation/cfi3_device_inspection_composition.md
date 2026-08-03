# CFI-3 capability-first device inspection composition

Status: implemented on `agent/cfi3-device-inspection-composition`; benchmark and contribution product actions remain blocked.

## Purpose

CFI-3 connects the already merged CF2-A inspection kernel and CF2-B concrete inspection adapters to the supported capability-first Flask onboarding flow.

The integration is intentionally limited to:

1. requiring the existing stable device identity and revalidated federation membership;
2. running one bounded, local, read-only inspection;
3. persisting the frozen CF1 `DeviceInspectionSnapshot` with a monotonic revision;
4. displaying coarse operating-system, architecture, resource, service, handler, data-source, warning, expiry, and recommended-check evidence in the existing CF5 Inspect step;
5. preserving benchmark execution and contribution activation as later reviewed changes.

## Production composition

`CapabilityInspectionService` builds the existing `DeviceInspector` and `BenchmarkRegistry` from explicit CF2 adapters.

The default supported composition uses:

- `MtconnectSourceAdapter` over `MtconnectDiscoveryService.last_scan()`;
- `OllamaBenchmarkAdapter` over the already configured server-side Ollama target when legacy settings explicitly enable it;
- optional explicitly supplied CF2 compute, storage, network, or other adapters through `CAPABILITY_ONBOARDING_INSPECTION_ADAPTERS`.

The MTConnect adapter only reads the saved bounded discovery snapshot. It does not scan a subnet, add recorder sources, or start recording.

The Ollama adapter performs only the existing bounded `GET /api/tags` inventory request during inspection. It does not invoke `/api/chat`, run inference, expose the configured URL or model, register an AI provider, or grant compute/storage authority.

Compute adapters inspect registered descriptor inventory only. Storage and network adapters must be explicitly supplied through an existing trusted seam. CFI-3 does not invent a new inventory or authority source.

## Flask boundary

The new blueprint is registered before the unchanged role-first startup/runtime gates and owns:

| Method | Route | Behavior |
| --- | --- | --- |
| `GET`, `HEAD` | `/onboarding` | Render the existing CF5 shell with CFI-2 identity/federation state plus CFI-3 inspection state |
| `POST` | `/onboarding/inspect` | Run and persist one bounded inspection after CSRF and trusted-context validation |

The existing CFI-2 routes remain authoritative for identity, discovery, join, local creation, and reconnect.

The following remain unchanged and return the existing safe `409` response:

- `/onboarding/benchmarks/run`;
- `/onboarding/benchmarks/skip`;
- `/onboarding/contributions`.

## Persistence boundary

CFI-3 stores one canonical `msh.onboarding.device-inspection.v1` object in the existing onboarding SQLite database.

The store enforces:

- one device binding;
- strictly monotonic revisions;
- transactional `BEGIN IMMEDIATE` writes;
- canonical bounded JSON;
- full synchronous writes and WAL mode;
- restricted database permissions;
- fail-closed malformed or oversized state;
- no silent rebinding to another device identity.

The snapshot contains evidence only. It stores no internal session binding, enrollment or invitation material, endpoint, credential, local path, provider activation, job ownership, storage assignment, lease, term, grant, or fencing token.

## UI states

The Inspect step supports:

- blocked before federation connection;
- ready before the first run;
- current after a successful run;
- expired after the configured TTL;
- degraded when persisted evidence cannot be read safely;
- safe empty service and data-source states;
- explicit recommended checks with a statement that no benchmark has run.

An expired snapshot remains visible but is not marked complete. A degraded snapshot is not trusted or rendered from raw persisted bytes.

## Authority and privacy controls

- actor, device, session, federation, target, endpoint, and credential context cannot be supplied by the browser;
- inspection requires the existing `authorized_context()` reconnect/membership proof;
- the request requires the existing per-browser CSRF token;
- benchmark probes are registered only as definitions and are never invoked by CFI-3;
- inspection does not change membership, session revision, provider enrollment, health, contribution intent, storage authority, or job state;
- adapter failures become bounded safe warnings;
- private endpoints, paths, credentials, and unsafe diagnostics are filtered by the existing CF1/CF2 safety boundary before persistence or rendering.

## Acceptance evidence

`catalog/flask_app/tests/test_capability_inspection_route.py` covers:

- app-factory registration before the legacy gate;
- identity and trusted-federation prerequisites;
- safe inspection rendering and persistence;
- no benchmark execution and no federation authority mutation;
- monotonic rerun revisions and restart reopen;
- expiry behavior;
- corrupt-state fail-closed behavior and raw-byte leakage prevention;
- CSRF and request-context override rejection;
- existing MTConnect snapshot composition without starting a scan;
- configured Ollama read-only inventory without inference;
- continued benchmark and contribution blocking.

The dedicated CFI-3 workflow runs focused and affected tests, compilation, Compose validation, Ruff, and diff hygiene on Ubuntu and Windows. The permanent CF7-A gate is extended with the CFI-3 route suite and manifest classification.

## Remaining work

The following remain separate integration and acceptance changes:

- benchmark run, skip, expiry, invalidation, cancellation, and rerun through Flask;
- contribution candidate, intent, enable, disable, suspend, and reconcile actions;
- compatibility-controlled migration writes and startup/runtime transition;
- physical Windows and Linux installation checks;
- real MTConnect, Ollama/accelerator, multi-host, desktop, and mobile acceptance;
- complete capability-first and Federation v1 end-to-end acceptance;
- eventual CF8 retirement of role-first setup.
