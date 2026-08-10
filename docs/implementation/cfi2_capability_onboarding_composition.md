# CFI-2 capability-first onboarding composition

Status: implemented on `agent/cfi2-onboarding-federation-composition`; full
capability-first end-to-end acceptance is not claimed.

## Purpose

CFI-2 connects the already merged CF1 contracts, CF3 federation onboarding
authority adapter, CF5 onboarding shell, and CFI-1 read-only Federation overview
through the supported Flask application.

The integration is intentionally limited to:

1. creating or loading one durable Ed25519 device identity;
2. rendering a read-only legacy setup migration preview;
3. discovering configured or previously trusted federation candidates;
4. requiring explicit candidate selection and verification for a first join;
5. creating a local federation only after discovery completed with no candidate;
6. revalidating a saved trusted membership on application startup and onboarding
   requests;
7. persisting only the frozen CF1 `FederationSessionBinding`;
8. providing the revalidated server-bound context to the existing read-only
   Federation projections.

CFI-2 does not implement benchmark execution, benchmark skip/rerun, contribution
activation, contribution disable/re-enable, storage assignment, job mutation, or
the role-first setup transition.

## Composition boundary

The production Flask routes are:

| Method | Route | Behavior |
| --- | --- | --- |
| `GET`, `HEAD` | `/onboarding` | Render the existing CF5 shell from server-side identity, migration, discovery, and binding state |
| `POST` | `/onboarding/identity` | Create or reopen the stable local identity |
| `POST` | `/onboarding/federation` | Refresh discovery, perform verified join, create locally, or reconnect |
| `POST` | `/onboarding/inspect` | Explicit `409`; inspection product integration is not in CFI-2 |
| `POST` | `/onboarding/benchmarks/run` | Explicit `409`; benchmark execution remains blocked |
| `POST` | `/onboarding/benchmarks/skip` | Explicit `409`; benchmark skip remains blocked |
| `POST` | `/onboarding/contributions` | Explicit `409`; contribution mutations remain blocked |

The onboarding blueprint is registered before the legacy setup and runtime gates
so its own routes remain reachable. The old `/startup` route, `deployment_mode`,
`DEPLOYMENT_MODES`, `ROLE_CAPABILITIES`, runtime decisions, recorder behavior,
and setup persistence remain unchanged as the compatibility fallback.

## Authority boundary

CFI-2 creates no second membership or session authority.

All enrollment, verified join, local creation, membership validation, and
reconnect behavior delegates to:

- `FederationOnboardingDiscoveryService`;
- `SessionOnboardingAuthority`;
- the existing `SessionCoordinator`;
- the existing enrollment and invitation stores.

The Flask request cannot supply or override actor, device, session, federation,
target, enrollment-token, or invitation-token context. The actor and device are
loaded from the server-side identity store. The session is resolved from the
selected in-memory discovery record or the persisted trusted binding.

A returning-device reconnect calls the existing membership revalidation path.
It does not create enrollment tokens, invitations, sessions, or memberships.

## Persistence boundary

The device identity continues to use `IdentityStore`.

CFI-2 adds one transactional SQLite onboarding database containing one canonical
`fcp.onboarding.federation-binding.v1` object. It stores:

- public federation ID;
- private internal session binding;
- device ID;
- trusted connection state;
- revision and verification timestamps.

It never stores:

- enrollment or invitation tokens;
- discovery endpoints or private addresses;
- credentials;
- provider-local configuration;
- handler bindings;
- leases, grants, or fencing material.

The SQLite store uses `BEGIN IMMEDIATE`, full synchronous writes, schema
versioning, bounded canonical JSON, identity/federation replacement fencing, and
restricted file permissions. Corrupt or unsupported state fails closed and is
not overwritten by page rendering.

## Docker composition

The Flask and relay containers mount the same existing `relay_state` volume.
Flask receives the canonical coordinator database path
`/var/lib/fcp-relay/control.sqlite3`, which is already used by the relay.

This permits the onboarding adapter to call the existing authority directly
without creating a parallel coordinator database. Native execution defaults to
`data/federation/relay/control.sqlite3`.

Device identity and onboarding binding state remain under the existing mounted
`data/` directory.

## Security controls

All onboarding mutations require:

- a per-browser CSRF token;
- a server-issued stable command ID for federation mutations;
- rejection of request-supplied authority context;
- bounded verification-code input;
- safe, code-based user messages rather than raw exception text;
- no-store, no-cache, nosniff, and same-origin referrer headers.

Discovery results expose only the existing CF3 safe result fields. Private join
material remains in the server-side discovery source and is not serialized into
the browser, Flask session, onboarding database, logs, or projections.

## Acceptance evidence

`catalog/flask_app/tests/test_capability_onboarding_route.py` covers:

- app-factory registration and legacy fallback preservation;
- stable identity creation and reopen;
- safe legacy migration preview;
- wrong-code rejection without enrollment or membership effects;
- verified join and idempotent replay;
- explicit selection with several candidates;
- no-candidate local federation creation;
- returning-device startup reconnect without new authority;
- membership removal and reconnect fencing;
- CSRF and request-context override rejection;
- explicit blocking of later benchmark/contribution mutations;
- CFI-2 context composition into the read-only `/federation` route;
- private-data leakage checks;
- corrupt onboarding-state fail-closed behavior.

The dedicated workflow runs these tests and affected CF1, CF3, CF5, CFI-1, CF6,
and legacy setup regressions on Ubuntu and Windows. The permanent CF7-A gate also
includes the focused CFI-2 route suite.

## Remaining work

The following remain separate integration and acceptance changes:

- supported device inspection through onboarding;
- benchmark run, skip, expiry, and rerun through Flask;
- contribution intent and authority actions;
- compatibility-controlled migration writes and startup/runtime transition;
- physical Windows and Linux installation checks;
- real desktop and mobile browser acceptance;
- complete capability-first and Federation v1 end-to-end acceptance;
- eventual CF8 retirement of role-first setup.
