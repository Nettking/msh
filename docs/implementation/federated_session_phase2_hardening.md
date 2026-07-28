# Phase 2 hardening and maintenance plan

This document records follow-up work for the relay-first federation implementation without changing its Phase 2 contracts or introducing PR D-G behavior.

## Continuous integration

The `Phase 2 federation` workflow runs on Linux and Windows when federation, node, relay, dependency, or Compose files change.

Linux validates:

- compilation;
- Phase 0 and Phase 1 regressions;
- the complete Phase 2 unit and integration set;
- Ruff for the federation packages;
- default and `relay-dev` Compose configurations;
- diff hygiene.

Windows validates identity protection, durable node state, client behavior, relay behavior, protocol, and coordinator tests. This specifically exercises DPAPI and Windows file-lock behavior independently of a developer workstation.

## Development relay safety

The Compose relay is plaintext and is intended only for local development. Prefer:

```bash
docker compose --profile relay-dev up -d --build relay
```

The older `relay` profile remains temporarily as a compatibility alias. Both profiles publish only to host loopback. Do not change that binding, use host networking, or place this service behind a public listener. Cross-network deployments must use hostname-validated TLS directly or a maintained TLS reverse proxy.

## Dependency reproducibility

Security-sensitive Phase 2 dependencies are constrained in `constraints-phase2.txt`. CI installs with:

```bash
python -m pip install -r requirements.txt -c constraints-phase2.txt
```

Update the constraints deliberately and run the full Phase 2 matrix before merging dependency changes.

## Disclosure policy boundaries

Credential detection and backend-location detection are separate policies:

- `contains_secret_material` identifies credentials and secret-bearing fields;
- `contains_nonpublic_location` identifies backend paths, addresses, and storage locations;
- `contains_nonpublic_transport_data` applies both policies at relay trust boundaries;
- `redact_secret_material` removes only credentials;
- `redact_nonpublic_data` removes credentials and location details.

`redact_secrets` remains as a strict compatibility wrapper for the original Phase 2 callers. New code should select the narrowest policy that matches its trust boundary.

## Refactoring sequence

The large Phase 2 modules should be split only through behavior-preserving PRs. Each step must retain the existing tests and public imports.

1. Extract relay command parsing and dispatch from `catalog/relay/service.py`.
2. Extract relay lifecycle, live-connection registry, and bounded queue handling.
3. Split coordinator persistence by enrollment, sessions, capabilities, audit, and events while retaining one transactional store facade.
4. Split node client transport, replay, heartbeat, and CLI concerns.
5. Split node state migrations from event-application transactions.
6. Divide large test files by scenario family while preserving F2 scenario IDs.

Do not combine these structural changes with protocol changes, new Phase 3 features, or schema-semantic changes. The aim is reviewability, not redesign.
