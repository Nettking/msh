# CFI-6 compatibility-controlled startup transition

Status: implemented as a stacked integration after CFI-5. Full CF7 product acceptance remains open.

## Scope

CFI-6 makes capability-first onboarding the default browser entry point while retaining the role-first setup as an explicit compatibility fallback. It persists one local, versioned startup document after either:

- deterministic migration of a supported completed `server_settings.json`; or
- completion of the CFI-2 through CFI-5 onboarding flow.

The transition is additive. Existing setup fields, protocol v1 session boundaries, authority stores, recorder sources, Ollama configuration, provider enrollment, compute handlers, storage assignment, jobs, leases, grants, and membership state are not removed or redefined.

## Persisted state

The local document uses schema `fcp.onboarding.v1` and records only:

- stable device ID;
- public federation ID and existing internal session ID;
- connected federation state;
- inspection revision;
- generic contribution intents for workbench, runtime, recorder, language model, compute, and storage;
- completion and migration-source metadata;
- a UTC update timestamp.

It does not store private endpoints, credentials, invitation material, recorder source URLs, Ollama URLs, model files, handler paths, provider-local configuration, storage assignments, job authority, or executable payloads.

The device and federation binding are immutable once persisted. Corrupt, oversized, unsupported, or contradictory state fails closed.

## Legacy migration

Migration uses the frozen CF1 preview mapping for all supported legacy modes:

- `full-server`;
- `web-workbench`;
- `web-ui-only`;
- `recorder-only`;
- `language-model-provider`.

Migration preserves existing behavior but never silently enables compute or storage. It creates a stable identity and a local federation only when no saved binding or discovered federation requires operator selection. A broken saved binding is never replaced. A discovered federation that requires selection or verification must be handled through the normal onboarding authority path first.

The legacy file remains the compatibility carrier for private recorder and AI configuration until CF8. Capability-first state becomes the source for browser startup, navigation visibility, and runtime intent.

## Browser behavior

- Fresh installations open `/onboarding` instead of the role-first wizard.
- Supported configured legacy installations open the migration confirmation at `/onboarding?step=finish`.
- Returning completed installations open the read-only `/federation` overview once per browser session.
- `/startup?legacy=1` explicitly enables the bounded legacy fallback for that browser session.
- Actual migration and finish mutations require the current CSRF token and server-issued command ID.
- Browser-provided device, federation, session, actor, target, endpoint, or authority context is rejected.

## Runtime and authority boundaries

Capability-first completion writes compatible legacy settings only so the existing runtime and Compose surfaces continue to function until CF8. This does not grant new authority:

- recorder activation still uses the existing recorder control service and requires an existing selected source;
- language-model activation still uses the existing AI runtime;
- compute remains limited to registered handlers;
- storage remains candidate-only until existing control-plane assignment;
- benchmark evidence remains evidence only;
- disabling or suspending a contribution does not remove Federation membership;
- migration does not change protocol messages, terms, leases, grants, jobs, or membership revisions.

## Validation boundary

The dedicated Ubuntu/Windows gate covers migration mapping, state durability and corruption handling, immutable bindings, private-data exclusion, local-federation creation, ambiguous discovery fencing, saved-binding repair behavior, capability-derived runtime flags, route ordering, default startup redirects, returning-device Federation handoff, explicit fallback, CSRF, command IDs, and context-override rejection.

CFI-6 does not claim physical multi-host, MTConnect, Ollama/GPU, mobile-browser, or full end-to-end CF7 acceptance. Role-first deletion remains blocked until CF7 passes and CF8 is reviewed separately.
