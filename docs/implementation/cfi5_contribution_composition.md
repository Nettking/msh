# CFI-5 capability contribution composition

CFI-5 connects the frozen CF4 contribution service to the supported Flask onboarding flow after CFI-4 benchmark review. It does not create a second provider, storage, compute, recorder, membership, or job authority.

## Scope

The integration owns the existing Contributions step and adds these server-bound operations:

- `GET /onboarding`
- `POST /onboarding/contributions`
- `POST /onboarding/contributions/<candidate_id>/suspend`
- `POST /onboarding/contributions/reconcile`

Candidates are regenerated from the current device-bound inspection and existing immutable benchmark evidence. The browser may submit only candidate choices plus the current CSRF token and server-issued command ID. Device, actor, Federation, session, adapter, endpoint, credential, timeout, assignment, and authority context remain server-owned.

## Authority composition

CFI-5 uses the existing CF4 source, policy, intent-store, and adapter contracts:

- recorder candidates delegate to the existing durable recorder control service;
- configured local Ollama candidates delegate to the existing AI runtime manager;
- compute candidates are available only when an explicit local inventory and all activate, fence, and active-state seams are configured;
- storage candidates remain pending until the existing control plane reports assignment.

Recorder enablement is policy-blocked when the compatible recorder role and configured MTConnect sources are not already available. CFI-5 does not persist a legacy setup transition to make that condition true.

## State and reconciliation

Only CF1 `ContributionIntent` values and their revision history are persisted through the existing `SQLiteContributionIntentStore`. Startup reconciliation runs once when persisted intent exists. It may restore an explicitly enabled contribution, keep disabled or ask-later choices fenced, or suspend enabled intent whose inspection/candidate evidence has expired.

Reconciliation does not recreate membership, enroll providers, allocate storage, assign jobs, grant leases, or invoke compute handlers. Disable and suspend fence future use without removing the device from its Federation.

## Privacy and failure behavior

- Candidate and benchmark projections contain public-summary metadata only.
- Private endpoints remain inside existing adapters and are never stored in contribution intent or rendered.
- Raw adapter exceptions are logged by type only and replaced with safe user messages.
- Corrupt intent, inspection, benchmark, or authority composition fails closed.
- Missing or ambiguous authority adapters cannot activate a candidate.

## Deliberate boundaries

CFI-5 does not implement:

- persisted legacy-mode migration;
- provider enrollment or provider-health mutation;
- storage assignment;
- compute handler discovery or code transfer;
- job ownership, dispatch, artifacts, grants, leases, terms, or fencing authority;
- writable Federation overview controls;
- physical multi-host, MTConnect, Ollama, desktop, or mobile acceptance.

Those remain separate product or CF7 acceptance work.
