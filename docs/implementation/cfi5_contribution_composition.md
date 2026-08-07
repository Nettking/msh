# CFI-5 capability contribution composition

CFI-5 connects the frozen CF4 contribution service to the supported Flask onboarding flow after CFI-4 benchmark review. It does not create a second provider, storage, compute, recorder, membership, or job authority.

## Scope

The integration owns the existing Contributions step and adds these server-bound operations:

- `GET /onboarding`
- `POST /onboarding/contributions`
- `POST /onboarding/contributions/<candidate_id>/suspend`
- `POST /onboarding/contributions/reconcile`

Candidates are regenerated from the device-bound saved inspection and accepted immutable benchmark evidence. The browser may submit only candidate choices plus the current CSRF token and server-issued command ID. Device, actor, Federation, session, adapter, endpoint, credential, timeout, assignment, and authority context remain server-owned.

## Authority composition

CFI-5 uses the existing CF4 source, policy, intent-store, and adapter contracts:

- recorder candidates delegate to the existing durable recorder control service;
- configured local Ollama candidates delegate to the existing AI runtime manager;
- compute candidates are available only when an explicit local inventory and all activate, fence, and active-state seams are configured;
- storage candidates remain pending until the existing control plane reports assignment.

Recorder enablement is policy-blocked when the compatible recorder role and configured MTConnect sources are not already available. CFI-5 does not persist a legacy setup transition to make that condition true.

## State and reconciliation

Only CF1 `ContributionIntent` values and their revision history are persisted through the existing `SQLiteContributionIntentStore`.

The long-running Flask application owns one automatic startup reconciliation. Before it can restore enabled intent, it loads the saved inspection and evaluates the saved benchmark review through the installed run-once validity path. It never runs inspection or a benchmark as part of reconciliation.

The startup outcomes are deliberately asymmetric:

- saved evidence whose benchmark identity, implementation version, and declared dependency inputs still match may restore an explicitly enabled contribution even when legacy temporal metadata is in the past;
- disabled or ask-later choices remain fenced;
- definition-stale, dependency-stale, mismatched, missing, or malformed capability evidence cannot reactivate enabled intent and instead leaves or sends enabled candidates through the existing suspension/fencing path where the candidate can still be resolved;
- elapsed wall-clock time by itself is not an authority transition and does not trigger an automatic benchmark, inspection, or suspension.

The run-once product composition does not rewrite old inspection/benchmark timestamps. It regenerates transient contribution candidates from accepted saved evidence and disables candidate-age fencing in this installed composition only. The frozen CF4 generator/service defaults retain strict expiry behavior for isolated contract tests.

The isolated Windows `start.cmd --resume` process performs no contribution authority change. It reconnects the saved Federation and reads persisted evidence; reconciliation is deferred to the long-running Flask application so there is only one automatic authority path.

The explicit `POST /onboarding/contributions/reconcile` operation also requires the same accepted benchmark-review prerequisite used for contribution choices. A benchmark whose implementation or dependency inputs changed therefore remains server-side blocked even if a client attempts to call the endpoint directly.

Reconciliation does not recreate membership, enroll providers, allocate storage, assign jobs, grant leases, or invoke compute handlers. Disable and suspend fence future use without removing the device from its Federation.

## Privacy and failure behavior

- Candidate and benchmark projections contain public-summary metadata only.
- Private endpoints remain inside existing adapters and are never stored in contribution intent or rendered.
- Raw adapter exceptions are logged by type only and replaced with safe user messages.
- Corrupt intent, inspection, benchmark, or authority composition fails closed.
- Missing or ambiguous authority adapters cannot activate a candidate.
- Time-only expiry is ignored only after device binding and structural benchmark validity are checked; it is never used to bypass dependency/version review.

## Acceptance evidence

`catalog/flask_app/tests/test_run_once_capability_evidence.py` verifies the installed policy boundary: old temporal expiry does not force evidence recollection, while benchmark dependency or implementation-version changes remain invalid and require explicit review. The CFI-5, CFI-4, CFI-3, and F8.5 Windows/Linux gates include that regression permanently.

The existing CFI-5 route and core contribution suites continue to verify explicit intent, storage candidate-only behavior, authority delegation, disable/suspend fencing, and the strict core expiry semantics retained outside the installed run-once composition.

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
