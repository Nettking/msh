# Federation pending-contribution approval

Status: **active implementation contract**

Reviewed: **2026-08-11 Europe/Oslo**

## Problem

Capability-first members may persist an explicit local `enabled` contribution intent while the local authority adapter correctly leaves activation in `pending`. The authenticated Federation publication then advertises that candidate as `REGISTERING`.

The current product can therefore show pending compute/storage candidates on the Federation leader without giving the leader a supported product path to record an explicit approval decision. This is a product-composition gap, not a transport or membership failure.

The Federation creator's own first local storage bootstrap is a separate narrow exception: it may create the initial storage group, register the creator's local storage provider, and assign it as the first primary through the existing storage control plane. That bootstrap does not authorize joined members to self-approve storage.

## Required product behavior

The Federation/session creator gets a leader-only **Pending contributions** workflow over authenticated, public-safe capability announcements.

For one `REGISTERING` capability-first candidate the leader may:

- request/create the durable enrollment record;
- approve it explicitly;
- reject it by revoking the enrollment with a bounded operator reason;
- later suspend/revoke/reconcile the enrollment through the existing provider-enrollment authority.

Non-creator members remain read-only.

The supported relay already persists F8.1 provider enrollment and F8.2 provider health in sidecar databases beside the coordinator database. The Flask leader surface must reuse those exact durable authority stores. It must never create a second onboarding-local enrollment/health authority that could disagree with the relay.

Every mutation remains server-bound to the authenticated actor/session and uses the existing CSRF, command-id/idempotency, revision-fencing, and provider-enrollment audit boundaries. The browser must not supply a node identity, session identity, provider endpoint, executable path, storage path, credential, or arbitrary authority payload.

## Approval is not automatic activation

A capability announcement is metadata. Approval records the Federation creator's explicit acceptance of that announced candidate; it must not fabricate runtime readiness.

In particular:

- `REGISTERING` may be explicitly approved and remain not eligible for resource binding until the member later advertises the capability as `READY`;
- storage approval alone must not claim a `fcp-storage-candidate` is already a live `fcp-storage-v1` provider;
- storage primary/replica assignment remains owned by the storage control plane and requires an assignable provider/runtime;
- compute approval alone must not start or invent a worker; execution remains limited to registered handlers and existing compute-worker authority;
- AI approval grants no storage or compute authority;
- recorder/source control remains on its existing bounded recorder authority path.

This breaks the current approval deadlock safely: a leader may approve a pending candidate before runtime readiness, while downstream resource-binding code still requires the existing `APPROVED + READY` eligibility condition.

## Leader projection

The normal Federation product surface should expose pending contribution decisions without requiring a separately injected test-only operator surface.

The leader projection must distinguish at least:

- **Pending approval** — announced `REGISTERING`, no enrollment decision yet;
- **Approved, waiting for runtime** — enrollment `APPROVED`, announcement still `REGISTERING`;
- **Approved/eligible** — enrollment `APPROVED` and announcement/runtime evidence `READY` under the existing downstream authority;
- **Rejected/revoked**;
- **Suspended**;
- **Unavailable** when the announcing device disconnects or current authority evidence is absent.

The product should show public-safe device/capability labels and logical IDs only.

## Storage projection identity rule

The Federation Storage page must not compare a contribution `candidate_id` directly with a storage-control-plane `provider_id`. They are different identity domains.

For the built-in creator storage bootstrap, the local contribution card must be suppressed when the contribution's provider identity is already represented by authoritative storage-control-plane state. Existing installations using `fcp-local-data-storage` remain compatible.

A candidate-only row must appear only when no matching authoritative provider/assignment exists.

## Acceptance criteria

The implementation is complete when tests prove all of the following:

1. A `REGISTERING` capability can be requested and explicitly approved by the session creator.
2. The approved record is durable and revision fenced.
3. `APPROVED + REGISTERING` is **not** eligible for resource binding.
4. Reconciliation to a matching `READY` announcement can make the already-approved record eligible without creating a second approval authority.
5. A non-owner cannot approve/reject.
6. The production Flask composition exposes the leader operator surface from the existing trusted local Federation context rather than requiring arbitrary config injection.
7. The Flask leader surface uses the same provider enrollment/health authority files as the supported relay composition.
8. The Storage projection does not render the creator's already-authoritative local storage as an additional candidate-only card.
9. Existing provider/storage/compute authority tests remain fail-closed: approval alone never assigns storage, starts compute, executes handlers, or grants unrelated authority.

## Relation to the 2026-08-11 documentation reconciliation

PR #245 documented Federation operations, recorder operation, and the post-CF8 product baseline, but did not specify this leader-side pending-contribution decision workflow. This document closes that documentation gap before the runtime/product fix is introduced.
