# CF7-D streamlined onboarding and Federation flow

> **Status: historical and non-authoritative.** This document records a completed browser and projection correction. Its six-step onboarding sequence is superseded. Current mandatory onboarding is `Identity -> Federation -> Inspect -> finish setup`, with benchmarks and contribution decisions optional after workbench access.

## Historical guided onboarding

At this delivery stage, capability-first setup used:

```text
Identity -> Federation -> Inspect -> Benchmarks -> Contributions -> Finish
```

A successful migration or Finish POST returned to `/onboarding?step=finish`. The completed Finish panel confirmed that the device was ready and offered one explicit **Continue to MSH** action.

## Federation pages delivered

The overview originally linked to eight detail pages that were not registered by the Flask blueprint. This delivery exposed nine bounded GET/HEAD-only routes:

```text
/federation
/federation/device
/federation/devices
/federation/services
/federation/benchmarks
/federation/storage
/federation/jobs
/federation/activity
/federation/settings
```

They used public-safe CF6 projections, rejected writes, stripped browser query context, and shared one read-only detail template.

## Production composition delivered

The Federation surface was connected to durable capability services for:

- CFI-3 inspection snapshots;
- CFI-4 benchmark results;
- CFI-5 contribution candidates and persisted intent.

Unconfigured optional provider, storage, and job authorities became available empty surfaces rather than false failures. Configured projection failures still produced degraded state.

The delivery also corrected empty and connection semantics:

- a trusted current-device binding could not appear connected in one card and offline in the device list;
- zero benchmark results were shown as **Not run**;
- a connected device without inspection received an **Open inspection** recommendation.

## Historical validation

Implementation commit `988c66d4d48078fdabb0a4c491dbb68810a5ac21` passed the named CFI-1, CF6 projection, CFI-6 onboarding, and CF7-B gates on Ubuntu and Windows at that stage.

## Durable boundaries

The delivery did not change Federation authority, membership, discovery, benchmark execution, contribution activation, storage assignment, compute registration, persistence schemas, or runtime intent. Benchmark evidence remained non-authoritative, and storage remained candidate-only until control-plane assignment.

Use the [current capability-first plan](../../active/capability_first_federation_plan.md) for supported product behavior.