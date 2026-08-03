# CF7-D streamlined onboarding and Federation flow

Status: focused UX and read-only projection corrections discovered during the
first physical browser tests.

## Guided onboarding

Capability-first setup remains one continuous flow:

```text
Identity -> Federation -> Inspect -> Benchmarks -> Contributions -> Finish
```

A successful migration or Finish POST returns to
`/onboarding?step=finish`. The completed Finish panel confirms that the device
is ready and offers one explicit **Continue to MSH** action. The operator is not
redirected automatically to the Federation overview.

Capability onboarding uses the existing focused setup shell, so normal product
navigation is hidden until the operator chooses to continue.

## Federation pages

The overview originally linked to eight detail pages that were not registered
by the Flask blueprint. All nine projection pages are now exposed through
bounded GET/HEAD-only routes:

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

They use the existing public-safe CF6 projections, reject writes, strip browser
query context and share one read-only detail template.

## Live production composition

The Federation surface now reads the same durable capability services used by
onboarding:

- CFI-3 inspection snapshots;
- CFI-4 benchmark results;
- CFI-5 contribution candidates and persisted intent.

Optional provider, storage and job authorities that are not configured are
represented as available empty surfaces rather than false system failures. An
actual configured projection failure still produces a degraded state.

The overview also applies consistent empty and connection semantics:

- a trusted current-device binding cannot appear connected in one card and
  offline in the device list;
- zero benchmark results are shown as **Not run**, never as all evidence current;
- a connected device without inspection receives one direct **Open inspection**
  recommendation.

## Boundaries

These changes do not alter Federation authority, membership, discovery,
benchmark execution, contribution activation, storage assignment, compute
registration, persistence schemas or runtime intent. Production composition
performs existing read-only calls only. Benchmark evidence still never grants
authority, and storage remains candidate-only until the existing control plane
assigns it.
