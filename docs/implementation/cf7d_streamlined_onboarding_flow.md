# CF7-D streamlined onboarding flow

Status: focused UX correction discovered during the first physical browser test.

## Problem

Capability-first setup redirected the operator to the read-only Federation
overview immediately after migration or Finish. The Finish panel also exposed a
parallel Federation-overview link, and the normal product navigation remained
visible throughout the guided setup. This made onboarding feel like several
loosely connected pages instead of one continuous task.

## Corrected flow

The supported browser flow is now:

```text
Identity -> Federation -> Inspect -> Benchmarks -> Contributions -> Finish
```

A successful migration or Finish POST returns to
`/onboarding?step=finish`. The completed Finish panel confirms that the device
is ready and offers one explicit **Continue to MSH** action. The operator is not
redirected automatically to the Federation overview.

Capability onboarding uses the existing focused setup shell, so normal product
navigation is hidden until the operator chooses to continue. The Federation
overview remains available from normal product navigation after setup.

## Boundaries

This change does not alter Federation authority, membership, discovery,
benchmark execution, contribution activation, storage assignment, compute
registration, persistence schemas, or runtime intent. It changes only guided
navigation and presentation after setup completion.
