# Federation v1 acceptance and release status

Reviewed: **2026-10-20**.

## Physical acceptance execution

**PASS reported by the acceptance operator on 2026-10-20.** The physical acceptance campaign is no longer described as an unexecuted or failed task. This is an operator-reported outcome; the exact candidate commit, signed-off physical evidence bundle and scenario results were not available in the tracked repository at the time of this documentation reconciliation. Accordingly, this file does not independently certify each physical assertion or grant new runtime authority.

## Recorded acceptance versus release publication

| Milestone | Current status |
| --- | --- |
| Physical acceptance campaign execution | **PASS — operator reported (2026-10-20)** |
| Exact-commit evidence bundle and independent review | **Not yet linked/recorded in this repository** |
| Machine-readable CF7/V1 acceptance decision | **Not reconciled:** `catalog/federation/tests/cf7_acceptance/scenarios.json` still records `acceptance_claim: foundation-only` and the three `*_accepted` / `physical_evidence_accepted` flags as `false` |
| `v1.0.0` Git tag and GitHub Release | **Not published** as checked 2026-10-20 |

The manifest's still-false flags are a **record-keeping discrepancy to reconcile**, not evidence that the newly reported physical run failed. Conversely, an operator's reported PASS cannot alone prove the exact-commit, full-scope CF7 requirements enforced by the repository's release procedure.

## Closeout actions

1. Preserve or link the redacted final PASS report, scenario matrix (including P01–P12 and CF7 where applicable), approving reviewer, physical host observations, and **exact tested candidate SHA**. Do not infer a tested SHA from today's documentation commit.
2. Validate the bundle against the current physical-evidence schema and complete a separate evidence-backed acceptance decision. Update `scenarios.json` and remove obsolete blocked entries only when the evidence supports the corresponding claims.
3. Apply the release procedure's exact-commit/impact-revalidation policy if the accepted candidate differs from the final release commit, including documentation-only changes. Do not claim that merely editing docs extends a prior physical run to a new runtime commit.
4. Confirm required release gates, release notes and change log, then create `v1.0.0` and its GitHub Release **only after** the recorded acceptance and source identities match.

See [release process](../release_process.md), [release scope](federation_v1_scope.md), and [acceptance workspace](../implementation/federation/acceptance/).

This status supersedes older *current-state* wording that calls the physical test unexecuted, unaccepted as a test result, or still to be scheduled. Historical failed runs and requirements remain part of the record.
