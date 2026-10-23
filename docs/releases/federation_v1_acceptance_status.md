# Federation v1 acceptance and release status

Reviewed: **2026-10-23**.

## Acceptance outcome

**Federation v1 acceptance is finalized and tagged, as confirmed by the operator on 2026-10-23.** The physical campaign reported **PASS on 2026-10-20**. Do not describe the acceptance test as awaiting execution, incomplete, or blocked. The acceptance tag identifier and signed-off evidence bundle have not yet been linked in this tracked documentation, so this page records the operator's confirmation rather than independently revalidating each scenario.

## Repository records and publication

| Item | Current status |
| --- | --- |
| Physical acceptance campaign | **PASS — 2026-10-20** |
| Acceptance closeout / acceptance tagging | **Finalized and tagged — operator confirmation 2026-10-23** (acceptance tag reference not recorded here) |
| Retained exact-candidate evidence, sign-off and scenario matrix | **Archive linkage not present in this repository**; preserve the completed result and link its actual artifacts without inventing a candidate SHA |
| Machine-readable CF7 acceptance manifest | **Out of sync with the reported closeout**: `catalog/federation/tests/cf7_acceptance/scenarios.json` still says `acceptance_claim: foundation-only` and has `false` end-to-end and physical-evidence flags |
| FCP product release tag `v1.0.0` | **Not resolvable from this repository as checked on 2026-10-23** |
| GitHub product Release | **Not independently verified**; do not infer publication merely from acceptance tagging |

**Acceptance completion, machine-readable manifest synchronization, and product release publication are distinct records.** The old manifest values are a repository discrepancy; they should not be used to represent the successfully executed acceptance test as unexecuted. Conversely, the operator's confirmation is not a substitute for the evidence required before rewriting machine-enforced acceptance flags or asserting an exact release SHA.

## Record synchronization, not a new acceptance campaign

1. Link the actual acceptance tag/reference, tested commit SHA, final redacted PASS report, CF7/P01–P12 scenario results as applicable, and operator/reviewer sign-off. Preserve their provenance; do not derive a tested SHA from a documentation commit.
2. Reconcile `scenarios.json` and any retained acceptance artifacts only against the signed-off closeout and its validated physical-evidence contract. Do not flip flags solely to match prose.
3. Independently verify the `v1.0.0` Git tag and GitHub Release, if those have been published, and record the exact accepted commit they identify. Otherwise keep product release publication separate from the already-finalized acceptance campaign.
4. Follow the [release process](../release_process.md) for any subsequent product release action or impact assessment. Editing documentation does not extend the physical test to a new runtime commit.

See [release process](../release_process.md), [release scope](federation_v1_scope.md), and [acceptance workspace](../implementation/federation/acceptance/).

Historical failed attempts and evidence requirements remain part of the audit record; they do not supersede the finalized acceptance outcome.
