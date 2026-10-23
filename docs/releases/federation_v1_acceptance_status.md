# Federation v1 acceptance and release status

Reviewed: **2026-10-23**.

## Acceptance outcome

**Federation v1 acceptance closeout is finalized, as confirmed by the operator on 2026-10-23; the release candidate is tagged as `v1.0.0-rc.1`.** The physical campaign reported **PASS on 2026-10-20**. Do not describe the acceptance test as awaiting execution, incomplete, or blocked. The release-candidate tag is verifiable, but the signed-off scenario-level evidence bundle has not yet been linked in this tracked documentation; so this page records the operator's confirmation rather than independently revalidating each scenario.

## Repository records and publication

| Item | Current status |
| --- | --- |
| Physical acceptance campaign | **PASS — 2026-10-20** |
| Acceptance closeout | **Finalized — operator confirmation 2026-10-23** (evidence archive linkage remains separate) |
| Release-candidate Git tag | **`v1.0.0-rc.1` exists**, pointing to `ef435b13ad00db387b28be96b469e7d01e309bb0` (verified 2026-10-23) |
| Retained exact-candidate evidence, sign-off and scenario matrix | **Archive linkage not present in this repository**; preserve the completed result and link its actual artifacts without inventing a candidate SHA |
| Machine-readable CF7 acceptance manifest | **Out of sync with the reported closeout**: `catalog/federation/tests/cf7_acceptance/scenarios.json` still says `acceptance_claim: foundation-only` and has `false` end-to-end and physical-evidence flags |
| FCP product release tag `v1.0.0` | **Not resolvable from this repository as checked on 2026-10-23** |
| GitHub product Release | **Not independently verified**; a release-candidate Git tag is not proof of final-release publication |

**Acceptance completion, the release-candidate tag, machine-readable manifest synchronization, and final product release publication are distinct records.** The old manifest values are a repository discrepancy; they should not be used to represent the successfully executed acceptance test as unexecuted. Conversely, the operator's confirmation is not a substitute for the evidence required before rewriting machine-enforced acceptance flags or asserting an exact release SHA.

## Record synchronization, not a new acceptance campaign

1. Preserve the verified `v1.0.0-rc.1` tag reference (`ef435b13ad00db387b28be96b469e7d01e309bb0`) and link the actual accepted/tested candidate SHA, final redacted PASS report, CF7/P01–P12 scenario results as applicable, and operator/reviewer sign-off. Do not assume the tag's SHA is the tested candidate without evidence establishing that relationship.
2. Reconcile `scenarios.json` and any retained acceptance artifacts only against the signed-off closeout and its validated physical-evidence contract. Do not flip flags solely to match prose.
3. The existing Git tag `v1.0.0-rc.1` is a release candidate, not the final `v1.0.0` product release. Independently verify any subsequently published final Git tag and GitHub Release against the exact accepted commit.
4. Follow the [release process](../release_process.md) for any subsequent product release action or impact assessment. Editing documentation does not extend the physical test to a new runtime commit.

See [release process](../release_process.md), [release scope](federation_v1_scope.md), and [acceptance workspace](../implementation/federation/acceptance/).

Historical failed attempts and evidence requirements remain part of the audit record; they do not supersede the finalized acceptance outcome.
