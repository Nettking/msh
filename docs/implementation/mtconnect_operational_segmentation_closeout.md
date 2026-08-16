# MTConnect operational segmentation — S7 closeout

| Metadata | Value |
| --- | --- |
| Status | **STOP AND REVIEW — stacked implementation** |
| Parent plan | [`mtconnect_operational_segmentation_plan.md`](mtconnect_operational_segmentation_plan.md) |
| Policy | `fcp.mtconnect.operational-segmentation.v1` |
| Input authority | canonical MTConnect observations |
| Merge claim | None; stacked PR status is not merged status |
| Post-track work | Not authorized by this closeout |

## Purpose

This document records the implementation/review state reached by S7 without changing the parent plan's merge-only status table. The repository owner explicitly authorized stacked development while the v1.0.0 release candidate remains frozen, so implementation state and merge state must be reported separately.

## Stacked implementation evidence

| Phase | Implementation evidence | Review state |
| --- | --- | --- |
| Foundation | PR #284 | merged before this track |
| S1 semantic roles | PR #289, refined by PR #291 | predecessor work |
| S2 device timeline | PR #298 | stacked draft |
| S3 MachineRun | PR #299 | stacked draft |
| S4 ProductionCycle | PR #300 | stacked draft |
| S5 OperationalEpisode | PR #301 | stacked draft |
| S6 durable projection/API | PR #302 | stacked draft |
| S7 validation/closeout | PR #303 | stacked draft / review gate |

The merge-only phase table in the parent plan must be updated only as those PRs actually land. This closeout must never be read as evidence that #298-#303 are already on `main`.

## S7 acceptance matrix

PR #303 maintains anonymised end-to-end regressions for every scenario required by the parent plan:

1. temporary `PROGRAM_STOPPED` inside one run;
2. short ACTIVE non-production interval;
3. pallet transition plus independent post-transition process evidence;
4. repeated concrete tool sequence producing distinct episodes;
5. long interruption inside one tool tenure;
6. nested program/comment/line churn inside one tool tenure;
7. genuine Agent sequence gap plus context reset;
8. deterministic rebuild from equivalent canonical input;
9. two interleaved Devices sharing one Agent sequence space;
10. unknown tool context without fabricated tool-change boundary;
11. timestamp regression without negative duration.

The S7 report/CLI is observational. It adds no new segmentation rule.

## Read-only private-reference procedure

From an existing canonical projection:

```bash
python scripts/validate_mtconnect_operational_segmentation.py \
  --canonical-db /path/to/canonical.sqlite3
```

From a private recorder ZIP:

```bash
python scripts/validate_mtconnect_operational_segmentation.py \
  --capture-zip /path/to/private-capture.zip \
  --workspace /path/to/local/review-evidence
```

The ZIP route stages manifest/payload bytes unchanged and invokes the existing recorder raw → canonical projection. The production capture is not committed and is never modified.

The report includes MachineRun counts/boundaries/reasons, cycle classifications/evidence/confidence, episode counts and tool/context sequences, wall/ACTIVE/PROGRAM_STOPPED duration accounting, and explicit gap/time/partial/tool-context warnings.

## Private reference-shape review

The private reference capture remains outside the repository. Local evidence review established the qualitative shapes that motivated the v1 rules:

- substantial ACTIVE execution periods contain temporary `PROGRAM_STOPPED` intervals without implying separate runs;
- short ACTIVE periods exist and therefore must not be equated with production;
- concrete pallet transitions occur before subsequent process evidence;
- concrete tool tenures repeat across the capture;
- nested program/subprogram/line churn occurs inside tool tenures;
- a long `PROGRAM_STOPPED` interval occurs inside one concrete tool tenure;
- the inspected capture is sequence-continuous at the raw Agent level.

Exact private timestamps/counts belong to local review evidence, not committed fixtures.

## Semantic deviations and clarifications discovered during implementation

These are explicit refinements of the original planning text, not hidden fallbacks:

1. **S1 has a `MULTI_CHANNEL` resolution state.** A same-device role may be semantically coherent across multiple component paths without being safe to scalarize. `RESOLVED` means exactly one scalar value is safe to consume.
2. **V1 execution must be singleton-resolved.** `MULTI_CHANNEL`, `AMBIGUOUS`, or unavailable `EXECUTION_STATE` fails closed for operational segmentation rather than selecting a path arbitrarily. Full operational-lane/multi-path support is deferred.
3. **Optional context is scalar only when resolved as one value.** Missing/ambiguous/multi-channel optional context remains unresolved rather than being guessed.
4. **S1 exposes one generic PROGRAM role, not an inferred SUBPROGRAM role.** Episode `subprogram` remains explicitly unresolved; program churn is preserved as context history.
5. **S6 persists device/projection status in addition to segment tables.** This keeps blocked/partial semantics visible even when no MachineRun can be emitted.
6. **`STRONG_INFERENCE` is not `PARTIAL`.** A strong inferred pallet boundary retains its confidence without being represented as a partial capture edge.

No machine-specific data-item IDs, fuzzy source rules, comparison signatures, baselines, anomaly thresholds, or learned semantics are introduced by these refinements.

## Known v1 limitations

- no full multi-path operational-lane model;
- no independent subprogram semantic role;
- process-motion evidence is not proof of cutting/material removal;
- `OperationalEpisode` is a historical unit, not a learned same-operation group;
- no activity phases/features/baselines/anomaly/prediction/recommendation semantics exist in this track.

## Review gate

When PR #303's required tests/lint are green and the local private-reference output is reviewed, the implementation track reaches its intended boundary:

**STOP AND REVIEW.**

The next action is review of the S1-S7 semantics and stacked merge/rebase order. A new architecture/design gate is required before implementing any ActivityPhase, EpisodeFeatures, BehaviouralBaseline, DeviationAssessment, Prediction, OperatorRecommendation, OSL/SysML integration, dashboard, scheduler, or Federation protocol extension.
