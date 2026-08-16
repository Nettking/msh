# MTConnect operational segmentation

This package is the synchronous, machine-neutral interpretation layer over canonical MTConnect observations.

It preserves the distinction between **evidence** and **inference**:

```text
immutable recorder evidence
        ↓
canonical observations
        ↓
semantic roles
        ↓
per-device execution/context timeline
        ↓
MachineRun
        ↓
ProductionCycle
        ↓
OperationalEpisode
        ↓
durable operational projection
```

The policy identifier is `fcp.mtconnect.operational-segmentation.v1`. A semantic change that can alter partitioning, boundaries, classifications, durations, or deterministic identities requires a new policy version.

## Scope and partitioning

Canonical Agent sequence continuity belongs to `(source_key, agent_instance_id)`. Operational state belongs to `(source_key, agent_instance_id, device_key)`. A real Agent-wide sequence gap is detected before device partitioning; ordinary sequence-number jumps caused by observations from another Device are not gaps.

Execution, program, tool, pallet, runs, cycles, and episodes are never shared across Devices. Derived segments never cross source, Agent instance, or device boundaries.

## Semantic-role contract

Roles are resolved from canonical MTConnect metadata rather than source-specific data-item IDs. `RESOLVED` means one scalar semantic value is safe to consume. `MULTI_CHANNEL` preserves several compatible component channels without selecting one arbitrarily. `AMBIGUOUS` represents contradictory semantic candidates. `UNAVAILABLE` means no candidate exists.

The v1 operational timeline requires `EXECUTION_STATE` to be a single resolved value. Multi-channel or ambiguous execution fails closed instead of silently selecting a path. Optional context roles are consumed as scalar context only when they are resolved as one value. Full multi-path operational lanes are deliberately deferred to a later design gate.

## MachineRun

A `MachineRun` is conservative observed execution history, not a claim of production. `ACTIVE` starts a run; `READY` is the primary observed end. `PROGRAM_STOPPED` and `FEED_HOLD` remain inside the run. Capture edges, genuine sequence gaps, Agent-instance endings, and timestamp regressions close runs conservatively as partial evidence when required.

Duration accounting uses integer microseconds and reconciles:

- `active_duration_us`
- `program_stopped_duration_us`
- `feed_hold_duration_us`
- `other_execution_duration_us`
- `unknown_duration_us`

against `wall_duration_us`. Timestamp regressions never create negative elapsed time.

## ProductionCycle

`ACTIVE == production` is forbidden. A cycle is classified as `PRODUCTION_CANDIDATE`, `NON_PRODUCTION`, or `UNCERTAIN` from explicit same-device process-motion and manufacturing-context evidence. Motion above zero is evidence of process motion; it is not proof of cutting or material removal.

A concrete pallet/workholding transition may create an intra-run cycle boundary only when the post-transition interval independently qualifies as a production candidate. Such a boundary is `STRONG_INFERENCE`, not an observed execution boundary.

## OperationalEpisode

An `OperationalEpisode` is a historical comparison unit inside exactly one ProductionCycle. V1 boundaries are cycle edges plus direct transitions between two different concrete resolved tool identities.

Unknown, unavailable, empty, or ambiguous tool context does not fabricate a tool-change point. Tool number is preferred as the identity source; tool group is the deterministic fallback. Program, program-comment, and line churn remain context history and do not generically split episodes. Temporary execution stops also remain inside the episode and are visible through duration buckets.

S1 exposes one generic PROGRAM role; it does not infer a separate subprogram role. `subprogram` is therefore retained as unresolved rather than guessed from program churn.

## Durable projection

`store.py` keeps segmentation as disposable derived SQLite state separate from `canonical_observations`. Full rebuilds use canonical SQLite only; raw XML is not needed. A bounded incremental rebuild replaces one complete `(source_key, agent_instance_id)` so every Device sharing the Agent sequence space remains consistent.

The store persists projection/device status, runs, cycles, episodes, boundaries, and episode context transitions. Parent/child scope and containment, episode non-overlap, and duration reconciliation are enforced in SQLite as well as in domain code.

Queries expose only the operational history needed by later digital-twin work: runs, cycles, episodes, concrete tool context, context transitions, boundary provenance, latest segments, and projection/device status.

## S7 validation

The read-only closeout command is:

```bash
python scripts/validate_mtconnect_operational_segmentation.py \
  --canonical-db /path/to/canonical.sqlite3
```

To validate a private recorder ZIP through the repository's existing raw → canonical → operational pipeline:

```bash
python scripts/validate_mtconnect_operational_segmentation.py \
  --capture-zip /path/to/private-capture.zip
```

Use `--workspace /path/to/review-evidence` to retain the locally derived canonical and operational SQLite files. Use `--json` for deterministic machine-readable output.

The validator does not introduce a second MTConnect parser. ZIP bytes are staged unchanged into a temporary recorder layout, then the existing canonical recorder projection is invoked. The original capture is never modified and production capture bytes must not be committed.

The report includes MachineRun counts and boundaries, cycle classifications/evidence/confidence, episode tool/context sequences, wall/ACTIVE/PROGRAM_STOPPED durations, and explicit partial/gap/timestamp/tool-context warnings.

## Known v1 limitations

- No full multi-path operational-lane model; multi-channel execution fails closed.
- No separate inferred subprogram semantic role.
- Process-motion evidence is not a cutting/material-removal classifier.
- Episode identity is historical identity, not a `(program, tool)` comparison signature.
- No activity phases or learned features are part of this package.

## Explicit stop after S7

The operational-segmentation track ends after reference-shape validation and review. Do **not** continue from this package directly into activity phases, feature learning, behavioural baselines, anomaly detection, prediction, operator recommendations, OSL/SysML integration, dashboards, or a new Federation/scheduling protocol.

Those concerns require a separate architecture/design review before implementation.
