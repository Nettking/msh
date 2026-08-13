# MTConnect operational segmentation implementation plan

| Metadata | Value |
| --- | --- |
| Status | **Active authoritative implementation plan** |
| Track | Recorder → behavioural digital-twin operational segmentation |
| Audience | Repository owners, reviewers, and implementation agents |
| Baseline | `main` after PR #284 (`3e9155b6`) |
| Input authority | Canonical observations described by `canonical_mtconnect_observations.md` |
| End of this track | Durable/queryable `MachineRun`, `ProductionCycle`, and `OperationalEpisode` projections |
| Explicit stop | Do **not** continue into activity phases, feature learning, baselines, anomaly detection, prediction, recommendations, OSL/SysML, or dashboards |

## 1. Why this plan exists

PR #284 established a deterministic, rebuildable canonical MTConnect observation layer. The next task is to interpret those observations as operational history without corrupting the distinction between machine evidence and digital-twin inference.

This document is the execution order for that work. Agents should implement it **one PR at a time**. A later phase must not be started on an unmerged predecessor unless the repository owner explicitly asks for stacked PRs.

The target pipeline is:

```text
immutable recorder evidence
        ↓
canonical observations                   ← merged in PR #284
        ↓
semantic-role resolution                  ← S1
        ↓
sequence-safe state/context timeline      ← S2
        ↓
MachineRun + duration accounting          ← S3
        ↓
ProductionCycle inference                 ← S4
        ↓
OperationalEpisode segmentation           ← S5
        ↓
durable segmentation projection/API      ← S6
        ↓
reference-shape validation + closeout     ← S7
        ↓
STOP AND REVIEW
```

## 2. Semantic contract agents must preserve

The implementation may evolve internally, but the following distinctions are architectural requirements.

### Canonical observation = evidence

A canonical observation is a fact projected from verified recorder evidence. Segmentation must never modify canonical observations or raw recorder files.

### MachineRun = conservative execution interval

A `MachineRun` answers: *when was the controller in one coherent execution session?*

`ACTIVE` begins a run. `READY` is the primary observed end. Temporary `PROGRAM_STOPPED`/`FEED_HOLD` states remain inside the run and contribute to duration accounting.

A MachineRun is **not** a claim that a part was produced.

### ProductionCycle = inferred manufacturing interval

A `ProductionCycle` is a MachineRun, or a coherent part of one, that has substantive process-motion evidence plus manufacturing context. It is an inference and therefore stores its evidence and classification.

`ACTIVE == production` is forbidden.

### OperationalEpisode = comparison unit

An `OperationalEpisode` is a bounded interval inside one ProductionCycle with sufficiently stable context for later behavioural comparison.

V1 uses production-cycle boundaries and observed tool changes as episode boundaries. Program/subprogram/line churn is context history, not a generic episode splitter.

Episode identity must **not** be `(program, tool)` or another comparison signature. Comparison grouping belongs to a later analytical layer.

## 3. Global invariants

Every phase must preserve these invariants. If an implementation choice conflicts with one, stop and raise it in the PR instead of weakening the invariant silently.

1. Segmentation never writes to raw recorder evidence or `canonical_observations`.
2. No derived segment crosses `source_key` or `agent_instance_id`.
3. No derived segment crosses a canonical sequence gap.
4. Context is cleared across a sequence gap; it is not guessed across missing evidence.
5. Production cycles are contained in exactly one MachineRun.
6. Operational episodes are contained in exactly one ProductionCycle.
7. Temporary execution stops do not split runs/episodes by themselves.
8. A v1 observed tool-number change splits an episode exactly once.
9. Nested subprogram, program-comment, and line transitions do not split an episode by themselves.
10. All derived identities are deterministic and include the segmentation policy version.
11. All inferred boundaries/classifications are explainable through reason codes and canonical evidence sequences.
12. Unresolved time is represented explicitly; duration accounting may not silently drop time.
13. Same canonical input + same policy version must produce the same logical content and fingerprint.
14. Incremental execution, once implemented, must converge to a clean rebuild over the same complete canonical input.
15. Machine-specific data item IDs and fuzzy source-specific string rules do not belong in the generic segmenter.

## 4. Target package boundary

Prefer one new package under the recorder rather than spreading segmentation logic through Flask, federation, recorder capture, and analysis code:

```text
catalog/mtconnect_recorder/operational/
    __init__.py
    policy.py
    roles.py
    timeline.py
    model.py
    runs.py
    cycles.py
    episodes.py
    store.py
    projection.py
```

This is a recommended decomposition, not a mandate to create every file on day one. Each phase should add only the files it needs.

Use the existing `catalog/mtconnect_recorder/tests/` conventions for tests unless repository structure clearly favours another location.

The package must remain a synchronous domain/projection layer. Do not add a scheduler, worker pool, transport, federation protocol, or background thread. Existing job/federation infrastructure can call it later.

## 5. Policy/versioning contract

Define one explicit policy identifier before segment objects exist, for example:

```text
fcp.mtconnect.operational-segmentation.v1
```

All deterministic run/cycle/episode IDs and persisted segmentation rows must carry this policy identifier.

A semantic rule change that can change boundaries or classifications requires a policy version change. Do not silently reinterpret old IDs under changed rules.

Recommended deterministic identity payload:

```text
{
  "kind": "machine-run" | "production-cycle" | "operational-episode",
  "segmentation_policy": "fcp.mtconnect.operational-segmentation.v1",
  "source_key": "...",
  "agent_instance_id": 123,
  "start_sequence": 100,
  "end_sequence": 200
}
```

Hash canonical JSON with SHA-256. Do not use UUID4/random identifiers.

---

# S1 — Semantic-role resolver

## Objective

Give downstream segmentation a machine-neutral way to ask for *execution state*, *tool number*, *program*, *pallet*, *path feed*, *spindle velocity*, etc., without embedding Mazak-specific data item IDs.

## Dependency

PR #284 only.

## Required implementation

Create the operational package/policy constants and a semantic-role resolver over `CanonicalObservation` metadata.

V1 roles should cover at least:

```text
EXECUTION_STATE
CONTROLLER_MODE
PROGRAM
PROGRAM_COMMENT
LINE
TOOL_NUMBER
TOOL_GROUP
PALLET
SPINDLE_VELOCITY
PATH_FEEDRATE
AXIS_FEEDRATE
LOAD
```

Resolution precedence:

1. `mtconnect_type` / `sub_type` from the archived probe;
2. unambiguous `observation_type` semantics;
3. explicit compatibility aliases only when necessary and documented.

Do not use fuzzy matching. Do not add aliases such as `exec`, `tid`, or `spgm` merely because one reference machine uses them unless that compatibility behaviour is explicitly isolated and justified.

The resolver must be able to report:

```text
RESOLVED
UNAVAILABLE
AMBIGUOUS
```

An ambiguous role must not be resolved by arbitrary first-match behaviour.

## Tests

At minimum:

- role resolution from probe-derived MTConnect type;
- type/subtype precedence over incidental names;
- optional role absent → `UNAVAILABLE`;
- two incompatible candidates → `AMBIGUOUS`;
- source-specific-looking data item IDs do not accidentally become generic semantics;
- canonical observation objects remain unchanged.

## Acceptance gate

S1 is done when later code can consume semantic roles without knowing the reference Mazak's data item IDs.

## Forbidden scope

No timeline reconstruction. No MachineRun. No cycles. No episodes. No SQLite segmentation store.

---

# S2 — Sequence-safe state and context timeline

## Objective

Turn ordered canonical observations into deterministic execution/context transitions while handling gaps and partial captures correctly.

## Dependency

S1 merged.

## Required implementation

Build timeline primitives for one `(source_key, agent_instance_id)` at a time.

Required concepts should include equivalents of:

```text
ExecutionStateSpan
ContextSnapshot
ContextTransition
SequenceDiscontinuity
TimelineReport
```

Reconstruct at least these context values where roles are available:

```text
execution_state
controller_mode
program
program_comment
line
tool_number
tool_group
pallet
```

Ordering authority inside one Agent instance is canonical sequence, not timestamp sorting.

### Gap rule

When `next.sequence != previous.sequence + 1`:

- emit a sequence-gap discontinuity;
- close any state span at the last pre-gap evidence;
- clear reconstructed context;
- do not inherit tool/program/pallet state across the gap;
- mark post-gap context partial until observations establish it again.

A large timestamp jump with continuous sequence numbers is not automatically a gap.

### Context transitions

Every role change should preserve:

```text
sequence
timestamp
role
previous_value
new_value
data_item_id
```

Do not deduplicate away meaningful repeated controller events unless the semantic contract proves they carry no state change.

## Tests

- normal ordered state/context transitions;
- sequence gap resets context;
- timestamp gap with continuous sequence does not reset context;
- Agent instance separation;
- same timestamp on multiple observations still has deterministic sequence order;
- partial capture beginning with already-established context;
- ambiguous/unavailable execution role surfaces explicitly rather than inventing a timeline.

## Acceptance gate

S2 is done when the repository can deterministically reconstruct an explainable state/context timeline from canonical observations without yet deciding where runs/cycles/episodes are.

## Forbidden scope

No production classification. No tool-change episodes. No learned thresholds.

---

# S3 — MachineRun segmentation and duration accounting

## Objective

Implement the conservative observed execution layer and exact time accounting that later cycle/episode logic will depend on.

## Dependency

S2 merged.

## Required semantics

### Run start

A transition into `ACTIVE` from a non-running execution state starts a MachineRun.

If usable evidence begins already `ACTIVE`, create a partial run:

```text
reason = CAPTURE_BEGINS_ACTIVE
confidence = PARTIAL
partial_start = true
```

### Run continuation

These do not end a run by themselves:

```text
PROGRAM_STOPPED
FEED_HOLD
line change
program/subprogram change
program comment change
tool change
spindle stop/start
feed dropping to zero
```

### Run end

`READY` is the primary observed end:

```text
reason = EXECUTION_READY
confidence = OBSERVED
```

Sequence gap / Agent-instance end / capture end close the run as partial evidence.

### Duration accounting

At minimum calculate exact integer microseconds for:

```text
wall_duration_us
active_duration_us
program_stopped_duration_us
feed_hold_duration_us
other_execution_duration_us
unknown_duration_us
```

Use last-observation-holds-until-next-state semantics inside uninterrupted evidence.

Buckets must reconcile to wall duration; any unresolved interval belongs in `unknown_duration_us`.

## Required boundary model

Introduce named reason/confidence enums/constants rather than free-text business logic.

At least:

```text
EXECUTION_ACTIVE
EXECUTION_READY
CAPTURE_BEGINS_ACTIVE
CAPTURE_END
SEQUENCE_GAP
AGENT_INSTANCE_END

OBSERVED
STRONG_INFERENCE
PARTIAL
```

Every boundary retains trigger/evidence sequence information.

## Tests

Most important regression fixture:

```text
ACTIVE
PROGRAM_STOPPED
ACTIVE
PROGRAM_STOPPED
ACTIVE
READY
```

Expected: **one MachineRun**, with stopped duration represented separately.

Also cover:

- multiple independent runs separated by READY;
- capture starts ACTIVE;
- capture ends ACTIVE;
- sequence gap inside ACTIVE closes partial run and prevents context carry-over;
- duration bucket reconciliation;
- deterministic identity reproduction.

## Acceptance gate

S3 is done when MachineRun is useful as an observed execution-history object without claiming anything about production.

## Forbidden scope

Do not classify runs as production yet. Do not split by tool.

---

# S4 — ProductionCycle inference

## Objective

Distinguish likely production activity from incidental/setup/transfer ACTIVE periods, while keeping the result explicitly inferential.

## Dependency

S3 merged.

## Required evidence flags

Derive explainable flags for each run/candidate without baselines or ML:

```text
HAS_SPINDLE_MOTION
HAS_PATH_FEED_MOTION
HAS_AXIS_FEED_MOTION
HAS_TOOL_CONTEXT
HAS_PROGRAM_CONTEXT
HAS_PALLET_CONTEXT
HAS_LOAD_TELEMETRY
```

A motion flag means valid numeric evidence above zero during the candidate interval. It is evidence of process motion, not proof of cutting/material removal.

## Required classification

V1 classifications:

```text
PRODUCTION_CANDIDATE
NON_PRODUCTION
UNCERTAIN
```

A `PRODUCTION_CANDIDATE` requires:

1. at least one process-motion flag; and
2. at least one manufacturing-context flag.

If relevant motion roles are available and no process motion occurred, classify `NON_PRODUCTION`.

If the evidence needed to decide is unavailable/ambiguous, classify `UNCERTAIN` rather than treating missing data as zero.

Persist/expose the evidence flags and reason codes used for the classification.

## Cycle splitting in v1

Allow only strong work-context evidence to split a MachineRun into multiple cycles.

A pallet/workholding identity transition may split the run when the post-transition interval independently qualifies as a production candidate:

```text
reason = PALLET_CONTEXT_CHANGE
confidence = STRONG_INFERENCE
```

Do **not** generically split on program/subprogram/line transitions. The reference trace contains nested Renishaw macro/program churn inside one manufacturing activity.

## Tests

- long ACTIVE with motion + tool/program context → production candidate;
- short ACTIVE with available motion roles but no process motion → non-production;
- required motion semantics unavailable → uncertain;
- pallet change with substantive post-change process evidence creates a strong inferred boundary;
- pallet change without post-change production evidence does not fabricate a new production cycle;
- nested program/subprogram changes do not create cycle boundaries;
- cycle remains fully contained in its MachineRun.

## Acceptance gate

S4 is done when every cycle classification and inferred split is explainable from stored evidence and no rule equates ACTIVE with production.

## Forbidden scope

No behavioural baseline. No anomaly threshold. No cutting classifier. No part-completion claim.

---

# S5 — OperationalEpisode segmentation

## Objective

Create the stable historical units that later feature extraction can compare.

## Dependency

S4 merged.

## V1 boundary rules

Episode boundaries are limited to:

1. ProductionCycle start;
2. observed tool-number change;
3. ProductionCycle end;
4. inherited sequence-gap/capture-edge partial boundaries.

On a tool change, the tool-change observation belongs to the new context. The prior episode's observation membership ends immediately before it; the boundary timestamp is the tool-change timestamp.

Required reason codes include equivalents of:

```text
CYCLE_START
TOOL_CHANGE
CYCLE_END
SEQUENCE_GAP
CAPTURE_END
```

## Context at entry

Capture the context known at episode entry:

```text
main_program
subprogram
program_comment
line
controller_mode
tool_number
tool_group
pallet
```

Retain subsequent changes as `ContextTransition` records tied to the episode.

## Do not split on

```text
PROGRAM_STOPPED
FEED_HOLD
spindle velocity reaching zero
path feed reaching zero
line/block changes
program comments
nested subprogram/macro calls
```

## Required reference-shape tests

### Repeated tool sequence

A cycle with:

```text
72 → 155 → 108 → 91 → 60
```

must produce one episode per tool tenure, not one episode per line/subprogram transition.

### Tool 160 interruption

One tool tenure containing a long `PROGRAM_STOPPED` interval must remain **one episode**. Wall duration includes the interruption and `program_stopped_duration_us` exposes it separately.

### Renishaw-style nested calls

Many program/subprogram/comment/line transitions inside one tool tenure must remain one episode unless a v1 hard boundary occurs.

## Acceptance gate

S5 is done when episodes are deterministic, non-overlapping comparison units with complete context transition history and no comparison/baseline semantics baked into their identity.

## Forbidden scope

Do not group episodes into “same operation”. Do not implement activity phases or feature vectors.

---

# S6 — Durable segmentation projection and query API

## Objective

Make runs/cycles/episodes disposable, rebuildable, queryable derived state with the same engineering discipline as the canonical observation projection.

## Dependency

S5 merged.

## Storage boundary

Create a separate segmentation projection. Do not add segmentation columns to `canonical_observations`.

A natural default is a separate SQLite database, e.g.:

```text
operational_segmentation.sqlite3
```

Exact table names may follow repository conventions, but the model must cover conceptually:

```text
segmentation_runs
segmentation_cycles
segmentation_episodes
segmentation_boundaries
segmentation_context_transitions
segmentation_projection_runs
```

## Required persisted provenance

Every segment stores at least:

```text
segment_id
segmentation_policy
source_key
agent_instance_id
start_sequence
end_sequence
start_at
end_at
start_reason
end_reason
confidence / partial flags
parent_id where applicable
```

Every boundary must be traceable to canonical evidence sequences.

## Projection properties

### Deterministic

Same canonical observations + same policy → same content and IDs.

### Idempotent

Re-running unchanged input does not duplicate logical rows.

### Rebuildable

Delete the segmentation DB and rebuild from canonical observations only. Raw XML must not be needed for normal segmentation.

### Incremental

It is acceptable for the first implementation to rebuild one affected Agent instance rather than perform row-by-row streaming updates, provided it is bounded and documented. Whatever incremental strategy is chosen must converge exactly to clean-rebuild output.

### Auditable

Queries can walk segment → boundary/context transition → canonical observation identity.

### Fail visible

Ambiguous execution semantics, partial captures and sequence gaps appear in status/reason fields; they do not disappear silently.

## Narrow query API

Provide only what downstream digital-twin work needs, for example:

```text
runs for source/instance/time window
cycles for run or source/time window
episodes for cycle or source/time window
episodes for tool context
context transitions for episode
boundary/provenance lookup
latest/open segment where meaningful
```

Do not build a general analytics warehouse.

## Required tests

- deterministic IDs;
- identical content fingerprint after delete/rebuild;
- idempotent replay;
- incremental/converged result equals clean rebuild;
- parent/child containment enforced;
- no overlap inside a cycle;
- boundary provenance lookup;
- canonical/raw evidence remains byte/logically unchanged;
- schema migration/version validation;
- query ordering deterministic.

## Acceptance gate

S6 is done when operational history can be deleted and reproduced exactly from canonical observations and queried without reparsing raw XML.

## Forbidden scope

No federation transport, no remote scheduler, no baseline/anomaly/prediction tables.

---

# S7 — End-to-end validation, reference-shape check, and track closeout

## Objective

Prove the complete v1 segmentation stack behaves sensibly on anonymised fixtures shaped like the real recorder capture, then validate against the real capture locally without committing production data.

## Dependency

S6 merged.

## Required anonymised regression scenarios

Maintain small fixtures for all of these:

1. temporary `PROGRAM_STOPPED` periods inside one run;
2. short ACTIVE non-production interval;
3. pallet transition followed by independent process evidence;
4. repeated tool sequence creating episodes;
5. Tool 160-style long interruption inside one episode;
6. Renishaw-style nested program/subprogram calls inside one tool tenure;
7. canonical sequence gap and context reset;
8. deterministic rebuild from equivalent canonical input.

## Local real-capture validation

Do not commit the production capture.

Provide or document a read-only validation command/script that can point at the canonical projection/reference capture locally and report, at minimum:

```text
number of MachineRuns
run boundaries + reasons
cycle classifications + evidence flags
cycle boundaries + confidence
episode count per cycle
episode tool/context sequence
wall/ACTIVE/PROGRAM_STOPPED durations
partial/gap warnings
```

The purpose is inspection, not a dashboard.

The reference capture should reproduce the qualitative cases that motivated the design, including:

- the substantial ~09:32:50 → ~10:15:07 execution period remaining one MachineRun despite temporary PROGRAM_STOPPED states;
- short ACTIVE periods not automatically being labelled production;
- the pallet-context change around the later production activity being visible as boundary evidence;
- repeated tool tenures such as 72/155/108/91/60 becoming distinct episode instances;
- the long Tool 160 PROGRAM_STOPPED interval remaining inside one episode and appearing in interruption duration.

Exact timestamps/counts from private production data belong in local validation evidence or review notes, not committed fixtures unless the data owner explicitly approves publication.

## Documentation closeout

Update this plan's phase status, operational package documentation, and implementation index. Record any semantic deviation from the plan explicitly.

## Acceptance gate

S7 closes this track only when:

- all anonymised regression scenarios pass;
- local reference-capture output has been reviewed for plausible boundaries;
- full relevant repository tests are green;
- linting on touched code is green or parity with main is explicitly demonstrated;
- no later digital-twin semantics leaked into segmentation.

After S7: **STOP AND REVIEW**.

---

# 6. Work that may run in parallel

The core semantic chain S1 → S2 → S3 → S4 → S5 → S6 is serial by default.

Safe parallel support work, once its dependencies are merged, includes:

- anonymised fixture construction;
- documentation/examples;
- independent review of reason-code semantics;
- performance profiling of canonical queries;
- real-capture read-only validation tooling.

Do not run two agents in parallel implementing competing definitions of MachineRun/Cycle/Episode and merge whichever finishes first. Those semantics are shared architecture, not interchangeable implementation details.

# 7. Agent operating rules

Every implementation agent must:

1. Read this plan and `docs/implementation/canonical_mtconnect_observations.md` before coding.
2. Confirm the previous phase is merged into `main`.
3. Work only the assigned phase plus fixes strictly necessary for it.
4. Reuse existing recorder/canonical/storage abstractions rather than introducing parallel infrastructure.
5. Add tests that prove semantic boundaries, not merely helper coverage.
6. Run focused tests, then the broader relevant `catalog` suite, then Ruff on touched files.
7. Self-review for machine-specific assumptions and hidden fallbacks.
8. Push a branch and open a PR against current `main`.
9. **Do not merge the PR.** Report it for review.
10. In the final handoff, report:

```text
phase implemented
PR number + URL
head SHA
files changed
semantic decisions made
reason codes added/changed
tests and exact results
known limitations
anything that deviates from this plan
what the next phase may now rely on
```

If the agent discovers that a rule in this plan is contradicted by the MTConnect standard, the canonical data model, or observed recorder evidence, it must stop expanding scope and document the contradiction in the PR. Do not quietly “fix” the plan in code.

# 8. Copy/paste task template for agents

Use this when assigning a phase:

```text
Work on Nettking/msh.

Implement ONLY phase S<N> of:
  docs/implementation/mtconnect_operational_segmentation_plan.md

Before coding:
- fetch current main;
- read the complete implementation plan;
- read docs/implementation/canonical_mtconnect_observations.md;
- confirm all predecessor phases required by S<N> are already merged.

Treat the plan's semantic contract, global invariants, acceptance gate, and forbidden scope as requirements.

Do not implement later phases opportunistically. Do not add activity phases, feature baselines, anomaly detection, ML/prediction, recommendations, OSL/SysML, dashboards, a new scheduler, a new federation protocol, or a parallel storage architecture.

Implement the phase, add meaningful regression tests, run focused tests + the broader relevant catalog suite + Ruff on touched files, self-review the diff for machine-specific assumptions, then push and open a PR against current main. Do not merge it.

In your final response report the PR URL/number, exact head SHA, files changed, tests/results, semantic decisions, limitations, deviations from the plan, and what the next phase can safely assume.
```

# 9. Track status

Update this table only when a phase is merged.

| Phase | Status | Merge/PR evidence | Next allowed work |
| --- | --- | --- | --- |
| Foundation: canonical observations | **MERGED** | PR #284 / `3e9155b6` | S1 |
| S1 Semantic-role resolver | NOT STARTED | — | none |
| S2 State/context timeline | BLOCKED ON S1 | — | none |
| S3 MachineRun | BLOCKED ON S2 | — | none |
| S4 ProductionCycle | BLOCKED ON S3 | — | none |
| S5 OperationalEpisode | BLOCKED ON S4 | — | none |
| S6 Durable projection/API | BLOCKED ON S5 | — | none |
| S7 Validation/closeout | BLOCKED ON S6 | — | none |

# 10. Explicitly deferred next track

The following is **not part of this implementation plan**:

```text
ActivityPhase
        ↓
EpisodeFeatures
        ↓
BehaviouralBaseline
        ↓
DeviationAssessment
        ↓
Prediction
        ↓
OperatorRecommendation
```

A new design/review gate is required before beginning that stack. In particular, do not let S5/S6 encode `(program, tool)` or any other comparison signature as episode identity merely because it would make later baseline code easier.
