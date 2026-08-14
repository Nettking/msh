# Federation execution efficiency learning

| Field | Value |
| --- | --- |
| Classification | Maintained reference — describes current contracts and architecture |
| Scope | How the Federation learns where and how to execute jobs from real executions |
| Authority changes | None. Learning affects ranking of already-eligible candidates only |

## Problem

The Federation contains heterogeneous devices. Node A may be fastest for one
language-model workload, node B slower but far more memory-efficient, node C
strong on CPU-heavy analysis, and node D excellent for large jobs but dominated
by startup overhead on small ones. Before this work the scheduler ranked
compatible providers by a fixed capacity tuple — available slots, queue depth,
utilisation, then capability identity — which is workload-blind and identical
on the thousandth job and the first.

The central design principle is: **every useful execution teaches the
Federation something about how to execute the next comparable job more
efficiently.**

## What is reused

Nothing here is a parallel scheduler.

| Existing component | Role |
| --- | --- |
| `catalog/capabilities/jobs.py` | `JobContract` and `CapabilityRequirement` define the work and its hard requirements |
| `catalog/capabilities/provider_reports.py` | `ProviderResourceReport` is the live per-provider snapshot; `ProviderSelectionPolicy` carries operator constraints |
| `catalog/capabilities/provider_selection.py` | `evaluate_provider_candidate` keeps sole ownership of hard compatibility; `select_provider` remains the single ranking chokepoint used by the AI runtime, the lifecycle coordinator's reassignment path, and the analysis scheduler |
| `catalog/capabilities/dispatch.py` | Dispatch events already carry accepted/running/terminal timestamps, success and the worker's result details |
| `catalog/federation/onboarding_models.py` | `BenchmarkResult` and `DeviceInspectionSnapshot` provide cold-start evidence |
| `catalog/federation/sqlite_schema.py` | `ensure_sqlite_schema` + explicit `SQLiteMigration` steps |
| `catalog/federation/redaction.py` | Secret and non-public-location screening for all reported telemetry |
| `catalog/federation/event_log.py`, `human_auth.py` | The authoritative session log, and the "a member may publish only about itself" authority rule |

## New components

All new code lives in `catalog/capabilities/efficiency/`.

| Module | Responsibility |
| --- | --- |
| `contracts.py` | Versioned `WorkloadDescriptor`, `ExecutionEnvironment`, `ExecutionMeasurements`, `ExecutionObservation` |
| `profiles.py` | `DecayedStatistic`, `ProfileKey`, `LearnedProfile`, `PerformanceEstimate`, `EvidenceTier` |
| `policy.py` | `SchedulingObjective`, `EfficiencyPolicy`, `objective_cost` |
| `store.py` | `SQLiteExecutionLearningStore`: raw observations, derived profiles, decision history |
| `estimator.py` | `PerformanceEstimator`: the cold-start hierarchy; benchmark and hardware-heuristic adapters |
| `ranking.py` | `LearnedProviderRanker` plus the inspectable `SchedulingDecisionRecord` |
| `recorder.py` | Completed dispatches to observations |
| `federation.py` | Event payloads, authority enforcement, coordinator-side ingest |
| `projection.py` | `learning_snapshot` / `preferred_nodes` read model |

The one change to existing code is an **optional** keyword on
`select_provider(..., ranker=...)` plus a `ranking` field on
`ProviderSelection`. With no ranker the behaviour is byte-for-byte what it was.

## Observation schema

`fcp.execution-observation.v1` carries job/attempt/session identity, the
executing `node_id` and `capability_id`, the requested protocol and a
fingerprint of the requested requirements, and three sub-schemas:

- `fcp.workload-descriptor.v1` — `capability_type`, `job_kind`, and bounded
  scalar `features`.
- `fcp.execution-environment.v1` — provider kind, service, model, software and
  runtime version, hardware class, accelerator, OS/architecture, configuration.
- `fcp.execution-measurements.v1` — `succeeded` plus optional `error_code`,
  `retries`, `queue_millis`, `execution_millis`, `total_millis`,
  CPU/GPU utilisation, memory and VRAM peaks, input/output units, throughput,
  `quality_score` and `energy_millijoules`.

**Only `succeeded` is required.** A node that cannot measure VRAM still
produces useful evidence. Every numeric field is range-checked, text is bounded
and control-character free, and feature/configuration objects are screened for
secrets and non-public locations.

### Workload similarity

`WorkloadDescriptor.class_keys()` returns the same workload at four
coarsenings, most specific first: `exact` (all features, numeric ones bucketed
on a log2 scale), `family` (categorical features only), `kind`
(`capability_type/job_kind`), and `capability`. Identical keys collapse.

This is what lets a tiny classification, a long summarisation, an embedding
call and a batch inference share one capability yet be learned separately, and
it is extensible: adding a feature adds classes, it does not replace the
architecture. Nothing in the design is specific to language models.

## Learned state

`ProfileKey = (capability_type, workload_class, node_id, capability_id,
environment_fingerprint)`.

Each `LearnedProfile` holds an exponentially time-decayed count, mean and
mean-square per metric, plus a decayed success/trial statistic. Estimates carry
their evidence weight, a Laplace-smoothed success probability, a standard
deviation and the tier that produced them. **No single scalar score is stored**
— the policy decides which measurements matter.

Three views are maintained per workload tier so lookup is a direct key read:
`(node, provider, environment)`, `(node, provider, any environment)` and the
pooled federation prior.

### Why decayed statistics rather than reinforcement learning

Exponentially weighted statistics with a confidence-bounded comparison were
chosen over online regression or RL because they are the simplest method that
still adapts:

- Every number is a decayed mean, variance or rate that can be printed into a
  scheduling explanation. Nothing is opaque.
- Updates are O(1) with no training loop, no model artefact to version or
  serve, and no offline corpus.
- Non-stationarity is handled by construction: half-life decay is exactly the
  behaviour wanted for throttling, upgrades and changing load.
- The estimator interface (`PerformanceEstimator` → `PerformanceEstimate`) is a
  seam. Raw observations are retained separately, so an online regression or a
  richer contextual model can be recomputed from history later without touching
  job execution.

## Scheduling algorithm

1. `select_provider` evaluates hard compatibility exactly as before — session,
   status, report freshness, capability type/protocol/version, structural
   requirement matching, slot/queue/utilisation limits. Rejected providers are
   never visible to the ranker.
2. Eligible candidates are sorted by the existing deterministic tuple.
3. If a ranker is installed it may return a **permutation of exactly those
   candidates**. Any other result — a raise, `None`, an unknown capability id,
   a wrong length — falls back to the deterministic order.
4. The learned order is applied *within* the operator preference grouping, so
   `preferred_node_id` is never overridden by evidence.

Ranking itself computes, per candidate, an estimate and one objective cost,
normalises costs to `[0, 1]`, and sorts. An unmeasured candidate is treated
pessimistically when exploiting and optimistically when exploring.

### Multi-objective support

`SchedulingObjective` covers fastest completion, lowest queue time, highest
reliability, lowest resource cost, maximum throughput, balanced utilisation,
preferred-local and energy efficiency. Fastest completion is failure-aware:
`expected_cost = total_millis / P(success)`, the expected cost of the geometric
retry sequence the existing retry policy already performs. Energy efficiency is
implemented but only produces a comparison once nodes report
`energy_millijoules`.

## Cold start

| Tier | Source |
| --- | --- |
| `measured-exact` | This workload class, this node, this environment |
| `measured-similar` | A coarser workload class on the same node/environment |
| `measured-legacy-environment` | The same node before its current model/software version, at `cross_environment_evidence_weight` (default 0.25) |
| `benchmark` | Existing `BenchmarkResult` evidence for that node |
| `capability-heuristic` | Advertised `attributes.hardware.compute_units`, clamped to `[0.25, 4.0]`, applied to the pooled prior |
| `federation-prior` | Pooled behaviour of the workload class across the Federation, contributing a mean but **zero evidence** |
| `none` | The ranker declines and existing scheduler behaviour stands |

## Exploration

A deterministic hash of `(exploration_seed, job_id)` selects at most
`exploration_ratio` (default 0.1, hard-capped at 0.5) of decisions as
exploration slots. On those slots candidates are compared by
`normalised_cost - bonus`, where
`bonus = exploration_bonus * sqrt(ln(total_evidence + 1) / (evidence + 1))`.

This is a contextual multi-armed bandit in its simplest auditable form: the
context is the workload class, each eligible provider is an arm, and the reward
model is the stored statistics. Because the slot is derived from job identity
rather than a global random source, a replayed scheduling pass makes the same
choice, and `exploration_ratio = 0` makes ranking fully deterministic.

## Dynamic behaviour and staleness

- Recency weighting through the configurable half-life (default seven days).
- Version segmentation: model, software and runtime version are part of the
  environment fingerprint, so an upgrade starts a new profile and pre-upgrade
  evidence is only reusable at a demoted tier and reduced weight — or not at
  all with `reuse_cross_environment_evidence=False`.
- Unavailable nodes are removed by the existing hard constraints, not by
  learning.
- New nodes and new providers are covered by the exploration budget.
- `expire_stale_observations` drops history beyond
  `max_observation_age_seconds` and rebuilds profiles.
- Live queue depth and utilisation come from the current provider report, never
  from history.

## Federation learning

This is **not** federated machine learning. No model is exchanged and no user
data leaves a node — only bounded scalar facts about how long the Federation's
own work took.

The executing node builds the observation locally, because it is the only party
that can measure its own CPU, GPU and memory use, and appends a
`capability.execution.observed` event to the authoritative session event log.
The current operational leader validates and applies accepted events and is the
sole writer of derived scheduling state, which is also where scheduling
happens. No new transport, no new authority.

## Safety

- The learning component never modifies source, never executes generated
  commands, and holds no authority. Its entire effect is the order of an
  already-eligible candidate list.
- `enforce_observation_authority` requires `payload.node_id ==` the
  authenticated actor and the same rule for the embedded observation, mirroring
  the existing human-auth event rule. A member cannot report about another node.
- Ingest additionally checks session binding, clock skew and maximum age, and
  the store deduplicates by `(session, job, attempt)` so one lucky execution
  cannot be replayed into a reputation.
- All telemetry is range-bounded and screened for secrets and non-public
  locations.
- A ranker can only permute eligible candidates; it can never resurrect a
  provider rejected by a hard constraint.
- Corrupt or unavailable learned state degrades to a lower evidence tier or to
  the existing deterministic scheduler, never to an error.

## Persistence

`capabilities.execution_efficiency` schema version 1, created through
`ensure_sqlite_schema` with an explicit migration step, a legacy-version
detector and a post-migration column validator.

- `capability_execution_observations` — append-only raw truth, unique on
  `(session_id, job_id, attempt_id)`, bounded by a retention cap.
- `capability_execution_profiles` — the derived cache;
  `rebuild_profiles()` regenerates it entirely from observations.
- `capability_scheduling_decisions` — a bounded window of recent decisions.

## Explainability

Each learned decision produces a `fcp.scheduling-decision.v1` record holding the
objective, workload class, every candidate considered with its tier, evidence,
observation count, predicted metrics and cost, the providers rejected by hard
constraints and why, whether the decision was an exploration probe, and a
human-readable explanation such as:

> `balanced-node` was selected for
> `language-model/classify/[input_bytes=2^10,model=qwen3:4b]`
> [fastest-completion]: 182 comparable executions averaged 0.9 s at a 97%
> success rate (measured-exact evidence), versus 1.1 s over 14 executions on
> `cpu-node` at 87%.

## Operator surface

`GET /federation/efficiency.json` on the existing Federation blueprint returns
the learning snapshot: observation count, per-node counts and success rates,
learned per-node/workload performance with evidence, current preferred nodes,
and recent scheduling decisions. It inherits the existing `federation.read`
permission and returns an explicit unavailable snapshot when learning is not
configured on the device. No dashboard is introduced in this change.

## Evidence

`catalog/capabilities/tests/test_efficiency_learning_simulation.py` runs 600
jobs across three workload classes on four heterogeneous simulated nodes where
each node wins a different class and the fastest-looking node fails half the
time. Measured against the same seeded job sequence:

| Scheduler | Wall-clock cost |
| --- | --- |
| Existing default | 4 062 s |
| Learned | 1 851 s |
| Perfectly informed oracle | 1 501 s |

The share of jobs sent to the correct node rises from 32% over the first 50
decisions to 97% over the last 150, and the unreliable node never becomes a
preference.

## Not included

- Wiring the recorder into the production dispatch path on every node, and
  appending observation events to the live session log, are follow-up steps;
  the contracts, authority checks and ingest are in place and tested.
- No dashboard, no energy telemetry source, no regression-based estimator.
