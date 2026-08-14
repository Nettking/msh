# Federation execution efficiency learning

| Field | Value |
| --- | --- |
| Classification | Maintained implementation reference |
| Scope | Learn provider efficiency from real durable federation executions and use it for future placement |
| Authority changes | None — learning ranks already-eligible candidates only |

## Goal

Federation devices are heterogeneous, and the best provider depends on the workload. Static capacity ranking cannot improve from experience. This layer therefore retains bounded execution evidence and uses it to rank compatible providers for later comparable jobs.

It is deliberately **not** a second scheduler and not an RL system. Existing F7/F8 contracts continue to own eligibility, trusted provider activation, job ownership, leases, retries, dispatch, artifact access and result authority.

## Production boundary

The current durable F7 product workload is federated JSONL analysis. That path is wired end to end:

```text
AnalysisWorkSlice
        ↓
durable JobContract
        ↓
hard provider eligibility
        ↓
LearnedProviderRanker
        ↓
F7 ownership / lease / artifact grant
        ↓
existing LifecycleTransport
        ↓
LearningLifecycleTransport
        ↓
worker execution
        ↓
validated DispatchResponse
        ↓
ExecutionObservation
        ↓
SQLiteExecutionLearningStore
        ↓
profiles → next provider decision
```

Initial analysis placement uses learned ranking. Retry placement still goes through the existing F7.5 reassignment authority; learning supplies a preference rather than duplicating claim/fencing logic.

The older direct `LanguageModelRuntime` is not the durable F7.5 analysis lifecycle. The generic efficiency contracts can support that runtime later, but this implementation does not claim that direct AI invocations already contribute measured observations to the durable analysis learning loop.

## Reused components

- `JobContract` / `CapabilityRequirement` for work identity and hard requirements.
- `ProviderResourceReport` / `ProviderSelectionPolicy` for live provider state and operator constraints.
- `select_provider` as the hard-eligibility boundary and optional learned-ranking seam.
- F7.3/F7.5 job lifecycle for ownership, leases, retries and terminal result authority.
- Existing dispatch contracts for authenticated request/response identity and execution-state timestamps.
- F7.6 artifact authorization unchanged.
- Existing benchmark results as cold-start evidence.
- Existing SQLite schema migration helper and telemetry redaction rules.

## Package

`catalog/capabilities/efficiency/` contains:

| Module | Responsibility |
| --- | --- |
| `contracts.py` | workload, environment, measurements and execution-observation schemas |
| `profiles.py` | recency-weighted sufficient statistics and estimates |
| `policy.py` | scheduling objectives and costs |
| `store.py` | retained observations, rebuildable profiles and decision history |
| `estimator.py` | measured and cold-start evidence hierarchy |
| `ranking.py` | learned candidate ordering and explanations |
| `recorder.py` | dispatch response → execution observation |
| `runtime.py` | production ranker/recorder composition around the existing lifecycle transport |
| `federation.py` | validated federation-event contract for future member-published measurements |
| `projection.py` | bounded operator read model |

## Authority and failure boundary

Hard eligibility is evaluated before learning. The ranker receives only already-eligible candidates and may return only a permutation of that exact set. A malformed or failing ranker falls back to the existing deterministic order.

`preferred_node_id` remains an operator constraint. Learned ordering is applied inside that preference grouping. Decision persistence happens only after the final provider is known, so the stored selected provider matches the provider actually returned by `select_provider`.

Learning never grants ownership, issues artifact grants, executes generated commands, weakens provider enrollment, or commits an authoritative job result.

## Observations

`fcp.execution-observation.v1` stores bounded execution evidence:

- federation session, job and attempt identity;
- executing node and capability identity;
- workload descriptor and requirements fingerprint;
- execution environment fingerprint;
- success/failure;
- available timing/resource/quality measurements.

Observation identity is attempt-scoped, so a duplicate dispatch/replay cannot teach the same durable attempt twice.

### Production timing semantics

The analysis lifecycle response proves accepted/running/terminal timestamps, so the production adapter can learn execution duration, terminal outcome and bounded worker-reported metrics.

`DispatchRequest.sent_at` is **not** the durable queue-entry time. The adapter therefore leaves queue latency unknown instead of learning a fabricated value from transport timing. A future durable queue timestamp can add that measurement without changing the observation schema.

## Workload similarity

One `WorkloadDescriptor` produces progressively coarser class keys: exact, family, job kind and capability. Numeric features are bucketed so nearby workload sizes can share evidence without being declared identical.

The contracts are capability-generic; they are not language-model-specific.

## Profiles and time decay

A profile is keyed by:

```text
(capability_type, workload_class, node_id, capability_id, environment_fingerprint)
```

Profiles retain decayed sufficient statistics rather than an opaque score. The policy chooses which measurement matters for the current scheduling objective.

`DecayedStatistic.update` is arrival-order independent: a new observation decays existing state forward, while an older delayed observation is itself decayed forward to the existing time anchor. Chronological replay and delayed delivery therefore converge to the same logical statistic.

The store additionally keeps raw observations as the source of truth. Profile caches are rebuildable. When retention deletes observations, profiles are rebuilt transactionally so forgotten samples cannot continue influencing rankings.

Production analysis stores are isolated by federation session.

## Cold start and staleness

Evidence falls through this hierarchy:

1. exact measured workload/node/environment;
2. measured similar workload;
3. compatible historical environment at reduced weight when allowed;
4. federation benchmark evidence;
5. bounded advertised hardware heuristic;
6. pooled federation prior;
7. no learned opinion → existing deterministic scheduler order.

Software/model/runtime identity participates in the environment fingerprint, so upgrades do not silently inherit old evidence as current exact evidence.

## Exploration

A deterministic hash of `(exploration_seed, job_id)` assigns a bounded fraction of jobs to an optimistic exploration comparison. Exploration can only reorder eligible candidates, is reproducible, is capped by policy, and can be disabled with `exploration_ratio = 0`.

## Production recording

`LearningLifecycleTransport` is a decorator around the existing lifecycle transport, not a second transport. It delegates the real request, validates the returned `DispatchResponse` against the outgoing `DispatchRequest`, then records terminal execution evidence best-effort.

A learning-store failure never changes the dispatch response or job result.

The separate `capability.execution.observed` federation event contract remains available for future measurements that only the executing member can know. This implementation does **not** claim that every live node currently publishes such events.

## Operator surface

`GET /federation/efficiency.json` exposes a bounded read-only snapshot containing observation counts, node summaries, learned profiles, current evidence preferences and recent decisions.

In the supported Flask product the route resolves the learning store from the same authenticated, session-bound `AnalysisRuntime` used by scheduling. It does not open a parallel learning database. Tests and alternative compositions may explicitly inject a store through `FEDERATION_EFFICIENCY_LEARNING_STORE`.

If there is no trusted federation context or no usable store, the endpoint returns an explicit unavailable snapshot rather than failing the Federation page.

## Scheduling objectives

The policy supports fastest completion, lowest queue time, highest reliability, lowest resource cost, maximum throughput, balanced utilisation, preferred-local and energy efficiency.

Support does not imply that every execution adapter measures every metric. An objective becomes evidence-based only when the required measurement is present; otherwise estimation falls through to other evidence or the existing scheduler ordering.

## Verification requirements

The dedicated CI gate runs on Linux and Windows and covers:

- contracts, persistence, ranking, recorder and simulation;
- final stored decision matching operator-preferred selection;
- order-independent time decay and delayed-delivery/rebuild equivalence;
- retention/profile consistency;
- authenticated lifecycle response producing one observation;
- duplicate durable-attempt replay not producing duplicate learning;
- production analysis runtime/discovery integration;
- existing capability and AI scheduling behaviour without a ranker;
- Ruff on the capability domain and production integration file.

## Deferred work

- Direct `LanguageModelRuntime` invocations do not yet feed measured observations into this durable-job learning loop.
- True queue latency requires a durable queue-entry timestamp; it is intentionally not inferred from dispatch time.
- Node-local CPU/GPU/energy measurements can use the validated federation observation-event contract when a production publisher is added.
- No dashboard, regression model or RL estimator is introduced here.
