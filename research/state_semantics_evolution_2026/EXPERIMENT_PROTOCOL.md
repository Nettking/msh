# Controlled operational-state evolution protocol

Date: 2026-11-09. Source implementation: `cc80a75727bf3ea0ee3288803d8dfda60392aa25` on `research/state-semantics-evolution-2026`. This protocol is written after code/schema inspection and the initial 16-test validation, but **before historical transformation outcomes are executed**. It is an internal prospective protocol, not an externally preregistered study.

## Question and scope

For the existing FCP `DigitalTwinService`, which controlled changes preserve active/non-active/unclassified assessments, which cause bounded degradation, and which intentionally change their meaning? Assess software behavior, not physical correctness, stable/unstable cutting, fault detection, or true downtime. No independent physical labels are available.

Use the committed implementation, shared JSONL reader policy, and production classifier. Production files, recorder, services, telemetry, existing local/remote checkouts and the `view-paper` manuscript remain unchanged. Use a separate Nitro checkout and venv. Raw data and temporary replay files stay on Nitro. Export only hashes, pseudonyms, timestamps, row ordinals, classifications and aggregate measurements.

## Sampling and frozen inputs

Use the verified four-source October 5-6 historical subset. It contains the first 1,000 sequence batches per source; this is a convenience subset, not complete days or a random sample. Stream the compressed archive without extracting it. Inventory every JSONL member, count schema/parse exclusions, and hash archive/member bytes. Canonical input is the existing wide snapshot schema; do not reconstruct missing fields or forward-fill signals in research code.

Globally sort each machine's records by parsed UTC timestamp, then member name and physical line number. This deterministic tie order is an experimental convention, not proof of real causal order. Partition into consecutive blocks targeting 200 records, retaining complete timestamp groups and any shorter final block with positive elapsed time. Exclude blocks without positive elapsed time. Select six disjoint blocks per machine using a deterministic condition-coverage heuristic, followed by temporal spread, for a target of 24 windows. Report any shortfall. Conditions include reported active/non-active, absent/unavailable execution, fallback, transitions, gaps, duplicate timestamps and fallback speed in (100,150] rpm. Retain startup/poor-coverage windows and disclose quality instead of excluding them to improve agreement.

Freeze and write `DATASET_MANIFEST.json` **before** running transformations. Record selected window hashes, UTC boundaries, anonymized machine IDs, source member hashes/line references, archive hash, source revision, software commit, and sampling policy. Every scenario receives exactly the same original windows; perturbations remain in memory or temporary files. No overlapping timestamp groups from the same machine may occur in selected windows. A 200-record target is not a fixed duration or an independent replication.

## Baselines and hypotheses

**B0:** unchanged `catalog/flask_app/tests/test_digital_twin_service.py`. Run before and after research changes. These six existing tests exercise production behavior, not the new research adapters. Defects outside their execution scope are reported as such, not as evidence of inferior conventional methods.

**B1:** ordinary regression, differential and metamorphic testing using these same windows, transformations, expected masks and temporal conventions. Compare row states, service duration aggregates, independently accounted unclassified/excluded time, and observable transition boundaries. Expected-change specifications are available to B1.

**B2:** omit unless a concrete limitation remains after equal-information B1. Traceability and explicit expectations alone do not create a new testing method. Do not manufacture a comparison by withholding requirements from B1.

| Scenario | Prospective expectation / oracle |
|---|---|
| E1 field rename + adapter | Row classifications, durations, coverage and transition boundaries equal baseline. |
| E2 rpm to rad/s + inverse adapter | Same invariants, including strict 100 rpm boundary. Invalid numeric inputs remain missing. Roundoff-induced boundary differences are genuine failed preservation claims, not silently tolerated state changes. |
| E3 equivalent provider schema + adapter | Same invariants. This is a schema/interface simulation; no actual device/provider is replaced. |
| E4 execution removed every tenth source row | Only selected row classifications may differ; durations can change on their outgoing usable intervals. Report coverage and changes. No global preservation claim. |
| E5a duplicate + shuffled delivery | Preserve order within each timestamp tie group, add an identical record, and preserve observable durations/transitions. Compare original rows by identity, not zip after shuffle. |
| E5 conflicting-tie probe | Arbitrary tie reorder has no unconditional preservation contract. Report any sensitivity as ambiguity in ordering assumptions; do not count it as a false positive from a valid invariant. |
| E5b inserted telemetry gap | Shift latter half by 60 seconds; the new >30-second interval is excluded. Its absence is never added to non-active time. Quantify removed usable time, unknown time, shifted boundaries and gap duration. |
| E6 intended threshold >100 to >150 rpm | Execute an alternate research-local callable derived from the production function, changing only this threshold. Differences must be exactly fallback rows with 100<rpm<=150 and load not >1. Explicit execution states and unknown explicit values retain precedence. Measure evolved durations and transitions. |

## Controlled defects and decision denominators

Inject specific incorrect adapters/policies, including rad/s consumed as rpm and omitted adapter fields, as defined in `metrics.py`. Use the preservation expectation for each defective meaning-preserving transformation. Unknown-as-idle is a separate deliberately invalid policy. Record whether each defect is activated by a behavioral difference, then detected by B1. Aggregate both defect types and window-defect trials. A non-activated defect is not an activated miss. These selected mutants are sensitivity witnesses, not a sample of all real defects.

False positives mean rejection of a **valid declared expectation**, including E4/E6 expected changes and E5b exclusions. Misses mean an activated injected defect that B1 accepts. Report unexpected valid-control failures separately from injected defect counts. A valid implementation or preserved mutant with no activation does not establish physical correctness.

## Temporal and numerical measurement conventions

Reuse production stable timestamp ordering and left-endpoint interval ownership. Intervals in [0.1,30] seconds are usable; longer gaps and shorter positive intervals are excluded. Zero-length duplicate intervals contribute no elapsed time. Count unclassified duration only on usable intervals with an unclassified left endpoint. Class coverage is classified usable time divided by usable time; wall-span coverage is classified time divided by positive elapsed span. Neither is the production service's `continuity` ratio (usable/positive interval counts). Last observations receive no extrapolated duration. Transition boundaries describe changes supported by usable intervals under the chosen ordering; no physical transition within a gap is inferred.

Report exact row counts and pooled agreement (differences/compared rows), per-window metrics and range, active/non-active/unclassified duration deltas, excluded long/short intervals, transition additions/removals, and source row references for each difference. Continuous numeric aggregate comparison tolerates normal floating arithmetic (1e-8 absolute / 1e-9 relative); state labels and expected masks do not use numeric tolerance.

Measure monotonic elapsed time, process CPU time and peak resident memory on Nitro, with platform and dependency versions. These are one-host descriptive costs, not capacity or scalability qualification. Record all executed test commands, counts, exit codes, source hashes and commits. Run full relevant production/research tests before historical evaluation and again after fixes. CI, if used, retains the self-hosted Linux pool and never accesses confidential data.

## Interpretation and stopping rules

Investigate failed expectations without rewriting expected outcomes to obtain green tests. Fix confirmed harness defects, preserve failure evidence, repeat tests and re-run historical scenarios when needed. Treat uncovered data conditions as limitations and use labeled synthetic witnesses separately. If access/dependencies fail, report the precise blocker and complete independent work; never claim remote execution without logs.

A successful B1 result supports bounded engineering validation and RQ3 evidence about this modular implementation. It only narrowly informs RQ1's state/uncertainty semantics. Novel method claims require incremental evidence beyond established metamorphic/regression techniques. See `SCIENTIFIC_ASSESSMENT.md` for primary literature and claim limits.
