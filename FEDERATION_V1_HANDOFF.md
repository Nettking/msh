# Federation v1 authoritative handoff

Coordination only: `coord/federation-v1-release`. NEVER merge this branch into main. Product candidate and diagnostic harness commits are separate identities.

## Authoritative state

EXECUTIVE_OWNER: Astra
ACTING_ENGINEER: Astra; single writer of this handoff and controller of Nitro A/B execution. Claude's active remote session and published contributions are acknowledged.
CURRENT_PHASE: Diagnose failed software gates and run controlled full-suite candidate/main A/B. Physical acceptance awaits software gates.
CURRENT_MAIN_SHA: 6101c86d94294c70db47d1a8053cac93b9a41356
CURRENT_CANDIDATE_SHA: ba8a3b0b828f59c36c5aaaf6130480a2432a5578 — FROZEN
PR_435_HEAD: ba8a3b0b828f59c36c5aaaf6130480a2432a5578 on claude/federation-recorder-capability-id-19tqkk; OPEN, unmerged
ACTIVE_FIX_BRANCH: None. R001 f65d11fd028a8eea5478e2fd6dd634fdbdb9d9a1 and branding-only ba8a3b0b are already in the frozen candidate.
VALIDATION_BRANCH: ci/self-hosted-pr435 at fffba1c06cf37f1f5b7c15129bc4900174a7a6e3
ACTIVE_CI_RUN: 33975032244 — completed FAILURE. Windows101330241662 SUCCESS; PostgreSQL101330241653 SUCCESS; clean-checkout101330241502 FAILURE; Linux release101330241713 FAILURE; verdict101359577396 FAILURE.
DEPLOYED_NETTKING_SHA: 6101c86d94294c70db47d1a8053cac93b9a41356, source and no.fcp.build_commit labels observed 2026-09-05 14:00–14:02Z
DEPLOYED_NITRO_SHA: 6101c86d94294c70db47d1a8053cac93b9a41356, source and own-component build labels observed 14:00–14:02Z; recorder exited0
DEPLOYED_MSH_RECORDER_SHA: 6101c86d94294c70db47d1a8053cac93b9a41356, source and own-component build labels observed 14:00–14:02Z
CURRENT_SESSION_ID: UNKNOWN; no fresh authenticated Federation state query completed
LAST_UPDATED_UTC: 2026-09-05T19:33:00Z

## Failures and classification

KNOWN_FAILURES:
- Linux release job101330241713 failed step8, `Python 3.12.13 release regression suite in Docker`. It completed the full default-order suite: 1 failed,3632 passed,30 skipped,452 warnings in3970.45s. Failed test: `catalog/mtconnect_recorder/tests/test_source_availability_retry.py::test_first_real_data_is_durably_written_with_its_raw_manifest`, reported18:54:34Z at90%. The helper at line111 raised `Failed: recorder capture did not finish within the test deadline`, with timeout2.0s. Raw-manifest assertions were not reached. Captured teardown logged a commit of sequences1–3; that does not prove the later unexecuted assertions.
- Clean-checkout job101330241502 passed checkout, exact-SHA assertion, dependency/setup and storage preconditions. Its first shuffled full suite (seed20260813) failed: 4 failed,3629 passed,30 skipped,460 warnings in7740.80s. Second seed15 did not run. Failed nodes:
  - `catalog/mtconnect_recorder/tests/test_source_availability_retry.py::test_recorder_starts_recording_when_the_source_appears` — 2s capture helper deadline.
  - `catalog/relay/tests/test_phase2_integration.py::test_f2_006_revocation_rejects_traffic_and_reconnect_but_peer_survives` — 3s authentication receive timeout during setup.
  - `catalog/flask_app/tests/test_federation_pairing_relay.py::test_signed_pairing_code_projects_same_members_from_both_viewpoints` — 5s pairing deadline, pairing-relay-timeout.
  - `catalog/node/tests/test_live_storage_catchup.py::test_live_catchup_repairs_only_missing_batches_and_keeps_node_unassigned` — bootstrap timeout.
- Ownership cleanup succeeded in both jobs. Their post-suite repository-clean checks were skipped by default failure gating; clean after failure has not thereby been proven.
- Physical acceptance has not run on ba8a3b0b. Nitro recorder remains inactive; historical recorder identity collision and unavailable logical storage await controlled candidate validation.

FAILURE_CLASSIFICATIONS:
- CI004 Linux release: CLASSIFICATION PENDING. Observed test-helper deadline failure, distinct from the shuffled job. Setup completed; no job-budget cancellation. Host I/O contention, test fragility, suite state/accumulation and indirect candidate effects remain open until controlled Linux A/B.
- CI003 clean-checkout full suite: CLASSIFICATION PENDING. Four real timeout failures, not checkout/setup failure. Isolated Windows passes on candidate/main weaken a simple deterministic-regression hypothesis but cannot exclude Linux-, order-, load- or state-dependent defects.
- The failing test/runtime files are byte-identical main↔candidate, but this does NOT exclude indirect effects through changed node/client or reconciliation code. Prior categorical “candidate excluded by diff” and “not an order-independence defect” statements are withdrawn.
- ENV002: Linux job sampler showed high disk busy time during the suite (~83% mean across minute intervals). Causation is unproven. About1.7–2.1GiB MemAvailable alone does NOT establish memory starvation or CPU saturation. Fresh read-only Nitro19:30–19:32Z: 2CPU,~3.5GB totalRAM,~2.39GB available,~704MB swap used without active swap-in/out; measurable recent I/O pressure on rotational HDD, low CPU load. No cp or Runner.Worker process. Container kind_poitras runs a sleeping Python process; its block-I/O counters did not change in the sampled window. Do NOT stop/remove it based on the old assumed hung-cp narrative.
- CI001 hosted checks: GitHub's annotation reports account billing/spending-limit block before job startup. Distinct from self-hosted software failures; do not disable protections/checks.
- R001 reproduced retry-order defect is fixed in f65d11fd: active_client_id assigned only after successful reconciliation. Test failed before fix and passed after.
- R002 branding checker failure in PR's test identifiers fixed by mechanical ba8a3b0b rename; no assertion/fixture semantics changed.

## Evidence already obtained

COMPLETED_TESTS:
- Real Nettking Windows job101330241662 at ba8a3b0b:871 passed/1 skipped capability-product;367 passed/1 skipped transport-storage-failover; Go,Ruff,Compose,diff,storage preconditions passed.
- Real Nitro PostgreSQL job101330241653 SUCCESS:11 passed,0 skipped in8.75s against the healthy PostgreSQL service, exact ba8a3b0b asserted.
- R001 focused publication-recovery + real-relay identity suites:24 passed; meaningful red→green regression. Candidate history reviewed by Astra.
- Isolated four CI003 tests on Nettking Python3.12.10: candidate4 passed/5.97s, main4 passed/6.40s. Historical f043 run also4 passed; it is not evidence for current full-suite stability.
- Claude's reported local candidate full suite3633 passed/30 skipped in default and two shuffled orders, plus repeated recorder/reconnect coverage, are local prequalification—not Nitro or physical acceptance.
- Python preparation complete: official CPython NuGet3.12.10 x64; executable `C:\actions-runner\toolchains\python-3.12.10\python.exe`; pip/venv/imports/subprocess verified. Real runner jobs prove execution under NetworkService. Existing3.14 and globalPATH unchanged.
- Runtime inventory: all own component labels on threehosts6101c86; Nettking/Nitro source checkouts clean. MSH normal checkout has46 preserved untracked recorder JSONLs, no tracked edits; separate acceptance6101 checkout clean.

CURRENTLY_RUNNING_WORK:
- Astra prepares standalone diagnostic A/B tooling outside product checkouts and workflows; no A/B leg launched yet.
- Supporting agent owns outputs/nitro-ab harness implementation; root reviews and is sole remote execution controller.
- Supporting read-only Linux-log/resource audits are finishing their durable reports.
- Claude can continue read-only review or publish evidence on its separate diagnosis branch. Do not concurrently edit this handoff or launch Nitro workloads. An active remote Claude session is evidenced by repository commits; this desktop lacks a confirmed direct messaging channel.
- No product, workflow, live container, service, Federation or deployment changes in this resumption.

NEXT_ACTIONS:
1. Preserve frozen refs and obtain full job logs, duration progression, sampler evidence; keep CI003 and CI004 separate.
2. Review and publish diagnostic harness, recording its distinct SHA/hash and exact commands. Use fresh checkout for each leg, identical pinned Python image and dependencies within each mode, same Nitro account/runtime/storage/services and full test selection. Instrument CPU/load, memory/swap, pressure, disk counters, process/container state and per-test timing.
3. Run sequential candidate default, main default, candidate seed20260813, main seed20260813; then two consecutive candidate clean-checkout runs seed15 if no intervening change is needed. A failed candidate leg must not prevent the main control. Never reuse a prior leg's node/session/temp state.
4. Record all failures and setup/cleanup/cleanliness independently, including skipped later commands. Preserve raw evidence and failed scratch directories; remove only proven test-owned processes/containers.
5. Compare same-runner A/B, not Windows isolation. Do not increase TIMEOUT, skip/mark flaky/reduce coverage, tune storage policy or claim stability from one lucky pass. If a real code/test/workflow fix is needed, state exact root cause and narrow Claude task; establish a NEW candidate and qualify it afresh.
6. Software progression requires Linux release PASS and reproducible full clean-checkout PASS, preferably two consecutive candidate clean passes. Historical failed run stays failed; standalone diagnostic results are not silently relabeled GitHub gate success.
7. Only after software gates: controlled physical acceptance of exact candidate from clean checkouts on Nettking,Nitro,MSH Recorder. Record roles, initial/runtime/Federation state, supported startup commands, identity ownership, expected/observed results, reconnect/restart, cleanup/rollback and final state. Activate Nitro recorder only as part of that controlled test.
8. Do NOT merge PR435. User explicitly froze merge during diagnosis. Final review and physical evidence must make a later merge decision defensible.

## Authority and rotation

ACTIONS_CLAUDE_IS_AUTHORIZED_TO TAKE:
- Read-only diagnosis, source review and evidence preparation on claude/pr435-validation-diagnosis-n285av while Astra owns execution.
- On explicit cooldown transfer, fetch this branch first and claim ACTING_ENGINEER by normal push; continue the recorded A/B legs without duplication, collect results and maintain evidence.
- Formulate/implement only a bounded root-cause fix on a separate branch once evidence establishes necessity; product candidate promotion remains a separately recorded decision.
- No simultaneous writes to PR435, validation branch, coordination record or active Nitro harness. Publish useful commits/evidence; never leave key state only in chat.

ACTIONS_REQUIRING_ASTRA_REVIEW:
- Candidate or validation workflow changes, live rig startup/deployment, physical fault campaigns, persistent-state changes, final merge/release decision.
- No protection bypass, weakened test conditions or destructive cleanup is authorized.

MERGE_AUTHORIZATION: NONE. Do not merge435 or coordination branch.
BRANCH_OWNERSHIP: Astra owns this handoff and A/B runtime; PR435/ci/self-hosted-pr435 frozen. Claude owns its separate diagnosis branch for read-only findings. No force pushes.
ROTATION: Complete safe atomic work, push harness/evidence, record exact PID/container/run/leg and log paths, then explicitly transfer. Returning agent reads this handoff and validates actual refs first; do not rerun already valid evidence.

## Evidence locations

- PR435: https://github.com/Nettking/msh/pull/435
- Failed run: https://github.com/Nettking/msh/actions/runs/33975032244
- Previous full historical handoff preserved at coordination commit d856c043536cdf70e02b857b11f7e51d543b148b; its unqualified causal assertions are superseded by this record.
- Local task outputs: Federation-v1-runtime-inventory.md, Federation-v1-linux-release-diagnosis.md, Federation-v1-nitro-resource-baseline.md; final two produced during this resumption. Publish sanitized findings alongside A/B logs on coordination branch.
- Runtime proof uses no.fcp.build_commit (native recorder: native_runtime.build_commit), not source HEAD or container Running alone.
- Separate full physical requirements remain: P01–P12 and CF7 physical-evidence.v2; P07≥1 actual hour, P12≥24 actual hours, same candidate/host/run_id with valid in-window samples. Real MTConnect/Ollama/accelerator, desktop/mobile and multi-host observations remain required. No CI/simulated evidence substitutes.
- Private endpoints, credentials, pairing grants, SSH keys and complete environment dumps must stay out of committed evidence. Preserve all existing rig data and legacy projects.
