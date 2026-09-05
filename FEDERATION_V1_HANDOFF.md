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

## FAST-RUNNER VALIDATION OF THE FROZEN CANDIDATE — the primary release evidence

Executed by Claude under operator instruction, 2026-09-05T21:45Z-22:03Z.
ACTING_ENGINEER in this file remains Astra and is untouched; this section
reports. Astra holds the release, merge and physical-acceptance decisions.

    FAST_LINUX_VALIDATION:   PASS
    FAST_WINDOWS_VALIDATION: PASS on the existing self-hosted Windows gate;
                             INCOMPLETE on Beast-Windows (host has no Python)
    NITRO_SLOW_HOST_STRESS:  RUNNING

VALIDATED_CANDIDATE_SHA: ba8a3b0b828f59c36c5aaaf6130480a2432a5578 (FROZEN, asserted in every job before anything ran)
MAIN_SHA: 6101c86d94294c70db47d1a8053cac93b9a41356
HARNESS: ci/fast-linux-validation at e81bf4d5, run 33994338144; ci/fast-windows-validation at dcd3fca4, run 33994714769.

### Nettking-Linux, 20 cores, docker 29.1.3, python:3.12.13-bookworm verified

| job | result | duration |
| --- | --- | --- |
| Release static checks 101382063256 | **PASS, every step** | 90 s |
| Targeted repeats of the five Nitro failures 101381346930 | **0 of 20 failed** | 107 s |
| Targeted repeats, second independent run 101382063264 | **0 of 20 failed** | 112 s |
| **Full candidate suite, default order 101382542624** | **3633 passed, 30 skipped, 452 warnings** | **288.01 s (4:48)** |

The static set is the gate's own: storage/host-resource precondition, compileall
over catalog, the acceptance-manifest assertions, ruff across its exact 46-path
selection ("All checks passed!"), the product branding boundary, the Go direct
peer sidecar tests, Compose configuration, and diff hygiene. The full-suite job
also passed "Repository stays clean after the suite", so the suite left nothing
behind. Per-repeat timing in the targeted legs was 1.84-3.04 s for 8 tests.

THE NUMBER THAT MATTERS FOR CI003/CI004: 3633 passed / 30 skipped is exactly what
the same commit produces off-runner, and the five tests that failed on Nitro run
in about 2 s each here against roughly 52 s there. The full suite is 4m48s here
against 56-129 minutes on Nitro. Nothing was retimed, skipped or weakened to get
this.

### Windows

Windows on this exact candidate ALREADY PASSED on the existing self-hosted gate:
run 33975032244 job 101330241662, 871 passed / 1 skipped in the capability and
product subset, 367 passed / 1 skipped in transport/storage/failover, plus Go,
ruff, Compose and diff hygiene. That is the Windows release evidence.

Beast-Windows was attempted as independent confirmation and is BLOCKED, not
failing: the host has git and docker but **no python, no py and no go** (42.7 GB
free on C), and actions/setup-python@v5 did not put an interpreter on PATH, so
all ten targeted repeats reported "'python' is not recognized". Installing an
interpreter on that host is a host change and was not made. This says nothing
about the candidate.

### Beast-Linux never accepted a job

Four runs, ~30 minutes, both `beast-linux` and `[self-hosted, beast-linux]`:
runner_id 0, no runner assigned, every time. Beast-**Windows** accepts jobs on
the identical `[self-hosted, ...]` pattern and reports machine name BEAST, so the
job definitions are not the problem; the Beast-Linux registration, labels or
runner group needs checking on the host. No runner infrastructure was touched.

### Eight environment defects, all class C, none the candidate's

Recorded because each one produced a red job that says nothing about the product,
and the next person will hit them: setup-go@v5 with no version on a Go-less
runner runs a bare `version` and dies; `bash -lc` in the golang image is a login
shell and discards the image's PATH so `go` vanishes; `check_product_branding.py`
shells out to git, which refuses a uid-1001 repository from a root container
("dubious ownership"); the Compose plugin is absent on Nettking-Linux; a root
container leaves root-owned files that the next actions/checkout cannot clean;
Beast-Windows has no pwsh; its Windows PowerShell refuses to load .ps1 at all
("running scripts is disabled on this system"); and it has no Python. The
PowerShell policy and the missing interpreter were worked around or reported
rather than fixed, because both would mean changing host configuration.

### What this does and does not settle

It settles that the frozen candidate passes a complete Linux release gate on a
capable host, in under five minutes, reproducibly on the targeted set across two
independent runs. Combined with the existing Windows PASS, the software evidence
for ba8a3b0 is now positive on both platforms.

It does NOT settle the Nitro failures' root cause. Both trees passed the targeted
A/B on Nitro at the same disk saturation, and no candidate-versus-main difference
has ever been observed on any host; the Nitro-only failures remain wall-clock
timeouts on a two-core rotating-disk machine, still classified and still without
a proven mechanism. No product change is justified by anything measured tonight.

### Exact recommended next action for Astra

1. Decide whether Linux release evidence on Nettking-Linux is acceptable in place
   of Nitro for the gate, given Nitro is now classified slow-host/stress. This is
   an executive call and has not been made here.
2. If a second consecutive clean full suite is wanted before progressing, it costs
   five minutes on Nettking-Linux, not two hours.
3. Beast-Linux registration and Beast-Windows Python are operator items.
4. Physical acceptance has still NOT run on this candidate. Nothing merged,
   nothing deployed, Beast unchanged as AI_PROVIDER_ONLY.

## Nitro A/B harness: published, running, and open for Astra to adopt or cancel

Written and started by Claude under direct operator instruction at 19:53Z.
ACTING_ENGINEER in this file stays Astra and has not been touched; Astra claims
control of A/B execution, so this section reports rather than assumes. Cancel
the run if it conflicts with a leg sequence Astra has already begun.

HARNESS_BRANCH: `ci/nitro-ab-candidate-vs-main` at `a0f9e756d0e70bfcd5e8c3fe50500067361d63c4`.
It is a diagnostic identity, deliberately separate from both the product
candidate and `ci/self-hosted-pr435`; it contains one workflow file and no
product change.
AB_RUN: 33988447252, three jobs on `fcp-linux`, chained with `needs` so their
order is deterministic. `targeted` (120 min), then `pair_ab` (600 min), then
`pair_ba` (600 min).

LEGS. `targeted` runs the five failing tests 30 times against the candidate and
then 30 times against main -- minutes, not hours, and it answers whether the
failures reproduce at all before four full suites are spent. `pair_ab` runs the
full default-order suite candidate-then-main; `pair_ba` runs it
main-then-candidate. Counterbalancing is the point: cache warmth, page cache,
disk state and accumulated host state all drift one way across a long run, and
running each tree in both positions is what stops that drift from reading as a
branch effect.

HELD IDENTICAL BY CONSTRUCTION: one clone checked out to each SHA in turn with
`git checkout --detach` plus `git clean -xdff`, HEAD asserted against the
expected SHA before anything runs; the same `python:3.12.13-bookworm` container;
`pip==26.2.1` then `-r requirements.txt -c constraints-release.txt` then
`pytest==9.1.1` (requirements.txt, constraints-release.txt, pytest.ini and
conftest.py are byte-identical across the two commits, so the environment is
equal by construction rather than by assertion); the same
`python -m pytest -o addopts= -o cache_dir=/tmp/pytest_cache -p no:randomly -v --durations=50`;
the product's own storage preflight; and a shared pip cache so download
variance stays out of the measurement. Collection size is the one thing that
cannot be equal: 3663 on the candidate against 3645 on main, because the
candidate adds its own tests.

TELEMETRY, every 30 seconds for the length of each suite: load and process
counts, MemTotal/MemAvailable/SwapTotal/SwapFree/Dirty/Writeback, per-device
reads, writes, queue depth and both I/O time counters, free bytes and free
inodes, live pytest processes, running container count. Printed after each
phase whether it passed or failed, so a timeout can be lined up against what
the host was doing that minute.

CLEAN STATE, before and after every phase: orphan pytest processes, every
container, listening TCP ports, host test scratch, disk and inodes, and recent
kernel errors are all printed. The only thing removed is `git clean -xdff`
inside the job's own clone and the job's own telemetry file, and every removal
is logged. The harness contains no `docker stop`, `rm`, `kill`, `prune`,
`restart` or volume command and writes nothing under the rig's data roots --
verified by grep over the workflow before it was pushed. The diagnosis must not
disturb the thing being diagnosed.

DELTA AGAINST NEXT_ACTIONS 3. That list asks for candidate default, main
default, candidate seed 20260813, main seed 20260813, then two consecutive
candidate clean-checkout runs at seed 15. This run covers the two default-order
legs in both positions plus the targeted repeats. The seeded legs are NOT in it
and remain to be added; the harness takes the leg as a parameter, so adding
them is a workflow edit, not a rewrite.

### AB001 targeted leg: COMPLETE, and it rules out the simplest explanation

Job 101366217178 finished 20:51:57Z. Both phases asserted their HEAD before
running and both reported their collection: 3663 on the candidate, 3645 on main,
matching the counts measured independently off-runner.

RESULT, and it is symmetric:

| leg | SHA | repeats | failures | per-repeat |
| --- | --- | --- | --- | --- |
| t1-candidate | ba8a3b0 | 30 | **0** | 8 passed, 51-55 s |
| t2-main | 6101c86 | 30 | **0** | 8 passed, 51-53 s |

Sixty consecutive runs of the exact five failing tests, on the exact runner that
failed them, against both trees. Not one failure, and no timing separation
between the trees.

By the operator's rule this is the "both pass" branch: the earlier failures stay
INTERMITTENT, the gate is NOT approved, and further reproduction is required.
Nothing here excuses the red run.

WHAT IT RULES OUT, and this corrects the emphasis of Claude's own earlier
entries. The telemetry during these sixty clean repeats reads:

| leg | sda busy mean | median | max | MemTotal | MemAvailable mean | SwapFree mean | load mean |
| --- | --- | --- | --- | --- | --- | --- | --- |
| t1-candidate | 82.9% | 84.3% | 90.5% | 3.26 GiB | 2.00 GiB | 3.10 GiB | 2.11 |
| t2-main | 82.1% | 85.0% | 89.9% | 3.26 GiB | 2.00 GiB | 3.10 GiB | 2.09 |

That is the SAME disk saturation, within a percentage point, as the 83.4% mean
measured during the Linux run that failed. So ~83% sustained busy on this
rotational disk is Nitro's ordinary working state under this workload, and on
its own it does NOT produce the failures. Sixty repeats prove that. "The host is
saturated, therefore the tests time out" is not a sufficient explanation and
should stop being offered as one -- Claude's included.

Memory is likewise steady and unremarkable: 2.00 GiB available of 3.26 total,
with essentially no swap consumed during these legs, which is consistent with
Astra's independent observation and with Astra's rejection of the earlier
"memory-starved" framing.

WHAT REMAINS. The failures need something the isolated repeats do not have, and
the obvious candidates are properties of the full suite rather than of the host
baseline: state accumulated across 3600+ tests, memory pressure late in a long
process, page-cache eviction, or concurrent fixtures competing for the same
spindle. The full-suite legs now running are what can show this; the targeted
leg has done its job, which was to be cheap and to eliminate a hypothesis.

AB002 pair_ab (full suite candidate then main) started 20:52:14Z as job
101374058563. Note for whoever reads the run: a phase's step can show green even
when the phase failed, because phases record their rc and continue on purpose --
main's result is worthless if a failing candidate leg aborts the sequence. Read
`===== PHASE <name> RESULT rc=N =====` in the log, not the step colour. The
already-written next revision of the harness moves that verdict into the summary
step so the job itself turns red; it is committed but deliberately unpushed,
because pushing that branch cancels the run in flight.

## Two corrections to Claude's earlier entries, both of which Astra was right about

MEMORY. Claude described Nitro as "memory-starved" from MemAvailable readings
of 1.70-2.13 GiB. That was wrong, and Astra's fresh read-only observation says
why: the host has roughly 3.5 GB of RAM in total, so about 2 GB available is a
comfortable majority free, not starvation. The disk-busy figure stands on its
own measurement; the memory claim does not, and is withdrawn. What the fresh
observation adds is more useful than what it removes: the volume is a
ROTATIONAL HDD, which makes sustained high busy time a plausible capacity limit
for an fsync-heavy suite rather than evidence that something else is competing
for the spindle.

DIFF EXCLUSION. Claude wrote that the candidate was "excluded by diff" because
every failing test file and the modules under test are byte-identical to main.
Astra narrowed that correctly: byte-identical files do not exclude an indirect
effect reaching those paths through the code the candidate does change. The
byte-identity is still a fact and still worth having, but it is evidence, not a
proof of exclusion, and the categorical phrasing is withdrawn. The same-runner
A/B above is precisely what can settle it, which is why it is running.

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
