# Federation v1 authoritative handoff

Coordination only: `coord/federation-v1-release`. NEVER merge this branch into main. Product candidate and diagnostic harness commits are separate identities.

## Authoritative state

EXECUTIVE_OWNER: Astra
ACTING_ENGINEER: Astra; executive owner. PR435 final merge review COMPLETE and merge EXECUTED; authoritative merged-main qualification is now the open leg.
CURRENT_PHASE: Merged-main qualification INCOMPLETE pending static-cleanliness resolution and actual Compose. Run34025305598 completed: A/B/C, Windows and PostgreSQL PASS; D final cleanliness FAIL, E skipped. Claude's subsequent harness push already started34029983129; Astra monitors it without launching duplicates. No physical startup until qualification PASS.
CURRENT_MAIN_SHA: bcf5c9ab2fb453cb26129b70d41fb64fc4863dd4
MERGED_MAIN_SHA: bcf5c9ab2fb453cb26129b70d41fb64fc4863dd4 — merge commit; parents 6101c86d (base) and ba8a3b0b (candidate); tree bcbd778ba8009eb79e3349534e623da3861b0e45
PRE_MERGE_MAIN_SHA: 6101c86d94294c70db47d1a8053cac93b9a41356
MERGED_MAIN_SOFTWARE_QUALIFICATION: INCOMPLETE. Exact bcf5c9ab A/B/C, Windows and PostgreSQL passed in34025305598. D checks passed but its final cleanliness assertion failed; E skipped. No release/physical transition yet.
CURRENT_CANDIDATE_SHA: ba8a3b0b828f59c36c5aaaf6130480a2432a5578 — merged; retained as the qualified software identity
PR_435_HEAD: ba8a3b0b828f59c36c5aaaf6130480a2432a5578 on claude/federation-recorder-capability-id-19tqkk; MERGED 2026-09-06 as bcf5c9ab, normal merge commit with exact expected-head-SHA protection, no branch-protection bypass
ACTIVE_FIX_BRANCH: None. R001 f65d11fd028a8eea5478e2fd6dd634fdbdb9d9a1 and branding-only ba8a3b0b are already in the frozen candidate.
VALIDATION_BRANCH: claude/pr-435-final-merge-k01mfi at f5bded772dd70893416c1345c9e450b96633c6fe; diagnostic harness identity, all product checkouts pinnedbcf5c9ab.
ACTIVE_CI_RUN: 34029983129 — already started by Claude's f5bded77 workflow push at11:20:53Z. No Astra duplicate dispatched. Prior34025305598 completed FAILURE at D's post-check; its successful legs remain valid.
HISTORICAL_RELEASE_RUN: 33975032244 — completed FAILURE. Windows101330241662 SUCCESS; PostgreSQL101330241653 SUCCESS; clean-checkout101330241502 FAILURE; Linux release101330241713 FAILURE; verdict101359577396 FAILURE. These historical results are unchanged.
FAST_LINUX_VALIDATION: PASS — two successive default-order full candidate suites on Nettking-Linux, 40 targeted repeats independently verified, release/static checks plus actual Compose validation completed. This does not certify shuffled order independence or physical acceptance.
FAST_WINDOWS_VALIDATION: PASS — existing exact-candidate Nettking Windows gate reused. Optional Beast-Windows confirmation INCOMPLETE due to Python setup execution-policy failure before tests.
NITRO_SLOW_HOST_STRESS: FAIL — completed full-suite phase failures on both candidate and main; not RUNNING. Precise root cause remains INCONCLUSIVE.
DEPLOYED_NETTKING_SHA: 6101c86d94294c70db47d1a8053cac93b9a41356, source and no.fcp.build_commit labels observed 2026-09-05 14:00–14:02Z
DEPLOYED_NITRO_SHA: 6101c86d94294c70db47d1a8053cac93b9a41356, source and own-component build labels observed 14:00–14:02Z; recorder exited0
DEPLOYED_MSH_RECORDER_SHA: 6101c86d94294c70db47d1a8053cac93b9a41356, source and own-component build labels observed 14:00–14:02Z
CURRENT_SESSION_ID: UNKNOWN; no fresh authenticated Federation state query completed
LAST_UPDATED_UTC: 2026-09-06T07:16:15Z

## Astra live takeover — 2026-09-06 after run34025305598

Coordination source reconciliation: Opus addf77aa/c8cb43c were incorporated as d61df740/74861a0f; newer factual execution evidence from f5bded77 is reconciled here rather than copying stale handoff status. Astra remains executive owner and acting engineer. Local `work/msh-merged-validation` and `outputs/build-merged-qualification.py` are UNPUBLISHED, UNEXECUTED drafts superseded by the active remote harness. Do not push or launch them.

| Completed leg in34025305598 | Job | Exact merged-main result |
| --- | --- | --- |
| Nettking-Linux A default | 101474832429 | 3633 passed,30 skipped in273.84s; initial/final exact-SHA and clean checks PASS |
| Nettking-Linux B seed20260813 | 101475563810 | 3633 passed,30 skipped in224.32s; initial/final exact-SHA and clean checks PASS |
| Nettking-Linux C seed15 | 101476168715 | 3633 passed,30 skipped in216.61s; initial/final exact-SHA and clean checks PASS |
| Nettking Windows | 101474817915 | release matrix PASS, including367 passed,1 skipped transport/storage/failover in86.78s; exact SHA/Python3.12.10 asserted |
| PostgreSQL on Nitro | 101474818047 | 11 passed in3.37s against existing service configuration, Python3.12.13, exact SHA asserted |
| Nettking-Linux D | 101476759709 | compile/manifest/Ruff/branding/Go/diff PASS; final exact-SHA/cleanliness step FAIL (exit1) |
| Nettking-Linux E | 101476922743 | SKIPPED because D failed; no Compose PASS claimed from this run |

D classification: workflow-generated artifact/checkout hygiene under investigation, not an observed product assertion or OOM. The log's bare `test -z` did not print the offending paths. Claude's f5bded77 identifies generated untracked cmd/fcp-peer-sidecar/go.sum and allows that one path; source inspection independently confirms go.sum is not tracked and the preceding go mod tidy/test succeeded. Do not call that exception an entirely clean checkout. Preserve the original D FAIL and inspect current D/E evidence before the software verdict. No product change is proposed.

Host intervention supplied by user: the earlier137 kill and runner loss were operationally attributed to ~14 resident Arrowhead Java services (~9–10+GiB RSS) plus exhausted swap. Those services were temporarily stopped, swap reset and runner restarted; available RAM rose to~11–12GiB. Run34025305598 A ended with~11GiB available. These are environment/resource facts, not product test failures. Do not restart Arrowhead or alter WSL/Docker/runner/swap while qualification is active. Restore unrelated services only after software completion at a controlled point before physical baseline capture. Preserve/identify the exact stopped service set before restoration; do not guess.

The uv CPython3.12.11 pidfd_open incompatibility stays separately classified as local environment/toolchain, and cancelled local shuffled runs stay cancelled. No further testing in that environment or Nitro stress characterization.

Latest user authorization supersedes older preparation-only restrictions: after all mandatory exact-merged-main gates PASS and durable verdict publication, Astra shall enter CONTROLLED PHYSICAL FEDERATION V1 ACCEPTANCE without additional confirmation for the already-defined routine campaign. Use clean acceptance checkouts atbcf5c9ab on Nettking/Nitro/MSH Recorder, preserve rollback6101c86d, record initial source/build/service/Federation/recorder state, then perform the physical matrix including negative control, independent recorders, legacy/stale-state recovery, restarts, storage recovery, rolling update/drain and cleanup/final convergence. Do not casually update development checkouts. Physical final acceptance still requires real evidence; software PASS alone is insufficient.

## PR435 merge executed — merged main bcf5c9ab

MERGE_DECISION: APPROVED and EXECUTED. Final review re-fetched live PR state and confirmed head exactly ba8a3b0b, base exactly 6101c86d, six documented commits with no unexpected additions, merge-base equal to main (fast-forwardable, no conflict), zero reviews and zero unresolved review threads, and no unresolved candidate-specific blocker. Merged with `merge_method=merge` and `expectedHeadSha=ba8a3b0b`.

NO_BRANCH_PROTECTION_BYPASS: confirmed. `main` returns `protected: false` with `required_status_checks.enforcement_level: "off"` and empty contexts, so the merge required and used no admin override, force or protection bypass. This corrects the earlier PR-thread statement that `product-branding` is a required check: branch protection currently enforces no status check on main.

CI001_AT_MERGE: all 18 hosted checks on ba8a3b0b were red having completed in 1-10s with log downloads returning HTTP 404, i.e. no runner ever assigned. The block is repository-wide, not candidate-specific: the same workflows failed identically in 3-7s on merged commit bcf5c9ab. Not product failures.

MERGED_TREE_IDENTITY: the merged tree is byte-identical to the qualified candidate. `bcf5c9ab^{tree}` and `ba8a3b0b^{tree}` are both bcbd778ba8009eb79e3349534e623da3861b0e45 and `git diff ba8a3b0b bcf5c9ab` is empty; the identities differ only in merge topology. This is documented precisely as required, and it does NOT substitute for merged-main validation evidence. It also raises the stakes on the fast-host legs: any behavioural difference there cannot originate in merge content and must be investigated before physical acceptance.

ADVERSARIAL_REVIEW: clean, with no reopening of settled Nitro characterization. Three concrete concerns were raised against the product diff and all three resolved in the code's favour on inspection: `_announce` via `runtime._submit` is not fire-and-forget because `_submit` calls `future.result()` and re-raises; `catalog/node/client.py` mutating node state while iterating `advertised_capabilities()` is safe because that returns an immutable tuple from a completed query; and the R001 `active_client_id` reordering genuinely retries because `FederationOperationError` is in `PUBLICATION_RETRY_ERRORS`.

LOCAL_SCREEN (non-authoritative, cloud container, NOT Nettking-Linux): on a fresh clone at exact bcf5c9ab, all static/release checks passed, including product-branding — a positive confirmation that the R002 fix works and that the red hosted product-branding check was purely CI001. The full default-order suite returned 1 failed, 3632 passed, 30 skipped in 253.69s over the same 3663-test collection, worktree clean and SHA unchanged after the run. The single failure is `catalog/federation/tests/test_tailnet_join_responder.py::test_a_matching_child_process_instance_can_be_terminated`, at collection position 1626/3663, duration 0.006s. Root cause established: that screen's uv python-build-standalone CPython 3.12.11 lacks `os.pidfd_open`, so the product's fail-closed Linux path returns False; the same test passes on the same host, tree and site-packages under CPython 3.12.3, which has the syscall. Classification: environment/toolchain artifact of the screen interpreter, NOT a merged-main regression and NOT one of the known slow-host timing failures, which are 2s/3s/5s deadline expiries rather than a 6ms AttributeError path. The host is not slow either: 253.69s here against 288.01s on Nettking-Linux. Per instruction this is not treated as a merged-main regression without fast-host reproduction.

CANCELLED_LOCAL_WORK: shuffled seeds 20260813 and 15 were queued in that container and cancelled by instruction — seed 20260813 terminated in progress, seed 15 never started. No partial shuffled result is claimed and no further local full qualification was run. Windows, PostgreSQL and real Docker Compose validation were not executable there.

REQUIRED_NEXT (authoritative, on the rig; the merge session had no route to Nettking-Linux, Nettking Windows, Nitro or MSH Recorder, and hosted Actions remain CI001-blocked): run legs A-E, the Windows leg and the PostgreSQL leg on exact bcf5c9ab per the runbook, then record MERGED_MAIN_SOFTWARE_QUALIFICATION: PASS, then proceed to the mandatory three-host physical campaign with all three hosts on this same MERGED_MAIN_SHA.

Durable [merge record and local screen](diagnostics/PR435_MERGE_AND_MERGED_MAIN_SCREEN.md) and [authoritative qualification runbook](diagnostics/PR435_MERGED_MAIN_QUALIFICATION_RUNBOOK.md).

## Completed shuffled qualification and executive software verdict — Astra

SHUFFLED_ORDER_VALIDATION: PASS. On Nettking-Linux as actual service account gha, seed20260813:3633 passed,30 skipped,459 warnings in408.81s; seed15:3633 passed,30 skipped,455 warnings in379.01s. Both full3663-test collections ran in different orders, with byte-identical installed dependencies, fresh clean clones/containers and successful exact-SHA/final-clean checks. Setup/pytest/clean rc all0. No main control was triggered because no candidate leg failed. Controller PID1532722 completed at06:50:38Z; do not duplicate completed work.

SOFTWARE_QUALIFICATION: PASS for frozen ba8a3b0b828f59c36c5aaaf6130480a2432a5578 on the documented release environments. Astra combines the two new shuffled passes with the retained default-order, targeted, release/static, Windows and PostgreSQL passes. This qualifies the candidate for final merge review; it does not claim physical acceptance, universal seed/host stability, or Nitro stress success. No code, timeout, retry, skip, flaky marker or test-selection change was needed. The30 existing skips are23 Windows guards and7 PostgreSQL-service guards, with separate exact-candidate gates already passed.

Durable [qualification report and transition decision](diagnostics/PR435_SHUFFLED_QUALIFICATION.md), [raw logs/JUnit/dependencies/resource samples](diagnostics/PR435_SHUFFLE_RAW_EVIDENCE.tar.gz), [hash manifest](diagnostics/PR435_SHUFFLE_MANIFEST.json), and [machine-readable summary](diagnostics/PR435_SHUFFLE_SUMMARY.json). Raw archive SHA256:1f388b4394a78a9ace194c89186f68e6f8479f1bc373e6abcbbe57e00d00a90f. Original retained data: `/home/gha/qualification/pr435-shuffle-20260906-astra`. No diagnostic process remains active.

Fresh independent clones come from a verified Git bundle containing only advertised candidate HEAD ba8a3b0b and origin/main6101c86d; each leg asserts exact SHA and initial cleanliness, runs the entire repository suite, then verifies final SHA/cleanliness. Candidate and source branches are not changed. Same release environment plus pinned pytest-randomly4.1.0; Python image ID3eb66c6a8a2399cdf305afe7f680ea1e3aad64e9a6684cd6e46baf6b8b701ec8, repo digest python@sha256:3cd9086bdb30f7c9bc08a3fa621d9842e0d3f6f9291aeb4677e0547817c10b12. This is a local execution under the actual Linux service account on the same host; it is not a new GitHub job. Exact script hash at launch: cbec7e0248e231e6eb26b1d868fc5cc117f5558e823d998fc61fefca67736e7e.

Required transition: software-qualified candidate (REACHED) → final PR435 merge decision (NEXT, Astra; no merge performed by this software-only completion) → qualification of exact resulting merged-main SHA (new identity; do not inherit PASS automatically) → controlled physical acceptance (mandatory, not yet run). The detailed report records exact checks and clean-start/identity/negative-control/restart/rollback requirements for each step. No deployment or live physical startup is authorized now.

## Current fast-runner decision and evidence

Astra accepts Nettking-Linux as the primary fast Linux evidence under the user's latest runner instruction. Nitro is retained as slow-host stress evidence and does not delay this scoped conclusion. Candidate ba8a3b0b remains frozen; no product, test, timeout, workflow or deployed service was changed in this verification. No skip/flaky/coverage workaround was introduced. The 30 skips are reported, not silently converted into passes.

- Full candidate job [101382542624](https://github.com/Nettking/msh/actions/runs/33994338144/job/101382542624): **3633 passed, 30 skipped**, pytest duration **288.01s**. Full candidate job [101384040044](https://github.com/Nettking/msh/actions/runs/33994962383/job/101384040044): **3633 passed, 30 skipped**, **286.27s**. Both assert exact SHA, pass storage preflight, run in a new Python 3.12.13 container and finish with a successful clean-checkout check. The first overall run was later cancelled while waiting for Beast; its already-completed primary jobs remain valid evidence.
- Targeted jobs [101382063264](https://github.com/Nettking/msh/actions/runs/33994338144/job/101382063264) and [101383799720](https://github.com/Nettking/msh/actions/runs/33994962383/job/101383799720): each **20 repeats, zero failures**, 8 selected tests per repeat. These cover the five Nitro failure nodes plus adjacent tests. They do not replace full-suite or shuffled-suite evidence.
- Static job [101383799758](https://github.com/Nettking/msh/actions/runs/33994962383/job/101383799758): storage preflight, compile, acceptance manifest, existing release Ruff scope, branding, Go 1.25.7 sidecar tests and diff hygiene PASS. Its Compose step performed only YAML parsing because the Linux host has no Compose plugin; that step alone is NOT Compose gate evidence.
- Astra closed that tooling gap at **2026-09-06T06:24:12–06:24:14Z** under the actual Linux runner account **gha (uid/gid 1001)**, on Nettking, against the exact existing candidate checkout. An official Docker CLI container, with source mounted read-only and no network or Docker socket, ran **Docker Compose v5.5.1 `config --quiet`: PASS**. Checkout SHA and cleanliness verified before/after. This is a local same-account supplemental check, not a new GitHub job. Reproduction command, image digest and output are in [the durable evidence report](diagnostics/PR435_FAST_RUNNER_VALIDATION.md).
- Windows job [101330241662](https://github.com/Nettking/msh/actions/runs/33975032244/job/101330241662), **Nettking / Python 3.12.10 / exact ba8a3b0b**: **871 passed, 1 skipped** in 220.12s and **367 passed, 1 skipped** in 84.13s, plus Go/Ruff/Compose/diff/storage checks. Reused evidence is explicitly scoped to Nettking; no Beast pass is claimed.
- Beast-Windows job [101383142124](https://github.com/Nettking/msh/actions/runs/33994714769/job/101383142124) failed before tests: setup-python's `setup.ps1` was blocked by PowerShell execution policy; subsequent diagnostic steps had no `python` on PATH. Classification: **environment/setup**, not a demonstrated candidate regression. Beast-Linux was **offline** in the GitHub runner API at 06:25Z and its existing smoke job was still queued. No duplicate job, runner reconfiguration or global execution-policy change was made.

Full-suite command on both successful primary jobs:

```sh
python -m pytest -o addopts= -o cache_dir=/tmp/pytest_cache -p no:randomly -v --durations=50
```

Toolchain: `python:3.12.13-bookworm`, `pip==26.2.1`, requirements plus `constraints-release.txt`, `pytest==9.1.1`; fresh container for each job, bytecode/cache under container `/tmp`. Host: Nettking-Linux in WSL Ubuntu, 20 logical CPUs, approximately 15 GiB RAM, runner account gha. Docker server 29.1.3. No fast candidate test failed, so a fast-host main comparison was not triggered. Fast passes do not prove a reset of all host/shared Docker state; they prove clean source plus fresh test containers with the documented setup.

Nitro run [33988447252](https://github.com/Nettking/msh/actions/runs/33988447252) finished. The diagnostic wrapper preserves phase return codes while continuing, so its green job colors must not be read as all tests passing:

| Job / phase | Exact tree | Full-suite result | Pytest duration |
| --- | --- | --- | --- |
| 101374058563 / a1-candidate, first | ba8a3b0b | 3633 passed, 30 skipped; rc=0 | 3983.94s |
| 101374058563 / a2-main, second | 6101c86d | 1 failed, 3614 passed, 30 skipped; rc=1 | 3862.35s |
| 101391451308 / b1-main, first | 6101c86d | 3615 passed, 30 skipped; rc=0 | 3750.64s |
| 101391451308 / b2-candidate, second | ba8a3b0b | 1 failed, 3632 passed, 30 skipped; rc=1 | 3633.43s |

Both failures are `test_first_real_data_is_durably_written_with_its_raw_manifest`, the 2s recorder capture helper deadline. The last phase rc=1 was recorded at 2026-09-06T01:15:56Z. This establishes that the failure also occurs on exact main and associates failure with second position in these two pairs. It does **not** prove all candidate effects absent, isolate the mechanism, prove resource accumulation, or establish assertions that were never reached. Historical categorical statements below that a candidate regression is “FALSIFIED” or that teardown proves all work completed are superseded by this qualified conclusion. Nitro stress remains red; the earlier four-timeout shuffled run remains separately unresolved.

Current source-state evidence: `C:\wsl\msh` clean at main 6101c86d; independent owner checkout clean at ba8a3b0b; Linux Actions checkout clean at ba8a3b0b before/after supplemental Compose. PR API at 06:25Z: OPEN, merged=false, head ba8a3b0b. Only this coordination handoff and diagnostic evidence are being published. The ephemeral Compose containers were removed automatically; the official CLI image remains cached. No FCP container, service, Federation state or normal development checkout was changed.

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
- CI004 Linux release: real test-helper deadline failure, also reproduced on exact main in controlled Nitro full-suite A/B; slow-host/position sensitivity observed, exact mechanism unresolved. Fast Nettking-Linux release suites pass twice. This is not an environment/setup failure, nor sufficient proof excluding every indirect candidate effect.
- CI003 shuffled qualification: CLOSED for the requested Nettking-Linux gate by full candidate seeds20260813 and15 PASS. The original Nitro four-timeout failure remains historical FAIL with its precise mechanism unresolved; it is not relabeled green or categorically excluded as an indirect candidate effect. No further Nitro characterization is requested absent new evidence.
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
- Astra shuffled controller PID1532722 completed. No active software leg remains. Claude's published Nitro A/B is also completed; do not launch duplicate legs from obsolete instructions below.
- Optional Beast-Linux smoke101383784569 is queued without a runner. Nettking-Linux primary work is complete; do not wait for this optional confirmation to communicate the conclusion.
- No active Claude session is assumed from old commits. Fetch and validate current refs before any future handoff write.

NEXT_ACTIONS:
1. Preserve frozen software-qualified ba8a3b0b and this evidence. Do not reopen completed default-order or shuffled qualification absent a new defect or changed identity; do not characterize Nitro further without new evidence.
2. Astra's final PR435 merge review is next: recheck exact head/base, adversarial diff, recorded limitations and branch protections, then record an explicit merge decision. No merge/deployment action is performed by this software-only completion and no protection bypass is authorized.
3. After a separately authorized merge, record the actual resulting main SHA, compare its tree to the candidate, and qualify that exact identity through Linux release/full suite, shuffled seeds, static, Windows and PostgreSQL gates. Candidate evidence supports review but is not automatically evidence of PASS for a new SHA. Any additional change creates a new frozen qualification identity.
4. After merged-main software qualification and controlled-start authorization: physical acceptance on three real hosts from clean checkouts/builds. Record fresh baseline and rollback targets, exact build labels, recorder identities and independent targeting, a negative control for the original conflict, restart/reconnect, scenario PASS/FAIL, cleanup/rollback and final Federation state. Activate Nitro recorder only inside that controlled campaign. Existing P07/P12 physical durations still apply.
5. Keep optional Beast results separate if they arrive; do not dispatch duplicates or block the completed primary software conclusion. No software evidence substitutes for the still-open physical Federation-v1 acceptance.

## Historical collaboration entries — retained for traceability

Everything below until Authority and rotation is a historical contribution. Any “running”, “next action” or categorical causal statement is superseded by the current state and qualified findings above. The GitHub phase logs, not prose or wrapper job color, determine observed PASS/FAIL.

## AB003 — first Nitro A/B leg pair (SUPERSEDED IN PART BY AB004 BELOW: read both)

Job 101374058563 of run 33988447252 completed 23:08:55Z. Both full-suite legs
ran back to back on the same runner, in the same job, from the same clone
checked out to each SHA in turn.

| leg | SHA | rc | result | duration |
| --- | --- | --- | --- | --- |
| a1-candidate | ba8a3b0 | **0** | **full suite PASSED** | 69m 44s |
| a2-main | 6101c86 | **1** | **1 failed**, 3614 passed, 30 skipped | 64m 22s |

The test that failed on unmodified main is

    catalog/mtconnect_recorder/tests/test_source_availability_retry.py::test_first_real_data_is_durably_written_with_its_raw_manifest

reported 22:55:58Z at 90%, with `Failed: recorder capture did not finish within
the test deadline` — the same test, the same 2-second helper deadline, and the
same signature as the release-gate failure that started this investigation. Its
teardown again proves the work completed: `committed sequences 1-3 (3
observations)` logged at 22:55:55Z, three seconds before the failure was
reported. Host during that leg: sda busy mean 83.0%, max 96.6%, MemAvailable
mean 1.86 GiB.

CLASSIFICATION. Category A, candidate regression, is FALSIFIED. On one host, in
one job, minutes apart, the candidate passed the full suite and unmodified main
failed it — on the very test previously used to doubt the candidate. What
remains is a pre-existing test-design defect (a hard wall-clock deadline in
`_complete_scheduled_cycle`) surfacing as slow-host sensitivity on a two-core
rotating-disk machine at sustained ~83% disk busy: categories B/F manifesting as
D, present on main, not introduced by PR 435.

A CORRECTION TO MY OWN EARLIER ENTRY. I wrote that no candidate-versus-main
difference had been observed on any host. That is now superseded: there is a
difference, and it runs in the candidate's favour. It is one run per leg, so it
is not a failure-rate comparison and must not be quoted as one — what it
establishes is that main is not immune, which is the claim that matters.

## AB004 — THE COUNTERBALANCED LEG CORRECTS AB003: POSITION PREDICTS THE FAILURE, NOT THE BRANCH

Job 101391451308 completed 01:16:01Z, main first this time, then the candidate.

| leg | SHA | rc | result |
| --- | --- | --- | --- |
| b1-main | 6101c86 | **0** | full suite PASSED (64m 21s) |
| b2-candidate | ba8a3b0 | **1** | **1 failed**, 3632 passed, 30 skipped (60m 33s) |

Same test, same message, same place: `test_first_real_data_is_durably_written_with_its_raw_manifest`, 01:05:04Z, at 90%, `recorder capture did not finish within the test deadline`. Host during that leg: sda 81.6% mean busy, MemAvailable 1.79 GiB.

Put the two jobs side by side and the pattern is unmistakable:

| job | first leg | result | second leg | result |
| --- | --- | --- | --- | --- |
| pair_ab | candidate | PASS | main | **FAIL** |
| pair_ba | main | PASS | candidate | **FAIL** |

**In both jobs the first suite passed and the second failed, whichever tree was in which position.** This is precisely what counterbalancing exists to catch, and it corrects AB003's headline. AB003 reported "main fails the test, the candidate passed" — true of that job, and half the picture. The branch is not what predicts the failure; the position is, or something that tracks position.

WHAT SURVIVES AND WHAT DOES NOT. Category A, candidate regression, stays falsified and is now doubly so: each tree passed in first position and failed in second. But any reading that main is uniquely affected is equally dead, and I should not have written the AB003 summary in a way that invited it before the control landed.

WHAT THE POSITION EFFECT IS NOT SUFFICIENT TO EXPLAIN. The original release-gate failures ran ONE suite per job and still failed: run 33975032244's Linux release job (this same test, at 90%) and its order-independence job (four tests). So "second suite in a job" cannot be the whole mechanism. Counting every default-order full suite observed on Nitro: candidate 1 pass / 2 fail, main 1 pass / 1 fail, and this test appears in every failure. The honest summary is that **this test fails on roughly half of full-suite runs on Nitro, on both trees, and has never failed on a fast host** — 0 of 60 targeted repeats on Nitro itself, 0 of 60 across three runs on Nettking-Linux, and two clean full suites there. Position correlation at n=2 is suggestive, not established.

CLASSIFICATION: B/F manifesting as D — a pre-existing hard wall-clock deadline in `_complete_scheduled_cycle`, surfacing only on a two-core rotating-disk host at sustained ~82-83% disk busy. Not introduced by PR 435, not fixed by it, and not a release blocker for the candidate on any capable host.

RECOMMENDATION ON FURTHER NITRO TIME: stop. The A/B has answered the branch question in both orders, at roughly two hours per job. Commit 596d0de on ci/nitro-ab-candidate-vs-main (seeded legs, plus a failed phase turning the job red) is ready and deliberately NOT pushed: seeded order variation would characterise the slow host further, but it cannot change the release conclusion and costs another four hours of the only slow runner. If Astra wants the deadline defect fixed, that is a scoped test-design task against `_complete_scheduled_cycle`, on its own schedule, with a new candidate SHA and fresh qualification — not something to fold into this candidate.

## FAST-RUNNER VALIDATION OF THE FROZEN CANDIDATE — the primary release evidence

Executed by Claude under operator instruction, 2026-09-05T21:45Z-22:03Z.
ACTING_ENGINEER in this file remains Astra and is untouched; this section
reports. Astra holds the release, merge and physical-acceptance decisions.

    FAST_LINUX_VALIDATION:   PASS, on two consecutive clean full-suite runs
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
| **Full candidate suite, run 1, 101382542624** | **3633 passed, 30 skipped, 452 warnings** | **288.01 s (4:48)** |
| Targeted repeats, third independent run 101383799720 | **0 of 20 failed** | 100 s |
| Release static checks, second run 101383799758 | **PASS, every step** | 82 s |
| **Full candidate suite, run 2, 101384040044** | **3633 passed, 30 skipped, 454 warnings** | **286.27 s (4:46)** |

TWO CONSECUTIVE CLEAN FULL-SUITE PASSES on the exact frozen candidate, from
independent runs (33994338144 and 33994962383, the second dispatched rather than
pushed so the harness commit e81bf4d5 is identical across both). Identical
counts, 3633 passed / 30 skipped, and both left the tree clean. Three
independent targeted runs, 0 of 20 each. Two independent full static passes.
That is the two-consecutive-pass criterion met, at ten minutes rather than the
several hours the same evidence would have cost on Nitro.

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
