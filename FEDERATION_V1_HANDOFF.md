# Federation v1 authoritative coordination handoff

**NON-MERGE BRANCH: `coord/federation-v1-release`. Never merge this branch into main merely to deliver this document. Its HEAD is a coordination commit, not the product candidate.**

## Authoritative fields

EXECUTIVE_OWNER: Astra
ACTING_ENGINEER: Astra; Claude is the authorized cooldown successor, not yet confirmed running
CURRENT_PHASE: Software validation and independent review of PR #435; physical deployment not authorized yet
CURRENT_MAIN_SHA: 6101c86d94294c70db47d1a8053cac93b9a41356
CURRENT_CANDIDATE_SHA: f0434e4a4fd4cf86b3574563151d1e6241b4e06c
PR_435_HEAD: f0434e4a4fd4cf86b3574563151d1e6241b4e06c on claude/federation-recorder-capability-id-19tqkk; OPEN, not merged
ACTIVE_FIX_BRANCH: None created by Astra; claude/pr435-reconcile-retry is reserved for the bounded R001 follow-up below
VALIDATION_BRANCH: ci/self-hosted-pr435 at 87310dfdbaa33665957589046140b9714a617cdf
ACTIVE_CI_RUN: 33960401244 (push on ci/self-hosted-pr435 at 87310dfdbaa33665957589046140b9714a617cdf; product VALIDATED_SHA=f0434e4a4fd4cf86b3574563151d1e6241b4e06c). Windows SUCCESS; Linux CANCELLED; order-independence and PostgreSQL QUEUED. Overall API status queued; no duplicate run started by Astra.
DEPLOYED_NETTKING_SHA: Last physical observation 2026-09-04: 6101c86d94294c70db47d1a8053cac93b9a41356; live runtime not refreshed at takeover
DEPLOYED_NITRO_SHA: Last physical observation 2026-09-04: 6101c86d94294c70db47d1a8053cac93b9a41356; live runtime not refreshed at takeover
DEPLOYED_MSH_RECORDER_SHA: Last candidate-runtime observation 2026-09-04: 6101c86d94294c70db47d1a8053cac93b9a41356; legacy runtime also existed; live runtime not refreshed
CURRENT_SESSION_ID: UNKNOWN; do not infer from old logs or expose pairing/authentication secrets
LAST_UPDATED_UTC: 2026-09-05T13:47:30Z

KNOWN_FAILURES:
- Current PR-triggered runs at f0434e4a report failure; their causes are not yet established. The separate self-hosted run is identified above.
- Self-hosted Linux job 101291502702 was cancelled at the120-minute budget after64% of the full suite. The progress stream includes an `F` near60%; the failing node ID/traceback is not available in the cancelled log and must be identified. Do not classify the entire result as only an infrastructure timeout or claim no test failed.
- Full physical Federation v1 acceptance remains incomplete. No new physical acceptance result has been established by this takeover.
- Historical rig at main 6101c86 had a real multi-recorder `recorder-local` identity collision and no ready logical-storage group. PR435 addresses the identity collision; it is not yet physically validated.

FAILURE_CLASSIFICATIONS:
- R001: UNVERIFIED RELIABILITY REVIEW FINDING. Publication loop sets `active_client_id` before awaiting multi-step reconciliation. A transient coordinator lookup/legacy-retirement failure on an otherwise connected client may prevent future retry and leave duplicate owned recorder identities READY. Static review evidence exists; no regression test or runtime reproduction has yet been run by Astra. Do not state this as a proven reproduced defect.
- CI001: FAILED PR WORKFLOW RESULTS, CAUSE UNKNOWN. Do not equate a red hosted wrapper or billing/startup failure with a product regression.
- CI002: CONFIRMED incomplete Linux full gate:120-minute timeout plus one visible failed-test marker; exact test/root cause UNKNOWN. Investigate Nitro host/container contention, suite durations, and test collection order before selecting a fix. Do not simply extend the timeout and hide the failed test.
- ENV001: RESOLVED Windows Python bootstrap, now verified in a real Nettking Actions job. Job 101291502797 checked the exact candidate, ran Python 3.12.10 from the standalone toolchain, and completed successfully under the registered runner. CI intentionally upgraded pip to26.2.1; the earlier preparation report's25.0.1 is historical.
- HIST001: Claude's diagnosis branch documents storage-floor exhaustion, insufficient Linux timeout, root-owned bind-mount leftovers, and SQLite stale-backup test defects. These are historical evidence; inspect current branch/run before classifying anything as still failing. Its later notes say operator disk cleanup resolved the Windows floor and PR fixes advanced beyond the original 13967aea candidate.
- PHYS001: Physical evidence incomplete; software green is insufficient for release.

COMPLETED_TESTS:
- Python preparation: exact3.12.10 x64; pip 25.0.1; imports, subprocess, venv creation, offline wheel install/import and pip check passed under Nettking\\Martin. Package catalog SHA512 and PSF Authenticode signatures verified.
- Toolchain permissions: all 2091 audited entries permit read/execute through the runner group containing NetworkService; no ACL/account changes made.
- Product repository baselines: C:\wsl\msh remained clean main 6101c86; Actions checkout was clean detached13967aea at end of Python preparation. The Actions workspace is managed by jobs and is not the owner development checkout.
- Astra independent candidate review: scoped identity/migration approach matches the physical collision; no authority regression identified in reviewed production diff; R001 remains to validate.
- Real self-hosted Windows job 101291502797 SUCCESS:871 passed/1 skipped in capability/product subset;367 passed/1 skipped in transport/storage/failover subset; Go, Ruff, Compose and diff checks passed. Job ran on Nettking from10:24:02Z to10:31:57Z, validated f0434e4a, Python 3.12.10. This is software evidence, not physical acceptance.
- No product tests, CI dispatch, merge, deployment or physical acceptance operation has been run by Astra in this takeover yet. PR-body test counts are prior-author reports, not refreshed exact-head validation.

CURRENTLY_RUNNING_WORK:
- Astra: durable coordination published; next identify the failed Linux test and retry-reconciliation finding, inspect Nitro runner/container state, and refresh physical inventory read-only.
- Run 33960401244 still has queued Linux order-independence job 101291502804 and PostgreSQL job 101291502832. Nitro and Nettking both reported online/not-busy at the last API snapshot; queue cause not yet established. Preserve queued work until diagnosed.
- No newly delegated Claude task has been confirmed active. Do not assume pushing this file starts Claude automatically.
- Existing Claude work is preserved on the PR branch, ci/self-hosted-pr435 and claude/pr435-validation-diagnosis-n285av.
- Supporting read-only reviews are complete. One supporting CI-audit agent returned a usage-limit error; no unpublished implementation was produced by it.

NEXT_ACTIONS:
1. Fetch remote refs and read this file first. Compare main, PR435 head, validation SHA and newest run before changing anything. Do not restart a useful already-running validation.
2. Refresh run 33960401244 and queued jobs101291502804/101291502832. Its wrapper at 87310dfd checks out f0434e4a in every job. Linux job 101291502702 timed out after64% with an `F` near60%. Retrieve its log, identify that test using exact-head collection order or a focused verbose rerun, and inspect Nitro load/orphan CI containers read-only. Preserve Windows success101291502797 as exact-head evidence; do not rerun it without cause.
3. Investigate R001 at f0434e4a: `catalog/mtconnect_recorder/federation_node.py` near895 sets active_client_id before awaiting `_announce_connected`; reconciliation near777 includes coordinator lookup and legacy retirement. Test a one-time ordinary timeout through the publication loop, with the same connected client, and observe whether reconciliation retries. Keep this a local benign fault-injection test, not an exploit workflow.
4. If R001 is proven, implement only the minimal retry-order fix plus meaningful publication-loop regression coverage on claude/pr435-reconcile-retry based on f0434e4a. Run focused recorder/node/pairing/control coverage. Push the branch; record the commit and test results before requesting integration review.
5. Preserve useful Claude diagnosis: inspect claude/pr435-validation-diagnosis-n285av (observed dfc217793d4bcfc0b6e1723c41c4453613db6f2d) and its two docs. Its earlier 487610a and b2a7c6e references are historical, not current authority.
6. Continue focused -> broader -> complete self-hosted software gates with explicit candidate SHA. Separate environment faults, test defects and product regressions using evidence. Never reduce release thresholds or coverage to obtain green.
7. In parallel, refresh Nettking/Nitro/MSH Recorder checkouts, container build labels, mounts/projects and session/capability state read-only. Keep identities, telemetry and unrelated legacy runtimes intact.
8. After Astra's integration/merge decision, validate the exact resulting main SHA before deploying the same SHA to all three machines. Start physical acceptance only after runtime identity is proven and required storage/publication is ready.

ACTIONS_CLAUDE_IS_AUTHORIZED_TO TAKE:
- During a confirmed Astra cooldown, claim ACTING_ENGINEER in this file with a normal fast-forward push, then continue reversible diagnosis, scoped implementation, regression tests, focused/broad/full software validation and read-only machine inventory.
- Use claude/pr435-reconcile-retry for R001. Owned paths are `catalog/mtconnect_recorder/federation_node.py` and the smallest relevant recorder regression test file. Do not change authority semantics, thresholds or unrelated product paths.
- Push useful scoped commits and update this document immediately. Do not leave the only useful result in an unpushed checkout or chat.
- Claude may update the PR or validation branch only after confirming no other writer owns that branch and recording the transfer here. Preserve one candidate; distinguish proposed fixes from the accepted working candidate.
- Retrieve/log CI outcomes, repair proven CI-environment wrapper defects without changing product requirements, and continue an already-started physical test only if its authoritative candidate is unchanged.
- During Astra-active periods, default to read-only or a separately reserved fix branch. Coordination writes also require a single acting writer.

ACTIONS_REQUIRING_ASTRA_REVIEW:
- Accepting a newly proposed product fix as the authoritative candidate; inspect diff, verify diagnosis and focused tests before promotion.
- Merging a candidate that differs from the specific standing authorization below.
- Physical deployment, a new fault-injection campaign, persistent-state cleanup, new architecture and final release verdict.
- Branch-protection bypass and reduced release criteria are not authorized.

## Standing merge policy

MERGE_AUTHORIZATION: Claude may perform a normal merge of PR435 during Astra cooldown ONLY if all conditions below hold:
1. PR head is still exactly f0434e4a4fd4cf86b3574563151d1e6241b4e06c, and main is still 6101c86d94294c70db47d1a8053cac93b9a41356.
2. R001 has been conclusively ruled out with recorded production-path regression evidence; it is not merely untested or waived.
3. This exact head has complete self-hosted release-gate SUCCESS on required Windows and Linux jobs, including all required broader/order/storage/PostgreSQL/verdict checks; no unexplained failure, cancellation, skip of a required gate, stale-SHA substitution or threshold reduction remains.
4. PR remains mergeable, protection/review requirements are met normally, and no concurrent main/PR changes invalidate the evidence.
5. Record gate URLs, input SHA, merge action and resulting main SHA here immediately. Validate the exact merged main SHA before proposing deployment.

If a product fix changes PR head, these standing criteria do not authorize its merge. Continue useful implementation/testing and leave exact diff/evidence for Astra review. No physical deployment or release verdict is pre-authorized by this merge policy.

## Branch ownership and safe continuation

- `main`: protected; no direct/force pushes.
- `claude/federation-recorder-capability-id-19tqkk`: existing PR435; no concurrent pushes.
- `ci/self-hosted-pr435`: existing validation wrapper; no concurrent candidate changes or duplicate dispatch before inventory.
- `claude/pr435-reconcile-retry`: reserved bounded proposed fix, not authoritative until accepted.
- `coord/federation-v1-release`: coordination only; single acting writer, fetch before edit, normal fast-forward push, never force push or merge into main.
- Astra owner checkout: task-local `work/msh-owner`, detached at candidate. Coordination checkout: task-local `work/msh-coordination`. Neither is a physical deployed checkout.
- Runtime isolation requires explicit Compose project, data/results roots and ports. A different source checkout alone does not isolate project volumes. Never use `--fresh` to erase the live rig as a troubleshooting shortcut.

## Evidence index and physical boundaries

- PR: https://github.com/Nettking/msh/pull/435
- Validation branch: https://github.com/Nettking/msh/tree/ci/self-hosted-pr435
- Claude diagnosis branch: https://github.com/Nettking/msh/tree/claude/pr435-validation-diagnosis-n285av
- Current self-hosted run: https://github.com/Nettking/msh/actions/runs/33960401244 ; Windows job 101291502797 SUCCESS; Linux job 101291502702 CANCELLED; queued jobs101291502804/101291502832.
- Observed failed PR runs at f0434e4a:33960392304(recorder),33960392107(CF8),33960392114(branding),33960392104(B01),33960392229(release),33960392096(updates),33960392083(phase2),33960392066(docs). These are separate from the self-hosted run.
- Windows toolchain: `C:\actions-runner\toolchains\python-3.12.10\python.exe`; prepend its root and `Scripts` in GITHUB_PATH for subsequent steps. Current CI log shows pip 26.2.1 after its explicit upgrade. Runner remains NetworkService/Automatic. Existing Python 3.14 remains untouched.
- Local prior-task outputs: `physical-test-rig-remediation-report.md` final appended section supersedes early SSH/pairing diagnoses; `physical-discovery-record.md`; `federation-v1-physical-acceptance-report.md`; checklist. They live under the user's2026-09-04 physical-acceptance task outputs, not in this branch. Keep private endpoints, SSH details and tokens out of committed evidence.
- Historical candidate projects: Nettking `fcp-new`; MSH `fcp-local-new`; legacy `fcp` coexisted. Nitro project name requires live inspection. Historical MSH product checkout had untracked `record data` which must be preserved.
- Actual runtime commit evidence: container label `no.fcp.build_commit` or native recorder `native_runtime.build_commit`. Headless status alone does not prove runtime SHA.
- Strict P01-P12 and CF7 physical-evidence.v2 are separate required evidence sets. P07 requires at least1 actual hour; P12 at least24 actual hours, with same candidate/host/run_id and valid in-window samples. Old fragments or CI green cannot substitute. Current acceptance flags remain false.
- Real MTConnect, Ollama/accelerator, desktop/mobile and Windows/POSIX/multi-host observations remain necessary. Use approved existing devices/endpoints; no unrestricted discovery or exploit workflows.

## Rotation protocol

Before yielding for cooldown: finish the safe atomic operation, push useful commits, update all changed fields and evidence, list exact running job IDs, set ACTING_ENGINEER to Claude only as an explicit transfer (not a claim Claude is already executing), and provide the coordination branch URL to the operator/Claude channel. On return, Astra fetches this branch first, audits changed commits/CI/physical evidence, reconciles actual refs, then records ACTING_ENGINEER:Astra. Reuse valid evidence; do not restart completed work solely because the acting model changed.
