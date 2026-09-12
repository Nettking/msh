# Federation v1 diagnostic sweep — recover here first

**Current coordination:** [state-change-driven qualification](QUALIFICATION_COORDINATION.md)
supersedes historical execution next-actions below. Read it before acting.

Post-sweep D04 update18:14UTC: environment remediation complete on unchanged N.
Exact stale process was replaced under the [controlled host procedure](D04_CONTROLLED_HOST_PROCEDURE.md).
Current N responder owns5151; local and real Nettking peer health200 verified.
[Host receipt](diagnostics/d04-controlled-replacement.json),
[peer receipt](diagnostics/d04-real-peer-health.json). No product repair, legacy
identity copy, enrollment request, Docker restart or protected Recorder access.
M runtime admission must verify ownership again; no physical evidence carry-over.

**Mode: DIAGNOSTIC ONLY — NOT PHYSICAL ACCEPTANCE EVIDENCE.**

Runtime candidate / AUTHORITATIVE_SHA:
`0536f03d67eb277e11573c2188d8e820399627e3` (N).
This documentation branch is `codex/federation-v1-diagnostic-sweep-20260911`.
Its documentation commits are not new runtime candidates and must not be deployed.
GitHub's latest version of this file and its linked evidence are the authoritative
diagnostic checkpoint. The previous qualification-first handoff is superseded.

**Current phase: diagnostic sweep COMPLETE, September11 12:44UTC.** Read the
[sweep report](DIAGNOSTIC_SWEEP_REPORT.md) for the stop condition, coverage,
smallest fix set and next action. Historical next-action paragraphs below are
retained evidence history; the report supersedes them. D05 repair is now draft
PR461, HEAD5b826c6806ab1bdb960412ba20ca78192971fb1d, with50 focused tests passed.
Continue exact-head qualification without repeating valid work. Physical
hosts remain unchanged and formal acceptance remains stopped.

Latest post-sweep CI checkpoint: [source-specific native receipts and job state](diagnostics/post-sweep-qualification-checkpoint.json),
[four justified dispatches](diagnostics/post-sweep-dispatch-ledger.json).
At the snapshot456 had26 exact successful native logs,457 had20,461 had0;
these counts include non-required evidence and are NOT the37-check verdict.
PR457 software-update run34594191426 checked out3363b2c9, not143fe7a9, on both
platforms, so its exact-head workflow was dispatched after preserving both logs.
PR461 was missing cf7-acceptance-harness, cf7c-physical-test-readiness and
ci-test-sharding, so those three were dispatched on5b826c68. No valid result was
rerun. Other461 PR workflows are active/queued; its completed CFI2 Linux job used
a synthetic merge checkout, so additional exact-head reconciliation remains.
Do not cancel active jobs or infer qualification from the API head field.

**Exact next action:** refresh each PR's `github_qualification.py snapshot`
and `retain_qualification_logs.py <label>` using the local audit directory below;
review only newly completed logs/artifacts, retain actual checkout proofs, then
dispatch missing exact-head coverage only where current head/ref and existing
runs prove a gap. Existing `advance_exact_head_qualification.py` accepts only
readiness/discovery; do not falsely treat it as PR461 support. Wait for461 active
synthetic jobs to finish or reconcile only wholly inactive queues; do not create
duplicate runs. Required full exact-head gates, immutable image metadata and
ICSE artifacts remain incomplete. No merge/new candidate/final-main qualification
yet. Continue the existing automatic task follow-up; it was updated to this phase.

## Operating boundary

Expose independent blockers before selecting the next candidate. Keep all live
hosts on their existing checked-in sources. No repair deployment, cherry-pick,
modified product launcher, relaxed deadline, quorum bypass or blanket automate.
Protected MSH Recorder persistent data must not be deleted, reset, pruned or
overwritten. Do not read the protected recording corpus. No P/B/CF7 physical PASS
and no P07/P12 acceptance timers. Existing in-flight CI may finish, but do not
dispatch more full qualification or merge until the sweep is complete.

For each new independent confirmed blocker, immediately commit and push its
entry and exact safe evidence before substantial further investigation. Publish
an issue if the fix is unknown, or a draft PR for a concrete repair, and link it
back here. Keep hypotheses separate. Preserve command, host, stage, SHA, expected
and observed result, state effects and protected-data invariant. During usage
pressure, publish unfinished state and the exact next action before continuing.

## Accumulating blocker table

| ID | Physical stage | Symptom | Classification | Reproducible | Blocks what | Independent continuation possible? | Proposed repair |
|---|---|---|---|---|---|---|---|
| D01 | Supported Nitro startup, before formal P01/P03 | Core activation succeeds; readiness follows `/onboarding` into stateful `/login`, times out, launcher exits 1 | product | Actual N observation and focused regression | Supported startup completion; updater start after this gate | YES: independent metadata, local provider and bounded diagnostics; no startup PASS | [#456](https://github.com/Nettking/msh/pull/456), direct-response readiness |
| D02 | Ordinary tailnet discovery, before enrollment/P01 | Default 0.75 s probes time out; public advertisement requests fresh authority; later server 200 is too late | product | Three ordinary N timeouts; focused delayed-response and authority regressions | Ordinary discovery/zero-touch join; no longer-timeout workaround counts as fixed | YES: independently addressed metadata/provider checks and safe authority diagnostics | [#457](https://github.com/Nettking/msh/pull/457), local routing metadata and bounded total HTTP budget |
| D03 | Nettking local AI provider before P10/B/CF7 | Audit 60 s timeout; actual unchanged 120 s adapter completes in 103.55 s, correctly recommends not-recommended | harness (audit deadline); environmental cold-load latency, no product defect established | Actual product-bound observation completed, 100% GPU; no retry needed | No independent deadline blocker; cold latency remains unsuitable by existing policy | YES: independent authority/status paths | [#458](https://github.com/Nettking/msh/issues/458); no product repair proposed |
| D04 | Nitro host responder/runtime admission before joining | Legacy responder owns tailnet port5151; no N responder; legacy checkout5dfbcd1, deleted Python3.14 | environment | PID/socket ownership confirmed; actual-bind health timed out5.03s | N responder cannot bind default port; grant path is not N | YES: independent app/status checks; no legacy process mutation | [#459](https://github.com/Nettking/msh/issues/459); admission/ownership repair required |
| D05 | Configured responder port / candidate admission | Host FCP_AUTO_JOIN_PORT=5152, rendered Flask omits it and advertises5151 | product (Compose propagation) | Unchanged N render; 2 red non-default regressions, then50 focused green | Supported non-default responder ports, including isolated admission around D04 | YES: software repair/qualification after completed sweep; no deployment | [#461 draft](https://github.com/Nettking/msh/pull/461), tracks [#460](https://github.com/Nettking/msh/issues/460) |
| D06 | M responder admission before fresh P01/P03/join | Checked-in replacement signals old instance, then new process fails bind EADDRINUSE | product (asynchronous termination/rebind) | Actual Nitro M failure and real delayed-exit regression; focused repair tests PASS | M physical admission stopped; no PASS or timed runs | Isolated repair/qualification only; no physical retry workaround | [#462](https://github.com/Nettking/msh/issues/462); [PR463](https://github.com/Nettking/msh/pull/463), qualified exact2c1a8d93; merged2a9c9b8e, not deployed |
| D06-R1 | Unmerged D06 repair review; backup quiescence | Shared helper's5s wait raises before backup's10s stop/error contract | product regression in PR463, not a new physical observation | Two isolated red regressions at7286f30d; green after correction | Corrected source qualified in2c1a8d93 and merged2a9c9b8e | Isolated correction only; physical hosts untouched | [Evidence](diagnostics/D06-R1.md); [correction](https://github.com/Nettking/msh/pull/463#issuecomment-5640841676); RESOLVED in merged2a9c9b8e |
| D07 | Windows capability-product software qualification | Concurrent analysis submission raises WinError5 replacing plan.json | pre-existing product reader/replacement concurrency defect; unchanged by original PR463 | CI failure plus deterministic public store API reproduction on clean83955f65; repair qualified37+3 | Final combined candidate/physical qualification still pending | Remaining463 qualification; no physical deployment | [Exact evidence](diagnostics/D07.md); [#464](https://github.com/Nettking/msh/issues/464); [merged PR465](https://github.com/Nettking/msh/pull/465), merge b6a96b21 |

| D08 | Final-main F85 Windows software qualification | Concurrent submission ends FAILED/artifact-object-key-escape;1 failed,728 passed,1 skipped | product: leaf realpath races Windows POSIX publication into NTFS deleted namespace | Native CI plus focused public-API reproduction; exact repair84c66f81 qualified37+3, all17new cases have native PASS | Actual new-main qualification/candidate freeze/physical restart remain pending | YES: single actual17ab merged-main qualification; physical hosts unchanged | [Evidence](diagnostics/D08.md); [#466](https://github.com/Nettking/msh/issues/466); [merged467](https://github.com/Nettking/msh/pull/467),merge17ab3a05; [qualification](diagnostics/pr467-final-qualification.json), not deployed |
| D09 | Post-sweep CI migration branding gate; no physical stage | New migration document contains two prohibited repository-URL spellings; checker exits1 | CI documentation regression; checker correct, no runtime defect | Same exact native error on PR468 and canary469 | Merge of current PR468 head; prior main/F7 evidence remains source-valid | YES: immutable CI and separate docs-only correction; do not cancel active runs | [Evidence](diagnostics/D09.md); existing [draft468](https://github.com/Nettking/msh/pull/468); docs-only ba44100e integrated after2/2 native proof; final29 CI contracts and native branding PASS |
| D10 | Post-sweep CI release checkout; no physical stage | Beast cannot remove runner-owned .pytest_cache (EPERM); three shards never reach candidate/test execution | Host/environment; underlying ACL/handle cause unresolved | Three jobs across468/469 | Those shards and dependent release aggregates; prior main/F7 evidence unaffected | YES: independent CI and D09 docs integration; preserve state | [Evidence](diagnostics/D10.md); [#470](https://github.com/Nettking/msh/issues/470); no product repair |
| D11 | Linux release regression shard2; no physical stage | test_live_reinstatement_restores_replica_and_acknowledgement_policy ends retryable/TimeoutError instead of completed at catalog/node/tests/test_live_storage_reinstatement.py:410; 1 failed,1101 passed,19 skipped,3365 deselected; failing case49.57s. | UNRESOLVED: timeout is confirmed; product, test-harness timing, host load/admission or transient transport cause is not established. | One exact native failure; focused repro pending | Release shard2 and dependent aggregates fail on this PR source. Preserve prior main17ab and F7 equivalence evidence; do not call this a documentation regression or physical acceptance. | YES: independent unchanged-source diagnosis | [Evidence](diagnostics/D11.md); [#471](https://github.com/Nettking/msh/issues/471); no repair |
| D12 | Phase2 Go direct peer sidecar build; no physical stage | Go1.25.7 tests pass (0.191s), then go build fails: error obtaining VCS status: exit status128. Python Phase2 regression step is skipped after the build failure. | UNRESOLVED; host/toolchain Git provenance failure is suspected. Underlying Git stderr is not in the retained build error, and no product test failure was observed. | One exact native failure; focused repro pending | Phase2 Windows check blocked. Not the same proven mechanism as D10, although the same runner/workspace may share an environment cause; do not merge those findings without evidence. | YES: independent unchanged-source diagnosis | [Evidence](diagnostics/D12.md); issue publication next; no repair |

## D05

**Finding:** D05  
**Status:** RESOLVED-IN-BRANCH (candidate remains affected; repair not deployed)  
**Candidate SHA:** `0536f03d67eb277e11573c2188d8e820399627e3`  
**Host(s):** Nettking native Docker Compose rendering; applies to Windows/POSIX supported launchers; motivated by Nitro D04 port ownership  
**Physical stage:** Runtime configuration/admission before joining, no formal P-test and no alternative-port deployment  
**Observed:** With FCP_AUTO_JOIN_PORT=5152 in the rendering subprocess, the unchanged N Compose Flask environment omits the setting. Both supported host launchers use5152, whereas unchanged N auto_join_port(rendered_flask_environment) returns5151 for the discovery advertisement. No .env or overrides in the clean exact-N harness checkout.  
**Expected:** Configured host responder port and public advertised port agree, preserving default5151 when unset.  
**Classification:** product — missing Compose environment propagation  
**Acceptance impact:** A correctly configured separate candidate responder port cannot be advertised through the stock Compose service. This blocks the simple isolated-port admission option for D04 and any supported non-default responder port. It does not establish a deployed non-default runtime failure; the exact configuration mismatch is demonstrated without deployment.  
**Safe continuation:** YES — read-only reconciliation and final sweep coverage review; no local runtime workaround or new qualification until sweep complete.  
**Evidence:** [Exact N render and observed values](diagnostics/D05-configured-port.json), [audit procedure](diagnostics/probe_configured_responder_port.py). Command is Docker Compose `config --format json` with only the render child set to FCP_AUTO_JOIN_PORT=5152 and FCP_BUILD_COMMIT=N. Source chain: `start-tailscale.{sh,cmd}` -> missing `docker-compose.yml` Flask environment -> `federation_pairing_routes.py:_discovery_response` -> `tailnet_join_bridge.auto_join_port`.  
**State changed:** None; config rendered only. No containers created, ports bound, sources deployed or runtime env changed.  
**Protected Recorder data:** Untouched.  
**GitHub artifact:** https://github.com/Nettking/msh/issues/460  
**Repair:** Draft https://github.com/Nettking/msh/pull/461, branch `codex/tailnet-responder-port-propagation`, exact HEAD `5b826c6806ab1bdb960412ba20ca78192971fb1d`, checkout `C:\wsl\fcp-tailnet-responder-port-20260911`. One Compose environment entry plus real Compose-render regression. Pre-fix regression:3 passed/2 failed for non-default ports; after repair50 focused tests passed, zero skips. Ruff check/format and git diff --check passed. [Red log](diagnostics/D05-red.log), [red JUnit](diagnostics/D05-red-junit.xml), [green log](diagnostics/D05-green.log), [green JUnit](diagnostics/D05-green-junit.xml). Assertions were adjusted after the red run to show integer ports rather than environment dictionaries; behavior unchanged. No release qualification or deployment is claimed by these focused tests.  
**Next diagnostic action:** Sweep is complete. Reconcile valid exact-head qualification of456/457/461 and dispatch only required missing work. Do not rerun already valid N or unchanged PR-head results. Keep D04 for post-qualification owned admission; all physical hosts remain unchanged.

## D04

**Finding:** D04  
**Status:** CONFIRMED (host process provenance mismatch; endpoint ownership not yet established)  
**Candidate SHA:** `0536f03d67eb277e11573c2188d8e820399627e3`  
**Host(s):** Nitro  
**Physical stage:** Host-side responder/runtime admission before Federation joining  
**Observed:** No responder process has the candidate source cwd. A broader read-only /proc inspection found PID 1422341 from `/home/martin/fcp`, elapsed 630709 s, executable `/usr/bin/python3.14 (deleted)`, no FCP_BUILD_COMMIT environment. It owns the tailnet TCP5151 listener; actual-bind health timed out5.03s. Current legacy disk HEAD is clean5dfbcd1, not N; imported bytes remain unproven. Current clean N core containers cannot establish this older host daemon as N.  
**Expected:** Every process used for exact-N joining has provable N provenance and belongs to the owned campaign; no legacy service is silently used as N.  
**Classification:** environment  
**Acceptance impact:** Host responder/grant diagnostics cannot be presented as N until provenance/ownership is resolved. No authenticated grant request or membership mutation attempted. N `start-tailscale.sh` starts its responder only after `start.sh` succeeds; D01 prevents reaching that path. The existing default-port owner must also be resolved for subsequent N admission.  
**Safe continuation:** YES — read-only process/listener/health inspection and independently bounded unauthenticated app refusal. Do not kill, restart, reconfigure, or borrow the legacy responder; do not issue/redeem a grant through it.  
**Evidence:** [N-source-only process search](diagnostics/nitro-responder-metadata.json), [broader process metadata](diagnostics/nitro-responder-all-cwds-metadata.json), [streamed audit procedure](diagnostics/collect_nitro_responder_metadata.py). The `source_sha` field in the latter is the campaign checkout, NOT the legacy process source. Process elapsed age and cwd are the contrary evidence.  
**State changed:** None; /proc metadata and one health GET only. Secret content was not read.  
**Protected Recorder data:** Untouched.  
**GitHub artifact:** https://github.com/Nettking/msh/issues/459  
**Repair:** NONE; environment admission plan required after ownership check, no source defect established.  
**Next diagnostic action:** Listener ownership is now confirmed: PID1422341 owns socket31225091 on tailnet TCP5151; legacy checkout is clean `5dfbcd1ca8426ca1cdca702108021512316ec31b`, not N. Actual-bind health timed out at5.03s. [Listener evidence](diagnostics/nitro-legacy-listener.json). Do not retry it or replace this unrelated service during the sweep. Plan explicit owned candidate responder admission after D01/D02 repairs, resolving existing listener ownership without bypassing authority.

## D03

**Finding:** D03  
**Status:** CONFIRMED (audit-bound false alarm resolved; slow cold load is environmental observation, not established product defect)  
**Candidate SHA:** `0536f03d67eb277e11573c2188d8e820399627e3`  
**Host(s):** Nettking, existing exact-N Flask and pinned Ollama containers  
**Physical stage:** Independent local AI provider before P10/B/CF7; no formal scenario executed  
**Observed:** A single synthetic eight-token chat request for existing hash-verified `llama3.2:3b` exceeded the audit-selected 60 s HTTP limit. N raised `OllamaProbeError: Ollama did not respond within the benchmark limit`, caused by `TimeoutError: timed out`. GPU memory reached 2273 MiB and utilization reached 99%; server canceled model loading when the client closed, returning HTTP 499 after 1m0s. Both containers remained running; manifest unchanged; no OOM.  
**Expected:** The actual N adapter defaults/maxes at 120 s (`OllamaProbeTarget`, `OllamaBenchmarkAdapter.definition`), not 60 s. Corrected source review means the first request does not prove a product deadline failure. Successful inference within the actual bound still needs demonstration; GPU allocation alone is insufficient.  
**Classification:** harness (audit-only 60 s selection) / environment (slow cold load); no independent product deadline failure established  
**Acceptance impact:** Actual unchanged N adapter completed within its 120 s bound in 103.55 s, with 8 tokens, 80.279 tokens/s generation and `not-recommended` per unchanged latency policy. This is a diagnostic result, not physical acceptance or same-device Recorder+AI evidence.  
**Safe continuation:** YES — bounded read-only provider/resource inspection and independent status/authority paths; do not stack inference requests, change limits, download models or bypass authority.  
**Evidence:** [Exact command, timing, traceback and GPU samples](diagnostics/nettking-existing-ollama-inference.json); [audit-only procedure](diagnostics/probe_existing_ollama.py). Command: `C:\wsl\fcp-v1-fba508-nettking-20260910\.venv\Scripts\python.exe -B handoff/diagnostics/probe_existing_ollama.py` from this documentation checkout. Request ran in the existing Flask container through unchanged N `_bounded_json_request`; HTTP timeout 60 s, response cap 32768 bytes. Observation 2026-09-11T12:20:21Z–12:21:24Z.  
**State changed:** Ephemeral model/GPU memory and owned service logs only; no persistent configuration, model download, deployment or membership change.  
**Protected Recorder data:** Untouched; no protected corpus access or container/task mutation.  
**GitHub artifact:** https://github.com/Nettking/msh/issues/458  
**Repair:** NONE  
**Next diagnostic action:** No more inference retries. [Actual default-bound adapter observation](diagnostics/nettking-actual-ollama-adapter.json) confirms completion, existing hash-identical model and `100% GPU`/2.2 GB loaded. [Read-only first-request follow-up](diagnostics/D03-provider-followup.json) records prior cancellation and no OOM. Continue Nitro responder provenance/health and safe authority-path diagnosis. No provider settings or product deadline changed.

## D01

**Finding:** D01  
**Status:** RESOLVED-IN-BRANCH (candidate remains affected; repair not deployed)  
**Candidate SHA:** `0536f03d67eb277e11573c2188d8e820399627e3`  
**Host(s):** Nitro; Nettking retained native evidence  
**Physical stage:** Supported startup/runtime admission before formal P01/P03; no P-test was completed  
**Observed:** Supported `bash start.sh --resume` activation reached running exact-N core images, then exited 1. Direct `/onboarding` returned 302 in 0.0128 s; following `/login` timed out at the unchanged 2 s probe deadline. Native message: `The containers started, but FCP did not become ready.` Updater-start step was not reached.  
**Expected:** Readiness evaluates the direct application response without following a redirect into stateful login rendering; existing authority gates remain separate.  
**Classification:** product  
**Acceptance impact:** Startup/admission is incomplete; invalidates continuation as formal physical acceptance.  
**Safe continuation:** YES — independent metadata and isolated diagnostics which do not require successful supported startup; no repeated login GETs as purported read-only probes.  
**Evidence:** [D01 evidence](diagnostics/D01-readiness.json); original native log digest and exact procedure retained there. Source boundary: N `start.sh` readiness and login context.  
**State changed:** Historical supported activation changed owned Nitro core containers; the diagnostic login request may publish owned human-user metadata. No new deployment for this sweep.  
**Protected Recorder data:** Untouched; no protected container/task/data mutation by the recorded procedure.  
**GitHub artifact:** https://github.com/Nettking/msh/pull/456  
**Repair:** `codex/posix-readiness-direct-response`, HEAD `1a0c634f47f8a247b6d1d2d1a219f5c12590587d`; 25 focused regressions passed, repair withheld.  
**Next diagnostic action:** Inspect current owned Nitro runtime/status metadata, without following login or restarting the launcher; continue independent paths below.

## D02

**Finding:** D02  
**Status:** RESOLVED-IN-BRANCH (candidate remains affected; repair not deployed)  
**Candidate SHA:** `0536f03d67eb277e11573c2188d8e820399627e3`  
**Host(s):** Nettking ordinary discovery client to Nitro Flask  
**Physical stage:** Discovery/admission before enrollment; no formal P-test completed  
**Observed:** Three probes using N `_probe_peer` and its normal 0.75 s default returned no advertisement after 0.798–0.843 s. One 5 s diagnostic probe also timed out. Server subsequently logged 200; sequential client requests can overlap unfinished server work, so these are not independent steady-state measurements.  
**Expected:** Normal startup accepts an approximately 1 s valid reply within a bounded complete scan. Public routing metadata does not request fresh quorum; grants still require real peer verification, existing app authority/fresh quorum and one-use FCP1.  
**Classification:** product  
**Acceptance impact:** Ordinary discovery/zero-touch enrollment remains invalid; advertisement alone cannot confer membership.  
**Safe continuation:** YES — independently addressed metadata/local-provider tests and separately bounded authority diagnostics; do not bypass join authority.  
**Evidence:** [D02 evidence](diagnostics/D02-discovery.json), including original module hash, exact probe procedure and timed observations.  
**State changed:** Historical probe performed HTTP requests and local evidence writes; server authority refresh may advance existing control observations. No explicit identity/topology or source changes.  
**Protected Recorder data:** Untouched; no protected recording access.  
**GitHub artifact:** https://github.com/Nettking/msh/pull/457  
**Repair:** `codex/discovery-routing-budget`, HEAD `143fe7a9082193114af3d34dc437b85f845849a2`; 175 current focused cases pass across retained runs. Neither branch tests nor later diagnostics are physical acceptance.  
**Next diagnostic action:** Avoid retrying the known discovery timeout; inspect independent current runtime/provider/quorum prerequisites on N.

## Retained host context (refresh before dependent diagnostics)

- Nettking: three core containers verified on N; fingerprint `0efb6f56566ac0eb`.
  Owned project `fcp-v1-73c779-nettking`; source
  `C:\wsl\fcp-v1-73c779-nettking-runtime-20260910`; membership pending.
- Nitro: three core containers verified on N; fingerprint `ad885120af1f11cf`.
  Owned project `fcp-v1-fba508-nitro`; source
  `/home/martin/fcp-v1-73c779-nitro-20260910/source`.
  Last retained role LEADER, ready, term 8/index 1547. Readiness D01 remains.
- MSH Recorder / `DESKTOP-N5KI14R`: protected production SHA
  `6fb77c8419ead8a24af03bf6b9e6636c8664dcd8`, container
  `777cc74ab983786ee44c124ea57adef045a5d2f94f44c4eb43b5765117678ccd`,
  image `sha256:204976ddb98d3c3eea3a8ac05dcd7f99be7ba460d58fc4b8204d20255c934208`,
  started `2026-09-09T19:24:27...`, mount `C:/msh/git/data` to `/app/data`.
  Separate campaign voter remains `fba50818658aa8a52ac09b6c7310a9f90cc91d33`;
  fingerprint `8589698c32d44b1f`. Separate clean N source/native Python exists,
  but neither protected production nor voter has been upgraded to N.
- Fixed voters remain Beast, Nitro, MSH Recorder. Nettking is to join as a normal
  member. No recreated Federation, copied identities or topology substitution.
- Existing model `llama3.2:3b`, pinned Ollama 0.32.6; manifest SHA-256
  `a80c4f17acd55265feec403c7aef86be0c25983ab279d83f3bcd3abbcb5b8b72`.
  Previous GPU inference was on an older source and is not N acceptance.

## Hypotheses and environment prerequisites — not newly proved defects

- H01: unavailable Beast control/credential endpoint causes serial quorum-path
  and lifecycle-lock delay. Retained actual TCP connect times were 0.508 s and
  5.005 s; Recorder endpoints connected in 0.064 s and 0.144 s. Serial auth was
  9.339 s, session authority 2.240 s. This establishes a contributor, not exact
  attribution of every HTTP delay or a separately demonstrated grant defect.
- H02: Nettking time service reported stale synchronization/error 2 previously;
  current clock eligibility must be measured before time-sensitive diagnostics.
  Existing UAC request must not be repeated without a new necessity.
- Mixed Recorder runtime is a known admission prerequisite, not proof of a new
  product defect. Only metadata inspection of protected production is allowed.
- Prior private Nitro override exported three services into one image tag;
  corrected only the three private image names before N activation. This was an
  environment/configuration failure; do not repeat the failed build or call it
  another source defect.
- Hosted non-required PR jobs were prevented from starting by GitHub billing;
  no underlying product tests executed. In-flight self-hosted qualification is
  historical work in progress, not the diagnostic sweep's objective.
- H03 (new, not yet an independent confirmed defect): unauthenticated app grant
  POST did not yield expected JSON403: Nettking returned non-JSON after urllib
  redirect handling; Nitro timed out10s. Exact receipts:
  `diagnostics/{nettking,nitro}-unauthenticated-grant.json`. The audit caller
  followed redirects by default, so it may have entered stateful login rendering
  (owned human-user metadata mutation is possible, as in D01); its initial
  state-effects description is too narrow. No secret/grant was supplied or
  printed and protected Recorder data remained untouched. Next: one direct
  no-redirect HTTP POST response per owned app, preserving status/content type
  and route-only Location. Do not infer a grant defect from JSON parse failure.

H03 follow-up: direct no-redirect probes are now retained as
`diagnostics/{nettking,nitro}-direct-grant-boundary.json`. Nitro returns the
expected JSON403 `unauthenticated`, no grant, in1.992s. Nettking returns direct
HTML503 in0.052s. N `auth/routes.py:first_user_bootstrap_gate` intentionally
returns503 for JSON requests when no local human/remote binding exists; Nettking
is still pre-admission. Thus different prerequisite state explains the result,
not a proved grant-authority bypass or independent product defect. Earlier
Nitro10s latency remains a transient/H01 contributor, not independently isolated.
No secrets were supplied. Do not repeat these requests or relax the bootstrap gate.

## Next safe diagnostic action / recovery

1. Fetch this branch; verify the runtime candidate N separately from this
   documentation HEAD. Read the latest table and evidence before any probe.
2. From Nettking/Martin, collect **metadata only**: native host fingerprint and
   RAM/GPU, `tailscale status --json` reduced to expected host online state,
   owned project container/image/SHA/mount metadata, and relay status snapshots.
   On Recorder compare container/image/start/mount metadata with the baseline;
   do not enumerate/read record files or dump environment/credentials.
3. Select a bounded independent local-AI or status path from the resulting
   prerequisites. Read checked-in N probe code before invoking it. Diagnostic
   results go under this branch's `handoff/diagnostics/`; no formal campaign
   assertions are created. Commit/push each confirmed new blocker immediately.

Transport recovery: existing authenticated SSH from Nettking via
`wsl -d Ubuntu --exec /usr/bin/ssh -o BatchMode=yes -o StrictHostKeyChecking=yes
-o UserKnownHostsFile=/mnt/c/Users/Martin/.ssh/known_hosts -o ConnectTimeout=5`;
resolve Nitro or Recorder from current Tailscale metadata, use `martin` for
Nitro and `Utlån` for Recorder. Do not copy credentials or change accounts.
Local audit-only helper locations:
`C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance\ssh_campaign_script.py`
and native audit Python in its parent `.venv\Scripts\python.exe`.

End when remaining paths depend on known failures or cannot be exercised safely.
Then publish a sweep report and the smallest fix set before resuming the final
candidate workflow. ICSE companion work remains outside product source; its
four-page paper must continue to say physical acceptance is incomplete.


## Metadata sweep checkpoint, September 11 12:18 UTC

D01/D02 PRs are draft and cross-link this checkpoint (comments5634234400/5634234748). Nettking and Nitro retain clean N source and exact N core runtime bytes. Nettking GPU is idle with GPU requests configured for Ollama; existing3b model retained and provider RAM cap1536MiB. Native Windows RAM available7509740KiB during this wave. Time service now reports Sync and Last Sync Error0; H02 is not currently reproduced. No host-clock changes.

Protected Recorder container/image/start/mount metadata exactly match baseline; no corpus read/write. Native fingerprint8589698c32d44b1f and separate clean N source confirmed. Separate voter FOLLOWER/index1550. consensus_term/ready are absent fields in its status schema, not failed invariants. Published metadata receipts are diagnostics/sweep-{nettking,nitro,recorder}-metadata.json.

Nitro collector initially included three historical exited successful one-off setup containers alongside one running normal N relay. This is a resolved diagnostic selection error, not an independent candidate defect. Procedure and mechanism: diagnostics/sweep-nitro-collector-attempt1.json. The corrected collector selects running non-oneoff containers without changing them. Local stdout was CP1252 on Windows versus UTF8 from remote Linux; persisted JSON is normalized UTF8. No product script deployed: remote collector was streamed to existing Python stdin.

Next exact action: independently exercise the existing Nettking Ollama container using its existing3b model with bounded short inference, record GPU/loaded-model/runtime observations; inspect unchanged N service-health/update/provenance probes against owned roots. Do not enumerate protected Recorder files. No formal acceptance assertions or additional qualification dispatches.

## Diagnostic checkpoint, September 11 12:22 UTC

D03 supersedes the preceding next action: the inference timed out; evidence was pushed at e04434e8 and issue #458 published before further investigation. No acceptance PASS. The unchanged N diagnostic probes for running-commit-identity, service-health, host-resource-baseline and update-status completed and are stored under `diagnostics/nettking-probe-*.json`. Service-health observes running workers; local Recorder status is absent/stopped/not-started, so this does not prove active recording. Update-status ran against the clean detached N harness, correctly reported unsupported checkout with no network fetch, and does not test the live runtime updater. Storage roots share the C: backing resource; different directories alone cannot isolate a disk-pressure fault. These results are diagnostic observations only.
