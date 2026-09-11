# Federation v1 diagnostic sweep — recover here first

**Mode: DIAGNOSTIC ONLY — NOT PHYSICAL ACCEPTANCE EVIDENCE.**

Runtime candidate / AUTHORITATIVE_SHA:
`0536f03d67eb277e11573c2188d8e820399627e3` (N).
This documentation branch is `codex/federation-v1-diagnostic-sweep-20260911`.
Its documentation commits are not new runtime candidates and must not be deployed.
GitHub's latest version of this file and its linked evidence are the authoritative
diagnostic checkpoint. The previous qualification-first handoff is superseded.

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
