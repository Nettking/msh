# Frozen M physical admission

AUTHORITATIVE_SHA: `9b286f931497bf6291e215f6340443c5162826b0`.
Actual final-main qualification is complete and must not be rerun.
[Freeze](diagnostics/authoritative-candidate-9b286f93.json),
[clean revalidation](diagnostics/main-9b286f93-revalidation-review.json).

The checked-in planner exited2 because the discovery module is an unknown impact
path. It denies all carry-forward and requires all12 fresh CF7 observations.
This is the intended fail-closed decision, not a software qualification failure.
No impact-map change is needed. There are zero previous valid physical passes.

Current runtime: Nettking's three M cores are verified with actual image/env/source
hashes and preserved mounts/model. Nitro source is M and guarded supported startup
is running; last verified cores and host responder were N. Follow the current
operation in QUALIFICATION_COORDINATION.md; never launch a duplicate. Protected
Recorder production6fb77c and separate voterfba508 remain unchanged. Its operative
Python3.12.10/fingerprint and staged clean N source are verified. D04/#459 is
resolved for current N ownership; M responder provenance still needs verification.

## Automation decision

No blanket `-Action automate`. The separate harness/runtime roots need the
checked-in runner's explicit `--runtime-binding`. Execute individual reviewed
probes/actions only after runtime admission. The nine reviewed harness/contract
files are byte-identical between N and M (hashes in revalidation review); prior
side-effect analysis still applies. M startup changes only direct readiness,
and Compose additionally propagates the responder port. Verify the resolved
configuration on each host; never infer live source from disk HEAD alone.

| Full scenario | Stateful/disruptive work | Boundary |
|---|---|---|
|P01|Supported activations/build interruption/cache writes|Owned checkout/project/builder; serialized mutation; exact per-activation receipts|
|P02|Disk/inode capacity pressure|Independent bounded backing resource; shared C:/Docker storage is not isolated|
|P03|Restart/competing mutation/network or AI isolation|Owned targets and host lock; no concurrent launcher/updater or unrelated network effects|
|P04|Active concurrent recording/reserve pressure/pause|Synthetic sources, owned recording roots and isolated pressure resource|
|P05|Process kills/database/model/stream faults|Owned processes/databases only, established baseline and recovery|
|P06|Supervision stop/restart|Owned chain; no protected Recorder process or CI account changes|
|P08|Storage pressure/refusal/recovery|Dedicated bounded resources; no host-wide exhaustion|
|P09|Durable writes/crash/control-loss/clock faults|Owned workload; preserve fixed voters and quorum outside reviewed fault window; no host clock skew|
|P10|Provider/model faults/install refusal|Reuse hash-verified model; confine faults to campaign provider; no unnecessary download|
|P11|Quiesce/backup/copy/restore|Campaign-only data, independent destination and isolated restored root; no live protected corpus scan|

All ten full scenarios are stateful or disruptive. Narrow automated probes do
not perform all of that work and cannot turn preparation or READY into PASS.
Review each target even for read-only SQLite scans, capture continuity and corpus
enumeration: protected Recorder record contents remain excluded.

P07 needs real>=3600s with active capture, required aged corpus/backlog and
recovery. P12 needs real>=86400s of simultaneous capture/publication/analysis.
Do not overlap an outage/fault window with the stable soak. Neither is started.
Recorder+AI must be simultaneous on the same device; remote AI is no substitute.

## Admission order and next action

1. Fresh read-only host/runtime metadata, active updater and pending-request
   checks. Confirm existing project/mount/model/identity boundaries and headroom.
   Reverify protected Recorder container/image/start/mount metadata without
   reading recording contents. Verify fixed-voter readiness before any relay restart.
2. Stage exact M source and immutable per-service build configuration; preserve
   data/results, model mounts and fixed-voter topology. Source advancement and
   supported normal startup share the checked-in mutation lock. Never `--fresh`.
3. Admit each owned runtime in a controlled order, verify actual image/env/source
   bytes and responder process provenance, then create fresh runtime bindings.
   Keep the protected production Recorder outside mutation scope.
4. Use checked-in wrappers to prepare hosts only when their operative context
   is verified. Start individually safe scenarios and real timed work when the
   required membership/capture/isolation prerequisites are established.

Discovery repairs have software PASS; ordinary-path physical verification is
still pending on M. Any new product repair stops dependent physical qualification
and follows the user's PR/qualification/merge/revalidation process.
