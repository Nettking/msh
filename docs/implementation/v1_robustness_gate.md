# Federation v1 robustness gate

Status: **active release-closeout gate; independent adversarial review pending**

Reviewed: **2026-08-24 Europe/Oslo**

Audit baseline: `main` at `6ba682755082d08ac4b26c7adb85fff8f5546f11` (PR #325 merged)

## Purpose

This document is the systematic robustness gate for the Federation v1 release candidate. It exists because functional correctness, authority correctness, and green CI are not enough to prove that a long-running physical FCP installation fails safely under resource pressure, process failure, dependency loss, or host restart.

It does **not** replace the existing CF7 physical-evidence contract, release scope, authority model, or backup/recovery guide. It adds a closeout layer that must be reconciled before a candidate is frozen for final physical acceptance.

The concise operational property is:

> **No single resource exhaustion or single service failure should make an otherwise healthy FCP host unusable.**

The stronger invariant is:

> A failure or exhausted resource may degrade the affected capability, but must not corrupt committed state, exhaust unrelated host resources, create unbounded restart/work amplification, falsely report authority or health, or require a destructive reset to recover.

## Evidence vocabulary

A finding has exactly one current evidence state. The state describes evidence, not confidence.

| State | Meaning |
| --- | --- |
| `OPEN` | A verified gap exists and no complete closeout has been demonstrated. |
| `REVIEW` | The audit found a credible concern, but impact or product-path relevance still needs independent review. |
| `IMPLEMENTED` | A protection exists in merged production code, but the required evidence is incomplete. |
| `AUTOMATED-PROVEN` | Merged automated tests exercise the relevant property. This is not physical evidence. |
| `PHYSICAL-PROVEN` | The required physical fault/soak scenario passed on an exact candidate with retained evidence. |
| `ACCEPTED-V1-BOUNDARY` | The limitation is explicit and deliberately accepted for v1; no implementation is implied. |
| `DEFERRED` | Useful hardening that is outside the v1 acceptance boundary and does not mask a v1 blocker. |

Rules:

1. Green CI cannot move a physical finding to `PHYSICAL-PROVEN`.
2. A finding that can cause data loss, host exhaustion, authority confusion, or unrecoverable service loss cannot be silently converted to `DEFERRED`.
3. `ACCEPTED-V1-BOUNDARY` requires an explicit operator-visible limitation and a safe failure mode.
4. Per-request/per-object bounds do not count as cumulative storage bounds.
5. Test-only or currently dormant primitives must not be promoted into product blockers without proving the supported product reaches them.

## Non-goals

This gate must not become a pretext to redesign Federation v1. In particular, it does not require:

- Raft, replicated SQLite, Byzantine consensus, or a new coordinator protocol;
- automatic deletion of primary recorder evidence or user-owned uploaded data;
- Kubernetes-style orchestration or a general distributed resource scheduler;
- multi-tier archival/storage policy;
- automatic model eviction;
- hot/live copying of active coordinator SQLite state; or
- OSL/SysML work.

The target is bounded, visible, recoverable degradation using the existing architecture.

## Existing safeguards that should be preserved, not redesigned

The audit found substantial resilience already present:

- The recorder is local-first. Raw evidence is written before derived forms and the durable checkpoint is committed last; archived raw batches are used for deterministic crash recovery (`catalog/mtconnect_recorder/storage.py`, `catalog/mtconnect_recorder/runtime.py`).
- Per-source recorder failures are isolated and backed off, so one MTConnect source does not automatically stop other sources (`catalog/mtconnect_recorder/runtime.py`).
- Recorder-local file logging already rotates at 2 MB with three backups; this is separate from Docker stdout/stderr logging.
- Durable analysis jobs have persisted ownership, finite leases, retry/cancellation/reassignment, duplicate suppression, stale-worker fencing, and one committed-result authority (`catalog/capabilities/`).
- PR #324 fixed live-day starvation, duplicate jobs for moving source snapshots, and lifecycle work being stranded outside a bounded product view.
- PR #321 hardened relay replay cancellation, storage-authority restart behavior, and relay task cleanup on the physical recorder path.
- Flask startup is intentionally isolated from background runtime failure; a broken/degraded runtime does not by itself prevent the web process from starting (`catalog/flask_app/app.py`).
- Recorder telemetry mirror and generic federated JSONL mirror have explicit total quotas.
- Federation coordinator audit history is a bounded ring rather than an append-only audit table.
- PR #325 bounds logical Federation storage ingestion with an allocation/floor and hardens the normal Compose update agents with a host disk preflight plus bounded BuildKit cleanup.
- Backup/recovery already defines a conservative quiesced authority-consistent snapshot and same-installation restore boundary (`docs/backup_recovery.md`).

Robustness work should compose around these properties rather than duplicate them.

# A. Host resource envelope

## H01 — PR #325 disk fix requires physical proof

**State:** `AUTOMATED-PROVEN`  
**V1 disposition:** blocker until physical proof

PR #325 addresses the measured Beast incident:

- dependency installation is no longer invalidated by `FCP_BUILD_COMMIT` on every image build;
- update-agent BuildKit cache is pruned with a keep-storage bound;
- normal Compose update agents check real host free space before Docker build;
- logical Federation storage cannot consume below its configured/derived floor.

The implementation and CI are green, but the Beast incident is not closed by that. The physical closeout is P01/P02 below.

## H02 — no continuous host-level disk-pressure authority

**State:** `OPEN`  
**V1 disposition:** blocker

The application can inspect free space at its configured data path, and the storage allocator can protect its own volume. On Windows Docker Desktop those observations happen inside the Linux VM/container and cannot reliably represent the Windows system drive or the expanding Docker VHDX underneath it. The #325 update preflight sees the real host, but only while an update is being applied.

Required property:

- a host-owned observation path must provide the supported runtime with current real-host free-space/pressure state;
- it must not expose private host paths to Federation peers;
- stale/unavailable host observations must fail conservatively rather than claim `NORMAL`;
- the operator surface must show the pressure state and reason.

This can extend an existing host agent. It does not justify adding an unrelated daemon if the existing launcher/update-agent boundary can own it safely.

## H03 — device-wide disk-pressure response is missing

**State:** `OPEN`  
**V1 disposition:** blocker

Independent writers currently discover pressure independently. The recorder can reach a real filesystem I/O error; analysis, uploads, models and Docker can all write on the same host; storage allocation protects only remote logical-storage ingest.

FCP needs one pressure contract with hysteresis. The names below are requirements, not a mandated implementation API:

| State | Minimum behavior |
| --- | --- |
| `NORMAL` | Normal operation. |
| `WARNING` | Surface the condition and measurements; no destructive action. |
| `PRESSURE` | Refuse new nonessential/large work before it writes: new analysis packaging/jobs, new uploads/imports, model pulls, update builds and other reconstructible materialization. Remote storage must also refuse if the host signal is stricter than its local allocation view. Existing atomic/durable commits may finish when their reserved margin makes that safe. |
| `CRITICAL` | Do not begin new optional writes. The recorder must finish only the already-started durable commit/checkpoint boundary that the threshold reserved room for, then pause capture cleanly and report `HOST_STORAGE_CRITICAL` or equivalent. Existing primary evidence must not be silently deleted. |

The thresholds must leave enough emergency headroom for the largest already-started commit, checkpoint/status update, SQLite/WAL work and orderly shutdown. Recovery after space is restored must not require `--fresh`, volume deletion, or manual database editing.

## H04 — CPU, RAM, process/PID and descriptor pressure are not operationally classified

**State:** `REVIEW`  
**V1 disposition:** independent review must decide minimum v1 scope

Compose defines no FCP-specific memory/CPU/PID limits, and current device inspection reports coarse resource capacity rather than live pressure. This does not automatically mean hard limits should be added: a badly chosen memory limit can create more outages than it prevents.

Independent review should determine the smallest v1 property needed, likely:

- surface OOM/process-exit reason rather than treating it as generic offline state;
- prevent repeated restart amplification after a resource-driven crash;
- prevent new heavy jobs from being scheduled onto a provider already reporting local pressure, if that can be done without a new scheduler;
- prove unrelated services remain usable when one compute/model process exhausts its own resources.

Do not build a general resource scheduler unless physical evidence proves it necessary.

## H05 — model storage and model installation are not under the host pressure contract

**State:** `OPEN`  
**V1 disposition:** blocker for safe low-space behavior; model-retention policy may be deferred

Ollama/model-provider volumes are intentionally persistent and re-downloadable, but FCP sets no cumulative volume quota. A model identifier is validated; its eventual on-disk size is not known by that validation.

Required v1 behavior is conservative refusal under host pressure, not automatic model deletion. P10 exercises this.

# B. Durable and cumulative growth

The key distinction in this section is **per-item bound vs lifetime bound**. A 512 MiB maximum object does not prevent one thousand valid objects from filling a disk.

## D01 — recorder primary corpus grows in several independent representations

**State:** `OPEN`  
**V1 disposition:** blocker for pressure handling; archival/retention policy may be deferred

`DurableRecorderStore` owns at least these cumulative roots:

- `raw/`: compressed MTConnect XML plus manifests;
- `probe/`: probe/model snapshots;
- `observations/`: detailed NDJSON observation batches;
- `jsonl/`: wide FCP-compatible normalized snapshots;
- `gaps/`: loss/gap evidence; and
- `events/`: recorder events.

A normal batch deliberately writes raw XML, detailed observation NDJSON, and wide JSONL before the caller commits the checkpoint. No automatic retention applies to this primary evidence.

V1 must prevent this corpus from taking the host to filesystem failure. It must not solve that by deleting primary evidence without an explicit future retention/archive policy.

## D02 — recorder outbox payloads are compact, but completed-row history is unbounded

**State:** `OPEN`  
**V1 disposition:** cumulative-growth policy required

`compact_completed()` is not a row-retention bound. It rewrites legacy completed payloads into bounded receipts and explicitly performs no deletion/vacuuming. New acknowledgements already store compact receipts. Completed rows can still accumulate indefinitely.

Any closeout must preserve the idempotency/retry evidence that is actually required. Do not call `compact_completed()` and claim cumulative growth is fixed.

## D03 — coordinator/session history has append-only or unretired ledgers

**State:** `OPEN`  
**V1 disposition:** cumulative-growth policy required

`session_events` is append-only and is part of authoritative replay. The audit also found durable idempotency/request history such as `accepted_requests` with no retention path during this review. Low-frequency tables such as enrollment/token history are less urgent but belong in the same inventory.

A compaction/snapshot design must preserve replay correctness, immutable creator provenance, idempotency, revocation and human-auth projections. Deleting old events merely because they are old is not acceptable.

## D04 — analysis content store has per-object limits but no total-store GC/quota

**State:** `OPEN`  
**V1 disposition:** cumulative-growth policy required

`LocalArtifactContentStore` enforces a maximum size for each object and writes atomically. The production analysis runtime stores plan bodies, packed slices and result artifacts under `results/capabilities/artifacts`. New source signatures create new durable identities. No total byte budget or artifact lifecycle reclaim was found.

The isolated per-attempt workspace is already deleted in `finally`; do not conflate that good workspace cleanup with persistent artifact retention.

## D05 — job, attempt, command, grant, publication and analysis-registry history is cumulative

**State:** `OPEN`  
**V1 disposition:** cumulative-growth policy required

The analysis registry now marks terminal jobs `settled_at` so lifecycle scans stay proportional to unfinished work, but terminal rows remain in the product history. The underlying job/lifecycle store retains jobs, attempts, idempotency commands, audit/results/cancellation state. Artifact authority retains grants, registered descriptors, publications and artifact audit history; only temporary publication reservations are aged out.

This is mostly metadata, so a host-pressure guard may be sufficient for the immediate release, but the gate must make the lifetime-growth decision explicit and test it at realistic history size.

## D06 — uploads and workflow results are bounded per operation, not over installation lifetime

**State:** `REVIEW`  
**V1 disposition:** pressure integration required; retention remains operator policy

Browser uploads are bounded per batch (file count, per-file size and total batch size), staged safely and published only when verified. Successful published uploads are intentionally durable under `data/uploads`. `results/workflows` is also persistent user/research output.

These are not caches and must not be automatically deleted. They must participate in host-pressure admission control so a new upload/analysis is refused before it consumes the emergency floor.

## D07 — container stdout/stderr retention is not bounded by FCP Compose

**State:** `OPEN`  
**V1 disposition:** easy blocker

The recorder's own file logger rotates, but `docker-compose.yml` sets no `logging:` rotation/size policy for Flask, relay, recorder, Ollama or model-provider services. Actual daemon defaults are host configuration, not an FCP guarantee.

Add a conservative FCP-owned log bound or prove that the supported Docker deployment config supplies one. Logs needed for fault diagnosis must remain useful.

The POSIX launcher also appends update-agent output to `data/federation/update-agent/agent.log` without a rotation rule; that file belongs in the same review.

## D08 — superseded Docker images have no FCP lifecycle bound

**State:** `OPEN`  
**V1 disposition:** v1 closeout item

#325 bounds update-path BuildKit cache, not image history. Superseded FCP images can remain after successful activations. Any cleanup must retain whatever exact image/source evidence the update/recovery contract genuinely relies on and must never touch volumes.

## D09 — generic/durable object-transfer storage needs product-path classification

**State:** `REVIEW`  
**V1 disposition:** do not block v1 unless the supported product reaches the risk

The generic `FilesystemObjectTransferChunkStore` has a process-local byte counter. The durable subclass reconstructs staged-byte indexes from its journal after restart, which is the stronger path. The resumable journal caps incoming/outgoing record count, but completed records are not removed by `cleanup_abandoned()`, which targets unfinished old work.

Current code search during this audit did not prove that `ResumableChunkTransferEndpoint` is instantiated on the supported product path outside tests. Claude must classify this accurately: latent/test-only concerns belong in follow-up, not in a release blocker disguised as production evidence.

# C. Process and service supervision

## S01 — Flask, relay and Compose-managed recorder lack Docker healthchecks

**State:** `OPEN`  
**V1 disposition:** blocker

Ollama has a healthcheck. Flask, relay and recorder currently rely on process state plus `restart: unless-stopped`.

Required semantics:

- **liveness**: is the process/event loop alive enough to continue?;
- **readiness**: can it perform its owned function?;
- **degraded dependency**: is the process healthy while Federation/MTConnect/another dependency is unavailable?;
- health must not grant membership, leader, provider, storage or job authority.

A Flask process should be allowed to remain live while background runtime/Federation is degraded; the existing Flask startup isolation should be preserved.

## S02 — restart-storm amplification is unbounded at the Compose layer

**State:** `OPEN`  
**V1 disposition:** blocker

`restart: unless-stopped` can repeatedly restart a process whose deterministic failure remains present. Beast demonstrated the general risk during filesystem failure.

The v1 behavior needs a bounded or observable failure state such as `STARTUP_FAILURE`, `RESOURCE_FAILURE` or equivalent after repeated failures, with backoff and operator visibility. It must not turn a stopped contribution into a falsely healthy device or spin continuously against a full disk.

## S03 — native Windows recorder supervisor does not restart ordinary crashes

**State:** `OPEN`  
**V1 disposition:** blocker unless explicitly accepted as a supported-service limitation

The native supervisor owns update/trial replacement safely, but on an ordinary recorder child exit it exits with the child code. Therefore an unattended supervised recorder can stop permanently after an ordinary crash unless something outside FCP restarts the supervisor.

A v1 solution can be small: bounded restart/backoff with crash-loop fencing, or an explicit supported external service-manager contract. It must preserve operator stop semantics and must never loop a corrupting writer indefinitely.

## S04 — recorder status I/O can amplify disk failure into process failure

**State:** `OPEN`  
**V1 disposition:** covered by H03 plus targeted regression

Per-source capture wraps exceptions and backs off. `publish_status()` is outside that per-source boundary and writes its heartbeat atomically to disk. A status-file I/O failure can terminate the process; the `finally` path attempts another status write before executor cleanup. The physical Beast incident produced an `OSError: [Errno 5] Input/output error` on this path.

Preferred closeout is to prevent the recorder from reaching filesystem failure through host-pressure headroom. Independent review should also decide whether heartbeat/status write failure needs its own non-fatal/finally-safe handling.

## S05 — long-lived background threads need one operator-visible health inventory

**State:** `REVIEW`  
**V1 disposition:** integrate only where an existing monitor/status cannot express failure

Storage-authority supervision and analysis scheduling already have substantial retry/reconciliation logic. Do not add a second supervisor. Instead verify that death/stall of each required long-lived background thread can be distinguished from a healthy-but-idle state on an operator surface.

# D. Update and startup robustness

## U01 — update BuildKit cache/preflight is automated-proven, not physical-proven

**State:** `AUTOMATED-PROVEN`  
**V1 disposition:** physical blocker

Covered with H01/P01. Do not close from CI alone.

## U02 — normal supported launchers rebuild outside the update disk/cache policy

**State:** `OPEN`  
**V1 disposition:** blocker until bounded or physically proven harmless

Both `start.cmd` and `start.sh` run:

```text
docker compose build relay flask recorder
```

on normal startup. They do not call the #325 host disk preflight or post-build BuildKit keep-storage cleanup. The Dockerfile reorder makes unchanged/repeated builds much cheaper and fixes the measured 1 GiB dependency-layer churn, but that is not the same as a bounded host policy.

Closeout options include reusing the same host preflight/cache primitive from startup, or proving through repeated ordinary starts that the supported path is bounded and then defining the remaining limit. Do not create a second incompatible disk policy in each launcher.

## U03 — update resource preflight happens after source fetch/fast-forward

**State:** `REVIEW`  
**V1 disposition:** independent review must decide whether split source/runtime state is sufficiently safe

The Windows update flow validates/fetches the target and fast-forwards the source checkout before `Assert-DiskPreflight`; the preflight is correctly before Docker build and before Flask is stopped. If the host is too short for the build, FCP can therefore refuse with the source checkout already advanced while the running containers remain on the previous commit.

FCP already models current/source and running commit separately, so this may be a safe resumable state rather than a defect. The review must either prove that recovery/idempotency/operator state is sufficient, or move the resource refusal before source mutation. Do not demand rollback merely for aesthetic transactionality.

## U04 — missing-model pull occurs after build without a dedicated host-space check

**State:** `OPEN`  
**V1 disposition:** blocker for low-space activation

After build and service start, the Compose update agent verifies/installs the configured Ollama model. A missing model can be a large writer after build consumed part of the 10 GiB preflight headroom. The normal launchers have the same general issue.

The required property is simple: model installation must not be allowed to cross the host emergency floor. P10 verifies it.

## U05 — successful update does not retire superseded images

**State:** `OPEN`  
**V1 disposition:** same closeout as D08

Any cleanup must be narrowly scoped to unused FCP images and preserve volumes/state. `docker system prune` is not an acceptable implementation shortcut.

# E. Coordinator and disaster recovery

## C01 — supported quiesced backup/recovery boundary exists

**State:** `IMPLEMENTED`  
**V1 disposition:** preserve

`docs/backup_recovery.md` correctly requires services stopped for an authority-consistent backup, protects `data/`, auth state, `.env` where used, results when required, and retained relay/coordinator state, and distinguishes same-installation recovery, replacement members and creator loss.

Do not replace this with live SQLite copying for v1.

## C02 — exact-candidate backup/integrity/restore rehearsal is still physical evidence

**State:** `OPEN`  
**V1 disposition:** blocker already implied by release documentation

The exact candidate must demonstrate:

- quiesced backup;
- SQLite integrity/quick-check of the restored coordinator databases where applicable;
- isolated restore with the original instance offline;
- same identity and Federation resume where same-installation recovery is intended;
- recorder checkpoints/data and human-auth state where applicable; and
- no repair by deleting selective state.

The backup archive itself must remain private and outside Git.

## C03 — creator/coordinator authority is not replicated

**State:** `ACCEPTED-V1-BOUNDARY`  
**V1 disposition:** explicit limitation, not a blocker

Operational leader failover does not transfer immutable creator provenance, creator-backed human credential authority, or create a quorum-backed replacement coordinator. If creator identity/authority is irrecoverable, v1 may require creation of a new Federation.

Do not build Raft/distributed consensus in this gate.

## C04 — coordinator disappearance must be represented as control-plane unavailability, not invented member failure

**State:** `REVIEW`  
**V1 disposition:** physical semantics must be checked

When the authoritative coordinator/relay disappears, surviving devices must not manufacture leadership/authority or rewrite every member as definitively failed from missing control-plane evidence. P09 tests the product-visible state. If current semantics already satisfy this, close with evidence rather than new code.

# F. Dependency and network degradation

## N01 — sustained Federation outage while recorder continues is not yet physical-proven

**State:** `IMPLEMENTED`  
**V1 disposition:** physical blocker

Recorder capture is deliberately independent of Federation acknowledgement and publication is durable/retryable. Prove the full physical path under a sustained outage and backlog catch-up (P06).

## N02 — MTConnect source failure is isolated in code; physical multi-source behavior remains evidence

**State:** `IMPLEMENTED`  
**V1 disposition:** include in soak/fault campaign

Per-source exceptions update source status and use bounded backoff while the runtime continues other sources. Verify one dead source does not stop another healthy source or create busy polling.

## N03 — Ollama/model-provider loss should not take down unrelated Federation/recorder/workbench services

**State:** `REVIEW`  
**V1 disposition:** prove isolation; implement only if physical evidence fails

The services are separately composed and Flask startup can degrade. The final candidate should still demonstrate that an unavailable model provider does not stop recording, relay, membership or read-only operator access.

## N04 — wall-clock skew assumptions are not explicitly closed

**State:** `REVIEW`  
**V1 disposition:** likely documented/physical assumption unless a correctness defect is found

Pairing, update requests, leases, grants, health TTLs and other contracts use bounded wall-clock windows. This audit found no explicit clock-skew/NTP policy. Claude should determine whether existing tolerance and authenticated timestamps are sufficient for trusted v1 deployments or whether a modest-skew regression/operational prerequisite is needed. Do not add a distributed clock service.

# G. Durable-writer inventory

This table is the checklist for future reviews. “Cumulative bound” means the installation cannot grow indefinitely through that path under ordinary accepted work; it is stronger than a maximum request/object size.

| Writer / store | Persistence | Current bound | Cumulative bound? | v1 treatment |
| --- | --- | --- | --- | --- |
| Recorder raw XML + manifests | primary evidence | per batch/protocol only | **No** | H03/D01 pressure stop; no auto-delete |
| Recorder detailed observation NDJSON | primary evidence | per batch/protocol only | **No** | H03/D01 |
| Recorder wide JSONL | primary/compatibility evidence | per batch/protocol only | **No** | H03/D01 |
| Recorder probe/gap/event evidence | primary metadata/evidence | event shape only | **No** | D01 |
| Recorder checkpoint/status | current state | atomic replacement | effectively bounded | preserve emergency write headroom |
| Recorder file log | diagnostic | 2 MB × current + 3 backups | **Yes** | preserve |
| Recorder publication outbox | delivery/idempotency | completed payload receipts compact | **No row-history bound** | D02 |
| Federation logical-storage provider | remote durable data | #325 allocation + floor | **Yes for accepted remote writes** | physical exhaustion acceptance |
| Recorder telemetry mirror | reconstructible mirror | batch + materialization quota | **Yes** | preserve |
| Generic federated JSONL mirror | reconstructible mirror | total mirror quota | **Yes** | preserve |
| Browser upload staging | transient | batch/queue limits + cleanup | bounded per active request | preserve |
| Published browser uploads | user data | 1 GiB default per batch | **No** | admission control; no auto-delete |
| Analysis workspaces | transient | per-attempt cleanup in `finally` | normally bounded by active work | preserve/test crash cleanup |
| Analysis plans/slice/result content store | durable analysis artifacts | per-object max | **No** | D04 |
| Analysis job/attempt/command/grant/publication history | durable metadata | per-message/schema bounds | **No retention found** | D05 |
| Workflow outputs | user/research results | workflow-specific | **No total bound found** | admission control/operator policy |
| Coordinator audit log | diagnostic/security audit | bounded row ring | **Yes** | preserve |
| Coordinator session events | authoritative replay | event/replay-page bounds | **No** | D03 |
| Coordinator accepted/idempotency request history | authority metadata | request shape | **No retention found** | D03 review |
| Relay/coordinator SQLite WAL | authority state | normal SQLite/WAL behavior | not a standalone lifetime policy | backup/integrity + host pressure |
| BuildKit cache through Update all | reconstructible cache | keep-storage cleanup after #325 | bounded on that path | U01 physical proof |
| BuildKit cache through normal launch/manual builds | reconstructible cache | Dockerfile caching only | **No FCP path-wide bound** | U02 |
| Superseded FCP images | reconstructible runtime | no FCP retirement | **No** | D08/U05 |
| Docker container stdout/stderr | diagnostic | daemon-dependent | **No FCP bound** | D07 |
| POSIX update-agent `agent.log` | diagnostic | append | **No** | D07 |
| Ollama/model-provider volumes | re-downloadable models | requested model only | **No FCP quota** | H05/U04 |
| Branch-trial worktrees | reconstructible trial source | bounded retained worktree count | **Yes by current trial policy** | preserve |
| Durable resumable-transfer journal | transfer metadata/staging | staged bytes + record-count bound | completed-record retirement unclear | D09 product-path review |

Any newly introduced durable writer must be added here before it can be considered v1-safe.

# H. Service supervision matrix

| Component | Current restart/health behavior | Gate requirement |
| --- | --- | --- |
| Flask Compose service | `restart: unless-stopped`; no Compose healthcheck; background runtime startup isolated | add meaningful health without making dependency degradation equal process death; detect restart storm |
| Relay Compose service | `restart: unless-stopped`; no Compose healthcheck | liveness/readiness + restart-storm visibility |
| Managed recorder Compose service | `restart: unless-stopped`; status heartbeat exists; no Compose healthcheck | health should include fresh heartbeat/capture state without declaring source outage equal process death |
| Ollama | Compose healthcheck exists | prove failure isolation from unrelated services |
| Native Windows recorder | supervisor owns updates/trials; ordinary child crash exits supervisor | bounded crash restart/backoff or explicit external service-manager contract |
| Storage-authority/background runtimes | internal retry/reconciliation exists in several paths | expose required-thread failure; do not add duplicate supervisors |

# I. Update/start transaction review

The normal Windows Compose update path currently has this broad order:

```text
validate/fetch approved main
  -> fast-forward source when needed
  -> prove source + clean build context
  -> preserve relay volume selection
  -> host disk preflight
  -> docker compose build relay flask recorder
  -> bound BuildKit cache
  -> start relay/ollama/recorder
  -> ensure configured Ollama model
  -> stop Flask
  -> saved-setup resume
  -> start Flask
  -> prove exact running commit + required services
```

This order has useful properties: the disk check is before Docker build and before Flask is stopped. It also creates the U03/U04 review points above.

The normal `start.cmd`/`start.sh` path currently performs the Docker build and model verification/install without the update-agent preflight/cache lifecycle. U02 requires those supported entry points to share one consistent host-resource contract or be physically proven bounded.

The desired invariant is not “everything must rollback.” It is:

> Every resource-dependent refusal must happen before the first mutation that would make the currently healthy runtime unrecoverable, or the intermediate state must be explicitly durable, resumable, operator-visible and safe.

# J. Physical robustness campaign

The final candidate must run these tests on physical systems. Existing CF7 evidence rules still govern provenance and redaction. These results are additional release evidence, not a substitute for CF7 scenarios.

## P01 — repeated `Update all devices` growth test on Beast

- Start from an updater-capable old commit with enough host free space.
- Record: Windows free bytes, Docker VHDX physical size, `docker system df`, FCP data/results sizes, relay volume size and model-volume size.
- Perform at least three distinct real update activations through the supported Federation update path without weakening approved-main validation.
- Record the same measurements after every activation.
- Require BuildKit cache to remain within the configured lifecycle rather than increase by the previous ~3 GiB/activation pattern.
- Require all FCP services to recover on the intended exact commit without `--fresh`, volume deletion or state repair.

If a dedicated acceptance harness is needed to generate multiple exact main commits, it must exercise the same host build/cache code and may not bypass update authority.

## P02 — insufficient-disk update refusal

- Bring host free space below the configured update requirement without corrupting FCP state.
- Request an update.
- Require safe refusal while the existing runtime remains usable.
- Confirm no volume/state deletion and an explicit operator reason.
- Restore space and complete a later update without reset.

This scenario is particularly relevant on Beast because its post-recovery free space was below the new 10 GiB update preflight.

## P03 — repeated ordinary launcher starts

- Run normal `start.cmd` on Windows and `start.sh` on POSIX repeatedly against unchanged and changed source where appropriate.
- Measure host free space, Docker image/cache/log growth and service state.
- Require no unbounded cache slope outside the Update-all path.

## P04 — recorder reaches WARNING/PRESSURE/CRITICAL disk states

- Keep MTConnect producing real batches.
- Reduce real host headroom in a controlled way.
- Prove optional work is fenced before the emergency floor.
- At critical pressure, require current recorder commit/checkpoint to finish safely and later capture to pause without deleting evidence.
- Restore space and resume without `--fresh`.
- Verify sequence/checkpoint continuity and raw/derived integrity.

## P05 — Docker/service failure injection

Independently restart/crash:

- Docker Desktop/daemon;
- Flask;
- relay;
- managed recorder; and
- Ollama/model provider.

Require unrelated services to remain usable where architecture permits, state to survive, and restart loops to be bounded/visible.

## P06 — native recorder ordinary crash

Kill/fail the supervised recorder child in a way that is **not** an update/trial/operator stop. Require the chosen v1 supervision contract to recover it or explicitly surface a terminal service-manager failure without infinite churn. Verify checkpoint continuity.

## P07 — Federation unavailable for at least one hour while recording

- Disconnect relay/control-plane reachability while MTConnect remains available.
- Require local recorder capture/checkpoints to continue.
- Require publication backlog to remain durable.
- Restore Federation.
- Require bounded catch-up without duplicate committed batches or sequence loss.

## P08 — logical storage allocation exhaustion

Drive a real storage authority to its configured budget/floor. Require refusal before partial commit, existing committed reads to remain valid, catalogue/control surfaces to stay usable, and recovery after capacity is restored. Automated coverage exists; this is the physical counterpart.

## P09 — interruption during durable writes and coordinator disappearance

Exercise controlled process/power interruption around:

- recorder raw/derived/checkpoint commit boundaries;
- relevant SQLite transaction/WAL activity; and
- coordinator/relay disappearance.

Require recovery from committed evidence, no fabricated continuity, no self-promotion, and an accurate control-plane-unavailable state on surviving nodes.

## P10 — model pull under low host space

With the configured model absent, approach the host floor and trigger the supported model-install path through startup/update. Require refusal before the model can exhaust the host and keep unrelated FCP state/services recoverable.

## P11 — exact-candidate backup and restore rehearsal

Run the existing quiesced procedure, validate SQLite integrity on the recovered authority state, restore in isolation and prove identity/Federation/auth/checkpoint continuity as applicable.

## P12 — 24-hour growth and stability soak

Run the intended physical topology for at least 24 hours with recorder capture, Federation publication and background analysis active. Sample at fixed intervals:

- host free bytes;
- Docker VHDX size where applicable;
- Docker build cache/images/container-log use;
- recorder raw/probe/observations/jsonl/gap/event sizes;
- outbox database rows/size and pending count;
- session-event/coordinator database size;
- analysis artifact/job database size;
- workflow/results size;
- model-volume size;
- process restart count/health;
- publication and analysis backlog; and
- memory/CPU pressure observations where available.

The purpose is to measure **growth slopes**, not merely final free space. Every unexpected monotonic producer must be explained as primary/operator-owned data, explicitly bounded, or promoted to an open finding.

# K. Accepted/deferred boundaries

The following should not block v1 if the failure behavior above is satisfied:

| Boundary | State | Rationale |
| --- | --- | --- |
| Replicated creator/coordinator consensus | `ACCEPTED-V1-BOUNDARY` | documented creator-loss recovery boundary; do not add Raft |
| Automatic deletion/archive of primary recorder evidence | `DEFERRED` | requires an explicit retention/archive policy; v1 pauses safely instead |
| Automatic deletion of user uploads/results | `DEFERRED` | operator-owned data; admission control protects host |
| Sophisticated CPU/RAM-aware distributed scheduling | `DEFERRED` unless H04 finds a concrete blocker | avoid scope expansion without evidence |
| Automatic model eviction | `DEFERRED` | pressure refusal is sufficient for v1 |
| Long-running behavioural probation for recorder branch trials | `DEFERRED` | startup/fallback contract is separate; soak covers release runtime |
| Full hot/online coordinator backup | `DEFERRED` | quiesced authority-consistent backup is the supported v1 model |
| Dormant/test-only object-transfer lifecycle concerns | `DEFERRED` if D09 confirms no supported product path | do not invent a production blocker |

# L. Implementation order after independent review

Do **not** implement this list until the independent audit below is reconciled.

If the findings survive review, the preferred order is:

1. **Resource envelope:** H02/H03, U02/U04 and D07 so the host cannot be driven into the failure mode already observed.
2. **Supervision:** S01/S02/S03/S04 so failures degrade visibly rather than churn or silently stop.
3. **Cumulative-history policy:** D02-D06/D08, using deletion only for explicitly reconstructible/retirable state and host-pressure admission for primary/operator data.
4. **Coordinator closeout:** C02/C04 and any safe session-event compaction/snapshot decision from D03.
5. **Physical campaign:** P01-P12 on the exact candidate.
6. Only after these are reconciled should the candidate be frozen for final CF7 evidence and release-tag review.

# M. Exit criteria

This robustness gate is ready to close only when:

- every v1 blocker above is `PHYSICAL-PROVEN`, or deliberately changed to `ACCEPTED-V1-BOUNDARY` with a safe/operator-visible failure contract;
- no unresolved `REVIEW` item can plausibly cause authority confusion, data corruption/loss, host exhaustion or silent service loss;
- every durable writer in section G has a deliberate lifetime policy: cumulative bound, pressure admission, operator-owned retention, or accepted boundary;
- repeated update and ordinary-start growth no longer recreate the Beast failure mode;
- a full disk-pressure event recovers without destructive reset;
- Compose and native-recorder failure behavior is bounded and observable;
- exact-candidate backup/restore passes;
- the 24-hour soak has no unexplained growth or restart slope; and
- CF7 acceptance flags remain unchanged until their separate evidence-backed review.

# N. Independent adversarial review contract

The next step is **review, not implementation**.

The reviewer must start from current `main` and attempt to falsify this document. For every finding or disagreement:

1. cite concrete production file/function/line evidence;
2. identify any existing protection this audit missed;
3. distinguish supported product paths from test-only, historical or dormant primitives;
4. distinguish per-request/per-object bounds from cumulative lifetime bounds;
5. identify whether the failure can affect authority, committed data, host availability, only one optional capability, or only diagnostics;
6. classify it as v1 blocker, review, accepted boundary or post-v1 hardening;
7. challenge the proposed pressure actions for possible data loss/deadlock/authority side effects; and
8. propose the smallest correction to the gate before proposing code.

The reviewer must specifically challenge:

- whether H02/H03 can reuse the existing host update-agent/launcher boundary rather than add a new daemon;
- whether ordinary `start.cmd`/`start.sh` builds in U02 can still produce dangerous cache growth after the Dockerfile reorder;
- whether U03's source-before-preflight ordering is already safely resumable;
- whether U04 can know/protect model-pull headroom without introducing brittle model-size guesses;
- whether S03 should restart native recorder crashes itself or rely on an external service manager;
- whether D02/D03/D05 can be compacted without breaking idempotency/replay/audit semantics;
- whether D09 is actually reachable in the supported installed product;
- whether the recorder critical-pressure boundary can always leave enough room to finish its current raw/derived/checkpoint transaction;
- whether current control-plane outage semantics already satisfy C04;
- whether modest wall-clock skew can violate ownership/lease/update safety; and
- what material robustness scenario is still missing from P01-P12.

**Do not modify code, documentation, branches, PRs or acceptance flags during this review.** Return a structured audit first. Implementation starts only after the findings are reconciled.
