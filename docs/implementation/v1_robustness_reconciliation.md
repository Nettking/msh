# Federation v1 robustness review reconciliation

Status: **authoritative implementation input; independent review reconciled; implementation not started**

Reviewed: **2026-08-24 Europe/Oslo**

Code baseline reviewed: `main` at `1bcd9d4ac3b9543afc00254147d85a3df4e9c693`.

Related documents:

- [Federation v1 robustness gate](v1_robustness_gate.md) — initial systematic gate and physical-campaign design;
- [Disk accounting audit](disk_accounting_audit.md) — disk-specific forensic accounting and Beast evidence; and
- [Current task handoff](current_task_handoff.md) — current repository sequencing and authority boundaries.

This document records the reconciliation of an independent adversarial review against the production code. Where a classification or requirement here differs from `v1_robustness_gate.md`, **this document governs the next implementation step**. The original gate remains useful as the evidence vocabulary, durable-writer inventory, non-goals and first-pass audit trail.

No acceptance flag changes follow from this review. No physical claim is upgraded by code inspection.

## 1. Reconciliation outcome

The independent review materially improved the gate. Its core blocker set is accepted, with five important corrections to scope:

1. The `update.cmd` problem is a **supported-path release-safety defect**, not a meaningful privilege escalation. A local host administrator can already alter the checkout. The defect is that a documented product command contradicts FCP's approved-repository/main/exact-runtime/preflight guarantees and can accidentally run an arbitrary configured upstream/branch.
2. U03 does **not** require rollback. The Compose host update path deliberately represents `source ahead / runtime old` as an activation-required state; retry is a valid recovery. Fetch-space consumption remains part of the resource contract.
3. D09 is **deferred**. The journal-backed `ResumableChunkTransferEndpoint` exists and is tested, but no supported installed-product instantiation was found.
4. H04 is narrowed to concrete memory/OOM and process-isolation failures. A general CPU/RAM-aware scheduler, blanket container quotas and PID-resource framework are not v1 requirements without physical evidence.
5. S01 is a semantic-health requirement, not a prescription to add Docker `healthcheck:` everywhere. Docker health alone neither restarts a container nor expresses FCP authority/readiness/degraded-dependency semantics.

The following independent findings were verified directly in current production code and are accepted as real:

- recorder HTTP bodies and parsed batches have no finite total response/observation bound;
- up to eight recorder source workers may be active concurrently;
- continuity validation materializes the entire expected integer range;
- timestamp-derived day components are not path-confined before they are joined into recorder storage paths;
- recorder crash recovery scans historical manifests recursively on the ordinary capture path;
- discontinuity recorder events are timestamped, content-addressed files and can accumulate under repeated bad-source behavior;
- recorder publication reconciliation scans historical archives, one poisoned archive can abort a global pass, and `sqlite3.Error` is not among the publication worker's expected retry exceptions;
- normal `start.cmd` and `start.sh` rebuild FCP images outside the #325 update-agent preflight/cache lifecycle;
- `start-tailscale.cmd` stops an existing healthy Flask container before entering that ordinary build/model path;
- the documented `update.cmd` performs `git pull --ff-only` against the checkout's configured upstream and then starts FCP, despite documentation describing it as approved-main safe;
- launcher builds and host-agent source/build mutation are not covered by one shared host-mutation lock;
- model pulls are large persistent writes with no shared host-space admission and the normal launchers currently gate Flask startup on the required model;
- the Windows native-recorder supervisor exits on an ordinary child failure rather than owning bounded restart/backoff;
- recorder status-file I/O can terminate the recorder outside the source-error boundary and the shutdown path attempts another status write;
- the relay stale-heartbeat task can shut the relay server/connections while the provider process continues waiting for an OS signal, leaving a running-but-dead container;
- the analysis scheduling thread can terminate on database exceptions that are not in its expected error set and has no separate required-thread health surface;
- the tailnet responder PID file contains only a PID and can therefore target a reused unrelated PID after a stale-file/power-loss scenario;
- several authoritative/reconstructed session consumers have fixed lifetime replay ceilings, and at least some return a partial projection rather than proving the authoritative end revision was reached;
- analysis slice archives are written directly to their final deterministic name and retries trust an existing file, so a hard kill can leave a truncated artifact that is subsequently treated as the existing input;
- upload staging can be created before its durable database batch record, so a hard kill can strand a large staging directory that startup reconciliation cannot discover from the database;
- the documented backup procedure creates its destination inside the checkout, does not fence all host-side writers, can copy onto the same nearly-full source filesystem, and may pull a helper image only after Compose has been stopped; and
- storage lease expiry is evaluated against the provider's local wall clock, so bounded clock skew is a real correctness prerequisite rather than only an availability concern.

## 2. Release invariant

The original gate invariant stands:

> A failure or exhausted resource may degrade the affected capability, but must not corrupt committed state, exhaust unrelated host resources, create unbounded restart/work amplification, falsely report authority or health, or require a destructive reset to recover.

The independent review adds two necessary corollaries:

> A bound is not a usable resource guarantee until the operation consuming the resource has a finite maximum or a streaming admission mechanism that prevents it from exceeding the remaining reserve.

> A historical/replay cap is not a retention policy. If authoritative state can exceed the cap, the consumer must either start from a validated snapshot/base revision or fail closed rather than silently project a prefix as current truth.

## 3. Final v1 blocker families

These are the reconciled blockers. They are intentionally grouped by failure property rather than by file so implementation does not become a collection of unrelated patches.

### B01 — multi-filesystem host resource contract and finite active transactions

**State:** `OPEN`  
**Severity:** release blocker

The host-resource contract must cover the real backing resource used by each large writer, not one generic free-space number.

Required properties:

- observe free bytes for the actual checkout/build/Docker backing volume, FCP data volume, results volume and model/provider storage where these are distinct;
- observe inode/file exhaustion where the filesystem exposes it;
- represent stale/unavailable host measurements conservatively rather than as healthy;
- define `NORMAL`, `WARNING`, `PRESSURE` and `CRITICAL` semantics shared by large writers;
- establish finite recorder ingress/transaction bounds before claiming a recorder emergency reserve;
- account for aggregate concurrent recorder work rather than reserving for one hypothetical batch while several sources commit;
- keep space for atomic replacement/checkpoint/status/WAL/journal completion; and
- apply admission/refusal to builds, model pulls, uploads, analysis materialization and other large optional writers before they cross the emergency floor.

This does not require one new always-running daemon if the existing host updater/launcher boundary can safely publish the required measurements. The design should reuse existing host authority where possible.

### B02 — finite, scalable and path-confined recorder capture

**State:** `OPEN`  
**Severity:** release blocker

Required properties:

- finite maximum HTTP response bytes for `/current`, `/probe` and `/sample`;
- a total request deadline, not only a socket inactivity timeout, so a slow trickle cannot occupy a worker indefinitely;
- finite observation/sequence-span limits independent of whether the remote Agent honours requested `count`;
- continuity validation that does not allocate an integer list proportional to an arbitrary remote sequence range;
- timestamp/date path components validated and confined under the recorder roots before any write;
- one bad/slow source cannot indefinitely delay status publication and subsequent polling of healthy sources;
- crash recovery must not recursively rescan the lifetime archive on every healthy poll; use a durable/incremental recovery frontier or equivalent bounded lookup;
- repeated identical discontinuity/source-pathology evidence must be deduplicated/rate-bounded so a bad source cannot create durable event files at poll rate; and
- pressure/critical pause must preserve the recorder's raw-first/checkpoint-last recovery semantics and must never silently delete primary evidence.

The current eight-worker concurrency is not itself a defect. It becomes safe only once individual work is finite and aggregate resource use is admitted.

### B03 — recorder publication progress and durable outbox lifecycle

**State:** `OPEN`  
**Severity:** release blocker

Required properties:

- reconciliation progress must be incremental rather than repeatedly proportional to complete recorder history;
- one malformed/missing/oversized historical item must be isolated and surfaced without permanently preventing later eligible material from progressing;
- database/storage failures in the publication loop must be caught at the required-thread boundary, surfaced as degraded health and retried/restarted without losing durable work;
- backlog catch-up must make measurable forward progress after a long outage and after restart;
- outbox terminal history needs a durable retirement/frontier/tombstone design before rows can be deleted; TTL deletion alone is not safe because current reconciliation uses prior durable rows to suppress re-enqueue; and
- session/destination/source semantics used for duplicate suppression must survive any compaction.

`compact_completed()` remains a payload compactor, not a retention bound.

### B04 — one supported update/start contract and one host-mutation serialization boundary

**State:** `OPEN`  
**Severity:** release blocker

Required properties:

- ordinary `start.cmd` and `start.sh` builds use the same host resource preflight/cache-lifecycle primitive as supported activation, or an equivalent single policy;
- failed builds cannot leave uncontrolled BuildKit growth outside the lifecycle used by successful builds;
- the documented Windows `update.cmd` must either be retired or changed to enforce the same approved repository/main, clean-checkout, exact-commit and resource requirements as the supported product update model;
- launchers and host update agents must not mutate/read-build the same checkout concurrently; use one host-mutation lock/lease covering source mutation and image build identity;
- the image label/runtime proof must identify exactly the source tree that was built; and
- `source ahead / runtime old` remains an explicitly supported resumable state. Do not add source rollback for U03.

Local administrator authority is not being restricted. This blocker is about preventing supported commands from contradicting the product's own release/update guarantees.

### B05 — failure-safe activation and optional-model isolation

**State:** `OPEN`  
**Severity:** release blocker

Required properties:

- a failed target activation must leave the previous usable core runtime available, or leave a bounded explicit degraded state with a deterministic recovery path;
- do not stop healthy Flask early merely to enter an unpreflighted normal build path;
- model installation must use the shared host-resource admission contract on update, startup, browser and provider/profile paths;
- model absence, model download failure or Ollama unavailability must not prevent unrelated workbench/Federation/recorder/control surfaces from starting; and
- no brittle guessed model-size constant is required: streaming/download admission may enforce the floor dynamically if exact size is unknown.

AI capability may be unavailable while the rest of FCP remains healthy.

### B06 — required-process and required-loop supervision

**State:** `OPEN`  
**Severity:** release blocker

Required properties:

- service-specific liveness/readiness/degraded-dependency semantics for Flask, relay and managed recorder; a Docker healthcheck may implement part of this but is not the requirement itself;
- an internal relay task failure that closes the listening server must cause the process/container to become failed/restartable, not remain PID-alive and connection-dead;
- Docker's indefinite bounded-rate restart behavior must become visible as an FCP crash-loop/resource-failure state rather than an endless opaque retry history;
- the Windows native recorder supervisor owns ordinary child restart with bounded backoff and a deterministic-crash fence; an external service manager may own the supervisor process itself but must not become a competing child owner;
- Ctrl+C/operator stop and update/trial exits must retain their existing non-restart semantics;
- recorder status/final status I/O failure must not amplify an already-failing filesystem into uncontrolled process/shutdown failure;
- recorder publication and analysis scheduler driver failure must be observable and recoverable rather than silently stranding durable work; and
- stale host-process cleanup must verify responder process identity beyond a bare PID before terminating it.

### B07 — bounded reconstructible and cumulative metadata growth

**State:** `OPEN`  
**Severity:** release blocker for unbounded FCP-owned/reconstructible growth; policy work for primary/user data

Must be bounded/retired safely:

- Docker stdout/stderr logs under an FCP-owned policy;
- POSIX update-agent `agent.log`;
- BuildKit cache on every supported build path;
- superseded unused FCP images after verified activation/start transition;
- terminal analysis job/attempt/command/audit history where semantics permit;
- artifact descriptors/grants/publication/audit metadata where semantics permit;
- recurring provider-health command/audit history;
- retained host-update result/branch histories; and
- other FCP-generated low-rate lifetime histories discovered during implementation.

Must **not** be silently auto-deleted merely to satisfy this blocker:

- primary recorder evidence;
- user uploads; or
- user/research results.

Those operator/primary paths participate in admission/pressure policy instead.

D09 is explicitly `DEFERRED`: journal-backed resumable transfer retention is not a supported installed-product path today.

### B08 — crash-correct derived and upload boundaries

**State:** `OPEN`  
**Severity:** release blocker

Required properties:

- analysis slice archive creation must be atomic or retry must fully verify/rebuild an existing deterministic target before it is registered; a hard-killed partial file cannot become the accepted content identity;
- stale per-attempt workspaces from hard kills need bounded startup/age reconciliation where they can persist;
- upload startup reconciliation must detect/clean safe orphan staging directories created before the durable batch row exists;
- publication recovery must reconcile filesystem and database state for every crash window without exposing a partial batch to analysis; and
- all cleanup must be ownership/path-confined so unrelated user data cannot be removed.

### B09 — authority history, replay ceilings, control-plane representation and time correctness

**State:** `OPEN`  
**Severity:** release blocker

Current authoritative session history is append-only while several consumers replay from revision zero with fixed page/event ceilings. At least one leadership path can finish its bounded loop and return a projection without proving it reached the authoritative current revision; human-auth event reading similarly returns what it accumulated after its page loop.

Required properties:

- no authority/security projection may silently treat a bounded prefix as current truth;
- consumers that cannot reach the current revision fail closed with an explicit bounded error until a supported snapshot/base-revision mechanism exists;
- if session-event compaction is implemented, it requires a coordinator-authenticated snapshot/base revision that every affected member/projection understands before old authoritative events are retired;
- member-replicated session history is included in the lifetime design, not only the coordinator database;
- accepted/idempotent request history may be retired only with a tombstone/hash horizon that preserves duplicate/request semantics;
- coordinator disappearance remains fail-closed for authority and is represented to the operator as control-plane unavailable/reconnecting, not new setup or invented member failure; and
- trusted v1 deployments have an explicit bounded-clock-skew/NTP prerequisite and fault coverage for lease/grant behavior, especially storage write leases evaluated on provider wall clock.

Do not introduce distributed clock consensus. Existing owner/term/fencing checks remain valuable and must be preserved.

### B10 — quiesced, capacity-safe backup/recovery and host-process identity

**State:** `OPEN`  
**Severity:** release blocker

The existing recovery *model* remains correct: same-installation recovery, replacement member and creator loss are distinct, and creator authority is not replicated. The documented backup procedure itself is not yet a safe implemented closeout.

Required properties:

- destination is explicitly outside the checkout/source state and has sufficient independent capacity before production writers are stopped;
- backup cannot fill the same production filesystem it is protecting;
- all relevant writers are fenced for the selected topology, including host update mutation and any native recorder/responder that can touch included state;
- concurrent update/activation is refused during the backup window;
- any helper image/tool needed after quiescing is pre-existing or the procedure uses tools that require no post-stop network/download;
- failure does not automatically restart into a newly exhausted host without an explicit safe check;
- whole SQLite/WAL state is copied coherently and every applicable restored database is integrity/quick-checked;
- restore is isolated with the original logical instance offline and does not require model download for core recovery; and
- same-identity Windows recovery retains the existing DPAPI boundary.

The separate stale responder PID finding is implemented under B06 but must also be included in reboot/recovery testing.

### B11 — corrected exact-candidate physical campaign

**State:** `OPEN`  
**Severity:** final release blocker

The exact final candidate must pass the corrected campaign in section 5 after B01-B10 are implemented and automated gates are green.

## 4. Findings closed, narrowed or deferred by review

| Finding | Reconciled state | Decision |
| --- | --- | --- |
| H04 general CPU/RAM scheduler | `DEFERRED` | Keep finite recorder memory/response bounds and OOM isolation; no general resource scheduler without evidence. |
| D09 resumable-transfer journal | `DEFERRED` | No supported installed-product construction found. |
| S01 “add Docker healthchecks” | `NARROWED` | Require semantic liveness/readiness/degradation; implementation mechanism is open. |
| S02 restart spin | `NARROWED` | Docker has bounded-rate restart backoff; problem is indefinite retry, cumulative work/logs and absent FCP-visible crash-loop state. |
| U03 source-before-preflight | `PROTECTED` | Source-ahead/runtime-old is deliberately represented and retryable. No rollback work. Fetch-resource admission remains B01/B04. |
| C03 replicated creator/coordinator | `ACCEPTED-V1-BOUNDARY` | Preserve current creator-loss boundary; no Raft/quorum work. |
| automatic recorder retention | `DEFERRED` | Pause/admission before full disk; do not delete primary evidence silently. |
| automatic user upload/result deletion | `DEFERRED` | Operator-owned data; admission/pressure instead. |
| automatic model eviction | `DEFERRED` | Refuse/pause download under pressure. |
| hot/live coordinator backup | `DEFERRED` | Correct quiesced backup is sufficient for v1. |

## 5. Corrected physical robustness campaign

The original P01-P12 names are retained for traceability, but their scope is corrected here.

### P01 — repeated Federation update growth and failure cleanup

On Beast and one POSIX host:

- measure host free bytes, relevant filesystem/inode state, checkout/data/results volumes, Docker root/VHDX, BuildKit cache, images, container logs and model volumes separately;
- run unchanged and distinct-commit cases where meaningful;
- perform at least three real supported activations;
- include one failed/interrupted build and prove cache/resource cleanup or safe pressure behavior afterward;
- prove exact running commit and retained FCP state after each activation; and
- require no return to the historical multi-gigabyte-per-activation growth slope.

### P02 — independent backing-resource exhaustion

Independently pressure the resources that can differ physically:

- checkout/build/Docker host volume;
- data volume;
- results volume;
- model/provider volume; and
- inode/file capacity where applicable.

Require explicit resource refusal/degradation while the old healthy core remains usable where the operation is optional.

### P03 — all supported start/update entry points and concurrency

Exercise:

- `start.cmd`;
- `start.sh`;
- `start-tailscale.cmd`;
- the final disposition of `update.cmd`;
- concurrent launcher attempts; and
- launcher versus pending/active host update.

Prove exact source/image/runtime identity, mutation serialization, bounded build state and model/network failure isolation.

### P04 — recorder finite transaction and disk pressure

Only after finite ingress bounds exist:

- run up to eight simultaneous sources;
- exercise maximum accepted responses/observations/sequence spans;
- pressure byte and inode/file capacity;
- prove aggregate admission leaves room for raw/manifest/observation/JSONL/checkpoint/status/journal completion;
- enter WARNING/PRESSURE/CRITICAL states;
- pause safely without deleting primary evidence; and
- restore capacity and prove sequence/checkpoint continuity without `--fresh`.

### P05 — service/failure injection

Include at minimum:

- Flask crash;
- relay process crash;
- relay stale-sweep database failure;
- managed recorder crash;
- native recorder child crash;
- publication database failure;
- analysis scheduler database failure;
- poison recorder archive;
- slow-trickle MTConnect response;
- oversized MTConnect response/observation set;
- huge sequence discontinuity;
- malicious/malformed timestamp path component;
- repeated discontinuity event storm;
- Ollama/model absence; and
- stale responder PID reuse.

Require bounded logs, visible health and no unrelated authority/data loss.

### P06 — native recorder supervision contract

Prove:

- one unexpected child crash restarts with backoff;
- repeated deterministic crashes reach the declared crash-loop fence;
- operator Ctrl+C does not restart;
- approved update/trial replacement semantics remain unchanged;
- checkpoint continuity survives restart; and
- supervisor crash/reboot behavior is tested only to the extent an external startup/service-manager contract is actually declared.

### P07 — long Federation outage with aged corpus

Before disconnecting Federation, seed a non-trivial historical corpus/outbox rather than testing only a fresh recorder.

During at least one hour of control-plane outage:

- local capture/checkpoints continue;
- backlog remains durable;
- source polling and publication reconciliation latency remain bounded; and
- no required worker silently dies.

After reconnect:

- catch-up shows continuous forward progress;
- a poison item cannot block later eligible work;
- duplicate suppression remains correct; and
- restart during backlog does not lose progress.

### P08 — logical storage exhaustion under host pressure

Drive a real storage authority to its allocation/floor while also checking the stricter host-pressure contract. Existing committed reads and control surfaces must remain available; partial commits are forbidden; recovery after restored capacity requires no repair.

### P09 — durable-write crash windows, control-plane loss and clock skew

Inject process/power interruption around:

- raw temp/final publication;
- raw manifest;
- observation archive;
- compatibility JSONL;
- checkpoint/status replacement;
- outbox transaction;
- analysis slice archive;
- upload staging/database/publication transitions; and
- coordinator/relay SQLite/WAL activity.

Also test coordinator disappearance and bounded positive/negative clock offsets around storage lease expiry. Require no fabricated continuity, no self-promotion and no silent partial authority projection.

### P10 — model installation through every product path

With required models absent and with constrained host/model storage, exercise:

- normal startup;
- Federation update;
- browser-triggered installation; and
- provider/profile installation paths.

Model failure must not remove workbench/Federation/recorder/control availability. No model pull may cross the emergency host floor.

### P11 — exact-candidate backup/restore

- preflight an external backup destination;
- pre-stage any helper required after quiescing;
- fence Compose plus relevant host writers;
- refuse concurrent update;
- exercise a failed copy safely;
- integrity-check all applicable restored SQLite databases;
- restore in isolation;
- verify device/Federation/auth/recorder continuity as applicable; and
- prove core recovery does not require model download.

### P12 — aged-history 24-hour soak plus accelerated ceiling tests

The 24-hour soak begins with meaningful history rather than an empty installation. Sample:

- free bytes and inode/file counts per backing filesystem;
- Docker VHDX/root, cache, images and logs;
- recorder corpus roots and per-poll recovery time;
- publication reconciliation duration/progress;
- outbox rows by session/destination/source and pending count;
- session-event/member revision counts;
- job/attempt/command/grant/artifact/provider-health history counts and DB sizes;
- update-result history;
- orphan staging/workspaces;
- process/restart/required-thread health;
- publication/analysis backlog; and
- CPU/RAM observations sufficient to identify a concrete leak or OOM isolation defect.

Use accelerated automated/physical tests to cross the known authority/history page ceilings; a fresh 24-hour run alone will not reach them.

## 6. Implementation order

Implementation may begin only from this reconciled blocker set, one named delivery per branch/PR.

Recommended order:

1. **Immediate correctness/confinement defects** — recorder path confinement, analysis atomic slice publication, upload orphan recovery, stale responder PID identity, relay running-but-dead failure propagation.
2. **Recorder finite ingress and scaling** — response/deadline/observation/sequence bounds, bounded continuity validation, incremental recovery lookup, slow-source isolation and event-storm control.
3. **Host resource envelope** — per-backing-filesystem observation, aggregate durable-write admission, pressure states, shared build/model/upload/analysis admission.
4. **Supported start/update contract** — normal launcher cache/preflight, `update.cmd` disposition, shared host-mutation lock and build/source identity proof.
5. **Activation/model isolation** — preserve core UI/control under model failure and make activation failure-safe across support services.
6. **Publication and required-loop supervision** — incremental/poison-safe publication, SQLite recovery/health, native child restart/backoff, analysis driver health and crash-loop visibility.
7. **Cumulative metadata/reconstructible lifecycle** — logs, images, caches, outbox frontier, job/artifact/provider/update histories.
8. **Authority/replay/time correctness** — fail-closed replay ceilings or authenticated snapshot base, accepted-request tombstones, control-plane UI state and bounded-skew contract.
9. **Backup/recovery procedure** — capacity-safe fully quiesced backup and exact-candidate restore rehearsal support.
10. **Physical campaign P01-P12** — only after automated gates are green on one exact candidate.

Do not combine these into one broad robustness PR. Each delivery must preserve the existing authority and recovery invariants and must state which blocker/evidence state it advances.

## 7. Exit criteria

Federation v1 robustness is not closed until:

- B01-B10 are either `PHYSICAL-PROVEN` where physical evidence is required or explicitly accepted as a safe v1 boundary;
- all critical/high crash-correctness and path-confinement defects have automated regression tests;
- no supported update/start/model path can bypass the shared resource and mutation contract;
- recorder capture has finite ingress/transaction bounds and remains performant with aged data;
- publication continues around poison/history and required worker failure is visible/recoverable;
- no authority projection silently truncates authoritative history;
- backup/restore is capacity-safe and actually quiesced for the tested topology;
- the corrected P01-P12 campaign passes on one exact commit; and
- CF7 acceptance flags are changed only by their separate evidence-backed review.

## 8. Explicit non-goals retained

Do not introduce for this gate unless a new concrete blocker proves they are necessary:

- Raft, Byzantine consensus or replicated creator authority;
- automatic deletion of primary recorder evidence;
- automatic deletion of user uploads/results;
- Kubernetes-style orchestration;
- a general distributed CPU/RAM resource scheduler;
- automatic model eviction;
- hot/live coordinator backup; or
- OSL/SysML implementation work.
