# Current task handoff

Status: **current repository handoff**

Reviewed: **2026-09-01 Europe/Oslo**

## Repository state

- Repository: the current FCP source repository.
- Default branch: `main`
- Always resolve the current `main` head directly before starting work.
- Published Federation v1 release tag: not created.
- Capability-first Federation implementation: merged baseline.
- Role-first installed-product runtime retirement (CF8): merged.
- Verified manual Federation-wide software updates: merged.
- Standalone recorder Federation bootstrap/publication and Federation-wide recorder control: merged.
- Disk-allocation/update-cache hardening from PR #325: merged; physical disk-exhaustion closeout remains open until the real-host robustness campaign passes.
- Independent adversarial robustness review: reconciled; implementation must follow the reconciled blocker set.
- Complete physical CF7 acceptance: not accepted.
- Complete Federation v1 end-to-end acceptance: false.
- OSL integration: separate planning track; production implementation status is governed by the OSL track documents.

Historical phase notes, branch handoffs, old commit hashes, and pre-CF8 sequencing do not override this current handoff. Acceptance flags are different: only the named acceptance source and a separate evidence-backed review may change them.

## Track A: capability-first Federation

Durable product/authority plan:

- [Capability-first Federation plan](federation/active/capability_first_federation_plan.md)

Current operational documentation:

- [Federation operations](../federation_operations.md)
- [Standalone recorder](../standalone_recorder.md)
- [Current architecture](../architecture.md)

Detailed runtime-update design:

- [Manual Federation-wide FCP updates](federation/active/manual_updates.md)

Active v1 robustness closeout:

- [Reconciled robustness review](v1_robustness_reconciliation.md) — **authoritative implementation input**;
- [Federation v1 robustness gate](v1_robustness_gate.md) — initial systematic gate/evidence vocabulary;
- [Disk accounting audit](disk_accounting_audit.md) — disk-specific forensic input.

The independent review has been reconciled. New robustness implementation must start from `v1_robustness_reconciliation.md`, which overrides classifications in the initial gate where they differ. The documents do not change Federation authority or acceptance flags by themselves; their corrected physical campaign supplies additional evidence alongside the existing CF7 contract.

Acceptance workspace:

- [Federation acceptance documentation](federation/acceptance/)

Machine-readable acceptance truth:

```text
catalog/federation/tests/cf7_acceptance/scenarios.json
```

### Current merged Federation/product baseline

The installed product now includes:

- stable persistent device identity;
- authenticated Federation discovery, verified join, signed pairing, reconnect, revocation, and local creation;
- the required first-run path `Identity -> Federation -> Inspect -> finish setup`;
- bounded device inspection and optional benchmark evidence;
- independent contribution recommendation/intent/enable/disable/suspend/reconcile behavior;
- capability-first runtime/configuration authority with the former role-first product runtime retired;
- public-safe Federation overview/detail surfaces plus explicit reviewed mutation surfaces;
- coordinator-owned **Check for updates** and **Update all devices** with exact-commit host validation and running-runtime proof;
- Windows/POSIX host-owned update agents and conservative Windows legacy migration bootstrap;
- a supervised native standalone recorder that participates in the same **Update all devices** rollout over its existing Federation connection;
- a headless MTConnect recorder that can join a Federation using the normal `FCP1-...` pairing flow;
- recorder-local startup network discovery with first-configuration auto-selection;
- local-first checkpoint-gated recorder publication through Federation logical-storage authority;
- `/federation/recorders` control from any trusted Federation device for bounded recorder-local scans and add/remove source selection;
- bounded logical-storage allocation/free-space floors and update-path Docker build-cache/preflight protection from PR #325;
- storage, transport, AI/provider, compute/job/artifact, recovery, fencing, and authority boundaries;
- permanent Ubuntu and Windows component/product/release gates.

The pairing-code UX currently issues signed one-use codes valid for up to 10 minutes and permits a fresh code to be generated when another join attempt is required.

A successful software activation remains internally `runtime_verified`; the UI presents that terminal success as **Updated**.

### Standalone recorder update coverage

A native standalone recorder started through its supervisor (`start-tailscale-recorder.cmd`, which runs `scripts/windows/fcp_recorder_supervisor.ps1`) now participates in **Check for updates -> Update all devices** like any other Federation member. The host update agent runs inside the recorder process and starts only when the supervisor has set `FCP_RECORDER_SUPERVISOR_SESSION`; the supervisor performs only the two steps that cannot happen inside the recorder, namely the fast-forward after the process exits and the single relaunch. Success still requires a different process ID and a different process-instance nonce under the same supervisor, running the exact target commit, with a fresh heartbeat and connected Federation membership. See [Standalone recorder](../standalone_recorder.md).

Two limits remain, and neither may be softened in documentation:

- A recorder launched directly with `python start_recorder.py` has no supervisor session, so it starts no host update agent. It is still a headless Federation node and still receives the update event, but its bounded handoff is answered by nothing and the coordinator records `host_update_agent_unavailable` for that device. It is reported as an error, never as a silent success, and never as an updated device.
- The supervisor exists for Windows only. There is no POSIX native-recorder supervisor, so a native recorder on Linux or macOS is in the unsupervised case above.

A recorder whose current checkout predates this capability needs one manual fast-forward and one supervised start before it can be updated by the Federation flow, for the same reason normal FCP devices do.

### Federation work still open

Robustness implementation is proceeding as isolated delivery PRs. The merged
tailnet responder process-identity and relay required-loop deliveries close two
distinct B06 properties with automated evidence: stale process records cannot
authorize termination of a reused unrelated process, and a fatal relay
stale-heartbeat sweep now wakes both supported relay owners and produces a
nonzero process exit so the existing Compose restart policy can act. Together
these are **B06 2/8 properties automated-proven; B06 remains open**.

The recorder status-I/O boundary delivery adds a third. `publish_status` writes
the recorder heartbeat on every cycle and once more inside `run`'s shutdown
block, and an `OSError` from a full, read-only or otherwise failing host
filesystem used to escape the run loop, raise again during shutdown, replace the
real stop reason and skip stop-target cleanup -- while the same failure inside
capture was already contained by the per-source boundary. The supervised native
recorder made the consequence concrete: its supervisor reads an operator stop
from a graceful zero exit, so a refused heartbeat write during Ctrl+C turned the
operator's own stop into a nonzero exit and restarted capture behind them. The
write is now contained, announced when the condition appears or changes rather
than once per cycle, and carried into the next heartbeat that reaches disk
through an additive `status_publication_error` field. Nothing is fabricated: a
refused write leaves the published file byte-identical and stale, which is
exactly what the host updater's freshness check must read as not proven healthy.
Automated evidence covers the graceful operator-stop exit, continued capture and
raw archival during the failure, the unchanged stale file, reporting after
recovery, and the bounded announcement. That brings B06 to **3/8 properties
automated-proven; B06 remains open**. No physical evidence or acceptance state
changed.

Writing the coverage the publication/scheduler driver bullet was waiting on
found two more defects on the publication half. `run_forever`'s retry family
omitted `sqlite3.Error` even though the outbox behind it is SQLite, so a locked
or unreadable store escaped the loop and the supervisor rebuilt the whole worker
instead -- discarding that loop's own failure count and poll interval, so a
store unreadable for hours read as a first retry. And both supervisor recovery
paths waited a fixed second with no count and no ceiling, so an unreachable
Federation or an unopenable store meant reloading the authorized context and
reconstructing an authenticated storage client once per second, indefinitely.
A durable-store failure is now an ordinary cycle failure, counted and paced by
the loop that owns it, and the supervisor counts its own restarts into the
snapshot and waits on a bounded ladder capped at 60 seconds that only a cycle
which actually published can clear. With both halves evidenced, that bullet is
automated-proven and **B06 is 4/8 properties automated-proven; B06 remains
open**. The same audit found the creator's logical-storage authority supervisor
carrying the second of those defects -- a snapshot with no restart count and a
fixed five-second rebuild with no ceiling -- and it now counts and backs off the
same way, cleared only by an announcement. That is the same discipline applied
to a third required driver rather than a new property, so the count is unchanged.

The standalone recorder's four required loops -- Federation update, host update
agent, update activation and recorder control -- were the same shape again and
the worst placed for it. Each retried correctly and then discarded the failure,
so a loop that had failed on every pass since startup looked exactly like a loop
with nothing to do, on a device whose only operator surface is its heartbeat: a
`/federation/recorders` scan or source change silently never applied, and a
device silently absent from an **Update all devices** rollout. Each now keeps a
bounded consecutive-failure record with a named error code, announced when the
condition appears or changes rather than once per poll, and the launcher
publishes those records into the heartbeat under an additive, count-bounded
`workers` key through the same read-only provider seam Federation status uses.
No loop's lifecycle changed. This is added observability for the same bullet,
not a further property, so the count is still 4/8.

No physical evidence or acceptance state changed.

Archive reconciliation and delivery now cover all six B03 software properties.
The item-level archive faults -- missing, unreadable, malformed, empty,
sequence-discontinuous, receipt-less or overlarge observation evidence -- fence
only their source and remain visible in a bounded quarantine summary; other
sources continue publishing, primary evidence is never deleted, and a repaired
item can publish on a later pass. The native and Flask publication loops catch
database/storage failures at their required-thread boundaries, expose bounded
failure evidence and retry from durable state.

The remaining restart amplification was in the delivery side: the queue decoded
the entire pending outbox before applying its delivery limit, and the restart
gate did the same just to answer whether work existed. `SQLiteOutbox` now has a
bounded `pending_for_delivery()` window and a one-row `has_pending()` existence
probe. The window includes each ordered dataset's oldest row before filling the
configured limit, so one offline dataset cannot hide a healthy one; deferred
heads remain visible to preserve per-dataset fencing; no backlog snapshot is
carried across restart. Real SQLite consequence tests prove the window is
bounded, fair and monotonically drains a durable backlog across a queue restart.

The outbox also retains deterministically undeliverable rows as durable
`retired` tombstones rather than deleting or retrying them forever. Session,
destination, schema, idempotency, content and dataset-ordering identity survive
receipt compaction; repeated archive reconciliation cannot resurrect a retired
batch, and changed content still fails closed. The retirement/compaction
consequence suites cover restart, migration, repair, crash windows, primary
evidence preservation and degraded-health persistence. This brings B03 to
**6/6 software properties automated-proven; B03 remains open** only for the
exact-candidate physical P05/P07/P09/P12 evidence. No physical evidence or
acceptance state changed.

The recorder path-confinement, finite-transaction, incremental recovery-frontier,
healthy-source progress-isolation, and durable event-storm deliveries together
advance **B02 8/9 properties automated-proven; B02 remains open**.
Timestamp-derived storage days are validated/confined before writes. `/current`,
`/probe`, and `/sample` have finite decoded response-byte ceilings and a finite
total request deadline; accepted XML is structurally budgeted before retained-tree
parsing; parsed sample batches have hard observation and sequence-span ceilings;
and continuity validation is bounded by accepted observation count rather than
materializing an arbitrary remote integer range. Detailed observation NDJSON and
wide compatibility JSONL are streamed through byte-bounded atomic publications,
and carried compatibility checkpoint state has a finite serialized ceiling, so
bounded ingress cannot amplify into an effectively quadratic in-memory/disk
batch. Ordinary crash recovery publishes one atomic per-source/per-Agent-instance
frontier before raw publication and consults that fixed path instead of
recursively rescanning lifetime history on every healthy poll. A pre-frontier
archive is scanned once and migrated to a clear frontier; explicit state-loss
rebuilds and offline canonical projection retain their deliberate historical
scans. Source scheduling keeps at most one capture transaction in flight per
source and harvests only completed work, so a bounded slow/bad source no longer
forms a global barrier in front of recorder heartbeat publication or subsequent
polls of healthy sources. Shutdown still drains already-scheduled bounded source
transactions without cancelling raw-first/checkpoint-last work. Recorder
pathology/discontinuity events are now coalesced into atomic hourly summaries per
source/event type: repeated identical evidence and rapidly changing source payload
values cannot create durable event files at poll rate, while first/latest payload
samples and occurrence/change counters remain bounded operational evidence. The
summary writer uses `fcp.mtconnect.recorder_event.v2`; historical v1 event files
remain untouched and have no authoritative reader.

The host-storage-refusal delivery closes the last B02 software property, taking
**B02 to 9/9 software properties automated-proven; B02 remains open** for
exact-candidate physical evidence only. Admission reserves against an estimate,
so a concurrent writer or an underestimate could still leave the host with no
room when the admitted write ran; the filesystem then refused it with `ENOSPC`,
which carried no pause signal and reached the remote-source error boundary. A
healthy Agent was recorded as the failing party and backed off toward
`BACKOFF_MAX`, while the local condition that actually stopped capture was never
published. An observable out-of-room refusal inside an admitted transaction now
reaches the existing pause path with a fresh measurement of the resource that
refused. The boundary stays narrow: only `ENOSPC`/`EDQUOT`, and only for writes
the recorder's own reservation admitted -- a `save_state` outside one, or the
unreserved legacy migration clear, keeps its ordinary `OSError`. Coverage tracks
that reservation rather than a list of writers: recovery publishes the
compatibility view directly, the composed runtime store writes its
publication-discovery record after the wrapped observation writer returns, and
the recovery frontier's pending and clear markers each end the transaction while
unwinding, so all of those are reclassified at the write itself. A resource that
cannot be measured after refusing still pauses, but reports
`measurement_unavailable` and no capacity rather than inventing one. Raw evidence
is retained and no durable checkpoint advances past it on any refusal path, and
nothing is deleted to make room. P04/P05/P09 physical evidence remains open; B01
still owns aggregate host-resource admission across concurrent writers. No
physical acceptance state changed.

The analysis-slice, upload crash-correctness, and analysis-workspace
reconciliation deliveries together provide **B08 5/5 properties
automated-proven**. Deterministic analysis slice archives are atomically
published or fully verified/rebuilt before reuse. Upload staging has a durable
ownership record plus exact legacy-layout orphan recovery; publication startup
reconciliation hides non-ready batches behind an exact `.fcp-importing`
ownership marker and repairs supported database/filesystem crash windows; upload
cleanup validates confined batch/file paths, regular non-reparse types, expected
sizes and content digests before unlinking and preserves ambiguous or unrelated
content. Federated analysis workspaces now receive an ownership marker before
large materialization; provider startup performs age-bounded, scan-bounded
reconciliation of stale owned or exact legacy attempt layouts, never follows
symlinks/reparse points, and preserves ambiguous content. Same-attempt retry
reset is also ownership- and path-confined rather than blindly deleting the
expected pathname. B08's automated implementation properties are complete;
P05/P09 exact-host hard-kill/power-loss evidence remains open and no physical
acceptance state changed.

Authoritative-replay completeness is shared by one primitive that folds a
caller's own bounded pages and returns only once the coordinator's reported
current revision has been reached; every other exit raises
`authoritative-replay-incomplete`. Leadership and human-auth were wired onto it
first. Shared knowledge is now wired onto it too, because its prefix behaviour
was worse than under-reporting: a read that stopped before a document's delete
event re-published that withdrawn document into the append-only authoritative
log for every member. It now degrades to the local cache and changes nothing
shared, reported at warning level rather than as an ordinary unreachable relay.
The Federation authority projection adapter is wired onto it too: its bounded
loop measured progress by page length rather than by revision, so an empty page
or a non-contiguous page was folded into a `current` overview that presented a
revoked device as a current member and a demoted node as leader. The remaining
paged consumers -- the capability-request, update, software-version and
recorder-control report aggregators -- still return what they accumulated at
their ceilings, so **B09 stays at 1/7 properties automated-proven and remains
open**. The two member authority surfaces -- user administration and password
change -- now report an unresolvable authority as their existing bounded `503`
rather than letting the refusal escape a `before_request` hook as a broken
device; the explicit control-plane unavailable/reconnecting operator surface is
still not built. No page ceiling was widened and no physical evidence or
acceptance state changed.

The container-supervision delivery closes the last four B06 software
properties at once, because they were one missing primitive rather than four.
A crash-looping FCP service was invisible to FCP itself: Compose restarts it,
the service comes back, and every health probe reports the fresh process as
healthy, so a service failing every thirty seconds and one running for a week
read identically. The obvious source -- the Docker API -- is not available to
the product and must not be made available: `docker.sock` is deliberately not
mounted in `docker-compose.yml`, and mounting it would hand the web application
root-equivalent control of the host to gain a status field. Each supervised
service therefore journals its own incarnations instead. A start records
whether the previous incarnation ended intentionally, a bounded window of
recent incarnations is retained, and a trailing run of unclean starts inside a
time window is a crash loop that the existing core-service health snapshot reports
as `not_ready`/`degraded` with a `<service>-crash-loop` code, carrying the
prior probe's own code so the underlying fault is not hidden by the loop
verdict. Exception/nonzero failure exits remain unclean evidence even when
their own error path records a stop; only normal, operator, update, and trial
stops are clean. Writing it exposed a real supervision defect: the Flask service
installed no `SIGTERM` handler, so an ordinary `docker compose stop` exited
through the default disposition and every operator stop would have been
journaled -- and read -- as a crash. Journaling is disabled under `debug`,
where the reloader's own process churn is not a fault. Every write is
best-effort and every read total, so a full or read-only host degrades
supervision visibility and never the service. With this, **B06 is 8/8 software
properties automated-proven; B06 remains open** for the exact-candidate
physical campaign only.

The bounded-growth delivery closes the B07 items whose retirement frontier the
existing contracts already imply, and names the invariant blocking each one it
does not. The coordinator audit ring had a row bound but no work bound, so on a
coordinator whose history predates the ring the first write after upgrade
retires the entire backlog inside the same transaction as an ordinary audited
action; `provider_health` and `provider_enrollment` mirror the same row bound
and both already carried the per-pass batch bound that `audit_log` lacked.
Superseded contribution intent revisions accumulate on every
enable/disable/suspend/reconcile and no query in the product reads them, so a
bound there is semantics-preserving rather than a retention choice; retirement
is by the candidate's own monotonic revision and confined to the candidate just
written. Artifact grant expiry selected every due grant in one unbounded pass
that also appends an audit row per grant -- latent today because the entry point
has no production caller, and bounded now rather than when one is added. The
analysis job and artifact metadata tables are deliberately untouched: their
frontier is `UNIQUE(session_id, idempotency_key)`, command replay suppression
and per-job audit reads, which are authority/history semantics owned by the B09
lane, not a bound to be invented here. Recorder and upload evidence is user
primary data and was not touched at all. No physical evidence or acceptance
state changed.

### Reconciled robustness branches

Two branches were preserved for follow-up after the cleanup sequence and are now
reconciled onto `main`. Neither was merged; both were re-derived, because both
predated `main` substantially.

- `claude/federation-v1-b06-recorder-status-io-boundary` — the recorder
  status-I/O defect was still live on `main`, so the containment and its
  consequence tests were rebuilt against the current runtime and are now merged
  into this branch's B06 delivery. Nothing from that branch remains unported.
- `claude/federation-v1-hardening-qfqvaf` — its `catalog/federation/event_replay.py`
  reader and that reader's tests are **obsolete**, superseded by the merged
  `catalog/federation/authoritative_replay.py`, which proves every shape that
  one did and additionally refuses an event beyond the reported current revision,
  refuses a non-advancing revision separately from a non-contiguous one, requires
  the applied revision to *equal* rather than merely reach the reported head, and
  validates event revision types. Its human-auth wiring is likewise superseded by
  the merged delivery. Porting either would have created a second competing
  primitive. What was still needed and is now delivered here: the shared-knowledge
  consumer wired onto the merged reader, and `resolved_authority` for the two
  member authority surfaces.

1. Continue the reconciled robustness blockers B01-B10 from [the authoritative reconciliation](v1_robustness_reconciliation.md), one named delivery/PR at a time and in the recommended dependency order.
2. Keep the documented non-goals and accepted boundaries out of the v1 implementation unless new concrete evidence invalidates them.
3. Execute the corrected P01-P12 physical fault/growth/restore campaign on one exact candidate after the blocker implementations and automated gates are green; green CI alone is insufficient.
4. Reconcile physical-acceptance instructions with the current post-CF8, update-capable, recorder-capable product baseline.
5. Resolve any verified runtime-parity, native-host, privacy, browser, restart, multi-host, recorder-control, update-rollout, resource-exhaustion or recovery defects found on the exact candidate.
6. Freeze one exact candidate only after known blockers are closed.
7. Execute the complete physical CF7 campaign on that same commit.
8. Update acceptance flags only through a separate evidence-backed review.
9. Create a Federation v1 release tag only after the release acceptance contract is satisfied.
10. Decide whether a POSIX native-recorder supervisor is in scope; until one exists, native recorders on Linux and macOS stay outside **Update all devices** by construction rather than by policy.

Do **not** restart CF1-CF6 implementation waves and do **not** reintroduce role-first runtime authority to solve migration or startup defects.

## Track B: OSL integration

Plan index:

- [OSL integration index](osl_integration/)

Authoritative execution plan:

- [OSL implementation roadmap](osl_integration/10_phased_implementation_roadmap.md)

W3 end-to-end acceptance scenario:

- [Notebook-to-OSL method alignment](../planned-work/method-osl-fcp-alignment.md)

Federation work and OSL work remain separate review boundaries. Before beginning any OSL implementation delivery, re-read the current OSL index/roadmap rather than relying on older Federation handoff text.

## Cross-track boundaries

- Do not combine Federation physical acceptance/runtime fixes with OSL production implementation.
- OSL review, approval, or publication grants no Federation, provider, compute, storage, job, artifact, lease, fencing, update, recorder-control, or machine authority.
- Federation device identity is not a human OSL reviewer, approver, or publisher identity.
- AI cannot sign, approve, publish, or create canonical human authority merely because it is available as an FCP capability.
- Existing operator records and legacy SysML exports are compatibility inputs, not proof of OSL conformance.

## Agent operating discipline

1. Start from updated `main`.
2. Scope each branch and PR to one named delivery, defect, acceptance unit, or documentation unit.
3. Declare owned paths before editing shared Flask, setup, navigation, security, persistence, update, recorder-control, or workflow files.
4. Commit after coherent boundaries so partial work remains recoverable.
5. Open a draft PR unless the repository owner explicitly requests another state.
6. Distinguish automated, simulated, browser, physical, multi-host, service, resource-pressure and human evidence.
7. Preserve authority, privacy, migration, restart, recovery and cross-platform gates.
8. Stop when a missing decision would require scope expansion or a permissive assumption.

## Resume safety

- Safe to continue Federation robustness implementation: **yes, only from the reconciled blocker set and one named delivery at a time**.
- Safe to treat the initial robustness gate as authoritative where the reconciliation disagrees: **no**.
- Safe to mark physical CF7 accepted from merged code/green CI alone: **no**.
- Safe to treat CF8 as future/blocking work: **no; CF8 is already merged for the installed product**.
- Safe to reintroduce role-first authority for convenience: **no**.
- Safe to document Federation-wide updates as automatic/background updates: **no; activation remains explicit/manual**.
- Safe to claim *supervised* native standalone recorders are updated by **Update all devices**: **yes**.
- Safe to claim an unsupervised `python start_recorder.py` process is updated by **Update all devices**: **no; it reports `host_update_agent_unavailable`**.
- Safe to begin an OSL delivery: only after checking the current OSL track documents and respecting its named prerequisite/review boundaries.
