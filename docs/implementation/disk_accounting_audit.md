# FCP disk accounting audit

| Metadata | Value |
| --- | --- |
| Status | Active |
| Audience | Maintainers, release reviewers, physical test operators |
| Scope | Every FCP-owned path that consumes host disk, and whether it is bounded |
| Reviewed | 2026-08-24 Europe/Oslo |

## Why this exists

A physical host filled its drive while serving a Federation and doing its own
work. The first fix bounded Federation logical storage. That was a real gap, but
it was chosen because it was the plausible producer, not because it was the
measured one. This audit exists so the protection matches the failure.

**Federation logical storage did not cause the incident.** That is measured, not
inferred. See the physical evidence below.

The consumer-side telemetry mirror -- the path that receives another device's
recorder data and materializes it for the local product -- was already capped at
2 GiB of batches plus 512 MiB of materializations before any of this work. A path
with a 2.5 GiB ceiling does not fill a drive. The authority-side ingest path
genuinely was unbounded and is now bounded, and that remains worth having, but it
is not what filled the host.

## Physical evidence

Measured on the affected Windows host:

| Location | Size |
| --- | --- |
| `C:\wsl\msh\data` | ~0.01 GB |
| `results` | effectively empty |
| `fcp_relay_state` volume | ~47 MB |
| Ollama model volumes | ~2 GB |
| **`docker_data.vhdx`** | **40.5 GB** |
| **BuildKit cache (`docker system df`)** | **21.08 GB, 18.93 GB reclaimable** |

The cache held many entries of roughly 1.01 GB each, created three days apart in
a cluster. That figure is not a coincidence: the Python dependency tree this
project installs measures about 726 MiB, and with the base image and application
source on top, one FCP image layer set comes to roughly 1 GB.

Every FCP data path on the host together accounts for well under 1% of the drive.
The Docker build cache accounts for half of it.

### Why each build writes a fresh gigabyte

`Dockerfile:3-9` and `Dockerfile.cli:3-8` write `FCP_BUILD_COMMIT` into `ENV` at
line 8, *above* the dependency install at lines 15-16. Changing a build argument
invalidates the layer that consumes it and every layer after it, so a new commit
means the entire dependency install is re-run and re-cached. Nothing is shared
with the previous build but the base image.

The Windows update agent runs `docker compose build relay flask recorder` on
every activation. Three images, roughly a gigabyte of fresh cache each, on every
update, with no cleanup and no bound anywhere in the update path.

## Bounded

| Path | Bound | Evidence |
| --- | --- | --- |
| Storage authority committed batches | Preallocated budget plus a derived free-space floor | `catalog/federation/storage_allocation.py`, enforced in `local_storage.py` `ingest` |
| Federated telemetry mirror | 2 GiB of batches, 512 MiB of materializations, both totals not per-item | `telemetry_mirror.py:40-42`, enforced at `:654` and `:919` |
| Federation audit log | Ring buffer; the oldest rows are deleted on write | `persistence.py:507` |
| Efficiency observation store | Explicit retention bounds, pruned on write | `catalog/capabilities/efficiency/store.py:170,293` |
| Object transfer staging | `MAX_TRANSFER_OBJECT_BYTES` per staging root | `object_transfer_staging.py:68` |
| Branch trial worktrees | `MAX_RETAINED_TRIALS = 3`, older ones pruned | `catalog/mtconnect_recorder/native_trial.py:75` |
| SQLite write-ahead logs | Default autocheckpoint, roughly 4 MB per database | `journal_mode=WAL` set with no `wal_autocheckpoint` override |

## Not bounded

Every entry here can grow until the volume is full. None of them is affected by
the storage allocation.

| Path | State | Evidence |
| --- | --- | --- |
| Recorder raw capture (`raw/**.xml.gz` and manifests) | No retention of any kind. Grows for as long as the recorder records | `catalog/mtconnect_recorder/storage.py:42,98` |
| Recorder normalized JSONL (`jsonl/**`) | No retention | `catalog/mtconnect_recorder/storage.py:45,231` |
| Recorder outbox completed rows | A bound exists and is never applied: `compact_completed()` has no caller outside tests | `catalog/federation/outbox.py:462` |
| Federation session event log | Append-only with "no compaction, snapshotting, or retention". Lives in the retained `relay_state` volume | `persistence.py:422`; `docs/implementation/federation_sharing_evaluation.md` |
| Docker images produced by updates | Each update rebuilds three images that share no expensive layer, and nothing prunes the previous set | `Dockerfile:3-9` writes the build commit into `ENV` above the dependency install at `:15-16`; no `prune` or `rmi` in either host update agent, `start.cmd`, or `start.sh` |
| Docker build cache | Never pruned | As above |
| Docker container logs | No `logging:` limits are configured, so the default json-file driver grows without bound | `docker-compose.yml` |
| Ollama model volumes | Two separate volumes, each holding full models | `docker-compose.yml:198-201` |

Recorder retention is a deliberate absence rather than an oversight.
`docs/implementation/federation/reference/recorder_federation_delivery.md`
lists retention policy among unstarted operational hardening at `:172`, and
records "delete local files after upload" as an explicitly **rejected**
alternative at `:204`, because a remote acknowledgement is not a safe
garbage-collection trigger. Any future bound has to respect that.

## What the storage allocation does and does not claim

It bounds **what this device accepts from other Federation members** into its
logical-storage authority. Within that scope it is strong: the budget is
reserved on disk in advance, and a batch that does not fit is refused before
any file is created.

It does **not** prevent FCP from filling a host disk. It does not bound this
device's own recorder capture, its outbox, the session event log, or anything
Docker holds. A device can still fill its drive with the allocation working
exactly as designed -- and on the affected host, that is precisely what
happened. Nobody should read a green allocation as evidence that a host is safe
from exhaustion.

## Follow-up work this audit names

Ordered by measured contribution on the affected host:

1. **Build-cache lifecycle and update preflight.** This is the physical cause and
   the only item the evidence puts above the rest. Move the `ARG`/`ENV`/`LABEL`
   block below the dependency install in both Dockerfiles so a build reuses the
   cached dependency layer instead of writing a fresh gigabyte; bound the
   BuildKit cache rather than letting it grow without limit; and refuse an
   update activation that does not have room to complete, so a host stops before
   exhaustion rather than during a rebuild with Flask already stopped.
2. **Recorder capture retention.** Needs a policy decision first, given the
   rejected alternative above.
3. **Outbox compaction.** The bound already exists; it needs a caller.
4. **Session event log.** Compaction or snapshotting on the coordinator's
   retained relay volume, which is also the volume whose loss is unrecoverable.
5. **Container log limits.** A `logging:` block in `docker-compose.yml`.

Until at least the first of these is delivered, disk exhaustion should not be
treated as closed.
