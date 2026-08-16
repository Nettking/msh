# Changelog

FCP uses Semantic Versioning for published releases. The Git tag is the release
version identity; built FCP services retain the exact source commit separately
through `FCP_BUILD_COMMIT`.

## [1.0.0]

This entry is the source-level release note for FCP Federation v1.0.0. Its
publication status and release date are defined by the Git tag `v1.0.0` and the
GitHub Release, not by a follow-up source edit.

Release scope includes capability-first Federation onboarding, persistent device
identity, authenticated membership and pairing, storage authority and recovery,
trusted AI/compute contribution, durable federated analysis jobs, recorder
capture/publication/control, deterministic MTConnect operational segmentation,
human authentication/RBAC, and bounded manual Federation-wide updates.

### MTConnect operational segmentation

The v1 recorder pipeline includes a machine-neutral operational interpretation
layer over canonical MTConnect observations. It reconstructs sequence-safe,
device-partitioned execution/context timelines and derives deterministic
`MachineRun`, conservatively classified `ProductionCycle`, and tool-tenure
`OperationalEpisode` history with explicit boundary confidence, duration
accounting, provenance, and a disposable SQLite projection/query boundary.

The interpretation remains evidence-conservative: `ACTIVE` is not equated with
production, process motion is not claimed to prove cutting or material removal,
unknown tool context does not fabricate tool-change points, and ambiguous or
multi-channel execution fails closed. Full multi-path operational lanes,
behavioural baselines, anomaly detection, prediction, recommendations, and
OSL/SysML integration are outside this v1 scope.

### Release finalization contract

This entry must be finalized **before** the physical release acceptance campaign.
One exact candidate commit is then selected and all automated and physical
acceptance evidence must identify that same commit.

If any source, dependency lock, container reference, documentation, or release
note changes after physical acceptance, that acceptance is invalid for release
purposes and must be rerun on the new candidate commit.

After the exact candidate commit passes the required physical acceptance
campaign, create the Git tag `v1.0.0` and the GitHub Release on that same accepted
commit. Do not create a source-only commit merely to mark the release as
published. The GitHub Release supplies publication metadata such as the release
date while this source entry remains unchanged.

The physical campaign includes fresh installs, real multi-host Federation
behavior, MTConnect/recorder evidence, update and migration paths, browser review,
and the backup/recovery rehearsal. See `docs/release_process.md` for the complete
publication sequence and verification commands.

### Release-build hardening

- Freeze the complete Python runtime/test dependency resolution used by the v1
  Docker build and release gate.
- Pin the Python runtime base image to an immutable digest.
- Pin the Ollama runtime image to an explicit version and immutable multi-platform
  index digest.
- Pin release-CI Python patch versions and test/lint tooling.
- Expand the release-gate path boundary to cover Dockerfiles, launchers,
  migration/setup files, release constraints, and changelog changes.
- Verify immutable container references against registry metadata before the
  release candidate is accepted.
