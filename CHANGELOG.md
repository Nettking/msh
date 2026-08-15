# Changelog

FCP uses Semantic Versioning for published releases. The Git tag is the release
version identity; built FCP services retain the exact source commit separately
through `FCP_BUILD_COMMIT`.

## [Unreleased]

### Candidate for v1.0.0

The current line is being stabilized as the first FCP Federation v1 release
candidate. **This is not a published release and no `v1.0.0` tag exists yet.**

Candidate scope includes capability-first Federation onboarding, persistent
device identity, authenticated membership and pairing, storage authority and
recovery, trusted AI/compute contribution, durable federated analysis jobs,
recorder capture/publication/control, human authentication/RBAC, and bounded
manual Federation-wide updates.

Before publication, one exact candidate commit must still pass the required
physical acceptance campaign, including fresh installs, real multi-host
Federation behavior, MTConnect/recorder evidence, update and migration paths,
browser review, and the backup/recovery rehearsal. Release notes and the
`v1.0.0` GitHub Release must point to that exact accepted commit.

### Release-build hardening

- Freeze the complete Python runtime/test dependency resolution used by the v1
  Docker build and release gate.
- Pin the Python runtime base image to an immutable digest.
- Replace mutable Ollama `latest` image references with an explicit tested
  version.
- Pin release-CI Python patch versions and test/lint tooling.
- Expand the release-gate path boundary to cover Dockerfiles, launchers,
  migration/setup files, release constraints, and changelog changes.
