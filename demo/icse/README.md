# FCP ICSE Tool Demonstration

This directory is the paper-specific reviewer entrypoint for the FCP tool
demonstration. It is intentionally separate from normal product deployment and
from physical Federation-v1 acceptance.

For the reviewer-oriented instructions, expected output, and limitations, see
[`ARTIFACT.md`](ARTIFACT.md). Maintainer freeze/release instructions are in
[`RELEASE.md`](RELEASE.md). Repository-level citation metadata is in
[`CITATION.cff`](../../CITATION.cff).

## Current scope

The runner exposes four claim-aligned scenarios:

- **E1 — selective contribution:** one member observes several functions and
  independently enables, disables, or defers them.
- **E2 — authority boundary:** a requested storage contribution remains pending
  until the separate storage authority permits it, while AI, compute, and
  storage contributions retain capability-specific activation semantics.
- **E3 — runtime eligibility:** the same provider identity is selected while its
  short-lived resource report is ready, but rejected when it is draining or its
  report has expired.
- **E4 — ownership boundary:** provider recommendation creates no job ownership;
  a separate coordinator claim creates a leased attempt, and progress under an
  expired lease is rejected without mutating that attempt.

E1 and E2 instantiate the production FCP onboarding, inspection, benchmark,
contribution, coordinator, persistence, policy, and Flask route implementations
directly. `harness.py` owns only deterministic stand-ins for external physical
services and hardware observations; it does not import acceptance tests or
replace the FCP control plane with demo-specific implementations. E3 exercises
the production analysis capability contract, provider-report validation, and
provider-selection logic directly. E4 composes that production selection logic
with the production durable job store and ownership-lease checks.

Each scenario is executed in its own child process. The parent removes the
scenario workspace only after that process exits. This gives the reviewer path
deterministic process-lifecycle cleanup on both POSIX and Windows, including
SQLite-backed production services that intentionally keep connections for the
lifetime of a service instance.

## Run

From the repository root, the shortest reviewer command is:

```bash
docker compose -f demo/icse/docker-compose.yml run --rm demo
```

The process prints deterministic JSON and exits non-zero if a scenario fails.
The current summary schema is `fcp.icse-demo-summary.v1`.

To retain commit-bound evidence on the host as well as print it:

```bash
FCP_BUILD_COMMIT=$(git rev-parse HEAD) \
  docker compose -f demo/icse/docker-compose.yml run --rm demo \
  --output /evidence/icse-summary.json
```

The Compose file bind-mounts `demo/icse/evidence/` to `/evidence` by default, so
the retained file is `demo/icse/evidence/icse-summary.json`. The evidence
directory is intentionally ignored by Git.

## CI and publication bundle

The dedicated `ICSE tool demonstration` workflow executes:

1. the direct Python entrypoint on clean Ubuntu and Windows runners;
2. the Docker Compose reviewer path on Ubuntu, retaining its JSON evidence; and
3. a publication-bundle job that refuses to package the run unless all three
   summaries report the same source revision and the same four passing
   scenarios.

The resulting workflow artifact contains:

- Ubuntu, Windows, and Docker Compose `icse-summary.json` evidence;
- `artifact-manifest.json` binding those records to the source revision;
- `CITATION.cff`, this README, and the reviewer guide;
- `SHA256SUMS` for the unpacked artifact evidence/metadata;
- `ZENODO_SHA256` for the complete publication archive; and
- one self-contained `fcp-icse-tool-demo-<version>.zip`.

The self-contained ZIP contains the exact Git-tracked source tree directly under
`source/` and the validated artifact evidence/metadata under `artifact/`. This is
the canonical file to attach to the GitHub Release and deposit as the Zenodo
software artifact.

Tags matching `fcp-icse-tool-demo-v*` trigger the same workflow. A tag such as
`fcp-icse-tool-demo-v0.1.0` yields `fcp-icse-tool-demo-0.1.0.zip`. The paper must
cite the immutable release/tag and DOI whose tag-triggered run is green, not a
moving draft branch or a pre-tag CI result.

## Evidence emitted

E1 records each observed capability's desired and activation states plus the
runtime consequences at the synthetic environmental boundary. E2 records the
contribution states and the storage transition before and after separate
authority assignment (`PENDING` to `ACTIVE`). E3 holds the provider identity and
job requirement constant while recording the selection outcome for `READY`,
`DRAINING`, and expired resource reports. E4 records that selection leaves
ownership absent, an explicit claim creates the lease, and an expired lease is
refused with `ownership-lease-expired` while the attempt stays `ASSIGNED`.

These records are scenario evidence, not a performance benchmark. Runtime timing
may be collected later for reproducibility but must not be interpreted as a
performance-superiority result without a separate evaluation design.

## Claim boundary

This artifact supports the bounded tool-integration claim that FCP provides one
inspectable control plane across heterogeneous engineering contribution classes
while preserving the evaluated capability-specific activation, runtime
eligibility, recommendation, and durable-ownership boundaries. The individual
mechanisms are not claimed as new architectural primitives.

The artifact is **not** evidence of complete physical Federation-v1 acceptance,
comparative developer productivity, configuration-space reduction, performance
superiority, general scalability, hardware-specific generality, optimal
scheduling, or a general worker-security theorem.
