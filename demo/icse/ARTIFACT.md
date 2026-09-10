# Reviewer Artifact Guide

## Artifact identity

**Artifact:** FCP ICSE Tool Demonstration
**Candidate version:** 0.2.0
**Primary entrypoint:** `python -B -m demo.icse.network.run`
**Supporting component entrypoint:** `demo/icse/docker-compose.yml`
**Network evidence schema:** `fcp.icse-network-demo.v1`
**Component evidence schema:** `fcp.icse-demo-summary.v1`

The final Federation release SHA/tag and ICSE artifact tag are established by
the release process, not this candidate version label. Do not infer a completed
release or physical PASS from this document. The product and paper artifact
must use the same exact accepted source revision.

The primary network story follows independently running members through
authenticated join, owner-scoped declarations, real synthetic-payload exchange,
leader-process loss, quorum recovery and repeated interaction. Its production
TCP/WebSocket paths use separate process state. E1–E4 provide supporting
component experiments with synthetic external observations and authorities.

## Requirements

The network demonstration requires Python 3.12 and the repository's
`requirements.txt` with `constraints-phase2.txt`. Use Git for a clean checkout,
or a verified complete publication ZIP and its trusted digest for archive mode.
Follow `source/demo/icse/network/README.md` in the ZIP (or
`demo/icse/network/README.md` in a checkout). It includes Windows/POSIX commands,
strict Git/archive identity checks and output isolation. No Docker, Tailscale
account, private hostnames, model endpoint or physical machine is required.

The supporting E1–E4 Compose path requires:

- Git, when running from a repository checkout;
- Docker Engine with Docker Compose V2; and
- network access during the first image build unless all required image and
  Python package layers are already cached.

No external Federation, database server, model endpoint, recorder, or physical
machine is required for E1-E4.

The direct E1–E4 CI path uses the same Python 3.12 requirements/constraints as
the network demonstration. The two experiments have separate commands and
evidence; passing one does not replace the other.

## Supporting E1–E4 quick start

### From a Git checkout

Use the immutable tag named by the paper. Verify that its resolved commit equals
the release record's full SHA and that `git status --porcelain` is empty. From
the repository root:

```bash
export FCP_BUILD_COMMIT="$(git rev-parse HEAD)"
mkdir -p demo/icse/evidence
docker compose -f demo/icse/docker-compose.yml run --build --rm demo \
  --output /evidence/icse-summary.json
```

On PowerShell:

```powershell
$env:FCP_BUILD_COMMIT = git rev-parse HEAD
New-Item -ItemType Directory -Force -Path demo/icse/evidence | Out-Null
docker compose -f demo/icse/docker-compose.yml run --build --rm demo --output /evidence/icse-summary.json
if ($LASTEXITCODE -ne 0) { throw 'ICSE demonstration failed.' }
```

### From the publication ZIP, without Git metadata

Download the canonical `fcp-icse-tool-demo-<version>.zip` and its
`ZENODO_SHA256` sidecar from the same release. Before extraction, compare the
ZIP's SHA-256 with the sidecar. On Linux, with both files in the current
directory, use `sha256sum -c ZENODO_SHA256`; on macOS use
`shasum -a 256 -c ZENODO_SHA256`. On PowerShell use
`Get-FileHash -Algorithm SHA256` on the ZIP and require equality with the
sidecar's digest. A mismatch is a failed integrity check; do not continue.

After extraction, the versioned directory contains `source/` and `artifact/`.
The source was produced by `git archive` and has no `.git` directory. Open
`artifact/artifact-manifest.json`, require schema
`fcp.icse-artifact-manifest.v1`, and compare its `source_revision` with the
release record's exact SHA. Set `FCP_BUILD_COMMIT` to that value before running
from `source/`; do not substitute the revision of an enclosing Git repository.

On PowerShell, starting in the extracted versioned directory:

```powershell
$manifest = Get-Content -Raw artifact/artifact-manifest.json | ConvertFrom-Json
if ($manifest.schema -ne 'fcp.icse-artifact-manifest.v1' -or
    $manifest.source_revision -notmatch '^[0-9a-f]{40}$') {
    throw 'Invalid artifact source identity.'
}
$env:FCP_BUILD_COMMIT = $manifest.source_revision
# Require this value to equal the exact SHA in the release record.
$env:FCP_ICSE_OUTPUT_DIR = Join-Path (Get-Location) 'reviewer-component-evidence'
New-Item -ItemType Directory -Force -Path $env:FCP_ICSE_OUTPUT_DIR | Out-Null
Set-Location source
docker compose -f demo/icse/docker-compose.yml run --build --rm demo --output /evidence/icse-summary.json
if ($LASTEXITCODE -ne 0) { throw 'ICSE demonstration failed.' }
```

On a POSIX shell, copy the verified 40-character `source_revision` value from
the manifest into `FCP_BUILD_COMMIT`, set `FCP_ICSE_OUTPUT_DIR` to an absolute
directory outside `source/`, create that evidence directory, enter `source/`,
and use the same Compose command above. This component path does not require a
host Python installation or Git checkout. Keeping output outside source also
preserves the complete file set required by network archive verification; use a
fresh extraction if a prior command created extra source files or bytecode.

The retained archive evidence/metadata can also be checked from `artifact/`
using `sha256sum -c SHA256SUMS` (or the macOS equivalent). These checksums and
the archive's release provenance establish source/evidence identity; the
revision printed by a new run is supplied input, not independent attestation.

### Check the new result

The command prints the summary and writes a host copy to:

```text
demo/icse/evidence/icse-summary.json
```

When `FCP_ICSE_OUTPUT_DIR` is set, the retained file is instead
`icse-summary.json` inside that external directory.

A successful result has all of the following properties:

- `schema` is `fcp.icse-demo-summary.v1`;
- `implementation_commit` equals the revision being reviewed;
- `passed` is `4` and `total` is `4`; and
- E1, E2, E3, and E4 each report `result: "pass"`.

Any scenario failure produces a non-zero process exit status.

Require the current command to exit successfully before inspecting the new
output; an old retained JSON file is not evidence that the current command passed.
Use `--build` even after changing source revisions, because supplying a new
`FCP_BUILD_COMMIT` at runtime does not update an already-built image. Keep the
reviewer's newly generated summary separate from the canonical tag-run evidence.

## Scenario-to-claim map

| Scenario | Observed boundary | Passing evidence |
| --- | --- | --- |
| E1 | Selective contribution | Independent desired/activation states and runtime consequences for heterogeneous local functions |
| E2 | Authority separation | Storage moves from pending to active only after separate authority assignment |
| E3 | Runtime eligibility | A constant provider identity is selected when ready and rejected when draining or expired |
| E4 | Ownership separation | Selection creates no ownership; explicit claim creates a lease; expired-lease progress is rejected |

The scenario evidence supports only these evaluated boundaries. It does not turn
synthetic physical observations into evidence about hardware performance or
physical deployment reliability.

## Publication evidence bundle

The `ICSE tool demonstration` workflow retains all three E1–E4 runs (Linux,
Windows and Linux Compose) and additionally runs the network story on both
self-hosted operating systems. Its bundle command supplies
`--network-evidence-root` and requires exactly Linux/Windows public network
artifacts, matching source revision, all ten checks PASS and completed process
teardown. Missing or partial network results cannot be replaced by component
results. Private state, extra files and inconsistent event logs are rejected.

The bundle includes:

```text
ARTIFACT.md
CITATION.cff
README.md
DEMONSTRATION.md
ARCHITECTURE.md
VIDEO.md
RELEASE.md
SHA256SUMS
ZENODO_SHA256
artifact-manifest.json
evidence/
  compose.json
  Linux.json
  Windows.json
network-evidence/
  Linux/
    summary.json
    events.jsonl
    operator-report.html
  Windows/
    summary.json
    events.jsonl
    operator-report.html
figures/
  federation-v1-overview.svg
network/
  README.md
fcp-icse-tool-demo-<version>.zip
```

`artifact-manifest.json` records the exact source revision, workflow run,
scenario set, per-evidence SHA-256 digests, archive layout, and whether a
repository license was declared at bundle time. Its additive `network_evidence`
entries record execution labels, required checks and all three public-file
digests per OS. `SHA256SUMS` covers the unpacked
artifact metadata and evidence. `ZENODO_SHA256` identifies the complete
self-contained publication ZIP.

The publication ZIP contains the exact Git-tracked source tree directly under
`source/` and the validated reviewer metadata/evidence under `artifact/`. A tag
named `fcp-icse-tool-demo-v0.2.0` therefore produces
`fcp-icse-tool-demo-0.2.0.zip`.

For the publication version, archive exactly the ZIP produced by the
tag-triggered workflow for the release tag cited by the paper. Do not rebuild the
ZIP locally after the tag run.

## Synthetic boundary

`demo/icse/harness.py` supplies deterministic substitutes for external physical
services and observations. Federation onboarding, contribution state,
capability-specific authority, provider reports, provider selection, durable job
state, ownership leases, and the relevant Flask/application services remain the
production implementations under evaluation.

This distinction matters: E1-E4 demonstrate integration and control boundaries;
they do not claim physical realism of the synthetic endpoints.

E2's three member stacks share a coordinator inside one process. This experiment
uses Flask test clients and configured discovery, not independently running
network nodes or a live browser. E3 supplies report timestamps to evaluate
eligibility; E4 supplies an expired lease time. Neither is a wall-clock failure
detection or failover experiment. No actual compute workload result or Recorder
measurement is produced by E1-E4.

## Reproducibility notes

- Each scenario executes in a separate child process.
- Scenario workspaces are deleted only after the child process exits.
- The summary records the supplied source revision.
- The CI bundle refuses mixed-revision or incomplete evidence.
- The publication ZIP's `source/` tree is generated by `git archive` from the
  same revision used by all evidence jobs. The revision's tracked export rules
  omit the capture and identifying capture-analysis note described in
  `example-data/README.md`, whose public redistribution provenance is
  undocumented. Both demonstrations use synthetic inputs and remain complete
  without those private capture materials.
- Release tags matching `fcp-icse-tool-demo-v*` rerun the complete artifact
  workflow.

## Known limitations

The first Docker build may require internet access to obtain base-image and
Python dependency layers. The artifact does not currently publish a prebuilt
container image.

The network demonstration's capability declarations and dataset are
illustrative. Generic membership-authorized relay delivery is not approved
provider activation, Recorder capture, compute execution, storage transfer or
JSONL materialization. Clients explicitly reconnect to the elected relay; this
does not demonstrate transparent endpoint migration. WebSockets use explicit
development plaintext on loopback, and the offline report is not the Flask UI.
All processes share one machine. Physical Federation-v1 and strict evidence
validation remain separate; neither local processes nor E1–E4 establish physical
multi-host, hardware, browser or duration acceptance. Actual release-specific
execution results remain pending until their recorded evidence passes.

The repository is licensed under the MIT License. The publication bundle records
that license state and includes the repository `LICENSE` file in the exact source
tree archived for the evaluated revision.

`CITATION.cff` identifies Martin Arthur Andersen as the sole scholarly creator,
with ORCID `0009-0004-9991-3578`. A DOI is intentionally omitted until a real
Zenodo record has been reserved or published.

## Claim boundary

The artifact supports the bounded claim that FCP provides one inspectable control
plane across heterogeneous engineering contribution classes while preserving the
evaluated capability-specific activation, runtime-eligibility, recommendation,
and durable-ownership boundaries.

It does not establish complete physical Federation-v1 acceptance, comparative
developer productivity, configuration-space reduction, performance superiority,
general scalability, hardware-specific generality, optimal scheduling, or a
general worker-security theorem.
