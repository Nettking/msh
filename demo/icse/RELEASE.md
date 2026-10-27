# Federation demonstration: optional immutable archive procedure

This maintainer checklist applies if a reproducible demonstration bundle is
released or archived. No release, venue submission, recording, or archival
deposit is planned or required by this guide. Do not describe a moving branch
as an immutable artifact.

## 1. Confirm identity and metadata

Use the title **FCP Federation demonstration**, the actual bundle version and
exact source revision. The repository license is **MIT**, with `LICENSE` at
repository root. `CITATION.cff` records Martin Arthur Andersen as the sole
scholarly creator, with ORCID `0009-0004-9991-3578`.

The existing `fcp-icse-tool-demo-v0.1.0` tag records the earlier 2026-08-31
artifact at commit `954faa357638b13d7291e69ea98fa620c0c3d637`. Preserve that
tag unchanged. The `icse` tag prefix, archive filenames and `ZENODO_SHA256`
sidecar are historical compatibility identifiers, as explained in `README.md`.
They do not commit this demonstration to a particular venue or archive service.

Keep the stated purpose and claim boundaries in `README.md`, `ARTIFACT.md` and
any accompanying report aligned with what the retained evidence actually shows.

## 2. Freeze the exact source

Finalize the intended demonstration source, setup documentation and scripts.
Merge their changes only after the exact PR head passes the Federation
demonstration workflow in `.github/workflows/icse-tool-demo.yml`. Qualify any
Federation product release under the normal procedure in
`docs/release_process.md`; a demonstration run does not replace software or
physical acceptance.

If a bundle is described as evidence for an accepted product release, it must
use that exact physically accepted source, not a later development commit. Its
source/tag and the product release tag must resolve to the same accepted
40-character commit. Any intervening source change changes the candidate and
requires the corresponding qualification again.

For an optional tagged demonstration archive, use the existing compatible
`fcp-icse-tool-demo-v<version>` naming pattern on the exact intended commit.
Choose a version not already used; this guide selects no upcoming version.
Do not move or reuse an existing tag, even if it has no GitHub Release. If a
post-release defect requires a code change, use a new version and tag.

## 3. Require matching explicitly invoked evidence

The workflow supports manual dispatch and path-filtered branch pushes, not
automatic tag-push execution. For a tagged archive, explicitly dispatch the same
workflow at that tag, verify the actual run's ref and exact source SHA, and
require it to complete successfully with:

- Ubuntu direct E1-E4: PASS;
- Windows direct E1-E4: PASS;
- Docker Compose E1-E4: PASS and retained evidence;
- independent-node network scenario on Linux and Windows: PASS, all ten checks
  PASS, and all owned processes stopped; and
- reproducible bundle: PASS.

The workflow routes these jobs to the intended self-hosted Windows and Linux
infrastructure. Confirm the component execution set contains `Linux`, `Windows`,
and `compose`, and the separate network evidence set contains exactly `Linux`
and `Windows`, all for the exact tagged SHA. The network entries must cover only
`summary.json`, `events.jsonl`, and `operator-report.html` with matching digests;
no `private-state/` may be shared. These jobs complement the full Federation
release and physical gates; they do not replace them.

Download `fcp-icse-tool-demo-<version>.zip` from that verified run and verify
it against `ZENODO_SHA256`. Retain `artifact-manifest.json` and `SHA256SUMS`
with the archival records. The manifest and all component and network evidence
must identify the tag's exact source revision.

Do not rebuild the canonical ZIP locally after that run.

## 4. Optional distribution or archival deposit

If the bundle is attached to a release or deposited in an archive, use the exact
CI-generated ZIP and its `ZENODO_SHA256` sidecar. Any distributed copies must be
byte-for-byte the same object. Retain the manifest and unpacked-evidence
`SHA256SUMS` with the archival records.

Before sharing the archive, verify:

- its checksum matches `ZENODO_SHA256`;
- it contains `source/` and `artifact/` under one versioned root;
- `artifact/artifact-manifest.json` names the exact release-tag commit;
- Ubuntu, Windows, and Compose evidence each report E1-E4 passing;
- Linux and Windows network evidence each report all required checks passing and
  complete owned-process teardown, with public-file digests verified; and
- title, actual version, creator, ORCID, and MIT license are correct.

If the package later changes, use a new artifact version rather than silently
replacing the archived object. Keep the immutable Git tag, exact commit SHA,
workflow run identifier, checksum and bounded evaluation statement together.
Include any release or archive link only after it exists and has been verified.
