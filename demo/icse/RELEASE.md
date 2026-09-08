# ICSE Artifact Freeze and Release Procedure

This file is a maintainer checklist for turning the draft reviewer artifact into
an immutable paper artifact. Do not cite a moving branch as the final artifact.

## 1. Confirm release metadata

The publication metadata is now selected:

- software license: **MIT**, with `LICENSE` at repository root;
- sole scholarly creator: **Martin Arthur Andersen**;
- ORCID: `0009-0004-9991-3578`;
- artifact version: `0.1.0` unless intentionally changed before freeze.

Before the public release, confirm that the artifact title and version still match
the paper and keep the claim boundary in `README.md` and `ARTIFACT.md` aligned
with the manuscript.

If the final DOI should be embedded in `CITATION.cff`, create a Zenodo draft and
reserve its DOI before the final code freeze. Do not invent or predict a DOI.

## 2. Freeze the code revision

Finalize all intended artifact source, setup documentation and scripts before
the Federation candidate freeze. Merge their changes only after the exact PR
head passes the `ICSE tool demonstration` workflow, then qualify the resulting
merged main under the normal Federation release procedure. See
`docs/release_process.md` at the repository root.

The paper artifact must use the exact physically accepted Federation v1 source,
not a later development commit. Complete the required software and physical
acceptance and establish the product `v1.0.0` tag at that accepted commit before
publishing the ICSE artifact. Any intervening source change changes the candidate
and requires the corresponding qualification again.

Create an immutable tag on the exact commit intended for publication:

```text
fcp-icse-tool-demo-v0.1.0
```

Require this tag and the Federation `v1.0.0` tag to resolve to the same accepted
40-character commit. They name product and paper artifact metadata; they must
not introduce different source versions. Final source SHA and actual tag names
remain pending until those release operations have succeeded.

Do not move or reuse a published tag. If a post-release defect requires a code
change, create a new patch release and tag instead.

## 3. Require tag-triggered evidence

The release tag must trigger the same workflow used during review. Before making
a GitHub Release or publishing the Zenodo record, require the tag run to complete
successfully with:

- Ubuntu direct E1-E4: PASS;
- Windows direct E1-E4: PASS;
- Docker Compose E1-E4: PASS and retained evidence; and
- independent-node network scenario on Linux and Windows: PASS, all ten checks
  PASS, and all owned processes stopped; and
- publication bundle: PASS.

The release workflow routes these jobs to the intended self-hosted Windows and
Linux infrastructure. Confirm the component execution set contains `Linux`,
`Windows`, and `compose`, and the separate network evidence set contains exactly
`Linux` and `Windows`, all for the exact accepted/tagged SHA. The network entries
must cover only `summary.json`, `events.jsonl`, and `operator-report.html` with
matching digests; no `private-state/` may be published. These artifact
jobs complement the full Federation release and physical gates; they do not
replace them.

Download the publication bundle from that tag-triggered run. For version 0.1.0,
the canonical archival file is:

```text
fcp-icse-tool-demo-0.1.0.zip
```

Verify it against `ZENODO_SHA256`. Also retain `artifact-manifest.json` and
`SHA256SUMS` with the publication records. The manifest and all three evidence
files must identify the tag's exact source revision.

Do not rebuild the canonical ZIP locally after the tag run.

## 4. Publish the immutable archive

Create a GitHub Release from the same immutable tag and attach the exact
CI-generated publication ZIP and its `ZENODO_SHA256` sidecar. The ZIP release
asset and the Zenodo file should be byte-for-byte the same object. Retain the
manifest and unpacked-evidence `SHA256SUMS` with the publication records.

Use a **manual Zenodo software deposit** as the canonical paper artifact so the
DOI identifies the validated source-and-evidence package, not merely a source
snapshot. Upload only the CI-generated publication ZIP to the software record,
select resource type `Software`, and use the final scholarly creator, version,
license, and related-paper metadata.

Because this manual deposit is the canonical DOI-bearing artifact, do not also
create a second automatic Zenodo GitHub-integration DOI for the same release.

Before publishing the Zenodo record, verify:

- the uploaded file checksum matches `ZENODO_SHA256`;
- the archive contains `source/` and `artifact/` under one versioned root;
- `artifact/artifact-manifest.json` names the exact release-tag commit;
- Ubuntu, Windows, and Compose evidence each report E1-E4 passing;
- Linux and Windows network evidence each report all required checks passing and
  complete owned-process teardown, with public-file digests verified;
- title, version, author, ORCID, and MIT license are correct; and
- the record is classified as software.

Publish the record only after these checks. If the package changes after that
point, create a new artifact version rather than silently replacing the cited
release.

## 5. Paper-facing finalization

After the DOI resolves, update the paper/tool-artifact record with:

- artifact title and version;
- immutable Git tag;
- exact commit SHA;
- Zenodo DOI;
- GitHub Release identifier;
- tag-triggered publication-bundle workflow run identifier;
- `ZENODO_SHA256`; and
- the bounded claim/evaluation statement used by the artifact.

The paper's evaluation statements must remain within the claim boundary stated
in the artifact documentation.
