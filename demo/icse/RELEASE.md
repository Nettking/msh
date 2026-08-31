# ICSE Artifact Freeze and Release Procedure

This file is a maintainer checklist for turning the draft reviewer artifact into
an immutable paper artifact. Do not cite a moving branch as the final artifact.

## 1. Resolve release metadata

Before the public release:

- choose and add the repository/software license deliberately;
- replace the provisional `Nettking` alias in `CITATION.cff` with the final
  scholarly author list and ORCID identifiers where available;
- confirm the artifact title and version (`0.1.0` unless intentionally changed);
- keep the claim boundary in `README.md` and `ARTIFACT.md` aligned with the paper.

If the final DOI should be embedded in `CITATION.cff`, create a Zenodo draft and
reserve its DOI before the final code freeze. Do not invent or predict a DOI.

## 2. Freeze the code revision

Merge the publication-hardening PR only after the exact PR head passes the
`ICSE tool demonstration` workflow. Record the resulting merged commit.

Create an immutable tag on the exact commit intended for publication:

```text
fcp-icse-tool-demo-v0.1.0
```

Do not move or reuse a published tag. If a post-release defect requires a code
change, create a new patch release and tag instead.

## 3. Require tag-triggered evidence

The release tag must trigger the same workflow used during review. Before making
a GitHub Release or publishing the Zenodo record, require the tag run to complete
successfully with:

- Ubuntu direct E1-E4: PASS;
- Windows direct E1-E4: PASS;
- Docker Compose E1-E4: PASS and retained evidence; and
- publication bundle: PASS.

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
CI-generated publication ZIP. The release asset and the Zenodo file should be
byte-for-byte the same object.

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
- title, version, authors, ORCIDs, and license are correct; and
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
