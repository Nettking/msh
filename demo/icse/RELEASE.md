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

Do not add a DOI before Zenodo has actually minted it.

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
a GitHub Release, require the tag run to complete successfully with:

- Ubuntu direct E1-E4: PASS;
- Windows direct E1-E4: PASS;
- Docker Compose E1-E4: PASS and retained evidence; and
- publication bundle: PASS.

Download the publication bundle from that tag-triggered run and retain its
`artifact-manifest.json` and `SHA256SUMS` with the publication records.

The manifest must identify the tag's exact source revision, and all three
evidence files must identify that same revision.

## 4. Archive through Zenodo

Before creating the GitHub Release, connect the `Nettking/msh` repository to the
Zenodo GitHub integration. `CITATION.cff` is sufficient for the metadata needed
here; do not add a duplicate `.zenodo.json` unless Zenodo-specific metadata is
actually required, because Zenodo gives `.zenodo.json` precedence when both are
present.

After the tag-triggered artifact run is green, create a GitHub Release from the
same immutable tag. Allow Zenodo to ingest the release and mint the software
record DOI. Verify that:

- the archived source corresponds to the release tag;
- title, version, authors, and license are correct;
- the Zenodo record is classified as software; and
- the DOI resolves before inserting it into the paper or external artifact page.

If Zenodo reports an archival failure, correct the release metadata and publish
a new version rather than rewriting the already cited tag.

## 5. Paper-facing finalization

Only after the DOI exists, update the paper/tool-artifact record with:

- artifact title and version;
- immutable Git tag;
- exact commit SHA;
- Zenodo DOI;
- GitHub Release identifier;
- publication-bundle workflow run identifier; and
- `SHA256SUMS` or the bundle digest used for the archived evidence copy.

The paper's evaluation statements must remain within the claim boundary stated
in the artifact documentation.
