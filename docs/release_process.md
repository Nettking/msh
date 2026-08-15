# FCP v1 release process

Status: **current release procedure**

This procedure keeps the source release notes, physical acceptance evidence, Git
tag, GitHub Release, and runtime build identity tied to one exact commit.

## Release authorities

FCP uses two complementary identities:

- `v1.0.0` is the Semantic Versioning release identity.
- `FCP_BUILD_COMMIT` is the exact source/runtime identity baked into FCP images.

The GitHub Release is publication metadata for the `v1.0.0` tag. It must not
create a second source version or point at a different commit.

## Finalization invariant

The source tree is finalized **before** physical acceptance. `CHANGELOG.md`,
dependency locks, container digests, launchers, documentation, and release gates
must already contain their final v1 content when the release candidate commit is
selected.

After physical acceptance, publishing v1.0.0 must not require another source
commit. If any tracked source file changes, the old physical acceptance is no
longer release evidence for the new commit and the affected acceptance must be
rerun.

## Required sequence

1. Merge all intended v1 source, documentation, dependency-lock, and container
   reference changes.
2. Confirm the worktree/repository candidate is clean and identify the exact
   candidate commit `C`.
3. Run the complete automated release gate on `C` and require every release job
   to pass.
4. Verify the immutable container references against registry metadata.
5. Run the physical acceptance campaign on `C`, including:
   - fresh installation on the supported platforms used for acceptance;
   - real multi-host Federation creation/join/reconnect behavior;
   - recorder discovery, MTConnect capture, Federation publication, digest/data
     visibility, restart, and resume behavior;
   - update and migration paths;
   - browser/operator review;
   - storage/failover scenarios required by the active acceptance contract; and
   - the backup/recovery rehearsal in `backup_recovery.md`.
6. Record `C` in the physical acceptance evidence. If anything requires a source
   fix, merge the fix, choose a new candidate commit, and repeat the required
   acceptance on that new commit.
7. Only after `C` is accepted, create the Git tag `v1.0.0` **at `C`**.
8. Verify the tag resolves to exactly `C`.
9. Create the GitHub Release from tag `v1.0.0`, using the finalized source
   release notes. The release must resolve to the same `C`.
10. Do not make a source-only commit merely to record that publication happened.
    Publication state and release date belong to Git/GitHub release metadata.

## Verification commands

Set the accepted commit explicitly before tagging:

```bash
C=<accepted-40-character-commit>
git fetch origin main --tags
test "$(git rev-parse origin/main^{commit})" = "$C"
git show --no-patch --oneline "$C"
```

Create the release tag only after physical acceptance:

```bash
git tag -a v1.0.0 "$C" -m "FCP v1.0.0"
git push origin v1.0.0
```

Then verify the remote tag resolves to the same commit:

```bash
git fetch origin tag v1.0.0 --force
test "$(git rev-list -n 1 v1.0.0)" = "$C"
```

When creating the GitHub Release, select the existing `v1.0.0` tag. Do not let a
release UI create a different tag or target another branch/commit.

## Failure rule

A tag or release must not be created when physical acceptance is incomplete,
evidence points at a different commit, the release gate is red, or the candidate
changed after acceptance. In those cases, fix the candidate and repeat the
required checks instead of weakening the release identity.

## After publication

Branch cleanup is separate from release publication. Delete obsolete merged
release/stabilization branches only after the `v1.0.0` tag and GitHub Release are
verified to resolve to the accepted commit.
