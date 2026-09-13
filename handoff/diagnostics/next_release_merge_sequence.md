# Prepared release continuation

This is preparation under the clean handoff, not evidence of a merge, candidate
freeze, deployment, or physical PASS.

## Before merging

Finish the existing f104 qualification and retain native/public artifact proof.
Use diagnostics/f104-remaining-qualification.md for the gap-only mapping.
Keep #475=5e6f1843, #483=06b95678, #473=440123f6 and main=b7194820 guarded.
The current small-diff review has no additional blocker. GitHub review lists
were empty at10:03UTC; this is not an external approval. Branch-protection,
effective-rules and ruleset reads returned403, so policy remains unknown.
Normal expected-head merge APIs must enforce server policy; no bypass option.

## Merge and qualify the resulting main

Merge #475 to main, retarget #483 from its now-merged parent branch to main,
verify its intended delta remains the same, then merge #483. Merge the separate
#473 retirement after applicable qualification/review is reconciled. Preserve
the separate repairs and do not bring coordination files into any merge.

Offline git merge-tree of f104 and440123f6 succeeded without conflicts and
produced expected final tree cc9b29515d47d754f24199bf403213e7ff111315.
This is an expected Git tree, not an authoritative commit. Its delta from f104
is precisely #473's eight workflow removals/four documentation edits. Diff
hygiene passes. No branch or checkout was merged by this calculation.

After the release changes reach main, record the actual main commit and compare
its tree to the reviewed expectation. Inspect automatic runs on that actual
commit, dispatch only absent required workflows, and retain each exact-source
result once. Do not qualify intermediate main or rerun valid final-main passes.
Freeze AUTHORITATIVE_SHA only after that qualification is complete.

## Revalidation and physical admission

CHANGELOG.md already contains finalized1.0.0 source notes; publication metadata
belongs to the later tag/release on the accepted commit. No follow-up source edit
is needed merely to record publication (docs/release_process.md).

Use fresh clean exact-SHA checkouts and checked-in revalidation before physical
host preparation. The checked-in physical wrappers record candidate/fingerprint,
OS/profile, runtime provenance and baseline samples. Inspect live owned targets
before any startup, restart or fault action; protected Recorder data is excluded.
Read candidate-version contracts and scenario side effects before execution.

Windows preparation uses v1_prepare_windows.ps1 with nettking/local-ai; POSIX
preparation uses v1_prepare_linux.sh with nitro/school-control. These are planned
roles; live role/provenance admission must be verified at execution. The separate
Recorder host and recorder-plus-AI case require their own safe target review.

P07 requires3600 real seconds, P12 requires86400, each with at least2 samples.
All timed assertion packets must have the same host/run ID and lie inside its
begin/finish interval. P12 must start with meaningful owned history. The untimed
wrappers never start P07/P12 implicitly. Use the strict timed layer; preparation,
automated probes, CI, missing observations and elapsed time alone cannot yield
physical PASS. Human/browser/platform observations remain evidence requirements.
No physical command has been executed in this preparation.
