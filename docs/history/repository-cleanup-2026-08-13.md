# Repository cleanup — 2026-08-13

Status: **provenance record**. This document is the audit trail for the branch
and pull-request consolidation performed on 2026-08-13. It records where every
branch's work ended up so that deleting the branches destroys no history.

Consolidation baseline: `origin/main` at
`59c31a9a1728a63091b7f36f6cf4253f7ccbc5cc`.

Consolidation result: `origin/main` at
`63d548730de882abfd71c9186bd247f16b7abbe6`.

## What changed

Five open pull requests were resolved, all by merging. None was closed
unresolved, and none was merged without its own review and a green
`Federation v1 automated release verdict` on its final head.

| PR | Title | Merge commit on main |
| --- | --- | --- |
| #279 | Phase 0: honest test baseline and an order-independence release gate | `b5c1f63` |
| #280 | Bound the benchmark timeout test by its declared maximum duration | `bde8471` |
| #276 | Make active-leader authority explicit | `5c7a7dd` |
| #278 | Share Federation control-command primitives | `605ead2` |
| #281 | Version Federation SQLite schemas with a transactional migration primitive | `fa38e8a` |
| #277 | Bound Windows fresh-reset shutdown | `c466b4a` |
| #275 | Use Federation login for fresh discovered devices | `63d5487` |

Two of these did not exist before the consolidation. #280 fixed a pre-existing
Windows timing flake that was blocking #276, and #281 recovered the SQLite
schema-versioning work that had been stranded on a branch with no pull request.

### Test baseline

The consolidation began by making the automated baseline honest, because a
release cannot be judged against a suite that passes intermittently.

| | Result |
| --- | --- |
| Before (`59c31a9`), run 1 | 1645 passed, 18 skipped, 3 failed |
| Before (`59c31a9`), run 2 | 1646 passed, 18 skipped, 2 failed |
| After (`63d5487`) | full suite green, and green under shuffled collection orders |

Three test defects were fixed at their root rather than papered over:

- **`test_recorder_artifact_refresh`** — intermittent, not order-dependent. The
  monitor polls the checkpoint every 50 ms and treats any change as one update,
  but `Path.write_text` truncates before writing, so a poll landing inside the
  write observed an intermediate state and the monitor correctly reported two
  changes for one write. The recorder itself writes atomically via
  `_write_json_atomic`, so the test was modelling a write the product never
  performs. Reproduced 1/30 under CPU contention, 0/30 after the fix.
- **`test_termux_phone_script`** — depended on whether the host running the
  tests happened to have `ssh` installed. GitHub runners do, minimal containers
  do not, so the same assertion passed in CI and failed locally.
- **`test_timeout_and_cancellation_are_bounded_and_persisted`** — asserted a
  1.0 s wall-clock bound and failed at 1.047 s on a hosted Windows runner. The
  assertion contradicted the comment above it, which already named the
  definition's two-second bound as the intended limit.

No assertion about product behavior was weakened in any of the three.

### Release gate

`federation-v1-release.yml` gained one job, `Clean-checkout suite order
independence`: the full suite twice from a clean checkout, at a fixed seed for
reproducibility and a rotating seed so new leaks surface over time, plus a
`git status --porcelain` clean-tree check on it and on the Linux matrix leg.
`Federation v1 automated release verdict` requires it, so it is blocking. This
was safe to make blocking immediately because the suite was measured clean
across three independent shuffled orders first.

## Branch disposition

Every branch below is proven contained in `main` by one of:

- **NO-OP** — `git merge-tree --write-tree origin/main <branch>` produces
  `main`'s exact tree, so merging the branch would change nothing.
- **PR-merge proof** — the branch head SHA is identical to the merged pull
  request's head SHA, that pull request's squash commit is present in `main`'s
  history, and a distinctive artifact the branch introduced exists in `main`.
  This is required for squash merges, where commit ancestry alone proves
  nothing.
- **ARCHIVE** — unique content preserved under an immutable tag.

### Contained in main — NO-OP proof

| Branch | Head | Related PR |
| --- | --- | --- |
| `agent/explicit-active-leader-authority` | `bf267eb` | duplicate of #276 |
| `agent/explicit-active-leader-authority-pr` | `1a58f40` | #276 |
| `agent/federation-control-command-primitives-pr` | `887c026` | #278 |
| `agent/federation-human-sso` | `c30b0dd` | #264 |
| `agent/federation-leader-failover` | `91ac268` | #267 |
| `agent/federation-login-first-onboarding` | `6ef3663` | #275 |
| `agent/fix-windows-migrate-reporoot` | `486a69c` | #261 |
| `agent/sqlite-schema-migrations-pr` | `f5addec` | rebuilt as #281 |
| `agent/wip-secure-federated-recorder-data` | `a7a6e48` | #253 |
| `claude/left-rail-navigation` | `1993c72` | #271 |
| `claude/recorder-federation-sharing-zcdjcq` | `8b426ef` | #279, #280 |
| `claude/remove-broken-cf8-rewrite-workflows` | `bcde4ef` | #263 |
| `feat/sqlite-schema-versioning` | `fe759d4` | #281 |
| `fix/federation-member-first-user-bootstrap` | `60b23be` | #274 |
| `fix/windows-fresh-shutdown-timeout` | `37776b3` | #277 |
| `fix/windows-fresh-start-empty-project` | `7b7a930` | #265 |
| `implement-user-authentication-and-authorization-system` | `7d6b4cd` | #255 |
| `pre-stabilization-2026-08-13` | `59c31a9` | consolidation anchor |

`agent/sqlite-schema-migrations-pr` deserves a note: its head is reachable from
`main` because #281 was branched from it, so even the two temporary agent
harness commits it carried remain in history. The harness *files* were deleted
on the way in and are absent from `main`'s tree.

### Contained in main — PR-merge proof

These branches are too far behind `main` for an automatic merge, so containment
rests on the three-part proof above rather than on a tree comparison.

| Branch | Head | PR | Squash commit in main | Artifact verified in main |
| --- | --- | --- | --- | --- |
| `agent/federation-active-leader-product-authority` | `f13a41b` | #268 | `cd3f2db` | leader-bound product controls |
| `agent/federation-capability-requests` | `34229f2` | #257 | `79a074b` | `federation_capability_requests.py` |
| `agent/federation-data-transparency` | `23ee83f` | #258 | `fbfbfa1` | `federated_jsonl_product_bridge.py` |
| `agent/first-user-web-bootstrap` | `8cef4ca` | #266 | `45e9388` | `test_first_user_bootstrap.py` |
| `agent/human-user-docs` | `cb52d86` | #259 | `fd2c877` | `docs/human-authentication.md` |
| `agent/refresh-user-guides-2026-08-12` | `410153a` | #272 | `ccc7183` | refreshed user guides |
| `agent/termux-federation-updates` | `04f73f4` | #256 | `a72a73a` | `termux/fcp-phone-update-agent.sh` |
| `claude/federation-evaluation-consistency-7y4nqf` | `811e9a4` | #269, #262, #260 | `f5668e7` | `federation_knowledge_service.py` |
| `feat/tailscale-federation-discovery` | `ed63597` | #270 | `3fba51a` | `tailscale_host_discovery.py` |
| `fix/admin-created-message` | `87e2ebc` | #273 | `cca1b61` | first-admin success message |

### Archived — unique content not in main

| Branch | Head | PR | Archive tag |
| --- | --- | --- | --- |
| `agent/federation-control-command-primitives` | `15b85a8` | none | `archive/branch/agent-federation-control-command-primitives-2026-08-13` |

This is the only branch in the repository carrying content that did not reach
`main`, and it is worth recording precisely why.

It is the pre-cleanup twin of the branch behind #278. The two differ by exactly
one hunk in `catalog/federation/control_commands.py`. The archived branch makes
`_targets()` raise a distinct `ValueError("malformed_targets")` when the value
is not a list or tuple. The version that shipped in #278 — four minutes newer,
committed as "Preserve malformed target validation contract" — deliberately
restores the original behavior, where a non-collection produces the *same*
error as an invalid item rather than a new distinct one.

The shipped version is the compatibility-preserving one and is the correct
choice. The archived hunk is therefore a deliberately reverted behavior change,
not lost work; it is preserved so the decision remains inspectable.

## Recovery anchors

| Tag | Commit | Purpose |
| --- | --- | --- |
| `pre-stabilization-2026-08-13` | `59c31a9a1728a63091b7f36f6cf4253f7ccbc5cc` | `main` immediately before the consolidation |
| `archive/branch/agent-federation-control-command-primitives-2026-08-13` | `15b85a859927c3068550cd19c18ce4e1a9d3e25c` | the one branch with unique content |

Both tags must exist before the corresponding branches are deleted. The session
that performed this consolidation could not create them: outbound `git push` was
refused by egress policy, and the GitHub API surface available to it can create
branches but not tags. `pre-stabilization-2026-08-13` was therefore created as a
*branch* ref at the correct commit as an interim anchor, and must be converted to
an annotated tag before that branch is removed. Verify with
`git ls-remote --tags origin` before deleting anything.

Pre-existing archive tags `archive-main-before-recorder-recovery-2026-07-28`,
`archive-phase-e72-wip-2026-07-31` and `backup-local-main-2026-07-31` are
unaffected.

## Convention going forward

One branch, one pull request, merged or explicitly archived, branch deleted.
No long-lived agent scratch branches, no stacked pull-request branch left
behind once its stack is resolved, and no work-in-progress branch without a
recorded disposition.
