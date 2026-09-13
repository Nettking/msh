# Remaining exact-source qualification

Source: f104038a2b77705adaa547cdd1df7d895bf80a41. PR #483 head06b95678
and this native PR merge source have identical Git trees. Results keep their real
native source SHA; no f104 result is relabelled as native06 or future main.

The clean handoff supersedes the archived plan to rerun every green workflow just
to replace a synthetic merge SHA with a byte-identical PR-head SHA. Preserve the
existing f104 results and fill its absent gates. A temporary qualification branch
at exact f104 gives unchanged workflow_dispatch jobs that same checkout identity.
It is not a frozen authoritative release candidate or a deployment ref.

Checked-in bases: docs/releases/federation_v1_scope.md release acceptance;
docs/implementation/federation/active/federation_v1_closeout_plan.md V1-F/V1-G;
docs/implementation/federation/acceptance/cf7_acceptance_harness.md exact-commit
CF7-A/CF7-B/broad matrices; existing workflow contracts below. Preserve the
established required 37-job plus 3 companion coverage without inventing new gates.

| Workflow | Required jobs | Action |
|---|---:|---|
| federation-v1-release.yml | 16 | Retain34746641263, reviewed native f104 |
| phase2-federation.yml | 2 | Retain34746641323 |
| cf7b-product-physical-acceptance.yml | 2 | Retain34746641366 |
| federation-software-update.yml | 2 | Retain34746641390 |
| product-branding.yml | 1 | Retain34746641274 |
| icse-tool-demo.yml | 4 | Recover failed jobs34746641262 attempt2; retain compose |
| cf7-acceptance-harness.yml | 2 | Missing on f104; dispatch once |
| cf7c-physical-test-readiness.yml | 2 | Missing on f104; dispatch once |
| cf8-role-retirement.yml | 2 | Missing on f104; dispatch once |
| phase-f85-operator-federation-surface.yml | 2 | Missing on f104; dispatch once |
| ci-test-sharding.yml | 2 | Missing on f104; dispatch once with default shared-pool |
| cfi2-onboarding-composition.yml | 2 | Missing companion on f104; dispatch once |
| release-image-metadata.yml | 1 | Missing verification companion on f104; dispatch once |

Guard PR475/483/473/main and f104 merge ref before dispatch, retain accepted or
uncertain operations, inspect existing runs before any retry. No workflow edits,
pool/label/account changes, successful ICSE compose repeat, image publication,
deployment or physical acceptance. Required gates must pass before merge; the
diagnostic10/10 result is not substituted for qualification. If ICSE recovery
fails, classify its actual failure before another action; no blind retry loop.

Merge review of unchanged #475:404 remains immediate; declared-body discard is
limited to4096 bytes and one absolute10-second cleanup budget; no identity/grant
authority is entered. Existing focused Windows/Linux46PASS and base-red proof
remain valid. Unchanged #483 adds allowlisted context and failure-path public
upload only; errors remain exit1, private logs/state are excluded, owned children
stop. Its existing focused proof and native failed-path evidence remain valid.
No new code-review blocker found in either small current diff.
