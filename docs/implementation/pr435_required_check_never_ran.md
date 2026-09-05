# PR 435: a required check has been failing, and the billing block hid it

`scripts/check_product_branding.py` exits 1 on PR 435's branch and exits 0 on
`main`. `product-branding` is a required check, so the pull request could not
have gone green. Nobody had seen it, because every hosted job on this
repository is blocked at the account level and completes in seconds with no
runner assigned — the check has never actually run on this branch.

This note records the defect, the fix, and the sweep that went looking for
others of the same shape.

## The defect

PR 435's own new test file names the rig's second recorder host after the
retired product spelling:

```
catalog/mtconnect_recorder/tests/test_multi_recorder_capability_identity.py
```

28 occurrences, across a module docstring, one comment, two `_recorder(...)`
display names, and the local variables that carry them.

The checker walks every tracked file and rejects the token outright. Its
`ALLOWED_REPOSITORY_FILES` set only strips the repository slug before matching,
which is a narrow allowance for files that must name the remote — it cannot
cover a bare host name, and it is not meant to.

Classification: **other product defect, introduced by PR 435**. `main` at
`6101c86` passes the same checker. The defect is real, it is this PR's, and it
blocks a required check.

## Why it was invisible

Every hosted job on both open pull requests reports `runner_id: 0` and an empty
`runner_name`, finishes 1–7 seconds after starting, records no steps, and
returns HTTP 404 from its log endpoint. GitHub's own annotation on each says
the account's payments have failed or its spending limit needs raising.

So `product-branding` did not evaluate this branch and report red — it never
ran at all. The red check on the pull request says nothing about branding; it
says the same thing every other red check there says. The defect would have
surfaced the moment billing was restored, most likely at the worst possible
time, with the candidate otherwise ready.

There is a general lesson in that, worth more than the specific fix: while the
hosted gate is down, **a green-looking absence of evidence is not evidence**,
and any check that only runs there has to be run by hand before a candidate is
called ready.

## The fix

The second host becomes `Second`, and the docstring and comment describe it as
the second, separately paired recorder host. The rename is mechanical: no
assertion, fixture, scenario, capability identity or timing changes. The file's
tests and the publication-store suite pass together, 24 in 8.91 s, and `ruff`
is clean on the file.

Adding the file to the checker's allowlist was rejected. That would widen a
product boundary to accommodate a test, which is the wrong direction: the
boundary is the product requirement and the test is the thing that should bend.

## Looking for others of the same shape

One instance of a class of defect is a reason to look for the rest of the
class. The full test suite cannot find them — this defect lives in a workflow
step that is not `pytest`, so a green suite says nothing about it.

Every `ruff check`, `compileall`, `check_product_branding.py` and acceptance
manifest assertion across all 57 workflow files was extracted and run against
the candidate, using the same `ruff` the release gate pins (0.16.3, from
`constraints-release.txt`):

```
98 distinct steps from 52 workflows
98 passed, 0 failed
```

The two acceptance-manifest assertions are multi-line shell in the workflows;
they were executed as whole `run` blocks rather than line by line, and both
exit 0.

That is not a release gate and does not replace one. It is the answer to a
narrower question — whether any *other* non-pytest gate step is red on this
candidate — and the answer is no.

## Where it lives

The fix is commit `ba8a3b0` on `claude/pr435-promotion-candidate`, on top of
the R001 retry-order fix `f65d11fd`, which is itself a direct child of PR 435's
current head `f0434e4a`. Advancing PR 435 to it is a fast-forward.

Related: [pr435_self_hosted_validation_diagnosis.md](pr435_self_hosted_validation_diagnosis.md),
[pr435_linux_gate_failing_test.md](pr435_linux_gate_failing_test.md),
[federation_v1_interim_handoff.md](federation_v1_interim_handoff.md).
