# PR435 merge record and merged-main local screen

Coordination artifact. Records the executed PR435 merge, the exact resulting
merged-main identity, and a non-authoritative local screen of that identity.
This document does **not** establish merged-main software qualification.

## 1. Merge decision and execution

Pre-merge verification against live GitHub state, immediately before merging:

| Gate | Required | Observed | Verdict |
| --- | --- | --- | --- |
| PR435 head | `ba8a3b0b828f59c36c5aaaf6130480a2432a5578` | identical | PASS |
| Base branch | `main` at `6101c86d94294c70db47d1a8053cac93b9a41356` | identical | PASS |
| Unexpected commits | none beyond the 6 documented | 6 commits, all documented | PASS |
| Mergeability | no conflict | merge-base == `6101c86d`, fast-forwardable | PASS |
| Candidate-specific blocker | none unresolved | none | PASS |
| Branch-protection bypass | must not be required | none required (see below) | PASS |
| Review threads | none unresolved | 0 reviews, 0 review threads | PASS |

### Branch protection

`main` is **not** a protected branch. `GET /repos/Nettking/msh/branches/main`
returns `protected: false` with
`protection.required_status_checks.enforcement_level: "off"` and an empty
`contexts`/`checks` set.

The merge therefore required, and used, **no** administrative bypass, no force,
and no protection override. This also corrects an earlier statement in the PR
thread describing `product-branding` as a required check: no status check is
currently enforced on `main` by branch protection.

### Red hosted checks at merge time

All 18 hosted check runs on `ba8a3b0b` were `failure`, each completing 1-10s
after start. Log download returns HTTP 404 for these jobs, consistent with no
runner ever being assigned. This is CI001, the account-level Actions billing
block, and it is repository-wide rather than candidate-specific: the same
workflows failed identically in 3-7s on the merged commit `bcf5c9ab`, and
`main` last produced real runs on 2026-09-03. They are not product failures.

### Executed merge

Normal merge commit (no squash, no rebase), with exact expected-head-SHA
protection `expectedHeadSha=ba8a3b0b828f59c36c5aaaf6130480a2432a5578`.

## 2. Exact resulting merged-main identity

```
MERGED_MAIN_SHA=bcf5c9ab2fb453cb26129b70d41fb64fc4863dd4
```

This is a new identity and does **not** inherit the candidate's PASS.

| Property | Value |
| --- | --- |
| Merge commit | `bcf5c9ab2fb453cb26129b70d41fb64fc4863dd4` |
| Parent 1 (base) | `6101c86d94294c70db47d1a8053cac93b9a41356` |
| Parent 2 (candidate) | `ba8a3b0b828f59c36c5aaaf6130480a2432a5578` |
| Tree | `bcbd778ba8009eb79e3349534e623da3861b0e45` |

### Tree identity, proven

The merged tree is **byte-identical** to the qualified candidate's tree:

```sh
git rev-parse bcf5c9ab^{tree}   # bcbd778ba8009eb79e3349534e623da3861b0e45
git rev-parse ba8a3b0b^{tree}   # bcbd778ba8009eb79e3349534e623da3861b0e45
git diff ba8a3b0b bcf5c9ab      # empty
```

The merged identity differs from the qualified candidate **only in merge
topology**. Because of that, any behavioural difference observed on the
authoritative fast host is especially significant and must be investigated
before physical acceptance rather than attributed to the merge content.

Tree identity is **not** by itself merged-main qualification: the required
fast-host evidence is still recorded against the exact merged SHA.

## 3. Non-authoritative local screen

Run on the ephemeral cloud container of the merge session, **not** on
Nettking-Linux. Recorded for completeness and explicitly **not** offered as
qualification evidence.

### Environment (not release-equivalent)

| Property | This screen | Authoritative release environment |
| --- | --- | --- |
| Interpreter | CPython **3.12.11**, uv `python-build-standalone` | CPython **3.12.13**, `python:3.12.13-bookworm` |
| `os.pidfd_open` | **absent** | present |
| Host | 4 vCPU, 15.7 GiB, kernel 6.18.44-fc-v24 | Nettking-Linux, 20 logical CPUs, ~15 GiB |
| Account | container root | `gha` |
| Compose | unavailable (no Docker daemon) | available |

`pytest==9.1.1`, `ruff==0.16.3`, `pytest-randomly==4.1.0`, requirements plus
`constraints-release.txt`. Fresh clone from origin, exact-SHA asserted before
and after, clean worktree before and after.

### Results

Static and release checks on the exact merged SHA, all PASS: host-resource
preflight (29.10 GiB free, NORMAL), `compileall catalog`, acceptance manifest
shape, **product-branding**, diff hygiene (`git diff --check HEAD^ HEAD`), and
the release Ruff scope under the CI ignore policy (`All checks passed!`).

The product-branding pass is a positive confirmation that the R002 fix carried
in `ba8a3b0b` works, and that the red hosted `product-branding` check was
purely CI001.

Full default-order suite:

```sh
python -m pytest -o addopts= -o cache_dir=/tmp/pytest_cache_default \
  -p no:randomly -q --durations=25
```

**1 failed, 3632 passed, 30 skipped, 453 warnings in 253.69s.** JUnit records
3663 collected, the same collection size as the qualified candidate legs on
Nettking-Linux. Worktree clean and SHA still `bcf5c9ab` after the suite.

Every test that passed on Nettking-Linux for the candidate also passed here.
The only delta against the candidate's Nettking-Linux result is the single
failure below.

### The single failure, classified

```
catalog/federation/tests/test_tailnet_join_responder.py::
  test_a_matching_child_process_instance_can_be_terminated
```

- Collection position 1626 of 3663 (44.4%).
- Test duration **0.006s**. Completed 2026-09-06T07:10:41Z.
- Assertion: `assert responder.terminate_process_if_same_instance(child.pid, token)` returned `False`.
- The start token resolved correctly (`linux:<boot_id>:<starttime>`); only the termination call failed.

Root cause, established rather than inferred. On Linux,
`terminate_process_if_same_instance` delegates to
`_terminate_linux_process_if_same_instance`, whose first statement is
`os.pidfd_open(pid, 0)` inside `except (AttributeError, OSError): return False`.
The uv `python-build-standalone` CPython 3.12.11 used for this screen was built
without the pidfd syscalls, so `os.pidfd_open` does not exist as an attribute:

| Interpreter | `os.pidfd_open` | `signal.pidfd_send_signal` |
| --- | --- | --- |
| uv standalone 3.12.11 (this screen) | **False** | **False** |
| Debian `/usr/bin/python3.12` 3.12.3 | True | True |
| `/usr/local/bin/python3` 3.11.15 | True | True |

The product correctly fails closed: it refuses to signal a PID it cannot pin
against reuse. The test asserts the capability is present.

Direct control, same tree, same host, same installed site-packages, same test,
only the interpreter changed:

```sh
PYTHONPATH=<venv>/lib/python3.12/site-packages /usr/bin/python3.12 \
  -m pytest -o addopts= -q \
  catalog/federation/tests/test_tailnet_join_responder.py::test_a_matching_child_process_instance_can_be_terminated
# 1 passed in 0.07s
```

**Classification: environment/toolchain artifact of this screen's interpreter
build. Not a merged-main regression, and not a candidate regression.**

Supporting points:

- The file is not touched by PR435 (the PR changes 7 files; this is not one).
- It is **not** a known slow-host timing failure. Those are deadline expiries
  of 2s/3s/5s; this failed in 6ms on an `AttributeError` path. The host is also
  not slow: 253.69s here versus 288.01s on Nettking-Linux for the same suite.
- It passes on an interpreter that provides the syscall, on this same host.
- The authoritative environment `python:3.12.13-bookworm` provides
  `os.pidfd_open`, so this failure is not expected to reproduce there.

Per the governing instruction, this failure is **not** treated as a
merged-main regression without fast-host reproduction. If the authoritative
Nettking-Linux leg does reproduce it, that would be new evidence and must be
investigated before physical acceptance.

### Not run locally, by instruction

Shuffled seeds 20260813 and 15 were queued in this environment and were
**cancelled before completion**; seed 20260813 was terminated in progress and
seed 15 never started. No partial shuffled result is claimed. The Windows gate,
the PostgreSQL gate and Docker Compose validation were not executable in this
container.

## 4. Status

```
MERGED_MAIN_SOFTWARE_QUALIFICATION: NOT ESTABLISHED
```

Pending the authoritative fast-host legs in
`PR435_MERGED_MAIN_QUALIFICATION_RUNBOOK.md`. Physical acceptance remains
mandatory and unrun.
