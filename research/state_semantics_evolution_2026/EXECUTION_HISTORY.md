# Execution record, 2026-11-09

The workstation started on `main` at `f6fcdacd0f87f3f629daaa82f8ef0e79dd2506ba`.
It was not switched or edited. A separate clone was created at
`C:\Users\utlan\Documents\ChatGPT\msh-state-evolution-20261109` from the
existing research branch, initially `cc80a75727bf3ea0ee3288803d8dfda60392aa25`.
There were no applicable `AGENTS.md` files in the inspected checkout/parents.

## Access and isolated source transport

Existing Windows SSH configuration resolved `nitro` to user `martin`, port 22,
with the existing Ed25519 identity and batch/public-key authentication. A live
`ssh -o BatchMode=yes -o ConnectTimeout=12 nitro 'hostname; id; ...'` succeeded.
No password, private key or new credential was requested or retained.

Read-only inspection found the main recording archive and its verified
four-source compressed subset. Only the fixed subset was processed. Source code
was transported by `git bundle`/`scp` into the new remote research clone at
`/home/martin/dt-state-evolution-20261109`. Existing remote Git checkouts,
recorder, services and raw archives were not changed. No dataset was downloaded.

The original source bundle was cloned with:

```bash
git clone --single-branch --branch research/state-semantics-evolution-2026 \
  /home/martin/dt-state-evolution-20261109.bundle \
  /home/martin/dt-state-evolution-20261109
```

Subsequent source bundles were fetched and fast-forwarded only in that clone.
One incremental fetch initially failed because its prerequisite commit had not
yet finished importing; it succeeded after the initial source import completed.
No reset, force-push or modification of an existing checkout was performed.

## Validation and confirmed research corrections

Before research edits, the workstation executed:

```text
python -m pytest -o addopts= -q catalog/flask_app/tests/test_digital_twin_service.py research/state_semantics_evolution_2026/test_evolution.py research/state_semantics_evolution_2026/test_replay.py --basetemp=C:\Users\utlan\Documents\ChatGPT\msh-state-evolution-20261109-testtmp-initial
16 passed in 17.29s
```

The review corrected research expected-mask handling of invalid load strings
and explicit falsy execution values (`0`/`False`), and the new temporal
measurement's handling of the final state in a conflicting timestamp tie.
The legacy file-local sampler was retained as a probe; the actual study uses a
tested cross-file, time-disjoint archive selector. E6 now executes an alternate
callable rather than merely counting potentially affected rows. No production
classifier source or expected production test outcome was changed.

Focused tests exercised each correction before the complete suite. An initial
selector test invocation hit the existing Windows default pytest temp-directory
permission error. Separately, the first new validation-wrapper invocation had
28 fixture errors because it had not created its output parent directory before
passing a child `--basetemp`. The wrapper was fixed to create the parent first.
These were setup failures, not changed assertions. The final workstation run
passed 6 production and 25 research tests; exact arguments and captured output
are in `LOCAL_TESTS.json`.

## Remote dependency failures and recovery

The first Nitro venv used the default Python 3.14.4. Installing only constrained
numpy/pandas/pytest succeeded, and the original research tests passed, but the
six production tests failed during their autouse Flask fixture setup:
`ModuleNotFoundError: No module named 'flask'`. That invocation reported
**10 passed, 6 errors in 19.06s**, not a successful complete validation.

Installing the full release-pinned requirements under Python 3.14 failed
resolution of the pinned `psycopg-binary==3.2.9` binary dependency. No constraints
were loosened. Inspection located an existing user installation of Python
**3.12.13** at `/home/martin/.local/bin/python3.12`. A separate `.venv312` was
created, and the full pinned requirements installed successfully:

```bash
/home/martin/.local/bin/python3.12 -m venv .venv312
.venv312/bin/python -m pip install -c constraints-release.txt -r requirements.txt pytest
.venv312/bin/python -m pip check
```

`pip check` reported `No broken requirements found.` The complete resolved
environment is in `DEPENDENCIES.txt`. Research venv/result paths were excluded
only in this clone's `.git/info/exclude` to avoid expensive untracked scans.

## Committed historical execution

The exact executed source was `e6f199be9e5917f5ae5e66f94e17e2fcff6ea2ab`, with
`tracked_dirty=false`. Commands executed from the new remote research clone:

```bash
.venv312/bin/python -m research.state_semantics_evolution_2026.validate --output results/TEST_RESULTS.json
.venv312/bin/python -m pip freeze > results/DEPENDENCIES.txt
nice -n 10 .venv312/bin/python -m research.state_semantics_evolution_2026.execute \
  --archive /home/martin/msh-recordings-archive/verified-four-source-compressed-20261005-06-20261029T092350Z/recordings.tar.gz \
  --output-dir results
```

Validation returned zero: **6 production tests passed in 90.42s**, **25 research
tests passed in 6.96s**. Historical evaluation then wrote a manifest before
running transformations and completed all 24 windows with **zero unexpected
scenario outcomes**. Costs were 71.177s wall, 66.636s CPU and 335,364 KiB peak RSS.
`TEST_RESULTS.json` preserves subprocess exit codes and output. `MEASUREMENTS.json`
preserves every window/scenario, source hash, expected mask and difference trace.
The shell invocation also tailed the log; the measured outcome counts, not the
tail command's exit code, are the evidence of scenario acceptance.

Only the four named summary/environment files were copied from Nitro. A second
read-only selection run independently checks the archive and all window hashes;
its outcome is retained in `REPRODUCIBILITY.json`. No raw records were copied.

## Established-baseline sensitivity and CI

The complementary B0 policy probe ran on the workstation in an isolated Python
subprocess, temporarily replacing the classifier callable in memory. It did not
rewrite production files. The unchanged suite rejected the unknown-as-non-active
defect with **1 failed, 5 passed in 3.12s**; the failing assertion is the intended
detection, and there were no collection/setup/other errors. Three research-only
adapter paths remain outside B0's execution scope. `B0_SENSITIVITY.json` includes
the complete command and evidence; `baseline_sensitivity.py` reproduces it.

The source commit's self-hosted CI run is
[42628360908](https://github.com/Nettking/msh/actions/runs/42628360908), assigned
to **Beast-Linux-WSL**, runner 29, with Linux/X64/`fcp-linux-fast` labels. It
reported **31 passed in 5.04s**, completed successfully, and its post-job cleanup
steps succeeded. `CI_RESULTS.json` records the GitHub run/job metadata. The
earlier source revision also passed on AQG7NCC-Linux (runner 31). No runner was
restarted or relabeled, and no hosted runner was introduced.

Final delivery adds measured evidence, reporting helpers and interpretation to
the research branch. These later files do not change the executed experiment
functions; the source revision and per-file hashes remain explicit. No merge
into `main` is part of this task.
