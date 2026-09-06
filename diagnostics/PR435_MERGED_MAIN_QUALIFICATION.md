# Merged-main software qualification — bcf5c9ab

```
MERGED_MAIN_SOFTWARE_QUALIFICATION: PASS
MERGED_MAIN_SHA: bcf5c9ab2fb453cb26129b70d41fb64fc4863dd4
```

Every required gate passed on that exact identity in a single coherent run.
This is a software qualification only. It is not physical acceptance, not
deployment approval, and not a Federation v1 acceptance claim.

## Authoritative run

**Run [34029983129](https://github.com/Nettking/msh/actions/runs/34029983129) — conclusion `success`.**
Harness `.github/workflows/merged-main-qualification.yml` at
`f5bded772dd70893416c1345c9e450b96633c6fe` on
`claude/pr-435-final-merge-k01mfi`. Completed 2026-09-06T11:47Z.

| Leg | Job | Runner | Result | Job duration |
| --- | --- | --- | --- | --- |
| identity smoke | 101477615248 | Nettking-Linux (27) | SUCCESS | 6s |
| A full suite, default order | 101477632578 | Nettking-Linux (27) | **3633 passed, 30 skipped**, 455 warnings, pytest 456.84s | 544s |
| B full suite, shuffled seed 20260813 | 101478798113 | Nettking-Linux (27) | **3633 passed, 30 skipped**, 456 warnings, pytest 486.54s | 578s |
| C full suite, shuffled seed 15 | 101480050185 | Nettking-Linux (27) | **3633 passed, 30 skipped**, 455 warnings, pytest 249.91s | 335s |
| D release and static gates | 101480768775 | Nettking-Linux (27) | SUCCESS | 69s |
| E actual Docker Compose validation | 101480923587 | Nettking-Linux (27) | SUCCESS | 10s |
| Windows release matrix | 101477615064 | Nettking (22, `fcp-windows`) | SUCCESS, all 19 steps | 515s |
| PostgreSQL storage release check | 101477615224 | Nitro (21, `fcp-linux`) | SUCCESS, all 11 steps | 153s |
| Automated verdict | 101480948149 | Nettking-Linux (27) | SUCCESS | 4s |

All three full suites report the identical **3633 passed, 30 skipped** over the
same 3663-test collection, matching the qualified candidate `ba8a3b0b` exactly.
The 30 skips are the documented 23 Windows guards and 7 PostgreSQL service
guards, both covered by their own dedicated gates in this same run.

Every job checked out and asserted `bcf5c9ab`, and asserted a clean worktree
before and after. Legs A, B and C each printed `CHECKOUT_BEFORE_CLEAN` and
`CHECKOUT_AFTER_CLEAN` under the strict check, which includes untracked files:
**the suites wrote nothing into the tracked tree in any of the three orders.**

## Environment

`python:3.12.13-bookworm`, `pip==26.2.1`, `requirements.txt` with
`constraints-release.txt`, `pytest==9.1.1`, `pytest-randomly==4.1.0` for the
shuffled legs, `ruff==0.16.3`, `golang:1.25.7` for the sidecar. Windows used the
runner's Python 3.12.10 toolchain. Runner account `gha`, fresh clone per job.

## Leg D — release and static gates

All gates green: host-resource precondition, `compileall catalog`, acceptance
manifest shape, the release Ruff scope under the CI ignore policy,
**product-branding**, the Go direct peer sidecar tests under `golang:1.25.7`,
and diff hygiene.

## Leg E — actual Docker Compose validation

Not a YAML parse. Nettking-Linux has no Compose plugin, so the gate's own check
ran inside the pinned official Docker CLI image, source read-only, no network
and no Docker socket:

```
host has no Compose plugin; using the pinned official Docker CLI image
sha256:f6f3bf33f3d4c8a86745323554dad9fcfa84d16884c8fce55dee2f13be54d99b
  [docker@sha256:eccaacfeed644c7de222ff047483568cb988dde95476fbaaf10ea2d04921bb66]
Docker Compose version v5.5.1
COMPOSE_CONFIG_PASS pinned-cli-image
CHECKOUT_AFTER_CLEAN
```

Same image ID and same Compose version Astra used for the candidate's
supplemental Compose evidence.

## Host memory evidence

Read-only `free(1)` sampling every 10s inside each Linux suite leg, reported
through an EXIT trap so it reports on failure as well as success.

| Leg | Lowest available during the suite | Host after the suite |
| --- | --- | --- |
| A | **11,108 MiB** | 15 GiB total, 3.7 GiB used, 11 GiB available |
| B | **11,126 MiB** | — |
| C | **11,086 MiB** | — |

Compare the killed leg A of run 34019865738: **837 MiB available**, container
SIGKILLed at exit 137. The margin is now better than thirteenfold, and no leg
came close to pressure.

## Host isolation — intentional and operator-performed

The earlier OOM root cause was confirmed operationally: roughly 14 Arrowhead
Java services held about 9-10+ GiB RSS on Nettking-Linux with swap exhausted.
**The Arrowhead stack was deliberately stopped for host isolation before this
qualification**, swap was reset, and the runner service was restarted. Measured
before the run: ~11 GiB available, swap 0 B.

This is environment preparation, not a product modification. No product code,
test, timeout, retry policy, skip policy, flaky policy, coverage setting or
storage-admission threshold was changed at any point. **Arrowhead remains
stopped and must not be restarted until the operator decides to.**

## Superseded runs, recorded rather than hidden

- **34019865738 — FAILURE, ENV.** Leg A SIGKILLed (exit 137) at 93% with 837 MiB
  available, no test having failed, followed by loss of the runner service.
  Classified ENV: host memory exhaustion. The operational finding above confirms
  that classification; it is not reclassified as a product failure. Windows and
  PostgreSQL passed in that run on the merged SHA.
- **34025305598 — FAILURE, harness defect.** Windows, PostgreSQL and legs A, B
  and C all passed (leg A: 3633 passed, 30 skipped in 273.84s; sampler minimum
  11,020 MiB). Leg D passed every actual gate and then failed on the harness's
  own post-check, which asserted a fully clean worktree after `go mod tidy`.
  `cmd/fcp-peer-sidecar/go.sum` is not tracked, not ignored, and has never been
  tracked on any branch, while `go.mod` requires ~90 external modules, so the
  gate's own `go mod tidy` must generate it. The authoritative release job never
  observes this because it asserts cleanliness *before* its Go step. Fixed by
  correcting the check, not the gate: that one generated path is named
  explicitly, any other untracked path still fails, tracked-file modification
  fails separately, and offending paths are now printed.

No test was skipped, disabled, quarantined, re-deadlined or retried to reach
this verdict, and no failure was masked.

## Standing observation, not acted on

With no committed `go.sum`, the Go sidecar gate resolves those ~90 modules with
no checksum pinning. This is pre-existing on `main` and on `ba8a3b0b`, is not a
property of the merged identity, and is outside this qualification's remit. It
is recorded for the executive owner to decide on separately.

## Status

```
MERGED_MAIN_SOFTWARE_QUALIFICATION: PASS   bcf5c9ab2fb453cb26129b70d41fb64fc4863dd4
PHYSICAL_ACTION_STARTED: NO
```

Beast-Linux remains OFFLINE and was deliberately not repaired; it is optional
confirmation only and its Federation role is unchanged at AI_PROVIDER_ONLY.

Physical acceptance on Nettking, Nitro and the MSH Recorder remains mandatory
and unrun. All three hosts are still deployed at `6101c86d`, and rollback to
`6101c86d` is unchanged. Federation v1 is not accepted.
