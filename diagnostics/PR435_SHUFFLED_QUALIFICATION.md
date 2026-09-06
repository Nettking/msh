# Frozen PR435 candidate: shuffled software qualification

Executive owner and executor: Astra. Completed 2026-09-06T06:50:38Z.

SHUFFLED_ORDER_VALIDATION: **PASS**

SOFTWARE_QUALIFICATION: **PASS for candidate ba8a3b0b828f59c36c5aaaf6130480a2432a5578**, on the documented Linux/Windows release environments. This combines these two full shuffled runs with the already-verified default-order, targeted, release/static, Windows and PostgreSQL evidence. It is not a claim of physical acceptance or successful Nitro stress, and it does not authorize deployment or silently turn the historical failed workflow green.

## Observed results

Runner host Nettking-Linux, WSL Ubuntu on Nettking, actual service account `gha` uid/gid1001, 20 logical CPUs, Docker29.1.3. These are direct local executions under that account, not new GitHub Actions jobs. No default-order suite was rerun, and no further Nitro characterization was performed.

| Seed | Exact SHA | Full pytest result | Pytest duration | Leg wall time including setup | Post-run SHA / checkout |
| --- | --- | --- | --- | --- | --- |
| 20260813 | ba8a3b0b828f59c36c5aaaf6130480a2432a5578 | 3633 passed,30 skipped,459 warnings | 408.81s | 502s | Exact / CLEAN |
| 15 | ba8a3b0b828f59c36c5aaaf6130480a2432a5578 | 3633 passed,30 skipped,455 warnings | 379.01s | 463s | Exact / CLEAN |

Controller started06:34:27Z. First leg finished06:42:52Z; second finished06:50:38Z. Setup, pytest and final cleanliness return codes are all0 for both legs. JUnit independently records3663 tests,0 failures,0 errors,30 skips for each. The complete test-ID multiset is identical; the execution-order hashes differ, confirming that two different orders actually ran. No candidate failure occurred, so the conditional same-host main control was not triggered. No claim of a shuffled main PASS is made.

The 30 skips in each run are existing platform/service guards:23 Windows-only cases and7 PostgreSQL cases without a service on port5432. No skip, retry, flaky marker, timeout increase or reduced selection was introduced. Separate exact-candidate Windows job101330241662 and PostgreSQL job101330241653 were already green. Test warnings are preserved in raw logs, not ignored in reporting.

## Reproduction and environment

Exact controller is [run-pr435-shuffled.sh](run-pr435-shuffled.sh). Script hash at execution: `cbec7e0248e231e6eb26b1d868fc5cc117f5558e823d998fc61fefca67736e7e`.

Each leg starts with a new `git clone --no-checkout` from a local verified Git bundle, then `git checkout --detach <exact SHA>`, exact-SHA assertion and empty `git status --porcelain`. Bundle advertised HEAD is candidate ba8a3b0b and origin/main is6101c86d94294c70db47d1a8053cac93b9a41356. Bundle SHA256: `c7e94265a0f6a9e4f4df729e6c5930d3c362c8e11e6b281fb8b3225dec2164fc`. No existing Actions or development checkout is reused for the leg.

Fresh `python:3.12.13-bookworm` container for each leg:

```text
Image ID: sha256:3eb66c6a8a2399cdf305afe7f680ea1e3aad64e9a6684cd6e46baf6b8b701ec8
Repo digest: python@sha256:3cd9086bdb30f7c9bc08a3fa621d9842e0d3f6f9291aeb4677e0547817c10b12
Python 3.12.13
pip 26.2.1
pytest 9.1.1
pytest-randomly 4.1.0
```

Release requirements and `constraints-release.txt` installed unchanged; complete `pip freeze` files are byte-identical across legs. The randomization plugin is the same pinned version used by the recorded qualification workflow. Bytecode and pytest cache are inside the fresh container under `/tmp`; test source is mounted at `/workspace`. Host network and Docker socket match the successful fast full-suite environment. Host resource/storage preflight passes per leg. No global environment, runner service, storage policy or FCP runtime was reconfigured.

Exact full-suite command, once for each seed:

```bash
python -m pytest -o addopts= -o cache_dir=/tmp/pytest_cache -v --durations=25 \
  -p randomly --randomly-seed="$SHUFFLE_SEED" --junitxml=/evidence/pytest.xml
```

This reproduces the recorded shuffled command; JUnit output is the added evidence destination. It does not change selection or assertions. Final source ownership is returned to gha only within the newly created leg checkout, then SHA and porcelain cleanliness are checked. Test containers are removed automatically; both diagnostic checkouts and all logs are preserved. The shared host/daemon was not reset; this is clean source and fresh test-container evidence, not proof of erasing all host state.

## Host state

Before, after and every15 seconds: timestamp, load, RAM/swap, disk free bytes, CPU/memory/I/O pressure, device disk counters, relevant process state and running container inventory. Raw measurements are archived.

| Measure | Seed20260813 | Seed15 |
| --- | --- | --- |
| Samples | 33 | 31 |
| One-minute load min / mean / max | 4.72 / 5.68 / 8.06 | 4.71 / 6.10 / 8.07 |
| MemAvailable min / mean / max, bytes | 509415424 / 1358775513 / 2225836032 | 835964928 / 1434898036 / 2138664960 |
| Minimum free bytes on checkout filesystem | 949637210112 | 949553221632 |
| Swap used | approximately4GiB throughout | approximately4GiB throughout |

Host RAM total16614338560 bytes. Swap was already full at initial observation. The suites passed under this measured state; no causal diagnosis or claim of an idle/uncontended host follows from that. No new Nitro inference is added.

## Durable evidence

- [Raw logs, JUnit, exact installed dependencies and host measurements](PR435_SHUFFLE_RAW_EVIDENCE.tar.gz):372367 bytes, SHA256 `1f388b4394a78a9ace194c89186f68e6f8479f1bc373e6abcbbe57e00d00a90f`.
- [Per-file hash manifest](PR435_SHUFFLE_MANIFEST.json).
- [Machine-readable results, skip reasons, collection/order hashes and resource summaries](PR435_SHUFFLE_SUMMARY.json).
- Original retained data: `/home/gha/qualification/pr435-shuffle-20260906-astra` on Nettking Ubuntu. Controller PID1532722 completed; no further leg remains active.
- Prior software evidence: [PR435_FAST_RUNNER_VALIDATION.md](PR435_FAST_RUNNER_VALIDATION.md).

Source checks after execution: normal `C:\wsl\msh` remains clean at main6101c86d; independent owner checkout remains clean at candidateba8a3b0b. No product code, product workflow, product branch head, deployment or Federation state change was made. Only coordination evidence and new task-owned diagnostic paths were written. The original CI failure and Nitro stress result remain historical FAIL.

## Executive transition decision

1. **Software-qualified candidate — reached.** Exact ba8a3b0b has passed the requested fast default and shuffled qualification, release/static checks, Windows scope and separate PostgreSQL gate. Keep this identity frozen. Evidence does not imply stability for every seed or host.
2. **PR435 final merge decision — next.** Astra retains ownership. Recheck the exact PR head/base, final adversarial diff and protected-check state against the published evidence. Decide explicitly whether to merge; do not bypass protections, infer deployment approval, or silently dismiss known Nitro sensitivity. No merge action is authorized or performed by this software-only completion. The software gate itself no longer blocks that review; physical release acceptance remains open.
3. **Exact merged-main SHA qualification — required after an authorized merge.** Record the actual resulting main SHA and compare its tree to ba8a3b0b. Any additional code/configuration requires a new frozen qualification identity. Bind fresh Linux release/full-suite, the two shuffled seeds, release/static, Windows and PostgreSQL evidence to the actual merged SHA; prior candidate evidence is supporting evidence, not an automatic PASS for a new SHA. Verify clean checkouts and build/provenance labels for that SHA. This is the next identity's qualification, not reopening the completed candidate runs now.
4. **Controlled physical Federation acceptance — mandatory after that software qualification and explicit controlled-start authorization.** Use clean checkouts/builds of the qualified merged SHA on the three real machines. Capture fresh initial runtime/Federation state and rollback targets; activate Nitro recorder only within the campaign. Prove distinct multi-recorder identities and independent targeting, a negative control that detects the original collision, reconnect/restart behavior, expected versus observed results, and deterministic cleanup/rollback/final state. Complete the existing physical evidence requirements, including P07/P12 duration requirements. CI, loopback tests and schema-only evidence cannot substitute. No deployment or live physical startup is performed now.

MERGE_PERFORMED: NO. DEPLOYMENT_PERFORMED: NO. PHYSICAL_ACCEPTANCE: NOT RUN on this candidate. Federation v1 end-to-end/release acceptance remains unclaimed.
