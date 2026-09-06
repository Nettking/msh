# PR435 fast-runner evidence — Astra, 2026-09-06

Product candidate: `ba8a3b0b828f59c36c5aaaf6130480a2432a5578` (frozen).
Main control: `6101c86d94294c70db47d1a8053cac93b9a41356`.
Executive owner: Astra. Decision: no merge, no deployment.

## Scoped verdict

- FAST_LINUX_VALIDATION: **PASS**, Nettking-Linux.
- FAST_WINDOWS_VALIDATION: **PASS**, reused exact-candidate Nettking Windows gate. Beast-specific confirmation incomplete.
- NITRO_SLOW_HOST_STRESS: **FAIL**, completed failures on candidate and main; precise cause inconclusive.

These labels describe the requested fast validation. They do not approve physical acceptance, shuffled suite-order independence, or merging PR435. No product change was made or justified by this verification.

## Completed GitHub evidence

| Run / job | Runner | Result |
| --- | --- | --- |
| [33994338144 / 101382063264](https://github.com/Nettking/msh/actions/runs/33994338144/job/101382063264) | Nettking-Linux | Targeted: 20 repetitions, 8 tests each, zero failures |
| [33994962383 / 101383799720](https://github.com/Nettking/msh/actions/runs/33994962383/job/101383799720) | Nettking-Linux | Targeted: another 20 repetitions, zero failures |
| [33994338144 / 101382542624](https://github.com/Nettking/msh/actions/runs/33994338144/job/101382542624) | Nettking-Linux | 3633 passed, 30 skipped, 452 warnings, 288.01s; SHA, storage, final clean check PASS |
| [33994962383 / 101384040044](https://github.com/Nettking/msh/actions/runs/33994962383/job/101384040044) | Nettking-Linux | 3633 passed, 30 skipped, 454 warnings, 286.27s; SHA, storage, final clean check PASS |
| [33994962383 / 101383799758](https://github.com/Nettking/msh/actions/runs/33994962383/job/101383799758) | Nettking-Linux | Compile, manifest, existing release Ruff scope, branding, Go, diff and storage preflight PASS; Compose YAML fallback only |
| [33975032244 / 101330241662](https://github.com/Nettking/msh/actions/runs/33975032244/job/101330241662) | Nettking Windows | 871 passed/1 skipped in 220.12s; 367 passed/1 skipped in 84.13s; Go, Ruff, Compose, diff, storage PASS |

Linux validation wrapper SHA: `e81bf4d59da813bc9f647209113efa8da9951aba`, separate from the product checkout. Authoritative exact scripts: [fast-linux-validation.yml at wrapper SHA](https://github.com/Nettking/msh/blob/e81bf4d59da813bc9f647209113efa8da9951aba/.github/workflows/fast-linux-validation.yml).

Every cited candidate job checks out and asserts ba8a3b0b. Primary Linux jobs use `python:3.12.13-bookworm`, `pip==26.2.1`, `-r requirements.txt -c constraints-release.txt`, and `pytest==9.1.1`. Each Docker test container is removed after its job; bytecode and pytest cache are in container `/tmp`. Host runner gha uid1001, Nettking WSL Ubuntu, 20 CPUs, ~15GiB RAM, Docker server29.1.3. Windows uses the pre-existing standalone CPython3.12.10 under the Nettking runner service identity.

Targeted selection (8 tests covering the five previous failures plus adjacent cases), repeated 20 times per job with failure return codes counted:

```sh
python -m pytest -o addopts= -o cache_dir=/tmp/pytest_cache -p no:randomly -q \
  catalog/mtconnect_recorder/tests/test_source_availability_retry.py \
  catalog/relay/tests/test_phase2_integration.py::test_f2_006_revocation_rejects_traffic_and_reconnect_but_peer_survives \
  catalog/flask_app/tests/test_federation_pairing_relay.py \
  catalog/node/tests/test_live_storage_catchup.py
```

Full suite, unchanged default order:

```sh
python -m pytest -o addopts= -o cache_dir=/tmp/pytest_cache -p no:randomly -v --durations=50
```

Both primary full suites passed in succession on the same host. No fast candidate test failed, so the instruction to compare any fast failure against main was not triggered. These runs do not exercise the original shuffled seeds. They do not reset the shared Docker daemon or all host state. Windows confirmation is the recorded release subsets, not a claim of a new full Windows suite.

The first overall run was cancelled after these primary jobs completed. The second overall run is still queued because optional Beast-Linux smoke job101383784569 has no assigned runner. Job evidence must be distinguished from whole-workflow status.

## Actual Compose check, same Linux service account

The static CI step's PyYAML fallback is not equivalent to Compose validation. Astra completed the real command locally on the same Nettking-Linux machine and account, against the Actions checkout at the exact candidate. No source write, Docker socket mount, persistent plugin installation, network access inside the checking container or FCP service operation was needed.

Artifact: [official Docker CLI image](https://hub.docker.com/_/docker), pinned after pull to:

```text
docker@sha256:eccaacfeed644c7de222ff047483568cb988dde95476fbaaf10ea2d04921bb66
Image ID: sha256:f6f3bf33f3d4c8a86745323554dad9fcfa84d16884c8fce55dee2f13be54d99b
Docker Compose version v5.5.1
```

Exact completed script, run from Windows with `wsl.exe -d Ubuntu -u gha -- bash <task-local-script>`:

```bash
set -euo pipefail
cd /home/gha/actions-runner/_work/msh/msh
date -u +%FT%TZ
hostname
id
test "$(git rev-parse HEAD)" = ba8a3b0b828f59c36c5aaaf6130480a2432a5578
git rev-parse HEAD
test -z "$(git status --porcelain)"
echo CHECKOUT_BEFORE_CLEAN
cli_image=docker@sha256:eccaacfeed644c7de222ff047483568cb988dde95476fbaaf10ea2d04921bb66
docker image inspect "$cli_image" --format '{{.Id}} {{json .RepoDigests}}'
docker run --rm --network none "$cli_image" docker compose version
docker run --rm --network none \
  --mount type=bind,source=/home/gha/actions-runner/_work/msh/msh,target=/workspace,readonly \
  -w /workspace "$cli_image" docker compose config --quiet
echo COMPOSE_CONFIG_PASS
test "$(git rev-parse HEAD)" = ba8a3b0b828f59c36c5aaaf6130480a2432a5578
test -z "$(git status --porcelain)"
echo CHECKOUT_AFTER_CLEAN
date -u +%FT%TZ
```

Observed output, exit0:

```text
2026-09-06T06:24:12Z
Nettking
uid=1001(gha) gid=1001(gha) groups=1001(gha),123(docker)
ba8a3b0b828f59c36c5aaaf6130480a2432a5578
CHECKOUT_BEFORE_CLEAN
sha256:f6f3bf33f3d4c8a86745323554dad9fcfa84d16884c8fce55dee2f13be54d99b ["docker@sha256:eccaacfeed644c7de222ff047483568cb988dde95476fbaaf10ea2d04921bb66"]
Docker Compose version v5.5.1
COMPOSE_CONFIG_PASS
CHECKOUT_AFTER_CLEAN
2026-09-06T06:24:14Z
```

An initial image-metadata formatting attempt failed before invoking Compose because Docker returned a different template type for RepoDigests. The diagnostic script changed to JSON formatting, then completed as shown. No product/workflow change resulted. Check containers were automatically removed; official CLI image remains cached. This supplements CI evidence without relabeling the earlier YAML fallback.

## Failures and controls

Beast-Windows [33994714769 / 101383142124](https://github.com/Nettking/msh/actions/runs/33994714769/job/101383142124): exact candidate checkout succeeded, then setup-python's setup.ps1 was blocked by PowerShell execution policy. Later diagnostic steps found no `python` on PATH. **Environment/setup failure before product tests.** No product main A/B can classify a test that never started. No host-wide policy change was made.

Fresh GitHub API snapshot 2026-09-06T06:25Z: Beast-Linux offline/idle, Beast-Windows online/idle, Nettking-Linux online/idle, Nettking online/idle, Nitro online/idle. Existing Beast-Linux smoke remains queued; no additional job was dispatched or runner altered. Independent Beast confirmation is incomplete and does not delay the requested primary conclusion.

Nitro completed controlled A/B, wrapper `a0f9e756d0e70bfcd5e8c3fe50500067361d63c4`, [run33988447252](https://github.com/Nettking/msh/actions/runs/33988447252):

| Pair / position | Tree | Pytest result | Phase rc |
| --- | --- | --- | --- |
| [AB job101374058563](https://github.com/Nettking/msh/actions/runs/33988447252/job/101374058563), first | ba8a3b0b | 3633 passed,30 skipped in3983.94s | 0 |
| AB, second | 6101c86d | 1 failed,3614 passed,30 skipped in3862.35s | 1 |
| [BA job101391451308](https://github.com/Nettking/msh/actions/runs/33988447252/job/101391451308), first | 6101c86d | 3615 passed,30 skipped in3750.64s | 0 |
| BA, second | ba8a3b0b | 1 failed,3632 passed,30 skipped in3633.43s | 1 |

Both failures: `catalog/mtconnect_recorder/tests/test_source_availability_retry.py::test_first_real_data_is_durably_written_with_its_raw_manifest`, `recorder capture did not finish within the test deadline` (2s helper). Last phase rc1 logged2026-09-06T01:15:56Z. Wrapper jobs are green because they record rc and continue; the test results are not all green.

Classification: reproduced on unmodified main; failure correlates with second position in these two pairs. Slow-host/full-suite state sensitivity is supported, exact mechanism and any indirect candidate effect are not settled. Sustained disk busy time alone is not sufficient causation: isolated repeats also passed at high busy time. The original four shuffled-suite timeouts require their own full ordered reproduction. No timeout increases, test skips, flaky markings or reduced coverage were used.

## Preserved state and continuation

At verification: `C:\wsl\msh` clean at6101c86d; independent `work/msh-owner` clean atba8a3b0b; Actions Linux checkout clean atba8a3b0b before/after Compose. PR API: OPEN, merged=false, head exactba8a3b0b. Product candidate, normal checkout branches, containers used by FCP, services and Federation state were not modified by this verification. Only coordination documentation, task-local diagnostics and an ephemeral tooling container were written/run.

Remaining release work: original shuffled-order qualification, complete clean-start/shared-state evidence, then a separately authorized physical acceptance campaign on exact candidate. No physical acceptance has been run on ba8a3b0b. No merge/deployment is authorized. Astra remains executive owner; reuse valid evidence rather than repeating completed work. Fetch the coordination branch before further writes.
