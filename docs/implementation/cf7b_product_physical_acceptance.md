# CF7-B product and physical acceptance

Status: automated product acceptance and the physical evidence contract are implemented on the CF7-B branch. **Physical acceptance has not been performed or claimed.**

## Purpose

CF7-B is the independent acceptance wave after CFI-6. It adds no production authority and does not redesign onboarding, contribution, storage, provider, membership, or protocol contracts.

It separates two different kinds of evidence:

1. **Automated product evidence** — deterministic scenarios that CI can execute on clean Ubuntu and Windows runners using independent state roots and bounded local adapter fixtures.
2. **Physical product evidence** — observations that require real Windows/Linux hosts, independent network paths, MTConnect, Ollama/model files, target accelerators, and real desktop/mobile browsers.

A green CI workflow proves only the first category and the correctness of the physical evidence validator. Full capability-first and Federation v1 end-to-end acceptance remain false until the physical document validates against the exact commit being accepted.

## Automated acceptance

`catalog/federation/tests/cf7_acceptance/test_product_acceptance.py` composes the supported Flask application and the real CFI-2 through CFI-6 services.

The automated scenarios cover:

- a fresh state root with no candidate federation;
- stable identity creation;
- safe local federation creation;
- device inspection;
- explicit benchmark execution;
- simultaneous recorder and language-model contribution;
- contribution disable and re-enable without membership change;
- capability-first finish and persisted `msh.onboarding.v1` state;
- safe Federation overview rendering without private recorder configuration;
- process-level restart, identity reopen, trusted reconnect, and contribution reconciliation;
- benchmark and inspection expiry followed by a new inspection, benchmark rerun, and contribution recovery;
- three independent device state roots joined to one existing federation;
- separate language-model, registered-compute, and storage-candidate paths;
- storage remaining pending until the existing assignment authority reports an assignment;
- no contribution action changing federation membership revision.

These tests use bounded deterministic adapter fixtures. They do not prove real MTConnect reachability, Ollama performance, GPU behavior, multi-host transport, storage durability on target hardware, or browser rendering.

## Physical evidence contract

The canonical template is:

```text
catalog/federation/tests/cf7_acceptance/physical_evidence.template.json
```

The validator and command-line entry point are:

```text
catalog/federation/tests/cf7_acceptance/physical_evidence.py
```

The template is deliberately incomplete. It must fail full validation until every required environment, scenario, evidence reference, and privacy confirmation is complete.

The evidence document is bound to:

- schema `msh.cf7.physical-evidence.v1`;
- one exact lowercase 40-character Git commit SHA;
- one UTC execution time;
- named Windows and Linux environments;
- the complete required physical scenario set;
- relative evidence references below a local `evidence/` directory;
- affirmative privacy and redaction review.

The validator rejects:

- pending, skipped, failed, or missing scenarios;
- missing Windows or Linux fresh-checkout evidence;
- a commit mismatch;
- malformed, oversized, or non-UTF-8 JSON;
- unexpected fields or scenario IDs;
- absolute paths or parent traversal;
- raw URLs, IP addresses, credentials, invitation/enrollment material, private keys, setup-file names, and private database locations.

## Required physical scenarios

| Scenario | Required observation |
| --- | --- |
| Fresh Windows checkout | Clone the exact commit into a new directory, confirm clean state, install dependencies, run the documented gates, and start the product without pre-existing state. |
| Fresh Linux checkout | Repeat the same procedure on a physical Linux host. |
| Multi-host relay and network path | Use independent hosts and the approved deployment network; prove authenticated discovery or configured reachability without treating network presence as trust. |
| MTConnect recorder source | Select a real or approved MTConnect source, enable recording explicitly, observe fresh records and status, then disable safely. |
| Ollama model and accelerator | Use the target model files and accelerator, run the explicit benchmark, observe bounded completion and safe failure behavior. |
| Mobile and desktop browser review | Complete onboarding and inspect Federation, status, contribution, degraded, and repair states in real desktop and mobile browsers. |
| Recorder plus AI on one device | Enable both contributions simultaneously and prove that neither activation grants storage or compute authority. |
| Separate AI, compute, and storage devices | Join independent physical devices; expose only the registered compute handler; keep storage candidate-only until existing assignment authority acts. |
| Benchmark expiry and rerun | Observe real expiry or dependency invalidation, confirm contribution suspension where required, rerun explicitly, and recover safely. |
| Contribution disable and re-enable | Fence future use of one contribution without deleting membership or unrelated contributions, then re-enable it. |
| Restart and reconciliation | Restart the process or machine and prove stable identity, trusted reconnect, persisted intent, and safe contribution reconciliation. |
| Revocation and controlled rejoin | Revoke one member, prove route fencing and failed reconnect, then complete a verified controlled rejoin without granting another device authority. |

## Preparing the evidence directory

Do not place raw secrets or unrestricted logs in the repository.

Create a local working directory:

```text
evidence/
  windows/commands.txt
  linux/commands.txt
  scenarios/
  cf7-physical-evidence.json
```

Copy the template to `evidence/cf7-physical-evidence.json`. Replace the placeholder commit, time, operator, statuses, notes, and evidence references only after the corresponding observation has passed.

Evidence references must be relative paths beginning with `evidence/`. The referenced files should contain redacted summaries, screenshots, or command output. Remove all credentials, raw service URLs, IP addresses, private hostnames, local absolute paths, invitation/enrollment material, and database locations before validation or review.

## Fresh checkout commands

Use the same exact commit on both physical hosts.

### Linux

```bash
git clone <repository> msh-cf7-acceptance
cd msh-cf7-acceptance
git checkout <exact-commit-sha>
git status --short
python -m pip install --upgrade pip
python -m pip install -r requirements.txt -c constraints-phase2.txt
python -m pip install pytest ruff
python -m pytest -o addopts= -q catalog/federation/tests/cf7_acceptance
python -m pytest -o addopts= -q catalog/flask_app/tests/test_capability_onboarding_route.py catalog/flask_app/tests/test_capability_inspection_route.py catalog/flask_app/tests/test_capability_benchmark_route.py catalog/flask_app/tests/test_capability_contribution_route.py catalog/flask_app/tests/test_capability_startup_transition_route.py catalog/flask_app/tests/test_federation_overview_route.py
docker compose config
```

### Windows PowerShell

```powershell
git clone <repository> msh-cf7-acceptance
Set-Location msh-cf7-acceptance
git checkout <exact-commit-sha>
git status --short
python -m pip install --upgrade pip
python -m pip install -r requirements.txt -c constraints-phase2.txt
python -m pip install pytest ruff
python -m pytest -o addopts= -q catalog/federation/tests/cf7_acceptance
python -m pytest -o addopts= -q catalog/flask_app/tests/test_capability_onboarding_route.py catalog/flask_app/tests/test_capability_inspection_route.py catalog/flask_app/tests/test_capability_benchmark_route.py catalog/flask_app/tests/test_capability_contribution_route.py catalog/flask_app/tests/test_capability_startup_transition_route.py catalog/flask_app/tests/test_federation_overview_route.py
docker compose config
```

`git status --short` must be empty before the physical run. Record the exact commands and redacted results in the environment command-log files.

## Validating completed evidence

### Linux

```bash
commit="$(git rev-parse HEAD)"
python -m catalog.federation.tests.cf7_acceptance.physical_evidence \
  evidence/cf7-physical-evidence.json \
  --commit "$commit"
```

### Windows PowerShell

```powershell
$commit = (git rev-parse HEAD).Trim()
python -m catalog.federation.tests.cf7_acceptance.physical_evidence `
  evidence\cf7-physical-evidence.json `
  --commit $commit
```

Successful validation prints a compact JSON summary with `"accepted":true` and exits with status 0. Any incomplete, unsafe, malformed, or commit-mismatched document exits with status 2.

## Acceptance decision

CF7 may be declared complete only when all of the following are true for the same exact commit:

- the permanent CF7-A and CF7-B workflows are green on Ubuntu and Windows;
- the broad Federation v1 regression matrices are green;
- every physical scenario has passed on real equipment;
- the physical evidence document validates successfully;
- the operator and reviewer confirm that evidence is redacted and corresponds to the named commit;
- no unresolved authority, privacy, data-loss, or cross-platform defect remains.

The acceptance flags in `scenarios.json` must be updated only in a separate, evidence-backed review after those conditions are met. Until then:

```json
{
  "federation_v1_end_to_end_accepted": false,
  "capability_first_onboarding_end_to_end_accepted": false,
  "physical_evidence_accepted": false
}
```

CF8 role-first retirement must not start merely because the automated workflow is green. It starts only after the physical evidence-backed CF7 decision is reviewed and accepted.
