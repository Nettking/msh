# CF7-B product and physical acceptance

Status: automated product acceptance and the physical evidence contract are merged on `main`. **Physical acceptance has not been claimed.**

## Purpose

CF7-B is the independent acceptance layer after CFI-6. It adds no production authority and does not redesign onboarding, contribution, storage, provider, membership, or protocol contracts.

It separates two kinds of evidence:

1. **Automated product evidence** — deterministic scenarios that CI executes on clean Ubuntu and Windows runners using independent state roots and bounded local adapter fixtures.
2. **Physical product evidence** — observations requiring real Windows/Linux hosts, independent network paths, MTConnect, Ollama/model files, target accelerators, and real desktop/mobile browsers.

A green CI workflow proves only the first category and the correctness of the physical evidence tooling. Full capability-first and Federation v1 end-to-end acceptance remain false until the required physical evidence validates against the release candidate.

## Automated acceptance

`catalog/federation/tests/cf7_acceptance/test_product_acceptance.py` composes the supported Flask application and the real CFI-2 through CFI-6 services.

The automated scenarios cover fresh state, stable identity, local Federation creation, inspection, benchmark execution, contribution lifecycle, restart/reconciliation, benchmark invalidation/rerun, three independent device roots, separate AI/compute/storage paths, and membership invariants.

These tests use bounded deterministic adapter fixtures. They do not prove real MTConnect reachability, Ollama performance, GPU behavior, multi-host transport, storage durability on target hardware, or browser rendering.

## Physical evidence v2

The canonical template is:

```text
catalog/federation/tests/cf7_acceptance/physical_evidence.template.json
```

The validator is:

```text
catalog/federation/tests/cf7_acceptance/physical_evidence.py
```

The impact map and revalidation planner are:

```text
catalog/federation/tests/cf7_acceptance/physical_impact_map.json
catalog/federation/tests/cf7_acceptance/physical_revalidation.py
```

Schema `msh.cf7.physical-evidence.v2` replaces the original all-or-nothing single-observation model. The final document still targets one exact candidate commit in `commit_sha`, but each environment and each physical scenario also records:

- `provenance: observed` or `provenance: carried-forward`;
- `observed_commit`: the exact commit where the physical observation was actually made;
- the existing redacted evidence references and operator notes.

### Observed evidence

`observed` means the physical action was performed on the candidate being accepted. Its `observed_commit` must equal the document `commit_sha`.

### Carried-forward evidence

`carried-forward` is allowed only when all of the following are proven automatically:

1. the old `observed_commit` is an ancestor of the candidate;
2. Git can compute the complete changed-path set between the two commits;
3. every changed path is classified by the checked-in impact map;
4. no changed path maps to the physical scenario being carried forward;
5. no unknown product path exists;
6. the candidate's permanent automated acceptance and regression workflows are green before final acceptance review.

The validator fails closed. A non-ancestor baseline, missing commit, unreadable Git history, unknown product path, or impacted scenario produces a rerun requirement rather than silently trusting old evidence.

This means a small Federation projection fix does **not** automatically invalidate a previously observed physical Ollama/GPU benchmark, while a Docker/runtime-composition change deliberately invalidates every physical scenario.

## Impact-map rules

`physical_impact_map.json` is auditable release-policy data, not runtime authority.

The policy has three classes:

- `no_physical_impact`: documentation, tests, CI workflow definitions, and CF7 acceptance tooling. These changes still require CI, but do not change the product behavior that was physically observed.
- `global_invalidation`: runtime composition/bootstrap/dependency changes where narrow impact cannot be established safely. These require all physical scenarios to rerun.
- scenario-specific rules: known production code paths mapped to the physical observations they can affect.

Any production path not covered by one of these classes is **unknown** and therefore invalidates the full physical scenario set until the map is deliberately reviewed.

To inspect a candidate before doing physical work:

```bash
python -m catalog.federation.tests.cf7_acceptance.physical_revalidation \
  --baseline <observed-commit> \
  --candidate <candidate-commit> \
  --repo-root .
```

The command prints machine-readable JSON containing:

- changed paths;
- scenarios requiring rerun;
- scenarios safe to carry forward;
- unknown paths;
- a `safe` decision.

Exit code `2` means the plan cannot authorize carry-forward safely.

## Required physical scenarios

| Scenario | Required observation |
| --- | --- |
| Fresh Windows checkout | Clone the exact observed commit into a new directory, confirm clean state, run the documented gates, and start the product without pre-existing state. |
| Fresh Linux checkout | Repeat the same procedure on a physical Linux host. |
| Multi-host relay and network path | Use independent hosts and the approved deployment network; prove authenticated discovery or configured reachability without treating network presence as trust. |
| MTConnect recorder source | Select a real or approved MTConnect source, enable recording explicitly, observe fresh records and status, then disable safely. |
| Ollama model and accelerator | Use the target model files and accelerator, run the explicit benchmark, and observe bounded completion and safe failure behavior. |
| Mobile and desktop browser review | Complete onboarding and inspect Federation, status, contribution, degraded, and repair states in real desktop and mobile browsers. |
| Recorder plus AI on one device | Enable both contributions simultaneously and prove that neither activation grants storage or compute authority. |
| Separate AI, compute, and storage devices | Join independent physical devices; expose only the registered compute handler; keep storage candidate-only until existing assignment authority acts. |
| Benchmark expiry and rerun | Observe real expiry or dependency invalidation, confirm contribution suspension where required, rerun explicitly, and recover safely. |
| Contribution disable and re-enable | Fence future use of one contribution without deleting membership or unrelated contributions, then re-enable it. |
| Restart and reconciliation | Restart the process or machine and prove stable identity, trusted reconnect, persisted intent, and safe contribution reconciliation. |
| Revocation and controlled rejoin | Revoke one member, prove route fencing and failed reconnect, then complete a verified controlled rejoin without granting another device authority. |

A carried-forward record satisfies the same required observation; it does not weaken the observation. It only proves that the product code capable of changing that observation has not changed since it was made.

## Preparing the evidence directory

Do not place raw secrets or unrestricted logs in the repository.

Use a local working directory:

```text
evidence/
  windows/commands.txt
  linux/commands.txt
  scenarios/
  cf7-physical-evidence.json
```

Copy the template to `evidence/cf7-physical-evidence.json` and replace placeholders only after an observation passes or the impact analyzer authorizes carry-forward.

Evidence references must be relative paths beginning with `evidence/`. Remove credentials, raw service URLs, IP addresses, private hostnames, local absolute paths, invitation/enrollment material, and database locations before validation or review.

## Fresh checkout commands

When a fresh-checkout environment requires a rerun, use the candidate commit on that physical host.

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

`git status --short` must be empty before the physical observation.

## Revalidation workflow for a new release candidate

1. Freeze the candidate commit.
2. Run the permanent CI/regression gates on that candidate.
3. For every previously passed physical record, run the impact planner from its `observed_commit` to the candidate.
4. Mark unaffected records `carried-forward`; retain their original `observed_commit` and evidence references.
5. Rerun only impacted, missing, or previously failed physical records on the candidate and mark them `observed`.
6. Perform privacy review.
7. Validate the completed v2 document from the candidate checkout.
8. Change acceptance flags only in a separate evidence-backed review.

A release fix therefore creates a bounded revalidation set instead of automatically resetting all physical work.

## Validating completed evidence

### Linux

```bash
commit="$(git rev-parse HEAD)"
python -m catalog.federation.tests.cf7_acceptance.physical_evidence \
  evidence/cf7-physical-evidence.json \
  --commit "$commit" \
  --repo-root .
```

### Windows PowerShell

```powershell
$commit = (git rev-parse HEAD).Trim()
python -m catalog.federation.tests.cf7_acceptance.physical_evidence `
  evidence\cf7-physical-evidence.json `
  --commit $commit `
  --repo-root .
```

Successful validation prints compact JSON with `"accepted":true`, the observed-record count, and the carried-forward-record count. Any incomplete, unsafe, malformed, impacted, unknown, non-ancestor, or commit-mismatched document exits with status `2`.

## Acceptance decision

CF7 may be declared complete only when all conditions are true for the candidate:

- the permanent CF7-A and CF7-B workflows are green on Ubuntu and Windows;
- the broad Federation v1 regression matrices are green;
- every required physical scenario is either observed on the candidate or validly carried forward under the fail-closed impact policy;
- both required physical OS environments are either observed on the candidate or validly carried forward;
- the v2 physical evidence document validates successfully from the candidate checkout;
- the operator and reviewer confirm that evidence is redacted and corresponds to the recorded provenance;
- no unresolved authority, privacy, data-loss, platform, or browser defect remains.

The acceptance flags in `scenarios.json` may change only in a separate evidence-backed review. Until then:

```json
{
  "federation_v1_end_to_end_accepted": false,
  "capability_first_onboarding_end_to_end_accepted": false,
  "physical_evidence_accepted": false
}
```

CF8 must not start merely because automated workflows are green.
