# Self-hosted runner migration inventory

This inventory records the workflow audit at baseline commit
`6aa7e33fc31bc890a4b128f6d322f2f73a9b8d34`. It describes configuration, not a
claim that the complete Federation v1 qualification has passed on the new pools.

The baseline contained 51 workflows and 62 execution-job declarations. Of those,
44 job declarations in 36 workflows could select GitHub-hosted machines.
There were no reusable workflow calls, `workflow_call` entry points, macOS
routes, `pull_request_target` entry points, or hosted fallback expressions.
Every workflow and its matrix were included in the audit.

## Hosted routes replaced

In the table, `OS matrix` means `matrix.os: [ubuntu-latest, windows-latest]`
with `runs-on: ${{ matrix.os }}`. `Mixed matrix` means Linux used
`ubuntu-24.04` while Windows already selected `self-hosted, fcp-windows`.

| Workflow | Affected jobs | Baseline routing |
| --- | --- | --- |
| `b01-federated-jsonl-admission.yml` | `admission` | OS matrix |
| `b01-recorder-resource-admission.yml` | `recorder-resource-admission` | OS matrix |
| `b06-service-supervision.yml` | `service-supervision` | OS matrix |
| `b07-bounded-growth.yml` | `bounded-growth` | OS matrix |
| `capability-config.yml` | `capability-config` | OS matrix |
| `capability-product-readers.yml` | `product-readers` | OS matrix |
| `cf1-onboarding-contracts.yml` | `contracts` | OS matrix |
| `cf2-benchmark-adapters.yml` | `adapters` | OS matrix |
| `cf3-onboarding-discovery.yml` | `discovery` | OS matrix |
| `cf4-contribution-service.yml` | `contribution-service` | OS matrix |
| `cf5-ui-shell.yml` | `ui-shell` | OS matrix |
| `cf6-federation-projections.yml` | `projections` | OS matrix |
| `cf7b-product-physical-acceptance.yml` | `acceptance` | Mixed matrix |
| `cfi1-federation-overview.yml` | `federation-overview` | OS matrix |
| `cfi6-compatibility-startup-transition.yml` | `transition` | OS matrix |
| `command-bootstrap.yml` | `command-bootstrap` | OS matrix |
| `federation-efficiency-learning.yml` | `efficiency-learning`; `repository-hygiene` | OS matrix; `ubuntu-latest` |
| `federation-v1-physical-campaign.yml` | `campaign-tooling` | `matrix.runner`: `ubuntu-24.04`, `windows-2025` |
| `federation-v1-release.yml` | `release-checks` | `matrix.runner`: `ubuntu-24.04`, `windows-2025` |
| `federation-v1-release.yml` | `linux-regressions`; `postgres-storage`; `suite-order-runs`; `release-matrix`; `suite-order-independence`; `release-verdict` | `ubuntu-24.04` |
| `federation-v1-release.yml` | `windows-regressions` | `windows-2025` |
| `phase-f641-chunk-contract.yml` | `contract` | OS matrix |
| `phase-f642-verified-chunk-transfer.yml` | `verified-transfer` | OS matrix |
| `phase-f65-resumable-chunk-transfer.yml` | `durable-resume` | OS matrix |
| `phase-f661-circuit-relay-foundation.yml` | `circuit-relay-foundation` | OS matrix |
| `phase-f662-session-route-rendezvous.yml` | `session-route-rendezvous` | OS matrix |
| `phase-f7-closeout.yml` | `capability-closeout` | Mixed matrix |
| `phase-f8-closeout.yml` | `trusted-provider-closeout` | Mixed matrix |
| `phase-f81-provider-enrollment.yml` | `provider-enrollment` | OS matrix |
| `phase-f82-provider-health.yml` | `provider-health` | OS matrix |
| `phase-f83-remote-ai-binding.yml` | `remote-ai` | OS matrix |
| `phase-f85-operator-federation-surface.yml` | `operator-surface` | Mixed matrix |
| `phase-f86-reconnect-reconciliation.yml` | `reconciliation` | OS matrix |
| `phase2-federation.yml` | `linux` | `ubuntu-24.04` |
| `recorder-federation-publication.yml` | `recorder-publication` | OS matrix |
| `release-image-metadata.yml` | `verify-release-images` | `ubuntu-24.04` |
| `remote-storage-contribution.yml` | `remote-storage-contribution` | OS matrix |
| `windows-first-federation-start.yml` | `bootstrap` | `windows-latest` |

## Previously self-hosted routes audited

These workflows already required self-hosted runners. Their declarations were
also reviewed and normalized to the shared pools where necessary:

- `cf7-acceptance-harness.yml`: `harness`.
- `cf7c-physical-test-readiness.yml`: `readiness`.
- `cf8-role-retirement.yml`: `role-retirement`.
- `cfi2-onboarding-composition.yml`: `onboarding`.
- `cfi3-device-inspection-composition.yml`: `inspection`.
- `cfi4-benchmark-composition.yml`: `benchmarks`.
- `cfi5-contribution-composition.yml`: `contributions`.
- `ci-test-sharding.yml`: `shard-contract`; JSON runner arrays selected by
  dispatch input included machine-specific labels.
- `docs-portal.yml`: `docs-portal`.
- `federation-software-update.yml`: `update-contracts`.
- `icse-tool-demo.yml`: `reviewer-entrypoint`, `reviewer-compose`,
  `publication-bundle`.
- `nitro-archive-capacity-readonly.yml`: `capacity`; a machine-specific
  Windows label.
- `nitro-artifact-smoke.yml`: `transport`; conditional machine-family labels.
- `phase-f6-closeout.yml`: `transport-closeout`.
- `phase2-federation.yml`: `windows`.
- `product-branding.yml`: `product-branding`.

The routing contract is now exactly one of these pools, including when selected
through a matrix:

```yaml
runs-on: [self-hosted, Linux, X64, fcp-linux-fast]
runs-on: [self-hosted, Windows, X64, fcp-windows]
```

No individual machine is required. If no matching runner is online, the job
queues. The migration changes no runner registration, label, group, installation
or service and does not require AQG7NCC to be online.

## Persistent-runner security and isolation

Pull-request workflow definitions are editable by the pull-request author. A
job-level repository or fork check in that same editable definition is not an
independent security boundary. The checked-in workflows use pushes to branches
in this repository and existing authorized manual dispatch entry points; they
have no `pull_request` or `pull_request_target` execution entry point. Ordinary
fork pushes cannot select this repository's registered self-hosted runners. The
repository guard also skips copied, unchanged workflow definitions in other
repositories.

**An independent GitHub-side restriction is still required for the public
repository.** A fork author can change a workflow to add a pull-request trigger
and remove the policy check before GitHub evaluates the proposed workflow.
Removing triggers in this checkout does not prevent that attack. Before this
migration can be considered secure for untrusted fork contributions, repository
administrators must require approval for every outside collaborator workflow
run, or enforce an equivalent restriction outside the editable workflow. A
first-time-contributor approval policy does not cover previously approved
contributors. This migration changes no repository settings or runner groups;
that prerequisite remains a deployment blocker until independently verified.
See GitHub's [workflow event-ref behavior](https://docs.github.com/en/actions/concepts/workflows-and-actions/workflows),
[self-hosted runner security guidance](https://docs.github.com/en/actions/reference/security/secure-use#hardening-for-self-hosted-runners),
and [fork approval controls](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/approve-runs-from-forks).

Path filters are preserved, and branch pushes cover trusted repository branches
so same-repository pull requests can receive checks for their head commit. The
checked-in workflow definitions do not automatically execute fork pull requests.
A maintainer must review the external change before bringing it onto a trusted
repository branch, and the independent restriction above must prevent modified
fork workflow definitions from bypassing that review. Checks
on branch pushes qualify that exact head SHA; they do not claim qualification
of GitHub's synthetic pull-request merge commit. Release qualification continues
to bind evidence to the exact checkout and event SHA.

Hosted SDK deletion is removed while the existing resource-admission preflight
is retained. Python environments and test temporary directories are created per
job and cleaned by the action's post step. PostgreSQL uses the existing
disposable self-hosted CI action rather than starting or stopping a host service.
Physical campaign tooling still tests the tooling contract; physical acceptance
requirements and registered acceptance-host procedures remain unchanged.

Static routing validation and workflow-configuration tests run on the Linux
self-hosted pool. Execution results and any capacity blockers must be reported
separately using completed job identities; this inventory provides no runtime
qualification claim.
