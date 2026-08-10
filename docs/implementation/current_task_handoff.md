# Current task handoff

Status: **current repository handoff**

Reviewed: **2026-08-11 Europe/Oslo**

## Repository state

- Repository: `Nettking/msh` (product name: Federated Capability Platform / FCP)
- Default branch: `main`
- Always resolve the current `main` head directly before starting work.
- Published Federation v1 release tag: not created.
- Capability-first Federation implementation: merged baseline.
- Role-first installed-product runtime retirement (CF8): merged.
- Verified manual Federation-wide software updates: merged.
- Standalone recorder Federation bootstrap/publication and Federation-wide recorder control: merged.
- Complete physical CF7 acceptance: not accepted.
- Complete Federation v1 end-to-end acceptance: false.
- OSL integration: separate planning track; production implementation status is governed by the OSL track documents.

Historical phase notes, branch handoffs, old commit hashes, and pre-CF8 sequencing do not override this current handoff. Acceptance flags are different: only the named acceptance source and a separate evidence-backed review may change them.

## Track A: capability-first Federation

Durable product/authority plan:

- [Capability-first Federation plan](federation/active/capability_first_federation_plan.md)

Current operational documentation:

- [Federation operations](../federation_operations.md)
- [Standalone recorder](../standalone_recorder.md)
- [Current architecture](../architecture.md)

Detailed runtime-update design:

- [Manual Federation-wide FCP updates](federation/active/manual_updates.md)

Acceptance workspace:

- [Federation acceptance documentation](federation/acceptance/)

Machine-readable acceptance truth:

```text
catalog/federation/tests/cf7_acceptance/scenarios.json
```

### Current merged Federation/product baseline

The installed product now includes:

- stable persistent device identity;
- authenticated Federation discovery, verified join, signed pairing, reconnect, revocation, and local creation;
- the required first-run path `Identity -> Federation -> Inspect -> finish setup`;
- bounded device inspection and optional benchmark evidence;
- independent contribution recommendation/intent/enable/disable/suspend/reconcile behavior;
- capability-first runtime/configuration authority with the former role-first product runtime retired;
- public-safe Federation overview/detail surfaces plus explicit reviewed mutation surfaces;
- coordinator-owned **Check for updates** and **Update all devices** with exact-commit host validation and running-runtime proof;
- Windows/POSIX host-owned update agents and conservative Windows legacy migration bootstrap;
- a headless MTConnect recorder that can join a Federation using the normal `FCP1-...` pairing flow;
- recorder-local startup network discovery with first-configuration auto-selection;
- local-first checkpoint-gated recorder publication through Federation logical-storage authority;
- `/federation/recorders` control from any trusted Federation device for bounded recorder-local scans and add/remove source selection;
- storage, transport, AI/provider, compute/job/artifact, recovery, fencing, and authority boundaries;
- permanent Ubuntu and Windows component/product/release gates.

The pairing-code UX currently issues signed one-use codes valid for up to 10 minutes and permits a fresh code to be generated when another attempt is required.

A successful software activation remains internally `runtime_verified`; the UI presents that terminal success as **Updated**.

### Important current limitation

A standalone recorder launched directly with `python start_recorder.py` is a headless Federation node but does not host the normal Flask update-event processor/host update agent. The current **Update all devices** activation path manages normal FCP installations and their Compose-managed recorder service; it does not restart an independently launched standalone recorder process.

Do not document or claim automatic standalone-recorder self-update until a dedicated bounded updater exists and is accepted.

### Federation work still open

1. Reconcile physical-acceptance instructions with the current post-CF8, update-capable, recorder-capable product baseline.
2. Resolve any verified runtime-parity, native-host, privacy, browser, restart, multi-host, recorder-control, or update-rollout defects found on the exact candidate.
3. Freeze one exact candidate only after known blockers are closed.
4. Execute the complete physical CF7 campaign on that same commit.
5. Update acceptance flags only through a separate evidence-backed review.
6. Create a Federation v1 release tag only after the release acceptance contract is satisfied.
7. Treat any future standalone-recorder update mechanism as a separate bounded authority/security delivery rather than extending peer control implicitly.

Do **not** restart CF1-CF6 implementation waves and do **not** reintroduce role-first runtime authority to solve migration or startup defects.

## Track B: OSL integration

Plan index:

- [OSL integration index](osl_integration/)

Authoritative execution plan:

- [OSL implementation roadmap](osl_integration/10_phased_implementation_roadmap.md)

W3 end-to-end acceptance scenario:

- [Notebook-to-OSL method alignment](../planned-work/method-osl-fcp-alignment.md)

Federation work and OSL work remain separate review boundaries. Before beginning any OSL implementation delivery, re-read the current OSL index/roadmap rather than relying on older Federation handoff text.

## Cross-track boundaries

- Do not combine Federation physical acceptance/runtime fixes with OSL production implementation.
- OSL review, approval, or publication grants no Federation, provider, compute, storage, job, artifact, lease, fencing, update, recorder-control, or machine authority.
- Federation device identity is not a human OSL reviewer, approver, or publisher identity.
- AI cannot sign, approve, publish, or create canonical human authority merely because it is available as an FCP capability.
- Existing operator records and legacy SysML exports are compatibility inputs, not proof of OSL conformance.

## Agent operating discipline

1. Start from updated `main`.
2. Scope each branch and PR to one named delivery, defect, acceptance unit, or documentation unit.
3. Declare owned paths before editing shared Flask, setup, navigation, security, persistence, update, recorder-control, or workflow files.
4. Commit after coherent boundaries so partial work remains recoverable.
5. Open a draft PR unless the repository owner explicitly requests another state.
6. Distinguish automated, simulated, browser, physical, multi-host, service, and human evidence.
7. Preserve authority, privacy, migration, restart, and cross-platform gates.
8. Stop when a missing decision would require scope expansion or a permissive assumption.

## Resume safety

- Safe to continue Federation closeout and CF7 preparation: **yes**.
- Safe to mark physical CF7 accepted from merged code/green CI alone: **no**.
- Safe to treat CF8 as future/blocking work: **no; CF8 is already merged for the installed product**.
- Safe to reintroduce role-first authority for convenience: **no**.
- Safe to document Federation-wide updates as automatic/background updates: **no; activation remains explicit/manual**.
- Safe to claim standalone `start_recorder.py` processes are updated by **Update all devices**: **no**.
- Safe to begin an OSL delivery: only after checking the current OSL track documents and respecting its named prerequisite/review boundaries.
