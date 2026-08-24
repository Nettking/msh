# Current task handoff

Status: **current repository handoff**

Reviewed: **2026-08-24 Europe/Oslo**

## Repository state

- Repository: the current FCP source repository.
- Default branch: `main`
- Always resolve the current `main` head directly before starting work.
- Published Federation v1 release tag: not created.
- Capability-first Federation implementation: merged baseline.
- Role-first installed-product runtime retirement (CF8): merged.
- Verified manual Federation-wide software updates: merged.
- Standalone recorder Federation bootstrap/publication and Federation-wide recorder control: merged.
- Disk-allocation/update-cache hardening from PR #325: merged; physical disk-exhaustion closeout remains open until the real-host robustness campaign passes.
- Independent adversarial robustness review: reconciled; implementation must follow the reconciled blocker set.
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

Active v1 robustness closeout:

- [Reconciled robustness review](v1_robustness_reconciliation.md) — **authoritative implementation input**;
- [Federation v1 robustness gate](v1_robustness_gate.md) — initial systematic gate/evidence vocabulary;
- [Disk accounting audit](disk_accounting_audit.md) — disk-specific forensic input.

The independent review has been reconciled. New robustness implementation must start from `v1_robustness_reconciliation.md`, which overrides classifications in the initial gate where they differ. The documents do not change Federation authority or acceptance flags by themselves; their corrected physical campaign supplies additional evidence alongside the existing CF7 contract.

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
- a supervised native standalone recorder that participates in the same **Update all devices** rollout over its existing Federation connection;
- a headless MTConnect recorder that can join a Federation using the normal `FCP1-...` pairing flow;
- recorder-local startup network discovery with first-configuration auto-selection;
- local-first checkpoint-gated recorder publication through Federation logical-storage authority;
- `/federation/recorders` control from any trusted Federation device for bounded recorder-local scans and add/remove source selection;
- bounded logical-storage allocation/free-space floors and update-path Docker build-cache/preflight protection from PR #325;
- storage, transport, AI/provider, compute/job/artifact, recovery, fencing, and authority boundaries;
- permanent Ubuntu and Windows component/product/release gates.

The pairing-code UX currently issues signed one-use codes valid for up to 10 minutes and permits a fresh code to be generated when another join attempt is required.

A successful software activation remains internally `runtime_verified`; the UI presents that terminal success as **Updated**.

### Standalone recorder update coverage

A native standalone recorder started through its supervisor (`start-tailscale-recorder.cmd`, which runs `scripts/windows/fcp_recorder_supervisor.ps1`) now participates in **Check for updates -> Update all devices** like any other Federation member. The host update agent runs inside the recorder process and starts only when the supervisor has set `FCP_RECORDER_SUPERVISOR_SESSION`; the supervisor performs only the two steps that cannot happen inside the recorder, namely the fast-forward after the process exits and the single relaunch. Success still requires a different process ID and a different process-instance nonce under the same supervisor, running the exact target commit, with a fresh heartbeat and connected Federation membership. See [Standalone recorder](../standalone_recorder.md).

Two limits remain, and neither may be softened in documentation:

- A recorder launched directly with `python start_recorder.py` has no supervisor session, so it starts no host update agent. It is still a headless Federation node and still receives the update event, but its bounded handoff is answered by nothing and the coordinator records `host_update_agent_unavailable` for that device. It is reported as an error, never as a silent success, and never as an updated device.
- The supervisor exists for Windows only. There is no POSIX native-recorder supervisor, so a native recorder on Linux or macOS is in the unsupervised case above.

A recorder whose current checkout predates this capability needs one manual fast-forward and one supervised start before it can be updated by the Federation flow, for the same reason normal FCP devices do.

### Federation work still open

1. Implement the reconciled robustness blockers B01-B10 from [the authoritative reconciliation](v1_robustness_reconciliation.md), one named delivery/PR at a time and in the recommended dependency order.
2. Keep the documented non-goals and accepted boundaries out of the v1 implementation unless new concrete evidence invalidates them.
3. Execute the corrected P01-P12 physical fault/growth/restore campaign on one exact candidate after the blocker implementations and automated gates are green; green CI alone is insufficient.
4. Reconcile physical-acceptance instructions with the current post-CF8, update-capable, recorder-capable product baseline.
5. Resolve any verified runtime-parity, native-host, privacy, browser, restart, multi-host, recorder-control, update-rollout, resource-exhaustion or recovery defects found on the exact candidate.
6. Freeze one exact candidate only after known blockers are closed.
7. Execute the complete physical CF7 campaign on that same commit.
8. Update acceptance flags only through a separate evidence-backed review.
9. Create a Federation v1 release tag only after the release acceptance contract is satisfied.
10. Decide whether a POSIX native-recorder supervisor is in scope; until one exists, native recorders on Linux and macOS stay outside **Update all devices** by construction rather than by policy.

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
6. Distinguish automated, simulated, browser, physical, multi-host, service, resource-pressure and human evidence.
7. Preserve authority, privacy, migration, restart, recovery and cross-platform gates.
8. Stop when a missing decision would require scope expansion or a permissive assumption.

## Resume safety

- Safe to continue Federation robustness implementation: **yes, only from the reconciled blocker set and one named delivery at a time**.
- Safe to treat the initial robustness gate as authoritative where the reconciliation disagrees: **no**.
- Safe to mark physical CF7 accepted from merged code/green CI alone: **no**.
- Safe to treat CF8 as future/blocking work: **no; CF8 is already merged for the installed product**.
- Safe to reintroduce role-first authority for convenience: **no**.
- Safe to document Federation-wide updates as automatic/background updates: **no; activation remains explicit/manual**.
- Safe to claim *supervised* native standalone recorders are updated by **Update all devices**: **yes**.
- Safe to claim an unsupervised `python start_recorder.py` process is updated by **Update all devices**: **no; it reports `host_update_agent_unavailable`**.
- Safe to begin an OSL delivery: only after checking the current OSL track documents and respecting its named prerequisite/review boundaries.
