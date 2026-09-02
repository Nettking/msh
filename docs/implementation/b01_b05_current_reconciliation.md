# B01 / B05 current-main reconciliation

Base: `87f670fa7aa6a23da3ae63990886c5e4ff522265`

Status: **B01 and B05 software properties reconciled as automated-proven; exact-candidate physical evidence remains required**.

This reconciliation audits the current merged implementation instead of copying the older blocker score text forward. It does not change CF7 acceptance flags and does not convert automated evidence into physical acceptance.

## B01 — multi-filesystem host resource contract and finite active transactions

The authoritative robustness input names eight B01 software properties. On the current base they are all represented by merged production code and consequence/regression evidence.

| Required property | Current evidence | Status |
| --- | --- | --- |
| Observe the actual checkout/build/Docker, data, results and model/provider backing resources where distinct | Shared `host_resources` measurement plus the Docker-backing-resource resolution used by controlled build/model paths; #383 reconciled the supported writer boundaries and #395 removed the remaining private-controller bypass for host builds/model pulls. | **PROVEN** |
| Observe inode/file exhaustion where exposed | Shared filesystem measurement and process admission account for free inodes and refuse transactions that would consume the configured emergency inode reserve. | **PROVEN** |
| Treat stale/unavailable measurements conservatively | Shared pressure assessment classifies unavailable/stale/invalid measurements conservatively rather than as healthy. | **PROVEN** |
| Shared `NORMAL` / `WARNING` / `PRESSURE` / `CRITICAL` semantics | `host_resources.py` owns the common pressure vocabulary used by admitted large writers. | **PROVEN** |
| Finite recorder ingress/transaction bounds | B02 reached 9/9 software properties, including response/deadline/observation/sequence limits and bounded admitted recorder transaction behavior. | **PROVEN** |
| Aggregate concurrent recorder work | Recorder resource reservations use the process-wide controller rather than independent per-source free-space guesses, so concurrent bounded transactions account against one shared resource ledger. | **PROVEN** |
| Preserve completion headroom for atomic replacement/checkpoint/status/WAL/journal work | #383 extended bounded byte/inode admission through the real durable mutation boundaries, including SQLite/WAL/journal headroom and exception-safe release; recorder pressure handling preserves raw-first/checkpoint-last semantics. | **PROVEN** |
| Apply admission/refusal to builds, model pulls, uploads, analysis materialization and other supported large optional writers | #352/#383/#395 plus the model, upload, analysis, logical-storage, cache/export and managed-workspace integrations cover the supported production boundaries. #383's final scorecard reported **0 MISSING** writer boundaries, and #395 subsequently fixed the remaining process-wide coordination bypass for build/model defaults. | **PROVEN** |

### Why the older B01 scorecard still contained `PARTIAL`

The #383 scorecard deliberately used `PARTIAL` for several stores whose **active writes were already bounded/admitted** but whose lifetime durable history remained cumulative. That is not a second missing B01 admission boundary. The remaining questions are retention/archive/replay semantics and physical aggregate-growth evidence, which are owned by B07/B09 and the physical campaign.

In particular, B01 does **not** authorize automatic deletion of recorder evidence, user uploads, user/research results, idempotency rows, tombstones, or authority history merely to make a table smaller. Those stores must be retired only behind the semantics established in their owning blocker family.

### B01 remaining release evidence

No further B01 software implementation gap was found in this reconciliation. The final exact candidate still needs the relevant physical resource campaign, especially P01, P02, P04, P08 and P10 where those paths are present in the tested topology.

**B01 software score: 8/8 automated-proven.**

## B05 — failure-safe activation and optional-model isolation

The authoritative robustness input names five B05 software properties. Current `main` retains the merged implementations and permanent tests for all five.

| Required property | Current evidence | Status |
| --- | --- | --- |
| Failed target activation leaves a previous usable core runtime or a bounded explicit recovery state | #382 is merged. POSIX and Windows runners correlate recovery to the exact apply request/target, make at most one previous-Flask restoration attempt, require runtime health for `activation_recovered`, and otherwise publish `activation_required` / `activation_restore_failed` for deterministic same-approved-apply retry. `test_b05_failure_safe_activation_contract.py` pins both platforms, bounded recovery and no source rollback. | **PROVEN** |
| Do not stop healthy Flask merely to enter an unpreflighted normal build path | The merged launcher/update contract moved normal builds onto the shared preflight/host-mutation lifecycle before activation boundaries; B04 is separately closed and the B05 recovery tests pin the stop/replacement boundary. | **PROVEN** |
| Model installation uses shared host-resource admission on update, startup, browser and provider/profile paths | #342 is merged and explicitly covers normal Windows/POSIX startup, host update agents, browser handoff, scripted setup, language-model provider profile and headless startup. #395 later made build/model production defaults share the same process-wide admission ledger as bounded writers. | **PROVEN** |
| Model absence/download failure/Ollama unavailability cannot remove unrelated core availability | #341 is merged. Current Windows/POSIX/headless startup paths explicitly represent AI as unavailable/resource-paused while keeping core FCP healthy/running. | **PROVEN** |
| No guessed model-size constant is required; unknown-size pulls enforce the floor dynamically | #342 resolves the actual Docker/model backing resource, starts only above `PRESSURE`, remeasures while the pull is active, and positively stops the optional writer when the floor is reached/unprovable. No guessed model byte size is used as the admission decision. | **PROVEN** |

### B05 remaining release evidence

No later merge in the audited current base removed the activation recovery states, model admission path coverage or optional-model startup behavior. The remaining work is physical exact-candidate evidence: supported entry points and update/activation behavior (P03/P05) plus model installation and constrained-storage behavior across every product path (P10, with P02 resource-pressure evidence where applicable).

**B05 software score: 5/5 automated-proven.**

## Release interpretation

B01 and B05 should no longer remain `OPEN` because of stale software-status prose once this reconciliation is independently reviewed against the final integrated candidate. They remain subject to the exact-candidate physical campaign; automated closure is not physical acceptance.
