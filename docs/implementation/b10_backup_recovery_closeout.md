# B10 backup/recovery closeout

Base: `87f670fa7aa6a23da3ae63990886c5e4ff522265`

Status: **software implementation candidate; physical P11 evidence still required**.

## Problem on base

The administrator backup procedure did not meet the B10 release contract:

- it created the backup under the Git checkout by default;
- it did not prove the destination was on an independent backing resource;
- it stopped production before proving destination capacity;
- it invoked an `alpine` helper after quiescence, so a missing image could introduce a post-stop network/download dependency;
- it restarted Compose from a `finally` block even after copy failure; and
- it did not integrity-check the copied SQLite state before calling the backup complete.

## Implemented boundary

`catalog.federation.backup_recovery` implements the ordinary Compose-managed v1 topology.

The backup:

1. requires a clean exact source checkout;
2. resolves effective Compose data/results sources and proves the relay service declares the retained relay mount;
3. resolves the **actual Docker volume name mounted by the live relay container**, rather than trusting the Compose logical volume key;
4. proves Docker's actual persistent backing resource;
5. requires a new destination on a different backing filesystem from checkout, data, results and Docker state;
6. sizes the local state and actual retained relay volume and proves byte/inode capacity before quiescence;
7. acquires the shared host-mutation lease, excluding supported update/build/activation;
8. refuses a detected live native recorder in this selected topology;
9. safely stops a matching tailnet responder only after PID plus OS process-creation identity match;
10. stops Compose and proves Flask, relay and managed recorder are stopped;
11. remeasures the payload and destination capacity after quiescence;
12. copies data/results/.env plus retained relay state without following link-like paths;
13. uses only the already-present relay image with `--pull=never --network=none` for relay-volume sizing and `docker cp` for the stopped relay-container copy;
14. counts both files and directories against destination inode demand;
15. leaves an explicit incomplete marker until copied SQLite state has passed `PRAGMA quick_check`;
16. publishes a bounded completion manifest and exact source commit only after those checks pass;
17. verifies that `source-commit.txt` agrees with the completion manifest; and
18. deliberately leaves FCP stopped so neither success nor failure implicitly restarts writers.

The administrator guide requires explicit verification and normal supported `--resume` startup after the operator has inspected the result and host capacity.

## Adversarial correction on the candidate

The first implementation candidate contained a concrete preflight defect that ordinary unit tests did not expose. `docker compose config` reports the service mount by its Compose source key (`relay_state`), while the repository gives that volume an explicit Docker name (`fcp_relay_state` by default, or `FCP_RELAY_VOLUME_NAME`). Passing the logical key to `docker run --mount` can create or inspect a different empty volume. That would undercount the backup payload even though the later `docker cp` copied the real relay container's state.

The corrected implementation obtains the retained volume name from the relay container's runtime `.Mounts` record and requires exactly one volume mounted at `/var/lib/fcp-relay`. Consequence tests explicitly model the `relay_state` → `fcp_relay_state` distinction and prove relay sizing uses the runtime volume name.

The same adversarial pass tightened two adjacent evidence properties: directory inode demand is included in preflight accounting, and verification rejects disagreement between `source-commit.txt` and `backup-manifest.json`.

## Scope

This does not claim online/hot backup. It does not attempt to clone Windows DPAPI identity to another user/machine. It does not automatically quiesce a live direct native-recorder topology; that topology is refused and must be administratively stopped before using the Compose-managed backup path. Profile-gated auxiliary writers are not silently claimed as part of the ordinary selected topology; they must be administratively stopped for the P11 rehearsal when they touch protected state.

The final exact release candidate still requires P11 on real hosts: independent-destination capacity refusal, successful backup, failed-copy behavior, isolated restore, SQLite integrity, identity/Federation/human-auth/recorder continuity, replacement-member semantics, Windows DPAPI where applicable, and core recovery without model download.

No CF7 acceptance flag changes with this software candidate.
