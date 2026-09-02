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

`catalog.federation.backup_recovery` now implements the ordinary Compose-managed v1 topology.

The backup:

1. requires a clean exact source checkout;
2. resolves effective Compose data/results/relay sources;
3. proves Docker's actual persistent backing resource;
4. requires a new destination on a different backing filesystem from checkout, data, results and Docker state;
5. sizes the local state and relay volume and proves bytes/inode capacity before quiescence;
6. acquires the shared host-mutation lease, excluding supported update/build/activation;
7. refuses a detected live native recorder in this selected topology;
8. safely stops a matching tailnet responder only after PID plus OS process-creation identity match;
9. stops Compose and proves Flask, relay and managed recorder are stopped;
10. remeasures the exact payload and destination capacity after quiescence;
11. copies the data/results/.env state plus retained relay volume without following link-like paths;
12. uses only the already-present relay image with `--pull=never --network=none` for relay-volume sizing and `docker cp` for the stopped volume copy;
13. leaves an explicit incomplete marker until copied SQLite state has passed `PRAGMA quick_check`;
14. publishes a bounded completion manifest and exact source commit only after those checks pass; and
15. deliberately leaves FCP stopped so neither success nor failure implicitly restarts writers.

The administrator guide now requires explicit verification and normal supported `--resume` startup after the operator has inspected the result and host capacity.

## Scope

This does not claim online/hot backup. It does not attempt to clone Windows DPAPI identity to another user/machine. It does not automatically quiesce a live direct native-recorder topology; that topology is refused and must be administratively stopped before using the Compose-managed backup path.

The final release candidate still requires P11 on real hosts: independent-destination capacity refusal, successful backup, failed-copy behavior, isolated restore, SQLite integrity, identity/Federation/human-auth/recorder continuity, replacement-member semantics, and core recovery without model download.

No CF7 acceptance flag changes with this software candidate.
