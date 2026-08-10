# Federation update rollout acceptance — 2026-08-10

A live two-device Windows rollout exposed two activation-path regressions after both devices had already verified the same immutable target commit.

## Observed regressions

1. Windows PowerShell 5.1 promoted normal `docker compose stop flask` progress written on stderr into a terminating PowerShell error while the native process itself was succeeding. The host agent therefore stopped after Flask had been stopped and before resume/start/runtime verification.
2. A manually bootstrapped agent could run without the retained `FCP_RELAY_VOLUME_NAME` environment selected by the supported launcher. In that state, Compose could fall back to a different retained relay volume and `existing_setup_resume` could fail with `unknown-node`.

No identity, Federation membership, or Docker volume was deleted during recovery. Both devices were recovered by selecting their already-mounted Federation state, resuming the saved setup, recreating Flask, and verifying the running build commit.

## Required fix properties

- Native Docker progress on stderr must never determine success; native exit status remains authoritative.
- Exit code `4` from `existing_setup_resume` remains an accepted partial-success result.
- The updater must preserve the relay volume already mounted by the active Flask/relay runtime before any Compose recreation.
- If Flask and relay disagree about the mounted coordinator volume, activation must stop safely rather than guess.
- If there is no mounted coordinator volume, an explicitly inherited `FCP_RELAY_VOLUME_NAME` may be used; otherwise activation must stop safely.
- The update agent must continue to use the fixed local repository, `main`, fast-forward-only source mutation, clean-build verification, and exact running-commit verification.

## Live acceptance after merge

The fix commit itself is bootstrapped once on both Windows hosts so the running host agents contain the repaired activation logic. Then a separate harmless probe commit is merged to `main` and used as the exact Federation update target.

Acceptance requires:

1. `Check for updates` reports both devices eligible for the same exact probe commit.
2. `Update all devices` requests the remote device first and the coordinator last.
3. Each host preserves its existing Federation coordinator volume, builds the new images, resumes saved setup, restarts Flask, and reports `runtime_verified` only after the running commit equals the exact target.
4. A final `Check for updates` reports both devices `Up To Date` with the same source and running commit.

The probe must not require manual Docker, Git, volume, or identity repair. If any host stops before runtime verification, the acceptance test fails and the rollout must not be considered production-ready.
