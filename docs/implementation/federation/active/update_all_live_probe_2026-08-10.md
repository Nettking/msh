# Federation Update All live probe — 2026-08-10

This file is an intentionally harmless source-only probe used to validate the repaired two-device Federation `Update all devices` rollout path on Windows.

Acceptance requires both hosts to fast-forward to this exact commit, preserve their existing Federation relay state, rebuild and restart the supported runtime, resume saved setup, and report `runtime_verified` with the running build equal to the exact target commit without manual repair.
