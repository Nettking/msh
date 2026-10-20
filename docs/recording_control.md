# MTConnect recording control — superseded guide

Status: **compatibility redirect; non-authoritative**

The older role-based recorder/startup instructions on this page were superseded by FCP's capability-first onboarding and current recorder services. Do not use retired **Recorder station / Full server** setup or role-based state files as the current operating procedure.

Use the maintained guides instead:

- [Operator guide](operator_guide.md) — source configuration, live monitoring, status, and normal workbench operations
- [Standalone recorder](standalone_recorder.md) — headless pairing, source discovery, loss-aware capture, and Federation publication
- [Source synchronization](source_synchronization.md) — source configuration and ingestion
- [Server setup](server_setup.md) — supported launchers, deployment, and host configuration
- [Troubleshooting](troubleshooting.md) — current recorder/runtime diagnostics

Preserve existing source identities, raw recordings, checkpoints, Federation and human-auth state during any migration. Deleting or resetting these resources requires an explicitly scoped, reviewed operation.

The prior procedure remains available in Git history for migration investigations; it does not establish present product behavior.
