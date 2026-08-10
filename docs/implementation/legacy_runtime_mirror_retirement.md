# Legacy runtime mirror retirement

This change makes `data/server_setup/server_settings.json` a migration/bootstrap input rather than a current product configuration mirror.

## Current authority boundary

- `data/capabilities/config.json` stores technical language-model and recorder parameters.
- Capability startup state stores completed capability intent.
- Contribution intent/activation owns contribution authority.
- `data/source_state/mtconnect_recorder_control.json` fences the managed recorder on/off state.
- `deployment_mode` and `ai_enabled` in a legacy setup document are never read as current recorder authority after capability-first migration.

## Compatibility retained

- CFI-6 may still read a completed legacy setup to produce a deterministic migration preview.
- The managed recorder accepts an explicitly configured `msh.server_setup.v3` document as a temporary technical fallback, but reads only recorder sources and poll interval from it; legacy role/AI authority fields are ignored.
- `setup_msh.py`, legacy phone bootstrap migration, the read-only legacy startup notice, and `/startup/choose` remain for a later retirement boundary.
- The core `CapabilityStartupTransitionService` still supports an injected legacy-shaped saver for isolated compatibility tests; the product app explicitly installs the role-free capability-config saver.

## Safety property

Writing or completing current capability-first configuration must not mutate `server_settings.json`. A configuration file by itself also cannot enable recording: the separate recorder control/contribution authority path remains required.
