# CF7-C machine handoff

Use this page only after the CF7-C branch head is frozen and all permanent
gates are green on that exact commit.

## Machine assignments

- `local-ai`: local Windows GPU/Ollama machine.
- `cnc-recorder`: MSH-network machine connected to both CNC MTConnect Agents.
- `school-control`: Høgskolen i Østfold control, registered-compute and
  storage-candidate machine.

At least one of the latter two machines must be a physical Linux host.

## Per-machine handoff

1. Clone a fresh checkout and check out the frozen 40-character commit.
2. Install the pinned repository requirements, `pytest`, and `ruff`.
3. Set only the private environment variables for that machine profile.
4. Run the matching PowerShell or Bash wrapper with `Action all`.
5. Keep the generated `evidence/` directory local and uncommitted.
6. Start the supported product services and execute the issue #180 physical
   scenarios through the real UI and physical services.
7. Redact the evidence before final validation.

The wrapper result proves readiness only. It does not mark any physical scenario
passed and it cannot unblock CF8. The commit used by all three machines must be
the same final reviewed candidate; do not combine evidence from different
branch heads.
