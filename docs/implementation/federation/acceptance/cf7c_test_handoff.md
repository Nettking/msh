# CF7-C machine handoff

Status: **active physical-acceptance handoff**

Use this page only after one exact candidate commit is frozen and all required permanent gates are green on that commit.

## Machine assignments

- `local-ai`: local Windows GPU/Ollama machine.
- `cnc-recorder`: MSH-network machine connected to both CNC MTConnect Agents.
- `school-control`: Høgskolen i Østfold control, registered-compute, and storage-candidate machine.

At least one of the latter two machines must be a physical Linux host.

## Per-machine handoff

1. Clone a fresh checkout and check out the same frozen 40-character commit.
2. Install the pinned repository requirements, `pytest`, and `ruff`.
3. Set only the private environment variables for that machine profile.
4. Run the matching PowerShell or Bash readiness wrapper with `Action all`.
5. Keep the generated `evidence/` directory local and uncommitted.
6. Start the supported product using `start.cmd` on Windows or the documented Compose command on Linux.
7. Complete mandatory Identity, Federation, Inspect, and Finish onboarding.
8. Execute the named physical scenarios through the real UI and physical services, including any scenario-specific benchmarks and contribution actions.
9. Redact the evidence before final validation.

Readiness output proves preparation only. It does not mark a physical scenario passed and cannot unblock CF8.

Do not combine evidence from different commits. Complete physical CF7 acceptance and all three end-to-end acceptance flags remain false until the separate evidence-backed review is accepted.