# Windows update-agent smokes

These scripts are executed by the dedicated Federation software-update workflow on Windows runners. They cover Windows PowerShell native-process behavior that is not reliably reproduced by cross-platform Python tests.

`fcp_update_agent_activation_smoke.ps1` verifies that native stderr does not override a successful exit code and that the updater can observe the resume workflow's accepted partial-success exit code `4` without PowerShell promoting stderr into a terminating error.
