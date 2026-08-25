# Windows update-agent smokes

These scripts are executed by the dedicated Federation software-update workflow on Windows runners. They cover Windows PowerShell native-process behavior that is not reliably reproduced by cross-platform Python tests.

`fcp_update_agent_activation_smoke.ps1` verifies that native stderr does not override a successful exit code and that the updater can observe the resume workflow's accepted partial-success exit code `4` without PowerShell promoting stderr into a terminating error.

`fcp_recorder_supervisor_restart_smoke.ps1` drives the real recorder supervisor with a compiled fake child and verifies the ordinary-failure restart state machine: an operator stop and a `STATUS_CONTROL_C_EXIT` Ctrl+C are never restarted, one unexpected failure is, repeated rapid failures fence supervision with exit code `6`, the backoff stays inside its ladder, a healthy runtime decays the fence, and the approved-update finalize call is unchanged. The Ctrl+C case is Windows-only by nature: an 8-bit POSIX exit status cannot carry `0xC000013A`.
