# D04 controlled Nitro host procedure

The user's post-sweep authorization supersedes the earlier no-mutation/dedicated
port proposal. Actual merged main9b286f93 is fully qualified. This is environment
maintenance, not physical acceptance, and changes no product source.

Fresh18:08UTC inspection proves the same stale PID1422341, start token120268097,
UID1000, cwd/home/martin/fcp, deleted Python3.14 executable, session601 cgroup,
and socket31225091 owning5151. It is orphaned under PID1, not a service cgroup.
Nitro fingerprintad885120af1f11cf and clean running source0536f03d are unchanged.

1. Run the checked-in audit script `diagnostics/d04_controlled_responder_replacement.py`
   through the existing authenticated `ssh_campaign_script.py nitro` transport,
   with `--nitro-runtime-python --timeout 75`. It refuses an existing operation receipt.
2. Recheck exact PID/start/cwd/argv hash/UID/cgroup/socket ownership and existing
   candidate secret, Flask data mount, source and running core SHA before acting.
3. Send only SIGTERM through a PID file descriptor to that proved stale instance.
   No process-group kill, escalation, Docker action, source advance or data reset.
4. Start the unchanged checked-in N host responder using its existing native
   Python3.12 environment, existing campaign secret/data/PID paths and actual
   Flask tailnet port. This replaces a legacy daemon with the currently deployed
   campaign service; it does not deploy a repair or silently mix N and M runtime.
5. Verify exact new process/socket/source, real local health200/ready/Tailscale,
   no stale respawn during10s, unchanged core container IDs/start times and existing
   secret metadata. The candidate PID record and maintenance log are expected writes.
   No enrollment/grant request is sent. No Recorder-host operation occurs.
6. Persist the local and GitHub receipt immediately; verify health from Nettking.
   D04's current-runtime port conflict can then be resolved. M still needs fresh
   positive responder/runtime verification after its own admission; N observations
   cannot count as M physical evidence. Freeze M, run clean checked-in revalidation,
   and align all admitted runtime with M before collecting physical evidence.

If any guard fails, inspect the saved host receipt and stop dependent mutations.
Never rerun blindly. Host receipt:
`/home/martin/fcp-v1-73c779-nitro-20260910/inputs/d04-controlled-replacement-20260911.json`.
Do not remove its evidence or alter protected MSH Recorder persistent data.
