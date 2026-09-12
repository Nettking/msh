# D12: controlled Beast CI checkout trust repair

Status: APPLIED AND VERIFIED on actual Beast NETWORK SERVICE identity; affected CI check pending. This is CI host maintenance, not candidate source
deployment or physical acceptance. Evidence: diagnostics/D12-confirmed-ownership-root-cause.json.

Actual Git stderr identifies the cause: checkout
`C:\actions-runner\_work\msh\msh` is owned by `Beast\nksra`; the native runner
executes as NETWORK SERVICE (`S-1-5-20`). The ownership guard blocks ordinary
native Git operations. Checkout/cleanup actions use their own temporary trust
configuration, explaining why those actions can succeed.

Use the existing authenticated Actions runner registration, with its existing
`beast-windows` label, to avoid requiring a new SSH trust/credential setup.
The maintenance workflow is confined to this coordination branch and an exact
workflow-file push trigger. It must never be merged into the release candidate.
It performs no checkout and no product build/test/deployment.

Before mutation, fail closed unless all conditions match:

- GitHub repository is Nettking/msh, runner name is Beast, computer is BEAST,
  operating system is Windows and current SID is S-1-5-20.
- GITHUB_WORKSPACE resolves exactly to `C:\actions-runner\_work\msh\msh`;
  workspace, parent directories and `.git` are real directories with no reparse points.
- The runner registration name/work-folder match the known dedicated CI instance.
- Git resolves to the installed Git for Windows cmd/bin executable.
- The local origin is the expected repository and the existing checkout is one
  of the exact observed b7194820/0355023f/440123f6 sources. These reads may use a
  process-local trust entry for this single verified CI path; no wildcard.
- Preserve the before-state owner/SID, plain Git error, current global trust
  entries, executable and source SHA as the audit evidence.

The only repair is adding that **one exact CI checkout path**, if absent, to the
existing runner identity's global `safe.directory` list. It does not change the
service account, ownership, ACLs, runner labels/pools, worktree files, Git refs,
CI assertions/deadlines, VCS stamping or any product/runtime configuration.
Existing unrelated Git trust entries remain unchanged. No wildcard is added.

Afterward, require unmodified plain Git calls (without process-local exceptions)
to verify the same commit from both repository root and sidecar directory using
both installed cmd/bin Git entry points. Require the final diff check to work.
Fail on a different source or any unexpected result; never mask failure by a retry.
Retain the before/after JSON as a GitHub artifact and in the diagnostic checkpoint.

The maintenance job is serialized on the same native runner registration; no
external cleanup or account change is performed during other native jobs.
Protected MSH Recorder data and physical runtime paths are never accessed.
Once correction is actually demonstrated on Beast, retry only the affected failed
PR473 Windows release check on its unchanged exact source. Do not repeat successful
qualification or the native replacement proof. Older source failures remain durable.

If preconditions, access, or correction fail, preserve the result under D12 and
stop this repair path. Do not relax Git ownership protection or use another host
as evidence that Beast is fixed.

Actual reviewed run34704678898/job103582477854: see diagnostics/D12-host-repair-reviewed.json. The workflow control SHA and unchanged existing CI checkout SHA are separately recorded.
