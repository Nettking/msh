# Federation software-version branch trials

| Metadata | Value |
| --- | --- |
| Status | Active |
| Audience | Maintainers, reviewers, physical recorder operators |
| Scope | Selecting an approved-repository branch from the Federation UI and running it on a device with automatic verified fallback to the exact known-good `main` commit |
| Authority | Extends [Manual Federation-wide FCP updates](manual_updates.md); does not replace or relax it |
| Parent | [Active Federation plans](index.md) |
| Reviewed | 2026-08-21 Europe/Oslo |

## Problem

Physical development against a standalone Windows MTConnect recorder currently
means updating that recorder to `main` and hoping. There is no supported way to
try a branch on real hardware, and no supported way to get back to the exact
version that was working if the branch cannot start.

The existing updater is deliberately fail-closed: approved repository only,
`main` only, clean checkout only, prevalidate before stopping capture, mutate
only after the process exits, and prove the replacement. Branch trials must be
built *inside* that architecture, not beside it.

## Decisions

### 1. Where branch selection belongs

Three layers, three different authorities, unchanged from the updater:

* **Approved-source boundary** — `catalog/federation/software_update.py` already
  owns `APPROVED_REPOSITORY`, the approved-remote check and the shell-free Git
  runner. Branch *discovery* is added there: `GitUpdateAdapter.approved_branches()`
  runs `git ls-remote --heads <remote>` only after the same approved-remote
  validation the updater performs. The dropdown is therefore populated from the
  local checkout's own approved remote. It can never contain another repository.
* **Federation control plane** — the leader publishes one bounded declarative
  command carrying `{repository, branch, target_commit, target_node_ids, ttl}`.
  No path, URL, command, argument, interpreter or environment value travels.
* **Local host** — the device re-resolves the branch against *its own* approved
  remote and resolves every path, interpreter and command itself.

### 2. How exact commits are resolved

The leader sends the exact 40-character commit it read from `ls-remote`. The
device does not trust it. It fetches the named branch from its own approved
remote, resolves that branch tip, and requires the requested commit to be
contained in that branch (`merge-base --is-ancestor`). A stale dropdown, a
force-push or a renamed branch therefore fails closed instead of running an
object nobody published on that branch.

### 3. Worktrees: yes

Trials run from a **separate detached Git worktree**. The production checkout
never leaves `main` and is never mutated during a trial.

The decisive reason is rollback. If the production checkout were switched
between branches, rolling back would require a Git mutation *while capture is
stopped and the recorder is down* — a second thing that can fail at exactly the
worst moment, and one that would need `checkout`/`reset` on a tree this design
only ever moves with `merge --ff-only`. With a worktree, rollback is "launch the
child from the production root again": **no Git operation at all**.

Two further properties follow for free:

* every `GitUpdateAdapter.inspect()` invariant (on `main`, clean, approved
  remote) keeps holding throughout a trial, so `Check for updates ->
  Update all devices` is bit-for-bit unaffected; and
* `git worktree add --detach` is purely additive — it never touches the
  production working tree, index or `HEAD`.

Layout, resolved locally from the repository root and never from a peer:

```
C:\fcp\git\v2\                     production checkout, always main, known-good
C:\fcp\git\v2-fcp-trials\<id>\     detached trial worktree at the exact commit
```

The trial root is always named from the repository root the operator actually
uses, whatever that is; the paths above are one worked example.

The trial root is a **sibling** of the repository root, never inside it: a
worktree inside the checkout would make `git status --untracked-files=all`
dirty and permanently refuse ordinary updates.

Trial worktrees are never removed with `--force` and never while they contain a
`data` directory, so no automated cleanup path can reach recorder data.

### 4. How the known-good fallback is pinned

Before the recorder is asked to stop, a durable bounded journal
(`data/federation/recorder-update-agent/trial-journal.json`) records:

```
safe_branch  = main
safe_commit  = <exact commit the running recorder proved in its own heartbeat>
safe_root    = <production checkout>
trial_branch, trial_commit, trial_root, trial_id
```

`safe_commit` comes from the **running process's own heartbeat**, cross-checked
against the production checkout `HEAD`. It is never "whatever `origin/main`
points at later". Because the production checkout is not moved during a trial,
that exact commit is still checked out when rollback needs it, and the agent
re-verifies `HEAD == safe_commit` before relaunching rather than assuming it.

### 5. How startup success is evaluated

A bounded 60-second acceptance window. The verdict is deliberately made from
**outside the branch under test**: a trial child runs from the trial worktree,
so any check living in its own tree is one that branch could omit, break, or
simply predate — and a branch that never fails itself would keep an unproven
recorder running indefinitely. The supervisor therefore starts one bounded
watchdog from the *permanent* checkout alongside a trial child, and that
watchdog owns the verdict. The code inside the trial tree is only an actuator:
it reads a stop marker addressed to its exact process instance and ends its own
capture the way Ctrl+C would.

Rollback verification stays in-process, because a rollback always runs from the
permanent checkout by construction.

All of the following are required:

1. a replacement process exists and is running;
2. it is a *different* instance — new PID **and** the exact process-instance
   nonce the supervisor recorded for this launch;
3. its `build_commit` is exactly the trial commit;
4. its heartbeat is newer than the activation and fresh;
5. Federation membership reports `connected`; and
6. capture reaches a healthy state, judged against the pinned baseline: the
   trial must reach `recording`, or, when the known-good version itself was not
   recording (an unplugged source, a recorder on standby), at least the state
   the known-good version was in. A trial is never failed for a fault the safe
   version already had, and `error`/`stopped` is never healthy.

A Python process existing is not success anywhere in this path.

### 6. How rollback happens and is verified

The trial recorder verifies *itself*. This is what makes the fallback work
without ever force-killing anything:

* if the trial process crashes outright, the supervisor sees a non-approved exit
  code from a trial child and consults the agent;
* if the trial process starts but cannot prove health inside the window, the
  permanent checkout's watchdog records the failure durably and writes a stop
  marker naming that exact process instance; the trial process reads it, ends
  capture the same way Ctrl+C would, and exits with the approved-restart code.

A trial that ignores its stop marker is **reported, never killed**: the trial
child writes to the real recorder data directory, so forcing it dead is exactly
the corruption this path exists to avoid. The watchdog waits a bounded grace and
records whether the stop was honoured.

Either way the supervisor asks the agent what to launch next and gets the pinned
safe checkout. The rollback is then verified with the *same* acceptance
predicate against `safe_commit`, again requiring a different process instance.
An operator's Ctrl+C still wins: the latch is only honoured when the operator
did not stop the process.

Stages, all durable: `preparing`, `trial_starting`, `trial_verifying`,
`trial_running`, `trial_failed`, `rollback_starting`, `rollback_verifying`,
`safe_restored`, `rollback_failed`. Rollback is attempted once; a safe version
that also fails is reported explicitly rather than looped.

### 7. Compatibility with "Update all devices"

* `fcp.host-update-request.v1` and its validation are unchanged — still `main`
  only. Trials use a separate `fcp.host-trial-request.v1`.
* A trial and an update are mutually exclusive; each refuses while the other is
  active with a bounded code.
* While a device is on a trial branch, ordinary updates refuse on that device
  rather than fast-forwarding a checkout the running process is not using.
* Device rows gain additive fields only.

## Out of scope for v1

Long-running behavioural probation (rolling back later because capture stopped,
checkpoints stalled, publication stopped, backlog grew or jobs failed) is
deliberately not built. The journal and acceptance predicate are the seam it
would extend.
