# Manual Federation-wide MSH updates (v1)

This narrowly extends the read-only Federation overview with two explicit POST
operations. The GET remains passive: it reads bounded prior status and never
fetches or changes a checkout. **Check for updates** resolves one exact commit
from approved `Nettking/msh` `main`; **Update source on all devices** requires a
fresh checked target, CSRF, and a server-validated confirmation. This is not a
running-installation update: it does not rebuild or reinstall Docker images,
packages, virtual environments, or other built runtime artifacts.

Integration points:

- `GitUpdateAdapter` owns all local Git inspection and mutation. It accepts no
  remote URL, branch, path, command, arguments, or environment from a peer and
  permits only a verified `merge --ff-only`.
- `FederationUpdateService` binds operations to the existing internal session
  and its creator/coordinator identity. Membership alone is not update
  authority. CSRF is request-integrity protection, not user authentication.
- authenticated transport adapters expose only `UpdateIntent` check/apply
  calls. Every receiver revalidates session, sender authority, expiry, approved
  source, exact object ID, remote reachability, ancestry, and working-tree
  safety. Request history is bounded and conflicting replay fails closed.
- the rollout snapshots reachable peers, never queues offline nodes, handles
  peers before the initiating node, and retains one immutable target even if
  `main` advances.

The v1 local executor deliberately reports
`source_updated_restart_required` after a safe fast-forward. It does not claim
that the installed runtime or running process has activated the new code.
Operators restart with the platform's supported MSH launcher (`start.cmd
--resume` on Windows or the installation's owned Linux/Termux service). A
future lifecycle adapter may automate that handoff only after the supported
launchers provide a common, process-owned restart and post-start health proof.
No update-on-start, reconnect, delayed intent, destructive rollback, stash,
rebase, or force operation is implemented. A future action may be called
**Update all** only after installation-method-specific adapters implement and
prove fetch, fast-forward, rebuild/reinstall, owned-process restart, reported
commit identity, and bounded post-restart health verification on every
supported platform.
