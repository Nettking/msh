# Manual Federation-wide MSH updates

The Federation overview exposes two explicit coordinator-only POST operations:
**Check for updates** and **Update all devices**. GET remains passive. It may
refresh previously requested result state, but it never fetches Git or starts a
host mutation by itself.

The update target is one immutable 40-character Git commit from approved
`Nettking/msh` `main`. The browser supplies no repository, branch, path,
executable, command, or arguments. CSRF protects browser request integrity;
update authority is separately bound to the existing Federation session creator.

## Security boundary

Flask deliberately does **not** receive Git, Docker, shell, or general host
execution authority. Its only local mutation capability is an atomic, bounded
JSON handoff under the existing bind-mounted MSH data directory. A separately
owned host update agent validates that request again before doing anything.

Every host independently verifies:

1. the request schema, request ID, freshness, approved repository and `main`;
2. a full exact commit object ID;
3. that the local checkout is the expected repository root on `main`;
4. that `origin` is canonical `Nettking/msh` on GitHub with no embedded
   credentials, query, or fragment;
5. that the working tree is clean, including untracked files;
6. that the target commit exists and is reachable from the freshly fetched
   approved `main` tip;
7. that the current checkout can reach the target by fast-forward only.

The sole source mutation is `git merge --ff-only <exact-commit>`. There is no
stash, reset, clean, checkout, rebase, force push, arbitrary remote, or
peer-supplied process execution.

## Federation transport

Update intent uses the existing authoritative Federation session event log, not
a new remote-command channel. The coordinator publishes bounded declarative
events containing only the exact target, target node IDs, repository/branch
constants, request identity, and lifetime. Target devices act only when:

- the event actor is the session creator;
- their exact node ID is in the snapshotted target set;
- the event is fresh and bounded; and
- the local host agent independently accepts the same target.

Remote nodes derive and durably pin the coordinator identity from the
authenticated `session.created` event before accepting any update intent. This
works for already-paired devices without expanding the persisted pairing format.
An otherwise valid update event from another Federation member is ignored.

Check and activation results return as authenticated member events. Report
payloads contain only bounded state, source/target/running commit IDs, safe
reason codes, and safe messages. A device restart does not lose the operation:
the remote processor cursor and pending host request are durable, and the final
result is published after the saved Federation membership reconnects.

Offline devices are not queued. The coordinator snapshots reachability again at
**Update all** time. A device that became unavailable after the check is marked
`node_offline_not_queued` and retains its old runtime.

## Host activation

The supported Windows launcher (`start.cmd`) and POSIX launcher (`bash start.sh`)
start one bounded host-owned update agent. The agent is single-instance per
checkout/data directory and uses only locally fixed Git/Docker commands.

For an eligible target it:

1. revalidates Git and fast-forwards if needed;
2. exports the exact target as `MSH_BUILD_COMMIT`;
3. rebuilds the `relay`, `flask`, and `recorder` images;
4. starts/restarts the background services;
5. reads the required Ollama model from the newly built Flask image and verifies
   or installs that exact model before replacing the old Flask runtime;
6. stops the old Flask container and runs the existing saved-setup resume path;
7. starts the new Flask container;
8. reads the immutable build commit baked into the running image;
9. checks the Federation HTTP surface from inside the container; and
10. requires `relay`, `recorder`, and `flask` to be running.

Docker images receive `MSH_BUILD_COMMIT` as a build argument and bake it into the
image environment and label. Runtime success therefore cannot be inferred from
host `HEAD` alone.

The only success state for an activation is `runtime_verified`, and it requires
`running_commit == target_commit`. Source-only fast-forward is intentionally not
success.

The update agent also hashes its own implementation at startup. If a successful
fast-forward changed the updater itself, the POSIX agent replaces its process
with the newly checked-out script and the Windows agent starts the newly checked-
out PowerShell script after releasing its single-instance mutex. Future rollouts
therefore do not remain on stale host mutation code.

## Rollout ordering and failure semantics

Remote eligible devices receive the activation request first. The authoritative
coordinator queues its own host activation last, after the durable rollout state
and remote intent have been committed. The host handoff includes a short
activation grace; in practice the image build also occurs before Flask is
replaced, allowing the initiating HTTP response to complete.

Each device is independent. One dirty, offline, divergent, timed-out, or failed
host cannot convert another device's result into success. The overview retains
per-device states and reports `updated` only when every device that was actually
queued proves the exact running target. Mixed terminal results become
`update_completed_with_failures`.

There is no automatic update-on-start, reconnect-triggered update, delayed queue
for offline devices, destructive rollback, or self-selected target. An operator
must explicitly check, inspect the results, confirm the exact checked target,
and press **Update all devices**.

## Bootstrap requirement

Federation-wide runtime updates require the host-owned update agent and the
update-event processor to already exist on each participating device. A device
running an MSH version from before this capability was introduced cannot use a
remote update message to install the capability that would be needed to process
that message.

Such legacy devices need one normal manual update to an updater-capable `main`
commit and one start through the supported launcher (`start.cmd` or
`bash start.sh`). After that bootstrap, future approved `main` updates can use
the Federation flow. This transition is deliberate: MSH does not grant legacy
Flask containers a new shell/Docker escape hatch merely to bootstrap the first
remote update.
