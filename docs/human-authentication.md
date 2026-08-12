# Human users, Federation sign-in, and permissions

Status: **current user and administrator guide**
Reviewed: **2026-08-12**

FCP has a separate account system for **people using the web application**. Human identity is deliberately separate from device identity, pairing credentials, recorder keys, and Federation membership.

## What is shared across the Federation?

Human passwords are **not** copied to every Federation member.

The immutable Federation creator remains the human credential/password authority:

- authoritative human accounts are created and managed there;
- passwords and password hashes remain there;
- trusted Federation members redirect the browser to that authority for sign-in;
- the authority returns a short-lived Ed25519-signed assertion targeted to the member device; and
- non-secret authorization metadata such as email, active state, and roles is published through the authenticated Federation session log.

Members keep only the local shadow account needed for the local Flask session and permission checks.

This is Federation SSO, not password-database replication.

## Credential authority versus current operational leader

The human credential authority and the current Federation operational leader are intentionally different concepts.

- **Federation creator** — immutable creation provenance and human credential/password authority.
- **Current operational leader** — the node holding the current coordinator-authored leadership term and current-leader product controls such as software updates, capability requests, member/invitation administration, and reviewed provider actions.

If operational leadership transfers after bounded leader failover, the human password database does **not** silently move to the successor. Member sign-in continues to use the creator-backed human credential authority unless a future explicitly reviewed credential-migration mechanism changes that boundary.

## First-time setup: create the first administrator in the browser

Authentication is enabled by default. There is no built-in administrator and no fallback password.

1. Start FCP with `start.cmd` or `bash start.sh`.
2. Open the FCP web interface.
3. If this local authority has zero human users, normal browser requests redirect to:

   ```text
   /admin/users/bootstrap
   ```

4. Enter a valid email address.
5. Enter and confirm a password of at least **12 characters**.
6. Submit the form.

FCP atomically claims the one-time first-user bootstrap and creates exactly the first active `admin` account. After that commit, the anonymous bootstrap surface closes and ordinary login enforcement takes over.

You do **not** need the old `fcp-user create-admin` CLI command for the normal supported first-user workflow.

A remotely paired Federation member with an empty local shadow-user database does not reopen anonymous bootstrap. It stays on the Federation human sign-in path.

## Sign in on a Federation member

On a member, `/login` offers **Sign in through Federation leader/authority** instead of treating the member's local password database as authoritative.

The browser flow is conceptually:

```text
member /login
  -> human credential authority
  -> normal human login when needed
  -> authority signs short-lived assertion
  -> member callback
  -> local browser session
```

The assertion is bound to Federation/session identity, authority node, target member node, human subject, active state/roles, browser state, and expiry. An assertion for one member cannot simply be replayed on another.

## Browser addresses must be reachable

Federation relay connectivity and browser connectivity are different. For stable multi-device deployments, configure a browser-reachable origin when automatic request-origin discovery is insufficient:

```text
FCP_HUMAN_AUTH_BASE_URL=http://192.0.2.10:5000
```

Use the real HTTPS origin when serving FCP through HTTPS. `localhost` is unsuitable when the browser must move between different physical machines.

## Add and manage users

Administrators manage human accounts at:

```text
/admin/users
```

On the credential authority, the page shows:

- each user's active/inactive state;
- role chips and concise role summaries;
- a `You` marker for the signed-in administrator;
- the account-active toggle separately from role assignment; and
- a visible warning when an account is the last active administrator.

To add a user:

1. sign in as an administrator;
2. open `/admin/users`;
3. enter the user's email address;
4. enter an initial password of at least 12 characters;
5. select one or more roles; and
6. choose **Create user**.

The application refuses to deactivate or demote the final active administrator.

On a Federation member, opening `/admin/users` redirects to the human credential authority. Unsafe member-local writes are rejected rather than creating divergent credentials.

## Roles

| Role | Intended use | Main capabilities |
| --- | --- | --- |
| `viewer` | Read-only users | View dashboards, data, documentation, system/Federation state, and manage their own account password through the authority. |
| `operator` | Normal FCP operators | `viewer` plus upload, analysis, workflow, runtime, and recorder operations. |
| `admin` | FCP/Federation administrators | All human permissions, including Federation/provider administration, pairing, software updates, and human-user administration, still subject to Federation-side device authority. |

Current human permissions include:

- `dashboard.read`
- `data.read`
- `data.upload`
- `analysis.run`
- `workflow.run`
- `runtime.control`
- `recorder.control`
- `federation.read`
- `federation.manage`
- `pairing.manage`
- `software.update`
- `users.manage`
- `account.manage`

Routes are authorized by permissions rather than by scattered role-name checks.

## Password changes

Passwords live only on the human credential authority. A password change requested from a member is redirected to the authority. A member shadow account is not an independent Federation credential.

## Local authentication data

The credential-authority installation stores its human credential database and persistent web-auth secrets under `data/auth/` by default:

```text
data/auth/users.sqlite3
data/auth/flask-secret
data/auth/password-salt
```

Back up the credential database and its secrets together. Do not commit them to Git or copy them into logs/documentation.

Important consequences:

- deleting the authority `users.sqlite3` removes authoritative human accounts;
- changing/deleting its `password-salt` can invalidate password hashes;
- changing a device `flask-secret` invalidates browser sessions on that device;
- `start.cmd --fresh` preserves human-auth files unless the reset boundary explicitly says otherwise.

## Configuration

Normal single-machine use needs no human-auth environment variables. Advanced settings include:

- `FCP_AUTH_DATABASE` — local human/shadow database path;
- `FCP_AUTH_SECRET_DIR` — local persistent secret directory;
- `FCP_FLASK_SECRET` — explicit Flask session secret, at least 32 characters;
- `FCP_PASSWORD_SALT` — explicit password-hashing salt, at least 32 characters;
- `FCP_SESSION_MINUTES` — authenticated session lifetime;
- `FCP_HTTPS=1` — mark relevant cookies Secure when HTTPS is actually enforced;
- `FCP_HUMAN_AUTH_BASE_URL` — stable browser-reachable FCP origin;
- `FCP_HUMAN_AUTH_LOCAL_FALLBACK=1` — explicit member-local recovery login.

### Emergency local fallback

Member-local password login is disabled by default, including during a Federation outage. This avoids turning an old local credential database into a silent authorization bypass.

`FCP_HUMAN_AUTH_LOCAL_FALLBACK=1` deliberately weakens that boundary and should be used only as an operator-controlled recovery mechanism.

## Development-only authentication bypass

For local development/test only:

```text
FCP_DEVELOPMENT=1
FCP_AUTH_DISABLED=1
```

`FCP_AUTH_DISABLED=1` is rejected outside development/test operation.

## Sessions and CSRF

Human sessions use HttpOnly cookies with SameSite=Lax. Secure cookie behavior follows `FCP_HTTPS`.

Human login, first-user setup, user administration, and Federation sign-in entry points retain CSRF/server-side authorization checks. Hiding a UI control is never treated as authorization.

## Troubleshooting

### I opened a fresh installation and have no account

Open the FCP web interface. With zero local users, FCP should redirect to `/admin/users/bootstrap`. Create the first administrator there.

If the bootstrap claim exists while the user table is unexpectedly empty, FCP fails closed rather than reopening anonymous administrator creation. Treat that as damaged auth state and investigate it; do not delete individual auth files to bypass the guard.

### A Federation member does not show local first-admin setup

That is intentional. A remotely paired member uses Federation human sign-in instead of claiming new local human credential authority.

### A member says the human sign-in authority is unreachable

Verify the creator/credential-authority FCP web origin and `FCP_HUMAN_AUTH_BASE_URL`. The current operational leader may be a different device after leader failover; that does not move human credential custody.

### A user signs in but receives 403

Check roles at `/admin/users`. Authentication establishes identity; permissions still determine allowed browser actions, and Federation-side authority may impose an additional device-level check.

## Related guides

- [Quick start](quick_start.md)
- [Federation operations](federation_operations.md)
- [Tailscale Federation discovery](tailscale_federation_discovery.md)
- [Troubleshooting](troubleshooting.md)
