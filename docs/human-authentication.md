# Human users, Federation sign-in, and permissions

Status: **current user and administrator guide**
Reviewed: **2026-08-12**

FCP has a separate account system for **people using the web application**. Human identity is deliberately separate from device identity, pairing credentials, recorder keys, and Federation membership.

## What is shared now?

A standalone FCP installation still has a local human-account database. Once devices belong to a Federation, however, the **immutable Federation creator remains the human credential/password authority**:

- human accounts are created and managed on that credential-authority installation;
- the same account can sign in on trusted Federation member devices;
- passwords and password hashes **never leave the credential authority**;
- members authenticate by sending the browser to the authority and accepting a short-lived, Ed25519-signed login assertion targeted to that member device;
- non-secret authorization metadata — email address, active state, and roles — is published through the authenticated Federation session log;
- member devices keep only a local shadow account needed for the local Flask session and permission checks.

This is Federation SSO rather than password-database replication. A stolen member database therefore does not contain the authority's human password hashes.

Device and human authority remain independent. An authenticated human `admin` can request a Federation administration action, but the underlying device/session operation still has to satisfy Federation membership and current device-side authority policy.

## Credential authority versus current operational leader

The Federation creator and current operational leader are no longer necessarily the same device.

- **Creator / human credential authority** — immutable Federation provenance and the installation that retains authoritative human passwords/password hashes.
- **Current operational leader** — the node holding the current coordinator-authored monotonic leadership term and reviewed leader-only product controls such as software updates, capability requests, member/invitation operations, pairing, and provider-management decisions.

A valid leader failover does **not** silently transfer human credential custody. Federation member sign-in continues to use the creator-backed human credential authority unless a future explicitly reviewed credential-migration mechanism changes that boundary.

This separation is intentional: operational availability/failover does not automatically replicate or move password secrets.

## First-time setup: create the first administrator in the browser

Authentication is enabled by default. A fresh production installation does not create a default username or password.

1. Start the FCP installation that will create the local authority:

   **Windows**

   ```cmd
   start.cmd
   ```

   **Linux/macOS**

   ```bash
   bash start.sh
   ```

2. Open the FCP web interface.
3. While the local human-user database is empty, normal browser requests redirect to:

   ```text
   /admin/users/bootstrap
   ```

4. Enter a valid administrator email address.
5. Enter and confirm a password of at least **12 characters**.
6. Submit the form.

The first account is created as an active `admin`. The anonymous bootstrap closes as soon as the first account commits, and the browser returns to the normal sign-in flow.

The bootstrap uses a singleton database claim in the same transaction as first-user creation, so two concurrent anonymous forms cannot each create a first administrator. If that claim exists while the user table is unexpectedly empty, FCP fails closed with 503 instead of reopening anonymous administrator creation.

You do **not** need the old `fcp-user create-admin` CLI command for the normal supported first-user path.

There is intentionally no built-in administrator or fallback password.

### Federation-member exception

A remotely paired Federation member may temporarily have zero local shadow accounts before anyone signs in there. That does **not** make it a new human credential authority. The member keeps the Federation human sign-in path and does not expose local first-admin bootstrap merely because its local shadow table is empty.

## Signing in on Federation members

On a Federation member, `/login` shows **Sign in through Federation leader** instead of accepting the member's local password database as authoritative. The product wording may say “leader”, but the credential assertion still comes from the creator-backed human credential authority described above even if operational leadership has transferred.

The browser flow is:

```text
member /login
  -> credential authority /federation-auth/authorize
  -> normal human login on the authority when needed
  -> authority signs a short-lived assertion
  -> member /federation-auth/callback
  -> local browser session
```

The assertion is bound to:

- the Federation ID and internal session;
- the credential-authority node identity;
- the exact target member node;
- the human email, active state, and roles;
- a random browser `state` value;
- a short validity window.

The member verifies the Ed25519 signature using the public identity advertised through Federation state. An assertion issued for one member cannot be replayed on another member, and a response with the wrong browser state is rejected.

## Browser addresses must be reachable

Federation relay connectivity and browser connectivity are different things. The browser must be able to reach the web address advertised by both the credential authority and the member.

For stable multi-device deployments, set a routable origin on each installation:

```text
FCP_HUMAN_AUTH_BASE_URL=http://192.0.2.10:5000
```

Use the actual HTTPS origin when FCP is served through HTTPS. The value must be an `http://` or `https://` origin root without credentials, query parameters, fragments, or an application subpath.

If `FCP_HUMAN_AUTH_BASE_URL` is not set, FCP learns the origin from the browser request that publishes the sign-in metadata. `localhost` is usually unsuitable when the browser must move between physical machines, because `localhost` then refers to the browser's own machine.

Tailscale can provide a trusted private reachability path for those browser origins, but Tailscale discovery/membership is not an FCP human credential and does not bypass Federation SSO. See [Tailscale Federation discovery](tailscale_federation_discovery.md).

## Add and manage users

Human-user administration is Federation-scoped around the creator-backed credential authority. Manage accounts at:

```text
/admin/users
```

On the credential authority this page creates and changes the authoritative accounts. Opening the same page on a member redirects the browser to the authority. Member-side POST requests to mutate users are rejected rather than creating a divergent local account.

The current page makes account state and role meaning explicit:

- role assignments are shown as chips with concise summaries;
- Active/Inactive state is visible independently of roles;
- the signed-in administrator's row is marked `You`; and
- the last-active-admin protection is shown on the affected row instead of only appearing after a rejected save.

To add a user:

1. sign in as an administrator;
2. open `/admin/users`;
3. enter the user's email address;
4. enter an initial password of at least 12 characters;
5. select one or more roles;
6. choose **Create user**.

Changes to active state and roles are published as non-secret Federation metadata. Existing member sessions refresh that authorization state from the Federation; deactivating a user causes the member shadow account to be deactivated when the updated state is observed.

The application prevents the final active administrator on the credential authority from being deactivated or stripped of the `admin` role.

## Roles

Use the least-privileged role that matches the person's job.

| Role | Intended use | Main capabilities |
| --- | --- | --- |
| `viewer` | Read-only users | View dashboards, data, analyses, documentation, system state, Federation status, devices, and capabilities; manage their own account password through the authority. |
| `operator` | Normal FCP operators | Everything in `viewer`, plus data upload, analyses, workflows, normal runtime controls, and recorder controls. |
| `admin` | FCP/Federation administrators | All human permissions, including Federation/provider administration, pairing, software updates, and human-user administration, subject to Federation device/session authority. |

An `operator` is intentionally **not** a Federation administrator.

Current human permissions are:

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

Routes are authorized by permissions rather than by hard-coded role names.

## Password changes

Passwords live only on the creator-backed human credential authority. Therefore a password change requested from a member is redirected to that authority. Unsafe member-side password-change requests are rejected.

A member shadow account is not an independent credential. Its purpose is to represent an already verified Federation human inside the member's local Flask session and permission model.

## Federation human-auth events

The durable Federation log can contain these human-auth control records:

- `human_auth.authority.published` — credential-authority node ID, public key, and browser base URL;
- `human_auth.member_endpoint.published` — a member's own browser base URL;
- `human_auth.user.changed` — email, active state, and roles.

Coordinator policy permits only the creator-backed credential authority to publish authority/user state. A member may advertise an endpoint only for its own authenticated node identity.

**Passwords, password hashes, password salts, Flask session secrets, pairing tokens, and device private keys are not placed in these events.**

Operational `session.leader.changed` events do not rewrite this human-auth authority.

## Local authentication data

The credential authority stores its human credential database and local web-session secrets under `data/auth/` by default:

```text
data/auth/users.sqlite3
data/auth/flask-secret
data/auth/password-salt
```

A member can also have `users.sqlite3`, but Federation-authenticated users there are shadow records rather than an authoritative password store.

Back up the credential-authority authentication database and its secrets together. Do not commit them to Git or copy them into documentation/logs.

Important consequences:

- deleting the authority `users.sqlite3` removes the authoritative human-account database;
- changing/deleting the authority `password-salt` can make its password hashes unusable;
- changing a device's `flask-secret` invalidates browser sessions on that device;
- `start.cmd --fresh` preserves human-auth files unless the reset operation explicitly says otherwise.

## Configuration

Normal single-machine use needs no human-auth environment variables. Relevant advanced settings are:

- `FCP_AUTH_DATABASE` — local human/shadow database path; default `data/auth/users.sqlite3`;
- `FCP_AUTH_SECRET_DIR` — local secret directory; default `data/auth`;
- `FCP_FLASK_SECRET` — explicit Flask session secret, at least 32 characters;
- `FCP_PASSWORD_SALT` — explicit local password-hashing salt, at least 32 characters;
- `FCP_SESSION_MINUTES` — authenticated session lifetime in minutes; default `480`;
- `FCP_HTTPS=1` — mark session/remember cookies Secure when HTTPS is actually enforced;
- `FCP_HUMAN_AUTH_BASE_URL` — stable browser-reachable FCP origin for Federation SSO;
- `FCP_HUMAN_AUTH_LOCAL_FALLBACK=1` — explicitly allow member-local password login for emergency recovery.

### Emergency local fallback

Member-local password login is disabled by default, including when the Federation relay is temporarily unreachable. This prevents a network outage from silently turning an old local password database into an authentication bypass.

`FCP_HUMAN_AUTH_LOCAL_FALLBACK=1` deliberately weakens that boundary and should be used only as an operator-controlled recovery mechanism. When enabled, the member login page labels the local form as recovery mode.

## Development-only authentication bypass

For local development only, human authentication can still be disabled with both:

```text
FCP_DEVELOPMENT=1
FCP_AUTH_DISABLED=1
```

`FCP_AUTH_DISABLED=1` is rejected outside development/test operation.

## Sessions and CSRF protection

Human sessions use HttpOnly cookies with SameSite=Lax. Secure cookies are enabled when `FCP_HTTPS=1`.

Human login, first-user bootstrap, user administration, and the member's Federation sign-in start form use CSRF protection. The Federation login callback additionally validates the signed assertion and random browser state. Hiding a UI control is never treated as authorization; permission and Federation authority checks are enforced server-side.

## Upgrading an existing Federation

For an existing installation:

1. update all Federation devices to a version containing Federation human SSO and browser first-user handling;
2. ensure the creator-backed credential authority has the intended authoritative human accounts;
3. give the authority and members browser-reachable `FCP_HUMAN_AUTH_BASE_URL` values when automatic request-origin discovery is not sufficient;
4. load/sign in to the credential-authority FCP so its authority metadata is published;
5. open a member `/login` and choose **Sign in through Federation leader**.

Accounts that existed only on a member are **not automatically promoted to Federation credentials**, because that would let an arbitrary member create a Federation-wide human identity. Recreate any such account on the creator-backed credential authority if it should be Federation-wide.

If operational leadership later transfers, do not recreate/move passwords merely to match the new leader. Current leader and credential authority are separate by design.

## Troubleshooting

### I only see first-user setup and have no account

On a fresh local authority, this is expected. Create the first administrator at `/admin/users/bootstrap`.

### First-user setup returns 503 even though there are no users

The atomic bootstrap claim may exist while the user table is unexpectedly empty. FCP treats this as inconsistent auth state and fails closed rather than reopening anonymous admin creation. Investigate/restore the human-auth database as a unit instead of deleting the claim or salt individually.

### A remotely paired member does not offer first-user setup

That is intentional. Use Federation human sign-in. An empty local shadow-user table is not permission to claim new human credential authority.

### A member says the Federation sign-in authority is unavailable

Open the creator-backed credential-authority FCP web UI/login page and verify its Federation connection/browser reachability. For multi-host use, configure `FCP_HUMAN_AUTH_BASE_URL` to a browser-reachable authority origin rather than `localhost`.

If a different device is current operational leader, that does not move the credential authority.

### The browser is redirected to the wrong machine

Check `FCP_HUMAN_AUTH_BASE_URL` on the authority and member. Federation relay addresses are not substitutes for browser HTTP/HTTPS addresses.

### A user can sign in but receives 403 Forbidden

Check the user's roles on the credential authority at `/admin/users`. Authentication establishes identity; permissions still determine which FCP operations are allowed. Some operations additionally require the FCP device itself to be the current operational leader.

### I cannot use a member's old local password

That is intentional. Federation members trust the creator-backed credential authority for human authentication. Use **Sign in through Federation leader**, or explicitly enable the emergency local fallback only when you understand the security trade-off.

## Related guides

- [Quick start](quick_start.md)
- [Federation operations](federation_operations.md)
- [Tailscale Federation discovery](tailscale_federation_discovery.md)
- [Server setup](server_setup.md)
- [Troubleshooting](troubleshooting.md)
