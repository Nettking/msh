# Human users, sign-in, and permissions

Status: **current user and administrator guide**  
Reviewed: **2026-08-12**

FCP has a separate account system for **people using the web application**. These human accounts are not Federation devices and do not reuse node identities, pairing codes, recorder keys, or machine credentials.

If you are installing FCP for the first time, the human-account step comes before device onboarding:

```text
Start FCP
  -> create the first administrator
  -> sign in
  -> Identity
  -> Federation
  -> Inspect
  -> finish setup
```

## First-time setup: create the first administrator

Authentication is enabled by default. A fresh production installation does not create a default username or password.

1. Start FCP normally so the Flask image and persistent authentication state are available:

   **Windows**

   ```cmd
   start.cmd
   ```

   **Linux/macOS**

   ```bash
   bash start.sh
   ```

2. From the repository directory, create the first administrator. The same one-line command works from Windows Command Prompt, PowerShell, Linux, and macOS when Docker Compose is available:

   ```bash
   docker compose run --rm --no-deps --entrypoint flask flask --app catalog.flask_app.app:create_app fcp-user create-admin
   ```

3. Enter the administrator's email address and a password of at least **12 characters** when prompted. The command normalizes the email address, refuses duplicate accounts, hashes the password, and grants the `admin` role.

4. Open:

   ```text
   http://localhost:5000/login
   ```

   If the launcher selected another web port, use the address printed by `start.cmd` or `start.sh`.

5. Sign in with the administrator you just created. On a fresh device, continue to:

   ```text
   http://localhost:5000/onboarding
   ```

There is intentionally no built-in default administrator. Losing all administrator credentials therefore requires an explicit administrative recovery action rather than falling back to a known default password.

## Add and manage users

An administrator can manage human accounts at:

```text
http://localhost:5000/admin/users
```

The current administration page is deliberately small. To add a user:

1. Sign in as an administrator.
2. Open `/admin/users`.
3. Enter the user's email address.
4. Enter an initial password of at least 12 characters.
5. Select one or more roles.
6. Choose **Create user**.

To change an existing user, use the same page to change whether the account is active and which roles it has, then choose **Save**.

The application prevents the final active administrator from being deactivated or stripped of the `admin` role. User creation and changes are logged by human email address; plaintext passwords and password hashes are not written to those audit messages.

### Which role should I choose?

Use the least-privileged role that matches the person's job.

| Role | Intended use | Main capabilities |
| --- | --- | --- |
| `viewer` | Read-only users | View dashboards, data, analyses, documentation, system state, Federation status, devices, and capabilities; manage their own account password. |
| `operator` | Normal FCP operators | Everything in `viewer`, plus data upload, analyses, workflows, normal runtime controls, and recorder controls. |
| `admin` | FCP/Federation administrators | All permissions, including Federation/provider administration, pairing, software updates, human-user administration, and leader-only management actions when Federation authority also allows them. |

An `operator` is intentionally **not** a Federation administrator. In particular, operator access does not include `federation.manage`, `pairing.manage`, `software.update`, or `users.manage`.

Routes are authorized by permissions rather than by hard-coded role names. The current permissions are:

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

`account.manage` permits an authenticated user to change only their own password. `users.manage` is the separate administrative permission for managing other human users.

## Human accounts versus Federation authority

Human authorization and Federation authorization are separate security layers.

For example, an FCP administrator may be allowed to press a Federation management button in the browser, but the underlying Federation operation still has to pass its device/session authority checks. Human `admin` status does not turn the browser user into a Federation node and does not bypass membership, coordinator, provider, storage, recorder, or contribution policy.

This separation is intentional:

- **human account** — answers who may use a web operation;
- **device identity** — identifies one FCP installation;
- **Federation membership** — determines whether that device belongs to a trusted Federation;
- **Federation/local policy** — determines whether the requested distributed action is actually authorized.

## Authentication data that must be preserved

By default, FCP stores human-account state under `data/auth/`:

```text
data/auth/users.sqlite3
data/auth/flask-secret
data/auth/password-salt
```

On the first production start, FCP creates independent random values for the Flask session secret and password-hashing salt when explicit values are not supplied. Normal starts reuse them.

Back up the authentication database and secrets together with the rest of the persistent FCP state. Do not commit them to Git or copy them into documentation/logs.

Important consequences:

- deleting `users.sqlite3` removes the human-account database;
- changing or deleting `password-salt` can make existing password hashes unusable;
- changing `flask-secret` invalidates existing browser sessions;
- `start.cmd --fresh` resets device/Federation identity but intentionally preserves human accounts and human-auth secrets.

## Configuration

The normal installation needs no authentication environment variables. The defaults are persistent and production-safe for a single FCP data directory.

Advanced deployments can override:

- `FCP_AUTH_DATABASE` — human account database path; default `data/auth/users.sqlite3`;
- `FCP_AUTH_SECRET_DIR` — persistent secret directory; default `data/auth`;
- `FCP_FLASK_SECRET` — explicit Flask session secret, at least 32 characters;
- `FCP_PASSWORD_SALT` — explicit password-hashing salt, at least 32 characters;
- `FCP_SESSION_MINUTES` — authenticated session lifetime in minutes; default `480`;
- `FCP_HTTPS=1` — mark session and remember-me cookies Secure when HTTPS is actually enforced.

Do not set `FCP_HTTPS=1` on a plain-HTTP deployment, because a browser will not send Secure cookies over HTTP.

## Development-only authentication bypass

For local development only, authentication can be disabled with both:

```text
FCP_DEVELOPMENT=1
FCP_AUTH_DISABLED=1
```

`FCP_AUTH_DISABLED=1` is rejected outside development/test operation. Do not use this bypass for a deployed FCP installation.

When development mode is used without explicit secrets, ephemeral development secrets may be generated. Those are not a substitute for persistent production authentication state.

## Sessions and CSRF protection

Human sessions use HttpOnly cookies with SameSite=Lax. Secure cookies are enabled when `FCP_HTTPS=1`. The default session lifetime is eight hours (`480` minutes).

Human authentication and user-administration forms use Flask-WTF CSRF protection. Existing FCP/Federation actions retain their own scoped request validation as well. Hiding a button in the interface is never treated as authorization; permission checks are enforced server-side.

## Upgrading an existing FCP installation

An existing FCP data directory can be upgraded in place:

1. update and start FCP normally;
2. allow FCP to create the persistent authentication secrets and account schema;
3. run `fcp-user create-admin` once if the installation has no human administrator;
4. sign in and continue using the existing device/Federation state.

There is no migration from Federation identities to human accounts because they belong to different security domains.

## Troubleshooting

### I only see the login page and have no account

Create the first administrator from the repository directory:

```bash
docker compose run --rm --no-deps --entrypoint flask flask --app catalog.flask_app.app:create_app fcp-user create-admin
```

Then reload `/login`.

### The administrator command says the account already exists

Do not try to recreate it. Sign in with that account or use another active administrator to manage users from `/admin/users`.

### A user can sign in but receives 403 Forbidden

Check the user's roles at `/admin/users`. A valid login proves identity; it does not imply permission for every FCP operation.

### I reset the device with `start.cmd --fresh` and my user still exists

That is expected. A fresh-device reset replaces device/Federation identity and setup state while preserving human accounts, authentication secrets, recorded data, and other explicitly retained application data.
