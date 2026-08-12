# Human authentication and authorization

FCP web accounts authorize **people**. They are intentionally stored in a dedicated
SQLite database and never reuse Federation node IDs, pairing grants, recorder keys,
device identities, or machine credentials. Existing Federation verification remains
required after a human administrator authorizes an action in the browser.

## Initial setup

1. Start FCP normally with `start.cmd` or `bash start.sh`. On the first production
   start, FCP atomically creates two independent random secrets under
   `data/auth/`: `flask-secret` for sessions and `password-salt` for password
   hashing. They are reused on later starts. Back them up together with the human
   account database and never commit or log them.

   Deployments may instead provide explicit `FCP_FLASK_SECRET` and
   `FCP_PASSWORD_SALT` values. Each explicit value must contain at least 32
   characters. Rotating the password salt invalidates existing password hashes, so
   it must remain stable.
2. Human accounts default to `data/auth/users.sqlite3`. Override this with
   `FCP_AUTH_DATABASE` when required. Secret storage can be moved independently with
   `FCP_AUTH_SECRET_DIR`.
3. After the Flask image has been built, create the first administrator through the
   same containerized runtime used by the supported launcher:

   ```bash
   docker compose run --rm --no-deps --entrypoint flask flask \
     --app catalog.flask_app.app:create_app fcp-user create-admin
   ```

   The command prompts for email and password, requires at least 12 characters, and
   safely refuses a duplicate. It never creates or recreates a default administrator.
4. Open `/login` and sign in. Administrators can manage human accounts at
   `/admin/users`.

For local development only, set `FCP_DEVELOPMENT=1`; missing placeholder secrets are
replaced with ephemeral values. An explicit `FCP_AUTH_DISABLED=1` bypass is accepted
only together with `FCP_DEVELOPMENT=1`; in that mode Flask-Security and its request
hooks are not installed, preserving the legacy development/test surface. Never use
that bypass in a deployment.

## Roles and permissions

The central policy is additive: `viewer` has read access; `operator` adds ordinary
data upload, analysis, workflow, runtime, and recorder controls; `admin` receives all
permissions, including Federation/pairing administration, software updates, and human
user management. Routes check permissions rather than role names.

Permissions are `dashboard.read`, `data.read`, `data.upload`, `analysis.run`,
`workflow.run`, `runtime.control`, `recorder.control`, `federation.read`,
`federation.manage`, `pairing.manage`, `software.update`, and `users.manage`.
`account.manage` permits each authenticated user to change only their own password.

The final active administrator cannot be deactivated or stripped of the admin role.
User creation and changes are logged by human email, without passwords or hashes.

## Sessions, CSRF, and HTTPS

Session cookies are HttpOnly and SameSite=Lax. Remember-me cookies use the same
HttpOnly/SameSite policy, are bounded to seven days, and are Secure whenever
`FCP_HTTPS=1`. Sessions expire after eight hours by default; set
`FCP_SESSION_MINUTES` to change that limit.

Human authentication forms use Flask-WTF CSRF protection. Existing FCP,
Federation, onboarding, recorder, and setup actions retain their established scoped
server-bound CSRF validation; adding human auth does not replace those request
protocols. Human permission checks run server-side and fail closed independently of
which controls are visible in the browser.

Set `FCP_HTTPS=1` only when HTTPS is actually enforced to Flask, or when forwarded
HTTPS is accepted exclusively from a trusted reverse proxy.

## Migration notes

Authentication is enabled by default. Existing `data` mounts can be upgraded in
place: start the new version once to create/persist the auth secrets and schema, then
run the containerized first-admin command above. There is no migration from Federation
identities because those identities belong to a separate security domain.
