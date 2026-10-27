# Self-hosted workflow runner prerequisites

All software CI, including the Federation v1 software release gate and physical-
campaign tooling checks, uses the shared self-hosted pools:
`[self-hosted, Linux, X64, fcp-linux-fast]` and
`[self-hosted, Windows, X64, fcp-windows]`. Jobs queue when their matching pool
is unavailable. Workflow routing does not select a named physical machine or
fall back to hosted runners. The physical acceptance campaign itself remains
bound to its registered real test hosts.

Branch pushes in the trusted repository provide checks for the exact branch head,
including same-repository pull requests. The checked-in definitions have no
fork pull-request trigger. An independent GitHub approval restriction is still
required because a fork can modify those definitions before execution; see the
[migration security prerequisite](../../SELF_HOSTED_RUNNER_MIGRATION.md#persistent-runner-security-and-isolation).
A maintainer must review external changes before placing them on a trusted
repository branch. No checked-in workflow executes code through
`pull_request_target`.

Windows requires Python 3.12.10 on the runner account's PATH, Windows PowerShell
5.1, Git for Windows including Bash, and Docker CLI/Compose. Linux requires the
qualified Python 3.12.13 installation at
`/opt/fcp-ci/python/3.12.13/bin/python3.12`, Git, Docker/Compose/Buildx, and Docker
access for the runner account. `actions/setup-go` retains the version in
`cmd/fcp-peer-sidecar/go.mod`.

Before creating the virtual environment, the action probes SQLite's `->` and
`->>` JSON operators and expression indexes in a fresh process of the exact
pinned interpreter. It logs the Python/SQLite versions, extension module and
loaded library paths. Capable runtimes require no additional download or build.
An older Linux SQLite runtime triggers a job-local build of the official SQLite
3.54.0 amalgamation, verified against its published SHA3-256 checksum, using an
already installed `cc` or `gcc`. The source and shared library stay inside the
unique job directory. A fresh probe must prove that the same pinned interpreter
loads that exact library and supports the required features before the action
exports a job-only `LD_LIBRARY_PATH` prefix. Inherited library paths and
`LD_PRELOAD` remain intact. A missing compiler or failed verification fails the
job without installing packages or changing runner services. Windows only
probes its existing runtime. The post-job cleanup also removes these optional
build files.

The Python action checks the exact platform version and creates a new virtual
environment under a unique directory inside the job's temporary directory.
Existing workflow dependency and tool pins remain authoritative. Linux test
temporary files use disk storage inside that unique directory; Windows uses a
unique short child of `C:\fcp-qtmp`, with the extended path form only for the
release and broad capability groups that require it. A nested JavaScript
action registers an unconditional job-completion cleanup before Python setup
starts; it removes only the recorded job directories, including on setup or
test failure. Shared parent directories are never removed. Native Windows PowerShell modules and Git
Bash are made available explicitly. CI wrapper scripts set their execution
policy for that PowerShell process only; machine and user policy are unchanged.
The Windows F8.5 job runs its three read-only SQLite assertions in a separate
ordinary-path step because SQLite rejects extended-path URI authorities. The
remaining F8.5 tests retain extended paths for nested artifact fixtures; every
original test remains required, and Linux keeps the original combined suite.
No acceptance threshold changes.

The PostgreSQL action creates a job-owned PostgreSQL 16 container with an
ephemeral data directory and an automatically allocated loopback port. Every
consumer has an unconditional cleanup step that verifies the job ownership
label before stopping that container. Existing databases and product services
are not used or removed.

The storage precondition only checks the product's unchanged admission floor;
it never removes installed SDKs or changes services. Demonstration Compose
rebuilds the current source and creates the evidence directory as the runner
account before mounting it. The
container retains its documented user so application result directories remain
writable; its readable JSON output can be removed by the runner because the
parent directory is runner-owned. This avoids stale images and root-owned mount
directories on a persistent runner.

Passing software gates does not replace the physical campaign. Freeze only the
actual merged commit after all required jobs and the release verdict pass.
