# Federation release runner prerequisites

The required release and acceptance workflows use `Beast` (`self-hosted`,
`fcp-windows`) and `Beast-Linux-WSL` (`self-hosted`, `fcp-linux-fast`). Nitro is
reserved for physical acceptance.

Windows requires Python 3.12.10 on the runner account's PATH, Windows PowerShell
5.1, Git for Windows including Bash, and Docker CLI/Compose. Linux requires the
qualified Python 3.12.13 installation at
`/opt/fcp-ci/python/3.12.13/bin/python3.12`, Git, Docker/Compose/Buildx, and Docker
access for the runner account. `actions/setup-go` retains the version in
`cmd/fcp-peer-sidecar/go.mod`.

The Python action checks the exact platform version and creates a new virtual
environment under the job's temporary directory. Existing workflow dependency
and tool pins remain authoritative. Linux test temporary files use disk storage;
Windows uses the short `C:\fcp-qtmp` path, with the extended path form only for the
release and broad capability groups that require it. Native Windows PowerShell modules and Git
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

Hosted Ubuntu SDK cleanup is disabled on self-hosted runners; the product's own
storage precondition still runs. ICSE Compose rebuilds the current source and
writes evidence as the runner account to avoid stale images or root-owned files
on a persistent runner.

Passing software gates does not replace the physical campaign. Freeze only the
actual merged commit after all required jobs and the release verdict pass.
