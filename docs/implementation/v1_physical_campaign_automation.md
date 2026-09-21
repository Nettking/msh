# Federation v1 physical campaign — machine-by-machine automation

Status: **operator runbook for the automated P01–P12 layer**

This document is the machine-by-machine procedure. The evidence rules it runs on
are unchanged and still live in:

- `docs/implementation/v1_physical_campaign.md` — the evidence contract, privacy
  seal and multi-host merge; and
- `docs/implementation/v1_physical_campaign_strict.md` — the strict same-run
  P07/P12 binding used for the release decision.

Nothing here weakens those. The automation layer only removes hand-written
commands and hand-interpreted status from the parts that can be measured.

## What is automated and what is not

Every P01–P12 assertion is classified in one checked-in lane. Print the whole
classification with:

```bash
python -m scripts.acceptance.v1_physical_runner classify
python -m scripts.acceptance.v1_physical_runner classify --scenario P05
```

| Lane | Meaning | Operator work |
| --- | --- | --- |
| `automated-probe` | A checked-in probe proves the assertion on the target host. | Run one command. |
| `operator-fault-injection` | PREPARE and VERIFY are automated; the fault itself is a deliberate physical action. | PREPARE → do the fault → ACTION → VERIFY. |
| `human-observation` | The proof needs a person, a real browser/device, or an external platform boundary. | Observe, then record through the base harness. |

The current split is 27 fully automated assertions, 76 with automated
preparation and automated consequence verification around one explicit operator
action, and 3 that genuinely require a human, a browser, or a platform surface
this harness must not simulate (`P06/service-manager-boundary`,
`P10/browser-install`, `P11/windows-dpapi`).

The runner **never performs a fault**. It never fills a disk, kills a process,
corrupts a file, changes a clock, revokes a member, downloads a model, or runs a
backup. Where a narrowly scoped repository helper exists for an action, PREPARE
prints its command for the operator to review and run; it is not executed for
you.

List the checked-in probes and what each one accepts:

```bash
python -m scripts.acceptance.v1_physical_runner probes
```

A probe returns `pass`, `fail`, or `unavailable`. **`unavailable` never becomes a
pass.** If a probe cannot see its surface — no Docker, no recorder corpus, an
unsupported checkout shape — the assertion stays missing and the runner exits
non-zero.

## Step 1 — prepare each physical host

Use a fresh clean checkout of the exact candidate on every host. Each host must
declare a profile (`local-ai`, `cnc-recorder`, `school-control`); the runner
refuses to probe a host that never declared one.

Windows (Beast):

```powershell
powershell -ExecutionPolicy Bypass -File scripts\acceptance\v1_prepare_windows.ps1 `
  -Commit <candidate-sha> -HostId beast -HostProfile local-ai -Action prepare
```

Linux/POSIX (Nitro and other hosts):

```bash
bash scripts/acceptance/v1_prepare_linux.sh <candidate-sha> nitro school-control Martin prepare
```

`prepare` initializes the candidate-bound campaign, registers this host with its
hashed machine fingerprint and profile, records a baseline resource sample with
the P12 soak series attached, and prints the progress report.

## Step 2 — run every automated probe this host may run

```powershell
powershell -ExecutionPolicy Bypass -File scripts\acceptance\v1_prepare_windows.ps1 `
  -Commit <candidate-sha> -HostId beast -HostProfile local-ai -Action automate
```

```bash
bash scripts/acceptance/v1_prepare_linux.sh <candidate-sha> nitro school-control Martin automate
```

This walks every non-timed scenario and runs only the automated assertions this
host's OS and profile allow. Windows-only assertions are skipped on POSIX and
vice versa, and the skip is reported explicitly rather than silently passing.

A non-zero exit here is normal while physical work is still outstanding.

To run one scenario or one assertion directly:

```bash
python -m scripts.acceptance.v1_physical_runner scenario \
  --commit <candidate-sha> --host nitro --scenario P03

python -m scripts.acceptance.v1_physical_runner probe \
  --commit <candidate-sha> --host nitro --scenario P11 \
  --assertion external-destination --option destination=/mnt/backup-target
```

Only options a probe declares are accepted; anything else is refused before the
probe runs. Options are validated against a safe character set and are never
passed through a shell.

A few assertions name a subject only you can supply, and are refused until you
do. `runner report` prints them per row as `required_options`, and `automate`
reports them as not run rather than skipping them silently:

| Assertion | Required option | Why |
| --- | --- | --- |
| `P01/windows-three-activations`, `P01/posix-three-activations` | `activations` | Only the operator knows how many supported activations were completed. |
| `P11/external-destination`, `P11/successful-backup`, `P11/isolated-restore` | `destination` | The independent backup destination is an operator choice. |
| `P11/sqlite-integrity`, `P11/successful-backup`, `P11/isolated-restore` | `path` | Integrity must be checked against the *restored* copy, never the live data directory. |

## Step 3 — work the report

```bash
python -m scripts.acceptance.v1_physical_runner report \
  --commit <candidate-sha> --host nitro
```

The report is machine-readable and gives one state per assertion:

| State | Meaning |
| --- | --- |
| `pass` | Recorded and currently passing. |
| `fail` | Recorded and currently failing. |
| `ready-for-operator-action` | PREPARE recorded; the physical action and VERIFY are outstanding. |
| `missing` | No evidence yet. |
| `not-applicable` | Recorded N/A where the contract explicitly permits it. |

Each row carries `next_step`: the exact command to run next for that assertion on
that host. Work the report top to bottom; there is nothing to interpret.

For P07 and P12 the report is computed from the **single strict session** that
the release decision would use, so a `pass` in the report can never come from a
different outage, a different soak, or a different host.

## Step 4 — fault-injection assertions

Fault injection is three explicit steps.

**PREPARE** — records the staged state and prints the exact physical action:

```bash
python -m scripts.acceptance.v1_physical_runner prepare \
  --commit <candidate-sha> --host nitro --scenario P05 --assertion relay-crash
```

The output contains `prepare_id`, `operator_action`, any `reviewed_helper`, and
`harness_performs_this_action: false`. The preparation packet carries no verdict
at all: running PREPARE can never satisfy the assertion.

**OPERATOR ACTION** — perform the physical fault yourself, then attest it:

```bash
python -m scripts.acceptance.v1_physical_runner action \
  --commit <candidate-sha> --host nitro --scenario P05 --assertion relay-crash \
  --prepare-id <prepare-id> --note "killed the relay process; supervision restarted it"
```

**VERIFY** — runs the consequence probes and records the verdict:

```bash
python -m scripts.acceptance.v1_physical_runner verify \
  --commit <candidate-sha> --host nitro --scenario P05 --assertion relay-crash \
  --prepare-id <prepare-id>
```

VERIFY refuses unless a matching PREPARE **and** a matching OPERATOR ACTION exist
for this candidate, this host and this assertion, and the action was recorded
after its preparation. If any verify probe returns `unavailable`, nothing is
recorded and the case stays open.

## Step 5 — timed P07 and P12 evidence

The timed rules are unchanged: P07 needs one real hour, P12 needs one real 24
hours, and every assertion must belong to the same begin/finish run on the same
host.

```bash
# Start the run and keep the run id.
python -m scripts.acceptance.v1_physical_campaign begin \
  --commit <candidate-sha> --host nitro --scenario P07

# Samples and probes during the run must carry it.
python -m scripts.acceptance.v1_physical_runner sample \
  --commit <candidate-sha> --host nitro --scenario P07 \
  --run-id <run-id> --label outage-midpoint

python -m scripts.acceptance.v1_physical_runner probe \
  --commit <candidate-sha> --host nitro --scenario P07 \
  --run-id <run-id> --assertion aged-corpus

python -m scripts.acceptance.v1_physical_runner prepare \
  --commit <candidate-sha> --host nitro --scenario P07 \
  --run-id <run-id> --assertion capture-continues

# After the real minimum duration.
python -m scripts.acceptance.v1_physical_campaign finish \
  --commit <candidate-sha> --host nitro --scenario P07 --run-id <run-id>
```

The runner refuses a timed assertion without `--run-id`, refuses a `--run-id` on
a non-timed scenario, and refuses a run that belongs to another host or has
already been finished. All timed verdicts are written through the strict layer,
so they stay bound to that one run.

`runner sample` is the sampling command to use for P12: it records the same
resource snapshot as the base harness plus the recorder, publication, history,
orphan and CPU/RAM series the P12 series assertions require. The series
assertions then check the recorded samples rather than trusting a claim.

P12 resource samples must also use an explicit runtime binding. Set
`FCP_RUNTIME_BINDING` for the platform wrapper, or pass the binding before the
subcommand when invoking the runner directly:

```bash
python -m scripts.acceptance.v1_physical_runner \
  --runtime-binding /absolute/path/p12-runtime-binding.json \
  sample --commit <candidate-sha> --host nitro --scenario P12 \
  --run-id <run-id> --label soak-sample
```

The binding must name both the deployed `runtime.data_root` and
`runtime.results_root`, and it must pin the candidate and clean acceptance
harness. An unbound checkout sample is refused because it cannot establish the
growth surface for the runtime under test. This does not change the growth
ceiling or any elapsed-time requirement.

The POSIX and Windows preparation wrappers accept optional sample arguments
after the action: `sample-scenario` and `sample-run-id`. To sample the existing
P12 session without starting a new timed run, set `FCP_RUNTIME_BINDING` and run
`bash scripts/acceptance/v1_prepare_linux.sh <candidate-sha> nitro school-control Martin sample P12 <run-id>`
or the equivalent PowerShell command with `-Action sample -SampleScenario P12
-SampleRunId <run-id>`. The wrapper forwards the explicit run ID and binding to
the checked-in runner; it does not create or restart a session.

Growth probes count each backing filesystem once. Samples retain each measured
root and an opaque filesystem alias, so `data` and `results` on one volume share
one measurement while distinct volumes remain additive. Missing identities,
contradictory aliases, or changes to the measured roots, volumes or capacities
make growth evidence unavailable. Older samples without identities cannot prove
growth; retain them and collect fresh evidence with the pinned harness revision.
The hourly and per-activation ceilings are unchanged.

## Step 6 — human-observation assertions

Three assertions have no probe. Record them through the base harness after the
real observation:

```bash
python -m scripts.acceptance.v1_physical_campaign observe \
  --commit <candidate-sha> --host beast --scenario P10 \
  --assertion browser-install --status pass \
  --note "installed the required model from a real desktop browser and watched it complete"
```

`classify` prints the recorded rationale for why each of these cannot be
automated. `not-applicable` remains accepted only where the checked-in contract
explicitly permits it.

## Step 7 — merge, seal and validate

Multi-host merge, the privacy seal and the release decision are unchanged. After
copying `evidence/v1-physical/hosts/` and `evidence/v1-physical/observations/`
from every host into one coordinator tree:

```bash
python -m scripts.acceptance.v1_physical_campaign privacy --commit <candidate-sha>
python -m scripts.acceptance.v1_physical_campaign_strict validate --commit <candidate-sha>
```

Everything the harness writes itself — including structured probe detail — is
redacted before it reaches the evidence tree, and privacy sealing remains the
backstop for anything copied in by hand. Probe detail never carries a raw source
name, hostname, endpoint, address, local path, credential or pairing code:
private identifiers are reduced to stable non-reversible aliases.

## What the automation still cannot do

After this layer, the remaining work genuinely requires one of:

- **the physical machines** — Beast/Nitro Docker Desktop layouts, real recorder
  sources, real backup destinations;
- **elapsed wall-clock time** — the real P07 hour and the real P12 24 hours;
- **browser or device observation** — `P10/browser-install`, and the separate
  CF7 real-browser evidence requirement;
- **deliberate fault injection** — filling disks, killing processes, interrupting
  builds and durable writes, moving clocks, removing the coordinator, and
  quiescing the host for backup and restore; or
- **an external platform boundary** — `P06/service-manager-boundary` and
  `P11/windows-dpapi`.

Passing strict P01–P12 validation is still only one side of release acceptance.
The separate CF7 physical evidence document must also validate, including the
real desktop/mobile browser observation, before any acceptance flag is changed in
a separate evidence-backed review.
