# Federation v1 physical robustness campaign

Status: **executable pre-release physical evidence procedure**

This procedure operationalizes the corrected P01-P12 campaign from
`v1_robustness_reconciliation.md`. It supplements the existing CF7 physical
acceptance contract; it does not replace CF7-B and cannot turn CI evidence into
physical evidence.

## Release rule

Run the complete campaign against one exact candidate commit. Do not change
production source, dependencies, runtime composition, or the candidate commit
while the campaign is in progress. If a release-blocking defect requires a code
change, stop the campaign, produce a new candidate, rerun the permanent gates,
and use the physical impact/revalidation policy before reusing any prior
observation.

The campaign harness is:

```text
scripts/acceptance/v1_physical_campaign.py
```

The operator-facing automation layer above it is:

```text
scripts/acceptance/v1_physical_automation.py   # per-assertion automation lane
scripts/acceptance/v1_physical_probes.py       # checked-in read-only probes
scripts/acceptance/v1_physical_runner.py       # orchestrator, report, prepare/verify
```

`docs/implementation/v1_physical_campaign_automation.md` is the machine-by-machine
runbook. Use it to execute the campaign; use this document for the evidence rules
it runs on.

It writes only below the ignored local workspace:

```text
evidence/v1-physical/
```

The final result is derived from checked-in P01-P12 assertions, exact candidate
identity, registered host provenance, required OS boundaries, elapsed-time
requirements, recorded command results, resource samples, and a final privacy
digest. Do not edit the evidence JSON by hand.

## What the harness does and does not do

The harness:

- verifies the checkout is clean and exactly at the supplied 40-character SHA;
- records a hashed host fingerprint rather than the private hostname;
- rejects packets whose candidate, host fingerprint or OS provenance does not
  match the registered campaign host;
- records Windows/POSIX provenance for OS-specific assertions;
- validates host and OS requirements before executing an operator command;
- executes explicit operator-supplied probes without a shell and captures their
  bounded/redacted output;
- records physical observations as immutable portable packets;
- captures host/data/results disk state plus `docker system df` and
  `docker compose ps` when Docker is available;
- enforces the one-hour P07 outage and 24-hour P12 soak durations from recorded
  begin/finish timestamps rather than trusting a claimed elapsed value;
- requires P07/P12 resource samples to belong to the same timed run and host and
  to fall inside its begin/finish interval;
- fails closed on missing/failed assertions;
- refuses preparation or operator-action evidence that carries an assertion
  verdict, so staging a fault-injection case can never satisfy it;
- redacts every string it writes itself, including structured probe detail, to
  the same standard the privacy seal enforces;
- scans evidence for raw private endpoints, addresses, paths, credentials and
  reusable pairing material; and
- binds the final decision to a SHA-256 digest of the reviewed evidence tree.

It deliberately does **not** autonomously fill disks, kill processes, corrupt
files, alter clocks, revoke members, download models, run backups, or power-cycle
hosts. Those actions are
physical test operations and must be selected deliberately for the target test
environment. Use the harness to execute a reviewed fault-injection command or to
record the observed consequence. This prevents a release helper from becoming a
blind destructive test runner.

## Prepare the hosts

Use a fresh clean checkout of the candidate on every physical host. Every host
must declare a profile (`local-ai`, `cnc-recorder`, `school-control`); the
automated runner refuses to probe a host that never declared one.

Windows:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\acceptance\v1_prepare_windows.ps1 `
  -Commit <candidate-sha> `
  -HostId beast `
  -HostProfile local-ai
```

Linux/POSIX:

```bash
bash scripts/acceptance/v1_prepare_linux.sh \
  <candidate-sha> nitro school-control Martin prepare
```

The preparation wrapper initializes the candidate-bound campaign, registers the
host with its profile, captures a pre-campaign P01 resource sample with the P12
soak series attached, and prints the current P01-P12 progress report.
Registration stores only a hashed machine fingerprint; the chosen safe host alias
such as `beast` or `nitro` remains visible for audit readability. A host alias is
permanently bound to one machine fingerprint, one candidate and one profile.

At least one Windows and one POSIX host must be present in the final evidence
set. P01 is explicitly split into Windows and POSIX assertions. P03 pins the
Windows/POSIX launcher assertions to their applicable OS. P06 native-recorder
supervision assertions can only be recorded on Windows.

## Run the automated layer first

Before writing any command by hand, run the checked-in probes:

```bash
bash scripts/acceptance/v1_prepare_linux.sh <candidate-sha> nitro school-control Martin automate

python -m scripts.acceptance.v1_physical_runner report \
  --commit <candidate-sha> --host nitro
```

The report states PASS, FAIL, READY FOR OPERATOR ACTION, MISSING or NOT
APPLICABLE for every P01-P12 assertion and prints the exact next command for each
one. The manual `run`/`observe` commands below remain available for evidence the
checked-in probes cannot express, and for the three human-observation
assertions.

## Inspect the exact scenario contract

Before each scenario:

```bash
python -m scripts.acceptance.v1_physical_campaign \
  plan --scenario P01
```

Omit `--scenario` to print the complete checked-in P01-P12 contract. Assertion
IDs printed by `plan` are the only IDs accepted by `run` and `observe`.

## Record evidence

### Execute a probe or test command

Use `run` when command exit status itself proves the assertion or produces useful
redacted evidence:

```bash
python -m scripts.acceptance.v1_physical_campaign \
  run --commit <candidate-sha> --host nitro --scenario P08 \
  --assertion committed-reads \
  --label "committed read remains available at storage refusal" \
  -- python <reviewed-probe> --read-existing-object
```

The host and assertion OS are validated before the command starts. The command
is then executed directly (`shell=False`). Its command text and output are
redacted before persistence. A zero exit is expected by default; use
`--expect-exit N` when refusal is the expected safe outcome.

### Record an observed physical consequence

Use `observe` when the proof requires a physical action or human/device
observation that cannot be reduced to a local process exit code:

```bash
python -m scripts.acceptance.v1_physical_campaign \
  observe --commit <candidate-sha> --host beast --scenario P05 \
  --assertion relay-crash --status pass \
  --note "relay recovered with bounded restart; Flask remained available"
```

`not-applicable` is accepted only for assertions that the checked-in contract
explicitly marks as conditional. It cannot be used to skip ordinary required
assertions.

### Capture resource state

For ordinary scenarios:

```bash
python -m scripts.acceptance.v1_physical_campaign \
  sample --commit <candidate-sha> --host beast --scenario P01 \
  --label after-third-activation
```

Use additional reviewed OS-specific commands through `run` for measurements the
portable sampler cannot discover directly, such as Windows Docker Desktop VHDX
physical size or product-specific database/history counters.

## P07 and P12 elapsed-time evidence

Start a timed session and retain the returned `run_id`:

```bash
python -m scripts.acceptance.v1_physical_campaign \
  begin --commit <candidate-sha> --host nitro --scenario P07
```

Every resource sample for P07/P12 must carry that active run ID:

```bash
python -m scripts.acceptance.v1_physical_campaign \
  sample --commit <candidate-sha> --host nitro --scenario P07 \
  --run-id <run-id> --label outage-midpoint
```

A timed sample is rejected if the run is missing, belongs to another host, or has
already finished. During final status calculation, only samples whose timestamp
falls between that run's matching begin/finish packets count toward the timed
scenario.

After at least one real hour:

```bash
python -m scripts.acceptance.v1_physical_campaign \
  finish --commit <candidate-sha> --host nitro --scenario P07 \
  --run-id <run-id>
```

P12 uses the same flow but cannot pass until a real 24 hours has elapsed. The
24-hour soak must start with meaningful history and must also include the
accelerated ceiling-crossing assertion; an empty idle installation does not
satisfy P12.

## Multi-host evidence

Observation filenames contain a high-resolution UTC timestamp and random suffix,
so packets are portable and collision-resistant. Each host may collect evidence
in its own local checkout. Before final validation, copy these directories from
each host into one coordinator evidence tree, preserving paths and merging rather
than replacing files:

```text
evidence/v1-physical/hosts/
evidence/v1-physical/observations/
```

Do not copy raw logs, private endpoints, pairing codes, credentials, database
files, screenshots containing secrets, or unrestricted service output into this
tree. If richer evidence is required, reduce/redact it first and record the
result through the harness. Imported packets are rejected if their candidate SHA,
host fingerprint, or OS provenance does not match the registered host evidence.

## Campaign status

```bash
python -m scripts.acceptance.v1_physical_campaign \
  status --commit <candidate-sha>
```

A scenario reports the exact missing assertions, failed assertions, elapsed time
and qualifying resource-sample count. A later failed observation for an assertion
supersedes an earlier pass, so a discovered regression cannot be hidden by stale
evidence.

## Privacy seal and final validation

After the final observation and after multi-host evidence is merged:

```bash
python -m scripts.acceptance.v1_physical_campaign \
  privacy --commit <candidate-sha>

python -m scripts.acceptance.v1_physical_campaign \
  validate --commit <candidate-sha>
```

The privacy command rejects raw HTTP/WS endpoints, IPv4 addresses, local absolute
paths, credential-like material and reusable `FCP1-...` pairing material, then
writes a SHA-256 digest over the reviewed evidence. Adding or changing any
evidence afterward invalidates that seal and makes final validation fail until
privacy review is repeated.

`validate` exits `0` only when:

- the checkout still equals the candidate and is clean;
- both Windows and POSIX evidence are present;
- every required P01-P12 assertion is currently passing (or explicitly allowed
  as not applicable);
- P07 contains one matching-host timed session with at least one real hour and
  the required in-window resource samples;
- P12 contains one matching-host timed session with at least 24 real hours and
  the required in-window resource samples; and
- the privacy digest still matches the complete evidence tree.

A successful robustness validation is still only one side of release acceptance.
The separate CF7 physical evidence document must also validate, including the
real desktop/mobile browser observation. Only after both evidence contracts are
complete should the acceptance flags be changed in a separate evidence-backed
review.
