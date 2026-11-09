# Nitro: read-only replay of historical MTConnect telemetry

## Executed archive experiment (2026-11-09)

The balanced experiment supersedes the legacy file-local pilot below. Ordinary
`ssh nitro` access worked with the existing key. The separate research checkout
is `/home/martin/dt-state-evolution-20261109`; no existing checkout was changed.
Source was transported by Git bundles over SSH, without transferring telemetry.
The closed, verified input is
`/home/martin/msh-recordings-archive/verified-four-source-compressed-20261005-06-20261029T092350Z/recordings.tar.gz`.
The archive reader does not extract members. Hashes and exact source-line ranges
are in `DATASET_MANIFEST.json`; execution revision is in `MEASUREMENTS.json`.

Nitro's default Python 3.14 cannot install the full release-pinned binary
dependencies. The existing `/home/martin/.local/bin/python3.12` is Python
3.12.13 and supports them. Use it in a research-only venv. For a fresh separate
clone of the research branch, reproduce with:

```bash
git checkout --detach e6f199be9e5917f5ae5e66f94e17e2fcff6ea2ab
printf '\n.venv*/\nresults*/\n' >> .git/info/exclude
/home/martin/.local/bin/python3.12 -m venv .venv312
.venv312/bin/python -m pip install -c constraints-release.txt -r requirements.txt pytest
.venv312/bin/python -m pip check
.venv312/bin/python -m research.state_semantics_evolution_2026.validate --output results/TEST_RESULTS.json
.venv312/bin/python -m pip freeze > results/DEPENDENCIES.txt
nice -n 10 .venv312/bin/python -m research.state_semantics_evolution_2026.execute \
  --archive /home/martin/msh-recordings-archive/verified-four-source-compressed-20261005-06-20261029T092350Z/recordings.tar.gz \
  --windows-per-machine 6 --target-records 200 --output-dir results
```

Proceed to evaluation only after validation returns zero. The manifest is
written before transformations; unexpected scenario outcomes produce a nonzero
exit while retaining measured evidence. Existing production code, raw data,
services and recorder state are unchanged. Raw canonical records remain on
Nitro in memory or temporary replay files. Export only the named summary files
`DATASET_MANIFEST.json`, `MEASUREMENTS.json`, `TEST_RESULTS.json`, and
`DEPENDENCIES.txt` from the new research results directory.

Once those files are in the research directory, generate the report locally:

```bash
python -m research.state_semantics_evolution_2026.report \
  --measurements research/state_semantics_evolution_2026/MEASUREMENTS.json \
  --manifest research/state_semantics_evolution_2026/DATASET_MANIFEST.json \
  --tests research/state_semantics_evolution_2026/TEST_RESULTS.json \
  --output research/state_semantics_evolution_2026/EXPERIMENT_RESULTS.md
```

The report generator checks the frozen manifest hash and reads no telemetry.
The detached revision above reproduces the exact executed experiment. The
reporting helpers were added in the later delivery commit; run the report
command from that delivery revision on the workstation after copying summaries.
The remaining sections describe the older exploratory `replay.py` pilot. Its
file-local sampler is not the selection procedure used in the archive study.

This is an operator-executed runbook for `Nettking/msh` branch
`research/state-semantics-evolution-2026`. Last prepared: 2026-11-09.

**Important:** The GitHub Actions label `fcp-linux-fast` is a CI pool, not a
guarantee the job executes on Nitro. The restricted `fcp-archive` Nitro SSH
account and its `/srv/fcp-artifacts` directory are for archival evidence,
**not** for telemetry research or an interactive shell. Use your authorised
ordinary SSH login, not any CI/archive credential.

The code does not contact CNC equipment or operate a recorder. No raw machine
data are committed to GitHub.

## 1. First check from your computer

Substitute the real SSH alias or destination. **Do not send SSH keys, passwords,
private IP addresses, or access tokens to the assistant.**

```bash
ssh <nitro-ssh-alias> 'uname -s; python3 --version; command -v git'
```

This only checks shell, Python and Git. If SSH fails, solve host/identity/VPN
connectivity before proceeding. If Python is missing, stop; do not install
system-wide packages on Nitro as part of the research test.

## 2. Use a separate research checkout on Nitro

If Nitro already has GitHub read access for this private repository:

```bash
ssh <nitro-ssh-alias>
mkdir -p "$HOME/dt-state-evolution"
git clone --single-branch --branch research/state-semantics-evolution-2026 \
  git@github.com:Nettking/msh.git \
  "$HOME/dt-state-evolution/msh"
cd "$HOME/dt-state-evolution/msh"
python3 -m venv "$HOME/dt-state-evolution/venv"
. "$HOME/dt-state-evolution/venv/bin/activate"
python -m pip install -c constraints-release.txt numpy pandas pytest
```

If GitHub private-repo credentials are unavailable on Nitro, **do not paste
a token in a command or install the archive SSH key**. Instead transport a
reviewed source-code checkout through your existing authorised workstation
access; raw telemetry need not move. Use a different destination if the
example directory already exists. Do not run `git checkout` or `git pull`
inside Nitro's active FCP production checkout.

## 3. Verify the research package before touching historical data

```bash
python -m pytest -o addopts= -q \
  research/state_semantics_evolution_2026/test_evolution.py \
  research/state_semantics_evolution_2026/test_replay.py
```

If any test fails, stop and save the redacted traceback. Do not redefine the
expected result merely to obtain a pass.

## 4. Find a small permitted telemetry subtree

The operator must supply the **actual** readable directory containing JSONL
recordings. It has not been inferred from the archive location or from GitHub.

```bash
# Replace this path; confirm it is telemetry, not the archive.
DATA_ROOT="/path/to/authorized/jsonl-subtree"
test -d "$DATA_ROOT" && test -r "$DATA_ROOT" || {
  echo "Telemetry subtree unavailable"; exit 2;
}
```

Prefer one machine/day or a few daily directories rather than scanning the
entire 400+ GB archive in the first pilot. Avoid a symlink pointing into
restricted archive or live runtime state.

## 5. Run the bounded historical experiment on Nitro

```bash
mkdir -p "$HOME/dt-state-evolution/results"
python -m research.state_semantics_evolution_2026.replay \
  --source-root "$DATA_ROOT" \
  --max-windows 20 \
  --window-records 200 \
  --max-scan-records 200000 \
  --output "$HOME/dt-state-evolution/results/pilot.json"
```

This reads only complete JSONL records through the repo's normal reader,
replays them in isolated temporary files, and writes aggregate metrics,
transformation verdicts, trace hashes and pseudonymous machine identifiers.
The first complete windows are **not random or representative**. The program
exits nonzero if E1–E3 semantic equivalence fails or no full window was
selected; this is not by itself a physical fault-detection finding.

**Data exposure warning:** Report metadata still contains hashes and aggregate
results derived from the corpus. Review the report for institutional sharing
restrictions before copying it off Nitro. Keep output local if not approved.

## 6. Move only an authorised results summary

If permitted by your data agreement, from your workstation:

```bash
scp <nitro-ssh-alias>:dt-state-evolution/results/pilot.json ./pilot.json
```

Never `scp` the raw JSONL corpus to GitHub or into a chat. For a diagnosis
that avoids transferring even summary records, report the command exit code,
window count, equivalence failure count, and redacted error message.

## 7. Interpretation

A positive E1–E3 invariance result supports *behavioral preservation* under
specific source transformations; it does not validate correctness of machining
state. E4–E5 are controlled perturbations, and E6 is an intended change of
threshold semantics. Current classifier output means **active/non-active**,
not stable/unstable cutting.

Scientific novelty remains open: ordinary metamorphic/regression tests are the
strong baseline. Only build an evolution-specific procedure if it can be shown
to add independent, reproducible value over the baseline.
