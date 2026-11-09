# Nitro: read-only replay of historical MTConnect telemetry

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
