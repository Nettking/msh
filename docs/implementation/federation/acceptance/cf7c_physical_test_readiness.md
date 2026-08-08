# CF7-C physical test readiness

Status: repository-side preparation for physical CF7 testing. This tooling does not claim that physical acceptance has passed and does not change production runtime or authority.

The authoritative physical-evidence policy is CF7-B `physical-evidence.v2`: a new candidate does not automatically reset every physical observation. Use the impact planner first, carry forward only observations it explicitly authorizes, and rerun the impacted or missing observations on the candidate.

## Test topology

Use the three available machines as follows:

| Profile | Physical machine | Primary roles |
| --- | --- | --- |
| `local-ai` | Local Windows machine with the NVIDIA GPU and Ollama | language-model contribution and desktop browser |
| `cnc-recorder` | Machine on the MSH network with access to both CNC MTConnect Agents | recorder contribution and two real MTConnect sources |
| `school-control` | Machine at Høgskolen i Østfold | control node, registered compute seam and storage candidate |

At least one of `cnc-recorder` and `school-control` must be a physical Linux host. WSL may support preparation but does not replace required physical Linux evidence.

Validate the local topology using:

```text
ops/cf7_physical_topology.template.json
```

```bash
python -m ops.cf7_physical_readiness topology cf7-topology.local.json
```

The local topology file and all evidence remain ignored by Git.

## Readiness runner

`ops/cf7_physical_readiness.py` provides:

1. `init` — create the ignored evidence directory and bind a new campaign to one candidate commit.
2. `preflight` — verify commit, clean checkout, Python, Docker, and the machine-specific physical seam.
3. `gate` — run the CF7 acceptance tests, integrated capability routes, Compose validation, and diff hygiene, then write a redacted command summary.
4. `validate` — retain the strict direct validator path for all-observed evidence.

For v2 evidence containing `carried-forward` records, use the canonical `physical_evidence` CLI documented below; it performs the required Git ancestry and impact analysis.

Private endpoint values are read only from process environment variables. Readiness summaries deliberately retain `"accepted": false`; only the complete CF7-B evidence document may report acceptance.

## Before testing a new candidate

First calculate what actually requires another physical observation:

```bash
python -m catalog.federation.tests.cf7_acceptance.physical_revalidation \
  --baseline <PREVIOUS_OBSERVED_COMMIT> \
  --candidate <CANDIDATE_COMMIT> \
  --repo-root .
```

The result names:

- scenarios that must be rerun;
- scenarios that may be carried forward;
- unknown paths, which fail closed and force a full rerun set.

Do not repeat an unaffected physical scenario merely because the candidate SHA changed. Do not carry a scenario forward unless the planner authorizes it.

For every machine/scenario that **does** require a new observation, use a clean checkout at the candidate commit:

```bash
git clone https://github.com/Nettking/msh.git msh-cf7-physical
cd msh-cf7-physical
git checkout <CANDIDATE_COMMIT>
python -m pip install --upgrade pip
python -m pip install -r requirements.txt -c constraints-phase2.txt
python -m pip install pytest ruff
```

Docker must be available from the same terminal. Do not modify the checkout except under the ignored `evidence/` directory and ignored local topology file.

## Local AI machine

When an AI/GPU-related scenario requires a rerun:

```powershell
ollama pull llama3.2:3b
$env:MSH_CF7_OLLAMA_URL = "http://<PRIVATE_OLLAMA_HOST>:11434"
$env:MSH_CF7_OLLAMA_MODEL = "llama3.2:3b"
$commit = (git rev-parse HEAD).Trim()
.\ops\cf7_prepare_windows.ps1 `
  -Machine local-ai `
  -Commit $commit `
  -Operator Martin `
  -Action all
```

The preflight verifies bounded Ollama inventory and model presence. It does not run the physical product benchmark or mark it passed.

## CNC recorder machine

When recorder/MTConnect-related scenarios require a rerun, configure the physical sources only through environment variables.

Linux example:

```bash
export MSH_CF7_MTCONNECT_ENDPOINTS='{
  "cnc-one":"http://<PRIVATE_AGENT_ONE>:5000",
  "cnc-two":"http://<PRIVATE_AGENT_TWO>:5000"
}'
commit="$(git rev-parse HEAD)"
bash ops/cf7_prepare_linux.sh cnc-recorder "$commit" Martin all
```

Windows example:

```powershell
$env:MSH_CF7_MTCONNECT_ENDPOINTS = '{
  "cnc-one":"http://<PRIVATE_AGENT_ONE>:5000",
  "cnc-two":"http://<PRIVATE_AGENT_TWO>:5000"
}'
$commit = (git rev-parse HEAD).Trim()
.\ops\cf7_prepare_windows.ps1 `
  -Machine cnc-recorder `
  -Commit $commit `
  -Operator Martin `
  -Action all
```

The preflight performs bounded `/probe` reads. It does not enable recording.

## School control machine

When multi-device/compute/storage scenarios require a rerun:

Linux example:

```bash
export MSH_CF7_PEERS='{
  "local-ai":"<PRIVATE_AI_HOST>:11434",
  "cnc-recorder":"<PRIVATE_RECORDER_HOST>:5000"
}'
commit="$(git rev-parse HEAD)"
bash ops/cf7_prepare_linux.sh school-control "$commit" Martin all
```

Windows example:

```powershell
$env:MSH_CF7_PEERS = '{
  "local-ai":"<PRIVATE_AI_HOST>:11434",
  "cnc-recorder":"<PRIVATE_RECORDER_HOST>:5000"
}'
$commit = (git rev-parse HEAD).Trim()
.\ops\cf7_prepare_windows.ps1 `
  -Machine school-control `
  -Commit $commit `
  -Operator Martin `
  -Action all
```

Peer probes prove bounded TCP reachability only. They do not enroll providers, assign storage, register handlers, or grant Federation trust.

## Execution order

### 1. Freeze the candidate

- Freeze one exact 40-character candidate commit.
- Confirm required CF7 and broad Federation workflows are green on that commit.
- Do not change the candidate while collecting new observations for that candidate.

### 2. Compute the revalidation set

For each previously observed record, compare its `observed_commit` with the candidate through `physical_revalidation.py`.

- `carried-forward`: retain the old observation and evidence reference only when authorized.
- `observed`: rerun on the candidate when impacted or missing.
- unknown path or ancestry failure: stop; do not carry forward.

### 3. Prepare only machines needed for new observations

Run `init`, `preflight`, and `gate` on required physical Windows/Linux hosts when their fresh-checkout or machine-specific observation is being rerun. Preserve redacted summaries from clean runs.

A previously passed physical OS/fresh-checkout record can itself be carried forward only when the impact policy authorizes the corresponding fresh-checkout scenario.

### 4. Start the supported product where needed

Windows:

```cmd
start.cmd
```

Linux:

```bash
docker compose up -d --build relay ollama recorder flask
```

Install the configured model when required:

```bash
docker compose --profile model-install run --rm ollama-pull
```

Open the product over the approved private network path. Do not record private endpoints in evidence.

### 5. Complete only the required physical observations

Depending on the planner output, this may include:

- authenticated multi-host pairing and cross-host status;
- both CNC MTConnect sources and explicit recorder enable/disable;
- Ollama benchmark on the target accelerator;
- simultaneous recorder and AI contribution;
- separate registered-compute and storage-candidate paths;
- benchmark expiry/invalidation and explicit rerun;
- contribution disable/re-enable;
- restart, trusted reconnect, and contribution reconciliation;
- revocation, fencing, and controlled rejoin;
- desktop/mobile Federation and repair states.

Benchmarks and contributions remain optional for normal setup, but required when a named acceptance scenario calls for them.

### 6. Record provenance

For a new observation:

```text
provenance = observed
observed_commit = <candidate commit>
```

For an authorized carried observation:

```text
provenance = carried-forward
observed_commit = <commit where it was physically observed>
```

Never rewrite `observed_commit` to make old evidence look as though it was produced on the candidate.

### 7. Redact and validate evidence

Store local summaries, screenshots, and observations only under `evidence/`. Never include raw IP addresses, URLs, credentials, invitation/enrollment material, private hostnames, absolute paths, or database locations.

Linux:

```bash
commit="$(git rev-parse HEAD)"
python -m catalog.federation.tests.cf7_acceptance.physical_evidence \
  evidence/cf7-physical-evidence.json \
  --commit "$commit" \
  --repo-root .
```

Windows:

```powershell
$commit = (git rev-parse HEAD).Trim()
python -m catalog.federation.tests.cf7_acceptance.physical_evidence `
  evidence\cf7-physical-evidence.json `
  --commit $commit `
  --repo-root .
```

Successful final validation prints `"accepted":true` plus observed and carried-forward record counts. Readiness, green CI, probes, or screenshots alone are insufficient.

## Stop conditions

Stop rather than accept when:

- an impacted scenario is marked carried-forward;
- an unknown changed product path exists;
- a carried observation's commit is not an ancestor of the candidate;
- required Git history is unavailable for impact analysis;
- a newly required OS observation is represented only by CI, container, or WSL;
- required MTConnect sources are unavailable or invalid;
- the target Ollama model cannot run when its scenario is required;
- private addresses would need to be committed;
- storage self-assigns authority;
- benchmark evidence grants authority;
- contribution disablement removes membership or unrelated contributions;
- revocation fails to fence future use;
- evidence cannot distinguish physical behavior from fixtures or loopback;
- any authority, privacy, data-loss, platform, browser, or cross-network blocker remains.
