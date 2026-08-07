# CF7-C physical test readiness

Status: repository-side preparation for physical CF7 testing. This tooling does not claim that physical acceptance has passed and does not change production runtime or authority.

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

1. `init` — create the ignored evidence directory and bind it to one exact commit.
2. `preflight` — verify commit, clean checkout, Python, Docker, and the machine-specific physical seam.
3. `gate` — run the CF7 acceptance tests, integrated capability routes, Compose validation, and diff hygiene, then write a redacted command summary.
4. `validate` — invoke the strict CF7-B physical-evidence validator.

Private endpoint values are read only from process environment variables. Readiness summaries deliberately retain `"accepted": false`; only the complete CF7-B evidence document may report acceptance.

## Before testing

Use the same frozen candidate commit on all three machines. Formal fresh-checkout observations require newly cloned clean worktrees.

```bash
git clone https://github.com/Nettking/msh.git msh-cf7-physical
cd msh-cf7-physical
git checkout <FROZEN_COMMIT_SHA>
python -m pip install --upgrade pip
python -m pip install -r requirements.txt -c constraints-phase2.txt
python -m pip install pytest ruff
```

Docker must be available from the same terminal. Do not modify the checkout except under the ignored `evidence/` directory and ignored local topology file.

## Local AI machine

```powershell
ollama pull qwen2.5:7b
$env:MSH_CF7_OLLAMA_URL = "http://<PRIVATE_OLLAMA_HOST>:11434"
$env:MSH_CF7_OLLAMA_MODEL = "qwen2.5:7b"
$commit = (git rev-parse HEAD).Trim()
.\ops\cf7_prepare_windows.ps1 `
  -Machine local-ai `
  -Commit $commit `
  -Operator Martin `
  -Action all
```

The preflight verifies bounded Ollama inventory and model presence. It does not run the physical product benchmark or mark it passed.

## CNC recorder machine

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

- Use one exact 40-character commit on all machines.
- Confirm required CF7 and broad Federation workflows are green on that commit.
- Do not change the candidate during the physical campaign.

### 2. Run preparation

Run `init`, `preflight`, and `gate` on required physical Windows and Linux hosts. Preserve redacted summaries from clean runs.

### 3. Start the supported product

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

### 4. Complete mandatory onboarding

On each device:

1. create or reopen stable identity;
2. create, join, or reconnect to the selected Federation through the authenticated path;
3. inspect the device;
4. finish setup and open Federation.

A current inspection is sufficient to finish setup.

### 5. Execute optional capability and physical scenarios

Run the specific benchmarks and contribution actions required by the physical scenario set:

- verify both CNC MTConnect sources;
- enable and disable recording explicitly;
- execute the selected Ollama benchmark on the target accelerator;
- enable recorder and AI simultaneously where assigned;
- expose only registered compute handlers;
- keep storage candidate-only until existing assignment authority acts;
- verify expiry/invalidation and explicit rerun;
- verify restart, trusted reconnect, and contribution reconciliation;
- verify revocation, fencing, and controlled rejoin;
- inspect desktop and mobile onboarding, Federation, degraded, blocked, and repair states.

Benchmarks and contributions are optional for normal setup, but required when a named acceptance scenario calls for them.

### 6. Redact and validate evidence

Store local summaries, screenshots, and observations only under `evidence/`. Never include raw IP addresses, URLs, credentials, invitation/enrollment material, private hostnames, absolute paths, or database locations.

Linux:

```bash
commit="$(git rev-parse HEAD)"
python -m ops.cf7_physical_readiness validate \
  evidence/cf7-physical-evidence.json \
  --commit "$commit"
```

Windows:

```powershell
$commit = (git rev-parse HEAD).Trim()
python -m ops.cf7_physical_readiness validate `
  evidence\cf7-physical-evidence.json `
  --commit $commit
```

Successful final validation prints `"accepted":true`. Readiness, green CI, probes, or screenshots alone are insufficient.

## Stop conditions

Stop rather than accept when:

- machines use different commits;
- one OS family is represented only by CI, container, or WSL;
- required MTConnect sources are unavailable or invalid;
- the target Ollama model cannot run;
- private addresses would need to be committed;
- storage self-assigns authority;
- benchmark evidence grants authority;
- contribution disablement removes membership or unrelated contributions;
- revocation fails to fence future use;
- evidence cannot distinguish physical behavior from fixtures or loopback;
- any authority, privacy, data-loss, platform, browser, or cross-network blocker remains.