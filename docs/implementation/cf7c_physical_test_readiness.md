# CF7-C physical test readiness

Status: repository-side preparation for physical CF7 testing. This tooling does
not claim that physical acceptance has passed and does not change production
runtime or authority.

## Test topology

Use the three available machines as follows:

| Profile | Physical machine | Primary roles |
| --- | --- | --- |
| `local-ai` | Local Windows machine with the NVIDIA GPU and Ollama | language-model contribution and desktop browser |
| `cnc-recorder` | Machine on the MSH network with access to both CNC MTConnect Agents | recorder contribution and two real MTConnect sources |
| `school-control` | Machine at Høgskolen i Østfold | control node, registered compute seam and storage candidate |

At least one of `cnc-recorder` and `school-control` must be a physical Linux
host. The formal acceptance requires one physical Windows checkout and one
physical Linux checkout. WSL can be useful for preparation, but it is not a
replacement for the required physical Linux observation.

The checked-in topology template contains only role names and OS-family choices:

```text
ops/cf7_physical_topology.template.json
```

Copy it locally to `cf7-topology.local.json`, select the actual OS family for
each machine, and validate it:

```bash
python -m ops.cf7_physical_readiness topology cf7-topology.local.json
```

The local topology file and all evidence are ignored by Git.

## What the readiness runner does

`ops/cf7_physical_readiness.py` provides four operator steps:

1. `init` creates the ignored evidence directory from the existing CF7-B
   evidence template and binds it to one exact commit.
2. `preflight` verifies the commit, clean checkout, Python, Docker and the
   physical seam assigned to the current machine.
3. `gate` runs the CF7 acceptance tests, integrated capability routes, Docker
   Compose validation and diff hygiene, then writes a redacted command summary.
4. `validate` invokes the existing strict CF7-B physical-evidence validator.

The runner never persists the private endpoint values. They are read only from
process environment variables. Generated summaries contain safe aliases,
booleans, counts and bounded timings.

The runner deliberately writes `"accepted": false` in readiness summaries.
Only the completed CF7-B evidence document may report acceptance.

## Before testing

Use the same frozen candidate commit on all three machines. Each checkout must
be newly cloned for the formal fresh-checkout scenario and `git status
--porcelain` must be empty.

On every machine:

```bash
git clone https://github.com/Nettking/msh.git msh-cf7-physical
cd msh-cf7-physical
git checkout <FROZEN_COMMIT_SHA>
python -m pip install --upgrade pip
python -m pip install -r requirements.txt -c constraints-phase2.txt
python -m pip install pytest ruff
```

Docker must be available from the same terminal. Do not create or edit files in
the checkout except below the ignored `evidence/` directory and the ignored
local topology file.

## Local AI machine

The `local-ai` profile is intentionally Windows-only because this is the known
local GPU/Ollama machine.

Use the model selected for the MSH product test. The standard strong workstation
profile is `qwen2.5:7b`:

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

The private URL exists only in the PowerShell process environment. The
preflight calls the bounded Ollama inventory endpoint and requires the selected
model to be present. It does not run inference or mark the physical benchmark
scenario passed.

For access from another machine, Ollama must listen on the approved network
interface and the host firewall must allow the selected private network path.
Do not record the raw address in evidence.

## CNC recorder machine

Configure aliases for both physical CNC MTConnect Agents. The value is a local
JSON object; aliases are safe, endpoints are private.

Linux example:

```bash
export MSH_CF7_MTCONNECT_ENDPOINTS='{
  "cnc-one":"http://<PRIVATE_AGENT_ONE>:5000",
  "cnc-two":"http://<PRIVATE_AGENT_TWO>:5000"
}'
commit="$(git rev-parse HEAD)"
bash ops/cf7_prepare_linux.sh cnc-recorder "$commit" Martin all
```

Windows PowerShell example:

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

The preflight performs a bounded read of `/probe` for both aliases. Both must
return an MTConnect probe document. It does not enable recording. Recorder
enablement and observation of fresh records happen later through the product
UI as an explicit physical scenario.

## Høgskolen control machine

The school machine must reach the other two MSH devices through the approved
network. Configure at least two safe aliases with private `host:port` targets.
Use the actual exposed MSH ports for the test deployment.

Linux example:

```bash
export MSH_CF7_PEERS='{
  "local-ai":"<PRIVATE_AI_HOST>:11434",
  "cnc-recorder":"<PRIVATE_RECORDER_HOST>:5000"
}'
commit="$(git rev-parse HEAD)"
bash ops/cf7_prepare_linux.sh school-control "$commit" Martin all
```

Windows PowerShell example:

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

The peer probe proves only TCP reachability to explicitly configured targets.
It does not treat network presence as Federation trust and does not enroll a
provider, assign storage or register a compute handler.

## Recommended execution order

### 1. Freeze the candidate

- Use one exact 40-character commit on all machines.
- Confirm the permanent CF7-A, CF7-B, CF7-C and broad Federation workflows are
  green on that commit.
- Do not change the candidate during the physical run.

### 2. Run `init` and `gate` on the physical Windows and Linux hosts

The `all` wrapper action performs `init`, `preflight` and `gate`. For the formal
fresh-checkout evidence, retain the redacted command summaries from the first
clean run on each OS family.

### 3. Start the physical product services

- Start Ollama on `local-ai` with the chosen model.
- Start the supported MSH Flask application on `school-control`.
- Start the supported MSH application/recorder surface on `cnc-recorder`.
- Keep all endpoint and credential configuration local.

Use the normal repository setup and Compose paths:

```bash
python setup_msh.py
docker compose up -d --build
```

Open the product from the desktop and mobile browsers using the approved
private network path.

### 4. Complete capability-first onboarding

Through the supported UI:

1. Create or reopen stable identities.
2. Create one Federation or join the selected existing Federation with normal
   verification.
3. Inspect each device.
4. Run the explicit relevant benchmarks.
5. Enable recorder and AI contributions where assigned.
6. Keep compute limited to a registered handler.
7. Keep storage candidate-only until existing assignment authority acts.
8. Finish onboarding and confirm returning-device startup behavior.

### 5. Execute the physical CF7 scenarios

Follow issue #180 and the CF7-B runbook. The important real observations are:

- both CNC sources produce valid MTConnect inventory;
- explicit recorder enablement creates fresh records and safe disablement stops
  future recording without removing membership;
- the selected Ollama model completes the explicit product benchmark on the
  target accelerator;
- recorder and AI can be enabled simultaneously without compute or storage
  authority leakage;
- the three independent devices retain separate identities and capability
  authority;
- expiry or dependency invalidation suspends use until explicit rerun;
- restart preserves identity, reconnect and contribution intent;
- revocation fences future use and controlled rejoin requires verification;
- desktop and mobile surfaces render onboarding, Federation, degraded and
  repair states correctly.

### 6. Redact and validate evidence

Store local summaries, screenshots and observations only under `evidence/`.
Never include raw IP addresses, URLs, credentials, invitation/enrollment
material, private hostnames, absolute paths or database locations.

After all scenarios are genuinely passed, update the ignored evidence document
and validate it:

Linux:

```bash
commit="$(git rev-parse HEAD)"
python -m ops.cf7_physical_readiness validate \
  evidence/cf7-physical-evidence.json \
  --commit "$commit"
```

Windows PowerShell:

```powershell
$commit = (git rev-parse HEAD).Trim()
python -m ops.cf7_physical_readiness validate `
  evidence\cf7-physical-evidence.json `
  --commit $commit
```

A successful final validation prints `"accepted":true`. Readiness, green CI,
successful probes or screenshots alone are not sufficient.

## Stop conditions

Stop the physical campaign instead of accepting when:

- any machine uses a different commit;
- one OS family is represented only by CI, a container or WSL;
- either CNC MTConnect Agent is unreachable or does not produce the expected
  inventory;
- Ollama does not contain or run the selected target model;
- peer connectivity requires storing private addresses in committed files;
- storage self-assigns authority;
- benchmark evidence grants authority by itself;
- contribution disablement removes membership or unrelated contributions;
- revocation fails to fence future use;
- evidence cannot distinguish real physical behavior from fixtures or loopback;
- any unresolved data-loss, privacy, authority or cross-platform defect remains.
