# Beast AI-provider-only Federation recovery runbook

Status: operational runbook. This document does not change product architecture
and does not define a release. It records the supported sequence for returning
the host named **Beast** to the running Federation contributing **only** its
language-model capability.

Target end state:

| Property | Value |
| --- | --- |
| Beast joined | yes |
| AI provider | READY |
| storage contribution | NONE |
| recorder contribution | NONE |

Host facts assumed by this runbook:

- Tailscale address `100.85.20.75`
- an existing repository checkout on the `C:` drive; the commands below
  refer to it as `$RepoRoot`
- historical service ports: relay `8765`, web `5000`, auto-join `5151`

## 1. The role: a joined device that contributes language-model only

Beast must appear in the Federation with its own node ID and own the
`language-model` capability. That rules out the `language-model-provider`
command profile: its `provider` Compose profile starts only `model-provider`
and the one-shot `model-provider-install`, with no `flask` service, and every
zero-touch step runs through `docker compose exec -T flask`. That profile is a
headless endpoint another device consumes over the "connected computer" path;
it never joins and never owns a capability.

Beast therefore runs the **full FCP device** and is constrained at the
contribution layer, not the process layer.

`catalog/federation/onboarding_compat.py` defines
`CONTRIBUTION_KEYS = ("workbench", "runtime", "recorder", "language-model",
"compute", "storage")`, and `_base_intents()` starts every key at
`ContributionDesiredState.DISABLED`. The AI-only role is exactly:
`language-model` enabled, all five others disabled.

Storage is additionally candidacy-only — the contribution service describes it
as "Request candidacy only; assignment remains control-plane owned" — and
provider enrollment is metadata only:
`catalog/capabilities/provider_enrollment.py` states an announcement "does not
create resource reports, reserve capacity, dispatch jobs, invoke providers, or
grant storage, artifact, or execution authority."

### Hazard: zero-touch auto-enables every available contribution

`scripts/zero_touch_federation_start.py` joins the Federation and then calls
`catalog.flask_app.services.automatic_capability_bootstrap`. Its
`_enable_available_contributions` iterates the recommended candidates and
enables **every one that is not BLOCKED**:

```python
intent = service.apply_choices(
    {candidate.candidate_id: ContributionDesiredState.ENABLED.value}
)[0]
```

There is no capability-type filter and no AI-only option. If Beast's storage
benchmark returns GREEN, `storage` is enabled; `APPROVAL_REQUIRED` and
`PENDING` are deliberately kept enabled. Running the script unconditionally can
therefore make Beast a storage contributor, which this recovery forbids.

The mitigation is the idempotency guard in `bootstrap_capabilities()`: when
capability startup has already completed it returns early with
`contributions_enabled: 0` and never re-runs the enable loop. So the safe
sequence depends on Beast's persisted startup state, and section 5 branches on
exactly that.

### Capability identifiers used below

| Concept | Value | Source |
| --- | --- | --- |
| capability type | `language-model` | `catalog/ai/runtime.py` |
| runtime protocol | `fcp-language-model` | `catalog/ai/runtime.py` |
| remote binding protocol | `fcp-remote-language-model` | `catalog/ai/remote_contracts.py` |
| transport/provider protocol label | `ollama` | `docker-compose.yml` |
| node ID shape | `^node-[A-Za-z0-9_-]{16,200}$` | `catalog/ai/remote_contracts.py` |
| capability config file | `data/capabilities/config.json` | `capability_config_service.py` |
| capability config schema | `fcp.capability_config.v1` | `capability_config_service.py` |

### Model profiles

| Profile | Model | Intended device |
| --- | --- | --- |
| `edge-small` | `smollm2:360m` | small CPU / Pi class |
| `laptop-standard` | `llama3.2:3b` | laptop or small server |
| `workstation-strong` | `qwen2.5:7b` | workstation or GPU server |

A provider node defaults to `edge-small` and is forced to provider mode `local`;
`build_command_plan` rejects `connected` for a provider node with "A
language-model provider must run its own local model."

## 2. Phase 0 — read-only baseline

Run every command in this phase before changing anything. Nothing here mutates
state. Capture the output; the classification in section 3 depends on it.

From an operator host that can reach Beast over Tailscale:

```bash
tailscale status | grep -i beast
ping -c 3 100.85.20.75
```

On Beast (PowerShell). Set `$RepoRoot` to the existing checkout path first:

```powershell
$RepoRoot = 'C:\<checkout>'   # the FCP repository checkout on Beast
Set-Location $RepoRoot
```

```powershell
# identity and OS
hostname
[System.Environment]::OSVersion.VersionString
tailscale ip -4

# repository state
git -C $RepoRoot rev-parse HEAD
git -C $RepoRoot rev-parse --abbrev-ref HEAD
git -C $RepoRoot status --short
git -C $RepoRoot log --oneline -5

# runtime / build SHA actually deployed
docker compose --profile provider ps
docker compose --profile provider images

# FCP + provider services
docker ps -a --filter "label=no.fcp.capability=language-model"
docker compose --profile provider ps model-provider

# Ollama endpoint and models (loopback first, then advertised address)
curl.exe -s http://127.0.0.1:11434/api/tags
curl.exe -s http://100.85.20.75:11434/api/tags

# current AI provider configuration (do not edit yet)
Get-Content $RepoRoot\data\capabilities\config.json
Get-Content $RepoRoot\.env | Select-String 'COMPOSE_PROFILES|FCP_PROVIDER_|FCP_AI_|OLLAMA_BASE_URL|FCP_SKIP_ORCHESTRATION|FCP_RECORDER_SOURCES'

# ports and disk
netstat -ano | Select-String ':11434|:8765|:5000|:5151'
Get-PSDrive C | Select-Object Used,Free

# logs
docker compose --profile provider logs --tail 200 model-provider
```

Record, explicitly:

- hostname / OS
- git HEAD, branch, working-tree status
- deployed image digest for `model-provider`
- which FCP services are running
- Ollama service status and listening endpoint
- installed models
- configured `ai_provider_mode`, `ai_provider_name`, `ollama_base_url`, `ai_model`
- federation/session state and node ID
- currently advertised capabilities
- **whether Beast advertises any storage or recorder capability**
- relevant logs, ports, free disk

### Baseline expected values for the AI-only role

| Key | Expected on a provider node |
| --- | --- |
| `COMPOSE_PROFILES` | `provider` |
| `FCP_SKIP_ORCHESTRATION` | `1` |
| `FCP_PROVIDER_BIND` | `0.0.0.0` |
| `FCP_PROVIDER_PORT` | `11434` |
| `FCP_PROVIDER_MODEL` | selected model, e.g. `smollm2:360m` |
| `ai_provider_mode` | `local` |
| `FCP_RECORDER_SOURCES` | empty |

`FCP_PROVIDER_*` keys are emitted only for a provider node — see
`env_lines_for_plan` in `catalog/command_setup.py`.

## 3. Classification

Do not guess. Map the Phase 0 evidence onto exactly one primary cause.

| Class | Cause | Deciding evidence |
| --- | --- | --- |
| A | FCP service stopped | `docker compose --profile provider ps` shows `model-provider` absent or exited |
| B | Ollama/provider service stopped | container up but `/api/tags` refuses or times out on loopback |
| C | node not joined | provider healthy locally, but absent from the coordinator's provider list |
| D | stale pairing/enrollment | coordinator shows Beast with an expired or revoked enrollment record |
| E | AI contribution disabled | `language-model` intent is `disabled` / `ask-later` in contribution state |
| F | provider health check failing | enrolled but health report expired — `remote-provider-health-expired` |
| G | network/relay issue | `/api/tags` answers on `127.0.0.1` but not on `100.85.20.75`, or Tailscale is down |
| H | stale software/configuration | `.env` or `config.json` missing provider keys, or `COMPOSE_PROFILES` is not `provider` |
| I | other | none of the above; capture evidence before acting |

Class G is the most common split-brain for a Tailscale host: a loopback-only
answer with a dead advertised address means the bind or the firewall, not the
model. `FCP_PROVIDER_BIND` must be `0.0.0.0`, and Windows needs a private-network
inbound rule for TCP 11434.

## 4. Role safety gate — run before rejoining

Confirm Beast will not become a storage or recorder contributor.

```powershell
# 1. Compose profile must be provider-only.
Get-Content $RepoRoot\.env | Select-String '^COMPOSE_PROFILES='
#    expected: COMPOSE_PROFILES=provider

# 2. No recorder source may be configured.
Get-Content $RepoRoot\.env | Select-String '^FCP_RECORDER_SOURCES='
#    expected: empty value

# 3. No storage/recorder container may exist on this host.
docker ps -a --format '{{.Names}}\t{{.Labels}}' | Select-String 'recorder|storage|relay|flask'
#    expected: no rows

# 4. Capability config must not carry recorder sources.
Get-Content $RepoRoot\data\capabilities\config.json
```

On the coordinator, confirm the contribution intents for Beast list
`language-model` as the only enabled key, and that `recorder`, `storage`,
`compute`, `workbench`, and `runtime` are `disabled`.

If a storage or recorder capability is enabled unintentionally, disable it
through the supported contribution surface (the `/onboarding` contribution step,
or the contribution service's `apply_choices` / `suspend`). Do not edit
coordinator database rows by hand, and do not delete persistent state merely to
remove a capability.

## 5. Rejoin — least invasive sequence

First determine which branch applies. Inside the Flask container:

```bash
docker compose exec -T flask python -m \
  catalog.flask_app.services.zero_touch_federation_cli --json status
```

Then read the persisted contribution intents and confirm whether capability
startup already completed, and whether any storage intent is already ENABLED.

### Branch A — capability startup already completed

`bootstrap_capabilities()` early-returns with `contributions_enabled: 0`, so the
supported script cannot add contributions:

```bash
python scripts/zero_touch_federation_start.py --web-port 5000
```

Never pass `--initialize-federation`. Beast is a later device and must be
join-only; the flag exists solely for first-Federation creation and would risk a
split Federation.

Before running, confirm no storage intent is already ENABLED. The early-return
path still calls `_repair_creator_storage_authority_evidence`, which acts only
on a node whose storage intent is already enabled — on an AI-only Beast there is
nothing for it to repair, and that is the state to verify rather than assume.

### Branch B — capability startup has not completed

Do **not** run the full script; its bootstrap would auto-enable every available
contribution. Join with the separable steps, then choose contributions
explicitly.

1. Discover and join, using the same tailnet/same-owner responder and a one-use
   pairing grant. Enrollment uses no human credentials.

   ```bash
   docker compose exec -T flask python -m \
     catalog.flask_app.services.zero_touch_federation_cli --json redeem-auto-join
   ```

2. Confirm membership before touching contributions:

   ```bash
   docker compose exec -T flask python -m \
     catalog.flask_app.services.zero_touch_federation_cli --json status
   ```

   Proceed only when `state` is `connected` and a `node_id` is present.

3. In `/onboarding`, enable `language-model` and leave `recorder`, `storage`,
   `compute`, `workbench`, and `runtime` disabled. Every supported contribution
   needs a persisted decision before startup completes, so disabled must be
   chosen explicitly rather than skipped.

Automatic join refuses ambiguity by design: it fails when no Federation is
discovered, and refuses to pick when more than one is. Both are correct
outcomes, not faults to work around.

### Supporting steps, only as the baseline requires

- **Class B** — start the model service the device serves locally.
- **Class G** — repair reachability: `tailscale status`, then confirm the
  provider port is bound on the tailnet address rather than loopback only.
- **Class H** — repair configuration through `setup_fcp.py`, which never enables
  a contribution and never marks onboarding complete.

Do not deploy `ba8a3b0b828f59c36c5aaaf6130480a2432a5578` for this recovery. It is
a release candidate under separate qualification; changing Beast's software is
justified only if the installed build is demonstrably incompatible.

## 6. End-state verification

Each numbered proof maps to the corresponding acceptance requirement.

```bash
# 3 + 7. provider reachable, healthy, and returns a valid response
curl -s http://100.85.20.75:11434/api/tags

# 6 + 7. one small harmless inference as functional proof (edge-small model)
curl -s http://100.85.20.75:11434/api/generate \
  -d '{"model":"smollm2:360m","prompt":"Reply with the single word: ready","stream":false}'
```

On the coordinator / consuming FCP node:

```bash
# 1, 2, 4, 5. Beast joined, stable node ID, capability discovered and owned
curl -s http://<coordinator>:5000/provider-federation/api/providers
```

In that response confirm:

1. Beast appears as a joined member.
2. Its node ID matches `^node-[A-Za-z0-9_-]{16,200}$` and is stable across a restart.
3. Provider status is READY / healthy and its health report is not expired.
4. A capability of type `language-model` is listed.
5. That capability's owning node ID is Beast's.
6. An invocation routes to Beast through the supported path.
7. Beast returns a valid response.

Then confirm the negative properties:

8. No capability of type `storage` is announced by Beast's node ID.
9. No capability of type `recorder` is announced by Beast's node ID.
10. No storage authority or storage assignment names Beast.
11. Existing Federation members remain healthy — re-check the members list and
    confirm no leadership change or degraded peer followed the rejoin.
12. No repeated auth, reconnect, or capability errors appear:

```powershell
docker compose --profile provider logs --tail 200 model-provider
```

Watch specifically for `remote-provider-health-expired`,
`remote-provider-type-mismatch`, and `remote-provider-protocol-mismatch`; these
are the defined failure codes in `catalog/ai/remote_provider.py`.

## 7. Constraints honored

This recovery is independent of the in-flight qualification of
`ba8a3b0b828f59c36c5aaaf6130480a2432a5578`. It does not push to
`ci/self-hosted-pr435`, cancel that run, consume the `fcp-linux` GitHub runner,
restart Nitro, or modify Nitro CI state.
