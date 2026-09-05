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

## 1. Why the provider profile is the correct role

The repository already separates *local process composition* from *capability
contribution authority*. Both layers must stay in the AI-only configuration;
neither is invented for this recovery.

**Layer 1 — command profile.** `catalog/command_setup.py` defines the
`language-model-provider` profile as "Run only the headless Ollama provider
service", with `skip_orchestration: True` and `compose_profile: "provider"`.
The module docstring is explicit: "Command profiles are local process-composition
choices. They are deliberately separate from capability onboarding and
contribution authority. ... selecting a command profile never enables a
contribution or marks onboarding complete."

The `provider` Compose profile in `docker-compose.yml` contains exactly two
services, `model-provider` and the one-shot `model-provider-install`. It starts
no `flask`, no `relay`, no `recorder`, and no storage service. A Beast running
this profile is therefore *structurally* incapable of contributing storage or
recording — those processes do not exist on the host.

`model-provider` carries the capability labels directly:

```yaml
labels:
  - "no.fcp.capability=language-model"
  - "no.fcp.protocol=ollama"
```

**Layer 2 — contribution authority.** `catalog/federation/onboarding_compat.py`
defines `CONTRIBUTION_KEYS = ("workbench", "runtime", "recorder",
"language-model", "compute", "storage")`. `_base_intents()` initializes every
key to `ContributionDesiredState.DISABLED`, and the `language-model-provider`
mode enables `language-model` only. `compute` and `storage` are left disabled
with the recorded warning that they "remain disabled until the user reviews new
benchmark candidates".

Storage is additionally candidacy-only even when enabled — the contribution
service describes it as "Request candidacy only; assignment remains
control-plane owned"
(`catalog/flask_app/services/capability_contribution_service.py`).

**Enrollment is metadata only.** `catalog/capabilities/provider_enrollment.py`
states that an announcement "does not create resource reports, reserve capacity,
dispatch jobs, invoke providers, or grant storage, artifact, or execution
authority."

Conclusion: the supported AI-provider-only role for Beast is the existing
`language-model-provider` command profile plus a `language-model`-only
contribution intent. No new role, key, or capability is required.

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

Stop at the first step that restores service; do not run later steps unless the
verification in section 6 still fails.

**Step 1 — start the provider service only.**

```powershell
Set-Location $RepoRoot
docker compose --profile provider up -d model-provider
docker compose --profile provider ps model-provider
```

The service declares a healthcheck (`ollama list`, 2s interval, 30 retries).
Wait for `healthy` before continuing.

**Step 2 — confirm the model is present, and pull only if missing.**

```powershell
curl.exe -s http://127.0.0.1:11434/api/tags
# if the selected model is absent:
python -m catalog.federation.model_resource_pull --target model-provider --model smollm2:360m
```

The model lives in the persistent `model_provider_models` Docker volume, so
ordinary restarts and repository updates do not re-download it.

**Step 3 — repair configuration only if Phase 0 showed class H.**

```powershell
python setup_fcp.py --profile language-model-provider --ai-profile edge-small --start --pull-model
```

This rewrites `.env` and `data/capabilities/config.json` for a provider node and
starts the `provider` Compose profile. It does not enable any contribution and
does not mark onboarding complete.

**Step 4 — restore reachability if Phase 0 showed class G.**

```powershell
tailscale status
tailscale up
# private-network inbound rule for the provider port, if absent:
New-NetFirewallRule -DisplayName "FCP language-model provider" `
  -Direction Inbound -Action Allow -Protocol TCP -LocalPort 11434 -Profile Private
```

Keep port 11434 on the trusted Tailscale/LAN surface only. A plain Ollama
endpoint has no authentication of its own.

**Step 5 — re-enroll only if Phase 0 showed class C or D.** Register Beast from
the consuming FCP node's provider surface. Enrollment is metadata only and grants
no storage, artifact, or execution authority.

**Step 6 — enable the AI contribution only if Phase 0 showed class E.** In
`/onboarding`, set `language-model` to enabled and leave `recorder`, `storage`,
and `compute` disabled.

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
