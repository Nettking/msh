# Physical acceptance pre-flight — read-only inventory

Run [34033694489](https://github.com/Nettking/msh/actions/runs/34033694489),
harness `.github/workflows/physical-preflight.yml` at `cd2e7fc`, 2026-09-06T12:38Z.
All three jobs SUCCESS.

**Nothing was deployed, started, stopped or mutated.** No service, container,
swap, Docker, WSL, runner or git state was changed on any host. No authenticated
Federation query was made.

```
PHYSICAL_ACTION_STARTED: NO
PHYSICAL_FEDERATION_V1_ACCEPTANCE: NOT RUN
```

## Host inventory

| | Nettking (Windows) | Nettking-Linux (WSL) | Nitro | MSH Recorder |
| --- | --- | --- | --- | --- |
| Runner label | `fcp-windows` | `nettking-linux` | `fcp-linux` | **none** |
| Runner account | `NT AUTHORITY\NETWORK SERVICE` | `gha` | `martin` | — |
| Deployment checkout | `C:\wsl\msh` | none found | `/home/martin/fcp` | **unknown** |
| Checkout HEAD | **6101c86d**, branch `main`, clean | — | **6101c86d**, branch `main`, clean | **unknown** |
| Has `bcf5c9ab`? | **NO — fetch required** | — | **NO — fetch required** | **unknown** |
| Has rollback `6101c86d`? | yes (`commit`) | — | yes (`commit`) | **unknown** |
| Containers | none visible | 0 running / 0 total | **5 running / 10 total** | **unknown** |
| `fcp`/`msh` images | none visible | 0 | **9** | **unknown** |
| Memory available | see caveat | **11845 MiB** | **2078 MiB** | **unknown** |
| Swap | — | **0 devices** | 1 device | **unknown** |
| Java / Arrowhead | none | **0 processes** | 0 processes | **unknown** |

The deployed SHAs recorded in the handoff are now **re-proven by live
inspection** rather than assumed: Nettking and Nitro are both on `6101c86d`,
on `main`, with clean worktrees.

## Tailnet

From Nitro (`tailscale status`), all three Federation hosts are present:

```
100.70.61.68    nettking      windows  active; relay "hel"
100.78.187.87   nitro         linux
100.66.214.22   msh-recorder  windows
100.85.20.75    beast         windows      <- AI_PROVIDER_ONLY, keep out
100.86.42.62    oneplus-15    android
100.107.229.78  utlan2026     windows  offline, last seen 22h ago
```

Peers show `-` rather than an active path, which is normal for idle tailnet
peers and is not evidence of unreachability. Actual host-to-host reachability
still has to be demonstrated inside the campaign.

## Arrowhead

On Nettking-Linux all 14 `arrowhead-*` systemd units are present and **not
running** — most in `failed`, the rest `inactive dead` — and **zero java
processes are resident**. Memory available is 11845 MiB with swap disabled
(0 devices). The operator's isolation therefore still holds, and the memory it
freed is still free.

Arrowhead was not touched by this pre-flight and remains stopped.

## Blocking findings

### B1 — No route to the MSH Recorder host (blocking)

The only self-hosted runner labels in the repository, across every branch, are
`fcp-linux`, `fcp-windows`, `nettking-linux` and `beast-linux`. **There is no
runner for MSH Recorder**, and no workflow references one. It is visible on the
tailnet at `100.66.214.22` (Windows) but is not reachable from this session.

MSH Recorder is **recorder 2** in the campaign. Without it, the rows that make
this candidate's acceptance meaningful cannot be attempted at all: two
recorders coexisting, distinct `recorder-{node_id}` identities, absence of
`capability-identity-conflict`, independent recorder-control targeting, legacy
migration and retirement, no duplicate ownership and no unauthorized takeover.

### B2 — Neither reachable host has the merged SHA (blocking, easily fixed)

`C:\wsl\msh` and `/home/martin/fcp` are both on `6101c86d` and neither can
resolve `bcf5c9ab`; each needs a fetch before it can be deployed. Both retain
the rollback commit, so rollback is reachable on both.

### B3 — Nitro headroom (assess before loading)

Nitro is the one host actually running the stack: 5 containers up, 9 FCP images,
and only **2078 MiB available** with one swap device. It is also the host whose
historical slow-host timing failures are documented. Adding campaign load and an
activated recorder to that host needs a deliberate headroom decision, not an
assumption.

### B4 — Automation limits on the Windows host (worked around, recorded)

The Nettking Windows runner has PowerShell execution policy **Restricted** for
the runner account: any `.ps1` step dies with `PSSecurityException` before
running. This is the same condition already recorded against Beast-Windows. The
pre-flight was rewritten to use `cmd`, matching the proven Windows release gate.
The host's execution policy was **not** changed.

`C:\wsl\msh` is owned by `Nettking\Martin` while the runner is `NETWORK
SERVICE`, so git refuses it with "dubious ownership". It was read using
per-process `GIT_CONFIG_COUNT/KEY/VALUE`, which writes nothing to the runner's
global config. `tailscale` on Nettking is owned by Martin's session and returns
`401 Unauthorized` to the runner account.

### Visibility caveat

Every figure above is taken as the runner account. Containers, services or state
owned by another user session may not be visible to it — note that the three
hosts run under three different accounts (`NETWORK SERVICE`, `gha`, `martin`).
"Not visible to the runner" is not the same claim as "not deployed", and nothing
here should be read as the latter.

## Disposition

Physical acceptance is **not started**, and deliberately so. B1 alone means the
three-host campaign cannot reach a verdict, and the plan already states that a
campaign whose hosts are not all on one identical SHA is void. Deploying
`bcf5c9ab` to Nettking and Nitro while MSH Recorder stays on `6101c86d` would
move two production hosts off a known-good SHA into a mixed-SHA state that
cannot be validated — cost without evidence.

Required to proceed:

1. A route to MSH Recorder — a self-hosted runner on it, or an operator willing
   to run the recorder-side steps and supply evidence.
2. `git fetch` on both reachable deployment checkouts so `bcf5c9ab` resolves.
3. A headroom decision for Nitro.
4. Part B recorder-identity coverage, which the existing P01-P12 campaign does
   not provide.
