# Quick start

Status: **current user guide**
Reviewed: **2026-08-18**

FCP is capability-first: devices are not assigned one permanent role. The normal multi-device v1 setup is **initialize one Federation once, then let trusted Tailscale devices discover, join, benchmark, and activate their available services automatically**.

## Prerequisites

For a normal full FCP Windows device:

- Git;
- Docker Desktop running;
- Python 3 available on the host;
- Tailscale installed and already signed in to the intended tailnet;
- internet access the first time the configured Ollama model must be downloaded.

Linux/macOS uses Docker Engine/Compose plus Python 3 and the local Tailscale CLI.

FCP does not require a Tailscale API key, auth key, OAuth secret, or reusable Federation enrollment token for the normal zero-touch path.

## First device: create the Federation once

Only the first device creates the Federation's first human administrator.

### Windows

From the repository directory:

```cmd
start-tailscale.cmd --fresh --initialize-federation
```

### Linux/macOS

```bash
sh start-tailscale.sh --fresh --initialize-federation
```

`--fresh` displays the complete reset boundary and requires typing:

```text
RESET
```

The reset removes mutable FCP identity, Federation, auth, onboarding, benchmark, provider, recorder runtime/configuration, analysis, and job state. It preserves the machine recording corpus and its integrity metadata, Docker/model resources, source code, and deployment settings.

After FCP starts, the terminal asks once for:

```text
Federation administrator email:
Federation administrator password:
```

The password prompt is hidden. The password is never a device-enrollment credential and is not accepted as a launcher argument.

The first-device startup then:

1. creates the first active `admin` human account;
2. creates the Federation through the existing onboarding authority;
3. publishes the creator-backed Federation human-auth authority and user metadata;
4. runs local capability inspection;
5. runs every applicable supported benchmark;
6. enables every locally available supported contribution through the existing authority adapters; and
7. opens the Federation workbench.

The Federation creator remains the immutable password/credential authority even if operational leadership later changes.

## Add another full FCP device

Sign the new host in to the same intended Tailscale tailnet with the same owning Tailscale user, then run:

### Windows

```cmd
start-tailscale.cmd --fresh
```

### Linux/macOS

```bash
sh start-tailscale.sh --fresh
```

After the required `RESET` confirmation, there is no Federation enrollment input. The joining device automatically:

1. starts FCP on its numeric Tailscale IPv4 address and fixed discovery web port;
2. discovers the existing Federation;
3. asks the existing host responder to authenticate the connecting Tailscale peer;
4. is accepted only for the reviewed same-tailnet/same-owner, non-shared, non-tagged identity case;
5. receives and redeems the existing signed, short-lived, one-use pairing grant;
6. creates its own stable FCP device identity and saves Federation membership;
7. publishes its Federation human-SSO endpoint;
8. inspects itself;
9. runs all applicable supported benchmarks; and
10. enables all available supported services through the normal contribution/control-plane path.

A normal joining device does **not** require:

- a Federation IP address;
- a relay IP address;
- an `FCP1-...` code;
- a username/email;
- a password;
- creation of a local administrator;
- a Tailscale API/auth key.

If discovery is empty on a joining device, startup fails rather than creating a second Federation. If discovery is ambiguous, startup refuses to guess. If Tailscale identity verification fails, no pairing grant is issued.

## What "activate everything" means

Automatic setup uses the existing installed capability model. It does not manufacture capabilities that are absent.

For every inspected capability FCP:

- runs the benchmark when that benchmark is applicable and runnable;
- persists the real benchmark result;
- enables a contribution only through its existing adapter;
- disables/fences a candidate that is genuinely blocked by local prerequisites;
- allows storage/provider authority to remain `PENDING` when the control plane has not yet granted authority; and
- never treats benchmark evidence alone as permission to bypass Federation authority.

Current contribution categories include the installed language-model, compute, recorder, and storage candidates where present. Future installed candidate types use the same recommendation/adapter composition rather than requiring launcher hard-coding.

## Human sign-in on any workbench member

Open the member directly on its numeric Tailscale address, normally:

```text
http://100.x.y.z:5000
```

The member uses **Federation sign-in**. The browser is sent to the creator-backed human credential authority, the human signs in there, and the authority issues a short-lived Ed25519-signed assertion bound to that exact member device.

Therefore the same Federation human account can be used across trusted workbench devices. Passwords and password hashes remain only on the credential authority; member databases contain only the minimum shadow/session state required after successful Federation authentication.

## Normal restart

After successful setup:

### Windows

```cmd
start-tailscale.cmd
```

### Linux/macOS

```bash
sh start-tailscale.sh
```

Saved membership is reused. Completed capability bootstrap is idempotent, so normal startup reuses current inspection/benchmark evidence instead of rerunning expensive checks without a reason.

Use `--resume` only when you explicitly want the saved-install resume verification before the normal zero-touch reconciliation.

## Standalone MTConnect recorder

For a Windows recorder host signed in to the same trusted Tailscale environment:

```cmd
start-tailscale-recorder.cmd
```

Normal first start requires no pairing-code, credential, IP, or storage-group prompt when the Federation exposes one unambiguous ready logical-storage group.

The recorder:

1. discovers the Federation;
2. obtains a one-use grant from the same verified Tailscale responder;
3. joins with its own stable recorder identity;
4. verifies the Tailscale relay path;
5. requires a usable Federation publication route;
6. performs the existing bounded private-network MTConnect discovery;
7. starts loss-aware local-first capture; and
8. publishes checkpoint-committed observations through Federation logical storage.

If the MTConnect Agent is not online yet, startup/capture uses the existing bounded retry/backoff behavior and begins recording when the source appears. Federation or storage outages never move the local capture checkpoint backward.

Later recorder starts reuse saved membership and source selection.

The explicit manual recovery path still exists:

```bash
python start_recorder.py FCP1-...
```

That is a fallback for deployments where the reviewed Tailscale zero-touch trust path is intentionally not applicable; it is not the normal Tailscale setup.

## Logical-storage authority

Normal full FCP startup enables supervision of the logical-storage authority. The existing authority monitor still permits the creator-owned authority only on the Federation session creator, so a joined member cannot self-promote.

A recorder or JSONL publisher still fails closed when no writable logical-storage authority is ready.

## Windows Firewall and responder

Automatic join uses a host responder on TCP `5151`. The Windows Tailscale launcher checks/adds the inbound firewall rule. If elevation is unavailable, it prints the exact one-time elevated command instead of hiding the failure.

The responder owns its PID lifecycle, replaces a stale previous responder, and prints `listening on ...` only after the socket has actually bound.

## Local-only and manual deployments

`start.cmd` / `start.sh` remain supported for deliberately local/manual deployments. They preserve normal saved state and do not silently opt a machine into the reviewed Tailscale trust path.

Manual signed `FCP1-...` pairing remains available as an explicit recovery/manual-network mechanism.

## Federation-wide updates

The current operational leader can use **Federation -> Check for updates** and **Update all devices**. Full FCP installations started through the supported launcher run the bounded host update agent and prove the exact requested runtime commit before reporting update success.

A standalone recorder process remains host-managed and is not itself the workbench host-update agent.

## Useful pages

After human sign-in:

- `/federation` — devices, capabilities, contributions, updates, activity;
- `/federation/recorders` — standalone recorder source control;
- `/status` — runtime/recorder/readiness status;
- `/control` — data/workflow controls;
- `/ai` — local/read-only AI system understanding;
- `/docs` — repository documentation.

## Related guides

- [One-command setup](one_command_setup.md)
- [Tailscale Federation discovery](tailscale_federation_discovery.md)
- [Standalone recorder](standalone_recorder.md)
- [Human users, sign-in, and permissions](human-authentication.md)
- [Federation operations](federation_operations.md)
- [Troubleshooting](troubleshooting.md)
