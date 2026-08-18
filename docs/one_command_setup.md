# One-command setup

Status: **current startup guide**  
Reviewed: **2026-08-18**

FCP's normal multi-device deployment model is **initialize the Federation once, then let trusted devices self-configure**. Human credentials belong to the Federation, not to each machine.

## First device: initialize the Federation once

The first device is the only device that creates a human administrator and a new Federation.

### Windows

```cmd
start-tailscale.cmd --fresh --initialize-federation
```

### Linux or macOS

```bash
sh start-tailscale.sh --fresh --initialize-federation
```

A fresh reset requires the explicit `RESET` confirmation. The launcher then starts FCP and asks in the terminal for:

```text
Federation administrator email:
Federation administrator password:
```

The password prompt is hidden. The password is not accepted on the command line and is not used as a device-enrollment credential.

The launcher then:

1. creates the first human administrator;
2. initializes the first Federation through the normal authenticated onboarding service;
3. publishes the creator-backed Federation human sign-in authority;
4. inspects the device;
5. runs every applicable supported benchmark;
6. enables every available supported contribution through the existing provider/storage authority adapters; and
7. opens the Federation workbench.

The creator remains the human credential authority. Passwords and password hashes stay there.

## Additional trusted FCP device

Sign the new host in to the same intended Tailscale tailnet with the same owning Tailscale user, then run:

### Windows

```cmd
start-tailscale.cmd --fresh
```

### Linux or macOS

```bash
sh start-tailscale.sh --fresh
```

After the `RESET` confirmation there is **no Federation enrollment prompt**. The device:

1. discovers the existing Federation through Tailscale;
2. asks the existing host responder to verify its Tailscale identity;
3. receives and redeems the normal signed, short-lived, one-use pairing grant;
4. creates its own device identity and joins the Federation;
5. publishes its Federation human-SSO callback endpoint;
6. inspects itself;
7. runs every applicable supported benchmark; and
8. enables every available supported contribution.

No Federation IP address, `FCP1-...` code, email, username, password, Tailscale API key, or local administrator is required on the joining device.

Shared-in peers, tag-owned peers, another Tailscale owner, another tailnet, malformed identity evidence, ambiguous Federation discovery, or a failed grant are rejected rather than silently creating a second Federation.

## Normal restart

Windows:

```cmd
start-tailscale.cmd
```

Linux/macOS:

```bash
sh start-tailscale.sh
```

Saved Federation membership is reused. Completed capability bootstrap is idempotent, so normal restart does not rerun expensive benchmarks merely because time passed.

Use `--resume` when you specifically want the existing-install resume verification before zero-touch reconciliation.

## Human sign-in on any member

Open the member's Tailscale web address, for example:

```text
http://100.x.y.z:5000
```

A Federation member does not become an independent password authority. **Sign in through Federation** sends the browser to the creator-backed credential authority and returns a short-lived assertion bound to that exact member. The same Federation account therefore works on all trusted workbench members without replicating password hashes.

## Standalone MTConnect recorder

On a Windows recorder host that is already signed in to the same trusted Tailscale environment, run:

```cmd
start-tailscale-recorder.cmd
```

Normal first start requires no `FCP1-...` code, credentials, IP address, or storage-group argument when exactly one ready logical-storage group exists. The launcher:

1. discovers the Federation;
2. uses the same verified Tailscale responder to obtain a one-use enrollment grant;
3. joins with its own recorder identity;
4. verifies the Federation publication route;
5. discovers the bounded private MTConnect network;
6. starts local-first loss-aware recording; and
7. publishes checkpoint-committed data through Federation storage.

If the MTConnect Agent is not available yet, the recorder keeps the existing bounded retry/backoff behavior and begins capture when the source appears.

The legacy explicit `python start_recorder.py FCP1-...` path remains available for deliberate recovery/manual deployments; it is not the normal Tailscale setup.

## Storage authority

Normal full FCP startup supervises logical-storage authority automatically. Only the immutable Federation creator is permitted by the existing authority monitor to serve the creator-owned storage authority; a joined member cannot self-promote merely because supervision is enabled.

Recorder and JSONL publishers still fail closed when no writable logical-storage authority is ready.

## Local-only / recovery launchers

`start.cmd` and `start.sh` remain available for deliberately standalone or manual-network deployments. They do not replace the reviewed Tailscale zero-touch trust path.

Manual `FCP1-...` pairing remains a recovery mechanism when the automatic Tailscale trust requirements are not applicable. It does not weaken or bypass the normal identity checks.

## Federation-wide updates

The **current operational leader** can run **Check for updates** and then **Update all devices**. Full FCP devices started with the supported launcher have the bounded host update agent needed for that flow.

A standalone recorder process remains host-managed; updating the workbench's Compose runtime does not itself replace an independently running recorder host process.

## Related guides

- [Quick start](quick_start.md)
- [Human users, sign-in, and permissions](human-authentication.md)
- [Federation operations](federation_operations.md)
- [Tailscale Federation discovery](tailscale_federation_discovery.md)
- [Standalone recorder](standalone_recorder.md)
