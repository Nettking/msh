# Tailscale Federation discovery

Status: **current user and administrator guide**
Reviewed: **2026-08-12**

FCP can use an already installed and signed-in Tailscale client to find another FCP Federation on the same tailnet during onboarding. This is a convenience feature for **discovery and reachability only**. Tailscale membership does not grant FCP Federation membership.

## What you need

- Tailscale installed on the FCP host.
- The host already signed in to the intended tailnet.
- The other FCP device online on the same tailnet.
- The normal FCP repository checkout and supported launcher prerequisites.

FCP does **not** require a Tailscale API key, auth key, OAuth credential, or reusable Tailscale secret for this feature. It reads the local `tailscale` CLI state that already belongs to the host.

## Start FCP with Tailscale discovery

### Windows

From the FCP repository directory:

```cmd
start-tailscale.cmd
```

### Linux or macOS

```bash
bash start-tailscale.sh
```

The Tailscale launcher:

1. checks whether the local Tailscale client is available and signed in;
2. resolves the host's Tailscale IPv4 address;
3. asks the local Tailscale client for currently online peers;
4. probes only Tailscale IPv4 peers in `100.64.0.0/10` for the bounded FCP discovery endpoint;
5. stores a public-safe discovery snapshot under `data/federation/onboarding/tailscale_discovery.json`; and
6. delegates normal startup to `start.cmd` or `start.sh`.

If Tailscale is unavailable, the launcher falls back to the normal supported FCP launcher. If discovery itself fails, normal Federation onboarding remains available.

## What appears in onboarding

When another FCP Federation is found, the Federation step shows the discovered Federation and the FCP device that advertised it. You can open that discovered FCP device using its Tailscale address to obtain the normal pairing code.

The discovery record contains only bounded public-safe metadata such as:

- Federation display label;
- a derived Federation fingerprint;
- advertising device name;
- Tailscale reachability address; and
- the relay port required by the existing pairing flow.

It does not contain a password, enrollment token, invitation token, pairing code, device private key, or Tailscale credential.

## Discovery does not join the Federation

Finding a Federation is not enough to join it. The joining FCP device must still use the normal signed one-use `FCP1-...` pairing flow:

1. use the discovered link to open the existing FCP device;
2. on the current Federation leader, create a pairing code;
3. copy the `FCP1-...` code;
4. paste it into the joining device's Federation onboarding step; and
5. complete the normal authenticated pairing validation.

Current browser-generated pairing codes are one-use and valid for up to 10 minutes. Generate a new code when a previous code expires or has already been redeemed.

## Why one discovery endpoint is public

`/onboarding/federation/discovery.json` is intentionally readable before human sign-in so a new FCP device can identify a reachable Federation before it has a human or Federation session on that host.

That endpoint advertises only the public-safe metadata described above. It cannot create pairing codes, redeem pairing codes, add members, change Federation state, or grant provider/update authority. Those controls keep their normal human and Federation authorization checks.

A remotely paired member does not advertise itself as the Federation authority through this endpoint.

## Network and port behavior

Discovery is currently IPv4-first. It probes the configured FCP web port, normally `5000`, on online Tailscale peers.

The Tailscale launcher binds the local FCP web interface to the host's Tailscale IPv4 address so another tailnet device can reach it. This is different from the normal launcher, which defaults to `127.0.0.1`.

Do not treat that as permission to expose FCP to the public internet. Keep FCP on the trusted tailnet, trusted LAN/VPN, or another reviewed private deployment boundary.

## Troubleshooting

### No Federation is discovered

Check:

```bash
tailscale status
tailscale ip -4
```

Then verify:

- both FCP devices are signed in to the same intended tailnet;
- the existing FCP device is online;
- the existing FCP web interface is reachable on its Tailscale IPv4 address and configured web port;
- the existing FCP device is the local Federation authority that can advertise discovery metadata; and
- a firewall is not blocking the FCP web port over Tailscale.

Discovery is a startup snapshot. If another device came online afterward, run the Tailscale launcher again to refresh the host-side discovery snapshot.

### The Federation is shown, but joining still asks for a code

That is expected. Tailscale proves private-network reachability, not FCP trust. Create and redeem the normal signed `FCP1-...` pairing code.

### `start-tailscale.cmd` says Python was not found

The Windows launcher can still bind FCP to the Tailscale address and continue with normal startup, but the pre-start discovery scan is skipped. Install a supported host Python if you want automatic discovery, or use the normal manual pairing flow.

## Related guides

- [Quick start](quick_start.md)
- [Getting started](getting_started.md)
- [Federation operations](federation_operations.md)
- [Human users, sign-in, and permissions](human-authentication.md)
- [Troubleshooting](troubleshooting.md)
