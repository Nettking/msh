# Tailscale Federation discovery

Status: **current user and administrator guide**
Reviewed: **2026-08-12**

FCP can use an already installed and signed-in Tailscale client to find another FCP Federation on the same tailnet during first-device setup. This is a convenience feature for **discovery and reachability only**. Tailscale membership does not itself grant FCP Federation membership.

## What you need

- Tailscale installed on the FCP host.
- The host already signed in to the intended tailnet.
- The existing FCP Federation authority online on the same tailnet.
- A human Federation administrator account on that existing FCP installation.
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
5. stores a public-safe discovery snapshot as `data/tailscale_discovery.json`; and
6. delegates normal startup to `start.cmd` or `start.sh`.

The snapshot is deliberately outside `data/federation`. A deliberate `--fresh` reset removes device and Federation authority state, but it must not erase the just-completed reachability scan that a new device needs to find the existing Federation.

If Tailscale is unavailable, the launcher falls back to the normal supported FCP launcher. If discovery itself fails, manual Federation onboarding remains available.

## What a fresh second device shows

If a fresh FCP installation discovers an existing Federation, the normal human sign-in page shows the discovered Federation instead of asking the operator to create another administrator and password.

Choose **Sign in to _Federation name_**. The browser opens the existing Federation authority over Tailscale. Enter the human credentials that already belong to that Federation there. An administrator then approves the new device.

The joining device does **not** receive or store that password. After approval, FCP internally uses the existing signed, short-lived, one-use pairing grant to enroll the device and then uses the normal Federation human SSO assertion flow.

Only a genuinely first device that is creating a new Federation, or a standalone installation, should use **Create the first administrator**.

## Discovery does not grant membership

The convenience flow combines discovery, human authentication, administrator approval, and the existing pairing primitive, but those are still separate security boundaries:

1. Tailscale discovery proves only that an FCP authority is reachable on the private tailnet.
2. The human signs in on the existing Federation authority.
3. The authenticated administrator explicitly approves the new device.
4. The authority creates the existing short-lived, one-use FCP pairing grant.
5. The joining device verifies and redeems that grant.
6. Normal Federation human SSO supplies the signed user assertion for the new member.

The discovery record itself contains only bounded public-safe metadata such as the Federation display label, a derived Federation fingerprint, advertising device name, Tailscale reachability address, web port, and relay port. It contains no password, enrollment token, invitation token, pairing code, device private key, or Tailscale credential.

The manual copy-and-paste `FCP1-...` pairing flow remains a fallback when Tailscale discovery or login-first enrollment is not being used.

## Automatic joining for your own devices

When both hosts are signed in to the same tailnet as the same user, a joining device is authorized automatically and nobody copies a pairing code.

This runs on the host, not in the application container, because only the host can ask `tailscaled` who a connecting peer really is. The container sits behind a published Docker port and sees the Docker gateway rather than the peer, so it could never verify identity itself.

The Tailscale launcher therefore starts a small host responder alongside discovery:

1. A joining host asks the discovered coordinator's responder to authorize it.
2. The responder reads the peer address from the connection itself and calls `tailscale whois`.
3. It accepts the peer only when it is on the **same tailnet**, owned by the **same login**, **not shared in** from another tailnet, and **not tag-owned**.
4. Only then does the responder ask the local FCP application for one ordinary pairing grant, authenticated with a secret stored in the mounted data directory.
5. The joining host stores that grant privately and FCP redeems it through the normal pairing flow on the next page load.

The grant is the same signed, one-use, short-lived pairing primitive used by manual pairing. The responder mints nothing itself, and there is no setting that turns the identity check off. A device that merely reaches the responder — a shared node, a tagged server, another user's device, or anything off the tailnet — is refused and never receives a grant.

The responder listens on the host's Tailscale address on port `5151` by default. Set `FCP_AUTO_JOIN_PORT` to change it.

On Windows the launcher also adds a firewall rule for that port. The web and relay ports reach the tailnet because Docker Desktop creates its own rules; the responder is an ordinary host process and gets none, so without this the joining device is simply dropped. Adding the rule needs elevation. If the launcher could not add it, it prints the exact command to run once in an elevated prompt:

```cmd
netsh advfirewall firewall add rule name="FCP tailnet join responder" dir=in action=allow protocol=TCP localport=5151
```

Each start replaces any responder left over from an earlier start, so the port is never held by a stale process. The responder prints `listening on <address>:<port>` only after the socket actually exists; if it cannot bind, it says so and says that manual pairing still works.

`--fresh` removes the responder secret and any stored grant along with the rest of the mutable installation state, so a factory-reset device cannot rejoin the old Federation with old material.

Because that reset runs after the launcher's host steps, a `--fresh` start deliberately skips the automatic join and says so:

```
Factory reset requested; automatic Federation joining runs on the next normal start.
```

Start the device again normally and it joins by itself. The responder keeps running through a reset on the coordinator side, because it re-reads its secret on every request.

### Checking automatic joining before a test

```bash
python3 catalog/federation/tailnet_join_responder.py --check
```

```cmd
python catalog\federation\tailnet_join_responder.py --check
```

It reports the signed-in Tailscale login, the tailnet, and the bind address, and exits non-zero with an explicit reason when Tailscale is missing, logged out, or has no address. The launcher runs the same check and prints `Automatic Federation joining is unavailable; manual pairing codes still work.` rather than letting the problem surface later as a Federation timeout.

## Enrollment return safety

The one-use pairing grant is not placed in an HTTP query or sent as a cross-site form body. The authority returns it in the browser URL fragment, which is not transmitted in the HTTP request. The joining device immediately removes the fragment from browser history and submits the grant to itself with a same-origin CSRF-protected request bound to an expiring high-entropy browser state.

The enrollment callback accepts only a Tailscale IPv4 origin and the exact FCP enrollment callback path. A grant whose Federation identity does not match the Federation fingerprint selected during discovery is rejected before redemption.

## Why one discovery endpoint is public

`/onboarding/federation/discovery.json` is intentionally readable before human sign-in so a new FCP device can identify a reachable Federation before it has a human or Federation session on that host.

That endpoint advertises only the public-safe metadata described above. It cannot create pairing grants, redeem pairing grants, add members, change Federation state, or grant provider/update authority. Those controls keep their normal human and Federation authorization checks.

A remotely paired member does not advertise itself as the Federation authority through this endpoint.

## Network and port behavior

Discovery is currently IPv4-first. Unless separately configured, it probes web port `5000` on online Tailscale peers.

The discovery port and the joining device's local web port are not the same setting. The Tailscale launcher no longer forces the joining device to reserve port `5000`; the normal launcher can use its safe local fallback behavior when that port is already occupied unless `FCP_WEB_PORT` was explicitly configured.

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
- the existing FCP authority is online;
- the existing FCP web interface is reachable on its Tailscale IPv4 address and discovery web port;
- the existing FCP device is the local Federation authority that can advertise discovery metadata; and
- a firewall is not blocking the FCP web or relay port over Tailscale.

Discovery is a startup snapshot. If another device came online afterward, run the Tailscale launcher again to refresh the host-side discovery snapshot.

### The new device shows first-administrator creation instead of Federation sign-in

That means FCP does not currently have a valid discovered Federation candidate. Re-run the Tailscale launcher and verify that it reports at least one discovered FCP Federation before the web application starts.

Do not create a second administrator merely to get past this page. If this device is supposed to join an existing Federation, correct discovery/reachability instead.

### Federation sign-in opens the authority but enrollment is rejected

The account approving a new device needs the current `pairing.manage` permission; with the default role policy this is an administrator permission. Also verify that the authority was opened through its Tailscale IPv4 address so it can construct a reachable private relay address for the joining device.

### `start-tailscale.cmd` says Python was not found

The Windows launcher can still bind FCP to the Tailscale address and continue with normal startup, but the pre-start discovery scan is skipped. Install a supported host Python if you want automatic discovery, or use the normal manual pairing flow.

Host discovery itself is standard-library-only and should not require PostgreSQL, `psycopg`, or `libpq` on the host.

## Related guides

- [Quick start](quick_start.md)
- [Getting started](getting_started.md)
- [Federation operations](federation_operations.md)
- [Human users, sign-in, and permissions](human-authentication.md)
- [Troubleshooting](troubleshooting.md)
