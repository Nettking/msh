# Tailscale Federation discovery and zero-touch enrollment

Status: **current user and administrator guide**
Reviewed: **2026-08-18**

FCP uses an already installed and signed-in Tailscale client for two separate purposes:

1. **discovery/reachability** — find an FCP Federation on online Tailscale IPv4 peers; and
2. **reviewed device authorization** — for the narrow same-tailnet/same-owner case, a host-side responder verifies the connecting peer through local `tailscaled` identity before asking FCP for one ordinary short-lived pairing grant.

Tailscale reachability alone never grants FCP membership.

## No Tailscale API key

FCP does not require or store a Tailscale API key, auth key, OAuth secret, or reusable enrollment secret. It uses the local `tailscale` CLI state that already belongs to the host.

## First device: initialize explicitly

Only the first device is allowed to create a new Federation in the zero-touch path.

Windows:

```cmd
start-tailscale.cmd --fresh --initialize-federation
```

Linux/macOS:

```bash
sh start-tailscale.sh --fresh --initialize-federation
```

After the fresh reset, discovery runs. If an existing Federation is found, initialization does not silently create a competing Federation. If none is found and `--initialize-federation` was explicitly supplied, the terminal asks once for the first administrator email and hidden password and then creates the Federation through the existing onboarding authority.

Human credentials created here belong to the Federation credential authority. They are not device enrollment credentials.

## Add another trusted full FCP device

Windows:

```cmd
start-tailscale.cmd --fresh
```

Linux/macOS:

```bash
sh start-tailscale.sh --fresh
```

After the `RESET` confirmation the device needs no enrollment input. It discovers the Federation, requests authorization from the discovered host responder, redeems the returned one-use grant through the normal pairing service, publishes its human-SSO endpoint, inspects itself, benchmarks applicable capabilities, and enables available contributions.

If no Federation is discovered, a joining device stops. It does not create one implicitly. If multiple Federations are discovered, it refuses the ambiguous choice.

## Why discovery runs after `--fresh`

A true factory reset removes mutable FCP application state under `data/` except the preserved machine recording corpus. That includes stale discovery and pairing material. Therefore the supported Tailscale launcher performs the reset first and only then creates a new bounded discovery snapshot and requests a one-use grant.

This prevents a grant fetched for the pre-reset identity/state from being destroyed or accidentally reused across the reset boundary.

## Discovery snapshot

The host discovery helper calls:

```text
tailscale status --json
```

It probes only online IPv4 peers inside Tailscale's `100.64.0.0/10` range and only the bounded public-safe FCP endpoint:

```text
/onboarding/federation/discovery.json
```

The persisted snapshot is schema-bounded and contains only public-safe routing/display metadata such as:

- Federation display label and fingerprint;
- advertising device name;
- numeric Tailscale IPv4 address;
- web port;
- relay port; and
- auto-join responder port.

It contains no password, pairing code, invitation token, private key, or Tailscale credential.

## Automatic joining trust boundary

The responder runs on the host rather than inside Docker because Docker Desktop can rewrite the incoming source address before the application container sees it. Only the host can ask the local Tailscale daemon who the real connecting peer is.

For an automatic join request, the responder accepts only when the local Tailscale identity evidence proves all of the reviewed conditions:

- the connection came from a Tailscale address;
- both devices are in the same intended tailnet;
- both are owned by the same human Tailscale login;
- the peer was not shared in from another tailnet;
- the peer is not tag-owned; and
- the identity evidence is complete/current enough for the parser to make that decision.

A shared node, tagged server, another user's device, another tailnet, malformed identity data, or an unverified peer is refused.

There is no environment variable that disables this identity verification.

## Pairing remains the membership primitive

Successful Tailscale identity verification does **not** write Federation membership directly.

The responder instead:

1. authenticates to the local FCP application using the existing secret stored in the mounted data directory;
2. asks FCP for the existing signed, short-lived, one-use pairing grant;
3. returns that grant only to the verified peer; and
4. the joining device redeems it through the existing pairing service.

The responder cannot mint grants by itself. Replay and expiry semantics remain the same as manual pairing.

Manual signed `FCP1-...` pairing remains available as a recovery path when this automatic trust model is deliberately not applicable.

## Host responder lifecycle

The responder listens on the host's Tailscale IPv4 address, TCP port `5151` by default.

Each supported launcher start:

- runs a Tailscale identity preflight first;
- replaces a responder left over from an earlier start;
- stores the current responder PID in mutable FCP state;
- reports `listening on <address>:<port>` only after the socket bind succeeds; and
- reports an explicit failure instead of claiming a listener exists when bind fails.

The responder is given the exact Tailscale-reachable FCP application origin so its authenticated grant request reaches the same Flask instance that discovery advertises.

## Windows Firewall

Docker Desktop manages publication rules for container ports, but the host responder is an ordinary Windows process. `start-tailscale.cmd` therefore checks/adds the inbound rule for its responder port.

If the launcher cannot add the rule because it is not elevated, it prints the exact command to run once from an elevated prompt:

```cmd
netsh advfirewall firewall add rule name="FCP tailnet join responder" dir=in action=allow protocol=TCP localport=5151
```

## Stable web, relay, and human-auth origins

The zero-touch Tailscale launcher deliberately uses the host's numeric Tailscale address and a fixed advertised web port (normally `5000`). It also supplies:

- the reachable pairing relay origin on Tailscale; and
- the reachable human-auth base URL for that exact device.

This matters because Federation SSO must redirect a browser between physical machines. A member cannot advertise `localhost` as its callback, and the credential authority cannot advertise `localhost` to a browser running on another PC.

Creator authority/user metadata and member callback metadata are published during zero-touch completion, so the first human login on a member does not have to bootstrap routing state.

## Standalone recorder

The Windows standalone recorder uses the same discovery and responder trust path:

```cmd
start-tailscale-recorder.cmd
```

On first start it discovers exactly one Federation, obtains an ordinary one-use grant from the verified responder, joins, validates the Tailscale relay path, and continues into the existing recorder publication/source bootstrap.

There is no normal `FCP1` prompt and no human credential prompt. Saved membership is reused on later starts.

The explicit `python start_recorder.py FCP1-...` path remains available for deliberate recovery/manual deployment.

## Troubleshooting

### Identity preflight fails

Run:

```cmd
tailscale status
tailscale ip -4
```

The host must have a signed-in IPv4 address in `100.64.0.0/10`. If the local Tailscale JSON shape cannot be verified safely, automatic joining fails closed rather than guessing identity.

### No Federation is discovered

Verify:

- the creator is online;
- it was started through the supported Tailscale launcher;
- both hosts are on the same tailnet;
- its web port is reachable over Tailscale; and
- the discovery endpoint is not blocked by host/tailnet policy.

A join-only device will not create a replacement Federation as a workaround.

### Responder is unreachable

Verify TCP `5151` and the Windows firewall rule on the existing Federation host. The responder itself must print a real `listening on ...` line after bind.

### Peer is refused

Do not bypass the check with an undocumented token or manual state edit. Inspect the returned safe reason (`peer-owned-by-another-user`, shared-in, tag-owned, tailnet mismatch, identity unavailable, and similar) and correct the Tailscale ownership/topology if automatic same-owner enrollment is intended.

### Human sign-in redirects to the wrong host

Check that the devices were started through the Tailscale launcher and that their configured/public human-auth origins use their numeric Tailscale `100.x.y.z` address rather than `localhost`.

## Related guides

- [Quick start](quick_start.md)
- [One-command setup](one_command_setup.md)
- [Standalone recorder](standalone_recorder.md)
- [Human users, sign-in, and permissions](human-authentication.md)
- [Federation operations](federation_operations.md)
- [Troubleshooting](troubleshooting.md)
