# Headless physical acceptance

Status: **pre-v1 acceptance workflow**

This guide provides a terminal-first physical acceptance path for normal FCP
installations and standalone MTConnect recorders. It removes browser steering
from device identity and Federation bootstrap; it does not weaken Federation
membership, pairing, update, storage, recorder, or provider authority.

All release evidence must still target one exact clean candidate commit. The
normal `docs/release_process.md` rules remain authoritative.

## Command model

There are three operator actions:

```text
normal first node       start-headless
normal additional node  connect FCP1-...
standalone recorder     connect-recorder FCP1-...
```

`connect` with no pairing code is run on the first/local-authority node to mint a
fresh signed one-use code for the next normal node or recorder.

A standalone recorder is **join-only**. `connect-recorder` requires an `FCP1-...`
pairing code and forces Federation bootstrap to succeed before recording can
continue. It has no create-Federation path.

## Network choice

The headless normal-node launcher uses this order:

1. explicit `--bind` / `--relay-url` when supplied;
2. the existing signed-in Tailscale IPv4 address when Tailscale CLI is available;
3. loopback when no remotely reachable private address is selected.

A pairing code is not minted from a loopback-only relay. For a trusted LAN or
VPN without Tailscale, pass the reachable private address explicitly rather than
letting FCP guess a route.

Examples:

```text
--bind 192.168.10.21
--relay-url ws://192.168.10.21:8765
```

Plain `ws://` pairing is for a trusted private LAN/VPN acceptance environment.
Use the product's reviewed TLS/WSS deployment boundary on untrusted networks.

## Windows acceptance flow

### Machine A — first normal FCP node

From a clean checkout of the exact candidate commit:

```bat
start-headless.cmd --fresh
```

The command:

- verifies a clean Git checkout and records the exact build commit;
- builds the normal relay, Flask, and recorder images;
- performs the same bounded fresh-state reset used by the supported Windows
  launcher;
- starts the host-owned update agent, relay, Ollama, managed recorder, and Flask;
- creates/reuses the stable FCP device identity;
- creates the first local Federation through the existing onboarding authority;
- verifies the runtime is reachable; and
- prints a one-use `FCP1-...` code when a reachable relay address is known.

If a new code is needed later:

```bat
connect.cmd
```

On a trusted LAN without automatic Tailscale binding:

```bat
start-headless.cmd --fresh --bind 192.168.10.21
```

or explicitly:

```bat
connect.cmd --relay-url ws://192.168.10.21:8765
```

### Machine B/C/... — additional normal FCP node

Generate a fresh code on Machine A with `connect.cmd`, then on the joining
machine run:

```bat
connect.cmd FCP1-...
```

For a clean physical-acceptance join:

```bat
connect.cmd FCP1-... --fresh
```

The joining command refuses to replace an existing Federation binding. Use
`--fresh` only when the acceptance procedure intentionally requires a fresh
installation.

### Standalone recorder — never the first node

Generate a fresh code on Machine A, then on the recorder host run:

```bat
connect-recorder.cmd FCP1-...
```

To require a ready logical-storage publication path before capture starts:

```bat
connect-recorder.cmd FCP1-... --require-data-sharing
```

To bind first MTConnect discovery to an explicit private subnet:

```bat
connect-recorder.cmd FCP1-... --scan-cidr 192.168.10.0/24
```

The recorder command cannot create a Federation and does not silently fall back
to local-only capture if Federation bootstrap fails.

## Linux/macOS acceptance flow

The same contract is exposed through POSIX wrappers:

```bash
bash start-headless.sh --fresh
bash connect.sh                 # mint a new code on the first node
bash connect.sh 'FCP1-...'      # join another normal node
bash connect-recorder.sh 'FCP1-...'
```

Use `--bind <trusted-private-IP>` or `--relay-url ws://HOST:8765` exactly as on
Windows when automatic Tailscale binding is not appropriate.

## Machine-readable state/evidence

A started normal FCP node can report its saved and revalidated Federation state
without opening the browser:

```bash
python headless_fcp.py status
```

The result identifies the local device, Federation/session identity, connection
state, and exact runtime commit without printing reusable pairing material.

Capability inspection can also run through the installed read-only service:

```bash
docker compose exec -T flask \
  python -m catalog.flask_app.services.headless_capability_cli --json inspect
```

Inspection is evidence only. It does not enable contributions or grant provider,
compute, storage, or Federation authority.

For physical release evidence, capture command output in the local redacted
`evidence/` workspace described by the CF7 physical-acceptance contract. Never
commit pairing codes, private endpoint details, credentials, private keys, raw
relay databases, or unrestricted logs.

## What can now be tested without GUI

The terminal path can drive and observe:

- fresh Windows/Linux startup;
- stable device identity;
- first Federation creation;
- signed one-use pairing-code issuance;
- normal-node join and reconnect;
- standalone-recorder join;
- Tailscale/private-LAN Federation reachability;
- MTConnect recorder startup/discovery/capture;
- capability inspection;
- restart/reconciliation through saved state;
- update-agent/runtime command exercises;
- storage/provider/recorder checks that already expose bounded service/CLI
  contracts; and
- collection of commit-bound command evidence.

## Remaining browser acceptance boundary

The current v1 physical-acceptance contract still includes one real mobile and
desktop browser review because route tests do not prove rendering and interaction
on actual browsers. This headless workflow removes browser dependency from
**setup and Federation operation**, but it does not silently delete that release
claim.

Treat browser review as one final visual smoke observation, not as a mechanism
needed to create/join the Federation or control the acceptance topology. If the
project later chooses a truly zero-GUI release contract, change that acceptance
claim explicitly in a separate review rather than weakening it implicitly here.

## Acceptance topology

A useful physical topology is:

```text
Machine A — normal FCP, first/local Federation authority
    |
    |-- FCP1 code --> Machine B — normal FCP, AI/compute as configured
    |
    |-- FCP1 code --> Machine C — normal FCP, storage as configured
    |
    `-- FCP1 code --> Recorder — standalone MTConnect recorder, join-only
```

The recorder is deliberately never an authority bootstrap node. All new
recorder joins originate from a pairing code issued by the existing normal FCP
Federation authority.
