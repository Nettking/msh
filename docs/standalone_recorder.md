# Standalone MTConnect recorder

Status: **current user/operator guide**

Reviewed: **2026-08-11**

The standalone recorder is a headless FCP device for loss-aware MTConnect capture. It can join an existing Federation, discover local MTConnect Agents, keep recording when the Federation is unavailable, publish checkpoint-committed data to Federation logical storage, and accept bounded scan/source-selection requests from any trusted device in the same Federation.

## Simplest first start

Generate a normal `FCP1-...` pairing code from the Federation owner and run, from the recorder checkout:

```bash
python start_recorder.py FCP1-...
```

That is the intended normal first-run command. The pairing code is the only required argument when the recorder can infer its private network and discover MTConnect Agents.

On first configuration the launcher:

1. loads or creates the recorder's stable FCP device identity;
2. runs the existing bounded private-network MTConnect scan by default;
3. selects discovered sources automatically only when no source selection has previously been completed;
4. redeems the signed one-use pairing code and joins the Federation;
5. starts managed loss-aware recording; and
6. starts the Federation recorder-control worker and publication reconciler.

If the recorder cannot infer a suitable private IPv4 `/24`, it still may join the Federation. You can then start a scan remotely from another trusted Federation device or provide `--scan-cidr` explicitly.

## Later starts

After the first successful join, the pairing code is not required again. The recorder reuses its stable identity, saved trusted Federation binding, and saved source selection:

```bash
python start_recorder.py
```

Normal restart does not automatically repopulate sources that you intentionally removed. A durable initial-selection marker distinguishes first-time auto-selection from later operator-managed configuration.

You can still supply explicit sources for controlled deployments:

```bash
python start_recorder.py Mazak=http://192.168.200.249:5000
```

or an explicit scan network:

```bash
python start_recorder.py FCP1-... --scan-cidr 192.168.200.0/24
```

Use `--no-auto-scan` only when you deliberately want to skip startup discovery.

## Pairing-code behavior

The browser-generated `FCP1-...` code is signed, one-use, and valid for up to **10 minutes**. Generate a new code whenever a previous attempt expired or another device must join.

The recorder never persists the pairing code, enrollment token, or invitation token. After successful enrollment it stores only the stable device identity and the public-safe reconnect binding required to reconnect later.

## Manage the recorder from any Federation device

Open:

```text
/federation/recorders
```

from any trusted FCP device in the same Federation.

The page lists connected standalone MTConnect recorders. An operator can choose a recorder and:

- request a new network scan;
- review the discovered machines returned by that recorder;
- add discovered machines to that recorder's configured source set; and
- remove sources that recorder is currently capturing.

The request is sent through authenticated Federation session events. The network scan itself always executes on the recorder host, not on the browser/device from which the operator pressed the button.

### Safety boundary

Remote recorder control does not grant shell or arbitrary network authority.

A remote member cannot:

- send an arbitrary command or executable;
- inject an arbitrary MTConnect URL or credentials;
- make the recorder scan an unrestricted network;
- target a normal FCP device that is not advertising the standalone recorder capability; or
- bypass the recorder's local validation.

Remote additions identify only opaque source IDs returned by the recorder's own latest scan. The recorder resolves and revalidates those IDs locally before changing configuration.

Network discovery remains bounded to validated RFC1918 private IPv4 networks of `/24` or smaller and retains the existing address, timeout, redirect, and response-size limits.

## What happens when sources change

Adding or removing a source changes future capture configuration. It does not delete telemetry already recorded for that source, and it does not erase durable historical checkpoints.

The managed recorder observes configuration changes without requiring a process restart. Its safe capability metadata can be re-announced after source changes without exposing MTConnect source URLs or credentials to the Federation.

## Capture remains local-first

Federation availability is not part of the recorder's local commit boundary.

The recorder first commits:

- the raw MTConnect response archive;
- normalized detailed observations;
- FCP-compatible JSONL; and
- the durable recorder checkpoint.

Only checkpoint-committed data becomes eligible for Federation publication.

If the relay, storage authority, primary, or replica is unavailable, local recording continues. Publication stays in a durable local outbox and retries later. Federation outages do not move the MTConnect checkpoint backward and do not block normal polling.

## Federation storage publication

The recorder never selects or writes directly to an arbitrary physical database. It publishes bounded deterministic chunks through the authenticated Federation to the session owner's logical-storage authority.

That authority applies the existing storage control-plane rules, including current assignment, fencing, acknowledgement policy, primary/replica routing, and manifest commit.

If exactly one ready logical-storage group is available, the recorder may select it automatically. When several are available, choose one explicitly:

```bash
python start_recorder.py --storage-group telemetry
```

If no ready logical-storage authority exists, capture still continues locally and publication waits safely.

## Useful options

```text
--device-name NAME            Stable display label for the recorder device
--storage-group ID            Explicit logical Federation storage group
--federation-timeout SECONDS  Federation request timeout
--require-federation          Stop instead of falling back to local capture when initial join/reconnect fails
--scan-cidr CIDR              Explicit private IPv4 scan network
--scan-port PORT              MTConnect scan port (default 5000)
--no-auto-scan                Skip startup discovery
--once                        Run one recorder catch-up cycle and exit
```

Manual `NAME=URL` source arguments remain supported for explicit deployments and tests.

## Durable recorder state

Important local state includes:

```text
data/capabilities/config.json
data/source_state/mtconnect_recorder_state.json
data/source_state/mtconnect_recorder_status.json
data/source_state/mtconnect_recorder.log
data/source_state/mtconnect_recorder_autoconfig.json
data/federation/device/
data/federation/onboarding/
data/federation/recorder_publication/
```

Do not delete these paths to fix a transient Federation or scan error. Use the product's explicit source controls, pairing/reconnect path, or documented reset/migration procedure instead.

For the recorder's storage formats and exact loss-aware capture algorithm, see [`catalog/standalone-recorder_v2/README.md`](../catalog/standalone-recorder_v2/README.md).

See also:

- [Federation operations](federation_operations.md)
- [Operator guide](operator_guide.md)
- [Server setup](server_setup.md)
- [Troubleshooting](troubleshooting.md)
