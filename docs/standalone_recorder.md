# Standalone MTConnect recorder

Status: **current user/operator guide**
Reviewed: **2026-08-12**

The standalone recorder is a headless FCP device for loss-aware MTConnect capture. It can join an existing Federation, discover local MTConnect Agents, keep recording when the Federation is unavailable, publish checkpoint-committed data to Federation logical storage, and accept bounded scan/source-selection requests from trusted Federation devices.

## Simplest first start

Generate a signed `FCP1-...` pairing code from the **current Federation leader** and run, from the recorder checkout:

```bash
python start_recorder.py FCP1-...
```

That is the intended normal first-run command. The pairing code is the only required argument when the recorder can infer its private network and discover MTConnect Agents.

On first configuration the launcher:

1. loads or creates the recorder's stable FCP device identity;
2. runs the bounded private-network MTConnect scan by default;
3. selects discovered sources automatically only when no source selection has previously been completed;
4. redeems the signed one-use pairing code and joins the Federation;
5. starts managed loss-aware recording; and
6. starts the Federation recorder-control worker and publication reconciler.

If the recorder cannot infer a suitable private IPv4 `/24`, it may still join the Federation. You can then request a scan remotely from another trusted Federation device or provide `--scan-cidr` explicitly.

## Later starts

After the first successful join, the pairing code is not required again:

```bash
python start_recorder.py
```

The recorder reuses its stable identity, saved trusted Federation binding, and saved source selection. Normal restart does not silently repopulate a source set that an operator deliberately emptied.

Explicit source configuration remains available:

```bash
python start_recorder.py Mazak=http://192.168.200.249:5000
```

Explicit scan network:

```bash
python start_recorder.py FCP1-... --scan-cidr 192.168.200.0/24
```

Use `--no-auto-scan` only when you deliberately want to skip startup discovery.

## Pairing-code behavior

Browser-generated `FCP1-...` codes are signed, one-use, and valid for up to **10 minutes**. Generate another code when the previous code expired, was redeemed, or another device must join.

The recorder does not persist the pairing code, enrollment token, or invitation token. After successful enrollment it keeps only its stable device identity and public-safe reconnect binding.

Operational leader authority may move after a valid leader transition. If that happens, create future pairing codes on the **current leader**, not necessarily the immutable Federation creator.

## Manage the recorder from any trusted Federation device

Open:

```text
/federation/recorders
```

The page lists connected standalone MTConnect recorders. A trusted operator can:

- request a new network scan;
- review machines returned by that recorder;
- add discovered machines to that recorder's configured source set; and
- remove sources that recorder currently captures.

The scan executes on the recorder host, not on the browser/device from which the request was made.

### Safety boundary

Remote recorder control does not grant shell or arbitrary network authority. A remote member cannot:

- send arbitrary commands/executables;
- inject arbitrary MTConnect URLs or credentials;
- make the recorder scan an unrestricted network;
- target a normal FCP device that is not advertising the standalone-recorder capability; or
- bypass recorder-local validation.

Remote additions identify only opaque source IDs returned by the recorder's own latest scan. The recorder resolves/revalidates them locally before changing configuration.

Network discovery remains bounded to validated RFC1918 private IPv4 networks of `/24` or smaller with the existing address, timeout, redirect, and response-size limits.

## Source changes

Adding/removing a source changes future capture only. It does not delete previously recorded telemetry or erase historical checkpoints.

The managed recorder observes configuration changes without requiring a process restart. Safe capability metadata can be re-announced without exposing MTConnect source URLs or credentials.

## Capture remains local-first

Federation availability is not part of the recorder's local commit boundary.

The recorder first commits local capture/checkpoint state. Only checkpoint-committed data becomes eligible for Federation publication.

If relay/storage authority is unavailable, local recording continues and the durable publication outbox retries later. Federation outages do not move the MTConnect checkpoint backward and do not block normal polling.

## Federation storage publication

Pairing authority and storage authority are separate.

The current operational leader controls reviewed leader actions such as pairing. Recorder data publication, however, uses the existing **session-owner logical-storage authority** and current storage-control-plane assignments. Do not interpret leader transfer as automatically moving storage ownership/assignments.

The recorder never selects an arbitrary physical database. It publishes bounded deterministic chunks through authenticated Federation storage authority, which applies current assignment, fencing, acknowledgement, primary/replica routing, and manifest commit rules.

If exactly one ready logical-storage group exists, the recorder may select it automatically. With several groups, choose one explicitly:

```bash
python start_recorder.py --storage-group telemetry
```

If no ready logical-storage authority exists, local capture still continues while publication waits safely.

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

Do not delete these paths to fix a transient Federation or scan error. Use the product's explicit source controls, pairing/reconnect path, or documented reset/migration procedure.

For storage formats and the capture algorithm, see [`catalog/standalone-recorder_v2/README.md`](../catalog/standalone-recorder_v2/README.md).

## Related guides

- [Federation operations](federation_operations.md)
- [Operator guide](operator_guide.md)
- [Server setup](server_setup.md)
- [Troubleshooting](troubleshooting.md)
