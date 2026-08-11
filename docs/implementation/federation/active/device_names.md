# Federation-scoped device names

Status: **active implementation contract**

Reviewed: **2026-08-11 Europe/Oslo**

## Purpose

A stable cryptographic `node_id` is necessary for identity and authority, but it is a poor operational label. When several trusted computers participate in one Federation, generic labels such as `Trusted FCP device 1` make update failures, provider approvals, and offline-device diagnosis unnecessarily ambiguous.

The Federation therefore supports a separate operator-assigned device name, for example `Office laptop`, `GPU workstation`, or `MTConnect recorder PC`.

## Authority and identity boundary

Device names are **Federation-scoped metadata**. They never replace or modify the Ed25519-derived `node_id` and therefore do not change:

- cryptographic identity;
- membership;
- routing;
- provider enrollment;
- storage assignment;
- compute/execution authority; or
- software-update authority.

A trusted member may assign or change the name of **its own** device. The Federation/session creator may additionally name any current member, including an offline member. Each accepted decision is appended to the authoritative Federation event history as `node.display-name.changed` with the target `node_id` and public-safe `display_name`.

Readers apply a naming event only when its authenticated actor is either the target device itself or the current session creator. A third member cannot rename another device merely by appending a similarly shaped event.

The creator identity must be recoverable from authoritative session history as well as coordinator status. This is required because remotely paired clients may receive a status representation that does not expose `created_by_node_id`; leader-assigned names must still resolve identically on every member.

## Product behavior

Every trusted member may edit its own name from Federation **This device**. The leader may additionally edit member names from Federation **Devices**, including while the target device is offline. Names are case-insensitively unique inside one Federation and generic labels such as `This FCP device` and `Trusted FCP device` are reserved.

The latest authorized name follows the stable node ID across trusted projections and should be used anywhere an operator needs to identify a computer, including:

- This device, Devices, and Federation overview;
- software update checks and rollout results;
- shared service metadata;
- provider/contribution approvals; and
- storage-provider ownership.

The underlying `node_id` remains available where technical identity is useful.

## Remote publication

A locally hosted Federation creator writes through the coordinator directly. A remotely paired member publishes its own naming event through its already-authenticated outbound relay client using the existing bounded `event.append` protocol. The web application does not expose a general remote coordinator mutation facade: this path is limited to the validated self-name event.

The read side replays authoritative session history, so a name written by the leader or by the device itself is visible to every trusted member after normal Federation refresh/replay. Naming does not depend on the target being online after the event has been persisted.

## Safety

Names are bounded public metadata. They reject control characters, credentials/secrets, strings that expose non-public backend/storage locations, and reserved generic labels. Normal GET projections remain read-only; only explicit CSRF-protected naming POSTs publish an event.

## Acceptance criteria

1. The leader can name a current Federation member while it is online or offline.
2. Every trusted member can name its own device from **This device**.
3. A non-leader cannot rename another member.
4. The assigned name is durable in authoritative event history.
5. Every trusted member resolves the same latest authorized name for the same `node_id`.
6. A leader-authored name remains visible on remotely paired members even when their coordinator status omits the creator field.
7. A self-authored naming event is accepted only for the authenticated actor's own `node_id`.
8. A third-party spoofed naming event cannot rename another member.
9. Names are unique within a Federation and do not alter node identity.
10. Software Updates uses the Federation name so an offline/error row identifies the affected computer immediately.
11. Services, provider approvals, and storage ownership use the same resolved name rather than inventing independent labels.
