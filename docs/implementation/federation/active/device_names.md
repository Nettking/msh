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

Only the Federation/session creator may assign or change a name. The decision is appended to the authoritative Federation event history as `node.display-name.changed` with the target `node_id` and public-safe `display_name`.

Readers apply a name only when the event was authored by the current session creator. A similarly shaped event written by another member must not override the displayed name.

## Product behavior

The leader may edit names from the Federation **Devices** page, including while the target device is offline. Names are case-insensitively unique inside one Federation and generic labels such as `This FCP device` and `Trusted FCP device` are reserved.

The current name follows the stable node ID across trusted read-only projections and should be used anywhere an operator needs to identify a computer, including:

- Devices and Federation overview;
- software update checks and rollout results;
- shared service metadata;
- provider/contribution approvals; and
- storage-provider ownership.

The underlying `node_id` remains available where technical identity is useful.

## Safety

Names are bounded public metadata. They reject control characters, credentials/secrets, and strings that would expose non-public backend/storage locations. Normal GET projections remain read-only; only the explicit leader POST appends a naming event.

## Acceptance criteria

1. The leader can name a current Federation member while it is online or offline.
2. A non-leader cannot assign a name.
3. The assigned name is durable in authoritative event history.
4. Every trusted member resolves the same latest leader-authored name for the same `node_id`.
5. A member-authored spoofed naming event cannot override the leader's name.
6. Names are unique within a Federation and do not alter node identity.
7. Software Updates uses the Federation name so an offline/error row identifies the affected computer immediately.
8. Services, provider approvals, and storage ownership use the same resolved name rather than inventing independent labels.
