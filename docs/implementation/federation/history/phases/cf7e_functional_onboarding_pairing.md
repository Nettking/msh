# CF7-E functional onboarding and pairing

> **Status: historical and non-authoritative.** This document records the delivery that made signed physical-device pairing functional. Its branch-era wording and mandatory benchmark/contribution sequence are superseded. Current mandatory onboarding is `Identity -> Federation -> Inspect -> finish setup`; benchmarks and contribution decisions are optional follow-up work.

## Problems corrected at this stage

The composed onboarding flow could report success while no benchmark existed, no contribution candidate was available, and no real path existed for two physical machines to join the same Federation. Entering a public device ID was not sufficient and could not grant membership.

## Historical browser flow

The delivery-stage operator sequence was:

1. Create or retain stable identity.
2. Create a local Federation on the first device.
3. Generate a signed five-minute pairing code.
4. Paste the code into the second device.
5. Inspect each device.
6. Run or review available capability-specific checks.
7. Choose independent contribution intent.
8. Finish setup.

A configured but unavailable capability remained visible as blocked rather than disappearing.

## Pairing authority delivered

The pairing code was signed by the host device identity and carried existing one-use coordinator enrollment and session-invitation material. The joining device redeemed it through the authenticated relay and `RelayNodeClient`.

Pairing codes:

- use the `MSH1-` format;
- expire after five minutes by default;
- are bounded in size;
- are signed and verified before use;
- use one-use enrollment and invitation tokens;
- are never persisted;
- grant no authority beyond existing coordinator and session contracts.

Only the relay root and public-safe trusted session binding were retained for reconnect. Plaintext WebSocket pairing was limited to trusted private LAN or VPN use; untrusted networks require WSS.

## Runtime composition delivered

The default Compose application started the authenticated relay on port `8765` and shared coordinator state with Flask. The joining node maintained a persistent authenticated relay connection in a private event-loop thread. Federation read models remained public-safe projections.

## Historical verification

Focused tests covered:

- pairing-code round trip, tamper rejection, and expiry;
- absence of one-use tokens from persisted state and object representations;
- real enrollment and session join against relay/coordinator;
- distinct identities in one authorized session;
- incomplete empty benchmark plans;
- configured unavailable AI remaining visible and blocked;
- read-only Federation and onboarding regressions;
- Ubuntu and Windows execution through the permanent CFI-2 gate.

This delivery did not claim physical acceptance. Complete physical CF7 acceptance remains false until the current acceptance campaign is completed and reviewed.

Use the [current capability-first plan](../../active/capability_first_federation_plan.md) for supported product behavior.