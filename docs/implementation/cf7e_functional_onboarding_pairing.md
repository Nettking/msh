# CF7-E functional onboarding and pairing

Status: draft implementation discovered through physical browser testing.

## Problems corrected

The composed onboarding flow could report success while no benchmark existed,
no contribution candidate was available, and no real path existed for two
physical machines to join the same Federation. Entering a public device ID was
not sufficient and must never grant membership.

## Browser flow

The supported operator flow is intentionally simple:

1. Create or retain the stable identity.
2. On the first device, create a local Federation.
3. Generate a signed five-minute pairing code.
4. Paste the code into the second device.
5. Inspect each device.
6. Run or review the capability-specific checks that actually exist.
7. Choose independent contribution intent.
8. Finish setup.

A configured capability remains visible when unavailable. It is shown as
blocked with a missing prerequisite instead of silently disappearing. A device
with no configured capability is directed to device configuration and cannot
mark Benchmarks, Contributions, or Finish complete.

## Pairing authority

The pairing code is signed by the host device identity and contains existing
one-use coordinator enrollment and session-invitation material. The joining
device redeems it through the existing WebSocket relay and `RelayNodeClient`.
The public device ID identifies a key only; it grants no membership.

Pairing codes:

- use the `MSH1-` format;
- expire after five minutes by default;
- are bounded in size;
- are signed and verified before use;
- use one-use enrollment and invitation tokens;
- are never persisted;
- expose no write, storage, compute, or provider authority beyond the existing
  coordinator and session contracts.

Only the relay root and the public-safe trusted session binding are retained for
reconnection. Plaintext WebSocket pairing is intended only for a trusted private
LAN or VPN. An untrusted network requires WSS.

## Runtime composition

The default Compose application starts the existing authenticated relay on port
`8765` and shares its coordinator database with the Flask service. The joining
node keeps a persistent authenticated relay connection in a private event-loop
thread. The Federation read model continues to use public-safe projections.

## Verification

Focused tests cover:

- signed pairing-code round trips, tamper rejection and expiry;
- absence of one-use tokens from persisted state and object representations;
- actual enrollment and session join against the real relay/coordinator;
- two distinct node identities visible in the same authorized session;
- empty benchmark plans remaining incomplete;
- completed contribution state selecting Finish;
- configured but unavailable AI remaining a visible blocked candidate;
- read-only Federation and existing onboarding regressions;
- Ubuntu and Windows execution through the permanent CFI-2 gate.

This draft does not claim physical acceptance. The copy-and-paste flow still
requires verification on the user's two real MSH machines before CF7 can be
accepted and before CF8 starts.
