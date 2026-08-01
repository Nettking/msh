# Phase F6.6.2 — Session route rendezvous

## Scope

F6.6.2 adds a bounded, session-scoped rendezvous mechanism for exchanging direct or circuit-v2 route descriptors between enrolled MSH nodes.

The implementation is additive:

- `SessionRouteDescriptor` binds one session, enrolled node identity, libp2p peer identity, multiaddr, X25519 receiver key, generation, issue time and expiry time.
- every descriptor is signed with the node's persistent Ed25519 identity;
- `SessionRouteRegistry` stores only bounded, process-local, short-lived rendezvous state;
- `RouteRendezvousRelayServer` extends the existing authenticated relay with `route.publish`, `route.resolve` and `route.withdraw`;
- `SessionRouteRendezvousClient` publishes, resolves, verifies and optionally registers routes with an existing direct-storage endpoint.

The existing `RelayServer` and `RelayNodeClient` remain unchanged. Deployments opt into the route-aware relay adapter explicitly.

## Security and authority boundary

A route descriptor is transport metadata only. It does not grant storage authority, provider authority, lease ownership, fencing authority, promotion authority or permission to perform a storage operation.

The relay accepts a route only when:

1. the WebSocket connection is authenticated as the descriptor owner;
2. the actor is still an active enrolled node;
3. the actor is a member of the descriptor's session;
4. the descriptor session matches the envelope session;
5. the descriptor node ID matches the authenticated actor;
6. the multiaddr binds the advertised libp2p peer ID;
7. the descriptor is signed by the enrolled Ed25519 public identity;
8. the issue and expiry window is within the bounded TTL and clock-skew limits;
9. the generation does not roll back or conflict with an existing generation.

Resolution repeats the coordinator membership check for both requester and target. A stale registry entry therefore cannot bypass a later membership removal or node revocation.

The resolving node verifies the returned descriptor again against the coordinator-provided public identity before registering it with a direct endpoint. This protects against route substitution inside the client boundary.

## Bounded state

The registry is deliberately non-durable and defaults to:

- maximum 2,048 active session/node routes;
- minimum route lifetime of 5 seconds;
- maximum route lifetime of 600 seconds;
- maximum accepted future clock skew of 30 seconds;
- one current generation for each `(session_id, node_id)` pair.

Expired entries are removed before publish, resolve and count operations. Nodes must republish after relay-process restart or route expiry.

## Protocol operations

### `route.publish`

The authenticated node submits one signed `msh.session_route.descriptor.v1` object. A repeated identical generation is idempotent. An older generation or a different descriptor with the same generation is rejected.

### `route.resolve`

A session member requests the current route for another member. The response contains the signed descriptor and the target's enrolled public identity. No result is returned for expired, withdrawn, non-member or unavailable routes.

### `route.withdraw`

The route owner removes its current route using a generation at least as new as the stored route. Older withdrawal generations are rejected.

## Validation

The focused Linux and Windows workflow compiles and lints the complete F6.6.2 boundary and covers:

- descriptor canonicalization and signature verification;
- node, peer, session and time binding;
- forged descriptor rejection;
- idempotent publication;
- generation rollback and conflict rejection;
- expiry cleanup;
- two enrolled members publishing and resolving a route through the authenticated relay;
- client-side signature verification and direct-peer registration;
- explicit withdrawal;
- cross-session resolution rejection.

The existing Phase 2 and F6 closeout workflows remain required before merge.

## Deferred

F6.6.2 does not deploy a public relay, prove DCUtR upgrades across restrictive physical NATs, automatically republish routes after process restart, select among several simultaneous routes, perform physical multi-network acceptance, delete F6 branches, close F6 or begin F7.
