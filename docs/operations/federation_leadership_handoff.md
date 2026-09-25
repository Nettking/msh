# Authenticated one-shot Federation leadership handoff

Use this operation when the current operational leader must explicitly hand
leadership to an existing, connected session member. It uses the current
leader's retained Ed25519 identity. It does not enroll nodes, create identities,
change session membership, or replace the managed node's live Relay connection.

The operator must already have local access to that leader's protected identity
directory. Never copy private keys into command arguments or evidence. Use a
verified TLS Relay endpoint, or an explicitly enabled loopback endpoint in the
same trusted host/network namespace. Ordinary private-network plaintext URLs
are not accepted by this command.

Record the current session, leader, term, explicit target identity and connected
target before issuing the command. Supply a new request ID for the intended
operation and preserve the public JSON receipt:

```sh
python -m catalog.node.leadership \
  --state-directory /protected/current-leader/device \
  --relay-url wss://relay.example.invalid \
  --actor-node-id node-current-leader \
  --session-id session-existing \
  --target-node-id node-existing-flask \
  --expected-term 2 \
  --request-id operator-handoff-unique-id \
  --timeout-seconds 300
```

The timeout must be positive, finite and no greater than 600 seconds. The
command makes one connection and one submission. It never retries automatically.
It loads an existing identity; missing/incomplete identity state fails closed.
The identity store retains its ordinary bounded local file-lock and permission
checks; no new NodeState database or membership is created by this command.

The signed Relay challenge includes the complete handoff scope: session, target,
expected term and request ID. The Relay verifies the enrolled public identity
before invoking its existing coordinator handoff. It never registers this
one-shot socket as the node's managed connection. The transactional coordinator
requires an active current-leader actor and a connected member target. The
expected-term check is made inside the authoritative transition transaction;
the replicated release facade retains its existing journal/quorum boundary.

A successful operation appends the ordinary coordinator-authored
`session.leader.changed` event and audit entry, increments the term, and fans
the event out to existing clients. A repeated or stale operation is rejected;
it cannot cause a second transition even if the old leader later returns.
An older Relay rejects the extended signature rather than treating this as an
ordinary login and replacing the managed connection.

A timeout or connection error means the outcome is **unconfirmed**, not that
the operation was rolled back. Inspect authoritative leadership and the matching
audit/event once before deciding on further action. Do not automatically retry.
After success, verify exactly one leader, unchanged enrollment/session/membership,
continued managed connectivity, and the intended Flask administrator login path.
