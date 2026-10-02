# Publication transaction repair review

## Candidate scope

This isolated candidate is based on `6e154311dc7378890691eea352657be27a54f54e` in the F6-derived diagnostic worktree. It does not modify the active F6 checkout, runtime, systemd unit, bindings, or P12 evidence. This candidate is not qualified or deployed; it must use the existing Federation v1 release qualification and deployment procedure before any live activation.

## Findings and corrections

- The durable sender called `SQLiteOutbox.acknowledge()` after a remote commit. A SQLite error could occur before the outbox transaction completed or after the completion was already durable. The old path did not reconcile that ambiguous result. The sender now reads back the row after a SQLite or OS write error and counts completion only when state is `completed` and the row ID, session, destination, schema, idempotency key, and content hash all match the attempted row. A matching `pending` row records a retryable failure and remains ordered/fenced. A readback error or identity/state mismatch never reports completion; the original acknowledgment exception remains primary, with its type/code and batch correlation in the structured event.
- Regression coverage exercises a failure before durable completion, an exception after durable completion, failure of the verification read, failure to persist retry state, mismatches for each immutable identity column, and a due normal retry preserving content and idempotency. In every local-write failure case the row remains pending and is never reported complete without durable proof. The provider attempt remains idempotent across replay.
- The response path now requires the authenticated actor, session, and provider identity to match the pending request. Late responses cannot satisfy a newer retry correlation. A relay delivery result is successful only when it explicitly reports `delivered: true`; response-send refusal remains a failed publication attempt.
- Progress-observer exceptions after a durable acknowledgment no longer reclassify a completed row as pending. Request-handler cancellation/failure and provider, authority-ingest, and authority-response stages have separate correlated diagnostics. Untrusted diagnostic identifiers are bounded and non-printable values are replaced with `invalid`; request payloads and credentials are not logged.
- The isolated transaction-boundary test uses the durable sender/outbox, relay storage client and authority handler with an idempotent local provider. It distinguishes an authority-ingest wait timeout from response-delivery refusal and confirms that a later normal retry completes idempotently. This is diagnostic/correction evidence for the isolated candidate, not a retrospective classification of the 2026-10-01 14:13 live timeout.

## Validation evidence

- Coupled regression suite: **55 passed**. Log: `test-evidence/publication-repair-final-coupled-20261002T064824Z.pytest.log`; SHA-256 `2824A537F3C71E4A40D620BA42BFF5F22184A7054E2F53062A34FF21216D8840`.
- Project-pinned Ruff 0.16.3 check: **passed**. Log: `test-evidence/ruff-pinned-20261002T064842Z.log`; SHA-256 `A4443AFDCFB6D7363ADB285762515CCF7CF50473B1A05C20C1A50F6BED4D26B0`.
- `compileall` for the changed source/test files: **passed**. `git diff --check`: **passed**.
- Earlier, unmodified reproductions and failure logs remain in `test-evidence/`, including `sqlite-outbox-ack-boundary-prefx-20261002T0610Z.pytest.log`; they have not been overwritten or relabeled.

## Acceptance boundary

The changes address isolated publication behavior and diagnostic gaps. The live 14:13 transaction remains unresolved because its retained evidence lacks the stage correlation added here. No live code was changed during P12, and isolated passing tests do not establish live MSH-to-Nettking publication or qualify this candidate. The F6 run remains bound to its original F6 candidate and must finish all scheduled measurements, assertions, and finish records before any change to that installation.
