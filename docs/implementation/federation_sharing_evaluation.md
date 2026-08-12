# Federation sharing evaluation

Status: **evaluation and finding record**; no runtime behaviour is changed by this document.

Baseline: `main` at `79a074b` (after #257 and #258).

Reviewed: **2026-08-12 Europe/Oslo**

## Purpose

This document answers three questions about the Federation as it exists on the
current baseline:

1. what state is actually shared between trusted devices today;
2. what state is device-local and should become shared;
3. which inconsistencies exist between the shared model, the code, and the
   canonical documentation.

Findings are classified by severity. Two correctness defects were reproduced
against the real `SessionCoordinator`; they are marked **reproduced** and the
reproduction is described inline. Everything else is a code or documentation
reading with file references.

This document authorizes no deletion, migration, protocol change, or acceptance
claim. It does not change `catalog/federation/tests/cf7_acceptance/scenarios.json`.

## Part 1 — What is shared today

Sharing happens through four distinct mechanisms. They have different authority
models, different durability, and different safety limits.

### 1.1 The authoritative session event log

`catalog/federation/persistence.py` `session_events` is the ordered, append-only
spine. It is per-`session_id`, protected by `UNIQUE(session_id, actor_node_id,
request_id)`, and has **no compaction, snapshotting, or retention**.

Shared through it:

| Domain | Event types | Owner |
| --- | --- | --- |
| Membership | `node.joined`, `node.left`, `node.revoked`, `session.created` | coordinator |
| Device naming | `node.display-name.changed` | `catalog/federation/device_names.py` |
| Capability health | `capability.health.changed` | provider health |
| Storage authority | `storage.group.created`, `storage.provider.registered`, `storage.assignment.changed`, `storage.leader.granted`/`revoked` | `storage_control_plane.py` |
| Jobs and artifacts | `job-submitted`, `job-queued`, `ownership-*`, `attempt-*`, `result-committed`, `artifact-registered`, `artifact-published`, `grant-*` | `catalog/capabilities/` |
| Software updates | `software.update.check.requested`/`reported`, `software.update.apply.requested`/`reported` | `federation_update_events.py` |
| Recorder control | `recorder.control.scan.requested`/`reported`, `recorder.control.sources.requested`/`reported` | `recorder_control_events.py` |
| Leader capability requests | `capability.onboarding.requested`/`reported` | `federation_capability_requests.py` (#257) |
| **Operator knowledge** | `knowledge.document.upserted`/`deleted` | `federation_knowledge_service.py` |

### 1.2 Federation logical storage — recorder telemetry

The recorder publishes checkpoint-gated batches under dataset schema
`fcp.mtconnect.observations` v1. Members discover them only through the
coordinator-owned committed manifest, verify session membership, dataset
identity, canonical size, content hash, and the allowlisted schema, and
materialize into `data/federation/shared/{batches,telemetry}` under an explicit
quota (`DEFAULT_MAX_TOTAL_BYTES` 2 GB, `DEFAULT_MAX_MATERIALIZATION_BYTES`
512 MB, `telemetry_mirror.py:40-42`).

### 1.3 Federation logical storage — generic JSONL corpus (#258)

`catalog/federation/shared_file_storage.py` adds dataset schema
`fcp.federation.jsonl-file-chunk` v1: actor-bound, content-addressed, gzip,
24 KiB chunks, authorized by Federation membership rather than recorder
authority. `federated_jsonl_product_bridge.py` publishes local files and
materializes remote ones into `data/federation/shared/jsonl-files/<producer>/`,
where the pre-Federation runner/orchestrator stack picks them up as ordinary
recursive JSONL input.

Scope: **every `*.jsonl` under `data/`**, recursively, minus two prefixes
(`federation/`, `sources/mtconnect_recorder/jsonl/`). This includes
`data/uploads/**` — browser-uploaded files.

### 1.4 Operator knowledge documents

Five collections are projected from the event log and cached to local JSON:

| Collection | Document schema | Local compatibility cache |
| --- | --- | --- |
| `operator-confirmations` | `fcp.operator_confirmations.v1` | `data/operator_confirmations/confirmations.json` |
| `first-part-checks` | `fcp.first_part_checks.v1` | `data/quality/first_part_checks.json` |
| `quality-outcomes` | `fcp.quality_outcomes.v1` | `data/quality/quality_outcomes.json` |
| `machine-notes` | `fcp.machine_notes.v1` | `data/source_config/machine_notes.json` |
| `operator-strategies` | `fcp.operator_strategy_records.v3` | `data/operator_strategy_records/operator_strategies.json` |

The local JSON files are compatibility caches and migration inputs only; the
event log is authoritative. Derived surfaces (support cards, strategy
comparison, OSL/SysML export, recommender artifacts) already read the federated
strategy service, so they inherit shared state correctly.

## Part 2 — What is device-local today

| Local state | Location | Deliberate? |
| --- | --- | --- |
| Human accounts, roles, password hashes, session/salt secrets | `data/auth/` | Yes — documented as a separate security domain |
| Machine and vibration-sensor inventory | `data/source_config/machines_and_sensors.json` | Not stated anywhere |
| Capability config (recorder sources, Ollama endpoint) | `data/capabilities/config.json` | Yes — endpoints are private by policy |
| Server setup settings | `data/server_setup/server_settings.json` | Yes |
| Device inspection snapshots | `device_inspection_snapshot` table | Yes — evidence is not authority |
| Benchmark results and skip decisions | onboarding SQLite | Yes — but only aggregate counts are shared, see Part 4 item 4 |
| Recorder runtime state, checkpoints, scans | `data/source_state/` | Yes |
| Workflow sessions, filters, caches | `catalog/runner/`, `results/` | Yes |
| Analysis outputs, playback exports | `results/` | Yes — derived |
| Data upload records and analysis jobs | `data/imports/uploads.sqlite3` | Yes — but the *files* are shared, see I-3 |
| OSL/SysML export, recommender artifacts | `data/sysml/`, `data/osl/` | Yes — derived |

## Part 3 — Inconsistencies

### Severity 1 — reproduced correctness defects

**I-1. Reverting or restoring a shared knowledge document fails hard.**

`FederationKnowledgeRepository._request_id()`
(`federation_knowledge_service.py:124-134`) derives the event `request_id`
deterministically from `(collection, document_id, operation, content_hash)`,
where `content_hash` covers the *document*. The event *payload* built at
`:315-323` additionally carries `"changed_at": _stamp(self._now())`, which is
different on every call.

`CoordinatorStore._append_event_tx` (`persistence.py:877-894`) treats a repeated
`request_id` as idempotent only when the stored `content_hash` matches, and
otherwise raises `idempotency-conflict`. Because `changed_at` always differs,
any second write of previously published content is a conflict, not a
no-op.

Reproduced against a real coordinator:

```text
write A       : OK -> ['coolant on']
write B       : OK -> ['coolant off']
revert to A   : FederationOperationError: request_id: event request ID was
                reused with different content (idempotency-conflict)

delete rec-1  : True
re-add same   : FederationOperationError: ... (idempotency-conflict)
```

Consequences:

- toggling `mark_reusable` off and on again, or any edit that restores an
  earlier value of an operator strategy record, raises an unhandled
  `FederationOperationError`;
- a deleted document can never be recreated with its original content by the
  same device — the tombstone is permanent for that content;
- the same conflict fires whenever two devices converge back onto content one
  of them already published, which is the normal outcome of an offline edit
  reconciled by `load_payload`'s timestamp comparison (`:353-362`).

The append-only collections (confirmations, first-part checks, quality
outcomes, machine notes) use fresh `uuid4` ids and are not currently reachable;
operator strategies are editable and deletable and are reachable.

Fix direction: either drop `changed_at` from the payload so the event is truly
content-addressed, or include it in the `request_id` so each write is a new
request. The two cannot both stay as they are.

**I-2. Shared knowledge breaks on the coordinator device past 10,000 events.**

`_events()` (`federation_knowledge_service.py:183-195`) falls back to
`coordinator.replay(..., last_applied_revision=0)` for the local/creator
device. That is the *unpaged* API: `CoordinatorStore.replay_events`
(`persistence.py:979-994`) raises `RevisionGapError("replay-window-too-large")`
as soon as `current_revision - last_applied_revision > MAX_REPLAY_EVENTS`
(10,000, `persistence.py:48`).

Members are unaffected — `ResilientPairingRelayRuntime.session_events` goes
through `RelayNodeClient.request_replay`, which pages
(`catalog/node/client.py:700-725`, up to `MAX_REPLAY_PAGES_PER_PASS` 1024).

So once any Federation accumulates 10,000 session events — of *any* type:
storage, updates, recorder control, or knowledge upserts themselves — the
Knowledge, Assist, First part, Outcomes, and Machine notes pages fail on the
Federation creator while continuing to work on every member. Reproduced by
lowering `MAX_REPLAY_EVENTS` to 5:

```text
note 0: OK
note 1: OK
note 2: OK
note 3: RevisionGapError: use paged replay for more than 5 events
        (replay-window-too-large)
```

The log has no compaction or retention, so this ceiling is reached, not
approached. `replay_page` already exists and is what this call site should use.

### Severity 2 — boundary and policy contradictions

**I-3. The JSONL bridge is general file sharing, which `architecture.md` says
the product does not do.**

`docs/architecture.md:192-194` states:

> This path is intentionally not general file sharing. Arbitrary uploads and
> other peer files require a separately authorized object grant, object catalog,
> type policy, and materializer before they can be exposed to another device.

`FederatedJsonlProductBridge` is wired into the reconnect monitor
(`federation_pairing_install.py:405-410`) and runs on every sync pass. It
enumerates every `*.jsonl` under `data/` (`:310-318`) minus `federation/` and
`sources/mtconnect_recorder/jsonl/`, and publishes it to Federation storage.
`data/uploads/` is not excluded, so a file uploaded through the Data upload page
under the `data.upload` permission is republished to every trusted device with
no separate grant, no per-file consent, and no operator-visible confirmation.

There is a type policy (`.jsonl` only) and a materializer, but no object grant
and no opt-in. Either the documentation or the default needs to change; today
they contradict each other.

**I-4. The JSONL mirror has no quota; the telemetry mirror does.**

`architecture.md:188-190` describes federated reads entering "a quota-limited,
content-addressed local mirror". That is true of `FederatedTelemetryMirror`
(2 GB total / 512 MB materialization). `federated_jsonl_product_bridge.py`
contains no quota, size cap, or eviction of any kind — only per-sync page and
chunk limits, which bound *rate*, not *total*. Every member therefore
accumulates every other member's entire JSONL corpus without bound.

The two mirrors also differ in verification depth: the telemetry path validates
the allowlisted recorder schema and per-observation shape; the JSONL path
verifies hashes, sizes, and offsets but performs no schema check, by design.

**I-5. Human identity is local; the permissions it gates are Federation-wide.**

Human accounts, roles, password hashes, and the session/password secrets live in
`data/auth/` per device (`docs/human-authentication.md`). This is a deliberate
security-domain separation and the doc says so.

The inconsistency is downstream of that decision:

- `federation.manage`, `pairing.manage`, and `software.update`
  (`catalog/flask_app/auth/policy.py:10-44`) authorize actions whose effects are
  Federation-wide — approving providers, issuing pairing grants, rolling an
  update across every device — but are enforced only against the local account
  store on the device where the browser happens to be;
- deactivating a person, or stripping their admin role, propagates to no other
  device; there is no revocation path across the Federation;
- **no human identity is carried into Federation state at all.** No Federation
  service reads `current_user`; every shared write — knowledge documents,
  update rollouts, provider approvals, recorder control — is attributed solely
  to `actor_node_id`. The coordinator `audit_log`
  (`persistence.py:447-457`) can answer "which device did this" and never
  "which person did this";
- an *N*-device Federation requires *N* independent first-admin bootstraps, and
  a person with no account on device B is still affected by what an operator on
  device A rolls out to it.

`README.md` describes this as "Human authentication and central RBAC". The
policy table is central; the user store is not. A reader will read "central" as
Federation-wide.

**I-6. Session event payloads are never redaction-checked, and free-text
operator content now flows through them.**

`catalog/federation/redaction.py` classifies credentials
(`SECRET_FIELD_NAMES`, `SECRET_TEXT_PREFIXES`) and private locations
(`NONPUBLIC_LOCATION_KEYS` — `host`, `port`, `url`, `address`, `path`, `dsn`;
`NONPUBLIC_LOCATION_PREFIXES` — `http://`, `postgres:`, `s3:`; plus an IPv4
regex). `redact_secrets` is applied to relay envelope fields
(`protocol.py:40`, `:146`), capability announcements (`node/state.py:1121`),
status projections (`persistence.py:2514-2611`), and the coordinator audit log
(`persistence.py:485-503`).

It is **not** applied to `session_events.payload_json`.
`_append_event_tx` (`persistence.py:864-870`) runs the payload through
`_canonical_json`, which validates JSON-ness and a 48 KB size bound and nothing
else. So the audit record *about* an event append is redacted while the
authoritative payload it describes is not.

That asymmetry was low-risk while every event payload was machine-generated and
schema-bound. Knowledge documents changed that: `machine-notes`,
`quality-outcomes`, `operator-confirmations`, and `operator-strategies` carry
operator free text straight into the shared log. An operator who types an agent
URL, a LAN address, or a credential into a machine note writes it verbatim into
Federation state that is replayed to every trusted device, has no compaction or
retention, and cannot be erased — `delete_document` appends a tombstone that
hides the document from the projection while the original upsert event remains
in the log forever.

Fix direction: apply the existing `redact_secrets` classification to knowledge
document payloads at the `_upsert` boundary, or reject rather than redact so the
operator learns the note was not accepted.

**I-7. Shared knowledge is not degradation-tolerant.**

`_default_context()` guards context acquisition with a bare
`except Exception` and a comment that disconnected devices retain the local
cache (`federation_knowledge_service.py:154-157`). Nothing guards `_events()`
or `_append()`. A device that is paired but whose relay is momentarily
unreachable raises `FederationOperationError("pairing-relay-disconnected")` out
of `ensure_connected` and up through the route. `operator_strategy_routes.index`
catches only `OperatorStrategyError` (`:29-33`), so the page returns 500 rather
than degrading to the cache the design says exists.

### Severity 3 — model fragmentation

**I-8. Shared documents reference a device-local identity domain.**

Machine notes, first-part checks, quality outcomes, and operator strategies all
carry `machine_id` (and strategies carry `sensor_ids`). Those ids come from
`SourceInventoryService`, backed by `data/source_config/machines_and_sensors.json`
— which is **not shared**. The form dropdowns are populated from the local
inventory (`machine_notes.html:7`), and the list view renders the raw id
(`machine_notes.html:15`: `machine {{ item.machine_id or 'unassigned' }}`).

A note captured on device A therefore arrives on device B as a shared document
pointing at a `uuid4` that B has never seen, and B cannot author a matching one.
Even on A, the list shows the opaque id rather than the machine name.

**I-9. At least five machine/source identity domains coexist, none reconciled:**

1. `machines_and_sensors.json` machine ids (`uuid4`, device-local);
2. `data/capabilities/config.json` recorder source ids (device-local);
3. recorder scan source ids returned by a recorder's latest bounded scan
   (per-recorder, opaque, used by recorder control);
4. Federation storage `dataset_id` (recorder dataset identity, `_DATASET_PATTERN`
   in `recorder_payload.py:29`);
5. `machine_id` inside the telemetry observations themselves, derived from the
   MTConnect stream (`telemetry_mirror.py:696`, `:829`).

Shared knowledge uses (1); shared telemetry uses (4) and (5); shared recorder
control uses (3). Nothing maps between them.

### Severity 4 — documentation drift

**I-10. #257 and #258 shipped with no documentation.** Neither commit touches
`docs/`. Three sharing mechanisms are absent from every canonical document:

- federated operator knowledge documents — not in `architecture.md`,
  `federated_session_network.md`, `data_contract.md`, or
  `releases/federation_v1_scope.md`;
- the federated JSONL corpus — same, and it directly contradicts
  `architecture.md:192-194` (I-3);
- leader-issued capability requests — same.

`architecture.md` is stamped "Reviewed: 2026-08-11" and its "Recorder and
telemetry data flow" section still describes recorder publication as the only
Federation data path.

**I-11. Human authentication is missing from the docs index and the v1 scope.**
`docs/human-authentication.md` exists and is linked from `README.md`, but not
from `docs/index.md`. `releases/federation_v1_scope.md` is stamped
"Reviewed: 2026-08-11" — after #255 merged human auth — and does not mention
human accounts, roles, or permissions anywhere in its supported boundary.

### Severity 5 — minor

- **I-12.** `/assist` passes `storage_scope` to the template
  (`operator_support_routes.py:38`) but `assist.html` never renders it. Four of
  the five shared collections advertise their scope; confirmations do not.
- **I-13.** Templates default the scope label to `'Federation shared'`
  (`knowledge.html:6`, `first_part.html:6`, `machine_notes.html:3`,
  `quality_outcomes.html:3`). The label fails *open*: a missing value claims
  shared storage. `'Local cache'` is the safe default.
- **I-14.** `FEDERATED_JSONL_MAX_FILE_BYTES` (`shared_file_storage.py:36-38`) is
  `CHUNK_BYTES * MAX_CHUNKS * 16` but is used as the bound for `encoded_size`
  (`:255-259`), which `chunk_count` can only express up to
  `CHUNK_BYTES * MAX_CHUNKS`. The `chunk_count == expected_count` check fails
  closed, so this is a misleading error rather than a hole — but the constant is
  16× wrong for the field it guards.
- **I-15.** Default coordinator database paths diverge: `app.py:170-176` falls
  back to `data/federation/relay/control.sqlite3` and
  `catalog/relay/service.py:63` agrees, but `docker-compose.yml` pins both
  services to `/var/lib/fcp-relay/control.sqlite3`. Consistent under Compose;
  divergent for any non-Compose invocation.

### Test coverage gap

**I-16.** `catalog/flask_app/services/federation_knowledge_service.py` has **no
tests**. Nothing under `catalog/` references `FederationKnowledgeRepository`,
`federation_knowledge`, or the knowledge event types outside the three service
modules and four templates that use them. This is the mechanism that carries all
operator knowledge across the Federation, and both Severity 1 defects live in
it.

## Part 4 — What should become shared

Ranked by value over cost.

**1. Source inventory, as a public-safe projection.** This is the highest-value
gap and it unblocks I-8. `machines_and_sensors.json` carries `mtconnect_url`,
`vpn_test_host`, and `vpn_test_port` — exactly the fields
`catalog/federation/redaction.py` and the envelope validators forbid from
Federation payloads. So this is not a federate-the-file change: share a
projection of `{id, name, machine_type, controller, notes}` plus sensor
`{id, name, machine_id, axis, unit}` through the existing knowledge-document
mechanism, and keep endpoints device-local. Shared knowledge then resolves to
machine *names* on every device.

**2. Human attribution on Federation writes.** Independent of whether the user
store itself is ever federated, every shared mutation should carry the acting
human's stable identifier alongside `actor_node_id`, so the coordinator audit
log can answer "who". This is a small, additive change to the event payloads and
`audit_log`, and it removes the sharpest edge of I-5 without needing a
distributed identity design.

**3. Federated human accounts and role grants.** The real fix for I-5, and a
genuine design decision rather than a defect to patch: either a Federation-wide
user directory, or per-device accounts with Federation-wide role grants and
revocation. Both need a threat model — the current doc explicitly separates the
security domains, and `federation_v1_scope.md` lists "public anonymous
participation" and "decentralized consensus" as out of scope. Recommend
treating this as a scoped post-v1 track, with item 2 shipped first.

**4. Benchmark evidence and inspection summaries.** Today only aggregate counts
cross the Federation (`report_payload`, `federation_capability_requests.py:146-191`);
the evidence stays in each device's onboarding SQLite. The post-v1 roadmap
already asks for "benchmark comparison and expiry visibility", and #257 built
the request/report channel that would carry it. Share bounded, versioned
benchmark *results* — still evidence, still not authority.

**5. Recorder source selection, as Federation-visible state.** A trusted member
can already mutate another recorder's sources through the bounded control path,
but the resulting selection is only visible in the report event, not as durable
projected state. Sharing the current selection closes the loop between "I
changed it" and "this is what it is".

**Not recommended to share:** analysis outputs under `results/`, playback
exports, telemetry caches, workflow session state, and the OSL/SysML and
recommender artifacts. All are derived — share the inputs and let each device
regenerate. Private endpoints, credentials, and recorder URLs must stay local
under the existing redaction policy.

## Part 5 — Suggested order of work

1. Fix I-1 and I-2 in `federation_knowledge_service.py`, and add the missing
   test module (I-16) covering revert, delete-then-recreate, offline
   reconciliation, and a log larger than one replay page.
2. Apply the existing redaction classification to knowledge document payloads
   (I-6). This is small, and it is harder to fix later — the log has no
   compaction, so anything accepted today is permanent.
3. Make knowledge reads degrade to the local cache on transport failure (I-7).
4. Decide I-3: either gate JSONL publication behind an explicit operator opt-in
   and per-directory scope, or amend `architecture.md` to describe what the
   product now actually does. Add a mirror quota either way (I-4).
5. Ship human attribution on Federation writes (Part 4 item 2), then reconcile
   the "central RBAC" wording (I-5).
6. Share the source-inventory projection (Part 4 item 1), which resolves I-8 and
   starts on I-9.
7. Reconcile the documentation set (I-10, I-11) in one pass rather than per
   feature.
8. Sweep the minor items (I-12 … I-15).

Steps 1-3 are contained defects. Steps 4 and 5 are product decisions and should
not be made inside a bug fix.
