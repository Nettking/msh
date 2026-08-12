from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if text.count(old) != 1:
        raise SystemExit(f"{label} shape changed")
    return text.replace(old, new)


update = Path("catalog/flask_app/services/federation_update_events.py")
text = update.read_text(encoding="utf-8")
text = replace_once(
    text,
    """from catalog.federation.software_update import (\n    APPROVED_BRANCH,\n    APPROVED_REPOSITORY,\n    OID_RE,\n    UpdateInspection,\n)\n""",
    """from catalog.federation.control_commands import (\n    ControlCommandEnvelope,\n    correlated_event_request_id as _event_request_id,\n    ensure_bounded_json,\n    stamp_utc as _stamp,\n)\nfrom catalog.federation.software_update import (\n    APPROVED_BRANCH,\n    APPROVED_REPOSITORY,\n    OID_RE,\n    UpdateInspection,\n)\n""",
    "update import",
)
text = replace_once(
    text,
    """def _stamp(value: datetime) -> str:\n    return value.astimezone(timezone.utc).isoformat().replace(\"+00:00\", \"Z\")\n\n\ndef _parse_stamp(value: object) -> datetime:\n    if not isinstance(value, str):\n        raise TypeError(\"malformed_timestamp\")\n    parsed = datetime.fromisoformat(value.replace(\"Z\", \"+00:00\"))\n    if parsed.tzinfo is None or parsed.utcoffset() is None:\n        raise ValueError(\"malformed_timestamp\")\n    return parsed.astimezone(timezone.utc)\n\n\ndef _bounded(value: object) -> None:\n    encoded = json.dumps(\n        value,\n        sort_keys=True,\n        separators=(\",\", \":\"),\n        ensure_ascii=False,\n        allow_nan=False,\n    ).encode(\"utf-8\")\n    if len(encoded) > MAX_EVENT_BYTES:\n        raise ValueError(\"update_event_too_large\")\n""",
    """def _bounded(value: object) -> None:\n    ensure_bounded_json(\n        value,\n        max_bytes=MAX_EVENT_BYTES,\n        error_code=\"update_event_too_large\",\n    )\n""",
    "update helper",
)
text = replace_once(
    text,
    """    if not request_id or len(request_id) > 128:\n        raise ValueError(\"malformed_request_id\")\n    if not OID_RE.fullmatch(target_commit):\n        raise ValueError(\"malformed_target\")\n    targets = tuple(dict.fromkeys(target_node_ids))\n    if not targets or len(targets) > MAX_TARGETS:\n        raise ValueError(\"malformed_targets\")\n    if any(\n        not isinstance(item, str) or not item or len(item) > 512\n        for item in targets\n    ):\n        raise ValueError(\"malformed_targets\")\n    if expires_at <= created_at or (expires_at - created_at).total_seconds() > 900:\n        raise ValueError(\"invalid_lifetime\")\n    value: dict[str, object] = {\n        \"schema\": EVENT_SCHEMA,\n        \"request_id\": request_id,\n        \"repository\": APPROVED_REPOSITORY,\n        \"branch\": APPROVED_BRANCH,\n        \"target_commit\": target_commit,\n        \"target_node_ids\": list(targets),\n        \"created_at\": _stamp(created_at),\n        \"expires_at\": _stamp(expires_at),\n    }\n""",
    """    envelope = ControlCommandEnvelope.issue(\n        request_id=request_id,\n        target_node_ids=target_node_ids,\n        created_at=created_at,\n        expires_at=expires_at,\n        max_lifetime=timedelta(minutes=15),\n        max_targets=MAX_TARGETS,\n    )\n    if not OID_RE.fullmatch(target_commit):\n        raise ValueError(\"malformed_target\")\n    value: dict[str, object] = {\n        \"schema\": EVENT_SCHEMA,\n        **envelope.payload_fields(),\n        \"repository\": APPROVED_REPOSITORY,\n        \"branch\": APPROVED_BRANCH,\n        \"target_commit\": target_commit,\n    }\n""",
    "update command_payload",
)
text = replace_once(
    text,
    """    request_id = value.get(\"request_id\")\n    target = value.get(\"target_commit\")\n    targets = value.get(\"target_node_ids\")\n    if not isinstance(request_id, str) or not request_id or len(request_id) > 128:\n        raise ValueError(\"malformed_request_id\")\n    if (\n        value.get(\"repository\") != APPROVED_REPOSITORY\n        or value.get(\"branch\") != APPROVED_BRANCH\n    ):\n        raise ValueError(\"unapproved_source\")\n    if not isinstance(target, str) or not OID_RE.fullmatch(target):\n        raise ValueError(\"malformed_target\")\n    if (\n        not isinstance(targets, list)\n        or not 1 <= len(targets) <= MAX_TARGETS\n        or any(\n            not isinstance(item, str) or not item or len(item) > 512\n            for item in targets\n        )\n    ):\n        raise ValueError(\"malformed_targets\")\n    created = _parse_stamp(value.get(\"created_at\"))\n    expires = _parse_stamp(value.get(\"expires_at\"))\n    now = datetime.now(timezone.utc)\n    if (\n        created > now + timedelta(minutes=1)\n        or expires <= now\n        or (expires - created).total_seconds() > 900\n    ):\n        raise ValueError(\"expired_or_invalid_request\")\n""",
    """    ControlCommandEnvelope.parse_payload(\n        value,\n        max_lifetime=timedelta(minutes=15),\n        max_targets=MAX_TARGETS,\n        require_unique_targets=False,\n    )\n    target = value.get(\"target_commit\")\n    if (\n        value.get(\"repository\") != APPROVED_REPOSITORY\n        or value.get(\"branch\") != APPROVED_BRANCH\n    ):\n        raise ValueError(\"unapproved_source\")\n    if not isinstance(target, str) or not OID_RE.fullmatch(target):\n        raise ValueError(\"malformed_target\")\n""",
    "update validator",
)
text = replace_once(
    text,
    """def _event_request_id(prefix: str, request_id: str, node_id: str) -> str:\n    digest = hashlib.sha256(\n        f\"{request_id}\\0{node_id}\".encode()\n    ).hexdigest()[:32]\n    return f\"{prefix}-{digest}\"\n\n\n""",
    "",
    "update event request id",
)
update.write_text(text, encoding="utf-8")

capability = Path("catalog/flask_app/services/federation_capability_requests.py")
text = capability.read_text(encoding="utf-8")
text = replace_once(
    text,
    """from catalog.federation.onboarding_models import (\n    BenchmarkState,\n    ContributionActivationState,\n    ContributionDesiredState,\n    ContributionPolicyState,\n)\n""",
    """from catalog.federation.control_commands import (\n    ControlCommandEnvelope,\n    correlated_event_request_id as _event_request_id,\n    ensure_bounded_json,\n    parse_utc_stamp as _parse_stamp,\n    stamp_utc as _stamp,\n)\nfrom catalog.federation.onboarding_models import (\n    BenchmarkState,\n    ContributionActivationState,\n    ContributionDesiredState,\n    ContributionPolicyState,\n)\n""",
    "capability import",
)
text = replace_once(
    text,
    """def _stamp(value: datetime) -> str:\n    return value.astimezone(timezone.utc).isoformat().replace(\"+00:00\", \"Z\")\n\n\ndef _parse_stamp(value: object) -> datetime:\n    if not isinstance(value, str):\n        raise TypeError(\"malformed_timestamp\")\n    parsed = datetime.fromisoformat(value.replace(\"Z\", \"+00:00\"))\n    if parsed.tzinfo is None or parsed.utcoffset() is None:\n        raise ValueError(\"malformed_timestamp\")\n    return parsed.astimezone(timezone.utc)\n\n\ndef _bounded(value: object) -> None:\n    encoded = json.dumps(\n        value,\n        sort_keys=True,\n        separators=(\",\", \":\"),\n        ensure_ascii=False,\n        allow_nan=False,\n    ).encode(\"utf-8\")\n    if len(encoded) > MAX_EVENT_BYTES:\n        raise ValueError(\"capability_event_too_large\")\n""",
    """def _bounded(value: object) -> None:\n    ensure_bounded_json(\n        value,\n        max_bytes=MAX_EVENT_BYTES,\n        error_code=\"capability_event_too_large\",\n    )\n""",
    "capability helper",
)
text = replace_once(
    text,
    """    if not request_id or len(request_id) > 128:\n        raise ValueError(\"malformed_request_id\")\n    targets = tuple(dict.fromkeys(target_node_ids))\n    if not 1 <= len(targets) <= MAX_TARGETS:\n        raise ValueError(\"malformed_targets\")\n    if any(\n        not isinstance(item, str) or not item or len(item) > 512\n        for item in targets\n    ):\n        raise ValueError(\"malformed_targets\")\n    if expires_at <= created_at or expires_at - created_at > COMMAND_TTL:\n        raise ValueError(\"invalid_lifetime\")\n    value: dict[str, object] = {\n        \"schema\": EVENT_SCHEMA,\n        \"request_id\": request_id,\n        \"actions\": list(REQUEST_ACTIONS),\n        \"target_node_ids\": list(targets),\n        \"created_at\": _stamp(created_at),\n        \"expires_at\": _stamp(expires_at),\n    }\n""",
    """    envelope = ControlCommandEnvelope.issue(\n        request_id=request_id,\n        target_node_ids=target_node_ids,\n        created_at=created_at,\n        expires_at=expires_at,\n        max_lifetime=COMMAND_TTL,\n        max_targets=MAX_TARGETS,\n    )\n    value: dict[str, object] = {\n        \"schema\": EVENT_SCHEMA,\n        **envelope.payload_fields(),\n        \"actions\": list(REQUEST_ACTIONS),\n    }\n""",
    "capability request_payload",
)
text = replace_once(
    text,
    """    request_id = value.get(\"request_id\")\n    targets = value.get(\"target_node_ids\")\n    actions = value.get(\"actions\")\n    if not isinstance(request_id, str) or not request_id or len(request_id) > 128:\n        raise ValueError(\"malformed_request_id\")\n    if actions != list(REQUEST_ACTIONS):\n        raise ValueError(\"unsupported_actions\")\n    if (\n        not isinstance(targets, list)\n        or not 1 <= len(targets) <= MAX_TARGETS\n        or len(set(targets)) != len(targets)\n        or any(\n            not isinstance(item, str) or not item or len(item) > 512\n            for item in targets\n        )\n    ):\n        raise ValueError(\"malformed_targets\")\n    created = _parse_stamp(value.get(\"created_at\"))\n    expires = _parse_stamp(value.get(\"expires_at\"))\n    now = datetime.now(timezone.utc)\n    if (\n        created > now + timedelta(minutes=1)\n        or expires <= now\n        or expires <= created\n        or expires - created > COMMAND_TTL\n    ):\n        raise ValueError(\"expired_or_invalid_request\")\n""",
    """    ControlCommandEnvelope.parse_payload(\n        value,\n        max_lifetime=COMMAND_TTL,\n        max_targets=MAX_TARGETS,\n        require_unique_targets=True,\n    )\n    actions = value.get(\"actions\")\n    if actions != list(REQUEST_ACTIONS):\n        raise ValueError(\"unsupported_actions\")\n""",
    "capability validator",
)
text = replace_once(
    text,
    """def _event_request_id(prefix: str, request_id: str, node_id: str) -> str:\n    digest = hashlib.sha256(f\"{request_id}\\0{node_id}\".encode()).hexdigest()[:32]\n    return f\"{prefix}-{digest}\"\n\n\n""",
    "",
    "capability event request id",
)
capability.write_text(text, encoding="utf-8")
