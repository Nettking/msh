from pathlib import Path

path = Path("catalog/federation/former_primary_repair.py")
text = path.read_text(encoding="utf-8")
old = '''        expected = (
            item.session_id,
            item.group_id,
            item.dataset_id,
            item.schema_name,
            item.schema_version,
            item.item_id,
            item.idempotency_key,
            item.content_hash,
        )
        actual = (
            identity.session_id,
            identity.group_id,
            identity.dataset_id,
            identity.dataset_schema_name,
            identity.dataset_schema_version,
            identity.batch_id,
            identity.idempotency_key,
            identity.content_hash,
        )
'''
new = '''        expected = (
            item.dataset_id,
            item.schema_name,
            item.schema_version,
            item.item_id,
            item.idempotency_key,
            item.content_hash,
        )
        actual = (
            identity.dataset_id,
            identity.dataset_schema_name,
            identity.dataset_schema_version,
            identity.batch_id,
            identity.idempotency_key,
            identity.content_hash,
        )
'''
if text.count(old) != 1:
    raise SystemExit("expected identity-validation block was not found exactly once")
path.write_text(text.replace(old, new), encoding="utf-8")
