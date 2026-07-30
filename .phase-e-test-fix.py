from pathlib import Path

path = Path("catalog/federation/tests/test_phase_e72_former_primary_transfer.py")
text = path.read_text(encoding="utf-8")
old_import = "from catalog.federation.storage_protocol import BatchIngestResult, StorageResponseEnvelope\n"
new_import = """from catalog.federation.storage_protocol import (\n    BatchIngestResult,\n    BatchIngestState,\n    StorageResponseEnvelope,\n)\n"""
old_result = """                content_hash="sha256:" + "f" * 64,\n                persisted=True,\n                duplicate=False,\n"""
new_result = """                content_hash="sha256:" + "f" * 64,\n                state=BatchIngestState.STORED,\n"""
if text.count(old_import) != 1:
    raise SystemExit("expected storage-protocol import was not found exactly once")
if text.count(old_result) != 1:
    raise SystemExit("expected forged result block was not found exactly once")
text = text.replace(old_import, new_import).replace(old_result, new_result)
path.write_text(text, encoding="utf-8")
