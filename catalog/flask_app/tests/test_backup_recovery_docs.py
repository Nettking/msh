from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DOC = ROOT / "docs" / "backup_recovery.md"
INDEX = ROOT / "docs" / "index.md"


def test_backup_recovery_contract_preserves_authority_boundaries() -> None:
    text = DOC.read_text(encoding="utf-8")

    assert "Copying `data/` to an arbitrary replacement Windows PC or user account is not a supported same-identity restore." in text
    assert "DPAPI" in text
    assert "Operational leader failover does not transfer immutable creator provenance" in text
    assert "Federation v1 does not provide a mechanism for another member to impersonate or rewrite the old creator." in text
    assert "create a new Federation" in text
    assert "Do **not** copy the old member's identity/Federation state" in text


def test_backup_recovery_requires_quiesced_non_destructive_snapshot() -> None:
    text = DOC.read_text(encoding="utf-8")

    assert "Quiesced backup requirement" in text
    assert "docker compose stop" in text
    assert "docker compose down -v" in text
    assert "A normal backup must never delete Docker volumes." in text
    assert "source-commit.txt" in text
    assert "relay-state.tgz" in text


def test_backup_recovery_is_discoverable_from_current_docs_index() -> None:
    index = INDEX.read_text(encoding="utf-8")

    assert "[Backup and recovery](backup_recovery.md)" in index
