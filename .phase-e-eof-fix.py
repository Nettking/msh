from pathlib import Path

path = Path("catalog/federation/former_primary_repair.py")
text = path.read_text(encoding="utf-8")
path.write_text(text.rstrip() + "\n", encoding="utf-8")
