from __future__ import annotations

from pathlib import Path

path = Path("catalog/federation/tests/cf7_acceptance/test_product_acceptance.py")
text = path.read_text(encoding="utf-8")
old = '    assert finished.location == "/onboarding?step=finish"\n'
new = '    assert finished.location == "/federation"\n'
if text.count(old) != 1:
    raise SystemExit(
        f"Expected one finish redirect assertion, found {text.count(old)}"
    )
path.write_text(text.replace(old, new, 1), encoding="utf-8")
print("CF7 acceptance redirect updated")
