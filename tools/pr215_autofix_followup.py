from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file_path = Path(path)
    text = file_path.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected exactly one match in {path}, found {count}: {old!r}")
    file_path.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "catalog/runner/data_filtering.py",
    ").encode(\"utf-8\")\n        for represented_date",
    ").encode()\n        for represented_date",
)
replace_once(
    "catalog/runner/data_filtering.py",
    "        records_scanned = 0\n        with destination_file.open(\"w\", encoding=\"utf-8\") as dst:\n",
    "        with destination_file.open(\"w\", encoding=\"utf-8\") as dst:\n",
)
