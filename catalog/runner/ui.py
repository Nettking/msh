"""Runner CLI display and input helpers."""

from __future__ import annotations

from typing import Iterable


def print_numbered_menu(title: str, options: Iterable[str]) -> None:
    """Print a numbered menu to stdout."""
    print(f"\n{title}", flush=True)
    for index, option in enumerate(options, start=1):
        print(f"{index}) {option}", flush=True)


def prompt_menu_choice(max_choice: int, prompt: str) -> int:
    """Prompt the user for a numeric menu choice."""
    while True:
        raw = input(prompt).strip()
        if not raw.isdigit():
            print("Please enter a number.", flush=True)
            continue

        value = int(raw)
        if 1 <= value <= max_choice:
            return value

        print(f"Please choose a value between 1 and {max_choice}.", flush=True)
