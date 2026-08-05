from __future__ import annotations

from pathlib import Path


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    if old not in text:
        if new in text:
            return
        raise SystemExit(f"Patch anchor not found in {path}: {old[:100]!r}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


acceptance = Path(
    "catalog/federation/tests/cf7_acceptance/test_product_acceptance.py"
)
replace_once(
    acceptance,
    '    assert finished.location == "/onboarding?step=finish"\n',
    '    assert finished.location == "/federation"\n',
)

service = Path(
    "catalog/flask_app/services/capability_startup_transition_service.py"
)
replace_once(
    service,
    '''        return intents

    @staticmethod
    def _compatibility_mode(intents: Mapping[str, str]) -> str:
''',
    '''        return intents

    def _completion_intents(self) -> dict[str, str]:
        """Preserve fully reviewed choices, otherwise finish with safe defaults."""

        has_persisted_intents = getattr(
            self.contribution_service,
            "has_persisted_intents",
            None,
        )
        if not callable(has_persisted_intents) or not has_persisted_intents():
            return self._fast_start_intents()
        try:
            return self._current_intents()
        except FederationOperationError as exc:
            if exc.code in {
                "contribution-benchmarks-required",
                "contribution-inspection-expired",
                "startup-transition-benchmarks-required",
                "startup-transition-contributions-required",
            }:
                return self._fast_start_intents()
            raise

    @staticmethod
    def _compatibility_mode(intents: Mapping[str, str]) -> str:
''',
)
replace_once(
    service,
    '''        intents = self._fast_start_intents()
        self._write_compatibility_settings(intents)
''',
    '''        intents = self._completion_intents()
        self._write_compatibility_settings(intents)
''',
)

print("CF7-G acceptance and reviewed-choice preservation updated")
