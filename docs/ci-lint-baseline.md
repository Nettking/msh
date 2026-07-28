# Phase 2 lint baseline

The Phase 2 CI workflow enables Ruff for the federation, node, and relay code.

The first repository-wide run identified nine pre-existing style-only findings in Phase 0/1 files: import ordering, `__all__` ordering, a dataclass default construction warning, a test `dict()` literal, and a test dictionary iteration style warning. These checks are temporarily excluded from the CI gate so compilation, tests, substantive lint checks, Compose validation, and diff hygiene remain enforceable.

The exclusions are intentionally listed in the workflow rather than applied globally. Remove them after a dedicated formatting-only cleanup has been reviewed and merged.
