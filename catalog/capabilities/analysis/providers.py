"""Provider reports for analysis scheduling.

There is exactly one availability model: the F8.2 provider health authority.
This adapter presents it to the scheduler; it invents no reports of its own, and
a node with no approved, fresh, enrolled provider record simply offers nothing.
"""

from __future__ import annotations

from ..provider_reports import ProviderResourceReport


class FederatedProviderReportSource:
    """Adapt the existing F8.2 health authority to analysis scheduling."""

    def __init__(self, health, *, actor_node_id: str) -> None:
        self.health = health
        self.actor_node_id = actor_node_id

    def reports(
        self,
        *,
        session_id: str,
        capability_type: str,
    ) -> tuple[ProviderResourceReport, ...]:
        return tuple(
            self.health.fresh_reports(
                session_id=session_id,
                actor_node_id=self.actor_node_id,
                capability_type=capability_type,
            )
        )


__all__ = ["FederatedProviderReportSource"]
