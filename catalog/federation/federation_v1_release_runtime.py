"""Release composition for the Federation v1 replicated runtime.

The implementation is kept separate from the reviewed consensus core. This
last composition point makes automatic legacy recovery use the same bounded
authenticated quorum acquisition as explicit Federation creation and places the
private credential-generation marker beside the replicated auth database so the
Flask process can observe an atomic restore across the container boundary.
"""

from __future__ import annotations

from .control_plane_legacy_migration import LegacyMigrationError
from .control_plane_replication import ControlPlaneError
from .control_plane_runtime import AUTH_GENERATION_FILE
from .federation_v1_runtime import FederationV1Runtime


class FederationV1ReleaseRuntime(FederationV1Runtime):
    """Exact runtime used by the product relay and physical-readiness tests."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if self.human_auth_database is not None:
            self._auth_generation_path = (
                self.human_auth_database.parent / AUTH_GENERATION_FILE
            )

    def _attempt_existing_federation_bootstrap(self) -> None:
        with self._lifecycle_lock:
            self._resume_witnessed_bootstrap()

    def _resume_witnessed_bootstrap(self) -> None:
        if self.bootstrap_federation_id is None or self.bootstrap_session_id is None:
            return
        existing_id = self.node.state.get("federation_id")
        if existing_id is not None and existing_id != self.bootstrap_federation_id:
            raise LegacyMigrationError(
                "configured migration Federation ID conflicts with replicated authority"
            )
        if self.ready:
            return

        # A transient voter-socket startup race is not authority. Every retry
        # is a normal authenticated election and must independently prove 2/3.
        self._acquire_bootstrap_leadership()
        manifest = self._matching_witness_quorum(
            self.bootstrap_federation_id,
            self.bootstrap_session_id,
        )
        self._bootstrap_from_witness_manifest(manifest)
        matched = self.node.synchronize(self.transport)
        if matched + 1 < self.node.quorum:
            raise ControlPlaneError(
                "witnessed migration lost voter quorum before materialization"
            )
        self.materialize()
        self._restore_human_credentials_if_available()
        self._sync_human_credentials_if_due(force=True)


__all__ = ["FederationV1ReleaseRuntime"]
