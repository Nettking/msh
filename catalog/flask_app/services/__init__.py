"""Service layer for the Flask web app."""

# Install the transferable-leader adapters before route/monitor modules import
# the creator-pinned compatibility classes. The installer is idempotent and
# performs no application/context I/O at import time.
from .federation_active_leader_runtime import install_active_leader_runtime

install_active_leader_runtime()

__all__ = ["install_active_leader_runtime"]
