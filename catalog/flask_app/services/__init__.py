"""Service layer for the Flask web app."""

from .federation_active_leader_runtime import install_active_leader_runtime

install_active_leader_runtime()

__all__ = ["install_active_leader_runtime"]
