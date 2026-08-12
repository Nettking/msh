"""One definition of the device-local Federation state paths.

The Flask app, the relay service, and the fresh-reset tool each need to know
where the coordinator database lives when no environment override is set. They
previously carried separate literals that did not agree, so a non-Compose
installation could serve one location while the reset tool reasoned about
another.

This module is deliberately dependency-free so the reset tool can import it
without loading the storage or transport stacks.
"""

from __future__ import annotations

from typing import Final

#: Repository-relative default for the coordinator/relay control database.
#: Compose overrides this with ``FCP_FEDERATION_COORDINATOR_DATABASE`` so the
#: Flask and relay services share the mounted relay volume instead.
DEFAULT_COORDINATOR_DATABASE: Final = "data/federation/relay/control.sqlite3"

#: Mount point Compose uses for the relay-state volume.
DEFAULT_RELAY_STATE_ROOT: Final = "/var/lib/fcp-relay"

__all__ = ["DEFAULT_COORDINATOR_DATABASE", "DEFAULT_RELAY_STATE_ROOT"]
