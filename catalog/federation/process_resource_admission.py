"""Process-wide host-resource admission shared by bounded FCP writers.

A ProcessResourceAdmission prevents concurrent in-process writers from
independently spending the same measured filesystem headroom only when they use
the same controller. Keep one default controller here for product/runtime
writers that can coexist in one process. Callers may still inject a dedicated
controller in tests.
"""

from __future__ import annotations

from .host_resources import ProcessResourceAdmission

PROCESS_RESOURCE_ADMISSION = ProcessResourceAdmission()

__all__ = ["PROCESS_RESOURCE_ADMISSION"]
