"""Framework-neutral Federation product projection service."""

from __future__ import annotations

from .detail_pages import DetailProjectionMixin
from .models import FederationPage, FederationViewModel
from .overview_page import OverviewProjectionMixin
from .service_core import (
    FederationProjectionCore,
    ProjectionAdapters as ProjectionAdapters,
)


class FederationProjectionService(
    OverviewProjectionMixin,
    DetailProjectionMixin,
    FederationProjectionCore,
):
    """Build safe product view models without granting or changing authority."""

    def project(
        self,
        page: FederationPage | str,
        *,
        include_technical: bool = False,
    ) -> FederationViewModel:
        page = FederationPage(page)
        builders = {
            FederationPage.OVERVIEW: self.overview,
            FederationPage.THIS_DEVICE: self.this_device,
            FederationPage.DEVICES: self.devices,
            FederationPage.SERVICES: self.services,
            FederationPage.BENCHMARKS: self.benchmarks,
            FederationPage.STORAGE: self.storage,
            FederationPage.JOBS: self.jobs,
            FederationPage.ACTIVITY: self.activity,
            FederationPage.SETTINGS: self.settings,
        }
        return builders[page](include_technical=include_technical)


__all__ = ["FederationProjectionService", "ProjectionAdapters"]
