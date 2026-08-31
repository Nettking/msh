"""Host-resource admission for supported JSONL upload staging."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from werkzeug.datastructures import FileStorage

from catalog.federation.host_resources import HostResourceRefused, ProcessResourceAdmission
from catalog.federation.process_resource_admission import PROCESS_RESOURCE_ADMISSION

from .data_upload_service import DataUploadError, DataUploadService


def enqueue_with_resource_admission(
    service: DataUploadService,
    files: Iterable[FileStorage],
    *,
    admission: ProcessResourceAdmission | None = None,
) -> dict[str, Any]:
    """Reserve the bounded staging transaction before the service writes it.

    The reservation covers the upload's declared maximum staging growth plus the
    directory/owner/file entries it may create. Multipart parsing can happen before
    this boundary and is not claimed by this reservation. The actual staged bytes
    remain visible to the next filesystem measurement after the reservation is
    released, while normal publication uses ``os.replace`` rather than duplicating
    the batch.
    """

    controller = admission or PROCESS_RESOURCE_ADMISSION
    try:
        if isinstance(service, DataUploadService):
            # DataUploadService admits the staging + SQLite metadata transaction
            # itself. Keeping this handoff inside the service avoids nesting the
            # route reservation around asynchronous publication work.
            return service.enqueue(files, resource_admission=controller)
        with controller.reserve(
            service.staging_root,
            bytes_required=service.max_total_bytes,
            inodes_required=service.max_files + 2,
        ):
            return service.enqueue(files)
    except HostResourceRefused as exc:
        raise DataUploadError(
            "upload-resource-pressure",
            "The upload was not started because host storage is under resource pressure.",
        ) from exc


__all__ = ["enqueue_with_resource_admission"]
