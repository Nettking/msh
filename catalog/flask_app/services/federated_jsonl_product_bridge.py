"""Federated JSONL bridge with stable staged-chunk lifecycle hardening.

The implementation lives in ``_federated_jsonl_product_bridge_impl``.  This
module keeps the public import surface unchanged while overriding the remaining
staged-chunk lifecycle operations that must never dereference a persisted
lexical path directly after admission/staging.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO

from . import _federated_jsonl_product_bridge_impl as _impl

FederatedJsonlPublishResult = _impl.FederatedJsonlPublishResult


class FederatedJsonlProductBridge(_impl.FederatedJsonlProductBridge):
    """Bridge whose staged reads/stats/deletes remain below the pinned cache root."""

    def _staged_components(self, value: object) -> tuple[Path, str]:
        path = Path(str(value))
        try:
            relative = path.relative_to(self.cache_root)
        except ValueError as exc:
            raise _impl.FederationValidationError(
                "unsafe-federated-jsonl-staging-target",
                "content.chunk_index",
                "staged chunk path escaped the managed cache root",
            ) from exc
        if len(relative.parts) < 2 or relative.parts[0] != "remote":
            raise _impl.FederationValidationError(
                "unsafe-federated-jsonl-staging-target",
                "content.chunk_index",
                "staged chunk path is outside the managed remote cache",
            )
        return relative.parent, relative.name

    @contextmanager
    def _open_staged(self, value: object) -> Iterator[BinaryIO]:
        relative_parent, name = self._staged_components(value)
        with self._stable_directory(self.cache_root, relative_parent) as directory:
            with directory.open_read(name) as handle:
                yield handle

    def _stat_staged(self, value: object):
        relative_parent, name = self._staged_components(value)
        with self._stable_directory(self.cache_root, relative_parent) as directory:
            return directory.stat(name)

    def _unlink_staged(self, value: object, *, missing_ok: bool = True) -> None:
        relative_parent, name = self._staged_components(value)
        try:
            with self._stable_directory(self.cache_root, relative_parent) as directory:
                directory.unlink(name, missing_ok=missing_ok)
                directory.fsync()
        except FileNotFoundError:
            if not missing_ok:
                raise

    def _physical_staged_usage(self) -> tuple[int, int]:
        remote = self.cache_root / "remote"
        if not remote.is_dir():
            return 0, 0
        total = 0
        files = 0
        for staged in remote.rglob("*.chunk"):
            try:
                total += self._stat_staged(staged).st_size
            except FileNotFoundError:
                continue
            files += 1
            if total >= self._staged_quota_bytes() or files >= self._staged_file_quota():
                break
        return total, files

    def _cleanup_orphaned_staged_chunks(self) -> int:
        with _impl._STAGED_CACHE_LOCK:
            remote = self.cache_root / "remote"
            if not remote.is_dir():
                return 0
            maximum = self._positive_bound(
                "FEDERATED_JSONL_MAX_BATCHES_PER_SYNC",
                _impl._DEFAULT_MAX_REMOTE_BATCHES,
                _impl._HARD_MAX_REMOTE_BATCHES,
            )
            scan_limit = min(
                self._staged_file_quota(), maximum * _impl._STAGED_CACHE_SCAN_MULTIPLIER
            )
            removed = 0
            scanned = 0
            with self._connect() as connection:
                for staged in remote.rglob("*.chunk"):
                    if removed >= maximum or scanned >= scan_limit:
                        break
                    scanned += 1
                    referenced = connection.execute(
                        "SELECT 1 FROM seen_batches WHERE chunk_path=? LIMIT 1",
                        (str(staged),),
                    ).fetchone()
                    if referenced is not None:
                        continue
                    try:
                        self._unlink_staged(staged)
                        removed += 1
                    except FileNotFoundError:
                        continue
            return removed

    def _consume_staged_rows(self, rows: Iterable[object]) -> None:
        staged_rows = tuple(rows)
        if not staged_rows:
            return
        with self._write_connection() as connection:
            connection.executemany(
                """
                UPDATE seen_batches SET chunk_path=NULL
                WHERE session_id=? AND group_id=? AND dataset_id=? AND batch_id=?
                """,
                (
                    (
                        str(row["session_id"]),
                        str(row["group_id"]),
                        str(row["dataset_id"]),
                        str(row["batch_id"]),
                    )
                    for row in staged_rows
                ),
            )
        for row in staged_rows:
            if row["chunk_path"]:
                self._unlink_staged(row["chunk_path"])

    def _record_materialized_file(
        self,
        *,
        session_id: str,
        dataset_id: str,
        producer: str,
        relative_path: str,
        file_sha256: str,
        source_mtime_ns: int,
        target: Path,
        size: int,
        rows: Iterable[object],
    ) -> None:
        staged_rows = tuple(rows)
        with self._write_connection() as connection:
            connection.execute(
                """
                INSERT INTO materialized_files(
                    session_id,dataset_id,producer_node_id,relative_path,
                    file_sha256,source_mtime_ns,target_path,size_bytes,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(session_id,dataset_id) DO UPDATE SET
                    producer_node_id=excluded.producer_node_id,
                    relative_path=excluded.relative_path,
                    file_sha256=excluded.file_sha256,
                    source_mtime_ns=excluded.source_mtime_ns,
                    target_path=excluded.target_path,
                    size_bytes=excluded.size_bytes,
                    updated_at=excluded.updated_at
                """,
                (
                    session_id,
                    dataset_id,
                    producer,
                    relative_path,
                    file_sha256,
                    source_mtime_ns,
                    str(target),
                    size,
                    _impl._stamp(self.clock()),
                ),
            )
            connection.executemany(
                """
                UPDATE seen_batches SET chunk_path=NULL
                WHERE session_id=? AND group_id=? AND dataset_id=? AND batch_id=?
                """,
                (
                    (
                        str(row["session_id"]),
                        str(row["group_id"]),
                        str(row["dataset_id"]),
                        str(row["batch_id"]),
                    )
                    for row in staged_rows
                ),
            )
        for row in staged_rows:
            if row["chunk_path"]:
                self._unlink_staged(row["chunk_path"])

    def _try_materialize(
        self,
        *,
        session_id: str,
        dataset_id: str,
        file_sha256: str,
        encoded_sha256: str,
    ) -> bool:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM seen_batches
                WHERE session_id=? AND dataset_id=? AND file_sha256=? AND encoded_sha256=?
                ORDER BY chunk_index
                """,
                (session_id, dataset_id, file_sha256, encoded_sha256),
            ).fetchall()
        if not rows:
            return False
        local_duplicate = self._local_duplicate(file_sha256)
        remote_duplicate = self._remote_duplicate(session_id, dataset_id, file_sha256)
        if local_duplicate is not None or remote_duplicate is not None:
            self._consume_staged_rows(rows)
            return False
        first = rows[0]
        chunk_count = int(first["chunk_count"])
        if len(rows) != chunk_count or [int(row["chunk_index"]) for row in rows] != list(
            range(chunk_count)
        ):
            return False
        if any(not row["chunk_path"] for row in rows):
            return False
        producer = str(first["producer_node_id"])
        relative_path = str(first["relative_path"])
        source_mtime_ns = int(first["source_mtime_ns"])
        declared_encoded_size = int(first["encoded_size"])
        declared_file_size = int(first["file_size"])
        with self._connect() as connection:
            current = connection.execute(
                "SELECT file_sha256,source_mtime_ns FROM materialized_files WHERE session_id=? AND dataset_id=?",
                (session_id, dataset_id),
            ).fetchone()
        if current is not None:
            current_key = (int(current["source_mtime_ns"]), str(current["file_sha256"]))
            candidate_key = (source_mtime_ns, file_sha256)
            if candidate_key <= current_key:
                return False

        quota = self._mirror_quota_bytes()
        retained = self._mirrored_bytes(excluding=(session_id, dataset_id))
        if retained + declared_file_size > quota:
            self._set_state(
                mirror_bytes=retained,
                mirror_quota_bytes=quota,
                mirror_quota_reached=True,
            )
            return False

        staged_size = 0
        for row in rows:
            staged_size += self._stat_staged(row["chunk_path"]).st_size
        if staged_size != declared_encoded_size:
            raise _impl.FederationValidationError(
                "federated-jsonl-encoded-size-mismatch",
                "content.encoded_size",
                "staged gzip size does not match committed metadata",
            )

        target = self._target_path(producer, relative_path)
        relative_parent = target.parent.relative_to(self.mirror_root)
        try:
            with self._stable_directory(self.mirror_root, relative_parent) as target_directory:
                if (
                    target_directory.is_file(target.name)
                    and target_directory.stat(target.name).st_size == declared_file_size
                    and target_directory.sha256(target.name) == file_sha256
                ):
                    self._record_materialized_file(
                        session_id=session_id,
                        dataset_id=dataset_id,
                        producer=producer,
                        relative_path=relative_path,
                        file_sha256=file_sha256,
                        source_mtime_ns=source_mtime_ns,
                        target=target,
                        size=declared_file_size,
                        rows=rows,
                    )
                    return True
        except FileNotFoundError:
            pass

        requirements = (
            (self.cache_root, declared_encoded_size, _impl._JSONL_MATERIALIZATION_INODES),
            (
                target.parent,
                declared_file_size,
                _impl._JSONL_MATERIALIZATION_INODES + len(relative_parent.parts),
            ),
        )
        with self._reserve_many(requirements) as materialization_reservations:
            if not materialization_reservations:
                raise _impl.FederationOperationError(
                    "federated-jsonl-resource-identity-unavailable",
                    "atomic resource admission returned no backing resource",
                )
            cache_reservation = materialization_reservations[0]
            target_reservation = materialization_reservations[-1]
            with self._stable_directory(self.cache_root) as cache_directory, self._stable_directory(
                self.mirror_root, relative_parent, create=True
            ) as target_directory:
                self._assert_stable_reserved_resource(cache_directory, cache_reservation)
                self._assert_stable_reserved_resource(target_directory, target_reservation)
                with cache_directory.temporary_file(prefix="fcp-encoded-") as (
                    encoded_name,
                    encoded,
                ):
                    bounded_encoded = _impl._BoundedWriter(encoded, declared_encoded_size)
                    for row in rows:
                        with self._open_staged(row["chunk_path"]) as chunk:
                            while data := chunk.read(1024 * 1024):
                                bounded_encoded.write(data)
                    encoded.flush()
                    _impl.os.fsync(encoded.fileno())
                    if cache_directory.stat(encoded_name).st_size != declared_encoded_size:
                        raise _impl.FederationValidationError(
                            "federated-jsonl-encoded-size-mismatch",
                            "content.encoded_size",
                            "reconstructed gzip size does not match committed metadata",
                        )
                    if cache_directory.sha256(encoded_name) != encoded_sha256:
                        raise _impl.FederationValidationError(
                            "federated-jsonl-encoded-hash-mismatch",
                            "content.encoded_sha256",
                            "reconstructed gzip hash does not match committed metadata",
                        )
                    with target_directory.temporary_file(prefix="fcp-raw-") as (
                        raw_name,
                        raw,
                    ):
                        bounded_raw = _impl._BoundedWriter(raw, declared_file_size)
                        digest = _impl.hashlib.sha256()
                        size = 0
                        with cache_directory.open_read(encoded_name) as encoded_source, _impl.gzip.GzipFile(
                            fileobj=encoded_source, mode="rb"
                        ) as compressed:
                            while data := compressed.read(1024 * 1024):
                                size += len(data)
                                digest.update(data)
                                bounded_raw.write(data)
                        raw.flush()
                        _impl.os.fsync(raw.fileno())
                        if size != declared_file_size:
                            raise _impl.FederationValidationError(
                                "federated-jsonl-file-size-mismatch",
                                "content.file_size",
                                "reconstructed JSONL size does not match committed metadata",
                            )
                        if f"sha256:{digest.hexdigest()}" != file_sha256:
                            raise _impl.FederationValidationError(
                                "federated-jsonl-file-hash-mismatch",
                                "content.file_sha256",
                                "reconstructed JSONL hash does not match committed metadata",
                            )
                        target_directory.replace(raw_name, target.name)
                        target_directory.fsync()
                self._record_materialized_file(
                    session_id=session_id,
                    dataset_id=dataset_id,
                    producer=producer,
                    relative_path=relative_path,
                    file_sha256=file_sha256,
                    source_mtime_ns=source_mtime_ns,
                    target=target,
                    size=size,
                    rows=rows,
                )
                return True


__all__ = ["FederatedJsonlProductBridge", "FederatedJsonlPublishResult"]
