"""Owned off-loop atomic storage phases on one control-plane runtime.

No connection or cursor crosses threads: the synchronous operation creates,
uses and closes its SQLite transactions on its executor thread. Cancellation
cannot abandon an already admitted writer or continue to the next phase.
"""
from __future__ import annotations

import asyncio
import contextvars
import threading
from functools import partial

_MAX_WAITING_STORAGE_PHASES = 64
_ADMISSION_LOCK = threading.Lock()


class _StorageWork:
    def __init__(self) -> None:
        self.loop = None
        self.lock = None
        self.waiting = 0

    async def run(self, operation, *args, **kwargs):
        loop = asyncio.get_running_loop()
        with _ADMISSION_LOCK:
            if self.loop is not loop:
                if self.loop is not None and (
                    not self.loop.is_closed() or self.lock.locked() or self.waiting
                ):
                    raise RuntimeError("storage control runtime belongs to another live loop")
                self.loop, self.lock = loop, asyncio.Lock()
        if self.waiting >= _MAX_WAITING_STORAGE_PHASES:
            raise RuntimeError("storage atomic phase admission capacity exceeded")
        self.waiting += 1
        try:
            await self.lock.acquire()
        finally:
            self.waiting -= 1
        try:
            # A Future (not a detached asyncio Task) also survives cancellation
            # of all owner tasks during ordinary event-loop shutdown.
            context = contextvars.copy_context()
            future = loop.run_in_executor(None, context.run, partial(operation, *args, **kwargs))
            try:
                return await asyncio.shield(future)
            except asyncio.CancelledError as cancelled:
                while not future.done():
                    try:
                        await asyncio.shield(future)
                    except asyncio.CancelledError:
                        continue  # Repeated close/cancel must still drain ownership.
                    except BaseException as error:
                        if not future.done():
                            raise RuntimeError("owned storage phase failed before drain") from error
                        break  # Retrieve and retain the original worker error below.
                try:
                    future.result()
                except BaseException as error:
                    raise cancelled from error
                raise
        finally:
            self.lock.release()


async def owned_storage_call(control_plane, operation, *args, **kwargs):
    # Client and builtin provider share the same control-plane object, so their
    # cold phases cannot stampede the same store. Admission does no I/O/await.
    with _ADMISSION_LOCK:
        work = getattr(control_plane, "_storage_async_work", None)
        if work is None:
            work = _StorageWork()
            control_plane._storage_async_work = work
    return await work.run(operation, *args, **kwargs)
