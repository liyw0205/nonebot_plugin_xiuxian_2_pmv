from __future__ import annotations

import logging
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from .repository import StickerRepository, _safe_pack_id
from .schemas import INSTALL_JOB_RETENTION_SECONDS, MAX_RETAINED_INSTALL_JOBS


_LOGGER = logging.getLogger(__name__)


class StickerApplication:
    """Catalog and in-process install-job owner; jobs are not persisted."""

    def __init__(
        self,
        repository: StickerRepository,
        *,
        thread_starter: Callable[[Callable[[], None]], None] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._repository = repository
        self._thread_starter = thread_starter or self._start_daemon_thread
        self._clock = clock or time.monotonic
        self._jobs: dict[str, dict[str, Any]] = {}
        self._job_finished_at: dict[str, float] = {}
        self._job_finished_order: dict[str, int] = {}
        self._completion_sequence = 0
        self._jobs_lock = threading.Lock()

    def _prune_terminal_jobs_locked(self) -> None:
        now = self._clock()
        terminal_ids = [
            job_id
            for job_id, job in self._jobs.items()
            if job.get("status") in {"complete", "error"}
        ]
        cutoff = now - INSTALL_JOB_RETENTION_SECONDS
        for job_id in terminal_ids:
            finished_at = self._job_finished_at.get(job_id, now)
            if finished_at <= cutoff:
                self._jobs.pop(job_id, None)
                self._job_finished_at.pop(job_id, None)
                self._job_finished_order.pop(job_id, None)

        retained = [
            (
                self._job_finished_at.get(job_id, now),
                self._job_finished_order.get(job_id, 0),
                job_id,
            )
            for job_id, job in self._jobs.items()
            if job.get("status") in {"complete", "error"}
        ]
        retained.sort()
        for _finished_at, _order, job_id in retained[:-MAX_RETAINED_INSTALL_JOBS]:
            self._jobs.pop(job_id, None)
            self._job_finished_at.pop(job_id, None)
            self._job_finished_order.pop(job_id, None)

    @staticmethod
    def _start_daemon_thread(target: Callable[[], None]) -> None:
        threading.Thread(target=target, daemon=True).start()

    def catalog(self, force_refresh: bool = False) -> dict[str, Any]:
        remote = self._repository.fetch_remote_catalog(force=bool(force_refresh))
        return self._repository.build_merged_catalog(remote)

    def start_install(self, pack_id: str, force: bool = False) -> dict[str, Any]:
        selected_id = _safe_pack_id(pack_id)
        if not selected_id:
            raise RuntimeError("无效表情包 ID")
        with self._jobs_lock:
            self._prune_terminal_jobs_locked()
            for job in self._jobs.values():
                if job.get("status") == "running" and job.get("pack_id") == selected_id:
                    return dict(job)
            job_id = uuid.uuid4().hex
            job = {
                "success": True,
                "job_id": job_id,
                "status": "running",
                "stage": "queued",
                "percent": 0,
                "message": "准备下载表情包",
                "pack_id": selected_id,
            }
            self._jobs[job_id] = job

        def update(**state: Any) -> None:
            with self._jobs_lock:
                current = self._jobs.get(job_id)
                if current is not None:
                    current.update(state)
                    if current.get("status") in {"complete", "error"}:
                        if job_id not in self._job_finished_at:
                            self._completion_sequence += 1
                            self._job_finished_at[job_id] = self._clock()
                            self._job_finished_order[job_id] = self._completion_sequence
                        self._prune_terminal_jobs_locked()

        def run() -> None:
            try:
                catalog = self._repository.install_pack(
                    selected_id,
                    force=bool(force),
                    progress=update,
                )
                pack = next(
                    (item for item in catalog.get("packs") or [] if item.get("id") == selected_id),
                    {},
                )
                update(
                    status="complete",
                    stage="complete",
                    percent=100,
                    message=f"{pack.get('name') or selected_id} 下载完成",
                    catalog=catalog,
                )
            except Exception as exc:  # noqa: BLE001
                _LOGGER.exception("stickers install job failed")
                update(
                    status="error",
                    stage="error",
                    message=f"安装表情包失败: {exc}",
                    error=str(exc),
                )

        self._thread_starter(run)
        return dict(job)

    def install_status(self, job_id: str) -> dict[str, Any] | None:
        with self._jobs_lock:
            self._prune_terminal_jobs_locked()
            job = self._jobs.get(str(job_id or ""))
            return dict(job) if job else None

    def resolve_sticker_path(self, token: str) -> Path | None:
        return self._repository.resolve_sticker_path(token)

    def resolve_file(self, pack_id: str, filename: str) -> Path | None:
        return self._repository.resolve_file(pack_id, filename)


__all__ = ["StickerApplication"]
