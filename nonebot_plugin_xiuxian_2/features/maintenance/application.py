"""Startup receipt compaction: bounded, resumable, then a guarded one-time reclaim.

Called from ``runtime._maintain_database`` under the existing
``xiuxian_startup_database_maintenance`` switch.  It rewrites the historical
snapshot columns declared in :mod:`schemas` (never replay columns), records
progress in a JSON state file, and -- when rows were actually converted -- asks
:mod:`infrastructure.database.maintenance` for a guarded, once-only ``VACUUM``.

Nothing here claims a file-size win: the measured before/after bytes are part of
the report and the startup log.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from ...infrastructure.database.maintenance import database_space, reclaim_space
from .repository import ReceiptCompactionRepository
from .schemas import DEFAULT_BATCH_SIZE, DEFAULT_BUDGET_SECONDS, TASKS, TaskSpec


@dataclass
class TaskReport:
    key: str
    converted: int = 0
    skipped: int = 0
    failed: int = 0
    batches: int = 0
    done: bool = False
    watermark: int = 0


@dataclass
class CompactionReport:
    tasks: dict[str, TaskReport] = field(default_factory=dict)
    elapsed_seconds: float = 0.0
    reclaim: dict[str, Any] = field(default_factory=dict)
    space_before: dict[str, Any] = field(default_factory=dict)
    space_after: dict[str, Any] = field(default_factory=dict)

    @property
    def converted(self) -> int:
        return sum(task.converted for task in self.tasks.values())

    @property
    def skipped(self) -> int:
        return sum(task.skipped for task in self.tasks.values())

    @property
    def failed(self) -> int:
        return sum(task.failed for task in self.tasks.values())

    @property
    def finished(self) -> bool:
        return all(task.done for task in self.tasks.values())

    def summary(self) -> str:
        parts = [
            "%s: 转换 %d / 跳过 %d / 失败 %d%s"
            % (key, task.converted, task.skipped, task.failed,
               "（已完成）" if task.done else "（下次启动继续）")
            for key, task in sorted(self.tasks.items())
        ]
        return "；".join(parts) if parts else "无任务"


class ReceiptCompactionApplication:
    """One startup pass over the historical receipt tables."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        state_path: str | Path,
        repository: ReceiptCompactionRepository | None = None,
    ) -> None:
        self.game_database = Path(game_database)
        self.player_database = Path(player_database)
        self.repository = repository or ReceiptCompactionRepository(state_path)

    def compact(
        self,
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
        budget_seconds: float = DEFAULT_BUDGET_SECONDS,
        allow_reclaim: bool = True,
        logger: Any = None,
        free_bytes: int | None = None,
        backup_dir: str | Path | None = None,
        clock: Callable[[], float] = time.monotonic,
        tasks: tuple[TaskSpec, ...] = TASKS,
    ) -> CompactionReport:
        databases = {"game": self.game_database, "player": self.player_database}
        state = self.repository.load_state()
        report = CompactionReport()
        started = clock()
        report.space_before = {
            name: database_space(path) for name, path in databases.items() if path.is_file()
        }

        def log(level: str, message: str) -> None:
            if logger is not None:
                getattr(logger, level, logger.info)(message)

        for spec in tasks:
            saved = state["tasks"].get(spec.key) or {}
            task = TaskReport(
                key=spec.key,
                done=bool(saved.get("done")),
                watermark=int(saved.get("watermark") or 0),
            )
            report.tasks[spec.key] = task
            if task.done:
                continue
            database = databases[spec.database]
            if not database.is_file():
                task.done = True
                state["tasks"][spec.key] = {
                    "watermark": task.watermark, "done": True,
                    "converted": task.converted, "skipped": task.skipped, "failed": task.failed,
                }
                self.repository.save_state(state)
                continue
            while True:
                if clock() - started >= budget_seconds:
                    log("info", "回执整理达到本轮时间预算，%s 下次启动继续" % spec.key)
                    break
                converted, skipped, failed, watermark, done = self.repository.run_batch(
                    database, spec, task.watermark, batch_size
                )
                task.batches += 1
                task.converted += converted
                task.skipped += skipped
                task.failed += failed
                task.watermark = watermark
                task.done = bool(done)
                state["tasks"][spec.key] = {
                    "watermark": task.watermark,
                    "done": task.done,
                    "converted": task.converted,
                    "skipped": task.skipped,
                    "failed": task.failed,
                }
                self.repository.save_state(state)
                if converted or skipped or failed:
                    log(
                        "info",
                        "回执整理 %s：本批转换 %d / 跳过 %d / 失败 %d（watermark=%d）"
                        % (spec.key, converted, skipped, failed, task.watermark),
                    )
                if done:
                    break
        report.elapsed_seconds = round(clock() - started, 3)

        if allow_reclaim and report.converted:
            report.reclaim = self._reclaim(
                state, databases, logger=logger, free_bytes=free_bytes, backup_dir=backup_dir
            )
        report.space_after = {
            name: database_space(path) for name, path in databases.items() if path.is_file()
        }
        return report

    def _reclaim(
        self,
        state: dict[str, Any],
        databases: dict[str, Path],
        *,
        logger: Any = None,
        free_bytes: int | None = None,
        backup_dir: str | Path | None = None,
    ) -> dict[str, Any]:
        if state.get("vacuum_done"):
            return {"status": "already_done"}
        outcome: dict[str, Any] = {}
        pending = False
        for name, path in databases.items():
            result = reclaim_space(path, free_bytes=free_bytes, require_backup_dir=backup_dir)
            outcome[name] = result
            if logger is not None:
                logger.info(
                    "回执整理空间回收 %s：%s（前 %s B → 后 %s B）"
                    % (name, result.get("status"),
                       (result.get("before") or {}).get("file_bytes"),
                       (result.get("after") or {}).get("file_bytes"))
                )
            if result.get("status") in {"no_headroom", "no_backup"}:
                # Retry on a later boot once headroom/backup exists.
                pending = True
        if not pending:
            state["vacuum_done"] = True
            self.repository.save_state(state)
        return outcome


def compact_receipts(
    game_database: str | Path,
    player_database: str | Path,
    *,
    state_path: str | Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
    budget_seconds: float = DEFAULT_BUDGET_SECONDS,
    allow_reclaim: bool = True,
    logger: Any = None,
    free_bytes: int | None = None,
    backup_dir: str | Path | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> CompactionReport:
    """Module-level entry point used by the startup hook and tests."""

    return ReceiptCompactionApplication(
        game_database, player_database, state_path=state_path
    ).compact(
        batch_size=batch_size,
        budget_seconds=budget_seconds,
        allow_reclaim=allow_reclaim,
        logger=logger,
        free_bytes=free_bytes,
        backup_dir=backup_dir,
        clock=clock,
    )


__all__ = ["CompactionReport", "TaskReport", "ReceiptCompactionApplication", "compact_receipts"]
