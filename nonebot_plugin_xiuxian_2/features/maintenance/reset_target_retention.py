"""Retention for the per-user daily-reset target ledgers.

`work_daily_refresh_reset_targets` (99,032 rows / 12.3 MB) and
`world_boss_daily_limit_reset_targets` (39,231 rows / 4.4 MB) each hold one row
per player per business date, written by the chunked daily reset.  They are
progress detail for **one** run: the reset resumes by reading the pending rows of
the date it is asked to run, and a date whose parent operation row is already
`completed` short-circuits to `duplicate` before any target row is read.

So the detail of a finished, old run is dead weight, while three things must
survive:

* the parent operation row (business_date + status) -- it is the idempotency and
  completion marker that answers "already done" for that date;
* every `pending` row, whatever its age -- an interrupted run must still resume;
* player state (`user_xiuxian.work_num`, `boss.boss_*`) -- never touched here.

A row is therefore only removable when all of these hold: its status is terminal
(`applied`/`skipped`), its business date is older than the cutoff, and the parent
operation for that date exists and is `completed`.  A missing parent (legacy row)
is treated as *not* terminal: it is kept.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.filesystem.atomic import atomic_write

STATE_VERSION = 1
DEFAULT_RETENTION_DAYS = 30
DEFAULT_BATCH_SIZE = 500
DEFAULT_BUDGET_SECONDS = 10.0
# Only these statuses may expire; anything else (pending, or a future status) stays.
EXPIRABLE_STATUSES = ("applied", "skipped")


@dataclass(frozen=True)
class RetentionTask:
    key: str
    database: str  # "game" | "player"
    table: str
    operation_table: str


TASKS: tuple[RetentionTask, ...] = (
    RetentionTask(
        "work_reset_targets",
        "game",
        "work_daily_refresh_reset_targets",
        "work_daily_refresh_reset_operations",
    ),
    RetentionTask(
        "boss_reset_targets",
        "player",
        "world_boss_daily_limit_reset_targets",
        "world_boss_daily_limit_reset_operations",
    ),
)


@dataclass
class RetentionTaskReport:
    key: str
    deleted: int = 0
    batches: int = 0
    skipped: bool = False
    candidates_remaining: bool = False


@dataclass
class RetentionReport:
    tasks: dict[str, RetentionTaskReport] = field(default_factory=dict)
    cutoff: str = ""
    elapsed_seconds: float = 0.0

    @property
    def deleted(self) -> int:
        return sum(task.deleted for task in self.tasks.values())

    def summary(self) -> str:
        parts = [
            "%s: 清理 %d 行%s" % (key, task.deleted,
                                 "（本轮已收敛）" if not task.candidates_remaining else "（下次启动继续）")
            for key, task in sorted(self.tasks.items())
        ]
        return "；".join(parts) if parts else "无任务"


def cutoff_date(today: date | str, retention_days: int = DEFAULT_RETENTION_DAYS) -> str:
    """Oldest business date that is still retained (inclusive)."""

    current = date.fromisoformat(today) if isinstance(today, str) else today
    return (current - timedelta(days=max(0, int(retention_days)))).isoformat()


def _tables_ready(uow: DatabaseUnitOfWork, task: RetentionTask) -> bool:
    for table in (task.table, task.operation_table):
        row = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        if row is None:
            return False
    return True


def _candidate_clause(task: RetentionTask) -> str:
    placeholders = ",".join("?" for _ in EXPIRABLE_STATUSES)
    # Only canonical ISO dates are eligible: a row whose business_date is not
    # `YYYY-MM-DD` (legacy/unknown format) is never expired, because string
    # comparison cannot order it safely against the cutoff.
    return (
        'FROM "%s" AS t WHERE t.business_date < ? '
        "AND t.business_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]' "
        "AND t.status IN (%s) "
        'AND EXISTS (SELECT 1 FROM "%s" AS o '
        "WHERE o.business_date = t.business_date AND o.status = 'completed')"
        % (task.table, placeholders, task.operation_table)
    )


def _state_path_for(state_path: str | Path) -> Path:
    return Path(state_path)


def _load_state(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return {"version": STATE_VERSION, "tasks": {}}
    if not isinstance(document, dict):
        return {"version": STATE_VERSION, "tasks": {}}
    tasks = document.get("tasks")
    document["tasks"] = tasks if isinstance(tasks, dict) else {}
    document.setdefault("version", STATE_VERSION)
    return document


def _save_state(path: Path, state: dict[str, Any]) -> None:
    atomic_write(path, json.dumps(state, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8"))


def prune_task(
    database: str | Path,
    task: RetentionTask,
    *,
    cutoff: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    budget_seconds: float = DEFAULT_BUDGET_SECONDS,
    clock: Callable[[], float],
    started: float,
    logger: Any = None,
) -> RetentionTaskReport:
    """Delete expired detail for one table in bounded transactions."""

    report = RetentionTaskReport(key=task.key)
    path = Path(database)
    if not path.is_file():
        report.skipped = True
        return report
    clause = _candidate_clause(task)
    params = (cutoff, *EXPIRABLE_STATUSES)
    while True:
        with DatabaseUnitOfWork(path, immediate=True) as uow:
            if not _tables_ready(uow, task):
                report.skipped = True
                return report
            probe = uow.query_one("SELECT 1 AS present " + clause + " LIMIT 1", params)
            if probe is None:
                return report
            if clock() - started >= budget_seconds:
                report.candidates_remaining = True
                return report
            cursor = uow.execute(
                'DELETE FROM "%s" WHERE rowid IN (SELECT t.rowid %s ORDER BY t.rowid LIMIT ?)'
                % (task.table, clause),
                (*params, batch_size),
            )
            deleted = int(getattr(cursor, "rowcount", 0) or 0)
            report.deleted += deleted
            report.batches += 1
        if deleted and logger is not None:
            logger.info(
                "每日重置明细清理 %s：本批删除 %d 行（cutoff=%s）" % (task.key, deleted, cutoff)
            )
        if deleted == 0:
            return report


def prune_reset_targets(
    game_database: str | Path,
    player_database: str | Path,
    *,
    state_path: str | Path,
    today: date | str,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    budget_seconds: float = DEFAULT_BUDGET_SECONDS,
    logger: Any = None,
    clock: Callable[[], float] = __import__("time").monotonic,
) -> RetentionReport:
    """Run the retention pass, reusing the startup maintenance window."""

    databases = {"game": Path(game_database), "player": Path(player_database)}
    cutoff = cutoff_date(today, retention_days)
    state_file = _state_path_for(state_path)
    state = _load_state(state_file)
    report = RetentionReport(cutoff=cutoff)
    started = clock()
    for task in TASKS:
        saved = state["tasks"].get(task.key) or {}
        saved_cutoff = str(saved.get("cutoff") or "")
        if saved_cutoff == cutoff and saved.get("complete"):
            # Same cutoff already converged on an earlier boot.  Still probe once
            # (an indexed lookup) because a run that was `running` back then may
            # have completed since and become eligible.
            path = databases[task.database]
            still_clear = True
            if path.is_file():
                with DatabaseUnitOfWork(path, read_only=True) as uow:
                    if _tables_ready(uow, task):
                        probe = uow.query_one(
                            "SELECT 1 AS present " + _candidate_clause(task) + " LIMIT 1",
                            (cutoff, *EXPIRABLE_STATUSES),
                        )
                        still_clear = probe is None
            if still_clear:
                report.tasks[task.key] = RetentionTaskReport(key=task.key, skipped=True)
                continue
        task_report = prune_task(
            databases[task.database],
            task,
            cutoff=cutoff,
            batch_size=batch_size,
            budget_seconds=budget_seconds,
            clock=clock,
            started=started,
            logger=logger,
        )
        report.tasks[task.key] = task_report
        state["tasks"][task.key] = {
            "cutoff": cutoff,
            "complete": not task_report.candidates_remaining and not task_report.skipped,
            "deleted": int(saved.get("deleted") or 0) + task_report.deleted
            if saved_cutoff == cutoff
            else task_report.deleted,
            "skipped": task_report.skipped,
        }
        _save_state(state_file, state)
    report.elapsed_seconds = round(clock() - started, 3)
    return report


__all__ = [
    "TASKS",
    "RetentionTask",
    "RetentionTaskReport",
    "RetentionReport",
    "cutoff_date",
    "prune_task",
    "prune_reset_targets",
    "DEFAULT_RETENTION_DAYS",
]
