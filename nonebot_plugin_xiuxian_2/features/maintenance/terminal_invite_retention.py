"""Retention for terminal dungeon team invites.

`dungeon_team_invites` keeps one row per invitation.  A row is written as
`pending` and later only ever *updated* to a terminal status (`expired`,
`joined`, ...) with `consumed_at` set -- nothing deletes it, so every invitation
ever sent leaves a row behind forever.

Only the *pending* rows are business state.  Every reader filters on
`status='pending'`:

* ``due_invites`` -- ``status='pending' AND consumed_at IS NULL AND expires_at<=?``
* ``pending_for_user`` -- ``status='pending' AND expires_at>?``
* ``join`` -- a terminal row and a missing row both answer ``invite_invalid``
* the 60 s ``team_manager`` notification task -- ``invite_by_id`` returning
  ``None`` returns silently, and a terminal row makes ``expire()`` return a
  non-``applied`` status, which is also silent

An invitation is created with ``expires_at = now + 60`` and ``join`` requires
``expires_at > now``, so one invitation is actionable for at most 60 seconds.
The 30-day window is therefore ~43,200x the whole invite lifecycle: no real
replay or notification path can still care about such a row.

The deletion predicate is stable and idempotent, so a pass stopped by the time
budget simply selects the same remaining eligible rows on the next boot -- there
is deliberately no progress/state file.  Only one aggregate log line is emitted.

Two things are never touched: ``pending`` rows (whatever their age -- the
existing expiry flow owns them) and ``dungeon_team_operations``, which is the
idempotency/replay receipt.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from ...infrastructure.database import DatabaseUnitOfWork

DEFAULT_RETENTION_DAYS = 30
DEFAULT_BATCH_SIZE = 500
DEFAULT_BUDGET_SECONDS = 10.0

INVITE_TABLE = "dungeon_team_invites"
REQUIRED_COLUMNS = frozenset({"invite_id", "expires_at", "status", "consumed_at"})


@dataclass
class InviteRetentionReport:
    deleted: int = 0
    batches: int = 0
    skipped: bool = False
    schema_missing: bool = False
    candidates_remaining: bool = False
    cutoff_epoch: float = 0.0
    elapsed_seconds: float = 0.0

    def summary(self) -> str:
        if self.schema_missing:
            return "缺表或缺列，跳过（不建表）"
        if self.skipped:
            return "数据库不可用，跳过"
        tail = "下次启动继续" if self.candidates_remaining else "本轮已收敛"
        return "清理 %d 行（%s）" % (self.deleted, tail)


def cutoff_epoch(now: float | datetime, retention_days: int = DEFAULT_RETENTION_DAYS) -> float:
    """Oldest ``expires_at`` that is still retained (exclusive cutoff)."""

    reference = now.timestamp() if isinstance(now, datetime) else float(now)
    return reference - max(0, int(retention_days)) * 86400.0


def _candidate_clause() -> str:
    # Terminal rows only: a pending row is business state even when it is old,
    # and a non-numeric expires_at cannot be ordered against the cutoff safely.
    return (
        'FROM "%s" AS t '
        "WHERE (t.status <> 'pending' OR t.consumed_at IS NOT NULL) "
        "AND typeof(t.expires_at) IN ('real','integer') "
        "AND t.expires_at < ?" % INVITE_TABLE
    )


def _table_ready(uow: DatabaseUnitOfWork) -> bool:
    row = uow.query_one(
        "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
        (INVITE_TABLE,),
    )
    if row is None:
        return False
    columns = {
        str(column["name"])
        for column in uow.query_all('PRAGMA table_info("%s")' % INVITE_TABLE)
    }
    return REQUIRED_COLUMNS.issubset(columns)


def prune_terminal_invites(
    player_database: str | Path,
    *,
    now: float | datetime | None = None,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    budget_seconds: float = DEFAULT_BUDGET_SECONDS,
    logger: Any = None,
    clock: Callable[[], float] = time.monotonic,
) -> InviteRetentionReport:
    """Delete terminal invite rows older than the retention window.

    Bounded by ``batch_size`` rows per ``BEGIN IMMEDIATE`` transaction and by
    ``budget_seconds`` for the whole pass.  The predicate is stable, so a stopped
    pass needs no bookkeeping: the next call selects the same remaining rows.
    Exactly one aggregate log line is emitted.
    """

    report = InviteRetentionReport()
    started = clock()
    cutoff = cutoff_epoch(now if now is not None else time.time(), retention_days)
    report.cutoff_epoch = cutoff
    path = Path(player_database)
    if not path.is_file():
        report.skipped = True
        return report

    clause = _candidate_clause()
    while True:
        with DatabaseUnitOfWork(path, immediate=True) as uow:
            if not _table_ready(uow):
                report.schema_missing = True
                report.elapsed_seconds = round(clock() - started, 3)
                return report
            probe = uow.query_one("SELECT 1 AS present " + clause + " LIMIT 1", (cutoff,))
            if probe is None:
                report.elapsed_seconds = round(clock() - started, 3)
                return report
            if clock() - started >= budget_seconds:
                report.candidates_remaining = True
                report.elapsed_seconds = round(clock() - started, 3)
                return report
            cursor = uow.execute(
                'DELETE FROM "%s" WHERE invite_id IN '
                "(SELECT t.invite_id %s ORDER BY t.expires_at, t.invite_id LIMIT ?)"
                % (INVITE_TABLE, clause),
                (cutoff, int(batch_size)),
            )
            deleted = int(getattr(cursor, "rowcount", 0) or 0)
            report.deleted += deleted
            report.batches += 1
        if deleted == 0:
            report.elapsed_seconds = round(clock() - started, 3)
            return report


def maintain_terminal_invites(
    player_database: str | Path,
    *,
    now: float | datetime | None = None,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    budget_seconds: float = DEFAULT_BUDGET_SECONDS,
    logger: Any = None,
    clock: Callable[[], float] = time.monotonic,
) -> InviteRetentionReport:
    """Run the pass and emit the single aggregate log line."""

    report = prune_terminal_invites(
        player_database,
        now=now,
        retention_days=retention_days,
        batch_size=batch_size,
        budget_seconds=budget_seconds,
        logger=logger,
        clock=clock,
    )
    if logger is not None:
        logger.info(
            "终态组队邀请保留（cutoff_epoch=%.0f）：%s；%d 批；用时 %.2fs"
            % (report.cutoff_epoch, report.summary(), report.batches, report.elapsed_seconds)
        )
    return report


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_BUDGET_SECONDS",
    "DEFAULT_RETENTION_DAYS",
    "INVITE_TABLE",
    "InviteRetentionReport",
    "cutoff_epoch",
    "maintain_terminal_invites",
    "prune_terminal_invites",
]
