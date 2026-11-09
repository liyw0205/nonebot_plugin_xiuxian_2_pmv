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

So a terminal row whose expiry is well in the past is dead weight: deleting it
cannot change any player-visible result.  Two things are deliberately untouched:

* ``pending`` rows, whatever their age -- the existing expiry flow owns them;
* ``dungeon_team_operations`` -- that table is the idempotency/replay receipt
  and is never referenced from this module.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.filesystem.atomic import atomic_write

STATE_VERSION = 1
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
            return "终态组队邀请：缺表或缺列，跳过（不建表）"
        if self.skipped:
            return "终态组队邀请：本轮已收敛"
        tail = "（下次启动继续）" if self.candidates_remaining else "（本轮已收敛）"
        return "终态组队邀请：清理 %d 行%s" % (self.deleted, tail)


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


def _load_state(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return {"version": STATE_VERSION}
    if not isinstance(document, dict):
        return {"version": STATE_VERSION}
    document.setdefault("version", STATE_VERSION)
    return document


def _save_state(path: Path, state: dict[str, Any]) -> None:
    atomic_write(
        path,
        json.dumps(state, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8"),
    )


def prune_terminal_invites(
    player_database: str | Path,
    *,
    state_path: str | Path,
    now: float | datetime | None = None,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    budget_seconds: float = DEFAULT_BUDGET_SECONDS,
    logger: Any = None,
    clock: Callable[[], float] = time.monotonic,
) -> InviteRetentionReport:
    """Delete terminal invite rows older than the retention window.

    Bounded by ``batch_size`` rows per ``BEGIN IMMEDIATE`` transaction and by
    ``budget_seconds`` for the whole pass; an interrupted pass simply resumes on
    the next boot because the predicate is stable.
    """

    report = InviteRetentionReport()
    started = clock()
    cutoff = cutoff_epoch(now if now is not None else time.time(), retention_days)
    report.cutoff_epoch = cutoff
    path = Path(player_database)
    state_file = Path(state_path)
    if not path.is_file():
        report.skipped = True
        return report

    clause = _candidate_clause()
    while True:
        with DatabaseUnitOfWork(path, immediate=True) as uow:
            if not _table_ready(uow):
                report.schema_missing = True
                return report
            probe = uow.query_one("SELECT 1 AS present " + clause + " LIMIT 1", (cutoff,))
            if probe is None:
                return report
            if clock() - started >= budget_seconds:
                report.candidates_remaining = True
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
        if deleted and logger is not None:
            logger.info(
                "终态组队邀请清理：本批删除 %d 行（cutoff_epoch=%.0f）" % (deleted, cutoff)
            )
        if deleted == 0:
            return report


def maintain_terminal_invites(
    player_database: str | Path,
    *,
    state_path: str | Path,
    now: float | datetime | None = None,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    budget_seconds: float = DEFAULT_BUDGET_SECONDS,
    logger: Any = None,
    clock: Callable[[], float] = time.monotonic,
) -> InviteRetentionReport:
    """Run the pass, skipping it when this cutoff already converged."""

    report = prune_terminal_invites(
        player_database,
        state_path=state_path,
        now=now,
        retention_days=retention_days,
        batch_size=batch_size,
        budget_seconds=budget_seconds,
        logger=logger,
        clock=clock,
    )
    state_file = Path(state_path)
    state = _load_state(state_file)
    converged = not report.candidates_remaining and not report.schema_missing
    previous = float(state.get("cutoff_epoch") or 0)
    carried = int(state.get("deleted") or 0) if previous == report.cutoff_epoch else 0
    state.update(
        {
            "version": STATE_VERSION,
            "cutoff_epoch": report.cutoff_epoch,
            "deleted": carried + report.deleted,
            "complete": converged,
            "last_run": datetime.now().isoformat(timespec="seconds"),
            "schema_missing": report.schema_missing,
        }
    )
    if not report.schema_missing:
        _save_state(state_file, state)
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
