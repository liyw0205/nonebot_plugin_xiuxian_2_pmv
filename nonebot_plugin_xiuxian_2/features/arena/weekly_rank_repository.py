from __future__ import annotations
from ...core.numeric import as_int_like, sqlite_int_compare

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.clock import SystemClock
from .schema import SCHEMA_MISSING, table_ready


@dataclass(frozen=True)
class ArenaWeeklyRankReductionResult:
    status: str
    business_week: str
    task_status: str = ""
    reduce_steps: int = 0
    total: int = 0
    completed: int = 0
    changed: int = 0
    skipped: int = 0
    conflicted: int = 0
    last_error: str = ""

    @property
    def succeeded(self) -> bool:
        return self.status in {"applied", "duplicate"}


class ArenaWeeklyRankRepository:
    """Feature-owned, resumable weekly arena rank reduction on player.db."""

    RANKS = ("青铜", "白银", "黄金", "铂金", "钻石", "王者")
    INITIAL_SCORES = {
        "青铜": 1000,
        "白银": 1500,
        "黄金": 1900,
        "铂金": 2300,
        "钻石": 2700,
        "王者": 3200,
    }

    # The startup migrations own every table this repository touches:
    # ``arena.006`` builds the two reduction ledgers, ``arena.008``/``arena.009``
    # build ``arena`` and patch in the columns read below.  The request path only
    # verifies them, so a missing table or a half-migrated ``arena`` fails closed
    # instead of silently running DDL inside the settlement transaction.
    OPERATION_TABLES = (
        "arena_weekly_rank_reduction_operations",
        "arena_weekly_rank_reduction_targets",
    )
    ARENA_COLUMNS = frozenset({"user_id", "score", "rank", "win_streak"})

    def __init__(
        self,
        player_database: str | Path,
        lock: RLock | None = None,
        *,
        clock: Any | None = None,
    ) -> None:
        self.database = Path(player_database)
        self.lock = lock or RLock()
        self.clock = clock or SystemClock()

    @classmethod
    def normalize_week(cls, value: Any) -> str:
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            iso = value.isocalendar()
            return f"{iso.year}-W{iso.week:02d}"
        text = str(value).strip()
        if "-W" in text:
            year_text, week_text = text.split("-W", 1)
            monday = date.fromisocalendar(int(year_text), int(week_text), 1)
            iso = monday.isocalendar()
            return f"{iso.year}-W{iso.week:02d}"
        return cls.normalize_week(date.fromisoformat(text))

    @classmethod
    def _rank_for_score(cls, score: int) -> str:
        if score >= 3200:
            return "王者"
        if score >= 2700:
            return "钻石"
        if score >= 2300:
            return "铂金"
        if score >= 1900:
            return "黄金"
        if score >= 1500:
            return "白银"
        return "青铜"

    @classmethod
    def _target(cls, score: int, rank: str, steps: int) -> tuple[str, int]:
        current = rank if rank in cls.RANKS else cls._rank_for_score(score)
        target = max(0, cls.RANKS.index(current) - steps)
        target_rank = cls.RANKS[target]
        return target_rank, cls.INITIAL_SCORES[target_rank]

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        """Verify the startup-owned schema; never create or patch it here."""
        return table_ready(uow, "arena", cls.ARENA_COLUMNS) and all(
            table_ready(uow, table) for table in cls.OPERATION_TABLES
        )

    @staticmethod
    def _result(uow: DatabaseUnitOfWork, week: str, status: str) -> ArenaWeeklyRankReductionResult:
        row = uow.query_one(
            "SELECT status,reduce_steps,total,completed,changed,skipped,conflicted,last_error "
            "FROM arena_weekly_rank_reduction_operations WHERE business_week=?", (week,)
        )
        if row is None:
            return ArenaWeeklyRankReductionResult(status, week)
        return ArenaWeeklyRankReductionResult(
            status, week, str(row["status"]), int(row["reduce_steps"]),
            int(row["total"]), int(row["completed"]), int(row["changed"]),
            int(row["skipped"]), int(row["conflicted"]), str(row["last_error"] or ""),
        )

    def reduce(self, business_week=None, reduce_steps=2, *, chunk_size=500, updated_at=None):
        now = self.clock.now()
        week = self.normalize_week(now.date() if business_week is None else business_week)
        steps = max(0, int(reduce_steps))
        limit = max(1, int(chunk_size))
        stamp = str(updated_at or now.strftime("%Y-%m-%d %H:%M:%S"))
        try:
            with self.lock, DatabaseUnitOfWork(self.database, immediate=True) as uow:
                if not self._schema_ready(uow):
                    return ArenaWeeklyRankReductionResult(SCHEMA_MISSING, week)
                operation = uow.query_one(
                    "SELECT reduce_steps,status FROM arena_weekly_rank_reduction_operations "
                    "WHERE business_week=?",
                    (week,),
                )
                if operation is None:
                    users = tuple(
                        (
                            str(row["user_id"]),
                            as_int_like(row["score"]),
                            str(row["rank"] or ""),
                            as_int_like(row["win_streak"]),
                        )
                        for row in uow.query_all(
                            "SELECT user_id,COALESCE(score,1000) AS score,"
                            "COALESCE(rank,'') AS rank,COALESCE(win_streak,0) AS win_streak "
                            "FROM arena ORDER BY user_id"
                        )
                    )
                    uow.execute(
                        "INSERT INTO arena_weekly_rank_reduction_operations "
                        "(business_week,reduce_steps,total,status,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                        (week, steps, len(users), "completed" if not users else "running", stamp, stamp),
                    )
                    targets = []
                    for ordinal, (user_id, score, rank, streak) in enumerate(users):
                        target_rank, target_score = self._target(score, rank, steps)
                        targets.append(
                            (week, user_id, ordinal, score, rank, streak, target_score, target_rank, stamp)
                        )
                    uow.executemany(
                        "INSERT INTO arena_weekly_rank_reduction_targets "
                        "(business_week,user_id,ordinal,previous_score,previous_rank,previous_win_streak,"
                        "target_score,target_rank,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                        targets,
                    )
                    if not users:
                        return self._result(uow, week, "applied")
                elif int(operation["reduce_steps"]) != steps:
                    return self._result(uow, week, "operation_conflict")
                elif str(operation["status"]) == "completed":
                    return self._result(uow, week, "duplicate")

            with self.lock, DatabaseUnitOfWork(self.database, immediate=True) as uow:
                pending = uow.query_all(
                    "SELECT user_id,previous_score,previous_rank,previous_win_streak,target_score,target_rank "
                    "FROM arena_weekly_rank_reduction_targets WHERE business_week=? AND status='pending' "
                    "ORDER BY ordinal LIMIT ?",
                    (week, limit),
                )
                changed = skipped = conflicted = 0
                for row in pending:
                    user_id = str(row["user_id"])
                    previous = (
                        as_int_like(row["previous_score"]),
                        str(row["previous_rank"]),
                        as_int_like(row["previous_win_streak"]),
                    )
                    target = (as_int_like(row["target_score"]), str(row["target_rank"]), 0)
                    current = uow.query_one(
                        "SELECT COALESCE(score,1000) AS score,COALESCE(rank,'') AS rank,"
                        "COALESCE(win_streak,0) AS win_streak FROM arena WHERE user_id=?",
                        (user_id,),
                    )
                    if current is None:
                        skipped += 1
                        uow.execute(
                            "UPDATE arena_weekly_rank_reduction_targets SET status='skipped',"
                            "error_text='user_missing',updated_at=? WHERE business_week=? AND user_id=? "
                            "AND status='pending'",
                            (stamp, week, user_id),
                        )
                        continue
                    actual = (
                        as_int_like(current["score"]),
                        str(current["rank"]),
                        as_int_like(current["win_streak"]),
                    )
                    if (sqlite_int_compare(current["score"], previous[0]) != 0
                            or str(current["rank"]) != previous[1]
                            or sqlite_int_compare(current["win_streak"], previous[2]) != 0):
                        skipped += 1
                        conflicted += 1
                        uow.execute(
                            "UPDATE arena_weekly_rank_reduction_targets SET status='conflict',"
                            "error_text='state_changed',updated_at=? WHERE business_week=? AND user_id=? "
                            "AND status='pending'",
                            (stamp, week, user_id),
                        )
                        continue
                    if actual != target:
                        updated = uow.execute(
                            "UPDATE arena SET score=?,rank=?,win_streak=0 WHERE user_id=? "
                            "AND xiuxian_int_cmp(COALESCE(score,1000),?)=0 AND COALESCE(rank,'')=? "
                            "AND xiuxian_int_cmp(COALESCE(win_streak,0),?)=0",
                            (target[0], target[1], user_id, previous[0], previous[1], previous[2]),
                        )
                        if updated.rowcount != 1:
                            raise RuntimeError("arena weekly rank target changed")
                        changed += 1
                    uow.execute(
                        "UPDATE arena_weekly_rank_reduction_targets SET status='applied',"
                        "error_text='',updated_at=? WHERE business_week=? AND user_id=? "
                        "AND status='pending'",
                        (stamp, week, user_id),
                    )
                progress = uow.query_one(
                    "SELECT COUNT(*) AS total,COALESCE(SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END),0) "
                    "AS pending FROM arena_weekly_rank_reduction_targets WHERE business_week=?",
                    (week,),
                )
                if progress is None:
                    raise RuntimeError("arena weekly rank progress is missing")
                completed = int(progress["total"]) - int(progress["pending"])
                task_status = "completed" if int(progress["pending"]) == 0 else "running"
                uow.execute(
                    "UPDATE arena_weekly_rank_reduction_operations SET completed=?,changed=changed+?,"
                    "skipped=skipped+?,conflicted=conflicted+?,status=?,last_error='',updated_at=? "
                    "WHERE business_week=?",
                    (completed, changed, skipped, conflicted, task_status, stamp, week),
                )
                return self._result(uow, week, "applied")
        except Exception as exc:
            try:
                with self.lock, DatabaseUnitOfWork(self.database, immediate=True) as uow:
                    uow.execute(
                        "UPDATE arena_weekly_rank_reduction_operations SET last_error=?,updated_at=? "
                        "WHERE business_week=?",
                        (str(exc), stamp, week),
                    )
            except Exception:
                pass
            raise


__all__ = ["ArenaWeeklyRankRepository", "ArenaWeeklyRankReductionResult"]
