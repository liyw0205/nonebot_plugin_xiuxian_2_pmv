from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from threading import RLock
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.clock import SystemClock


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
    def _ensure_schema(cls, uow: DatabaseUnitOfWork) -> None:
        uow.execute(
            "CREATE TABLE IF NOT EXISTS arena(user_id TEXT PRIMARY KEY,score INTEGER DEFAULT 1000,"
            "rank TEXT DEFAULT '青铜',win_streak INTEGER DEFAULT 0)"
        )
        columns = {str(row[1]) for row in uow.execute("PRAGMA table_info(arena)").fetchall()}
        for name, definition in {
            "score": "INTEGER DEFAULT 1000",
            "rank": "TEXT DEFAULT '青铜'",
            "win_streak": "INTEGER DEFAULT 0",
        }.items():
            if name not in columns:
                uow.execute(f'ALTER TABLE arena ADD COLUMN "{name}" {definition}')
        uow.execute(
            "CREATE TABLE IF NOT EXISTS arena_weekly_rank_reduction_operations("
            "business_week TEXT PRIMARY KEY,reduce_steps INTEGER NOT NULL,total INTEGER NOT NULL,"
            "completed INTEGER NOT NULL DEFAULT 0,changed INTEGER NOT NULL DEFAULT 0,"
            "skipped INTEGER NOT NULL DEFAULT 0,conflicted INTEGER NOT NULL DEFAULT 0,"
            "status TEXT NOT NULL DEFAULT 'running',last_error TEXT NOT NULL DEFAULT '',"
            "created_at TEXT NOT NULL,updated_at TEXT NOT NULL)"
        )
        uow.execute(
            "CREATE TABLE IF NOT EXISTS arena_weekly_rank_reduction_targets("
            "business_week TEXT NOT NULL,user_id TEXT NOT NULL,ordinal INTEGER NOT NULL,"
            "previous_score INTEGER NOT NULL,previous_rank TEXT NOT NULL,previous_win_streak INTEGER NOT NULL,"
            "target_score INTEGER NOT NULL,target_rank TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',"
            "error_text TEXT NOT NULL DEFAULT '',updated_at TEXT NOT NULL,"
            "PRIMARY KEY(business_week,user_id))"
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
                self._ensure_schema(uow)
                operation = uow.query_one(
                    "SELECT reduce_steps,status FROM arena_weekly_rank_reduction_operations "
                    "WHERE business_week=?",
                    (week,),
                )
                if operation is None:
                    users = tuple(
                        (
                            str(row["user_id"]),
                            int(row["score"] or 0),
                            str(row["rank"] or ""),
                            int(row["win_streak"] or 0),
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
                        int(row["previous_score"]),
                        str(row["previous_rank"]),
                        int(row["previous_win_streak"]),
                    )
                    target = (int(row["target_score"]), str(row["target_rank"]), 0)
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
                        int(current["score"]),
                        str(current["rank"]),
                        int(current["win_streak"]),
                    )
                    if actual != previous:
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
                            "AND COALESCE(score,1000)=? AND COALESCE(rank,'')=? "
                            "AND COALESCE(win_streak,0)=?",
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
