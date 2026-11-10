from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from ...core.numeric import as_int_like

from ...infrastructure.database import DatabaseUnitOfWork
from ..base.ranking_position import PersonalRank


@dataclass(frozen=True)
class BossIntegralMutation:
    status: str
    user_id: str
    requested: int = 0
    applied: int = 0
    value: int | None = None

    @property
    def succeeded(self) -> bool:
        return self.status == "applied"


@dataclass(frozen=True)
class BossIntegralSnapshot:
    status: str
    user_id: str
    integral: int = 0


class BossIntegralSqlRepository:
    """Update the startup-migrated player-side boss integral projection."""

    MAX_RANKED_USERS = 50

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @staticmethod
    def _result(status: str, user_id: str, requested: int, applied: int = 0, value: int | None = None) -> BossIntegralMutation:
        return BossIntegralMutation(status, str(user_id), int(requested), int(applied), None if value is None else int(value))

    @staticmethod
    def _ready(uow: DatabaseUnitOfWork) -> bool:
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='boss_limit'"
        )
        if table is None:
            return False
        columns = {
            str(row["name"]).casefold()
            for row in uow.query_all('PRAGMA table_info("boss_limit")')
        }
        return {"user_id", "integral"}.issubset(columns)

    def top_integrals(self, limit: int = MAX_RANKED_USERS) -> list[tuple[str, int]]:
        try:
            limit = max(0, min(int(limit), self.MAX_RANKED_USERS))
        except (TypeError, ValueError, OverflowError):
            return []
        if limit == 0 or not self.database.is_file():
            return []

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._ready(uow):
                return []
            rows = uow.query_all(
                "SELECT entry.user_id AS user_id,"
                "COALESCE(entry.integral,0) AS integral "
                "FROM boss_limit AS entry "
                "WHERE entry.user_id IS NOT NULL "
                "AND TRIM(CAST(entry.user_id AS TEXT))<>'' "
                "AND entry.rowid=(SELECT MIN(candidate.rowid) FROM boss_limit AS candidate "
                "WHERE candidate.user_id=entry.user_id) "
                "ORDER BY xiuxian_int_text(COALESCE(entry.integral,0)) COLLATE xiuxian_integer DESC,entry.rowid ASC "
                "LIMIT ?",
                (limit,),
            )
        return [(str(row["user_id"]), as_int_like(row["integral"])) for row in rows]

    def integral_rank(self, user_id: str) -> PersonalRank:
        """本人世界积分名次：并列名次 + 有分人数 + 本人积分。"""
        user_id = str(user_id).strip()
        if not user_id or not self.database.is_file():
            return PersonalRank()
        deduped = (
            "SELECT entry.user_id AS user_id,"
            "COALESCE(entry.integral,0) AS value "
            "FROM boss_limit AS entry "
            "WHERE entry.user_id IS NOT NULL "
            "AND TRIM(CAST(entry.user_id AS TEXT))<>'' "
            "AND entry.rowid=(SELECT MIN(candidate.rowid) FROM boss_limit AS candidate "
            "WHERE candidate.user_id=entry.user_id)"
        )
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._ready(uow):
                return PersonalRank()
            row = uow.query_one(
                "SELECT COALESCE(integral,0) AS value "
                "FROM boss_limit WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                (user_id,),
            )
            value = as_int_like(row["value"]) if row is not None else 0
            total = int(
                (uow.query_one(
                    f"SELECT COUNT(*) AS n FROM ({deduped}) WHERE xiuxian_int_cmp(value,0)>0"
                ) or {"n": 0})["n"]
                or 0
            )
            if value <= 0:
                return PersonalRank(0, total, 0)
            better = int(
                (uow.query_one(
                    f"SELECT COUNT(*) AS n FROM ({deduped}) WHERE xiuxian_int_cmp(value,?)>0",
                    (value,),
                ) or {"n": 0})["n"]
                or 0
            )
        return PersonalRank(better + 1, total, value)

    def get_integral(self, user_id: str) -> BossIntegralSnapshot:
        user_id = str(user_id)
        if not self.database.is_file():
            return BossIntegralSnapshot("schema_missing", user_id)

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._ready(uow):
                return BossIntegralSnapshot("schema_missing", user_id)
            row = uow.query_one(
                "SELECT COALESCE(integral,0) AS integral "
                "FROM boss_limit WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                (user_id,),
            )
        if row is None:
            return BossIntegralSnapshot("user_missing", user_id)
        return BossIntegralSnapshot("found", user_id, as_int_like(row["integral"]))

    def grant_integral(self, user_id: str, amount: int) -> BossIntegralMutation:
        user_id = str(user_id).strip()
        amount = int(amount)
        if not user_id or amount < 0:
            return self._result("invalid", user_id, amount)
        if amount == 0:
            return self._result("applied", user_id, amount, 0, 0)
        if not self.database.is_file():
            return self._result("schema_missing", user_id, amount)

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._ready(uow):
                return self._result("schema_missing", user_id, amount)
            row = uow.query_one(
                "SELECT rowid AS _rowid,COALESCE(integral,0) AS value "
                "FROM boss_limit WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                (user_id,),
            )
            if row is None:
                return self._result("user_missing", user_id, amount)
            current = as_int_like(row["value"])
            target = current + amount
            changed = uow.execute(
                "UPDATE boss_limit SET integral=? WHERE rowid=? AND user_id=? "
                "AND xiuxian_int_cmp(COALESCE(integral,0),?)=0",
                (target, row["_rowid"], user_id, current),
            )
            if changed.rowcount != 1:
                return self._result("state_changed", user_id, amount, 0, current)
            return self._result("applied", user_id, amount, amount, target)


__all__ = ["BossIntegralMutation", "BossIntegralSnapshot", "BossIntegralSqlRepository"]
