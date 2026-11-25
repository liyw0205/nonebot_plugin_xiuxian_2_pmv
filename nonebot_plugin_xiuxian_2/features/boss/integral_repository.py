from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from ...core.numeric import as_int_like, bind_sqlite_param, require_exact_integer
from ...core.errors import DomainError

from ...infrastructure.database import DatabaseUnitOfWork
from ..base.ranking_position import PersonalRank
from ..base.public_display_snapshot import (
    shared_display_snapshot,
    shared_display_snapshot_with_live_value,
)
from ..base.schema_catalog import sqlite_table_columns


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
        columns = sqlite_table_columns(uow, uow.database, "boss_limit")
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
            return self._top_integrals_in_uow(uow, limit)

    @staticmethod
    def _top_integrals_in_uow(
        uow: DatabaseUnitOfWork, limit: int
    ) -> list[tuple[str, int]]:
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

    def top_integrals_snapshot(
        self, limit: int = MAX_RANKED_USERS
    ) -> dict[str, Any]:
        try:
            normalized_limit = max(0, min(int(limit), self.MAX_RANKED_USERS))
        except (TypeError, ValueError, OverflowError):
            normalized_limit = 0
        return shared_display_snapshot(
            self.database,
            "boss-integral-rank-v1",
            normalized_limit,
            loader=lambda: self.top_integrals(normalized_limit),
        )

    def top_integrals_snapshot_with_rank(
        self, limit: int, user_id: str
    ) -> tuple[dict[str, Any], PersonalRank]:
        """Share the cold display read, but calculate the viewer rank live on hits."""
        try:
            normalized_limit = max(0, min(int(limit), self.MAX_RANKED_USERS))
        except (TypeError, ValueError, OverflowError):
            normalized_limit = 0
        normalized_user_id = str(user_id).strip()
        if not normalized_user_id:
            return self.top_integrals_snapshot(normalized_limit), PersonalRank()

        def load_top_and_rank():
            if not self.database.is_file():
                return [], PersonalRank()
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if not self._ready(uow):
                    return [], PersonalRank()
                rows = self._top_integrals_in_uow(uow, normalized_limit)
                rank = self._integral_rank_in_uow(
                    uow, normalized_user_id
                )
                return rows, rank

        return shared_display_snapshot_with_live_value(
            self.database,
            "boss-integral-rank-v1",
            normalized_limit,
            loader=load_top_and_rank,
            read_live=lambda: self.integral_rank(normalized_user_id),
        )

    def integral_rank(self, user_id: str) -> PersonalRank:
        """本人世界积分名次：并列名次 + 有分人数 + 本人积分。"""
        user_id = str(user_id).strip()
        if not user_id or not self.database.is_file():
            return PersonalRank()
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if not self._ready(uow):
                return PersonalRank()
            return self._integral_rank_in_uow(uow, user_id)

    @staticmethod
    def _integral_rank_in_uow(
        uow: DatabaseUnitOfWork, user_id: str
    ) -> PersonalRank:
        deduped = (
            "SELECT entry.user_id AS user_id,"
            "COALESCE(entry.integral,0) AS value "
            "FROM boss_limit AS entry "
            "WHERE entry.user_id IS NOT NULL "
            "AND TRIM(CAST(entry.user_id AS TEXT))<>'' "
            "AND entry.rowid=(SELECT MIN(candidate.rowid) FROM boss_limit AS candidate "
            "WHERE candidate.user_id=entry.user_id)"
        )
        row = uow.query_one(
            f"WITH ranked AS MATERIALIZED ({deduped}), "
            "target AS (SELECT COALESCE((SELECT value FROM ranked WHERE user_id=?),0) AS value) "
            "SELECT target.value AS value, "
            "COALESCE(SUM(CASE WHEN xiuxian_int_cmp(ranked.value,0)>0 THEN 1 ELSE 0 END),0) AS total, "
            "COALESCE(SUM(CASE WHEN target.value>0 AND "
            "xiuxian_int_cmp(ranked.value,target.value)>0 THEN 1 ELSE 0 END),0) AS better "
            "FROM ranked CROSS JOIN target",
            (user_id,),
        )
        value = as_int_like((row or {}).get("value"))
        total = int((row or {}).get("total") or 0)
        better = int((row or {}).get("better") or 0)
        if value <= 0:
            return PersonalRank(0, total, 0)
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
        return BossIntegralSnapshot("found", user_id, require_exact_integer(row["integral"], field="boss_integral", subject=user_id))

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
            current = require_exact_integer(row["value"], field="boss_integral", subject=user_id)
            target = current + amount
            changed = uow.execute(
                "UPDATE boss_limit SET integral=xiuxian_int_store(?,'boss_integral','actor') WHERE rowid=? AND user_id=? "
                "AND xiuxian_int_cmp(COALESCE(integral,0),?,1,'boss_integral','actor')=0",
                (bind_sqlite_param(target, exact=True), row["_rowid"], user_id, bind_sqlite_param(current, exact=True)),
            )
            if changed.rowcount != 1:
                return self._result("state_changed", user_id, amount, 0, current)
            saved = uow.query_one("SELECT integral FROM boss_limit WHERE rowid=?", (row["_rowid"],))
            if saved is None or require_exact_integer(saved["integral"], field="boss_integral", subject=user_id) != target:
                raise DomainError("boss_integral_write_rejected", "boss integral grant did not persist exactly", {"reason": "boss_integral_write_rejected", "subject": user_id})
            return self._result("applied", user_id, amount, amount, target)


__all__ = ["BossIntegralMutation", "BossIntegralSnapshot", "BossIntegralSqlRepository"]
