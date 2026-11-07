from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any, Callable

from ...infrastructure.database import DatabaseUnitOfWork
from ..base.ranking_position import PersonalRank


class TrainingLeaderboardSqlRepository:
    _FIELDS = {"completed", "points"}

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        clock: Callable[[], float] | None = None,
        cache_ttl: float = 45.0,
    ) -> None:
        self.game_database = Path(game_database)
        self.player_database = Path(player_database)
        self.clock = clock or time.monotonic
        self.cache_ttl = max(0.0, float(cache_ttl))
        self._cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
        self._lock = threading.RLock()

    def top(self, field: str, *, limit: int = 50) -> list[dict[str, Any]]:
        field = str(field).casefold()
        if field not in self._FIELDS:
            raise ValueError("unsupported training leaderboard field")
        limit = max(1, min(int(limit), 50))
        with self._lock:
            cached = self._cache.get(field)
            now = self.clock()
            if cached is not None and cached[0] > now:
                return [dict(row) for row in cached[1][:limit]]
            if not self.game_database.is_file() or not self.player_database.is_file():
                raise RuntimeError("历练排行榜 schema 尚未迁移。")
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                uow.attach_database(self.player_database, "player_data")
                game_tables = {
                    str(row["name"]).casefold()
                    for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
                }
                player_tables = {
                    str(row["name"]).casefold()
                    for row in uow.query_all(
                        "SELECT name FROM player_data.sqlite_master WHERE type='table'"
                    )
                }
                training_columns = {
                    str(row["name"]).casefold()
                    for row in uow.query_all('PRAGMA player_data.table_info("training")')
                }
                if (
                    "user_xiuxian" not in game_tables
                    or "training" not in player_tables
                    or not {"user_id", field}.issubset(training_columns)
                ):
                    raise RuntimeError("历练排行榜 schema 尚未迁移。")
                rows = uow.query_all(
                    f'SELECT t.user_id,CAST(COALESCE(t."{field}",0) AS INTEGER) AS value,'
                    "(SELECT ux.user_name FROM user_xiuxian ux "
                    "WHERE ux.user_id=t.user_id ORDER BY ux.rowid ASC LIMIT 1) AS user_name "
                    "FROM player_data.training t "
                    f'ORDER BY CAST(COALESCE(t."{field}",0) AS INTEGER) DESC,t.rowid ASC LIMIT ?',
                    (limit,),
                )
            self._cache[field] = (now + self.cache_ttl, rows)
            return [dict(row) for row in rows]

    def rank_of(self, field: str, user_id: str) -> PersonalRank:
        """本人历练名次：并列名次 + 有效人数 + 本人数值（只读，缺库/缺表降级）。"""
        field = str(field).casefold()
        if field not in self._FIELDS:
            raise ValueError("unsupported training leaderboard field")
        user_id = str(user_id).strip()
        if not user_id or not self.game_database.is_file() or not self.player_database.is_file():
            return PersonalRank()
        try:
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                uow.attach_database(self.player_database, "player_data")
                training_columns = {
                    str(row["name"]).casefold()
                    for row in uow.query_all('PRAGMA player_data.table_info("training")')
                }
                if not {"user_id", field}.issubset(training_columns):
                    return PersonalRank()
                deduped = (
                    'SELECT t.user_id AS user_id,'
                    f'CAST(COALESCE(t."{field}",0) AS INTEGER) AS value '
                    "FROM player_data.training AS t "
                    "WHERE t.user_id IS NOT NULL "
                    "AND TRIM(CAST(t.user_id AS TEXT))<>'' "
                    "AND t.rowid=(SELECT MIN(c.rowid) FROM player_data.training AS c "
                    "WHERE c.user_id=t.user_id)"
                )
                row = uow.query_one(
                    f'SELECT CAST(COALESCE("{field}",0) AS INTEGER) AS value '
                    "FROM player_data.training WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                    (user_id,),
                )
                value = int(row["value"] or 0) if row is not None else 0
                total = int(
                    (uow.query_one(
                        f"SELECT COUNT(*) AS n FROM ({deduped}) WHERE value>0"
                    ) or {"n": 0})["n"]
                    or 0
                )
                if value <= 0:
                    return PersonalRank(0, total, 0)
                better = int(
                    (uow.query_one(
                        f"SELECT COUNT(*) AS n FROM ({deduped}) WHERE value>?",
                        (value,),
                    ) or {"n": 0})["n"]
                    or 0
                )
        except (OSError, ValueError, TypeError, KeyError):
            return PersonalRank()
        return PersonalRank(better + 1, total, value)


__all__ = ["TrainingLeaderboardSqlRepository"]
