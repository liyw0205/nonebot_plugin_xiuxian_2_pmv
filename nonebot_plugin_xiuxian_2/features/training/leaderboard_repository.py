from __future__ import annotations

from pathlib import Path
from typing import Any
from ...core.numeric import as_int_like

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.database.uow import PLAYER_PROJECTION_READ_OWNER
from ..base.first_user_rows import first_user_rows
from ..base.indexed_ranking import (
    TRAINING_RANK_INDEXES,
    indexed_personal_rank,
    rank_index_available,
)
from ..base.public_display_snapshot import (
    shared_display_snapshot,
    shared_display_snapshot_with_live_value,
)
from ..base.ranking_position import PersonalRank
from ..base.schema_catalog import sqlite_table_columns


class TrainingLeaderboardSqlRepository:
    _FIELDS = {"completed", "points"}

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
    ) -> None:
        self.game_database = Path(game_database)
        self.player_database = Path(player_database)

    def top(self, field: str, *, limit: int = 50) -> list[dict[str, Any]]:
        field = str(field).casefold()
        if field not in self._FIELDS:
            raise ValueError("unsupported training leaderboard field")
        limit = max(1, min(int(limit), 50))
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
            training_columns = sqlite_table_columns(
                uow, self.player_database, "training", schema="player_data"
            )
            if (
                "user_xiuxian" not in game_tables
                or "training" not in player_tables
                or not {"user_id", field}.issubset(training_columns)
            ):
                raise RuntimeError("历练排行榜 schema 尚未迁移。")
            rows = self._top_in_uow(uow, field, limit)
        return [{**row, "value": as_int_like(row["value"])} for row in rows]

    def top_snapshot(self, field: str, *, limit: int = 50) -> dict[str, Any]:
        normalized_field = str(field).casefold()
        if normalized_field not in self._FIELDS:
            raise ValueError("unsupported training leaderboard field")
        normalized_limit = max(1, min(int(limit), 50))
        return shared_display_snapshot(
            self.game_database,
            "training-leaderboard-v1",
            normalized_field,
            normalized_limit,
            loader=lambda: self.top(normalized_field, limit=normalized_limit),
            dependency_databases=(self.player_database,),
        )

    @staticmethod
    def _top_in_uow(uow: DatabaseUnitOfWork, field: str, limit: int) -> list[dict[str, Any]]:
        if field not in TrainingLeaderboardSqlRepository._FIELDS:
            raise ValueError("unsupported training leaderboard field")
        return uow.query_all(
            """
            WITH top AS MATERIALIZED (
                SELECT t.rowid AS ordinal,t.user_id,COALESCE(t."{field}",0) AS value
                FROM player_data.training t
                ORDER BY xiuxian_int_text(COALESCE(t."{field}",0)) COLLATE xiuxian_integer DESC,
                    t.rowid ASC LIMIT ?
            )
            SELECT top.user_id,top.value,
                (SELECT ux.user_name FROM user_xiuxian ux WHERE ux.user_id=top.user_id
                    ORDER BY ux.rowid ASC LIMIT 1) AS user_name
            FROM top ORDER BY xiuxian_int_text(top.value) COLLATE xiuxian_integer DESC,top.ordinal ASC
            """.format(field=field),
            (limit,),
        )

    def top_snapshot_with_rank(
        self, field: str, user_id: str, *, limit: int = 50
    ) -> tuple[dict[str, Any], PersonalRank]:
        """Use one attached read transaction on a display-snapshot cold miss."""
        normalized_field = str(field).casefold()
        if normalized_field not in self._FIELDS:
            raise ValueError("unsupported training leaderboard field")
        normalized_limit = max(1, min(int(limit), 50))
        normalized_user_id = str(user_id).strip()
        if not normalized_user_id:
            return self.top_snapshot(normalized_field, limit=normalized_limit), PersonalRank()

        dependencies = (self.player_database,)
        return shared_display_snapshot_with_live_value(
            self.game_database,
            "training-leaderboard-v1",
            normalized_field,
            normalized_limit,
            loader=lambda: self._top_and_rank(
                normalized_field, normalized_limit, normalized_user_id
            ),
            read_live=lambda: self.rank_of(normalized_field, normalized_user_id),
            dependency_databases=dependencies,
        )

    def _top_and_rank(
        self, field: str, limit: int, user_id: str
    ) -> tuple[list[dict[str, Any]], PersonalRank]:
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
            training_columns = sqlite_table_columns(
                uow, self.player_database, "training", schema="player_data"
            )
            if (
                "user_xiuxian" not in game_tables
                or "training" not in player_tables
                or not {"user_id", field}.issubset(training_columns)
            ):
                raise RuntimeError("历练排行榜 schema 尚未迁移。")
            rows = self._top_in_uow(uow, field, limit)
            rank = self._rank_in_uow(
                uow, field, user_id, database_path=self.player_database
            )
        return [{**row, "value": as_int_like(row["value"])} for row in rows], rank

    def rank_of(self, field: str, user_id: str) -> PersonalRank:
        """本人历练名次：并列名次 + 有效人数 + 本人数值（只读，缺库/缺表降级）。"""
        field = str(field).casefold()
        if field not in self._FIELDS:
            raise ValueError("unsupported training leaderboard field")
        user_id = str(user_id).strip()
        if not user_id or not self.game_database.is_file() or not self.player_database.is_file():
            return PersonalRank()
        try:
            with DatabaseUnitOfWork(self.player_database, read_only=True,
                                    read_owner=PLAYER_PROJECTION_READ_OWNER.when_unscoped()) as uow:
                training_columns = sqlite_table_columns(
                    uow, self.player_database, "training"
                )
                if not {"user_id", field}.issubset(training_columns):
                    return PersonalRank()
                return self._rank_in_uow(
                    uow, field, user_id, schema="main",
                    database_path=self.player_database,
                )
        except (OSError, ValueError, TypeError, KeyError):
            return PersonalRank()

    @staticmethod
    def _rank_in_uow(
        uow: DatabaseUnitOfWork, field: str, user_id: str, *,
        schema: str = "player_data", database_path: str | Path | None = None,
    ) -> PersonalRank:
        if schema not in {"main", "player_data"}:
            raise ValueError("unsupported SQLite schema")
        if database_path is not None:
            spec = TRAINING_RANK_INDEXES[field]
            if rank_index_available(
                uow, database_path, spec, schema=schema, require_unique_user=True,
            ):
                return indexed_personal_rank(uow, spec, user_id, schema=schema)
        deduped = (
            'SELECT t.user_id AS user_id,'
            f'COALESCE(t."{field}",0) AS value '
            f"FROM {schema}.training AS t "
            f"JOIN ({first_user_rows('training', schema=schema)}) AS first "
            "ON first.user_id=t.user_id AND first.first_rowid=t.rowid "
            "WHERE t.user_id IS NOT NULL "
            "AND TRIM(CAST(t.user_id AS TEXT))<>''"
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


__all__ = ["TrainingLeaderboardSqlRepository"]
