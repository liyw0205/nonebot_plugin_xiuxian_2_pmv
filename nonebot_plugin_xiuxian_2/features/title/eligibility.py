from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from ...core.numeric import normalize_user_row
from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True, slots=True)
class TitleConditionSnapshot:
    profile: dict[str, Any] | None
    statistics: dict[str, Any]
    tower: dict[str, Any]


class TitleEligibilitySqlRepository:
    """Read the profile and condition fields used by title eligibility."""

    _FIELD_TABLES = frozenset({"statistics", "tower"})

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)

    def read_snapshot(
        self,
        *,
        user_id: str,
        statistics_user_id: str,
        condition_keys: Iterable[str],
    ) -> TitleConditionSnapshot:
        keys = {str(key) for key in condition_keys if str(key)}
        profile = self._read_profile(str(user_id))
        statistics: dict[str, Any] = {}
        tower: dict[str, Any] = {}
        if keys:
            try:
                with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                    statistics = self._read_fields(
                        uow, "statistics", str(statistics_user_id), keys
                    )
                    tower_keys = {
                        field
                        for key, field in (
                            ("通天塔最高层", "max_floor"),
                            ("通天塔积分", "score"),
                        )
                        if key in keys
                    }
                    if tower_keys:
                        tower = self._read_fields(uow, "tower", str(user_id), tower_keys)
            except Exception:
                statistics, tower = {}, {}
        return TitleConditionSnapshot(profile, statistics, tower)

    def _read_profile(self, user_id: str) -> dict[str, Any] | None:
        try:
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                row = uow.query_one(
                    "SELECT * FROM user_xiuxian WHERE user_id = ? "
                    "ORDER BY rowid ASC LIMIT 1",
                    (user_id,),
                )
        except Exception:
            return None
        return normalize_user_row(dict(row)) if row is not None else None

    def _read_fields(
        self,
        uow: DatabaseUnitOfWork,
        table: str,
        user_id: str,
        requested: set[str],
    ) -> dict[str, Any]:
        if table not in self._FIELD_TABLES or not requested:
            return {}
        table_sql = self._quote_identifier(table)
        columns = [
            str(row["name"])
            for row in uow.query_all(f"PRAGMA table_info({table_sql})")
        ]
        if "user_id" not in columns:
            return {}
        selected = [column for column in columns if column in requested]
        if not selected:
            return {}
        select_sql = ",".join(self._quote_identifier(column) for column in selected)
        row = uow.query_one(
            f"SELECT {select_sql} FROM {table_sql} "
            f"WHERE {self._quote_identifier('user_id')} = ? LIMIT 1",
            (user_id,),
        )
        if row is None:
            return {}
        return {key: self._decode_legacy_value(value) for key, value in row.items()}

    @staticmethod
    def _quote_identifier(value: str) -> str:
        return '"' + value.replace('"', '""') + '"'

    @staticmethod
    def _decode_legacy_value(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        import json

        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return value


class TitleEligibilityApplication:
    """Batch read-only title eligibility and achievement inputs."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: TitleEligibilitySqlRepository | None = None,
    ) -> None:
        self.repository = repository or TitleEligibilitySqlRepository(
            game_database, player_database
        )

    def read_snapshot(
        self,
        *,
        user_id: str,
        statistics_user_id: str,
        condition_keys: Iterable[str],
    ) -> TitleConditionSnapshot:
        return self.repository.read_snapshot(
            user_id=user_id,
            statistics_user_id=statistics_user_id,
            condition_keys=condition_keys,
        )


__all__ = [
    "TitleConditionSnapshot",
    "TitleEligibilityApplication",
    "TitleEligibilitySqlRepository",
]
