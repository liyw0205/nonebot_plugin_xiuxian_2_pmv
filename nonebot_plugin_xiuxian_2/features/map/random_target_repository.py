from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


CANDIDATE_PAGE_SIZE = 256


class MapCandidateReadError(RuntimeError):
    """A failed scan cannot return a biased partial sample."""


class MapRandomTargetSqlQueryRepository:
    """Read map/profile pairs without retaining a connection between calls."""

    def __init__(self, player_database: str | Path, game_database: str | Path) -> None:
        self.player_database = Path(player_database)
        self.game_database = Path(game_database)

    def upper_rowids(self) -> tuple[int, int] | None:
        try:
            with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                uow.attach_database(f"{self.game_database.resolve().as_uri()}?mode=ro", "game_data")
                row = uow.query_one(
                    "SELECT (SELECT MAX(rowid) FROM map_status) AS map_upper,"
                    "(SELECT MAX(rowid) FROM game_data.user_xiuxian) AS profile_upper"
                )
            if row["map_upper"] is None or row["profile_upper"] is None:
                return None
            return int(row["map_upper"]), int(row["profile_upper"])
        except Exception as exc:
            raise MapCandidateReadError("candidate boundaries unavailable") from exc

    @staticmethod
    def _scope(upper_rowids, position, exclude_user_id):
        return (
            "FROM map_status AS map JOIN game_data.user_xiuxian AS profile "
            "ON CAST(profile.user_id AS TEXT)=CAST(map.user_id AS TEXT) "
            "WHERE map.realm=? AND map.heaven=? AND map.node_id=? "
            "AND CAST(map.user_id AS TEXT) COLLATE BINARY<>? "
            "AND map.rowid<=? AND profile.rowid<=? ",
            (*position, exclude_user_id, *upper_rowids),
        )

    def page(
        self,
        after: tuple[int, int] | None,
        upper_rowids: tuple[int, int],
        position: tuple[str, str, str],
        exclude_user_id: str,
    ) -> list[dict[str, Any]]:
        scope, params = self._scope(upper_rowids, position, exclude_user_id)
        if after is not None:
            scope += "AND (map.rowid,profile.rowid)>(?,?) "
            params += after
        try:
            with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                uow.attach_database(f"{self.game_database.resolve().as_uri()}?mode=ro", "game_data")
                # Numeric metadata only: large profile fields are fetched one at a time.
                rows = uow.query_all(
                    "SELECT map.rowid AS map_cursor,profile.rowid AS profile_cursor "
                    + scope + "ORDER BY map.rowid ASC,profile.rowid ASC LIMIT ?",
                    (*params, CANDIDATE_PAGE_SIZE),
                )
            return rows
        except Exception as exc:
            raise MapCandidateReadError("candidate page unavailable") from exc

    def candidate(
        self,
        cursor: tuple[int, int],
        upper_rowids: tuple[int, int],
        position: tuple[str, str, str],
        exclude_user_id: str,
        *,
        expected_user_id: str | None = None,
    ) -> dict[str, Any] | None:
        scope, params = self._scope(upper_rowids, position, exclude_user_id)
        if expected_user_id is not None:
            scope += "AND CAST(map.user_id AS TEXT) COLLATE BINARY=? "
            params += (str(expected_user_id),)
        try:
            with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                uow.attach_database(f"{self.game_database.resolve().as_uri()}?mode=ro", "game_data")
                row = uow.query_one(
                    "SELECT map.user_id,profile.user_name,profile.level,profile.power "
                    + scope + "AND map.rowid=? AND profile.rowid=? LIMIT 1",
                    (*params, *cursor),
                )
            if row is None:
                return None
            return {
                "user_id": str(row["user_id"]),
                "user_name": str(row["user_name"] or ""),
                "level": str(row["level"] or ""),
                "power": int(row["power"] or 0),
            }
        except Exception as exc:
            raise MapCandidateReadError("candidate state unavailable") from exc


__all__ = ["MapCandidateReadError", "MapRandomTargetSqlQueryRepository"]
