from __future__ import annotations

from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .random_target_repository import MapCandidateReadError, MapRandomTargetSqlQueryRepository


DISPLAY_PAGE_SIZE = 1


class MapNearbyDisplaySqlQueryRepository(MapRandomTargetSqlQueryRepository):
    """One canonical pair per string ID, unlike the battle's weighted pairs."""

    @staticmethod
    def _scope(upper_rowids, position, exclude_user_id):
        scope, params = MapRandomTargetSqlQueryRepository._scope(
            upper_rowids, position, exclude_user_id,
        )
        scope += (
            "AND NOT EXISTS (SELECT 1 FROM map_status AS earlier_map "
            "WHERE earlier_map.rowid<map.rowid "
            "AND CAST(earlier_map.user_id AS TEXT) COLLATE BINARY=CAST(map.user_id AS TEXT) "
            "AND earlier_map.realm=? AND earlier_map.heaven=? AND earlier_map.node_id=?) "
            "AND NOT EXISTS (SELECT 1 FROM game_data.user_xiuxian AS earlier_profile "
            "WHERE earlier_profile.rowid<profile.rowid "
            "AND CAST(earlier_profile.user_id AS TEXT)=CAST(map.user_id AS TEXT)) "
        )
        return scope, (*params, *position)

    def page(
        self,
        after: str | None,
        upper_rowids: tuple[int, int],
        position: tuple[str, str, str],
        exclude_user_id: str,
    ) -> list[dict[str, Any]]:
        scope, params = self._scope(upper_rowids, position, exclude_user_id)
        if after is not None:
            scope += "AND CAST(map.user_id AS TEXT) COLLATE BINARY>? "
            params += (after,)
        try:
            with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                uow.attach_database(f"{self.game_database.resolve().as_uri()}?mode=ro", "game_data")
                # One ID avoids multiplying an unbounded field by the page size.
                rows = uow.query_all(
                    "SELECT CAST(map.user_id AS TEXT) AS user_cursor,"
                    "map.rowid AS map_cursor,profile.rowid AS profile_cursor "
                    + scope + "ORDER BY CAST(map.user_id AS TEXT) COLLATE BINARY ASC LIMIT ?",
                    (*params, DISPLAY_PAGE_SIZE),
                )
            return rows
        except Exception as exc:
            raise MapCandidateReadError("display page unavailable") from exc


__all__ = ["MapNearbyDisplaySqlQueryRepository"]
