from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Iterator
from typing import Any, Callable

from ...core.numeric import require_exact_integer
from ...infrastructure.database import DatabaseUnitOfWork
from .random_target_repository import MapCandidateReadError, MapRandomTargetSqlQueryRepository


DISPLAY_PAGE_SIZE = 1


class MapNearbyDisplayReadError(MapCandidateReadError):
    def __init__(self, message: str, *, subject: str) -> None:
        super().__init__(message)
        self.subject = subject


class MapNearbyDisplaySqlQueryRepository(MapRandomTargetSqlQueryRepository):
    """One canonical pair per string ID, unlike the battle's weighted pairs."""

    def __init__(self, player_database, game_database, *, optimized: bool = False) -> None:
        super().__init__(player_database, game_database)
        self.optimized = bool(optimized)

    @contextmanager
    def candidates(
        self,
        upper_rowids: tuple[int, int],
        position: tuple[str, str, str],
        exclude_user_id: str,
    ) -> Iterator[Iterator[dict[str, Any]]]:
        """Stream the frozen read set through one attached read transaction.

        The legacy page/candidate pair remains available for old callers and
        schema fixtures.  The production command opts into this path after the
        additive nearby indexes are installed; SQLite's read transaction keeps
        map/profile rows consistent while the reservoir sampler yields.
        """
        scope, params = self._scope(upper_rowids, position, exclude_user_id)
        scope = scope.replace(
            "AND map.rowid<=? AND profile.rowid<=? ", "",
        )
        params = (*params[:4], *params[6:])
        sql = (
            "SELECT CAST(map.user_id AS TEXT) AS user_id,"
            "profile.user_name,profile.level,profile.power "
            + scope
            + "ORDER BY CAST(map.user_id AS TEXT) COLLATE BINARY ASC"
        )
        try:
            with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                uow.attach_database(f"{self.game_database.resolve().as_uri()}?mode=ro", "game_data")
                cursor = uow.execute(sql, params)

                def rows() -> Iterator[dict[str, Any]]:
                    while True:
                        batch = cursor.fetchmany(256)
                        if not batch:
                            return
                        for row in batch:
                            yield {
                                "user_id": str(row["user_id"]),
                                "user_name": str(row["user_name"] or ""),
                                "level": str(row["level"] or ""),
                                "power": require_exact_integer(
                                    row["power"] if row["power"] is not None else 0,
                                    field="power",
                                    subject="target",
                                ),
                            }

                yield rows()
        except Exception as exc:
            if isinstance(exc, MapNearbyDisplayReadError):
                raise
            raise MapNearbyDisplayReadError(
                "display stream unavailable", subject="nearby_candidates",
            ) from exc

    @contextmanager
    def candidates_for_user(
        self,
        user_id: str,
        exclude_user_id: str,
        status_validator: Callable[[dict[str, str]], bool],
    ) -> Iterator[tuple[dict[str, str], Iterator[dict[str, Any]]] | None]:
        """Read a canonical actor position and candidates in one read transaction."""
        subject = "map_status"
        try:
            with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                row = uow.query_one(
                    "SELECT realm,heaven,node_id FROM map_status WHERE user_id=?",
                    (str(user_id),),
                )
                if row is None:
                    yield None
                    return
                status = {
                    "realm": str(row["realm"] or ""),
                    "heaven": str(row["heaven"] or ""),
                    "node_id": str(row["node_id"] or ""),
                }
                if not all(status.values()) or not status_validator(status):
                    yield None
                    return
                position = (status["realm"], status["heaven"], status["node_id"])
                scope, params = self._scope((0, 0), position, str(exclude_user_id))
                scope = scope.replace(
                    "AND map.rowid<=? AND profile.rowid<=? ", "",
                )
                params = (*params[:4], *params[6:])
                sql = (
                    "SELECT CAST(map.user_id AS TEXT) AS user_id,"
                    "profile.user_name,profile.level,profile.power "
                    + scope
                    + "ORDER BY CAST(map.user_id AS TEXT) COLLATE BINARY ASC"
                )
                uow.attach_database(
                    f"{self.game_database.resolve().as_uri()}?mode=ro", "game_data"
                )
                subject = "nearby_candidates"
                cursor = uow.execute(sql, params)

                def rows() -> Iterator[dict[str, Any]]:
                    while True:
                        batch = cursor.fetchmany(256)
                        if not batch:
                            return
                        for candidate in batch:
                            yield {
                                "user_id": str(candidate["user_id"]),
                                "user_name": str(candidate["user_name"] or ""),
                                "level": str(candidate["level"] or ""),
                                "power": require_exact_integer(
                                    candidate["power"] if candidate["power"] is not None else 0,
                                    field="power",
                                    subject="target",
                                ),
                            }

                yield status, rows()
        except Exception as exc:
            if isinstance(exc, MapNearbyDisplayReadError):
                raise
            raise MapNearbyDisplayReadError("display actor stream unavailable", subject=subject) from exc

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
            raise MapNearbyDisplayReadError(
                "display page unavailable", subject="nearby_candidates",
            ) from exc


__all__ = ["MapNearbyDisplaySqlQueryRepository"]
