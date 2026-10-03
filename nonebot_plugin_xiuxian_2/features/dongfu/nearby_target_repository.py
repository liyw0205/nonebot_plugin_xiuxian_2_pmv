from __future__ import annotations

try:
    import ujson as json
except ImportError:
    import json

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class DongfuNearbyTargetSqlQueryRepository:
    """Select one same-node name match without materializing nearby profiles."""

    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.game_database = Path(game_database)
        self.player_database = Path(player_database)

    def get(self, user_id: str, user_name: str) -> dict[str, Any] | None:
        user_id = str(user_id or "").strip()
        if not user_id or not user_name:
            return None
        if not self.player_database.is_file() or not self.game_database.is_file():
            return None
        try:
            with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                position = uow.query_one(
                    "SELECT realm,heaven,node_id FROM map_status "
                    "WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                    (user_id,),
                )
                if position is None:
                    return None
                values = []
                # Preserve legacy map-field decoding and equality parameters.
                for value in position.values():
                    if isinstance(value, str):
                        try:
                            value = json.loads(value)
                        except ValueError:
                            pass
                    if not value:
                        return None
                    values.append(
                        json.dumps(value, ensure_ascii=False)
                        if isinstance(value, (dict, list)) else str(value)
                    )
                uow.attach_database(
                    f"{self.game_database.resolve().as_uri()}?mode=ro", "game_data"
                )
                # Match the first profile for each ID before filtering its name.
                target = uow.query_one(
                    "SELECT profile.user_id,profile.user_name FROM map_status AS nearby "
                    "JOIN game_data.user_xiuxian AS profile ON profile.rowid=("
                    "SELECT rowid FROM game_data.user_xiuxian "
                    "WHERE user_id=CAST(nearby.user_id AS TEXT) ORDER BY rowid ASC LIMIT 1) "
                    "WHERE nearby.realm=? AND nearby.heaven=? AND nearby.node_id=? "
                    "AND profile.user_name COLLATE BINARY=? ORDER BY nearby.rowid ASC LIMIT 1",
                    (*values, str(user_name)),
                )
        except Exception:
            return None
        if target is None:
            return None
        return {"user_id": str(target["user_id"]), "user_name": target["user_name"]}


__all__ = ["DongfuNearbyTargetSqlQueryRepository"]
