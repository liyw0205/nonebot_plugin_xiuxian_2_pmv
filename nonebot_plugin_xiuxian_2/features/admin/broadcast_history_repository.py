from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork
from .broadcast_repository import adapter_family


class AdminBroadcastHistoryRepository:
    """Read broadcast destinations without initializing the message database."""

    _SCENES = {
        "group": ("group", "channel_group"),
        "private": ("private", "channel_private"),
        "global": ("group", "channel_group", "private", "channel_private"),
    }
    _COMMON_COLUMNS = {"id", "adapter", "bot_id", "scene", "group_id", "user_id"}

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    def targets(
        self, adapter: str, bot_id: str, kind: str, now: datetime,
    ) -> list[dict[str, str]]:
        adapter = str(adapter or "")
        bot_id = str(bot_id or "").strip()
        scenes = self._SCENES.get(kind)
        family = adapter_family(adapter)
        qq = family == "qq"
        if not scenes or not bot_id or not family or not self.database.is_file():
            return []

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            if uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='messages'"
            ) is None:
                return []
            required = self._COMMON_COLUMNS | ({"direction", "created_at", "message_id"} if qq else set())
            columns = {str(row["name"]) for row in uow.query_all("PRAGMA table_info(messages)")}
            missing = sorted(required - columns)
            if missing:
                raise RuntimeError("broadcast history schema incomplete: " + ",".join(missing))
            if qq:
                return self._qq_targets(uow, adapter, bot_id, scenes, now)
            targets = []
            if kind in ("group", "global"):
                targets.extend(self._ob11_targets(uow, adapter, bot_id, "group_id", self._SCENES["group"]))
            if kind in ("private", "global"):
                targets.extend(self._ob11_targets(uow, adapter, bot_id, "user_id", self._SCENES["private"]))
            return targets

    @staticmethod
    def _qq_targets(
        uow: DatabaseUnitOfWork, adapter: str, bot_id: str,
        scenes: tuple[str, ...], now: datetime,
    ) -> list[dict[str, str]]:
        since = (now - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
        placeholders = ",".join("?" for _ in scenes)
        target = "CASE WHEN scene IN ('group','channel_group') THEN group_id ELSE user_id END"
        rows = uow.query_all(
            "WITH ranked_targets AS ("
            f"SELECT scene,{target} AS target_id,message_id,created_at,id,"
            f"ROW_NUMBER() OVER (PARTITION BY scene,{target} "
            "ORDER BY created_at DESC,id DESC) AS target_rank "
            "FROM messages WHERE adapter=? AND bot_id=? AND direction='recv' "
            f"AND scene IN ({placeholders}) AND created_at>=? "
            "AND message_id IS NOT NULL AND message_id<>'' "
            f"AND {target} IS NOT NULL AND {target}<>''"
            ") SELECT scene,target_id,message_id FROM ranked_targets "
            "WHERE target_rank=1 ORDER BY created_at DESC,id DESC",
            (adapter, bot_id, *scenes, since),
        )
        return [
            {"scene": str(row["scene"]), "target_id": str(row["target_id"]), "message_id": str(row["message_id"])}
            for row in rows
        ]

    @staticmethod
    def _ob11_targets(
        uow: DatabaseUnitOfWork, adapter: str, bot_id: str,
        target_column: str, scenes: tuple[str, ...],
    ) -> list[dict[str, str]]:
        placeholders = ",".join("?" for _ in scenes)
        rows = uow.query_all(
            f"SELECT scene,{target_column} AS target_id,MAX(id) AS latest_id "
            f"FROM messages WHERE adapter=? AND bot_id=? AND scene IN ({placeholders}) "
            f"AND {target_column} IS NOT NULL AND {target_column}<>'' "
            f"GROUP BY scene,{target_column} ORDER BY latest_id DESC",
            (adapter, bot_id, *scenes),
        )
        return [
            {"scene": str(row["scene"]), "target_id": str(row["target_id"]), "message_id": ""}
            for row in rows
        ]


__all__ = ["AdminBroadcastHistoryRepository"]
