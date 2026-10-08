from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork


class MessageReplyRepository:
    """Read QQ reply candidates from an existing message log database."""

    def __init__(
        self,
        database: str | Path,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.database = Path(database)
        self.now = now or SystemClock().now

    def get_latest_reply_candidates_for_qq(
        self, scene: str, target_id: str, limit: int = 3
    ) -> list[dict[str, Any]]:
        if scene in ("group", "channel_group"):
            target_column = "group_id"
            seconds = 4 * 60
        elif scene in ("private", "channel_private"):
            target_column = "user_id"
            seconds = 60 * 60
        else:
            return []

        if seconds <= 0:
            return []
        since = (self.now() - timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")
        return self._query_all(
            "SELECT * FROM messages "
            "WHERE adapter='QQ' AND direction='recv' AND scene=? "
            f"AND {target_column}=? AND message_id IS NOT NULL AND message_id!='' "
            "AND created_at>=? AND COALESCE(reply_used_count,0)<5 "
            "ORDER BY created_at DESC,id DESC LIMIT ?",
            (scene, str(target_id), since, limit),
        )

    def get_specific_reply_candidate_for_qq(
        self,
        *,
        scene: str,
        target_id: str,
        message_id: str,
    ) -> dict[str, Any] | None:
        if not message_id:
            return None
        target_column = self._target_column(scene)
        if target_column is None:
            return None

        row = self._query_one(
            "SELECT * FROM messages "
            "WHERE adapter='QQ' AND direction='recv' AND scene=? "
            f"AND {target_column}=? AND message_id=? "
            "AND COALESCE(reply_used_count,0)<5 "
            "ORDER BY created_at DESC,id DESC LIMIT 1",
            (scene, str(target_id), str(message_id)),
        )
        if row is None:
            return None

        seconds = 5 * 60 if scene in ("group", "channel_group") else 60 * 60
        created_at = row.get("created_at", "")
        if not created_at or seconds <= 0:
            return None
        try:
            msg_time = datetime.strptime(str(created_at)[:19], "%Y-%m-%d %H:%M:%S")
            now = self.now().replace(tzinfo=None)
            if now - msg_time > timedelta(seconds=seconds):
                return None
        except (TypeError, ValueError):
            return None
        return row

    def get_specific_reference_candidate_for_qq(
        self,
        *,
        scene: str,
        target_id: str,
        message_id: str = "",
        reference_id: str = "",
    ) -> dict[str, Any] | None:
        message_id = str(message_id or "").strip()
        reference_id = str(reference_id or "").strip()
        if not message_id and not reference_id:
            return None
        target_column = self._target_column(scene)
        if target_column is None:
            return None

        matches = []
        params: list[Any] = [scene, str(target_id)]
        if message_id:
            matches.append("message_id=?")
            params.append(message_id)
        if reference_id:
            matches.append("reference_id=?")
            params.append(reference_id)

        return self._query_one(
            "SELECT * FROM messages "
            "WHERE adapter='QQ' AND direction='recv' AND scene=? "
            f"AND {target_column}=? AND ({' OR '.join(matches)}) "
            "ORDER BY created_at DESC,id DESC LIMIT 1",
            params,
        )

    def _query_all(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        if not self.database.is_file():
            return []
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if not self._messages_table_exists(uow):
                    return []
                return uow.query_all(sql, params)
        except (FileNotFoundError, sqlite3.OperationalError):
            if not self.database.is_file():
                return []
            raise

    def _query_one(self, sql: str, params: tuple[Any, ...] | list[Any]) -> dict[str, Any] | None:
        if not self.database.is_file():
            return None
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if not self._messages_table_exists(uow):
                    return None
                return uow.query_one(sql, params)
        except (FileNotFoundError, sqlite3.OperationalError):
            if not self.database.is_file():
                return None
            raise

    @staticmethod
    def _messages_table_exists(uow: DatabaseUnitOfWork) -> bool:
        return uow.query_one(
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
            ("messages",),
        ) is not None

    @staticmethod
    def _target_column(scene: str) -> str | None:
        if scene in ("group", "channel_group"):
            return "group_id"
        if scene in ("private", "channel_private"):
            return "user_id"
        return None


__all__ = ["MessageReplyRepository"]
