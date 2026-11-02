"""Feature-owned persistence for the player's Xianshi watch list.

The watch list is a personal preference, not an asset: every mutation still
carries an operation id and writes a durable receipt, so a retried QQ message
or Web request replays the first result instead of adding a second row.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.clock import SystemClock
from ...infrastructure.database import DatabaseUnitOfWork
from .xianshi_market_schemas import (
    WATCH_LIMIT,
    is_valid_goods_name,
    name_key,
    normalize_goods_name,
)

ADD_ACTION = "add"
REMOVE_ACTION = "remove"


@dataclass(frozen=True)
class XianshiWatchResult:
    status: str
    goods_name: str
    user_id: str = ""
    watch_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status in {"added", "duplicate", "removed", "absent"}

    @property
    def changed(self) -> bool:
        return self.status in {"added", "removed"}

    @property
    def code(self) -> str | None:
        return None if self.succeeded else self.status


class XianshiWatchSqlRepository:
    """Add, remove and list watched item names in the game database."""

    def __init__(self, database: str | Path, *, clock: Any | None = None) -> None:
        self.database = Path(database)
        self.clock = clock or SystemClock()

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _has_table(uow: DatabaseUnitOfWork, name: str) -> bool:
        return (
            uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
                (name,),
            )
            is not None
        )

    @classmethod
    def _schema_ready(cls, uow: DatabaseUnitOfWork) -> bool:
        return cls._has_table(uow, "xianshi_watchlist") and cls._has_table(
            uow, "xianshi_watch_operations"
        )

    @staticmethod
    def _count(uow: DatabaseUnitOfWork, user_id: str) -> int:
        row = uow.query_one(
            "SELECT COUNT(*) AS total FROM xianshi_watchlist WHERE user_id=?",
            (user_id,),
        )
        return int(row["total"]) if row else 0

    @staticmethod
    def _receipt(
        uow: DatabaseUnitOfWork,
        operation_id: str,
        *,
        user_id: str,
        action: str,
        goods_name: str,
    ) -> str | None:
        """Return the stored result, or ``"conflict"`` for a mismatched replay."""

        row = uow.query_one(
            "SELECT user_id,action,goods_name,result FROM xianshi_watch_operations "
            "WHERE operation_id=?",
            (operation_id,),
        )
        if row is None:
            return None
        if (
            str(row["user_id"]) != user_id
            or str(row["action"]) != action
            or name_key(row["goods_name"]) != name_key(goods_name)
        ):
            return "conflict"
        return str(row["result"])

    @staticmethod
    def _write_receipt(
        uow: DatabaseUnitOfWork,
        operation_id: str,
        *,
        user_id: str,
        action: str,
        goods_name: str,
        result: str,
    ) -> None:
        uow.execute(
            "INSERT INTO xianshi_watch_operations"
            "(operation_id,user_id,action,goods_name,result) VALUES(?,?,?,?,?)",
            (operation_id, user_id, action, goods_name, result),
        )

    def _timestamp(self) -> str:
        return self.clock.now().strftime("%Y-%m-%d %H:%M:%S")

    # ------------------------------------------------------------------- reads
    def names(self, user_id: str) -> list[str]:
        """Return the player's watched names, newest first (read-only)."""

        user_id = str(user_id or "").strip()
        if not user_id or not self.database.is_file():
            return []
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if not self._has_table(uow, "xianshi_watchlist"):
                    return []
                rows = uow.query_all(
                    "SELECT goods_name FROM xianshi_watchlist WHERE user_id=? "
                    "ORDER BY rowid DESC",
                    (user_id,),
                )
        except sqlite3.Error:
            return []
        return [str(row["goods_name"]) for row in rows if str(row["goods_name"]).strip()]

    # ------------------------------------------------------------------ writes
    def add(self, operation_id: str, user_id: str, goods_name: str) -> XianshiWatchResult:
        operation_id = str(operation_id or "").strip()
        user_id = str(user_id or "").strip()
        name = normalize_goods_name(goods_name)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if not user_id:
            return XianshiWatchResult("invalid_user", name)
        if not is_valid_goods_name(name):
            return XianshiWatchResult("invalid_name", name)

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return XianshiWatchResult("schema_missing", name, user_id)
            replay = self._receipt(
                uow, operation_id, user_id=user_id, action=ADD_ACTION, goods_name=name
            )
            if replay == "conflict":
                return XianshiWatchResult("operation_conflict", name, user_id)
            if replay is not None:
                return XianshiWatchResult(
                    "duplicate", name, user_id, self._count(uow, user_id)
                )

            # One statement keeps the limit check and the insert atomic.
            inserted = uow.execute(
                "INSERT OR IGNORE INTO xianshi_watchlist(user_id,goods_name,created_at) "
                "SELECT ?,?,? WHERE "
                "(SELECT COUNT(*) FROM xianshi_watchlist WHERE user_id=?) < ?",
                (user_id, name, self._timestamp(), user_id, WATCH_LIMIT),
            ).rowcount
            count = self._count(uow, user_id)
            if inserted != 1:
                existing = uow.query_one(
                    "SELECT 1 AS present FROM xianshi_watchlist "
                    "WHERE user_id=? AND goods_name=? COLLATE NOCASE",
                    (user_id, name),
                )
                if existing is None:
                    # The limit branch writes nothing, so the same message can be
                    # retried after the player frees a slot.
                    return XianshiWatchResult("limit_reached", name, user_id, count)
                self._write_receipt(
                    uow,
                    operation_id,
                    user_id=user_id,
                    action=ADD_ACTION,
                    goods_name=name,
                    result="duplicate",
                )
                return XianshiWatchResult("duplicate", name, user_id, count)

            self._write_receipt(
                uow,
                operation_id,
                user_id=user_id,
                action=ADD_ACTION,
                goods_name=name,
                result="added",
            )
            return XianshiWatchResult("added", name, user_id, count)

    def remove(self, operation_id: str, user_id: str, goods_name: str) -> XianshiWatchResult:
        operation_id = str(operation_id or "").strip()
        user_id = str(user_id or "").strip()
        name = normalize_goods_name(goods_name)
        if not operation_id:
            raise ValueError("operation_id must not be empty")
        if not user_id:
            return XianshiWatchResult("invalid_user", name)
        if not is_valid_goods_name(name):
            return XianshiWatchResult("invalid_name", name)

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow):
                return XianshiWatchResult("schema_missing", name, user_id)
            replay = self._receipt(
                uow, operation_id, user_id=user_id, action=REMOVE_ACTION, goods_name=name
            )
            if replay == "conflict":
                return XianshiWatchResult("operation_conflict", name, user_id)
            if replay is not None:
                # The same operation already removed this watch: report the
                # idempotent no-op instead of replaying the add wording.
                return XianshiWatchResult(
                    "absent", name, user_id, self._count(uow, user_id)
                )

            removed = uow.execute(
                "DELETE FROM xianshi_watchlist WHERE user_id=? AND goods_name=? COLLATE NOCASE",
                (user_id, name),
            ).rowcount
            count = self._count(uow, user_id)
            if removed != 1:
                # Nothing was written: cancelling an absent watch is a no-op and
                # does not consume the operation id.
                return XianshiWatchResult("absent", name, user_id, count)
            self._write_receipt(
                uow,
                operation_id,
                user_id=user_id,
                action=REMOVE_ACTION,
                goods_name=name,
                result="removed",
            )
            return XianshiWatchResult("removed", name, user_id, count)


__all__ = ["ADD_ACTION", "REMOVE_ACTION", "XianshiWatchResult", "XianshiWatchSqlRepository"]
