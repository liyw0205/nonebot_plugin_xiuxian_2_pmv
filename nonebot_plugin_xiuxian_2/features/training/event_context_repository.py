from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


class TrainingEventContextSqlRepository:
    """Read the immutable player context needed to resolve one training event."""

    def __init__(self, game_database: str | Path) -> None:
        self.game_database = Path(game_database)

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA table_info("{table}")')
        }

    @staticmethod
    def _integer(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def read(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id).strip()
        if not user_id or not self.game_database.is_file():
            return {"status": "schema_missing"}
        with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
            tables = {
                str(row["name"]).casefold()
                for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
            }
            if not {"user_xiuxian", "back", "buffinfo"}.issubset(tables):
                return {"status": "schema_missing"}
            user_columns = self._columns(uow, "user_xiuxian")
            inventory_columns = self._columns(uow, "back")
            if not {"user_id", "stone", "exp", "hp", "mp", "level"}.issubset(user_columns):
                return {"status": "schema_missing"}
            if not {"user_id", "goods_id", "goods_name", "goods_num"}.issubset(inventory_columns):
                return {"status": "schema_missing"}

            row = uow.query_one(
                "SELECT user_id,stone,exp,hp,mp,level FROM user_xiuxian "
                "WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                (user_id,),
            )
            if row is None:
                return {"status": "user_missing"}
            user = dict(row)
            for field in ("stone", "exp", "hp", "mp"):
                user[field] = self._integer(user.get(field))
            user["level"] = str(user.get("level") or "")

            buff_columns = self._columns(uow, "BuffInfo")
            buff_row = uow.query_one(
                "SELECT * FROM BuffInfo WHERE user_id=? ORDER BY rowid ASC LIMIT 1",
                (user_id,),
            )
            buff_info = {
                name: self._integer((buff_row or {}).get(name))
                for name in ("sub_buff", "faqi_buff", "armor_buff")
                if name in buff_columns
            }
            for name in ("sub_buff", "faqi_buff", "armor_buff"):
                buff_info.setdefault(name, 0)

            inventory = uow.query_all(
                "SELECT goods_id,goods_name,goods_num FROM back "
                "WHERE user_id=? AND COALESCE(goods_num,0)>=1 ORDER BY rowid ASC",
                (user_id,),
            )
            for item in inventory:
                item["goods_id"] = self._integer(item["goods_id"])
                item["goods_num"] = self._integer(item["goods_num"])
        return {
            "status": "ready",
            "user": user,
            "buff_info": buff_info,
            "inventory": inventory,
        }


__all__ = ["TrainingEventContextSqlRepository"]
