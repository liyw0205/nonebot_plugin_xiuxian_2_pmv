from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork


@dataclass(frozen=True)
class InventoryGrantResult:
    status: str
    user_id: str
    item_id: int
    requested: int = 0
    applied: int = 0
    final_quantity: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status == "applied"


class PlayerInventorySqlRepository:
    """Bounded item grants against the existing legacy backpack table."""

    REQUIRED_COLUMNS = {"user_id", "goods_id", "goods_name", "goods_type", "goods_num"}

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database)

    @classmethod
    def _columns(cls, uow: DatabaseUnitOfWork) -> set[str]:
        table = uow.query_one(
            "SELECT 1 AS present FROM sqlite_master "
            "WHERE type='table' AND name='back'"
        )
        if table is None:
            return set()
        return {
            str(row["name"]).casefold()
            for row in uow.query_all('PRAGMA table_info("back")')
        }

    @staticmethod
    def _result(
        status: str,
        user_id: str,
        item_id: int,
        requested: int = 0,
        applied: int = 0,
        final_quantity: int = 0,
    ) -> InventoryGrantResult:
        return InventoryGrantResult(
            status,
            str(user_id),
            int(item_id),
            int(requested),
            int(applied),
            int(final_quantity),
        )

    def grant(
        self,
        user_id: str,
        item_id: int,
        item_name: str,
        item_type: str,
        quantity: int,
        *,
        bind_flag: int = 0,
        max_goods_num: int,
        require_full: bool = False,
    ) -> InventoryGrantResult:
        user_id = str(user_id).strip()
        item_id, quantity, max_goods_num = int(item_id), int(quantity), int(max_goods_num)
        raw_quantity = abs(quantity)
        requested = min(raw_quantity, max(max_goods_num, 0))
        if not user_id or item_id < 0 or max_goods_num < 0:
            return self._result("invalid", user_id, item_id, requested)
        if require_full and raw_quantity > max_goods_num:
            return self._result("inventory_full", user_id, item_id, requested)
        if not self.database.is_file():
            return self._result("schema_missing", user_id, item_id, requested)

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            columns = self._columns(uow)
            if not self.REQUIRED_COLUMNS.issubset(columns):
                return self._result("schema_missing", user_id, item_id, requested)
            if requested == 0:
                return self._result("applied", user_id, item_id, requested)

            row = uow.query_one(
                "SELECT rowid AS _rowid,COALESCE(goods_num,0) AS goods_num "
                "FROM back WHERE user_id=? AND goods_id=? ORDER BY rowid ASC LIMIT 1",
                (user_id, item_id),
            )
            current = max(int(row["goods_num"] or 0), 0) if row else 0
            final = min(current + requested, max_goods_num)
            applied = max(final - current, 0)
            if require_full and applied < requested:
                return self._result("inventory_full", user_id, item_id, requested, 0, current)
            bind = 1 if int(bind_flag) == 1 else 0
            updates = ["goods_name=?", "goods_type=?", "goods_num=?"]
            params: list[Any] = [str(item_name), str(item_type), final]
            if "update_time" in columns:
                updates.append("update_time=CURRENT_TIMESTAMP")
            if "bind_num" in columns:
                current_bind = 0
                if row is not None:
                    bind_row = uow.query_one(
                        "SELECT COALESCE(bind_num,0) AS bind_num FROM back WHERE rowid=?",
                        (row["_rowid"],),
                    )
                    current_bind = max(int(bind_row["bind_num"] or 0), 0) if bind_row else 0
                updates.append("bind_num=?")
                params.append(min(final, current_bind + applied) if bind else min(current_bind, final))

            if row is not None:
                changed = uow.execute(
                    "UPDATE back SET " + ",".join(updates) + " WHERE rowid=?",
                    (*params, row["_rowid"]),
                )
                if changed.rowcount != 1:
                    return self._result("state_changed", user_id, item_id, requested, 0, current)
            else:
                insert_columns = ["user_id", "goods_id", "goods_name", "goods_type", "goods_num"]
                values: list[Any] = [user_id, item_id, str(item_name), str(item_type), final]
                if "create_time" in columns:
                    insert_columns.append("create_time")
                    values.append(None)
                if "update_time" in columns:
                    insert_columns.append("update_time")
                    values.append(None)
                if "bind_num" in columns:
                    insert_columns.append("bind_num")
                    values.append(final if bind else 0)
                placeholders = ",".join("?" for _ in insert_columns)
                changed = uow.execute(
                    f"INSERT INTO back({','.join(insert_columns)}) VALUES({placeholders})",
                    tuple(values),
                )
                if changed.rowcount != 1:
                    return self._result("state_changed", user_id, item_id, requested, 0, current)
            return self._result("applied", user_id, item_id, requested, applied, final)


__all__ = ["InventoryGrantResult", "PlayerInventorySqlRepository"]
