from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class BaseRenameResult:
    def __init__(self, status: str, user_id: str, rename_kind: str, new_name: str, previous_name: str = "") -> None:
        self.status = status
        self.user_id = user_id
        self.rename_kind = rename_kind
        self.new_name = new_name
        self.previous_name = previous_name

    @property
    def succeeded(self) -> bool:
        return self.status in {"renamed", "duplicate"}


class BaseRenameSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {
            str(row["name"]).casefold()
            for row in uow.query_all(f'PRAGMA table_info("{table}")')
        }

    @classmethod
    def _schema_ready(
        cls,
        uow: DatabaseUnitOfWork,
        rename_kind: str,
        item_id: int | None,
    ) -> bool:
        name_column = "user_name" if rename_kind == "user" else "root"
        required = {
            "player_rename_operations": {
                "operation_id", "user_id", "rename_type", "new_name", "previous_name", "payload",
            },
            "user_xiuxian": {"user_id", name_column, "stone"},
        }
        if item_id is not None:
            required["back"] = {"user_id", "goods_id", "goods_num", "bind_num"}
        return all(expected.issubset(cls._columns(uow, table)) for table, expected in required.items())

    def get_result(self, operation_id: str) -> BaseRenameResult | None:
        operation_id = str(operation_id).strip()
        if not operation_id or not Path(self.database).is_file():
            return None
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            required = {"operation_id", "user_id", "rename_type", "new_name", "previous_name"}
            if not required.issubset(self._columns(uow, "player_rename_operations")):
                return None
            row = uow.query_one(
                "SELECT user_id,rename_type,new_name,previous_name "
                "FROM player_rename_operations WHERE operation_id=?",
                (operation_id,),
            )
        if row is None:
            return None
        return BaseRenameResult(
            "duplicate",
            str(row["user_id"]),
            str(row["rename_type"]),
            str(row["new_name"]),
            str(row["previous_name"] or ""),
        )

    def rename(self, operation_id: str, user_id: str, rename_kind: str, new_name: str, *, item_id: int | None = None, stone_cost: int = 0) -> BaseRenameResult:
        operation_id, user_id, rename_kind, new_name = str(operation_id).strip(), str(user_id), str(rename_kind), str(new_name).strip()
        if not operation_id or not user_id or not new_name or rename_kind not in {"user", "root"}:
            raise ValueError("rename operation, user, kind and name are required")
        column = "user_name" if rename_kind == "user" else "root"
        payload = json.dumps([user_id, rename_kind, new_name, item_id, int(stone_cost)], ensure_ascii=False, separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            if not self._schema_ready(uow, rename_kind, item_id):
                return BaseRenameResult("schema_missing", user_id, rename_kind, new_name)
            previous = uow.query_one("SELECT payload,user_id,rename_type,new_name,previous_name FROM player_rename_operations WHERE operation_id=?", (operation_id,))
            if previous is not None:
                if previous["payload"] is not None and str(previous["payload"]) != payload:
                    return BaseRenameResult("state_changed", user_id, rename_kind, new_name)
                return BaseRenameResult("duplicate", str(previous["user_id"]), str(previous["rename_type"]), str(previous["new_name"]), str(previous["previous_name"]))
            row = uow.query_one(f"SELECT COALESCE({column},'') AS name,COALESCE(stone,0) AS stone FROM user_xiuxian WHERE user_id=?", (user_id,))
            if row is None:
                return BaseRenameResult("user_missing", user_id, rename_kind, new_name)
            old = str(row["name"] or "")
            if old == new_name:
                return BaseRenameResult("unchanged", user_id, rename_kind, new_name, old)
            if rename_kind == "user" and uow.query_one("SELECT 1 FROM user_xiuxian WHERE user_name=? AND user_id<>?", (new_name, user_id)) is not None:
                return BaseRenameResult("name_conflict", user_id, rename_kind, new_name, old)
            if item_id is not None:
                changed = uow.execute("UPDATE back SET goods_num=goods_num-1,bind_num=CASE WHEN goods_num-1<=0 THEN 0 ELSE MIN(COALESCE(bind_num,0),goods_num-1) END WHERE user_id=? AND goods_id=? AND goods_num>0", (user_id, int(item_id)))
                if changed.rowcount != 1:
                    return BaseRenameResult("item_missing", user_id, rename_kind, new_name, old)
            elif int(stone_cost) > 0:
                changed = uow.execute("UPDATE user_xiuxian SET stone=stone-? WHERE user_id=? AND COALESCE(stone,0)>=?", (int(stone_cost), user_id, int(stone_cost)))
                if changed.rowcount != 1:
                    return BaseRenameResult("stone_insufficient", user_id, rename_kind, new_name, old)
            uow.execute(f"UPDATE user_xiuxian SET {column}=? WHERE user_id=? AND COALESCE({column},'')=?", (new_name, user_id, old))
            uow.execute("INSERT INTO player_rename_operations(operation_id,user_id,rename_type,new_name,previous_name,payload) VALUES(?,?,?,?,?,?)", (operation_id, user_id, rename_kind, new_name, old, payload))
            return BaseRenameResult("renamed", user_id, rename_kind, new_name, old)


__all__ = ["BaseRenameSqlRepository", "BaseRenameResult"]
