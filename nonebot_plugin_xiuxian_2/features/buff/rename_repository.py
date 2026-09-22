from __future__ import annotations

import json
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork


class BlessedSpotRenameResult:
    def __init__(self, status: str, user_id: str, name: str = "") -> None:
        self.status = status
        self.user_id = user_id
        self.name = name


class BlessedSpotRenameSqlRepository:
    def __init__(self, database: str | Path) -> None:
        self.database = str(database)

    def rename(self, operation_id: str, user_id: str, expected_name: str, new_name: str) -> BlessedSpotRenameResult:
        operation_id, user_id, expected_name, new_name = str(operation_id).strip(), str(user_id), str(expected_name), str(new_name).strip()
        if not operation_id or not new_name or len(new_name) > 9:
            raise ValueError("valid operation and name up to 9 characters are required")
        payload = json.dumps([user_id, new_name], ensure_ascii=False, separators=(",", ":"))
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE IF NOT EXISTS blessed_spot_operations(operation_id TEXT PRIMARY KEY,action TEXT NOT NULL,payload TEXT NOT NULL,result_json TEXT NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            previous = uow.query_one("SELECT payload,result_json FROM blessed_spot_operations WHERE operation_id=? AND action=?", (operation_id, "rename"))
            if previous is not None:
                if str(previous["payload"]) != payload:
                    return BlessedSpotRenameResult("state_changed", user_id)
                return BlessedSpotRenameResult("duplicate", user_id, json.loads(str(previous["result_json"]))["name"])
            row = uow.query_one("SELECT COALESCE(blessed_spot_flag,0) AS enabled,COALESCE(blessed_spot_name,'') AS name FROM user_xiuxian WHERE user_id=?", (user_id,))
            if row is None or int(row["enabled"]) == 0:
                return BlessedSpotRenameResult("blessed_spot_missing", user_id)
            if str(row["name"]) != expected_name:
                return BlessedSpotRenameResult("state_changed", user_id)
            changed = uow.execute("UPDATE user_xiuxian SET blessed_spot_name=? WHERE user_id=? AND COALESCE(blessed_spot_name,'')=? AND COALESCE(blessed_spot_flag,0)<>0", (new_name, user_id, expected_name))
            if changed.rowcount != 1:
                return BlessedSpotRenameResult("state_changed", user_id)
            uow.execute("INSERT INTO blessed_spot_operations(operation_id,action,payload,result_json) VALUES(?,?,?,?)", (operation_id, "rename", payload, json.dumps({"name": new_name}, ensure_ascii=False)))
            return BlessedSpotRenameResult("applied", user_id, new_name)


__all__ = ["BlessedSpotRenameSqlRepository", "BlessedSpotRenameResult"]
