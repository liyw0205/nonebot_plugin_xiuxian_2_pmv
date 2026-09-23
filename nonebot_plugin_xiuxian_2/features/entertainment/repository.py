from __future__ import annotations

import json
from pathlib import Path

from .._service_port import ServicePort
from .json_state import load_json_list, save_json_list


class EntertainmentRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("entertainment", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment")
        self.database = str(database)

    def toggle_auto_checkin(self, state_path: str | Path, index: int) -> dict:
        path = Path(state_path)
        rows = load_json_list(path)
        index = int(index)
        if index < 1 or index > len(rows) or not isinstance(rows[index - 1], dict):
            return {"status": "rejected", "message": f"序号 {index} 超出范围"}
        updated = [dict(row) if isinstance(row, dict) else row for row in rows]
        updated[index - 1]["auto_checkin"] = not bool(updated[index - 1].get("auto_checkin"))
        save_json_list(path, updated)
        return {"status": "applied", "index": index, "enabled": bool(updated[index - 1]["auto_checkin"])}

    def delete_accounts(self, state_path: str | Path, indices: list[int] | None) -> dict:
        rows = load_json_list(state_path)
        if not rows:
            return {"status": "rejected", "message": "当前没有已绑定的 NewAPI 账号"}
        if indices is None:
            removed = len(rows)
            save_json_list(state_path, [])
            return {"status": "applied", "removed": removed, "remaining": 0}
        requested = sorted({int(index) for index in indices})
        to_remove = sorted({index for index in requested if 1 <= index <= len(rows)}, reverse=True)
        if not to_remove:
            return {"status": "rejected", "message": f"序号无效，当前共 {len(rows)} 个账号"}
        updated = list(rows)
        for index in to_remove:
            updated.pop(index - 1)
        save_json_list(state_path, updated)
        return {"status": "applied", "removed": len(to_remove), "remaining": len(updated)}

    def execute(self, operation_id: str, user_id: str, action: str, payload: dict) -> dict:
        if str(action).casefold() == "toggle_auto_checkin":
            return self.toggle_auto_checkin(payload["state_path"], payload["index"])
        if str(action).casefold() == "delete_accounts":
            return self.delete_accounts(payload["state_path"], payload.get("indices"))
        return super().execute(operation_id, user_id, action, payload)


__all__ = ["EntertainmentRepository"]
