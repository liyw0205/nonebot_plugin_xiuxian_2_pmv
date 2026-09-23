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

    def execute(self, operation_id: str, user_id: str, action: str, payload: dict) -> dict:
        if str(action).casefold() == "toggle_auto_checkin":
            return self.toggle_auto_checkin(payload["state_path"], payload["index"])
        return super().execute(operation_id, user_id, action, payload)


__all__ = ["EntertainmentRepository"]
