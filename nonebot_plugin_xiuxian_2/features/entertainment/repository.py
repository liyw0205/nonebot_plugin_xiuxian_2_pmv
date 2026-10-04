from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from .._service_port import ServicePort
from .json_state import load_json_list, save_json_list
from .schemas import NewApiAccountListResult, NewApiAccountSummary

MAX_ACCOUNT_LIST_FILE_BYTES = 1024 * 1024
MAX_ACCOUNT_LIST_ROWS = 48
MAX_ACCOUNT_ID_CHARS = 24
MAX_ACCOUNT_BASE_URL_CHARS = 192
MAX_ACCOUNT_LABEL_CHARS = 64


def _bounded_display_text(value, limit: int) -> str:
    if isinstance(value, str):
        text = value
    elif isinstance(value, int) and not isinstance(value, bool):
        text = str(value)
    else:
        return ""
    text = " ".join(text.split())
    text = " ".join("".join(char for char in text if char.isprintable()).split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)] + "…"


def _display_base_url(value) -> str:
    raw = _bounded_display_text(value, MAX_ACCOUNT_BASE_URL_CHARS)
    if not raw:
        return "—"
    candidate = raw if "://" in raw else f"https://{raw}"
    try:
        parsed = urlsplit(candidate)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return "—"
    if parsed.scheme.casefold() not in {"http", "https"} or not hostname:
        return "—"
    if not all(char.isalnum() or char in ".-:" for char in hostname):
        return "—"
    if port is not None and not 1 <= port <= 65535:
        return "—"
    if ":" in hostname:
        hostname = f"[{hostname}]"
    authority = f"{hostname}:{port}" if port is not None else hostname
    path = parsed.path[:96]
    if len(parsed.path) > len(path):
        path = path[:-1] + "…"
    safe = urlunsplit((parsed.scheme.casefold(), authority, path, "", ""))
    return _bounded_display_text(safe, MAX_ACCOUNT_BASE_URL_CHARS) or "—"


def _account_summary(row: dict) -> NewApiAccountSummary:
    api_user_id = _bounded_display_text(row.get("api_user_id"), MAX_ACCOUNT_ID_CHARS)
    if not api_user_id.isdigit():
        api_user_id = "?"
    mode_value = row.get("mode")
    return NewApiAccountSummary(
        api_user_id=api_user_id,
        mode="cookie" if isinstance(mode_value, str) and mode_value.casefold() == "cookie" else "token",
        base_url=_display_base_url(row.get("base_url")),
        label=_bounded_display_text(row.get("label"), MAX_ACCOUNT_LABEL_CHARS),
        auto_checkin=bool(row.get("auto_checkin")),
    )


class EntertainmentRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("entertainment", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment")
        self.database = str(database)

    def list_account_summaries(self, state_path: str | Path) -> NewApiAccountListResult:
        path = Path(state_path)
        try:
            with path.open("rb") as handle:
                payload = handle.read(MAX_ACCOUNT_LIST_FILE_BYTES + 1)
        except FileNotFoundError:
            return NewApiAccountListResult("missing")
        except OSError:
            return NewApiAccountListResult("unavailable")
        if len(payload) > MAX_ACCOUNT_LIST_FILE_BYTES:
            return NewApiAccountListResult("too_large")
        try:
            rows = json.loads(payload)
        except (ValueError, RecursionError):
            return NewApiAccountListResult("invalid")
        if not isinstance(rows, list):
            return NewApiAccountListResult("invalid")
        if len(rows) > MAX_ACCOUNT_LIST_ROWS:
            return NewApiAccountListResult("too_many")
        summaries = tuple(_account_summary(row) for row in rows if isinstance(row, dict))
        return NewApiAccountListResult("ok", summaries)

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
