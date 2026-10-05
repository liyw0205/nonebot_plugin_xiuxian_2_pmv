from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from .._service_port import ServicePort
from .json_state import load_json_list, save_json_list
from .schemas import (
    NewApiAccountListResult,
    NewApiAccountSummary,
    NewApiCheckinTarget,
    NewApiCheckinTargetsResult,
)
from ...xiuxian.xiuxian_utils.json_store import update_json_file_bounded

MAX_ACCOUNT_LIST_FILE_BYTES = 1024 * 1024
MAX_ACCOUNT_LIST_ROWS = 48
MAX_ACCOUNT_ID_CHARS = 24
MAX_ACCOUNT_BASE_URL_CHARS = 192
MAX_ACCOUNT_LABEL_CHARS = 64
MAX_CHECKIN_SECRET_CHARS = 4096
MAX_CHECKIN_BASE_URL_CHARS = 512
MAX_CHECKIN_SELECTOR_CHARS = 64
MAX_CHECKIN_HISTORY_BYTES = 64 * 1024
MAX_CHECKIN_HISTORY_ROWS = 3
MAX_INFO_TARGETS = 8


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


def _read_account_rows(path: Path) -> tuple[str, list[dict] | None]:
    try:
        with path.open("rb") as handle:
            payload = handle.read(MAX_ACCOUNT_LIST_FILE_BYTES + 1)
    except FileNotFoundError:
        return "missing", None
    except OSError:
        return "unavailable", None
    if len(payload) > MAX_ACCOUNT_LIST_FILE_BYTES:
        return "too_large", None
    try:
        rows = json.loads(payload)
    except (ValueError, RecursionError):
        return "invalid", None
    if not isinstance(rows, list):
        return "invalid", None
    if len(rows) > MAX_ACCOUNT_LIST_ROWS:
        return "too_many", None
    return "ok", [row for row in rows if isinstance(row, dict)]


def _checkin_selector_indices(text: str, account_count: int) -> tuple[list[int] | None, str | None]:
    selector = (text or "").strip()
    if len(selector) > MAX_CHECKIN_SELECTOR_CHARS:
        return None, "序号输入过长"
    if not selector:
        return list(range(1, account_count + 1)), None

    indices: set[int] = set()
    for part in selector.replace("，", ",").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            left, right = part.split("-", 1)
            try:
                start, end = int(left.strip()), int(right.strip())
            except ValueError:
                return None, f"无法解析序号段：{part}"
            low, high = sorted((start, end))
            if high - low + 1 > MAX_ACCOUNT_LIST_ROWS:
                return None, f"一次最多选择 {MAX_ACCOUNT_LIST_ROWS} 个账号"
            indices.update(range(low, high + 1))
        else:
            try:
                indices.add(int(part))
            except ValueError:
                return None, f"无法解析序号：{part}"
        if len(indices) > MAX_ACCOUNT_LIST_ROWS:
            return None, f"一次最多选择 {MAX_ACCOUNT_LIST_ROWS} 个账号"
    if not indices:
        return None, "请指定序号，例如：newapi签到 1"
    for index in sorted(indices):
        if index < 1 or index > account_count:
            return None, f"序号 {index} 超出范围（1～{account_count}）"
    return sorted(indices), None


class EntertainmentRepository(ServicePort):
    def __init__(self, database: str | Path) -> None:
        super().__init__("entertainment", "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_entertainment")
        self.database = str(database)

    def list_account_summaries(self, state_path: str | Path) -> NewApiAccountListResult:
        path = Path(state_path)
        status, rows = _read_account_rows(path)
        if status != "ok":
            return NewApiAccountListResult(status)
        summaries = tuple(_account_summary(row) for row in rows if isinstance(row, dict))
        return NewApiAccountListResult("ok", summaries)

    def _resolve_account_targets(
        self,
        state_path: str | Path,
        selector: str,
        *,
        max_targets: int = MAX_ACCOUNT_LIST_ROWS,
    ) -> NewApiCheckinTargetsResult:
        status, rows = _read_account_rows(Path(state_path))
        if status != "ok":
            return NewApiCheckinTargetsResult(status)
        if not rows:
            return NewApiCheckinTargetsResult("empty")

        targets: list[NewApiCheckinTarget] = []
        for index, row in enumerate(rows, start=1):
            raw_id = row.get("api_user_id")
            api_user_id = str(raw_id) if isinstance(raw_id, (str, int)) and not isinstance(raw_id, bool) else ""
            secret_value = row.get("secret") or ""
            base_url_value = row.get("base_url") or ""
            mode_value = row.get("mode") or ""
            if (
                not api_user_id
                or len(api_user_id) > MAX_ACCOUNT_ID_CHARS
                or not api_user_id.isdigit()
                or not isinstance(secret_value, str)
                or len(secret_value) > MAX_CHECKIN_SECRET_CHARS
                or not isinstance(base_url_value, str)
                or len(base_url_value) > MAX_CHECKIN_BASE_URL_CHARS
                or not isinstance(mode_value, str)
                or len(mode_value) > 16
            ):
                return NewApiCheckinTargetsResult("invalid")
            targets.append(
                NewApiCheckinTarget(
                    index=index,
                    api_user_id=api_user_id,
                    mode=mode_value,
                    secret=secret_value,
                    base_url=base_url_value,
                )
            )

        indices, message = _checkin_selector_indices(selector, len(targets))
        if message:
            return NewApiCheckinTargetsResult("invalid_selector", message=message)
        selected = indices or ()
        if len(selected) > max_targets:
            return NewApiCheckinTargetsResult(
                "too_many",
                message=f"一次最多查询 {max_targets} 个账号",
            )
        return NewApiCheckinTargetsResult("ok", tuple(targets[index - 1] for index in selected))

    def resolve_checkin_targets(self, state_path: str | Path, selector: str) -> NewApiCheckinTargetsResult:
        return self._resolve_account_targets(state_path, selector)

    def resolve_info_targets(self, state_path: str | Path, selector: str) -> NewApiCheckinTargetsResult:
        return self._resolve_account_targets(state_path, selector, max_targets=MAX_INFO_TARGETS)

    def append_checkin_history(
        self,
        history_path: str | Path,
        *,
        account_index: int,
        api_user_id: str,
        base_url_stored: str,
        summary: str,
        source: str = "manual",
    ) -> None:
        row = {
            "at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "index": int(account_index),
            "api_user_id": str(api_user_id)[:MAX_ACCOUNT_ID_CHARS],
            "base_url": _display_base_url(base_url_stored),
            "summary": str(summary or "")[:500],
            "source": source if source in {"manual", "auto"} else "manual",
        }

        def prepend(rows):
            rows = [entry for entry in rows if isinstance(entry, dict)]
            return [row, *rows][:MAX_CHECKIN_HISTORY_ROWS]

        update_json_file_bounded(
            history_path,
            [],
            prepend,
            max_bytes=MAX_CHECKIN_HISTORY_BYTES,
            expected_type=list,
            indent=2,
        )

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
