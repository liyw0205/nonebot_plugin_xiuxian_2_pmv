"""NewAPI 账号绑定存储。"""
from __future__ import annotations

from datetime import datetime
import time
from pathlib import Path
from typing import Any, Literal

from .newapi_client import normalize_base_url
from ...xiuxian_utils.json_store import (
    load_json_file,
    save_json_file,
    update_json_file,
)
from ....paths import get_paths
from ....features.entertainment.application import EntertainmentApplication
from ....infrastructure.ids import UUIDGenerator

AuthMode = Literal["token", "cookie"]

_DATA_DIR = Path(__file__).resolve().parent / "data" / "newapi_bindings"
_DATA_DIR.mkdir(parents=True, exist_ok=True)

_CHECKIN_HISTORY_MAX = 3
_HISTORY_DIR = Path(__file__).resolve().parent / "data" / "newapi_checkin_history"
_HISTORY_DIR.mkdir(parents=True, exist_ok=True)

entertainment_application = EntertainmentApplication(get_paths().game_db)
runtime_ids = UUIDGenerator()


def _run_entertainment_write(
    action: str,
    operation_id: str,
    user_id: str,
    call,
    **payload: Any,
) -> tuple[bool, str]:
    def invoke():
        result = call()
        # The historical JSON store mutators return None on success.  Make
        # that contract explicit before the application normalizes the result.
        return {"status": "applied"} if result is None else result

    outcome = entertainment_application.execute_legacy_call(
        operation_id=str(operation_id),
        user_id=str(user_id),
        action=action,
        payload=payload,
        call=invoke,
    )
    data = dict(outcome.data or {})
    message = str(outcome.message or data.get("message") or data.get("result") or "")
    return outcome.ok, message


def _norm_stored_base(url: str | None) -> str:
    return (url or "").strip().rstrip("/")


def display_base_url(stored: str | None) -> str:
    n = _norm_stored_base(stored)
    if n:
        return n
    return "—"


def _path_for_qq(qq_id: str) -> Path:
    safe = "".join(c if c.isalnum() else "_" for c in str(qq_id))
    return _DATA_DIR / f"{safe}.json"


def _history_path(qq_id: str) -> Path:
    safe = "".join(c if c.isalnum() else "_" for c in str(qq_id))
    return _HISTORY_DIR / f"{safe}.json"


def load_accounts(qq_id: str) -> list[dict[str, Any]]:
    data = load_json_file(_path_for_qq(qq_id), [], list)
    return [x for x in data if isinstance(x, dict)]


def save_accounts(qq_id: str, accounts: list[dict[str, Any]]) -> None:
    save_json_file(_path_for_qq(qq_id), accounts, indent=2)


def append_account(
    qq_id: str,
    *,
    mode: AuthMode,
    api_user_id: str,
    secret: str,
    base_url: str,
    label: str = "",
    operation_id: str | None = None,
) -> tuple[bool, str]:
    api_user_id = str(api_user_id).strip()
    secret = (secret or "").strip()
    if not api_user_id.isdigit() or int(api_user_id) <= 0:
        return False, "站点用户 ID 须为正整数"
    if not secret:
        return False, "令牌或 Cookie 不能为空"

    base_stored = normalize_base_url(base_url) if (base_url or "").strip() else ""
    if not base_stored:
        return False, "须填写接口地址（绑定格式：站点用户ID#密钥#接口）"

    op_id = operation_id or f"entertainment:newapi-bind:{qq_id}:{api_user_id}:{base_stored}:{runtime_ids.new_id()}"
    payload = {
        "mode": mode,
        "api_user_id": api_user_id,
        "secret": secret,
        "base_url": base_stored,
        "label": (label or "").strip(),
    }
    accounts = load_accounts(qq_id)
    for acc in accounts:
        if (
            str(acc.get("api_user_id")) == api_user_id
            and _norm_stored_base(acc.get("base_url")) == _norm_stored_base(base_stored)
        ):
            hint = display_base_url(base_stored)
            duplicate = f"已存在相同站点用户 {api_user_id}（{hint}）"
            ok, message = _run_entertainment_write(
                "newapi_bind",
                op_id,
                str(qq_id),
                lambda: {"status": "rejected", "message": duplicate},
                **payload,
            )
            if ok:
                return True, message or duplicate
            return False, message or duplicate

    accounts.append({
        "mode": mode,
        "api_user_id": api_user_id,
        "secret": secret,
        "base_url": base_stored,
        "label": (label or "").strip(),
        "auto_checkin": False,
    })
    success_message = f"已绑定第 {len(accounts)} 个账号（站点用户 {api_user_id} · {display_base_url(base_stored)}）"
    ok, message = _run_entertainment_write(
        "newapi_bind",
        op_id,
        str(qq_id),
        lambda: (save_accounts(qq_id, accounts), {"status": "applied", "message": success_message})[1],
        **payload,
    )
    if not ok:
        return False, message or "绑定失败"
    return True, message or success_message


def delete_accounts(
    qq_id: str,
    indices: list[int] | None,
    *,
    operation_id: str | None = None,
) -> tuple[bool, str]:
    requested = "all" if indices is None else sorted({int(index) for index in indices})
    op_id = operation_id or f"entertainment:newapi-delete:{qq_id}:{requested}:{runtime_ids.new_id()}"
    payload = {"indices": requested}
    accounts = load_accounts(qq_id)
    if not accounts:
        message = "当前没有已绑定的 NewAPI 账号"
        ok, replay_message = _run_entertainment_write(
            "newapi_delete",
            op_id,
            str(qq_id),
            lambda: {"status": "rejected", "message": message},
            **payload,
        )
        return (True, replay_message) if ok else (False, replay_message or message)

    if indices is None:
        n = len(accounts)
        success_message = f"已删除全部 {n} 个绑定"
        ok, message = _run_entertainment_write(
            "newapi_delete",
            op_id,
            str(qq_id),
            lambda: (save_accounts(qq_id, []), {"status": "applied", "message": success_message})[1],
            **payload,
        )
        if not ok:
            return False, message or "删除失败"
        return True, message or success_message

    to_remove = sorted({i for i in requested if 1 <= i <= len(accounts)}, reverse=True)
    if not to_remove:
        invalid = f"序号无效，当前共 {len(accounts)} 个账号（1～{len(accounts)}）"
        ok, message = _run_entertainment_write(
            "newapi_delete",
            op_id,
            str(qq_id),
            lambda: {"status": "rejected", "message": invalid},
            **payload,
        )
        return (True, message or invalid) if ok else (False, message or invalid)

    for i in to_remove:
        accounts.pop(i - 1)
    success_message = f"已删除 {len(to_remove)} 个绑定，剩余 {len(accounts)} 个"
    ok, message = _run_entertainment_write(
        "newapi_delete",
        op_id,
        str(qq_id),
        lambda: (save_accounts(qq_id, accounts), {"status": "applied", "message": success_message})[1],
        **payload,
    )
    if not ok:
        return False, message or "删除失败"
    return True, message or success_message


def resolve_targets(qq_id: str, index_text: str) -> tuple[list[dict[str, Any]] | None, str | None]:
    accounts = load_accounts(qq_id)
    if not accounts:
        return None, "尚未绑定账号，请使用：newapi绑定 站点用户ID#令牌#接口地址"

    text = (index_text or "").strip()
    if not text:
        return accounts, None

    indices: set[int] = set()
    for part in text.replace("，", ",").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            try:
                lo, hi = int(a.strip()), int(b.strip())
                for i in range(min(lo, hi), max(lo, hi) + 1):
                    indices.add(i)
            except ValueError:
                return None, f"无法解析序号段：{part}"
        else:
            try:
                indices.add(int(part))
            except ValueError:
                return None, f"无法解析序号：{part}"

    picked = []
    for i in sorted(indices):
        if i < 1 or i > len(accounts):
            return None, f"序号 {i} 超出范围（1～{len(accounts)}）"
        picked.append(accounts[i - 1])
    if not picked:
        return None, "请指定序号，例如：newapi签到 1"
    return picked, None


def account_index(accounts: list[dict[str, Any]], acc: dict[str, Any]) -> int:
    for j, full in enumerate(accounts, start=1):
        if (
            str(full.get("api_user_id")) == str(acc.get("api_user_id"))
            and _norm_stored_base(full.get("base_url")) == _norm_stored_base(acc.get("base_url"))
        ):
            return j
    return 1


def append_checkin_history(
    qq_id: str,
    *,
    account_index: int,
    api_user_id: str,
    base_url_stored: str,
    summary: str,
    source: str = "manual",
) -> None:
    row = {
        "at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "index": account_index,
        "api_user_id": str(api_user_id),
        "base_url": _norm_stored_base(base_url_stored),
        "summary": (summary or "")[:500],
        "source": source,
    }

    def prepend(rows):
        rows = [x for x in rows if isinstance(x, dict)]
        return [row, *rows][:_CHECKIN_HISTORY_MAX]

    update_json_file(_history_path(qq_id), [], prepend, expected_type=list, indent=2)


def load_checkin_history(qq_id: str) -> list[dict[str, Any]]:
    data = load_json_file(_history_path(qq_id), [], list)
    return [x for x in data if isinstance(x, dict)][: _CHECKIN_HISTORY_MAX]


def toggle_auto_checkin(
    qq_id: str,
    index_text: str,
    *,
    operation_id: str | None = None,
) -> tuple[bool, str]:
    text = (index_text or "").strip()
    op_id = operation_id or f"entertainment:newapi-auto:{qq_id}:{text}:{runtime_ids.new_id()}"
    payload = {"index_text": text}
    accounts = load_accounts(qq_id)
    if not accounts:
        message = "尚未绑定账号"
        ok, replay_message = _run_entertainment_write(
            "newapi_auto_checkin",
            op_id,
            str(qq_id),
            lambda: {"status": "rejected", "message": message},
            **payload,
        )
        return (True, replay_message) if ok else (False, replay_message or message)

    if not text:
        message = "请指定序号，例如：newapi自动签到 1"
        ok, replay_message = _run_entertainment_write(
            "newapi_auto_checkin",
            op_id,
            str(qq_id),
            lambda: {"status": "rejected", "message": message},
            **payload,
        )
        return (True, replay_message) if ok else (False, replay_message or message)

    targets, err = resolve_targets(qq_id, text)
    if err or not targets or len(targets) != 1:
        message = err or "请只写一个序号，例如：newapi自动签到 1"
        ok, replay_message = _run_entertainment_write(
            "newapi_auto_checkin",
            op_id,
            str(qq_id),
            lambda: {"status": "rejected", "message": message},
            **payload,
        )
        return (True, replay_message) if ok else (False, replay_message or message)

    acc = targets[0]
    idx = account_index(accounts, acc)
    for i, row in enumerate(accounts):
        if i == idx - 1:
            outcome = entertainment_application.toggle_auto_checkin(
                operation_id=op_id,
                user_id=str(qq_id),
                state_path=_path_for_qq(qq_id),
                index=idx,
            )
            if not outcome.ok:
                return False, str(outcome.message or "操作失败")
            enabled = bool((outcome.data or {}).get("enabled"))
            state = "已开启" if enabled else "已关闭"
            return True, f"账号 {idx}（站点用户 {acc.get('api_user_id')}）自动签到{state}"
    return False, "未找到账号"


def iter_all_auto_checkin_bindings() -> list[tuple[str, int, dict[str, Any]]]:
    out: list[tuple[str, int, dict[str, Any]]] = []
    for p in _DATA_DIR.glob("*.json"):
        qq_key = p.stem
        data = load_json_file(p, [], list)
        for i, acc in enumerate(data, start=1):
            if isinstance(acc, dict) and acc.get("auto_checkin"):
                out.append((qq_key, i, acc))
    return out


def list_binding_files() -> list[Path]:
    return list(_DATA_DIR.glob("*.json"))
